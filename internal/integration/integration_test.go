//go:build integration

// Package integration runs the pipeline against real Redis, Neo4j and MinIO
// started with testcontainers-go. It needs Docker:
//
//	go test -race -tags integration ./internal/integration/...
package integration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	tcminio "github.com/testcontainers/testcontainers-go/modules/minio"
	tcneo4j "github.com/testcontainers/testcontainers-go/modules/neo4j"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/graph"
	"github.com/appu/leadgraph/internal/pipeline"
	"github.com/appu/leadgraph/internal/pipeline/pipelinetest"
	"github.com/appu/leadgraph/internal/store"
	"github.com/appu/leadgraph/internal/suppress"
	"github.com/appu/leadgraph/internal/testsite"
	"github.com/appu/leadgraph/pkg/model"
)

// infra is the set of containers shared by every test in the package.
var infra struct {
	redisAddr string
	neo4j     config.Neo4j
	s3        config.S3
}

func randomSecret() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// image returns the container image to use, overridable per environment.
func image(env, fallback string) string {
	if v := os.Getenv(env); v != "" {
		return v
	}
	return fallback
}

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()
	// Credentials are generated per run; nothing is hard-coded.
	neoPass, minioUser, minioPass := randomSecret(), "lg"+randomSecret()[:8], randomSecret()

	redisC, err := tcredis.Run(ctx, image("LEADGRAPH_TEST_REDIS_IMAGE", "redis:8"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting redis:", err)
		return 1
	}
	defer redisC.Terminate(ctx)
	uri, err := redisC.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "redis address:", err)
		return 1
	}
	infra.redisAddr = strings.TrimPrefix(uri, "redis://")

	neoC, err := tcneo4j.Run(ctx, image("LEADGRAPH_TEST_NEO4J_IMAGE", "neo4j:5-community"), tcneo4j.WithAdminPassword(neoPass))
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting neo4j:", err)
		return 1
	}
	defer neoC.Terminate(ctx)
	bolt, err := neoC.BoltUrl(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "neo4j address:", err)
		return 1
	}
	infra.neo4j = config.Neo4j{URI: bolt, Username: "neo4j", Password: neoPass, Database: "neo4j"}

	minioC, err := tcminio.Run(ctx, image("LEADGRAPH_TEST_MINIO_IMAGE", "cgr.dev/chainguard/minio:latest"),
		tcminio.WithUsername(minioUser), tcminio.WithPassword(minioPass))
	if err != nil {
		fmt.Fprintln(os.Stderr, "starting minio:", err)
		return 1
	}
	defer minioC.Terminate(ctx)
	endpoint, err := minioC.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "minio address:", err)
		return 1
	}
	infra.s3 = config.S3{Endpoint: endpoint, AccessKey: minioUser, SecretKey: minioPass, Region: "us-east-1"}

	return m.Run()
}

type world struct {
	site *testsite.Site
	cfg  config.Config
	rdb  *redis.Client
	blob *store.S3
	neo  *graph.Neo4j
	p    *pipeline.Pipeline
}

// newWorld gives a test its own Redis key prefix and S3 bucket and an empty,
// migrated graph.
func newWorld(t testing.TB, mutate func(*config.Config)) *world {
	t.Helper()
	ctx := context.Background()
	site, addr := pipelinetest.StartSite(t)
	cfg := pipelinetest.Config(addr)
	id := randomSecret()[:10]
	cfg.Redis.Addr, cfg.Redis.KeyPrefix = infra.redisAddr, "it"+id
	cfg.Neo4j = infra.neo4j
	cfg.S3 = infra.s3
	cfg.S3.Bucket = "leadgraph-" + id
	if mutate != nil {
		mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := cfg.RequireStores(true, true); err != nil {
		t.Fatal(err)
	}
	rdb := pipeline.NewRedis(cfg.Redis)
	t.Cleanup(func() { rdb.Close() })
	blob, err := store.NewS3(ctx, cfg.S3)
	if err != nil {
		t.Fatal(err)
	}
	neo, err := graph.NewNeo4j(ctx, cfg.Neo4j, cfg.Graph, pipelinetest.Logger())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { neo.Close(context.Background()) })
	if _, err := neo.Query(ctx, "RETURN 1", nil); err != nil {
		t.Fatal(err)
	}
	wipe(t, neo)
	if _, err := neo.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	w := &world{site: site, cfg: cfg, rdb: rdb, blob: blob, neo: neo}
	w.p = w.pipeline(cfg)
	return w
}

func (w *world) pipeline(cfg config.Config) *pipeline.Pipeline {
	return pipeline.New(pipeline.Deps{Cfg: cfg, RDB: w.rdb, Blob: w.blob, Graph: w.neo, Log: pipelinetest.Logger()})
}

// wipe removes all data (not the schema) so tests do not see each other.
func wipe(t testing.TB, neo *graph.Neo4j) {
	t.Helper()
	if err := exec(neo, `MATCH (n) WHERE NOT n:SchemaMigration DETACH DELETE n`, nil); err != nil {
		t.Fatal(err)
	}
}

// exec runs a write statement.
func exec(neo *graph.Neo4j, cypher string, params map[string]any) error {
	return neo.Exec(context.Background(), cypher, params)
}

func (w *world) query(t testing.TB, cypher string, params map[string]any) []map[string]any {
	t.Helper()
	rows, err := w.neo.Query(context.Background(), cypher, params)
	if err != nil {
		t.Fatalf("query failed: %v\n%s", err, cypher)
	}
	return rows
}

func (w *world) strings(t testing.TB, cypher string, params map[string]any) []string {
	t.Helper()
	var out []string
	for _, r := range w.query(t, cypher, params) {
		for _, v := range r {
			out = append(out, fmt.Sprint(v))
		}
	}
	sort.Strings(out)
	return out
}

func (w *world) count(t testing.TB, cypher string, params map[string]any) int64 {
	t.Helper()
	rows := w.query(t, cypher, params)
	if len(rows) != 1 {
		t.Fatalf("count query returned %d rows", len(rows))
	}
	for _, v := range rows[0] {
		n, _ := v.(int64)
		return n
	}
	return 0
}

func TestMigrationsAreIdempotent(t *testing.T) {
	w := newWorld(t, nil)
	ctx := context.Background()
	applied, err := w.neo.Migrate(ctx)
	if err != nil || len(applied) != 0 {
		t.Errorf("second migrate applied %v, err %v", applied, err)
	}
	constraints := w.strings(t, `SHOW CONSTRAINTS YIELD name RETURN name`, nil)
	for _, want := range []string{"company_id", "domain_name", "person_id", "email_address", "phone_e164", "social_url", "technology_name", "location_id", "page_url"} {
		if !contains(constraints, want) {
			t.Errorf("constraint %s missing (have %v)", want, constraints)
		}
	}
	indexes := w.query(t, `SHOW INDEXES YIELD name, type, state RETURN name, type, state`, nil)
	have := map[string]string{}
	for _, r := range indexes {
		have[fmt.Sprint(r["name"])] = fmt.Sprint(r["type"]) + "/" + fmt.Sprint(r["state"])
	}
	for name, typ := range map[string]string{"company_name": "RANGE/ONLINE", "person_full_name": "RANGE/ONLINE", "location_country": "RANGE/ONLINE", "company_search": "FULLTEXT/ONLINE"} {
		if have[name] != typ {
			t.Errorf("index %s = %q, want %s", name, have[name], typ)
		}
	}
	if n := w.count(t, `MATCH (m:SchemaMigration) RETURN count(m) AS n`, nil); n != int64(len(graph.Migrations)) {
		t.Errorf("recorded migrations = %d", n)
	}
	// The uniqueness constraint really is enforced by the database.
	if err := exec(w.neo, `CREATE (:Company {id: 'dup'}), (:Company {id: 'dup'})`, nil); err == nil {
		t.Error("duplicate Company.id was accepted")
	}
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func TestFullPipeline(t *testing.T) {
	w := newWorld(t, nil)
	ctx := context.Background()
	pipelinetest.Crawl(t, w.p, pipelinetest.Seeds...)

	// Companies.
	rows := w.query(t, `MATCH (c:Company)-[:HAS_DOMAIN]->(d:Domain {name: 'acme.test'})
		RETURN c.name AS name, c.founded AS founded, c.country AS country, c.size_hint AS size, c.name_conf AS conf, size(c.facts) AS facts,
		       c.first_seen <= c.last_seen AS ordered`, nil)
	if len(rows) != 1 || rows[0]["name"] != "Acme Robotics" || rows[0]["founded"] != "2012" || rows[0]["country"] != "US" ||
		rows[0]["size"] != "51-200" || rows[0]["conf"] != 0.95 || rows[0]["ordered"] != true || rows[0]["facts"].(int64) < 4 {
		t.Errorf("acme = %v", rows)
	}

	// People, deduplicated across /about and /team.
	people := w.strings(t, `MATCH (:Company {name: 'Acme Robotics'})-[:EMPLOYS]->(p:Person) RETURN p.full_name AS n`, nil)
	if strings.Join(people, ", ") != "Jane Doe, Priya Raman, Rahul Mehta, Tom O'Brien" {
		t.Errorf("acme people = %v", people)
	}
	if n := w.count(t, `MATCH (p:Person {full_name: 'Jane Doe'})<-[:MENTIONS]-(pg:Page) RETURN count(DISTINCT pg) AS n`, nil); n != 2 {
		t.Errorf("Jane should be one node mentioned by two pages, got %d", n)
	}
	jane := w.query(t, `MATCH (p:Person {full_name: 'Jane Doe'})
		OPTIONAL MATCH (p)-[:HAS_EMAIL]->(e:Email) OPTIONAL MATCH (p)-[:HAS_PHONE]->(ph:Phone) OPTIONAL MATCH (p)-[:HAS_SOCIAL]->(s:SocialProfile)
		RETURN p.seniority AS seniority, collect(DISTINCT e.address) AS emails, collect(DISTINCT ph.e164) AS phones, collect(DISTINCT s.url) AS socials`, nil)
	if len(jane) != 1 || fmt.Sprint(jane[0]["emails"]) != "[jane.doe@acme.test]" || fmt.Sprint(jane[0]["phones"]) != "[+14155552673]" ||
		fmt.Sprint(jane[0]["socials"]) != "[https://www.linkedin.com/in/janedoe]" {
		t.Errorf("jane = %v", jane)
	}
	if got := w.strings(t, `MATCH (:Person {full_name: 'Rahul Mehta'})-[:HAS_EMAIL]->(e) RETURN e.address AS a`, nil); fmt.Sprint(got) != "[rahul@acme.test]" {
		t.Errorf("rahul emails = %v", got)
	}
	if got := w.strings(t, `MATCH (:Person {full_name: "Tom O'Brien"})-[:HAS_EMAIL]->(e) RETURN e.address AS a`, nil); fmt.Sprint(got) != "[tom.obrien@acme.test]" {
		t.Errorf("tom emails = %v", got)
	}

	// Relationship provenance.
	rel := w.query(t, `MATCH (:Company {name: 'Acme Robotics'})-[r:EMPLOYS]->(:Person {full_name: 'Rahul Mehta'})
		RETURN r.title AS title, r.source_url AS src, r.confidence AS conf, r.first_seen <= r.last_seen AS ordered, size(r.sources) AS sources`, nil)
	if len(rel) != 1 || rel[0]["title"] != "CTO & Co-Founder" || !strings.HasPrefix(fmt.Sprint(rel[0]["src"]), "http://acme.test/") ||
		rel[0]["conf"].(float64) <= 0 || rel[0]["ordered"] != true || rel[0]["sources"].(int64) != 2 {
		t.Errorf("EMPLOYS relationship = %v", rel)
	}
	if n := w.count(t, `MATCH ()-[r]->() WHERE r.source_url IS NULL OR r.confidence IS NULL OR r.first_seen IS NULL OR r.last_seen IS NULL RETURN count(r) AS n`, nil); n != 0 {
		t.Errorf("%d relationships lack provenance properties", n)
	}

	// Company contacts, technology, location.
	emails := w.strings(t, `MATCH (:Company {name: 'Acme Robotics'})-[:HAS_EMAIL]->(e:Email) RETURN e.address AS a`, nil)
	for _, want := range []string{"hello@acme.test", "sales@acme.test", "support@acme.test", "press@acme.test", "legacy@acme.test", "status@acme.test"} {
		if !contains(emails, want) {
			t.Errorf("company emails %v lack %s", emails, want)
		}
	}
	if n := w.count(t, `MATCH (e:Email) WHERE e.address IN ['forbidden@acme.test', 'deep@acme.test', 'secret.person@acme.test', 'you@example.com'] RETURN count(e) AS n`, nil); n != 0 {
		t.Errorf("%d emails from disallowed, too-deep, noindex or placeholder sources were stored", n)
	}
	if n := w.count(t, `MATCH (e:Email) WHERE e.status <> 'unverified' OR NOT e.kind IN ['role', 'personal'] OR e.domain IS NULL RETURN count(e) AS n`, nil); n != 0 {
		t.Errorf("%d Email nodes have bad properties", n)
	}
	tech := w.strings(t, `MATCH (:Company {name: 'Acme Robotics'})-[:USES]->(t:Technology) RETURN t.name AS n`, nil)
	for _, want := range []string{"WordPress", "Google Analytics", "HubSpot", "Nginx"} {
		if !contains(tech, want) {
			t.Errorf("technologies %v lack %s", tech, want)
		}
	}
	loc := w.query(t, `MATCH (:Company {name: 'Acme Robotics'})-[:LOCATED_AT]->(l:Location) RETURN l.city AS city, l.country AS country, l.postal AS postal, l.street AS street`, nil)
	if len(loc) != 1 || loc[0]["city"] != "San Francisco" || loc[0]["country"] != "US" || loc[0]["postal"] != "94105" {
		t.Errorf("location = %v", loc)
	}

	// The example queries from docs/graph-schema.md.
	decisionMakers := w.strings(t, `MATCH (t:Technology {name: $tech})<-[:USES]-(c:Company)-[:LOCATED_AT]->(:Location {country: $country})
		MATCH (c)-[:EMPLOYS]->(p:Person) WHERE p.seniority IN ['founder', 'c_level', 'vp', 'director']
		RETURN DISTINCT p.full_name AS n`, map[string]any{"tech": "HubSpot", "country": "US"})
	if strings.Join(decisionMakers, ", ") != "Jane Doe, Priya Raman, Rahul Mehta, Tom O'Brien" {
		t.Errorf("decision makers at HubSpot users in the US = %v", decisionMakers)
	}
	linked := w.strings(t, `MATCH (:Company {name: $name})-[:LINKS_TO]->(c:Company) RETURN c.name AS n`, map[string]any{"name": "Globex Industries"})
	if fmt.Sprint(linked) != "[Acme Robotics]" {
		t.Errorf("companies linked from Globex = %v", linked)
	}
	if n := w.count(t, `MATCH (:Company {name: 'Acme Robotics'})-[:LINKS_TO]->(:Company {name: 'Globex Industries'}) RETURN count(*) AS n`, nil); n != 1 {
		t.Errorf("acme -> globex LINKS_TO count = %d", n)
	}
	search := w.strings(t, `CALL db.index.fulltext.queryNodes('company_search', 'robots') YIELD node RETURN node.name AS n`, nil)
	if !contains(search, "Acme Robotics") {
		t.Errorf("fulltext search for 'robots' = %v", search)
	}

	// Pages.
	pages := w.query(t, `MATCH (:Domain {name: 'acme.test'})-[:HAS_PAGE]->(p:Page) RETURN p.url AS url, p.type AS type, p.status AS status, p.skip_reason AS skip, p.raw_key AS raw`, nil)
	byURL := map[string]map[string]any{}
	for _, p := range pages {
		byURL[fmt.Sprint(p["url"])] = p
	}
	for url, typ := range map[string]string{"http://acme.test/": "home", "http://acme.test/about": "about", "http://acme.test/team": "team", "http://acme.test/contact": "contact"} {
		if p := byURL[url]; p == nil || p["type"] != typ || p["status"] != int64(200) || p["raw"] == nil {
			t.Errorf("page %s = %v", url, p)
		}
	}
	if p := byURL["http://acme.test/private/secret"]; p == nil || p["skip"] != model.SkipRobots {
		t.Errorf("robots-blocked page = %v", p)
	}
	if byURL["http://acme.test/leadership"] != nil {
		t.Error("non-existent well-known path became a Page")
	}

	// Compliance on the wire.
	for _, path := range []string{"/private/secret", "/members-only", "/careers", "/blog/third-post", "/brochure.pdf"} {
		if n := w.site.Hits(testsite.Acme, path); n != 0 {
			t.Errorf("%s was requested %d times", path, n)
		}
	}
	if n := w.site.Hits(testsite.Acme, "/robots.txt"); n != 1 {
		t.Errorf("robots.txt fetched %d times", n)
	}
	for _, host := range []string{testsite.Acme, testsite.Globex} {
		if peak := w.site.PeakConcurrency(host); peak != 1 {
			t.Errorf("%s saw %d concurrent requests", host, peak)
		}
	}

	// Raw and extraction objects are in MinIO and addressable from the graph.
	team := byURL["http://acme.test/team"]
	off := w.query(t, `MATCH (p:Page {url: 'http://acme.test/team'}) RETURN p.raw_offset AS off, p.raw_length AS len, p.extract_key AS ek`, nil)[0]
	rec, err := store.ReadRecord(ctx, w.blob, store.Ref{Key: team["raw"].(string), Offset: off["off"].(int64), Length: off["len"].(int64)})
	if err != nil || !strings.Contains(string(rec.Body), "Meet the Team") {
		t.Errorf("raw record not readable via ranged GET: %v", err)
	}
	var ex model.Extraction
	if err := store.GetJSON(ctx, w.blob, off["ek"].(string), &ex); err != nil || len(ex.People) < 4 {
		t.Errorf("extraction object: %d people, err %v", len(ex.People), err)
	}

	// Redis: RedisBloom is in use and every stream is drained.
	typ, _ := w.rdb.Type(ctx, w.cfg.Redis.KeyPrefix+":seen:"+w.cfg.Crawl.Epoch).Result()
	if !strings.HasPrefix(typ, "MBbloom") {
		t.Errorf("dedup key type = %q; expected a RedisBloom filter", typ)
	}
	lag, _ := w.p.Lag(ctx)
	for stream, n := range lag {
		if n != 0 {
			t.Errorf("stream %s has %d messages left", stream, n)
		}
	}

	// A recrawl in a new epoch changes nothing but timestamps.
	before := w.snapshot(t)
	cfg := w.cfg
	cfg.Crawl.Epoch = "2"
	pipelinetest.Crawl(t, w.pipeline(cfg), pipelinetest.Seeds...)
	pipelinetest.Diff(t, before, w.snapshot(t))
	if n := w.count(t, `MATCH (p:Page {url: 'http://acme.test/'}) RETURN CASE WHEN p.last_seen > p.first_seen THEN 1 ELSE 0 END AS n`, nil); n != 1 {
		t.Error("recrawl did not advance last_seen")
	}
}

// snapshot renders the Neo4j graph in the same form as
// pipelinetest.SnapshotMemory.
func (w *world) snapshot(t testing.TB) []string {
	t.Helper()
	var out []string
	for _, spec := range model.Labels {
		rows := w.query(t, fmt.Sprintf(`MATCH (n:%s) RETURN n.%s AS key, n.name AS name, n.full_name AS full_name, n.type AS type, n.status AS status, n.skip_reason AS skip`, spec.Label, spec.Key), nil)
		for _, r := range rows {
			// Compare only merge-ruled scalars, as SnapshotMemory does:
			// Domain.name is an identity key and Email.status a plain
			// property.
			name, typ, status, skip := r["name"], r["type"], r["status"], r["skip"]
			if spec.Label != model.LabelCompany {
				name = nil
			}
			if spec.Label != model.LabelPage {
				typ, status, skip = nil, nil, nil
			}
			out = append(out, pipelinetest.SnapshotNode(spec.Label, fmt.Sprint(r["key"]), name, r["full_name"], typ, status, skip))
		}
	}
	for _, spec := range model.Edges {
		from, _ := model.LabelByName(spec.From)
		to, _ := model.LabelByName(spec.To)
		rows := w.query(t, fmt.Sprintf(`MATCH (a:%s)-[:%s]->(b:%s) RETURN a.%s AS f, b.%s AS t`, spec.From, spec.Type, spec.To, from.Key, to.Key), nil)
		for _, r := range rows {
			out = append(out, pipelinetest.SnapshotEdge(spec.Type, spec.From, fmt.Sprint(r["f"]), spec.To, fmt.Sprint(r["t"])))
		}
	}
	sort.Strings(out)
	return out
}

// memoryBaseline runs the same crawl against miniredis and in-memory stores.
func memoryBaseline(t *testing.T) []string {
	t.Helper()
	_, addr := pipelinetest.StartSite(t)
	cfg := pipelinetest.Config(addr)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	mem := graph.NewMemory(cfg.Graph.MaxFacts)
	p := pipeline.New(pipeline.Deps{Cfg: cfg, RDB: rdb, Blob: store.NewMem(), Graph: mem, Log: pipelinetest.Logger()})
	pipelinetest.Crawl(t, p, pipelinetest.Seeds...)
	return pipelinetest.SnapshotMemory(mem)
}

// TestChaosParserKilledMidBatch is the crash-recovery proof against the real
// stores: the graph in Neo4j after a parser is killed mid-batch and replaced
// must equal the graph of an undisturbed crawl.
func TestChaosParserKilledMidBatch(t *testing.T) {
	want := memoryBaseline(t)

	w := newWorld(t, nil)
	doomed := w.cfg
	doomed.Queue.DrainTimeout = 0
	pipelinetest.KillParserMidBatch(t, w.p, w.pipeline(doomed))

	pipelinetest.Diff(t, want, w.snapshot(t))
	// No duplicate nodes, relationships or facts.
	for _, spec := range model.Labels {
		if n := w.count(t, fmt.Sprintf(`MATCH (n:%s) WITH n.%s AS k, count(*) AS c WHERE c > 1 RETURN count(k) AS n`, spec.Label, spec.Key), nil); n != 0 {
			t.Errorf("%d duplicated %s keys", n, spec.Label)
		}
	}
	if n := w.count(t, `MATCH (a)-[r]->(b) WITH a, b, type(r) AS t, count(*) AS c WHERE c > 1 RETURN count(*) AS n`, nil); n != 0 {
		t.Errorf("%d duplicated relationships", n)
	}
	if n := w.count(t, `MATCH (n) WHERE n.facts IS NOT NULL
		WITH n, size(n.facts) AS total, size(reduce(acc = [], f IN n.facts | CASE WHEN f IN acc THEN acc ELSE acc + f END)) AS uniq
		WHERE total <> uniq RETURN count(n) AS n`, nil); n != 0 {
		t.Errorf("%d nodes have duplicated facts", n)
	}
	if n, _ := w.p.Streams.Len(context.Background(), "dlq"); n != 0 {
		t.Errorf("%d dead letters", n)
	}
	for path, n := range w.site.Paths() {
		if n > 1 && !strings.HasSuffix(path, "/flaky") {
			t.Errorf("%s fetched %d times", path, n)
		}
	}
}

func TestMergeRulesInCypher(t *testing.T) {
	w := newWorld(t, nil)
	ctx := context.Background()
	t0 := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	sc := func(v any, c float64, at time.Time) model.Scalar {
		return model.Scalar{Value: v, Confidence: c, At: at}
	}
	company := func(at time.Time, scalars map[string]model.Scalar, facts ...string) *model.GraphMutation {
		return &model.GraphMutation{SourceURL: "https://m.test/", Nodes: []model.NodeUpsert{
			{Label: model.LabelCompany, Key: "m1", Props: map[string]any{"domain": "m.test"}, Scalars: scalars, Facts: facts, SeenAt: at}}}
	}
	apply := func(m ...*model.GraphMutation) {
		t.Helper()
		if err := w.neo.Apply(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	get := func() map[string]any {
		return w.query(t, `MATCH (c:Company {id: 'm1'}) RETURN c.name AS name, c.name_conf AS conf, c.description AS d, c.facts AS facts,
			c.first_seen AS first, c.last_seen AS last`, nil)[0]
	}
	later := t0.Add(time.Hour)
	apply(company(t0, map[string]model.Scalar{"name": sc("Acme", 0.8, t0), "description": sc("old", 0.7, t0)}, "f1", "f2"))
	apply(company(later, map[string]model.Scalar{"name": sc("from-title", 0.4, later), "description": sc("new", 0.7, later)}, "f2", "f3"))
	if g := get(); g["name"] != "Acme" || g["d"] != "new" || fmt.Sprint(g["facts"]) != "[f1 f2 f3]" {
		t.Errorf("lower confidence must not overwrite; equal confidence + newer must: %v", g)
	}
	apply(company(t0.Add(-time.Hour), map[string]model.Scalar{"name": sc("Acme Robotics", 0.95, t0.Add(-time.Hour)), "description": sc("stale", 0.7, t0)}))
	g := get()
	if g["name"] != "Acme Robotics" || g["conf"] != 0.95 || g["d"] != "new" {
		t.Errorf("higher confidence must win even when older; equal confidence + older must lose: %v", g)
	}
	if !g["first"].(time.Time).Equal(t0.Add(-time.Hour)) || !g["last"].(time.Time).Equal(later) {
		t.Errorf("first/last seen = %v / %v", g["first"], g["last"])
	}
	// Two upserts of one node in the same batch: the rule still decides,
	// not the order (Neo4j evaluates all rows before applying any SET).
	apply(company(later, map[string]model.Scalar{"name": sc("low-in-batch", 0.3, later)}),
		company(t0, map[string]model.Scalar{"industry": sc("robotics", 0.9, t0)}),
		company(later, map[string]model.Scalar{"industry": sc("weaker", 0.2, later)}))
	if b := w.query(t, `MATCH (c:Company {id: 'm1'}) RETURN c.name AS name, c.industry AS industry`, nil)[0]; b["name"] != "Acme Robotics" || b["industry"] != "robotics" {
		t.Errorf("same-batch duplicates broke the merge rule: %v", b)
	}
	g = get()
	// Idempotence: the same mutation again, twice in one batch.
	m := company(later, map[string]model.Scalar{"description": sc("new", 0.7, later)}, "f3")
	apply(m, m)
	if after := get(); fmt.Sprint(after) != fmt.Sprint(g) {
		t.Errorf("re-applying changed the node:\n%v\n%v", g, after)
	}
	// Facts are capped, keeping the newest.
	var many []string
	for i := 0; i < w.cfg.Graph.MaxFacts+10; i++ {
		many = append(many, fmt.Sprintf("bulk-%03d", i))
	}
	apply(company(later, nil, many...))
	facts := w.query(t, `MATCH (c:Company {id: 'm1'}) RETURN size(c.facts) AS n, c.facts[-1] AS last, c.facts[0] AS first`, nil)[0]
	if facts["n"] != int64(w.cfg.Graph.MaxFacts) || facts["last"] != many[len(many)-1] || facts["first"] == "f1" {
		t.Errorf("facts cap: %v", facts)
	}

	// Relationships.
	edge := func(conf float64, title, src string, at time.Time) *model.GraphMutation {
		return &model.GraphMutation{SourceURL: src, Edges: []model.EdgeUpsert{{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: "m1",
			ToLabel: model.LabelPerson, ToKey: "mp1", Props: map[string]any{"title": title}, SourceURL: src, Confidence: conf, SeenAt: at}}}
	}
	apply(edge(0.8, "CEO", "https://m.test/team", t0)) // the person does not exist yet
	if n := w.count(t, `MATCH (:Company {id: 'm1'})-[r:EMPLOYS]->() RETURN count(r) AS n`, nil); n != 0 {
		t.Fatal("relationship created to a node that does not exist")
	}
	apply(&model.GraphMutation{Nodes: []model.NodeUpsert{{Label: model.LabelPerson, Key: "mp1", SeenAt: t0}}}, edge(0.8, "CEO", "https://m.test/team", t0))
	apply(edge(0.5, "Chief", "https://m.test/blog", later))
	r := w.query(t, `MATCH (:Company {id: 'm1'})-[r:EMPLOYS]->(:Person {id: 'mp1'}) RETURN r.title AS title, r.confidence AS conf, r.source_url AS src, r.sources AS sources, r.last_seen AS last, count(*) AS n`, nil)[0]
	if r["title"] != "CEO" || r["conf"] != 0.8 || r["src"] != "https://m.test/team" || fmt.Sprint(r["sources"]) != "[https://m.test/team https://m.test/blog]" ||
		!r["last"].(time.Time).Equal(later) || r["n"] != int64(1) {
		t.Errorf("relationship after a lower-confidence update = %v", r)
	}
	apply(edge(0.9, "CEO & Founder", "https://m.test/about", t0))
	if r := w.query(t, `MATCH (:Company {id: 'm1'})-[r:EMPLOYS]->() RETURN r.title AS title, r.confidence AS conf`, nil)[0]; r["title"] != "CEO & Founder" || r["conf"] != 0.9 {
		t.Errorf("higher-confidence relationship update lost: %v", r)
	}
	// Typed scalars survive the JSON round trip the stream imposes.
	apply(&model.GraphMutation{Nodes: []model.NodeUpsert{{Label: model.LabelPage, Key: "https://m.test/", SeenAt: t0, Scalars: map[string]model.Scalar{
		"status": sc(float64(200), 1, t0), "fetched_at": sc("2026-03-01T10:00:00Z", 1, t0), "raw_offset": sc(float64(1234), 1, t0)}}}})
	pg := w.query(t, `MATCH (p:Page {url: 'https://m.test/'}) RETURN p.status AS status, p.fetched_at AS at, p.raw_offset AS off`, nil)[0]
	if pg["status"] != int64(200) || pg["off"] != int64(1234) || !pg["at"].(time.Time).Equal(t0) {
		t.Errorf("page scalars = %v (%T %T)", pg, pg["status"], pg["at"])
	}
	// A mutation with an unknown label is rejected as a whole.
	if err := w.neo.Apply(ctx, []*model.GraphMutation{{Nodes: []model.NodeUpsert{{Label: "Alien", Key: "x"}}}}); err == nil {
		t.Error("unknown label accepted")
	}
}

func TestEraseAgainstRealStores(t *testing.T) {
	w := newWorld(t, nil)
	ctx := context.Background()
	pipelinetest.Crawl(t, w.p, pipelinetest.Seeds...)

	team := w.query(t, `MATCH (p:Page {url: 'http://acme.test/team'}) RETURN p.raw_key AS k, p.raw_offset AS o, p.raw_length AS l, p.extract_key AS e`, nil)[0]
	ref := store.Ref{Key: team["k"].(string), Offset: team["o"].(int64), Length: team["l"].(int64)}
	janeID := w.strings(t, `MATCH (p:Person {full_name: 'Jane Doe'}) RETURN p.id AS id`, nil)[0]

	res, err := w.p.Erase.Email(ctx, "jane.doe@acme.test", true)
	if err != nil {
		t.Fatal(err)
	}
	if res.People != 1 || res.Pages != 2 || res.RawRecords != 2 || res.Extractions < 2 {
		t.Errorf("erase result = %+v", res)
	}
	if n := w.count(t, `MATCH (n) WHERE n.id = $id OR n.address = 'jane.doe@acme.test' OR n.e164 = '+14155552673' OR n.url = 'https://www.linkedin.com/in/janedoe' RETURN count(n) AS n`, map[string]any{"id": janeID}); n != 0 {
		t.Errorf("%d nodes of the erased person remain", n)
	}
	if n := w.count(t, `MATCH (:Company {name: 'Acme Robotics'})-[:EMPLOYS]->(p) RETURN count(p) AS n`, nil); n != 3 {
		t.Errorf("other people affected: %d remain", n)
	}
	pg := w.query(t, `MATCH (p:Page {url: 'http://acme.test/team'}) RETURN p.raw_key AS k, p.redacted AS r, p.status AS s`, nil)[0]
	if pg["k"] != nil || pg["r"] != true || pg["s"] != int64(200) {
		t.Errorf("page after erasure = %v", pg)
	}
	rec, err := store.ReadRecord(ctx, w.blob, ref)
	if err != nil || !rec.Tombstone || len(rec.Body) != 0 || rec.URL != "" {
		t.Errorf("raw record in MinIO not tombstoned: %+v, %v", rec, err)
	}
	if _, err := w.blob.Get(ctx, team["e"].(string)); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("extraction object still in MinIO: %v", err)
	}
	keys, _ := w.blob.List(ctx, store.RawPrefix)
	for _, k := range keys {
		if err := store.ScanBatch(ctx, w.blob, k, func(_ store.Ref, r *model.RawRecord) error {
			if strings.Contains(string(r.Body), "Jane Doe") || strings.Contains(string(r.Body), "jane.doe@") {
				t.Errorf("raw record for %s still contains the erased person", r.URL)
			}
			return nil
		}); err != nil {
			t.Errorf("batch %s unreadable after tombstoning: %v", k, err)
		}
	}
	flags := w.strings(t, `MATCH (s:Suppression) RETURN DISTINCT s.kind AS k`, nil)
	if fmt.Sprint(flags) != "[email person person_name]" {
		t.Errorf("suppression flags in the graph = %v", flags)
	}
	if n := w.count(t, `MATCH (s:Suppression) WHERE s.member CONTAINS '@' RETURN count(s) AS n`, nil); n != 0 {
		t.Error("graph suppression flags contain a plain email address")
	}
	// She does not come back on a recrawl.
	cfg := w.cfg
	cfg.Crawl.Epoch = "2"
	pipelinetest.Crawl(t, w.pipeline(cfg), pipelinetest.Seeds...)
	if n := w.count(t, `MATCH (n) WHERE n.full_name = 'Jane Doe' OR n.address = 'jane.doe@acme.test' RETURN count(n) AS n`, nil); n != 0 {
		t.Errorf("erased person returned after a recrawl (%d nodes)", n)
	}
	if n := w.count(t, `MATCH (:Company {name: 'Acme Robotics'})-[:EMPLOYS]->(p) RETURN count(p) AS n`, nil); n != 3 {
		t.Errorf("recrawl after erasure: %d people", n)
	}

	// Person erase and domain suppression.
	tomID := w.strings(t, `MATCH (p:Person {full_name: "Tom O'Brien"}) RETURN p.id AS id`, nil)[0]
	if res, err := w.p.Erase.Person(ctx, tomID); err != nil || res.People != 1 {
		t.Errorf("erase person: %+v, %v", res, err)
	}
	if n := w.count(t, `MATCH (n) WHERE n.id = $id OR n.address = 'tom.obrien@acme.test' RETURN count(n) AS n`, map[string]any{"id": tomID}); n != 0 {
		t.Errorf("%d nodes of Tom remain", n)
	}
	if _, err := w.p.Erase.SuppressAdd(ctx, suppress.KindDomain, "globex.test"); err != nil {
		t.Fatal(err)
	}
	if n := w.count(t, `MATCH (n) WHERE (n:Company AND n.domain = 'globex.test') OR (n:Domain AND n.registrable = 'globex.test') OR (n:Page AND n.url STARTS WITH 'http://globex.test') OR (n:Person AND n.full_name IN ['Hans Müller', 'Anna Schmidt']) RETURN count(n) AS n`, nil); n != 0 {
		t.Errorf("%d nodes of the suppressed domain remain", n)
	}
	if n := w.count(t, `MATCH (c:Company {name: 'Acme Robotics'}) RETURN count(c) AS n`, nil); n != 1 {
		t.Error("purging one company removed another")
	}
}

func TestS3Blob(t *testing.T) {
	w := newWorld(t, nil)
	ctx := context.Background()
	if err := w.blob.Put(ctx, "a/one", []byte("0123456789"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	w.blob.Put(ctx, "a/two", []byte("two"), "text/plain")
	w.blob.Put(ctx, "b/three", []byte("three"), "text/plain")
	if got, err := w.blob.Get(ctx, "a/one"); err != nil || string(got) != "0123456789" {
		t.Errorf("get = %q, %v", got, err)
	}
	if got, err := w.blob.GetRange(ctx, "a/one", 3, 4); err != nil || string(got) != "3456" {
		t.Errorf("range = %q, %v", got, err)
	}
	if keys, err := w.blob.List(ctx, "a/"); err != nil || fmt.Sprint(keys) != "[a/one a/two]" {
		t.Errorf("list = %v, %v", keys, err)
	}
	if _, err := w.blob.Get(ctx, "missing"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("missing key err = %v", err)
	}
	if err := w.blob.Delete(ctx, "a/one"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.blob.Get(ctx, "a/one"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("deleted key err = %v", err)
	}
	// Opening the same bucket again is fine.
	if _, err := store.NewS3(ctx, w.cfg.S3); err != nil {
		t.Errorf("reopening bucket: %v", err)
	}
}

func benchMutations(n, offset int) []*model.GraphMutation {
	t0 := time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)
	sc := func(v any, c float64) model.Scalar { return model.Scalar{Value: v, Confidence: c, At: t0} }
	muts := make([]*model.GraphMutation, n)
	for i := range muts {
		c := fmt.Sprintf("bench-%07d", offset+i)
		src := "https://" + c + ".test/team"
		nodes := []model.NodeUpsert{{Label: model.LabelCompany, Key: c, Props: map[string]any{"domain": c + ".test"}, SeenAt: t0,
			Scalars: map[string]model.Scalar{"name": sc("Company "+c, 0.9), "description": sc("We make things.", 0.7)}, Facts: []string{"f1-" + c, "f2-" + c}}}
		var edges []model.EdgeUpsert
		for j := 0; j < 5; j++ {
			p, e := fmt.Sprintf("%s-p%d", c, j), fmt.Sprintf("p%d@%s.test", j, c)
			nodes = append(nodes,
				model.NodeUpsert{Label: model.LabelPerson, Key: p, Props: map[string]any{"company_id": c}, SeenAt: t0,
					Scalars: map[string]model.Scalar{"full_name": sc("Person "+p, 0.8), "title": sc("Engineer", 0.8)}},
				model.NodeUpsert{Label: model.LabelEmail, Key: e, Props: map[string]any{"kind": "personal", "status": "unverified"}, SeenAt: t0})
			edges = append(edges,
				model.EdgeUpsert{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: c, ToLabel: model.LabelPerson, ToKey: p, Props: map[string]any{"title": "Engineer"}, Confidence: 0.8, SeenAt: t0, SourceURL: src},
				model.EdgeUpsert{Type: model.RelHasEmail, FromLabel: model.LabelPerson, FromKey: p, ToLabel: model.LabelEmail, ToKey: e, Confidence: 0.8, SeenAt: t0, SourceURL: src})
		}
		muts[i] = &model.GraphMutation{SourceURL: src, Nodes: nodes, Edges: edges}
	}
	return muts
}

// BenchmarkGraphWriter reports rows/sec written to Neo4j: new rows, and
// idempotent re-writes of rows that already exist.
func BenchmarkGraphWriter(b *testing.B) {
	for _, batchRows := range []int{500, 1000, 2000} {
		b.Run(fmt.Sprintf("insert/batch=%d", batchRows), func(b *testing.B) {
			w := newWorld(b, func(c *config.Config) { c.Graph.BatchRows = batchRows })
			const perIter = 200 // mutations per Apply: 4200 rows
			rows := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				muts := benchMutations(perIter, i*perIter)
				for _, m := range muts {
					rows += m.Rows()
				}
				if err := w.neo.Apply(context.Background(), muts); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(rows)/b.Elapsed().Seconds(), "rows/sec")
		})
	}
	b.Run("upsert-existing/batch=1000", func(b *testing.B) {
		w := newWorld(b, func(c *config.Config) { c.Graph.BatchRows = 1000 })
		muts := benchMutations(200, 0)
		if err := w.neo.Apply(context.Background(), muts); err != nil {
			b.Fatal(err)
		}
		rows := 0
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, m := range muts {
				rows += m.Rows()
			}
			if err := w.neo.Apply(context.Background(), muts); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(rows)/b.Elapsed().Seconds(), "rows/sec")
	})
}
