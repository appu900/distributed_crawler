package pipeline_test

import (
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/dlq"
	"github.com/appu/leadgraph/internal/fetch"
	"github.com/appu/leadgraph/internal/graph"
	"github.com/appu/leadgraph/internal/pipeline"
	"github.com/appu/leadgraph/internal/pipeline/pipelinetest"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/store"
	"github.com/appu/leadgraph/internal/suppress"
	"github.com/appu/leadgraph/internal/testsite"
	"github.com/appu/leadgraph/pkg/model"
)

type world struct {
	site *testsite.Site
	cfg  config.Config
	rdb  *redis.Client
	blob *store.Mem
	mem  *graph.Memory
	p    *pipeline.Pipeline
}

func newWorld(t *testing.T, mutate func(*config.Config)) *world {
	t.Helper()
	site, addr := pipelinetest.StartSite(t)
	cfg := pipelinetest.Config(addr)
	if mutate != nil {
		mutate(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("test config invalid: %v", err)
	}
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	w := &world{site: site, cfg: cfg, rdb: rdb, blob: store.NewMem(), mem: graph.NewMemory(cfg.Graph.MaxFacts)}
	w.p = w.pipeline(cfg)
	return w
}

// pipeline builds another pipeline over the same stores (a "second process").
func (w *world) pipeline(cfg config.Config) *pipeline.Pipeline {
	return pipeline.New(pipeline.Deps{Cfg: cfg, RDB: w.rdb, Blob: w.blob, Graph: w.mem, Log: pipelinetest.Logger()})
}

var (
	acmeID   = model.CompanyID(testsite.Acme)
	globexID = model.CompanyID(testsite.Globex)
)

func (w *world) peopleOf(companyID string) map[string]*graph.MemNode {
	out := map[string]*graph.MemNode{}
	for _, e := range w.mem.Edges(model.RelEmploys) {
		if e.FromKey == companyID {
			n := w.mem.Node(model.LabelPerson, e.ToKey)
			name, _ := n.Value("full_name").(string)
			out[name] = n
		}
	}
	return out
}

func (w *world) targets(typ, fromLabel, fromKey string) []string {
	var out []string
	for _, e := range w.mem.Edges(typ) {
		if e.FromLabel == fromLabel && e.FromKey == fromKey {
			out = append(out, e.ToKey)
		}
	}
	sort.Strings(out)
	return out
}

func names[V any](m map[string]V) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, ", ")
}

func contains(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func TestEndToEndCrawl(t *testing.T) {
	w := newWorld(t, nil)
	pipelinetest.Crawl(t, w.p, pipelinetest.Seeds...)

	// --- Company -----------------------------------------------------------
	acme := w.mem.Node(model.LabelCompany, acmeID)
	if acme == nil {
		t.Fatal("no Company node for acme.test")
	}
	if acme.Value("name") != "Acme Robotics" || acme.Value("founded") != "2012" || acme.Value("country") != "US" {
		t.Errorf("acme: name=%v founded=%v country=%v", acme.Value("name"), acme.Value("founded"), acme.Value("country"))
	}
	if acme.Value("size_hint") != "51-200" || acme.Value("industry") == nil {
		t.Errorf("acme: size_hint=%v industry=%v", acme.Value("size_hint"), acme.Value("industry"))
	}
	if len(acme.Facts) < 4 {
		t.Errorf("acme facts = %v", acme.Facts)
	}
	for _, f := range acme.Facts {
		if !strings.Contains(f, `"source_url":"http://acme.test/`) || !strings.Contains(f, `"extractor"`) || !strings.Contains(f, `"confidence"`) {
			t.Errorf("fact lacks provenance: %s", f)
		}
	}
	if g := w.mem.Node(model.LabelCompany, globexID); g == nil || g.Value("name") != "Globex Industries" {
		t.Errorf("globex = %+v", g)
	}

	// --- People ------------------------------------------------------------
	people := w.peopleOf(acmeID)
	if got := names(people); got != "Jane Doe, Priya Raman, Rahul Mehta, Tom O'Brien" {
		t.Errorf("acme people = %s", got)
	}
	jane := people["Jane Doe"]
	if jane == nil {
		t.Fatal("Jane Doe missing")
	}
	if s := jane.Value("seniority"); s != "c_level" && s != "founder" {
		t.Errorf("jane seniority = %v", s)
	}
	if got := w.targets(model.RelHasEmail, model.LabelPerson, jane.Key); len(got) != 1 || got[0] != "jane.doe@acme.test" {
		t.Errorf("jane emails = %v", got)
	}
	if got := w.targets(model.RelHasPhone, model.LabelPerson, jane.Key); len(got) != 1 || got[0] != "+14155552673" {
		t.Errorf("jane phones = %v", got)
	}
	if got := w.targets(model.RelHasSocial, model.LabelPerson, jane.Key); len(got) != 1 || got[0] != "https://www.linkedin.com/in/janedoe" {
		t.Errorf("jane socials = %v", got)
	}
	if got := w.targets(model.RelHasEmail, model.LabelPerson, people["Rahul Mehta"].Key); len(got) != 1 || got[0] != "rahul@acme.test" {
		t.Errorf("rahul emails (Cloudflare-obfuscated) = %v", got)
	}
	if got := w.targets(model.RelHasEmail, model.LabelPerson, people["Tom O'Brien"].Key); len(got) != 1 || got[0] != "tom.obrien@acme.test" {
		t.Errorf("tom emails (matched by name pattern) = %v", got)
	}
	mentions := 0
	for _, e := range w.mem.Edges(model.RelMentions) {
		if e.ToKey == jane.Key {
			mentions++
		}
	}
	if mentions != 2 {
		t.Errorf("Jane is on /about and /team: want 2 MENTIONS edges into one Person node, got %d", mentions)
	}
	if got := names(w.peopleOf(globexID)); got != "Anna Schmidt, Hans Müller" {
		t.Errorf("globex people = %s", got)
	}

	// --- Contacts ----------------------------------------------------------
	companyEmails := w.targets(model.RelHasEmail, model.LabelCompany, acmeID)
	for _, want := range []string{"hello@acme.test", "sales@acme.test", "support@acme.test", "press@acme.test", "legacy@acme.test", "status@acme.test"} {
		if !contains(companyEmails, want) {
			t.Errorf("company emails %v lack %s", companyEmails, want)
		}
	}
	for _, e := range w.mem.Nodes(model.LabelEmail) {
		switch e.Key {
		case "forbidden@acme.test":
			t.Error("email from a robots-disallowed page was stored")
		case "deep@acme.test":
			t.Error("email from beyond max depth was stored")
		case "secret.person@acme.test":
			t.Error("email from a noindex page was stored")
		case "you@example.com":
			t.Error("placeholder email was stored")
		}
		if e.Props["status"] != "unverified" || (e.Props["kind"] != "role" && e.Props["kind"] != "personal") {
			t.Errorf("email %s props = %v", e.Key, e.Props)
		}
	}
	if n := w.mem.Node(model.LabelEmail, "hello@acme.test"); n == nil || n.Props["kind"] != "role" {
		t.Errorf("hello@ = %+v", n)
	}
	if got := w.targets(model.RelHasPhone, model.LabelCompany, acmeID); !contains(got, "+14155552671") {
		t.Errorf("company phones = %v", got)
	}
	if got := w.targets(model.RelHasPhone, model.LabelCompany, globexID); !contains(got, "+49891234567") {
		t.Errorf("globex phones = %v", got)
	}
	socials := w.targets(model.RelHasSocial, model.LabelCompany, acmeID)
	for _, want := range []string{"https://www.linkedin.com/company/acme-robotics", "https://github.com/acme-robotics", "https://x.com/acmerobotics"} {
		if !contains(socials, want) {
			t.Errorf("company socials %v lack %s", socials, want)
		}
	}
	tech := w.targets(model.RelUses, model.LabelCompany, acmeID)
	for _, want := range []string{"WordPress", "Google Analytics", "HubSpot", "Nginx"} {
		if !contains(tech, want) {
			t.Errorf("technologies %v lack %s", tech, want)
		}
	}
	locs := w.targets(model.RelLocatedAt, model.LabelCompany, acmeID)
	if len(locs) != 1 {
		t.Fatalf("the JSON-LD and <address> forms of one address should resolve to one Location, got %d", len(locs))
	}
	if l := w.mem.Node(model.LabelLocation, locs[0]); l.Props["city"] != "San Francisco" || l.Props["country"] != "US" || l.Props["postal"] != "94105" {
		t.Errorf("location = %v", l.Props)
	}

	// --- Links between companies ------------------------------------------
	if w.mem.Edge(model.RelLinksTo, model.LabelCompany, acmeID, model.LabelCompany, globexID) == nil ||
		w.mem.Edge(model.RelLinksTo, model.LabelCompany, globexID, model.LabelCompany, acmeID) == nil {
		t.Errorf("LINKS_TO edges = %+v", w.mem.Edges(model.RelLinksTo))
	}

	// --- Pages -------------------------------------------------------------
	pageType := func(url string) any { return w.mem.Node(model.LabelPage, url).Value("type") }
	for url, typ := range map[string]string{
		"http://acme.test/": "home", "http://acme.test/about": "about", "http://acme.test/team": "team",
		"http://acme.test/contact": "contact", "http://acme.test/products": "other",
	} {
		if n := w.mem.Node(model.LabelPage, url); n == nil || pageType(url) != typ || n.Value("status") != int64(200) || n.Value("raw_key") == nil {
			t.Errorf("page %s = %+v", url, n)
		}
	}
	if n := w.mem.Node(model.LabelPage, "http://acme.test/private/secret"); n == nil || n.Value("skip_reason") != model.SkipRobots {
		t.Errorf("robots-blocked page should be recorded with its reason: %+v", n)
	}
	if n := w.mem.Node(model.LabelPage, "http://acme.test/go-offsite"); n == nil || n.Value("skip_reason") != model.SkipOffDomain {
		t.Errorf("off-site redirect should be recorded: %+v", n)
	}
	if w.mem.Node(model.LabelPage, "http://acme.test/leadership") != nil {
		t.Error("a well-known path that does not exist must not become a Page")
	}
	if w.mem.Node(model.LabelPage, "http://acme.test/old-team") != nil {
		t.Error("a redirected URL is recorded under its final URL only")
	}

	// --- Compliance: what was (not) requested ------------------------------
	for _, path := range []string{"/private/secret", "/members-only", "/careers", "/blog/third-post", "/brochure.pdf", "/hidden/next"} {
		if n := w.site.Hits(testsite.Acme, path); n != 0 {
			t.Errorf("%s was requested %d times; it is disallowed, nofollow, too deep or not HTML", path, n)
		}
	}
	if n := w.site.Hits(testsite.Acme, "/robots.txt"); n != 1 {
		t.Errorf("robots.txt fetched %d times; it must be cached per host", n)
	}
	if n := w.site.Hits(testsite.Acme, "/sitemap.xml"); n != 1 {
		t.Errorf("sitemap fetched %d times", n)
	}
	if n := w.site.Hits(testsite.Acme, "/flaky"); n != 3 {
		t.Errorf("/flaky requested %d times, want 2 x 429 then success", n)
	}
	for ua := range w.site.UserAgents() {
		if ua != "LeadGraphBot/1.0 (+https://example.com/bot)" {
			t.Errorf("unexpected user agent %q", ua)
		}
	}
	for _, host := range []string{testsite.Acme, testsite.Globex} {
		if peak := w.site.PeakConcurrency(host); peak != 1 {
			t.Errorf("%s saw %d concurrent requests; the limit is one per host", host, peak)
		}
	}
	for path, n := range w.site.Paths() {
		if n > 1 && !strings.HasSuffix(path, "/flaky") {
			t.Errorf("%s fetched %d times", path, n)
		}
	}

	// --- Raw store and queues ---------------------------------------------
	ctx := context.Background()
	raw, _ := w.blob.List(ctx, store.RawPrefix)
	extractions, _ := w.blob.List(ctx, store.ExtractPrefix)
	if len(raw) == 0 || len(extractions) < 8 {
		t.Errorf("raw batches=%d extraction objects=%d", len(raw), len(extractions))
	}
	team := w.mem.Node(model.LabelPage, "http://acme.test/team")
	rec, err := store.ReadRecord(ctx, w.blob, store.Ref{Key: team.Value("raw_key").(string), Offset: team.Value("raw_offset").(int64), Length: team.Value("raw_length").(int64)})
	if err != nil || !strings.Contains(string(rec.Body), "Meet the Team") || rec.Headers["Server"][0] != "nginx/1.25.3" {
		t.Errorf("raw record for /team not readable from the Page node's reference: %v", err)
	}
	lag, _ := w.p.Lag(ctx)
	for stream, n := range lag {
		if n != 0 {
			t.Errorf("stream %s still has %d messages", stream, n)
		}
	}
}

func TestPolitenessDelay(t *testing.T) {
	const delay = 60 * time.Millisecond
	w := newWorld(t, func(c *config.Config) { c.Crawl.DefaultDelay = delay; c.Crawl.MaxCrawlDelay = time.Second })
	pipelinetest.Crawl(t, w.p, "http://"+testsite.Globex)
	reqs := w.site.Requests(testsite.Globex)
	if len(reqs) < 4 {
		t.Fatalf("only %d requests", len(reqs))
	}
	for i := 1; i < len(reqs); i++ {
		gap := reqs[i].At.Sub(reqs[i-1].At)
		// Allow a little scheduling slack below the configured delay.
		if gap < delay-10*time.Millisecond {
			t.Errorf("%s -> %s only %v apart (delay %v)", reqs[i-1].Path, reqs[i].Path, gap, delay)
		}
	}
}

func TestRecrawlIsIdempotent(t *testing.T) {
	w := newWorld(t, nil)
	pipelinetest.Crawl(t, w.p, pipelinetest.Seeds...)
	before, _ := w.mem.Stats(context.Background())
	facts := len(w.mem.Node(model.LabelCompany, acmeID).Facts)
	hits := w.site.Total()

	// Seeding again in the same epoch fetches nothing.
	pipelinetest.Crawl(t, w.p, pipelinetest.Seeds...)
	if w.site.Total() != hits {
		t.Errorf("re-seeding in the same epoch made %d new requests", w.site.Total()-hits)
	}
	// A new epoch re-fetches, finds identical content, and changes nothing
	// but timestamps.
	cfg := w.cfg
	cfg.Crawl.Epoch = "2"
	pipelinetest.Crawl(t, w.pipeline(cfg), pipelinetest.Seeds...)
	if w.site.Total() <= hits {
		t.Fatal("a new epoch should re-fetch")
	}
	after, _ := w.mem.Stats(context.Background())
	for label, n := range before.Nodes {
		if after.Nodes[label] != n {
			t.Errorf("%s nodes: %d -> %d after recrawl", label, n, after.Nodes[label])
		}
	}
	for typ, n := range before.Edges {
		if after.Edges[typ] != n {
			t.Errorf("%s edges: %d -> %d after recrawl", typ, n, after.Edges[typ])
		}
	}
	if got := len(w.mem.Node(model.LabelCompany, acmeID).Facts); got != facts {
		t.Errorf("facts grew from %d to %d on unchanged content", facts, got)
	}
	home := w.mem.Node(model.LabelPage, "http://acme.test/")
	if !home.LastSeen.After(home.FirstSeen) {
		t.Errorf("recrawl should advance last_seen: first=%v last=%v", home.FirstSeen, home.LastSeen)
	}
}

// TestChaosParserKilledMidBatch kills the parser while it is processing: one
// page is fully handled but never acknowledged, others are cut off half-way.
// A new parser must finish the job with nothing lost and nothing duplicated.
func TestChaosParserKilledMidBatch(t *testing.T) {
	clean := newWorld(t, nil)
	pipelinetest.Crawl(t, clean.p, pipelinetest.Seeds...)
	want := pipelinetest.SnapshotMemory(clean.mem)
	if len(want) < 100 {
		t.Fatalf("clean run produced only %d graph entries", len(want))
	}

	w := newWorld(t, nil)
	doomedCfg := w.cfg
	doomedCfg.Queue.DrainTimeout = 0
	pipelinetest.KillParserMidBatch(t, w.p, w.pipeline(doomedCfg))

	pipelinetest.Diff(t, want, pipelinetest.SnapshotMemory(w.mem))
	for _, label := range []string{model.LabelCompany, model.LabelPerson} {
		for _, n := range w.mem.Nodes(label) {
			seen := map[string]bool{}
			for _, f := range n.Facts {
				if seen[f] {
					t.Errorf("%s %s has a duplicated fact: %s", label, n.Key, f)
				}
				seen[f] = true
			}
		}
	}
	if n, _ := dlq.Size(context.Background(), w.p.Streams); n != 0 {
		t.Errorf("%d messages were dead-lettered", n)
	}
	for path, n := range w.site.Paths() {
		if n > 1 && !strings.HasSuffix(path, "/flaky") {
			t.Errorf("%s was fetched %d times; a parser crash must not cause refetching", path, n)
		}
	}
}

func TestEraseAndSuppress(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	pipelinetest.Crawl(t, w.p, pipelinetest.Seeds...)

	jane := w.peopleOf(acmeID)["Jane Doe"]
	teamPage := w.mem.Node(model.LabelPage, "http://acme.test/team")
	ref := store.Ref{Key: teamPage.Value("raw_key").(string), Offset: teamPage.Value("raw_offset").(int64), Length: teamPage.Value("raw_length").(int64)}
	extractKey := teamPage.Value("extract_key").(string)
	aboutRef := w.mem.Node(model.LabelPage, "http://acme.test/about").Value("raw_key")

	res, err := w.p.Erase.Email(ctx, "Jane.Doe@acme.test", true)
	if err != nil {
		t.Fatal(err)
	}
	if res.People != 1 || res.Emails < 1 || res.Pages != 2 || res.RawRecords != 2 {
		t.Errorf("erase result = %+v; Jane is on /team and /about", res)
	}
	if w.mem.Node(model.LabelPerson, jane.Key) != nil || w.mem.Node(model.LabelEmail, "jane.doe@acme.test") != nil {
		t.Error("person or email still in the graph")
	}
	if w.mem.Node(model.LabelPhone, "+14155552673") != nil || w.mem.Node(model.LabelSocial, "https://www.linkedin.com/in/janedoe") != nil {
		t.Error("contact nodes only Jane owned must be erased with her")
	}
	if len(w.peopleOf(acmeID)) != 3 {
		t.Errorf("other people must be untouched: %s", names(w.peopleOf(acmeID)))
	}
	// Raw store: the record is tombstoned in place; neighbours stay readable.
	rec, err := store.ReadRecord(ctx, w.blob, ref)
	if err != nil || !rec.Tombstone || len(rec.Body) != 0 {
		t.Errorf("raw record not tombstoned: %+v %v", rec, err)
	}
	if _, err := w.blob.Get(ctx, extractKey); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("extraction object still present: %v", err)
	}
	if aboutRef == nil || w.mem.Node(model.LabelPage, "http://acme.test/about").Value("raw_key") != nil {
		t.Error("page reference to erased raw data must be cleared")
	}
	home := w.mem.Node(model.LabelPage, "http://acme.test/")
	if rec, err := store.ReadRecord(ctx, w.blob, store.Ref{Key: home.Value("raw_key").(string), Offset: home.Value("raw_offset").(int64), Length: home.Value("raw_length").(int64)}); err != nil || rec.Tombstone {
		t.Errorf("unrelated raw record damaged: %v", err)
	}
	// Nothing about her survives anywhere in the raw store.
	rawKeys, _ := w.blob.List(ctx, store.RawPrefix)
	for _, k := range rawKeys {
		if err := store.ScanBatch(ctx, w.blob, k, func(_ store.Ref, r *model.RawRecord) error {
			if body := string(r.Body); strings.Contains(body, "Jane Doe") || strings.Contains(body, "jane.doe@") {
				t.Errorf("raw record for %s still contains the erased person", r.URL)
			}
			return nil
		}); err != nil {
			t.Errorf("raw batch %s unreadable after erasure: %v", k, err)
		}
	}
	// Suppression: in Redis (hashed) and flagged in the graph.
	if ok, _ := w.p.Suppress.Has(ctx, suppress.KindEmail, model.EmailID("jane.doe@acme.test")); !ok {
		t.Error("email not suppressed")
	}
	if ok, _ := w.p.Suppress.Has(ctx, suppress.KindPerson, jane.Key); !ok {
		t.Error("person id not suppressed")
	}
	if ok, _ := w.p.Suppress.Has(ctx, suppress.KindPersonName, suppress.PersonNameMember(acmeID, "Jane Doe")); !ok {
		t.Error("person name not suppressed")
	}
	members, _ := w.p.Suppress.Members(ctx, suppress.KindEmail)
	for _, m := range members {
		if strings.Contains(m, "@") {
			t.Errorf("suppression list stores a plain address: %s", m)
		}
	}
	flagged, _ := w.mem.Suppressed(ctx)
	if len(flagged[suppress.KindEmail]) == 0 || len(flagged[suppress.KindPerson]) == 0 {
		t.Errorf("graph flags = %v", flagged)
	}

	// A full recrawl must not bring her back.
	cfg := w.cfg
	cfg.Crawl.Epoch = "2"
	pipelinetest.Crawl(t, w.pipeline(cfg), pipelinetest.Seeds...)
	if got := names(w.peopleOf(acmeID)); got != "Priya Raman, Rahul Mehta, Tom O'Brien" {
		t.Errorf("after recrawl people = %s", got)
	}
	if w.mem.Node(model.LabelEmail, "jane.doe@acme.test") != nil {
		t.Error("suppressed email returned after recrawl")
	}

	// Erase by person id.
	tom := w.peopleOf(acmeID)["Tom O'Brien"]
	if res, err := w.p.Erase.Person(ctx, tom.Key); err != nil || res.People != 1 || w.mem.Node(model.LabelPerson, tom.Key) != nil {
		t.Errorf("erase person: %+v %v", res, err)
	}
	if w.mem.Node(model.LabelEmail, "tom.obrien@acme.test") != nil {
		t.Error("Tom's personal address should be erased with him")
	}

	// Suppress a whole domain: its company disappears and it is not crawled again.
	if _, err := w.p.Erase.SuppressAdd(ctx, suppress.KindDomain, "globex.test"); err != nil {
		t.Fatal(err)
	}
	if w.mem.Node(model.LabelCompany, globexID) != nil || len(w.peopleOf(globexID)) != 0 {
		t.Error("suppressed domain still in the graph")
	}
	hits := len(w.site.Requests(testsite.Globex))
	cfg.Crawl.Epoch = "3"
	p3 := w.pipeline(cfg)
	if _, err := p3.Seed(ctx, []string{"http://" + testsite.Globex}); err == nil {
		t.Error("seeding a suppressed domain should be refused")
	}
	pipelinetest.Crawl(t, p3, "http://"+testsite.Acme)
	if len(w.site.Requests(testsite.Globex)) != hits || w.mem.Node(model.LabelCompany, globexID) != nil {
		t.Error("suppressed domain was fetched or stored again")
	}
	// Lifting the suppression and syncing from the graph.
	if existed, err := w.p.Erase.SuppressRemove(ctx, suppress.KindDomain, "globex.test"); err != nil || !existed {
		t.Errorf("remove: %v %v", existed, err)
	}
	w.rdb.FlushAll(ctx)
	if n, err := w.p.Erase.Sync(ctx); err != nil || n < 3 {
		t.Errorf("sync restored %d entries: %v", n, err)
	}
	if ok, _ := w.p.Suppress.Has(ctx, suppress.KindEmail, model.EmailID("jane.doe@acme.test")); !ok {
		t.Error("suppression not restored from graph flags")
	}
	if w.p.Suppress.DomainBlocked(ctx, "globex.test") {
		t.Error("removed suppression came back")
	}
}

func TestKillSwitchHaltsFetching(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, nil)
	if err := w.p.Frontier.SetKill(ctx, true, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := w.p.Seed(ctx, pipelinetest.Seeds); err != nil {
		t.Fatal(err)
	}
	r := pipelinetest.Start(t, w.p)
	time.Sleep(400 * time.Millisecond)
	if n := w.site.Total(); n != 0 {
		t.Fatalf("%d requests were made while the kill switch was on", n)
	}
	// Flip it mid-crawl: fetching stops within the switch's cache window.
	w.p.Frontier.SetKill(ctx, false, "")
	deadline := time.Now().Add(10 * time.Second)
	for w.site.Total() < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	w.p.Frontier.SetKill(ctx, true, "test")
	time.Sleep(700 * time.Millisecond) // longer than the fetchers' 500ms cache
	frozen := w.site.Total()
	time.Sleep(400 * time.Millisecond)
	if n := w.site.Total(); n != frozen {
		t.Errorf("%d requests were made after the kill switch was turned on", n-frozen)
	}
	if frozen == 0 {
		t.Fatal("crawl never started")
	}
	// Releasing it lets the crawl finish; nothing was lost while halted.
	w.p.Frontier.SetKill(ctx, false, "")
	pipelinetest.WaitIdle(t, w.p, 60*time.Second)
	r.Stop(t)
	if got := names(w.peopleOf(acmeID)); got != "Jane Doe, Priya Raman, Rahul Mehta, Tom O'Brien" {
		t.Errorf("people after resume = %s", got)
	}
	if n := w.site.Hits(testsite.Acme, "/team"); n != 1 {
		t.Errorf("/team fetched %d times across the halt", n)
	}
}

func TestBackpressureSlowsFetching(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, func(c *config.Config) { c.Fetch.ParseLagThreshold = 2; c.Fetch.Workers = 4 })
	if _, err := w.p.Seed(ctx, pipelinetest.Seeds); err != nil {
		t.Fatal(err)
	}
	// No parser is running, so the parse stream fills up.
	upstream := pipelinetest.Start(t, w.p, w.p.RunFrontier, w.p.RunFetch)
	time.Sleep(time.Second)
	backlog, _ := w.p.Streams.Len(ctx, queue.Parse)
	// Each worker may have passed the check just before the threshold was hit.
	if limit := w.cfg.Fetch.ParseLagThreshold + int64(w.cfg.Fetch.Workers); backlog > limit {
		t.Errorf("parse backlog %d exceeds threshold+workers (%d): fetchers did not slow down", backlog, limit)
	}
	if backlog == 0 {
		t.Fatal("nothing was fetched")
	}
	stalled := w.site.Total()
	time.Sleep(300 * time.Millisecond)
	if w.site.Total() != stalled {
		t.Errorf("fetching continued (%d more requests) while the parse stream was over its threshold", w.site.Total()-stalled)
	}
	downstream := pipelinetest.Start(t, w.p, w.p.RunParse, w.p.RunResolve, w.p.RunWrite)
	pipelinetest.WaitIdle(t, w.p, 60*time.Second)
	downstream.Stop(t)
	upstream.Stop(t)
	if len(w.peopleOf(acmeID)) != 4 {
		t.Errorf("crawl incomplete after backpressure released: %s", names(w.peopleOf(acmeID)))
	}
}

func TestDeadLetterAndReplay(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t, func(c *config.Config) { c.Queue.MaxDeliveries = 2 })
	if _, err := w.p.Seed(ctx, []string{"http://" + testsite.Acme}); err != nil {
		t.Fatal(err)
	}
	// A parser with a bug that fails on one page, every time.
	buggy, _ := w.p.NewParser()
	var failures atomic.Int32
	buggy.Wrap = func(next queue.Handler) queue.Handler {
		return func(ctx context.Context, m queue.Message) error {
			if strings.Contains(string(m.Payload), `"url":"http://acme.test/contact"`) {
				failures.Add(1)
				return errors.New("simulated parser bug")
			}
			return next(ctx, m)
		}
	}
	r := pipelinetest.Start(t, w.p, w.p.RunFrontier, w.p.RunFetch, buggy.Run, w.p.RunResolve, w.p.RunWrite)
	pipelinetest.WaitIdle(t, w.p, 60*time.Second)
	r.Stop(t)

	if failures.Load() != 2 {
		t.Errorf("poison message attempted %d times, want max_deliveries=2", failures.Load())
	}
	entries, err := dlq.List(ctx, w.p.Streams, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("dlq = %+v, %v", entries, err)
	}
	if e := entries[0]; e.Stream != queue.Parse || e.Error != "simulated parser bug" || e.Deliveries != 2 || !strings.Contains(string(e.Payload), "/contact") {
		t.Errorf("dlq entry = %+v", e)
	}
	if w.mem.Node(model.LabelPage, "http://acme.test/contact") != nil {
		t.Error("the failed page should not be in the graph yet")
	}
	if w.mem.Node(model.LabelPage, "http://acme.test/team") == nil {
		t.Error("one poison message must not block the rest")
	}
	if _, err := dlq.Replay(ctx, w.p.Streams, "bogus", 0); err == nil {
		t.Error("unknown stream should be rejected")
	}
	if n, _ := dlq.Replay(ctx, w.p.Streams, queue.Write, 0); n != 0 {
		t.Errorf("stream filter ignored: replayed %d", n)
	}
	// With the bug fixed, replay puts the page through the normal path.
	n, err := dlq.Replay(ctx, w.p.Streams, queue.Parse, 0)
	if err != nil || n != 1 {
		t.Fatalf("replayed %d, %v", n, err)
	}
	r = pipelinetest.Start(t, w.p)
	pipelinetest.WaitIdle(t, w.p, 60*time.Second)
	r.Stop(t)
	if w.mem.Node(model.LabelPage, "http://acme.test/contact") == nil || !contains(w.targets(model.RelHasEmail, model.LabelCompany, acmeID), "sales@acme.test") {
		t.Error("replayed message was not processed")
	}
	if n, _ := dlq.Size(ctx, w.p.Streams); n != 0 {
		t.Errorf("dlq still has %d entries after replay", n)
	}
	w.p.Streams.PublishNow(ctx, queue.DLQ, []byte("x"))
	if n, err := dlq.Purge(ctx, w.p.Streams, ""); err != nil || n != 1 {
		t.Errorf("purge: %d %v", n, err)
	}
}

func TestSeedInputs(t *testing.T) {
	seeds, err := pipeline.ReadSeeds(strings.NewReader("# comment\n\nacme.test  # trailing\n  http://globex.test/  \n"))
	if err != nil || strings.Join(seeds, "|") != "acme.test|http://globex.test/" {
		t.Errorf("seeds = %v, %v", seeds, err)
	}
	ctx := context.Background()
	w := newWorld(t, nil)
	if n, err := w.p.Seed(ctx, []string{"http://" + testsite.Acme, "not a url", ""}); n != 9 || err == nil {
		t.Errorf("seed: n=%d err=%v; good seeds are added and bad ones reported", n, err)
	}
	// The Redis seed queue feeds the frontier while it runs.
	w.rdb.RPush(ctx, w.p.SeedQueueKey(), "http://"+testsite.Globex)
	if idle, _ := w.p.Idle(ctx); idle {
		t.Error("a queued seed is pending work")
	}
	r := pipelinetest.Start(t, w.p)
	pipelinetest.WaitIdle(t, w.p, 60*time.Second)
	r.Stop(t)
	if w.mem.Node(model.LabelCompany, globexID) == nil {
		t.Error("seed pushed to the Redis queue was not crawled")
	}
}

type fakeRenderer struct {
	calls atomic.Int32
	urls  sync.Map
}

func (f *fakeRenderer) Render(_ context.Context, url string) ([]byte, error) {
	f.calls.Add(1)
	f.urls.Store(url, true)
	return []byte(`<html><head><title>Acme App</title></head><body><div id="root"><h1>Acme App</h1>
		<p>Rendered. Contact <a href="mailto:app@acme.test">app@acme.test</a></p></div></body></html>`), nil
}

func (f *fakeRenderer) Close() {}

// The headless fallback is used only for pages whose static HTML is an empty
// JavaScript shell.
func TestHeadlessFallbackOnlyForEmptyShells(t *testing.T) {
	w := newWorld(t, func(c *config.Config) { c.Fetch.Headless.Enabled = true })
	r := &fakeRenderer{}
	w.p.Renderer = r
	pipelinetest.Crawl(t, w.p, "http://"+testsite.Acme)
	if r.calls.Load() != 1 {
		t.Errorf("renderer called %d times; only /app is a JavaScript shell", r.calls.Load())
	}
	if _, ok := r.urls.Load("http://acme.test/app"); !ok {
		t.Error("/app was not rendered")
	}
	if !contains(w.targets(model.RelHasEmail, model.LabelCompany, acmeID), "app@acme.test") {
		t.Error("content that only exists after rendering was not extracted")
	}
	// Without the feature flag the renderer is never consulted.
	w2 := newWorld(t, nil)
	pipelinetest.Crawl(t, w2.p, "http://"+testsite.Acme)
	if contains(w2.targets(model.RelHasEmail, model.LabelCompany, acmeID), "app@acme.test") {
		t.Error("rendered-only content appeared with headless disabled")
	}
}

// TestHeadlessChrome drives a real Chrome. It is opt-in because it needs a
// browser on the machine: LEADGRAPH_CHROME_TEST=1 go test ./internal/pipeline/ -run HeadlessChrome
func TestHeadlessChrome(t *testing.T) {
	if os.Getenv("LEADGRAPH_CHROME_TEST") == "" {
		t.Skip("set LEADGRAPH_CHROME_TEST=1 to run against a real Chrome")
	}
	w := newWorld(t, func(c *config.Config) { c.Fetch.Headless.Enabled = true })
	r := fetch.NewChromeRenderer(w.cfg.Fetch.Headless, w.cfg.Fetch.UserAgent(), w.cfg.Fetch.HostOverrides)
	defer r.Close()
	w.p.Renderer = r
	pipelinetest.Crawl(t, w.p, "http://"+testsite.Acme)
	if !contains(w.targets(model.RelHasEmail, model.LabelCompany, acmeID), "app@acme.test") {
		t.Errorf("email rendered by JavaScript not extracted: %v", w.targets(model.RelHasEmail, model.LabelCompany, acmeID))
	}
	if w.site.Hits(testsite.Acme, "/static/app.js") == 0 {
		t.Error("the browser never loaded the page's script")
	}
	for ua := range w.site.UserAgents() {
		if !strings.HasPrefix(ua, "LeadGraphBot/") {
			t.Errorf("browser sent user agent %q", ua)
		}
	}
}
