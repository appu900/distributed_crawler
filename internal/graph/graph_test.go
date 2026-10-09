package graph

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/suppress"
	"github.com/appu/leadgraph/pkg/model"
)

var t0 = time.Date(2026, 3, 1, 10, 0, 0, 0, time.UTC)

func sc(v any, conf float64, at time.Time) model.Scalar {
	return model.Scalar{Value: v, Confidence: conf, At: at}
}

func companyNode(id string, at time.Time, scalars map[string]model.Scalar, facts ...string) model.NodeUpsert {
	return model.NodeUpsert{Label: model.LabelCompany, Key: id, Props: map[string]any{"domain": id + ".com"}, Scalars: scalars, Facts: facts, SeenAt: at}
}

func mut(nodes []model.NodeUpsert, edges ...model.EdgeUpsert) *model.GraphMutation {
	return &model.GraphMutation{SourceURL: "https://acme.com/", Nodes: nodes, Edges: edges}
}

func apply(t *testing.T, m *Memory, muts ...*model.GraphMutation) {
	t.Helper()
	if err := m.Apply(context.Background(), muts); err != nil {
		t.Fatal(err)
	}
}

func TestNodeCypherShape(t *testing.T) {
	spec, _ := model.LabelByName(model.LabelCompany)
	cy := NodeCypher(spec)
	for _, want := range []string{
		"UNWIND $rows AS row",
		"MERGE (n:Company {id: row.key})",
		"ON CREATE SET n.first_seen = row.first",
		"row.s.name.c > n.name_conf",
		"row.s.name.c = n.name_conf AND row.s.name.t >= n.name_at",
		"n.name = CASE WHEN u_name THEN row.s.name.v ELSE n.name END",
		"n.description_conf = CASE WHEN u_description",
		"[f IN row.facts WHERE NOT f IN coalesce(n.facts, [])]",
		"[-$maxFacts..]",
	} {
		if !strings.Contains(cy, want) {
			t.Errorf("company cypher lacks %q:\n%s", want, cy)
		}
	}
	// The update decision must be taken before any SET touches the node.
	if strings.Index(cy, "AS u_name") > strings.Index(cy, "n.name = CASE") {
		t.Error("decision computed after assignment")
	}
	dom, _ := model.LabelByName(model.LabelDomain)
	if cy := NodeCypher(dom); !strings.Contains(cy, "MERGE (n:Domain {name: row.key})") || strings.Contains(cy, "facts") {
		t.Errorf("domain cypher:\n%s", cy)
	}
}

func TestEdgeCypherCoversSchema(t *testing.T) {
	for _, spec := range model.Edges {
		cy := EdgeCypher(spec)
		from, _ := model.LabelByName(spec.From)
		to, _ := model.LabelByName(spec.To)
		for _, want := range []string{
			fmt.Sprintf("MATCH (a:%s {%s: row.from})", spec.From, from.Key),
			fmt.Sprintf("MATCH (b:%s {%s: row.to})", spec.To, to.Key),
			fmt.Sprintf("MERGE (a)-[r:%s]->(b)", spec.Type),
			"row.confidence >= r.confidence AS win",
			"r.first_seen", "r.last_seen", "r.source_url", "r.confidence",
		} {
			if !strings.Contains(cy, want) {
				t.Errorf("%s %s->%s lacks %q", spec.Type, spec.From, spec.To, want)
			}
		}
	}
	// Every relationship in the brief is present.
	for _, shape := range [][3]string{
		{"HAS_DOMAIN", "Company", "Domain"}, {"EMPLOYS", "Company", "Person"}, {"HAS_EMAIL", "Person", "Email"},
		{"HAS_EMAIL", "Company", "Email"}, {"HAS_PHONE", "Company", "Phone"}, {"HAS_PHONE", "Person", "Phone"},
		{"HAS_SOCIAL", "Company", "SocialProfile"}, {"HAS_SOCIAL", "Person", "SocialProfile"}, {"USES", "Company", "Technology"},
		{"LOCATED_AT", "Company", "Location"}, {"HAS_PAGE", "Domain", "Page"}, {"MENTIONS", "Page", "Person"},
		{"MENTIONS", "Page", "Email"}, {"MENTIONS", "Page", "Phone"}, {"LINKS_TO", "Company", "Company"},
	} {
		if _, ok := model.EdgeByShape(shape[0], shape[1], shape[2]); !ok {
			t.Errorf("schema lacks (%s)-[%s]->(%s)", shape[1], shape[0], shape[2])
		}
	}
}

func TestPlanOrderingAndChunking(t *testing.T) {
	var nodes []model.NodeUpsert
	for i := 9; i >= 0; i-- {
		nodes = append(nodes, model.NodeUpsert{Label: model.LabelEmail, Key: fmt.Sprintf("e%d@acme.com", i), SeenAt: t0})
	}
	nodes = append(nodes,
		model.NodeUpsert{Label: model.LabelPage, Key: "https://acme.com/", SeenAt: t0,
			Scalars: map[string]model.Scalar{"status": sc(float64(200), 1, t0), "fetched_at": sc("2026-03-01T10:00:00Z", 1, t0), "raw_offset": sc(float64(42), 1, t0)}},
		model.NodeUpsert{Label: model.LabelCompany, Key: "c1", SeenAt: t0},
		model.NodeUpsert{Label: "Alien", Key: "x", SeenAt: t0},
		model.NodeUpsert{Label: model.LabelPerson, Key: "", SeenAt: t0},
	)
	edges := []model.EdgeUpsert{
		{Type: model.RelMentions, FromLabel: model.LabelPage, FromKey: "https://acme.com/", ToLabel: model.LabelEmail, ToKey: "e1@acme.com", SeenAt: t0},
		{Type: model.RelHasEmail, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelEmail, ToKey: "e2@acme.com", SeenAt: t0},
		{Type: model.RelHasEmail, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelEmail, ToKey: "e1@acme.com", SeenAt: t0},
		{Type: model.RelEmploys, FromLabel: model.LabelPerson, FromKey: "p", ToLabel: model.LabelCompany, ToKey: "c1", SeenAt: t0}, // wrong direction
	}
	stmts, errs := Plan([]*model.GraphMutation{mut(nodes, edges...)}, 4)
	if len(errs) != 3 {
		t.Errorf("expected 3 rejected rows (unknown label, empty key, bad edge shape), got %v", errs)
	}
	var names []string
	for _, s := range stmts {
		names = append(names, fmt.Sprintf("%s:%s:%d", s.Kind, s.Name, len(s.Rows)))
	}
	want := "node:Company:1 node:Email:4 node:Email:4 node:Email:2 node:Page:1 edge:HAS_EMAIL:2 edge:MENTIONS:1"
	if got := strings.Join(names, " "); got != want {
		t.Errorf("plan = %s\nwant   %s", got, want)
	}
	if stmts[1].Rows[0]["key"] != "e0@acme.com" || stmts[3].Rows[1]["key"] != "e9@acme.com" {
		t.Error("node rows must be sorted by key")
	}
	if stmts[5].Rows[0]["to"] != "e1@acme.com" {
		t.Error("edge rows must be sorted by endpoints")
	}
	// JSON transport turns numbers into float64 and times into strings.
	s := stmts[4].Rows[0]["s"].(map[string]any)
	if v := s["status"].(map[string]any)["v"]; v != int64(200) {
		t.Errorf("status = %T %v, want int64", v, v)
	}
	if v := s["raw_offset"].(map[string]any)["v"]; v != int64(42) {
		t.Errorf("raw_offset = %T %v", v, v)
	}
	if v, ok := s["fetched_at"].(map[string]any)["v"].(time.Time); !ok || !v.Equal(t0) {
		t.Errorf("fetched_at = %v", s["fetched_at"])
	}
	groups := Transactions(stmts, 6)
	var sizes []int
	for _, g := range groups {
		n := 0
		for _, s := range g {
			n += len(s.Rows)
			if len(s.Rows) > 4 {
				t.Error("statement exceeds row cap")
			}
		}
		sizes = append(sizes, n)
		if n > 6 {
			t.Errorf("transaction has %d rows, cap is 6", n)
		}
	}
	if fmt.Sprint(sizes) != "[5 6 4]" {
		t.Errorf("transaction sizes = %v", sizes)
	}
}

func TestMemoryScalarMergeRule(t *testing.T) {
	m := NewMemory(50)
	later := t0.Add(time.Hour)
	apply(t, m, mut([]model.NodeUpsert{companyNode("c1", t0, map[string]model.Scalar{"name": sc("Acme", 0.8, t0), "description": sc("old", 0.7, t0)})}))
	// Lower confidence, newer: must not overwrite. Equal confidence, newer: must.
	apply(t, m, mut([]model.NodeUpsert{companyNode("c1", later, map[string]model.Scalar{"name": sc("acme-from-title", 0.4, later), "description": sc("new", 0.7, later)})}))
	c := m.Node(model.LabelCompany, "c1")
	if c.Value("name") != "Acme" || c.Value("description") != "new" {
		t.Errorf("after low-confidence update: name=%v description=%v", c.Value("name"), c.Value("description"))
	}
	// Higher confidence but older: wins on confidence.
	apply(t, m, mut([]model.NodeUpsert{companyNode("c1", t0.Add(-time.Hour), map[string]model.Scalar{"name": sc("Acme Robotics", 0.95, t0.Add(-time.Hour))})}))
	// Equal confidence but older: loses.
	apply(t, m, mut([]model.NodeUpsert{companyNode("c1", t0, map[string]model.Scalar{"description": sc("stale", 0.7, t0)})}))
	c = m.Node(model.LabelCompany, "c1")
	if c.Value("name") != "Acme Robotics" || c.Scalars["name"].Confidence != 0.95 || c.Value("description") != "new" {
		t.Errorf("final: name=%v (%v) description=%v", c.Value("name"), c.Scalars["name"].Confidence, c.Value("description"))
	}
	if !c.FirstSeen.Equal(t0.Add(-time.Hour)) || !c.LastSeen.Equal(later) {
		t.Errorf("first/last seen = %v / %v", c.FirstSeen, c.LastSeen)
	}
	// A scalar the schema does not know is ignored, not stored.
	apply(t, m, mut([]model.NodeUpsert{companyNode("c1", t0, map[string]model.Scalar{"shoe_size": sc("44", 1, t0)})}))
	if m.Node(model.LabelCompany, "c1").Value("shoe_size") != nil {
		t.Error("unknown scalar stored")
	}
}

func TestMemoryFactsAndIdempotence(t *testing.T) {
	m := NewMemory(3)
	n := companyNode("c1", t0, map[string]model.Scalar{"name": sc("Acme", 0.8, t0)}, "f1", "f2")
	e := model.EdgeUpsert{Type: model.RelHasEmail, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelEmail, ToKey: "a@acme.com",
		SourceURL: "https://acme.com/", Confidence: 0.9, SeenAt: t0}
	email := model.NodeUpsert{Label: model.LabelEmail, Key: "a@acme.com", Props: map[string]any{"kind": "role"}, SeenAt: t0}
	one := mut([]model.NodeUpsert{n, email}, e)
	apply(t, m, one)
	before, _ := json.Marshal(struct {
		N any
		E any
	}{m.Nodes(model.LabelCompany), m.Edges(model.RelHasEmail)})
	for i := 0; i < 3; i++ {
		apply(t, m, one, one)
	}
	after, _ := json.Marshal(struct {
		N any
		E any
	}{m.Nodes(model.LabelCompany), m.Edges(model.RelHasEmail)})
	if string(before) != string(after) {
		t.Errorf("re-applying a mutation changed the graph:\n%s\n%s", before, after)
	}
	st, _ := m.Stats(context.Background())
	if st.Nodes[model.LabelCompany] != 1 || st.Nodes[model.LabelEmail] != 1 || st.Edges[model.RelHasEmail] != 1 {
		t.Errorf("duplicates created: %+v", st)
	}
	// New facts append; the list is capped keeping the newest.
	apply(t, m, mut([]model.NodeUpsert{companyNode("c1", t0, nil, "f2", "f3", "f4")}))
	if got := strings.Join(m.Node(model.LabelCompany, "c1").Facts, ","); got != "f2,f3,f4" {
		t.Errorf("facts = %s", got)
	}
}

func TestMemoryEdges(t *testing.T) {
	m := NewMemory(50)
	edge := func(conf float64, title, src string, at time.Time) model.EdgeUpsert {
		return model.EdgeUpsert{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelPerson, ToKey: "p1",
			Props: map[string]any{"title": title, "ignored": "x"}, SourceURL: src, Confidence: conf, SeenAt: at}
	}
	// No endpoints yet: the edge is not created (MATCH semantics).
	apply(t, m, mut(nil, edge(0.8, "CEO", "https://acme.com/team", t0)))
	if len(m.Edges(model.RelEmploys)) != 0 {
		t.Fatal("edge created without endpoints")
	}
	nodes := []model.NodeUpsert{companyNode("c1", t0, nil), {Label: model.LabelPerson, Key: "p1", SeenAt: t0}}
	apply(t, m, mut(nodes, edge(0.8, "CEO", "https://acme.com/team", t0)))
	apply(t, m, mut(nil, edge(0.5, "Chief", "https://acme.com/blog", t0.Add(time.Hour))))
	e := m.Edge(model.RelEmploys, model.LabelCompany, "c1", model.LabelPerson, "p1")
	if e.Props["title"] != "CEO" || e.Confidence != 0.8 || e.SourceURL != "https://acme.com/team" {
		t.Errorf("lower-confidence edge overwrote: %+v", e)
	}
	if _, leaked := e.Props["ignored"]; leaked {
		t.Error("only schema-declared edge properties are stored")
	}
	if len(e.Sources) != 2 || !e.LastSeen.Equal(t0.Add(time.Hour)) || !e.FirstSeen.Equal(t0) {
		t.Errorf("provenance: sources=%v first=%v last=%v", e.Sources, e.FirstSeen, e.LastSeen)
	}
	apply(t, m, mut(nil, edge(0.9, "CEO & Founder", "https://acme.com/about", t0)))
	if e := m.Edge(model.RelEmploys, model.LabelCompany, "c1", model.LabelPerson, "p1"); e.Props["title"] != "CEO & Founder" || e.Confidence != 0.9 {
		t.Errorf("higher-confidence edge did not win: %+v", e)
	}
	for i := 0; i < 40; i++ {
		apply(t, m, mut(nil, edge(0.1, "x", fmt.Sprintf("https://acme.com/p%d", i), t0)))
	}
	if n := len(m.Edge(model.RelEmploys, model.LabelCompany, "c1", model.LabelPerson, "p1").Sources); n != MaxEdgeSources {
		t.Errorf("sources = %d, want cap %d", n, MaxEdgeSources)
	}
}

// seedPeople builds a company with two people who share one email and one
// page, and each have something of their own.
func seedPeople(t *testing.T, m *Memory) {
	t.Helper()
	node := func(label, key string, props map[string]any, scalars map[string]model.Scalar) model.NodeUpsert {
		return model.NodeUpsert{Label: label, Key: key, Props: props, Scalars: scalars, SeenAt: t0}
	}
	edge := func(typ, fl, fk, tl, tk string) model.EdgeUpsert {
		return model.EdgeUpsert{Type: typ, FromLabel: fl, FromKey: fk, ToLabel: tl, ToKey: tk, SourceURL: "https://acme.com/team", Confidence: 0.8, SeenAt: t0}
	}
	pageScalars := func(key string) map[string]model.Scalar {
		return map[string]model.Scalar{"raw_key": sc(key, 1, t0), "raw_offset": sc(int64(10), 1, t0), "raw_length": sc(int64(100), 1, t0),
			"extract_key": sc("extract/"+key, 1, t0), "content_hash": sc("h", 1, t0), "status": sc(int64(200), 1, t0)}
	}
	apply(t, m, mut([]model.NodeUpsert{
		node(model.LabelCompany, "c1", map[string]any{"domain": "acme.com"}, map[string]model.Scalar{"name": sc("Acme", 0.9, t0)}),
		node(model.LabelDomain, "acme.com", nil, nil),
		node(model.LabelPage, "https://acme.com/team", nil, pageScalars("raw/team")),
		node(model.LabelPage, "https://acme.com/about", nil, pageScalars("raw/about")),
		node(model.LabelPerson, "jane", map[string]any{"company_id": "c1"}, map[string]model.Scalar{"full_name": sc("Jane Doe", 0.8, t0)}),
		node(model.LabelPerson, "tom", map[string]any{"company_id": "c1"}, map[string]model.Scalar{"full_name": sc("Tom O'Brien", 0.8, t0)}),
		node(model.LabelEmail, "jane@acme.com", map[string]any{"kind": "personal"}, nil),
		node(model.LabelEmail, "shared@acme.com", map[string]any{"kind": "personal"}, nil),
		node(model.LabelEmail, "info@acme.com", map[string]any{"kind": "role"}, nil),
		node(model.LabelPhone, "+14155550001", nil, nil),
		node(model.LabelSocial, "https://www.linkedin.com/in/jane", nil, nil),
	},
		edge(model.RelHasDomain, model.LabelCompany, "c1", model.LabelDomain, "acme.com"),
		edge(model.RelHasPage, model.LabelDomain, "acme.com", model.LabelPage, "https://acme.com/team"),
		edge(model.RelHasPage, model.LabelDomain, "acme.com", model.LabelPage, "https://acme.com/about"),
		edge(model.RelEmploys, model.LabelCompany, "c1", model.LabelPerson, "jane"),
		edge(model.RelEmploys, model.LabelCompany, "c1", model.LabelPerson, "tom"),
		edge(model.RelHasEmail, model.LabelPerson, "jane", model.LabelEmail, "jane@acme.com"),
		edge(model.RelHasEmail, model.LabelPerson, "jane", model.LabelEmail, "shared@acme.com"),
		edge(model.RelHasEmail, model.LabelPerson, "tom", model.LabelEmail, "shared@acme.com"),
		edge(model.RelHasEmail, model.LabelCompany, "c1", model.LabelEmail, "info@acme.com"),
		edge(model.RelHasPhone, model.LabelPerson, "jane", model.LabelPhone, "+14155550001"),
		edge(model.RelHasSocial, model.LabelPerson, "jane", model.LabelSocial, "https://www.linkedin.com/in/jane"),
		edge(model.RelMentions, model.LabelPage, "https://acme.com/team", model.LabelPerson, "jane"),
		edge(model.RelMentions, model.LabelPage, "https://acme.com/team", model.LabelPerson, "tom"),
		edge(model.RelMentions, model.LabelPage, "https://acme.com/about", model.LabelEmail, "jane@acme.com"),
		edge(model.RelMentions, model.LabelPage, "https://acme.com/team", model.LabelEmail, "info@acme.com"),
	))
}

func TestMemoryErasePerson(t *testing.T) {
	m := NewMemory(50)
	seedPeople(t, m)
	rep, err := m.ErasePerson(context.Background(), "jane")
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.People) != 1 || rep.People[0].Name != "Jane Doe" || rep.People[0].CompanyID != "c1" {
		t.Errorf("report people = %+v", rep.People)
	}
	if strings.Join(rep.Emails, ",") != "jane@acme.com" {
		t.Errorf("only the address nobody else owns is erased with the person: %v", rep.Emails)
	}
	urls := map[string]PageRef{}
	for _, p := range rep.Pages {
		urls[p.URL] = p
	}
	if len(urls) != 2 || urls["https://acme.com/team"].RawKey != "raw/team" || urls["https://acme.com/team"].RawLength != 100 ||
		urls["https://acme.com/about"].ExtractKey != "extract/raw/about" {
		t.Errorf("pages mentioning the person or her address must be reported with their raw refs: %+v", rep.Pages)
	}
	for _, gone := range [][2]string{{model.LabelPerson, "jane"}, {model.LabelEmail, "jane@acme.com"}, {model.LabelPhone, "+14155550001"},
		{model.LabelSocial, "https://www.linkedin.com/in/jane"}} {
		if m.Node(gone[0], gone[1]) != nil {
			t.Errorf("%s %s still present", gone[0], gone[1])
		}
	}
	for _, kept := range [][2]string{{model.LabelPerson, "tom"}, {model.LabelEmail, "shared@acme.com"}, {model.LabelEmail, "info@acme.com"}, {model.LabelCompany, "c1"}} {
		if m.Node(kept[0], kept[1]) == nil {
			t.Errorf("%s %s was removed but belongs to someone else", kept[0], kept[1])
		}
	}
	for _, e := range append(m.Edges(model.RelHasEmail), m.Edges(model.RelMentions)...) {
		if e.FromKey == "jane" || e.ToKey == "jane" || e.ToKey == "jane@acme.com" {
			t.Errorf("dangling edge %+v", e)
		}
	}
	pg := m.Node(model.LabelPage, "https://acme.com/team")
	if pg.Value("raw_key") != nil || pg.Value("content_hash") != nil || pg.Props["redacted"] != true || pg.Value("status") != int64(200) {
		t.Errorf("page not redacted: %+v %+v", pg.Scalars, pg.Props)
	}
	if rep, _ := m.ErasePerson(context.Background(), "nobody"); len(rep.People) != 0 || rep.Nodes != 0 {
		t.Errorf("erasing an unknown person should be a no-op: %+v", rep)
	}
}

func TestMemoryEraseEmailAndPurge(t *testing.T) {
	ctx := context.Background()
	m := NewMemory(50)
	seedPeople(t, m)
	rep, _ := m.EraseEmail(ctx, "info@acme.com", true)
	if len(rep.People) != 0 || m.Node(model.LabelEmail, "info@acme.com") != nil || len(rep.Pages) != 1 {
		t.Errorf("role email erase: %+v", rep)
	}
	rep, _ = m.EraseEmail(ctx, "jane@acme.com", false)
	if len(rep.People) != 0 || m.Node(model.LabelPerson, "jane") == nil {
		t.Errorf("no-cascade must keep the person: %+v", rep)
	}
	rep, _ = m.EraseEmail(ctx, "shared@acme.com", true)
	if len(rep.People) != 2 || m.Node(model.LabelPerson, "jane") != nil || m.Node(model.LabelPerson, "tom") != nil {
		t.Errorf("cascade must erase every owner of a personal address: %+v", rep.People)
	}

	m = NewMemory(50)
	seedPeople(t, m)
	apply(t, m, mut([]model.NodeUpsert{{Label: model.LabelCompany, Key: "c2", SeenAt: t0}, {Label: model.LabelEmail, Key: "info@acme.com", SeenAt: t0}},
		model.EdgeUpsert{Type: model.RelHasEmail, FromLabel: model.LabelCompany, FromKey: "c2", ToLabel: model.LabelEmail, ToKey: "info@acme.com", Confidence: 0.5, SeenAt: t0},
		model.EdgeUpsert{Type: model.RelLinksTo, FromLabel: model.LabelCompany, FromKey: "c2", ToLabel: model.LabelCompany, ToKey: "c1", Confidence: 0.9, SeenAt: t0}))
	rep, _ = m.PurgeCompany(ctx, "c1")
	st, _ := m.Stats(ctx)
	if st.Nodes[model.LabelPerson] != 0 || st.Nodes[model.LabelPage] != 0 || st.Nodes[model.LabelDomain] != 0 || st.Nodes[model.LabelCompany] != 1 {
		t.Errorf("after purge: %+v", st.Nodes)
	}
	if m.Node(model.LabelEmail, "info@acme.com") == nil {
		t.Error("an email another company also has must survive the purge")
	}
	if len(rep.People) != 2 || len(rep.Pages) < 2 || st.Edges[model.RelLinksTo] != 0 {
		t.Errorf("purge report: people=%d pages=%d edges=%+v", len(rep.People), len(rep.Pages), st.Edges)
	}
	if rep, _ := m.PurgeCompany(ctx, "nope"); rep.Nodes != 0 {
		t.Error("purging an unknown company should be a no-op")
	}
}

func newSuppress(t *testing.T) (*suppress.List, *queue.Streams) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return suppress.New(rdb, "lg"), queue.NewStreams(rdb, "lg", 0)
}

func TestFilterSuppressed(t *testing.T) {
	ctx := context.Background()
	sup, _ := newSuppress(t)
	m := NewMemory(50)
	seedPeople(t, m)
	full := mut([]model.NodeUpsert{
		{Label: model.LabelCompany, Key: "c1", Props: map[string]any{"domain": "acme.com"}},
		{Label: model.LabelPerson, Key: "jane", Props: map[string]any{"company_id": "c1"}, Scalars: map[string]model.Scalar{"full_name": sc("Jane Doe", 0.8, t0)}},
		{Label: model.LabelPerson, Key: "tom-new-title", Props: map[string]any{"company_id": "c1"}, Scalars: map[string]model.Scalar{"full_name": sc("TOM O'BRIEN", 0.8, t0)}},
		{Label: model.LabelEmail, Key: "jane@acme.com"}, {Label: model.LabelEmail, Key: "info@acme.com"},
	},
		model.EdgeUpsert{Type: model.RelHasEmail, FromLabel: model.LabelPerson, FromKey: "jane", ToLabel: model.LabelEmail, ToKey: "jane@acme.com"},
		model.EdgeUpsert{Type: model.RelHasEmail, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelEmail, ToKey: "info@acme.com"},
		model.EdgeUpsert{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelPerson, ToKey: "tom-new-title"},
		model.EdgeUpsert{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelPerson, ToKey: "jane"},
	)
	out, dropped, err := Filter(ctx, sup, full)
	if err != nil || dropped != 0 || out != full {
		t.Fatalf("nothing suppressed: dropped=%d err=%v", dropped, err)
	}
	sup.Add(ctx, suppress.KindEmail, "JANE@acme.com")
	sup.AddMember(ctx, suppress.KindPersonName, suppress.PersonNameMember("c1", "Tom O'Brien"))
	out, dropped, err = Filter(ctx, sup, full)
	if err != nil || dropped != 2 {
		t.Fatalf("dropped=%d err=%v", dropped, err)
	}
	var keys []string
	for _, n := range out.Nodes {
		keys = append(keys, n.Key)
	}
	if strings.Join(keys, ",") != "c1,jane,info@acme.com" {
		t.Errorf("nodes after filter = %v", keys)
	}
	if len(out.Edges) != 2 {
		t.Errorf("edges touching suppressed nodes must go: %+v", out.Edges)
	}
	sup.Add(ctx, suppress.KindPerson, "jane")
	if out, _, _ = Filter(ctx, sup, full); len(out.Nodes) != 2 {
		t.Errorf("person suppressed by id still present: %+v", out.Nodes)
	}
	sup.Add(ctx, suppress.KindDomain, "acme.com")
	if out, _, _ = Filter(ctx, sup, full); len(out.Nodes) != 0 || len(out.Edges) != 0 {
		t.Errorf("suppressed domain must drop the whole mutation: %+v", out)
	}
	if out, dropped, _ := Filter(ctx, nil, full); out != full || dropped != 0 {
		t.Error("nil list filters nothing")
	}
}

func TestWriterBatchIsolation(t *testing.T) {
	ctx := context.Background()
	sup, streams := newSuppress(t)
	mem := NewMemory(50)
	w := NewWriter(config.Default(), streams, mem, sup, nil)
	var purged []string
	w.OnPurge = func(_ context.Context, rep EraseReport) { purged = append(purged, fmt.Sprint(rep.Nodes)) }

	msg := func(m *model.GraphMutation) queue.Message {
		b, _ := json.Marshal(m)
		return queue.Message{Payload: b}
	}
	good := func(id string) *model.GraphMutation {
		return mut([]model.NodeUpsert{{Label: model.LabelCompany, Key: id, Props: map[string]any{"domain": id + ".com"}, SeenAt: t0,
			Scalars: map[string]model.Scalar{"name": sc(id, 0.9, t0)}}})
	}
	// One undecodable message must not stop the rest of its batch.
	errs := w.HandleBatch(ctx, []queue.Message{msg(good("a")), {Payload: []byte("{garbage")}, msg(good("b"))})
	if errs[0] != nil || errs[2] != nil || !queue.IsPermanent(errs[1]) {
		t.Fatalf("errs = %v", errs)
	}
	if mem.Node(model.LabelCompany, "a") == nil || mem.Node(model.LabelCompany, "b") == nil {
		t.Fatal("good mutations not written")
	}
	// When the combined commit fails, each mutation is retried alone.
	mem.FailNext, mem.FailErr = 1, errors.New("deadlock detected")
	errs = w.HandleBatch(ctx, []queue.Message{msg(good("c")), msg(good("d"))})
	if errs[0] != nil || errs[1] != nil || mem.Node(model.LabelCompany, "c") == nil || mem.Node(model.LabelCompany, "d") == nil {
		t.Errorf("fallback to single-mutation commits failed: %v", errs)
	}
	// A persistent failure is reported per message so nothing is acked.
	mem.FailNext, mem.FailErr = 3, errors.New("neo4j down")
	errs = w.HandleBatch(ctx, []queue.Message{msg(good("e")), msg(good("f"))})
	if errs[0] == nil || errs[1] == nil || mem.Node(model.LabelCompany, "e") != nil {
		t.Errorf("failed commit must not be acknowledged: %v", errs)
	}
	// Purge requests run after the upserts of the same batch.
	purge := good("a")
	purge.PurgeCompanies = []string{"a"}
	if errs := w.HandleBatch(ctx, []queue.Message{msg(purge)}); errs[0] != nil || mem.Node(model.LabelCompany, "a") != nil || len(purged) != 1 {
		t.Errorf("purge: errs=%v purged=%v", errs, purged)
	}
	// The writer applies the suppression list itself.
	sup.Add(ctx, suppress.KindDomain, "z.com")
	if errs := w.HandleBatch(ctx, []queue.Message{msg(good("z"))}); errs[0] != nil || mem.Node(model.LabelCompany, "z") != nil {
		t.Error("suppressed domain was written")
	}
}

func TestCoerce(t *testing.T) {
	if coerce(float64(7), model.FieldInt) != int64(7) || coerce(7, model.FieldInt) != int64(7) || coerce("x", model.FieldInt) != "x" {
		t.Error("int coercion")
	}
	if v, ok := coerce("2026-03-01T10:00:00.5Z", model.FieldTime).(time.Time); !ok || v.Nanosecond() != 500000000 {
		t.Error("time coercion")
	}
	if coerce("not a time", model.FieldTime) != "not a time" || coerce("s", model.FieldString) != "s" {
		t.Error("passthrough")
	}
}

func benchMutations(n int) []*model.GraphMutation {
	muts := make([]*model.GraphMutation, n)
	for i := range muts {
		c := fmt.Sprintf("c%06d", i)
		var nodes []model.NodeUpsert
		var edges []model.EdgeUpsert
		nodes = append(nodes, model.NodeUpsert{Label: model.LabelCompany, Key: c, Props: map[string]any{"domain": c + ".com"}, SeenAt: t0,
			Scalars: map[string]model.Scalar{"name": sc("Company "+c, 0.9, t0), "description": sc("We make things.", 0.7, t0)}, Facts: []string{"f1", "f2"}})
		for j := 0; j < 5; j++ {
			p := fmt.Sprintf("%s-p%d", c, j)
			e := fmt.Sprintf("p%d@%s.com", j, c)
			nodes = append(nodes,
				model.NodeUpsert{Label: model.LabelPerson, Key: p, Props: map[string]any{"company_id": c}, SeenAt: t0,
					Scalars: map[string]model.Scalar{"full_name": sc("Person "+p, 0.8, t0), "title": sc("Engineer", 0.8, t0)}},
				model.NodeUpsert{Label: model.LabelEmail, Key: e, Props: map[string]any{"kind": "personal"}, SeenAt: t0})
			edges = append(edges,
				model.EdgeUpsert{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: c, ToLabel: model.LabelPerson, ToKey: p, Confidence: 0.8, SeenAt: t0, SourceURL: "https://" + c + ".com/team"},
				model.EdgeUpsert{Type: model.RelHasEmail, FromLabel: model.LabelPerson, FromKey: p, ToLabel: model.LabelEmail, ToKey: e, Confidence: 0.8, SeenAt: t0, SourceURL: "https://" + c + ".com/team"})
		}
		muts[i] = &model.GraphMutation{SourceURL: "https://" + c + ".com/team", Nodes: nodes, Edges: edges}
	}
	return muts
}

// BenchmarkPlan measures turning mutations into Cypher statements; the
// end-to-end Neo4j write benchmark lives in internal/integration.
func BenchmarkPlan(b *testing.B) {
	muts := benchMutations(100)
	rows := 0
	for _, m := range muts {
		rows += m.Rows()
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		stmts, _ := Plan(muts, 1000)
		Transactions(stmts, 1000)
	}
	b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/sec")
}

func BenchmarkMemoryApply(b *testing.B) {
	muts := benchMutations(100)
	rows := 0
	for _, m := range muts {
		rows += m.Rows()
	}
	mem := NewMemory(50)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := mem.Apply(context.Background(), muts); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(rows*b.N)/b.Elapsed().Seconds(), "rows/sec")
}

// Several upserts of one node or relationship in a batch must collapse to a
// single row that already reflects the merge rule; see mergeNodes.
func TestPlanMergesDuplicateKeys(t *testing.T) {
	later := t0.Add(time.Hour)
	muts := []*model.GraphMutation{
		mut([]model.NodeUpsert{companyNode("c1", later, map[string]model.Scalar{"name": sc("from-title", 0.4, later), "description": sc("newer", 0.7, later)}, "f2")}),
		mut([]model.NodeUpsert{companyNode("c1", t0, map[string]model.Scalar{"name": sc("Acme Robotics", 0.95, t0), "description": sc("older", 0.7, t0)}, "f1", "f2")}),
		mut([]model.NodeUpsert{companyNode("c2", t0, nil), {Label: model.LabelPerson, Key: "p1", SeenAt: t0}},
			model.EdgeUpsert{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelPerson, ToKey: "p1",
				Props: map[string]any{"title": "CEO"}, SourceURL: "https://acme.com/team", Confidence: 0.8, SeenAt: later},
			model.EdgeUpsert{Type: model.RelEmploys, FromLabel: model.LabelCompany, FromKey: "c1", ToLabel: model.LabelPerson, ToKey: "p1",
				Props: map[string]any{"title": "Chief"}, SourceURL: "https://acme.com/blog", Confidence: 0.5, SeenAt: t0}),
	}
	stmts, errs := Plan(muts, 1000)
	if len(errs) != 0 {
		t.Fatal(errs)
	}
	companies := stmts[0].Rows
	if len(companies) != 2 || companies[0]["key"] != "c1" {
		t.Fatalf("company rows = %v", companies)
	}
	row := companies[0]
	s := row["s"].(map[string]any)
	if s["name"].(map[string]any)["v"] != "Acme Robotics" || s["description"].(map[string]any)["v"] != "newer" {
		t.Errorf("merged scalars = %v", s)
	}
	if fmt.Sprint(row["facts"]) != "[f2 f1]" || !row["first"].(time.Time).Equal(t0) || !row["last"].(time.Time).Equal(later) {
		t.Errorf("merged row = %v", row)
	}
	edges := stmts[len(stmts)-1].Rows
	if len(edges) != 1 || edges[0]["confidence"] != 0.8 || edges[0]["props"].(map[string]any)["title"] != "CEO" ||
		fmt.Sprint(edges[0]["sources"]) != "[https://acme.com/team https://acme.com/blog]" {
		t.Errorf("merged edge = %v", edges)
	}
	// The in-memory sink, fed the same batch, agrees.
	m := NewMemory(50)
	apply(t, m, muts...)
	c := m.Node(model.LabelCompany, "c1")
	if c.Value("name") != "Acme Robotics" || c.Value("description") != "newer" {
		t.Errorf("memory sink disagrees: %v %v", c.Value("name"), c.Value("description"))
	}
}
