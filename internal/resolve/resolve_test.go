package resolve

import (
	"context"
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

type env struct {
	*Resolver
	rdb *redis.Client
	sup *suppress.List
}

func newEnv(t *testing.T, mutate func(*config.Config)) *env {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	cfg := config.Default()
	if mutate != nil {
		mutate(&cfg)
	}
	sup := suppress.New(rdb, cfg.Redis.KeyPrefix)
	return &env{Resolver: New(cfg, rdb, queue.NewStreams(rdb, cfg.Redis.KeyPrefix, 0), sup, nil), rdb: rdb, sup: sup}
}

func fact(v, extractor string, conf float64) model.Fact {
	return model.Fact{Value: v, SourceURL: "https://acme.com/", Extractor: extractor, Confidence: conf, ExtractedAt: t0}
}

func page(url string, typ model.PageType) *model.Extraction {
	host := strings.Split(strings.TrimPrefix(strings.TrimPrefix(url, "https://"), "http://"), "/")[0]
	domain := strings.TrimPrefix(host, "www.")
	return &model.Extraction{URL: url, Host: host, Domain: domain, PageType: typ, Status: 200, FetchedAt: t0,
		ContentHash: "h1", RawKey: "raw/x.jsonl.gz", RawOffset: 10, RawLength: 99, ExtractKey: "extract/x.json.gz"}
}

func person(name, title string, conf float64, linkedin string, emails ...string) model.PersonFact {
	pf := model.PersonFact{Fact: fact(name, "people.card", conf), Title: title, Emails: emails}
	if linkedin != "" {
		pf.Socials = []model.SocialFact{{Fact: fact(linkedin, "people.card", 0.85), Network: "linkedin", Handle: "x", Scope: "person"}}
	}
	return pf
}

func email(addr, kind string) model.EmailFact {
	return model.EmailFact{Fact: fact(addr, "mailto", 0.95), Kind: kind, Domain: addr[strings.IndexByte(addr, '@')+1:]}
}

type index struct {
	nodes map[string]model.NodeUpsert
	edges map[string]model.EdgeUpsert
}

func indexOf(m *model.GraphMutation) index {
	ix := index{nodes: map[string]model.NodeUpsert{}, edges: map[string]model.EdgeUpsert{}}
	for _, n := range m.Nodes {
		ix.nodes[n.Label+"/"+n.Key] = n
	}
	for _, e := range m.Edges {
		ix.edges[e.Type+" "+e.FromLabel+"/"+e.FromKey+" -> "+e.ToLabel+"/"+e.ToKey] = e
	}
	return ix
}

func (ix index) labelKeys(label string) []string {
	var out []string
	for _, n := range ix.nodes {
		if n.Label == label {
			out = append(out, n.Key)
		}
	}
	return out
}

func (ix index) hasEdge(typ, from, to string) bool {
	_, ok := ix.edges[typ+" "+from+" -> "+to]
	return ok
}

func mustResolve(t *testing.T, e *env, ex *model.Extraction) *model.GraphMutation {
	t.Helper()
	m, err := e.Resolve(context.Background(), ex)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDeterministicIDs(t *testing.T) {
	if model.CompanyID("Acme.com") != model.CompanyID("acme.com") || model.CompanyID("acme.com") == model.CompanyID("acme.org") {
		t.Error("company id")
	}
	if model.EmailID(" Jane@Acme.com ") != model.EmailID("jane@acme.com") {
		t.Error("email id must ignore case and space")
	}
	c := model.CompanyID("acme.com")
	a := model.PersonID(c, "jane doe", "ceo")
	if a != model.PersonID(c, "jane doe", "ceo") || a == model.PersonID(c, "jane doe", "cto") ||
		a == model.PersonID(model.CompanyID("globex.com"), "jane doe", "ceo") || len(a) != 32 {
		t.Error("person id")
	}
	if model.LocationID("US", "CA", "San Francisco", "94105", "500 howard st") != model.LocationID("us", "ca", "san francisco", "94105", "500 howard st") {
		t.Error("location id must ignore case")
	}
	if model.LocationID("GB", "", "London", "E14 5AB", "x") != model.LocationID("GB", "", "London", "E145AB", "x") {
		t.Error("location id must ignore postal spacing")
	}
}

func TestMergeRule(t *testing.T) {
	older, newer := t0, t0.Add(time.Hour)
	s := func(v string, c float64, at time.Time) model.Scalar {
		return model.Scalar{Value: v, Confidence: c, At: at}
	}
	tests := []struct {
		name      string
		candidate model.Scalar
		current   model.Scalar
		want      bool
	}{
		{"higher confidence wins even if older", s("a", 0.9, older), s("b", 0.5, newer), true},
		{"lower confidence never overwrites", s("a", 0.5, newer), s("b", 0.9, older), false},
		{"equal confidence: newer wins", s("a", 0.7, newer), s("b", 0.7, older), true},
		{"equal confidence: older loses", s("a", 0.7, older), s("b", 0.7, newer), false},
		{"same fact again is accepted (idempotent)", s("a", 0.7, older), s("a", 0.7, older), true},
		{"anything beats nothing", s("a", 0.1, older), model.Scalar{}, true},
		{"nothing never replaces a value", model.Scalar{}, s("b", 0.1, older), false},
	}
	for _, tt := range tests {
		if got := Better(tt.candidate, tt.current); got != tt.want {
			t.Errorf("%s: got %v", tt.name, got)
		}
	}
	facts := []model.Fact{
		{Value: " low ", Confidence: 0.4, ExtractedAt: newer},
		{Value: "first-high", Confidence: 0.9, ExtractedAt: older},
		{Value: "second-high", Confidence: 0.9, ExtractedAt: older},
		{Value: "newest-high", Confidence: 0.9, ExtractedAt: newer},
		{Value: "   ", Confidence: 1, ExtractedAt: newer},
	}
	best, ok := Best(facts, strings.TrimSpace)
	if !ok || best.Value != "newest-high" {
		t.Errorf("best = %+v", best)
	}
	if best, _ := Best(facts[:3], nil); best.Value != "first-high" {
		t.Errorf("ties must keep the earlier fact, got %q", best.Value)
	}
	if _, ok := Best(nil, nil); ok {
		t.Error("empty list has no best")
	}
	f := fact("Acme", "jsonld.organization", 0.95)
	if FactString("name", f) != FactString("name", f) || !strings.Contains(FactString("name", f), `"extractor":"jsonld.organization"`) ||
		FactString("name", f) == FactString("description", f) {
		t.Error("fact string must be deterministic and field-specific")
	}
}

func TestPolicy(t *testing.T) {
	tests := []struct {
		name    string
		c       config.Countries
		allowed map[string]bool
	}{
		{"no lists", config.Countries{Unknown: "allow"}, map[string]bool{"US": true, "RU": true, "": true}},
		{"deny list", config.Countries{Deny: []string{"RU", "KP"}, Unknown: "allow"}, map[string]bool{"US": true, "RU": false, "ru": false, "": true}},
		{"allow list", config.Countries{Allow: []string{"US", "DE"}, Unknown: "allow"}, map[string]bool{"US": true, "FR": false, "": true}},
		{"allow list, unknown denied", config.Countries{Allow: []string{"US"}, Unknown: "deny"}, map[string]bool{"US": true, "": false}},
		{"deny beats allow", config.Countries{Allow: []string{"US", "RU"}, Deny: []string{"RU"}, Unknown: "allow"}, map[string]bool{"US": true, "RU": false}},
		{"deny list alone never blocks unknown", config.Countries{Deny: []string{"RU"}, Unknown: "deny"}, map[string]bool{"": true}},
	}
	for _, tt := range tests {
		p := NewPolicy(tt.c)
		for iso, want := range tt.allowed {
			if got := p.Allowed(iso); got != want {
				t.Errorf("%s: Allowed(%q) = %v, want %v", tt.name, iso, got, want)
			}
		}
	}
}

func TestCountryOf(t *testing.T) {
	addr := func(c string) model.AddressFact {
		return model.AddressFact{Fact: fact("x", "jsonld.address", 0.9), Country: c}
	}
	phone := func(n string) model.PhoneFact { return model.PhoneFact{Fact: fact(n, "tel", 0.9)} }
	tests := []struct {
		name     string
		ex       model.Extraction
		iso      string
		strength int
	}{
		{"address beats tld", model.Extraction{Host: "acme.de", Addresses: []model.AddressFact{addr("United States")}}, "US", evidenceAddress},
		{"tld", model.Extraction{Host: "www.acme.co.uk"}, "GB", evidenceTLD},
		{"generic tld, unanimous phones", model.Extraction{Host: "acme.com", Phones: []model.PhoneFact{phone("+498912345670"), phone("+4930123456")}}, "DE", evidencePhone},
		{"mixed phones say nothing", model.Extraction{Host: "acme.io", Phones: []model.PhoneFact{phone("+498912345670"), phone("+442079460958")}}, "", evidenceNone},
		{"nothing", model.Extraction{Host: "acme.com"}, "", evidenceNone},
	}
	for _, tt := range tests {
		iso, strength := CountryOf(&tt.ex)
		if iso != tt.iso || strength != tt.strength {
			t.Errorf("%s: got %q/%d, want %q/%d", tt.name, iso, strength, tt.iso, tt.strength)
		}
	}
}

func TestMatchEmail(t *testing.T) {
	people := map[string]string{"p1": "jane doe", "p2": "tom o brien", "p3": "jane smith", "p4": "soren kierkegard", "p5": "maria von trapp"}
	tests := []struct {
		local, want string
		conf        float64
	}{
		{"jane.doe", "p1", 0.7}, {"jdoe", "p1", 0.7}, {"doe.jane", "p1", 0.7}, {"JANE_DOE", "p1", 0.7},
		{"jane.doe+news", "p1", 0.7},
		{"tom.obrien", "p2", 0.7}, {"tobrien", "p2", 0.7}, {"tom.brien", "p2", 0.7},
		{"jane", "", 0},           // two Janes: ambiguous
		{"tom", "p2", 0.5},        // unique first name: weak
		{"kierkegard", "p4", 0.5}, // unique surname: weak
		{"maria.vontrapp", "p5", 0.7},
		{"info", "", 0}, {"bob.builder", "", 0}, {"", "", 0},
	}
	for _, tt := range tests {
		id, conf := MatchEmail(tt.local, people)
		if id != tt.want || conf != tt.conf {
			t.Errorf("MatchEmail(%q) = %q/%v, want %q/%v", tt.local, id, conf, tt.want, tt.conf)
		}
	}
}

func TestResolveBuildsCompanyGraph(t *testing.T) {
	e := newEnv(t, nil)
	ex := page("https://www.acme.com/", model.PageHome)
	ex.Title = "Acme"
	ex.CompanyNames = []model.Fact{fact("Acme", "title", 0.4), fact("Acme Robotics, Inc.", "jsonld.organization", 0.95), fact("Acme Robotics", "meta.og:site_name", 0.8)}
	ex.Descriptions = []model.Fact{fact("  Robots   for factories ", "jsonld.organization", 0.9)}
	ex.Founded = []model.Fact{fact("2012", "jsonld.organization", 0.9)}
	ex.Emails = []model.EmailFact{email("hello@acme.com", model.EmailRole), email("jane.doe@acme.com", model.EmailPersonal), email("tom.obrien@acme.com", model.EmailPersonal), email("someone@gmail.com", model.EmailPersonal)}
	ex.Phones = []model.PhoneFact{{Fact: fact("+14155552671", "tel", 0.9), Kind: "landline"}, {Fact: fact("+14155552673", "tel", 0.9), Kind: "mobile"}}
	ex.Socials = []model.SocialFact{{Fact: fact("https://www.linkedin.com/company/acme", "link", 0.85), Network: "linkedin", Handle: "acme", Scope: "company"}}
	ex.Technologies = []model.TechFact{{Fact: fact("WordPress", "tech.meta", 0.9), Category: "cms"}}
	ex.Addresses = []model.AddressFact{{Fact: fact("500 Howard St, San Francisco, CA, 94105, US", "jsonld.address", 0.95),
		Street: "500 Howard St", City: "San Francisco", Region: "CA", Postal: "94105", Country: "US"}}
	jane := person("Dr. Jane Doe", "CEO & Co-Founder", 0.8, "https://www.linkedin.com/in/janedoe", "jane.doe@acme.com")
	jane.Phones = []string{"+14155552673"}
	ex.People = []model.PersonFact{jane, person("Tom O'Brien", "Head of Sales", 0.7, "")}

	m := mustResolve(t, e, ex)
	ix := indexOf(m)
	company := model.CompanyID("acme.com")
	c := ix.nodes["Company/"+company]
	if got := c.Scalars["name"]; got.Value != "Acme Robotics" || got.Confidence != 0.95 {
		t.Errorf("company name = %+v (legal suffix must be stripped, highest confidence must win)", got)
	}
	if c.Scalars["description"].Value != "Robots for factories" || c.Scalars["founded"].Value != "2012" || c.Scalars["country"].Value != "US" {
		t.Errorf("company scalars = %+v", c.Scalars)
	}
	if len(c.Facts) != 5 {
		t.Errorf("company facts = %d, want all 5 values with provenance: %v", len(c.Facts), c.Facts)
	}
	if d := ix.nodes["Domain/www.acme.com"]; d.Props["registrable"] != "acme.com" || d.Props["tld"] != "com" {
		t.Errorf("domain = %+v", d)
	}
	pg := ix.nodes["Page/https://www.acme.com/"]
	if pg.Scalars["type"].Value != "home" || pg.Scalars["status"].Value != 200 || pg.Scalars["raw_key"].Value != "raw/x.jsonl.gz" || pg.Scalars["content_hash"].Value != "h1" {
		t.Errorf("page = %+v", pg.Scalars)
	}

	people := ix.labelKeys(model.LabelPerson)
	if len(people) != 2 {
		t.Fatalf("people = %v", people)
	}
	janeID := model.PersonID(company, "jane doe", "https://www.linkedin.com/in/janedoe")
	jn, ok := ix.nodes["Person/"+janeID]
	if !ok || jn.Scalars["full_name"].Value != "Jane Doe" || jn.Scalars["title"].Value != "CEO & Co-Founder" || jn.Scalars["seniority"].Value != "founder" {
		t.Errorf("jane = %+v", jn)
	}
	tomID := model.PersonID(company, "tom o brien", "head of sales")
	if tn := ix.nodes["Person/"+tomID]; tn.Scalars["seniority"].Value != "director" {
		t.Errorf("tom = %+v", tn)
	}
	if e := ix.edges["EMPLOYS Company/"+company+" -> Person/"+janeID]; e.Props["title"] != "CEO & Co-Founder" || e.Confidence != 0.8 {
		t.Errorf("employs edge = %+v", e)
	}

	want := []struct{ typ, from, to string }{
		{"HAS_DOMAIN", "Company/" + company, "Domain/www.acme.com"},
		{"HAS_PAGE", "Domain/www.acme.com", "Page/https://www.acme.com/"},
		{"HAS_EMAIL", "Company/" + company, "Email/hello@acme.com"},
		{"HAS_EMAIL", "Company/" + company, "Email/someone@gmail.com"},
		{"HAS_EMAIL", "Person/" + janeID, "Email/jane.doe@acme.com"},
		{"HAS_EMAIL", "Person/" + tomID, "Email/tom.obrien@acme.com"}, // linked by name pattern
		{"HAS_PHONE", "Company/" + company, "Phone/+14155552671"},
		{"HAS_PHONE", "Person/" + janeID, "Phone/+14155552673"},
		{"HAS_SOCIAL", "Company/" + company, "SocialProfile/https://www.linkedin.com/company/acme"},
		{"HAS_SOCIAL", "Person/" + janeID, "SocialProfile/https://www.linkedin.com/in/janedoe"},
		{"USES", "Company/" + company, "Technology/WordPress"},
		{"MENTIONS", "Page/https://www.acme.com/", "Person/" + janeID},
		{"MENTIONS", "Page/https://www.acme.com/", "Email/jane.doe@acme.com"},
		{"MENTIONS", "Page/https://www.acme.com/", "Phone/+14155552671"},
	}
	for _, w := range want {
		if !ix.hasEdge(w.typ, w.from, w.to) {
			t.Errorf("missing edge %s %s -> %s", w.typ, w.from, w.to)
		}
	}
	for _, not := range [][3]string{
		{"HAS_EMAIL", "Company/" + company, "Email/jane.doe@acme.com"},   // owned by Jane
		{"HAS_EMAIL", "Company/" + company, "Email/tom.obrien@acme.com"}, // owned by Tom
		{"HAS_PHONE", "Company/" + company, "Phone/+14155552673"},
	} {
		if ix.hasEdge(not[0], not[1], not[2]) {
			t.Errorf("unexpected edge %v", not)
		}
	}
	locs := ix.labelKeys(model.LabelLocation)
	if len(locs) != 1 || !ix.hasEdge("LOCATED_AT", "Company/"+company, "Location/"+locs[0]) || ix.nodes["Location/"+locs[0]].Props["country"] != "US" {
		t.Errorf("location = %v", locs)
	}
	if en := ix.nodes["Email/hello@acme.com"]; en.Props["kind"] != "role" || en.Props["status"] != "unverified" || en.Props["domain"] != "acme.com" {
		t.Errorf("email node = %+v", en.Props)
	}
	for _, edge := range m.Edges {
		if edge.SourceURL == "" || edge.Confidence <= 0 || edge.Confidence > 1 || edge.SeenAt.IsZero() {
			t.Errorf("edge without provenance: %+v", edge)
		}
		if _, ok := model.EdgeByShape(edge.Type, edge.FromLabel, edge.ToLabel); !ok {
			t.Errorf("edge shape not in schema: %+v", edge)
		}
	}

	// Resolving the same extraction again is byte-for-byte reproducible.
	again := mustResolve(t, e, ex)
	if len(again.Nodes) != len(m.Nodes) || len(again.Edges) != len(m.Edges) {
		t.Errorf("second resolve differs: %d/%d vs %d/%d", len(again.Nodes), len(again.Edges), len(m.Nodes), len(m.Edges))
	}
	for i := range m.Nodes {
		if again.Nodes[i].Key != m.Nodes[i].Key {
			t.Fatalf("node order differs at %d", i)
		}
	}
}

func TestPersonDedupeAcrossPages(t *testing.T) {
	e := newEnv(t, nil)
	ids := func(ex *model.Extraction) []string {
		return indexOf(mustResolve(t, e, ex)).labelKeys(model.LabelPerson)
	}
	one := func(ex *model.Extraction) string {
		t.Helper()
		got := ids(ex)
		if len(got) != 1 {
			t.Fatalf("expected one person, got %v", got)
		}
		return got[0]
	}

	about := page("https://acme.com/about", model.PageAbout)
	about.People = []model.PersonFact{person("Jane Doe", "Chief Executive Officer", 0.8, "")}
	first := one(about)

	team := page("https://acme.com/team", model.PageTeam)
	team.People = []model.PersonFact{person("Dr. Jane Doe", "CEO & Co-Founder", 0.8, "https://www.linkedin.com/in/janedoe")}
	if got := one(team); got != first {
		t.Errorf("same person with a different title and a LinkedIn link got a new ID")
	}
	blog := page("https://acme.com/blog/x", model.PageOther)
	blog.People = []model.PersonFact{person("Jane A. Doe", "", 0.6, ""), person("JANE DOE", "CEO", 0.6, "")}
	if got := ids(blog); len(got) != 1 || got[0] != first {
		t.Errorf("middle initial / casing variants not merged: %v", got)
	}

	other := page("https://acme.com/people", model.PageTeam)
	other.People = []model.PersonFact{
		person("John Doe", "CFO", 0.8, ""),
		person("Jane Dow", "Engineer", 0.8, ""),
		person("Jane Doe", "Engineer", 0.8, "https://www.linkedin.com/in/other-jane"),
	}
	got := ids(other)
	if len(got) != 3 {
		t.Fatalf("distinct people merged: %v", got)
	}
	for _, id := range got {
		if id == first {
			t.Errorf("a different person (other name, or same name with another LinkedIn profile) was merged into Jane")
		}
	}
	// The same name at another company is another person.
	globex := page("https://globex.com/team", model.PageTeam)
	globex.People = []model.PersonFact{person("Jane Doe", "Chief Executive Officer", 0.8, "")}
	if one(globex) == first {
		t.Error("people must be scoped to their company")
	}
}

func TestSuppressionAtResolver(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	company := model.CompanyID("acme.com")
	e.sup.Add(ctx, suppress.KindEmail, "Jane.Doe@acme.com")
	e.sup.AddMember(ctx, suppress.KindPersonName, suppress.PersonNameMember(company, "Tom O'Brien"))

	ex := page("https://acme.com/team", model.PageTeam)
	ex.Emails = []model.EmailFact{email("jane.doe@acme.com", model.EmailPersonal), email("hello@acme.com", model.EmailRole)}
	ex.People = []model.PersonFact{
		person("Jane Doe", "CEO", 0.8, "", "jane.doe@acme.com"),
		person("Tom O'Brien", "VP Sales", 0.8, ""),
		person("TOM O'BRIEN", "Head of Sales", 0.8, ""), // new title, same person: still suppressed
	}
	ix := indexOf(mustResolve(t, e, ex))
	if _, ok := ix.nodes["Email/jane.doe@acme.com"]; ok {
		t.Error("suppressed email was emitted")
	}
	for k := range ix.edges {
		if strings.Contains(k, "jane.doe@acme.com") {
			t.Errorf("edge to suppressed email: %s", k)
		}
	}
	if _, ok := ix.nodes["Email/hello@acme.com"]; !ok {
		t.Error("unrelated email dropped")
	}
	people := ix.labelKeys(model.LabelPerson)
	if len(people) != 1 || ix.nodes["Person/"+people[0]].Scalars["full_name"].Value != "Jane Doe" {
		t.Errorf("people = %v; Tom is suppressed by name, Jane is not", people)
	}
	if reg, _ := e.rdb.HLen(ctx, "lg:people:"+company).Result(); reg != 1 {
		t.Errorf("suppressed person leaked into the registry (%d entries)", reg)
	}

	e.sup.Add(ctx, suppress.KindDomain, "https://www.acme.com/anything")
	e.sup.Add(ctx, suppress.KindPerson, people[0])
	if m := mustResolve(t, e, ex); m != nil {
		t.Errorf("suppressed domain still produced a mutation: %+v", m)
	}
}

func TestCountryPolicyAtResolver(t *testing.T) {
	e := newEnv(t, func(c *config.Config) { c.Resolve.Countries.Deny = []string{"DE"} })
	ctx := context.Background()

	// The team page arrives first; nothing says where the company is.
	team := page("https://mueller.com/team", model.PageTeam)
	team.People = []model.PersonFact{person("Hans Müller", "Geschäftsführer", 0.8, "")}
	if m := mustResolve(t, e, team); m == nil || len(indexOf(m).labelKeys(model.LabelPerson)) != 1 {
		t.Fatal("unknown country should pass a deny-only policy")
	}
	// Then the contact page reveals a denied country.
	contact := page("https://mueller.com/kontakt", model.PageContact)
	contact.Addresses = []model.AddressFact{{Fact: fact("München", "address.tag", 0.6), City: "München", Postal: "80331", Country: "DE"}}
	contact.Emails = []model.EmailFact{email("info@mueller.com", model.EmailRole)}
	m := mustResolve(t, e, contact)
	if m == nil || len(m.Nodes) != 0 || len(m.PurgeCompanies) != 1 || m.PurgeCompanies[0] != model.CompanyID("mueller.com") {
		t.Fatalf("denied company must be purged, got %+v", m)
	}
	if !e.sup.DomainBlocked(ctx, "mueller.com") {
		t.Error("denied domain must be blocked from further crawling")
	}
	if m := mustResolve(t, e, team); m != nil {
		t.Error("later pages of a denied company must be dropped")
	}

	// ccTLD evidence, allow list, and the unknown-country default.
	e2 := newEnv(t, func(c *config.Config) {
		c.Resolve.Countries.Allow = []string{"US"}
		c.Resolve.Countries.Unknown = "deny"
	})
	if m := mustResolve(t, e2, page("https://acme.fr/", model.PageHome)); m == nil || len(m.PurgeCompanies) != 1 {
		t.Errorf("ccTLD outside the allow list should be purged: %+v", m)
	}
	if m := mustResolve(t, e2, page("https://acme.com/", model.PageHome)); m != nil {
		t.Errorf("unknown country with unknown=deny should be dropped silently: %+v", m)
	}
	us := page("https://initech.com/", model.PageHome)
	us.Addresses = []model.AddressFact{{Fact: fact("Austin", "jsonld.address", 0.95), City: "Austin", Country: "United States"}}
	if m := mustResolve(t, e2, us); m == nil || len(m.Nodes) == 0 {
		t.Error("allowed country dropped")
	}
	// Once established by an address, the country sticks for pages without evidence.
	if m := mustResolve(t, e2, page("https://initech.com/team", model.PageTeam)); m == nil || len(m.Nodes) == 0 {
		t.Error("company country was not remembered")
	}
}

func TestNonContentPages(t *testing.T) {
	e := newEnv(t, nil)
	probe := page("https://acme.com/leadership", model.PageTeam)
	probe.Probe, probe.Status = true, 404
	if m := mustResolve(t, e, probe); m != nil {
		t.Errorf("a missing well-known path is not a page: %+v", m)
	}
	blocked := page("https://acme.com/private/x", model.PageOther)
	blocked.Status, blocked.SkipReason = 0, model.SkipRobots
	blocked.Emails = []model.EmailFact{email("x@acme.com", model.EmailPersonal)}
	ix := indexOf(mustResolve(t, e, blocked))
	pg := ix.nodes["Page/https://acme.com/private/x"]
	if pg.Scalars["skip_reason"].Value != model.SkipRobots || len(ix.labelKeys(model.LabelEmail)) != 0 {
		t.Errorf("robots-blocked page should be recorded with its reason and nothing else: %+v", pg.Scalars)
	}
	if _, has := pg.Scalars["content_hash"]; has {
		t.Error("a page that was not fetched has no content")
	}
	notFound := page("https://acme.com/gone", model.PageOther)
	notFound.Status = 404
	if ix := indexOf(mustResolve(t, e, notFound)); ix.nodes["Page/https://acme.com/gone"].Scalars["status"].Value != 404 {
		t.Error("a linked page that 404s is recorded with its status")
	}
	unchanged := page("https://acme.com/about", model.PageAbout)
	unchanged.Unchanged = true
	unchanged.People = []model.PersonFact{person("Jane Doe", "CEO", 0.8, "")}
	ix = indexOf(mustResolve(t, e, unchanged))
	pg = ix.nodes["Page/https://acme.com/about"]
	if _, has := pg.Scalars["type"]; has || len(ix.labelKeys(model.LabelPerson)) != 0 || pg.Scalars["fetched_at"].Value == nil {
		t.Errorf("unchanged page should only refresh fetched_at: %+v", pg.Scalars)
	}
	noindex := page("https://acme.com/hidden", model.PageOther)
	noindex.NoIndex = true
	noindex.Emails = []model.EmailFact{email("x@acme.com", model.EmailPersonal)}
	if ix := indexOf(mustResolve(t, e, noindex)); len(ix.labelKeys(model.LabelEmail)) != 0 {
		t.Error("noindex pages contribute no facts")
	}
}

func TestLinksToIsOrderIndependent(t *testing.T) {
	e := newEnv(t, nil)
	acme, globex := model.CompanyID("acme.com"), model.CompanyID("globex.com")

	// Acme links to Globex before Globex has ever been crawled.
	a := page("https://acme.com/partners", model.PageOther)
	a.Depth = 1
	a.OutboundDomains = []string{"globex.com", "acme.com", ""}
	ix := indexOf(mustResolve(t, e, a))
	if !ix.hasEdge("LINKS_TO", "Company/"+acme, "Company/"+globex) {
		t.Fatal("outbound link edge missing")
	}
	if ix.hasEdge("LINKS_TO", "Company/"+acme, "Company/"+acme) {
		t.Error("self link emitted")
	}
	// When Globex's homepage is resolved, the remembered inbound link is replayed.
	g := page("https://globex.com/", model.PageHome)
	ix = indexOf(mustResolve(t, e, g))
	edge, ok := ix.edges["LINKS_TO Company/"+acme+" -> Company/"+globex]
	if !ok || edge.SourceURL != "https://acme.com/partners" {
		t.Errorf("inbound link not replayed with its original source: %+v", edge)
	}
	// Inner pages do not replay.
	inner := page("https://globex.com/team", model.PageTeam)
	inner.Depth = 1
	if indexOf(mustResolve(t, e, inner)).hasEdge("LINKS_TO", "Company/"+acme, "Company/"+globex) {
		t.Error("only the homepage replays inbound links")
	}
}

func TestHandleRejectsGarbage(t *testing.T) {
	e := newEnv(t, nil)
	ctx := context.Background()
	if err := e.Handle(ctx, queue.Message{Payload: []byte("{not json")}); !queue.IsPermanent(err) {
		t.Errorf("garbage should be a permanent error, got %v", err)
	}
	if err := e.Handle(ctx, queue.Message{Payload: []byte(`{"url":""}`)}); !queue.IsPermanent(err) {
		t.Errorf("empty extraction should be a permanent error, got %v", err)
	}
	if err := e.Handle(ctx, queue.Message{Payload: []byte(`{"url":"https://acme.com/","host":"acme.com","domain":"acme.com","status":200,"fetched_at":"2026-03-01T10:00:00Z"}`)}); err != nil {
		t.Fatal(err)
	}
	if n, _ := e.rdb.XLen(ctx, "lg:stream:write").Result(); n != 1 {
		t.Errorf("mutation not published (write stream len %d)", n)
	}
}
