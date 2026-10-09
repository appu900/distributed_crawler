package extract

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/appu/leadgraph/pkg/model"
)

var fetchedAt = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

func fixture(t testing.TB, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func run(t testing.TB, url, file string) *model.Extraction {
	t.Helper()
	return Page(Input{URL: url, Body: fixture(t, file), Status: 200, FetchedAt: fetchedAt,
		Headers: map[string][]string{"Server": {"nginx/1.25"}}})
}

func values(fs []model.Fact) []string {
	out := make([]string, len(fs))
	for i, f := range fs {
		out[i] = f.Value
	}
	return out
}

func has(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

func emailsOf(e *model.Extraction) map[string]model.EmailFact {
	m := map[string]model.EmailFact{}
	for _, f := range e.Emails {
		m[f.Value] = f
	}
	return m
}

func peopleOf(e *model.Extraction) map[string]model.PersonFact {
	m := map[string]model.PersonFact{}
	for _, f := range e.People {
		m[f.Value] = f
	}
	return m
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestHomeJSONLD(t *testing.T) {
	e := run(t, "https://acme-robotics.com/", "home_jsonld.html")

	if e.PageType != model.PageHome || e.Domain != "acme-robotics.com" || e.Lang != "en-us" {
		t.Errorf("page: type=%s domain=%s lang=%s", e.PageType, e.Domain, e.Lang)
	}
	names := values(e.CompanyNames)
	for _, want := range []string{"Acme Robotics, Inc.", "Acme Robotics Incorporated", "Acme Robotics"} {
		if !has(names, want) {
			t.Errorf("company names %q missing %q", names, want)
		}
	}
	if e.CompanyNames[0].Confidence != 0.95 || e.CompanyNames[0].Extractor != "jsonld.organization" {
		t.Errorf("first name fact = %+v", e.CompanyNames[0])
	}
	if !has(values(e.Founded), "2012") {
		t.Errorf("founded = %v", values(e.Founded))
	}
	if !has(values(e.SizeHints), "51-200") || !has(values(e.SizeHints), "150") {
		t.Errorf("size hints = %v", values(e.SizeHints))
	}
	if !has(values(e.Industries), "manufacturing") {
		t.Errorf("industries = %v", values(e.Industries))
	}
	if len(e.Descriptions) < 3 || e.Descriptions[0].Confidence != 0.9 {
		t.Errorf("descriptions = %+v", e.Descriptions)
	}

	em := emailsOf(e)
	for addr, kind := range map[string]string{
		"hello@acme-robotics.com": model.EmailRole, "sales@acme-robotics.com": model.EmailRole,
		"jane.doe@acme-robotics.com": model.EmailPersonal,
	} {
		if got, ok := em[addr]; !ok || got.Kind != kind || got.Domain != "acme-robotics.com" {
			t.Errorf("email %s = %+v (present %v)", addr, got, ok)
		}
	}

	var phones []string
	for _, p := range e.Phones {
		phones = append(phones, p.Value)
	}
	if !has(phones, "+14155552671") || !has(phones, "+14155552672") {
		t.Errorf("phones = %v", phones)
	}

	var socials []string
	for _, s := range e.Socials {
		socials = append(socials, s.Value)
		if s.Scope != "company" {
			t.Errorf("social %s scope %q", s.Value, s.Scope)
		}
	}
	sort.Strings(socials)
	wantSocials := []string{
		"https://github.com/acme-robotics", "https://www.facebook.com/acmerobotics",
		"https://www.instagram.com/acme.robotics", "https://www.linkedin.com/company/acme-robotics",
		"https://www.youtube.com/@acmerobotics", "https://x.com/acmerobotics",
	}
	if strings.Join(socials, " ") != strings.Join(wantSocials, " ") {
		t.Errorf("socials = %v\n want %v", socials, wantSocials)
	}

	if len(e.Addresses) != 1 {
		t.Fatalf("addresses = %+v", e.Addresses)
	}
	a := e.Addresses[0]
	if a.Street != "500 Howard Street" || a.City != "San Francisco" || a.Region != "CA" || a.Postal != "94105" || a.Country != "US" {
		t.Errorf("address = %+v", a)
	}

	pp := peopleOf(e)
	jane, ok := pp["Jane Doe"]
	if !ok || jane.Title != "CEO & Co-Founder" || !has(jane.Emails, "jane.doe@acme-robotics.com") ||
		len(jane.Socials) != 1 || jane.Socials[0].Value != "https://www.linkedin.com/in/janedoe" {
		t.Errorf("jane = %+v (people %v)", jane, sortedKeys(pp))
	}
	if rahul, ok := pp["Rahul Mehta"]; !ok || rahul.Title != "Founder" {
		t.Errorf("rahul = %+v", rahul)
	}

	tech := map[string]string{}
	for _, tf := range e.Technologies {
		tech[tf.Value] = tf.Category
	}
	for _, want := range []string{"WordPress", "Google Analytics", "HubSpot", "jQuery", "Nginx"} {
		if _, ok := tech[want]; !ok {
			t.Errorf("technology %s not detected (got %v)", want, sortedKeys(tech))
		}
	}

	var links []string
	for _, l := range e.Links {
		links = append(links, l.URL)
	}
	wantLinks := []string{"https://acme-robotics.com/about", "https://acme-robotics.com/team", "https://acme-robotics.com/contact"}
	if strings.Join(links, " ") != strings.Join(wantLinks, " ") {
		t.Errorf("links = %v", links)
	}
	for _, d := range []string{"globex-industries.com", "initech.io"} {
		if !has(e.OutboundDomains, d) {
			t.Errorf("outbound %v missing %s", e.OutboundDomains, d)
		}
	}
	// Provenance is on every fact.
	for _, f := range e.CompanyNames {
		if f.SourceURL != e.URL || f.Extractor == "" || f.Confidence <= 0 || f.Confidence > 1 || !f.ExtractedAt.Equal(fetchedAt) {
			t.Errorf("bad provenance: %+v", f)
		}
	}
}

func TestMessyTeamPage(t *testing.T) {
	e := run(t, "https://acme-robotics.com/team", "team_messy.html")
	if e.PageType != model.PageTeam {
		t.Fatalf("page type = %s", e.PageType)
	}
	pp := peopleOf(e)
	want := map[string]string{
		"Jane Doe":          "Chief Executive Officer",
		"Rahul Mehta":       "CTO & Co-Founder",
		"Søren Kierkegård":  "Head of Engineering",
		"María José García": "VP of Sales, EMEA",
		"Tom O'Brien":       "Director of Customer Success",
		"Priya Raman":       "Finance Manager",
		"Chen Wei":          "Senior Software Engineer",
		"Anna Schmidt":      "Marketing Director",
	}
	for name, title := range want {
		got, ok := pp[name]
		if !ok {
			t.Errorf("missing person %q (have %v)", name, sortedKeys(pp))
			continue
		}
		if got.Title != title {
			t.Errorf("%s title = %q, want %q", name, got.Title, title)
		}
	}
	for name := range pp {
		if _, ok := want[name]; !ok {
			t.Errorf("unexpected person %q: %+v", name, pp[name])
		}
	}
	jane := pp["Jane Doe"]
	if !has(jane.Emails, "jane.doe@acme-robotics.com") || !has(jane.Phones, "+14155552673") ||
		len(jane.Socials) != 1 || jane.Socials[0].Value != "https://www.linkedin.com/in/janedoe" {
		t.Errorf("jane contacts = %+v", jane)
	}
	rahul := pp["Rahul Mehta"]
	if !has(rahul.Emails, "rahul@acme-robotics.com") || len(rahul.Socials) != 1 || rahul.Socials[0].Network != NetGitHub {
		t.Errorf("rahul contacts = %+v", rahul)
	}
	if !has(pp["Priya Raman"].Emails, "priya@acme-robotics.com") || !has(pp["Chen Wei"].Emails, "chen.wei@acme-robotics.com") {
		t.Errorf("table emails: %+v %+v", pp["Priya Raman"], pp["Chen Wei"])
	}

	em := emailsOf(e)
	wantEmails := []string{"chen.wei@acme-robotics.com", "jane.doe@acme-robotics.com", "press@acme-robotics.com",
		"priya@acme-robotics.com", "rahul@acme-robotics.com"}
	if got := sortedKeys(em); strings.Join(got, " ") != strings.Join(wantEmails, " ") {
		t.Errorf("emails = %v\n  want %v", got, wantEmails)
	}
	if em["rahul@acme-robotics.com"].Extractor != "cfemail" || em["priya@acme-robotics.com"].Extractor != "deobfuscate" {
		t.Errorf("extractors: %+v", em)
	}
	if em["press@acme-robotics.com"].Kind != model.EmailRole || em["press@acme-robotics.com"].Confidence != 0.5 {
		t.Errorf("press = %+v", em["press@acme-robotics.com"])
	}
	// Person profiles are not company profiles.
	for _, s := range e.Socials {
		t.Errorf("unexpected company social %s", s.Value)
	}
}

func TestGermanContactPage(t *testing.T) {
	e := run(t, "https://www.mueller-maschinenbau.de/impressum", "contact_de.html")
	if e.PageType != model.PageContact {
		t.Errorf("page type = %s", e.PageType)
	}
	em := emailsOf(e)
	wantEmails := []string{"hans@mailinator.com", "info@mueller-maschinenbau.de", "support@mueller-maschinenbau.de", "vertrieb@mueller-maschinenbau.de"}
	if got := sortedKeys(em); strings.Join(got, " ") != strings.Join(wantEmails, " ") {
		t.Errorf("emails = %v", got)
	}
	if !em["hans@mailinator.com"].Disposable || em["info@mueller-maschinenbau.de"].Disposable {
		t.Error("disposable flag wrong")
	}
	phones := map[string]string{}
	for _, p := range e.Phones {
		phones[p.Value] = p.Kind
	}
	if _, ok := phones["+498912345670"]; !ok {
		t.Errorf("national number not normalized with DE region: %v", phones)
	}
	if phones["+4989123456799"] != "fax" {
		t.Errorf("fax not flagged: %v", phones)
	}
	if len(phones) != 2 {
		t.Errorf("dates, IDs or order numbers leaked into phones: %v", phones)
	}
	var muc, ham *model.AddressFact
	for i := range e.Addresses {
		switch e.Addresses[i].City {
		case "München":
			muc = &e.Addresses[i]
		case "Hamburg":
			ham = &e.Addresses[i]
		}
	}
	if muc == nil || muc.Postal != "80331" || muc.Street != "Industriestraße 12" || muc.Country != "DE" {
		t.Errorf("munich address = %+v (all %+v)", muc, e.Addresses)
	}
	if ham == nil || ham.Postal != "20457" || ham.Street != "Hafenweg 3" || ham.Country != "DE" || ham.Extractor != "microdata.address" {
		t.Errorf("hamburg address = %+v", ham)
	}
	if !has(values(e.CompanyNames), "Müller Maschinenbau GmbH") {
		t.Errorf("names = %v", values(e.CompanyNames))
	}
}

func TestRobotsMeta(t *testing.T) {
	body := `<html><head><meta name="robots" content="NOINDEX, nofollow"><title>Hidden</title></head>
		<body><a href="/next">next</a><a href="mailto:ceo@acme.com">mail</a></body></html>`
	e := Page(Input{URL: "https://acme.com/secret", Body: []byte(body), FetchedAt: fetchedAt})
	if !e.NoIndex || !e.NoFollow || len(e.Emails) != 0 || len(e.Links) != 0 {
		t.Errorf("noindex/nofollow not honored: %+v", e)
	}
	body = `<html><head><meta name="robots" content="noindex"></head><body><a href="/next">n</a>
		<a href="mailto:ceo@acme.com">m</a></body></html>`
	e = Page(Input{URL: "https://acme.com/secret", Body: []byte(body), FetchedAt: fetchedAt})
	if !e.NoIndex || e.NoFollow || len(e.Emails) != 0 || len(e.Links) != 1 {
		t.Errorf("noindex only: %+v", e)
	}
	e = Page(Input{URL: "https://acme.com/x", Body: []byte(`<a href="mailto:ceo@acme.com">m</a>`), FetchedAt: fetchedAt,
		Headers: map[string][]string{"X-Robots-Tag": {"noindex"}}})
	if !e.NoIndex || len(e.Emails) != 0 {
		t.Errorf("X-Robots-Tag not honored: %+v", e)
	}
}

func TestMalformedHTML(t *testing.T) {
	inputs := []string{
		"", "<", "<html", "</div></div><p>", "<a href=", "<script>", "\x00\x01\x02",
		"<table><tr><td><table><tr><td>Jane Doe<td>CEO",
		`<script type="application/ld+json">{"@type":"Organization","name":`,
		`<script type="application/ld+json">[[[[[[[[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]]]]]]</script>`,
		strings.Repeat("<div>", 5000) + "ceo@acme.com",
	}
	for _, in := range inputs {
		e := Page(Input{URL: "https://acme.com/team", Body: []byte(in), FetchedAt: fetchedAt})
		if e == nil {
			t.Fatalf("nil extraction for %q", in)
		}
	}
	e := Page(Input{URL: "https://acme.com/team", Body: []byte(inputs[7]), FetchedAt: fetchedAt})
	if p, ok := peopleOf(e)["Jane Doe"]; !ok || p.Title != "CEO" {
		t.Errorf("unclosed table: %+v", e.People)
	}
	if e := Page(Input{URL: "::bad::", Body: []byte("<p>x")}); e == nil {
		t.Fatal("nil for bad url")
	}
}

func TestEmailTextCases(t *testing.T) {
	tests := []struct {
		name, text string
		want       []string
	}{
		{"plain", "Mail jane.doe@acme.com.", []string{"jane.doe@acme.com"}},
		{"upper", "JANE@ACME.COM", []string{"jane@acme.com"}},
		{"bracket at dot", "jane [at] acme [dot] com", []string{"jane@acme.com"}},
		{"paren", "jane(at)acme(dot)com", []string{"jane@acme.com"}},
		{"bracket at plain dot", "jane [at] acme.co.uk", []string{"jane@acme.co.uk"}},
		{"spelled", "name at domain dot com is a pattern: bob at acme dot com", []string{"bob@acme.com"}},
		{"sentence end", "Press: press [at] acme [dot] com. Tom is nice", []string{"press@acme.com"}},
		{"sentence end spelled", "write bob at acme dot com today", []string{"bob@acme.com"}},
		{"spaced at sign", "jane @ acme.com", []string{"jane@acme.com"}},
		{"prose is not an address", "Visit us at acme.com or look at example.org", nil},
		{"asset", "logo@2x.png and icon@3x.webp", nil},
		{"placeholder", "you@example.com user@domain.com", nil},
		{"unknown tld", "jane@acme.notarealtld", nil},
		{"hash local", "0123456789abcdef0123456789abcdef@sentry.acme.com", nil},
		{"plus tag", "sales+eu@acme.com", []string{"sales+eu@acme.com"}},
		{"subdomain", "a.b@mail.acme.co.uk", []string{"a.b@mail.acme.co.uk"}},
		{"double dot", "a..b@acme.com", nil},
		{"multiple", "x@acme.com, y@acme.com; x@acme.com", []string{"x@acme.com", "y@acme.com"}},
		{"reserved tld rejected", "jane@acme.test", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &emailSink{best: map[string]model.EmailFact{}, now: func() model.Fact { return model.Fact{} }}
			textEmails(tt.text, s)
			got := sortedKeys(s.best)
			if strings.Join(got, " ") != strings.Join(tt.want, " ") {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
	if CleanEmail("jane@acme.test", EmailOptions{AllowReservedTLDs: true}) != "jane@acme.test" {
		t.Error("reserved TLD should be accepted when allowed")
	}
}

func TestClassifyEmail(t *testing.T) {
	tests := map[string]string{
		"info@acme.com": model.EmailRole, "sales+eu@acme.com": model.EmailRole,
		"customer.support@acme.com": model.EmailRole, "no-reply@acme.com": model.EmailRole,
		"jane@acme.com": model.EmailPersonal, "j.doe@acme.com": model.EmailPersonal,
		"jane.sales@acme.com": model.EmailPersonal, "team2@acme.com": model.EmailRole,
	}
	for in, want := range tests {
		if got := ClassifyEmail(in); got != want {
			t.Errorf("ClassifyEmail(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestDecodeCFEmail(t *testing.T) {
	if got := DecodeCFEmail("abd9cac3dec7ebcac8c6ce86d9c4c9c4dfc2c8d885c8c4c6"); got != "rahul@acme-robotics.com" {
		t.Errorf("got %q", got)
	}
	for _, bad := range []string{"", "zz", "a", "ab"} {
		if DecodeCFEmail(bad) != "" {
			t.Errorf("DecodeCFEmail(%q) should be empty", bad)
		}
	}
}

func TestNormalizePhone(t *testing.T) {
	tests := []struct {
		raw, region, want string
	}{
		{"+1 (415) 555-2671", "", "+14155552671"},
		{"(415) 555-2671", "US", "+14155552671"},
		{"020 7946 0958", "GB", "+442079460958"},
		{"089 1234567-0", "DE", "+498912345670"},
		{"+91 98765 43210", "US", "+919876543210"},
		{"12345", "US", ""},
		{"not a number", "US", ""},
		{"555-0100", "US", ""},
	}
	for _, tt := range tests {
		got, _, ok := NormalizePhone(tt.raw, tt.region)
		if got != tt.want || ok != (tt.want != "") {
			t.Errorf("NormalizePhone(%q,%q) = %q,%v want %q", tt.raw, tt.region, got, ok, tt.want)
		}
	}
}

func TestTextPhones(t *testing.T) {
	tests := []struct {
		name, text, region string
		want               []string
	}{
		{"intl", "Reach us on +44 20 7946 0958 today", "US", []string{"+442079460958"}},
		{"keyword", "Phone: (415) 555-2671", "US", []string{"+14155552671"}},
		{"no keyword no plus", "Order 4155552671 shipped", "US", nil},
		{"date", "Tel updated 12.05.2021", "DE", nil},
		{"year range", "Call history 2019-2023", "US", nil},
		{"two numbers", "Tel +1 415 555 2671 / Fax +1 415 555 2672", "US", []string{"+14155552671", "+14155552672"}},
		{"in word", "SKU-4155552671x", "US", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &phoneSink{region: tt.region, best: map[string]model.PhoneFact{}, now: func() model.Fact { return model.Fact{} }}
			textPhones(tt.text, s)
			got := sortedKeys(s.best)
			if strings.Join(got, " ") != strings.Join(tt.want, " ") {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCanonicalSocial(t *testing.T) {
	tests := []struct {
		in, want, network, scope string
	}{
		{"https://www.linkedin.com/company/Acme-Robotics/about/?trk=x", "https://www.linkedin.com/company/acme-robotics", NetLinkedIn, "company"},
		{"https://de.linkedin.com/in/Jane-Doe-123/", "https://www.linkedin.com/in/jane-doe-123", NetLinkedIn, "person"},
		{"https://linkedin.com/shareArticle?url=x", "", "", ""},
		{"https://twitter.com/AcmeHQ", "https://x.com/acmehq", NetX, ""},
		{"https://x.com/@acmehq?lang=en", "https://x.com/acmehq", NetX, ""},
		{"https://twitter.com/intent/tweet?text=hi", "", "", ""},
		{"https://twitter.com/share", "", "", ""},
		{"http://m.facebook.com/AcmeHQ/", "https://www.facebook.com/acmehq", NetFacebook, ""},
		{"https://www.facebook.com/sharer/sharer.php?u=x", "", "", ""},
		{"https://www.facebook.com/profile.php?id=1000123", "https://www.facebook.com/profile.php?id=1000123", NetFacebook, "person"},
		{"https://instagram.com/acme.hq/", "https://www.instagram.com/acme.hq", NetInstagram, ""},
		{"https://www.instagram.com/p/Cabc123/", "", "", ""},
		{"https://github.com/Acme/robot-sdk", "https://github.com/acme", NetGitHub, ""},
		{"https://github.com/orgs/acme/people", "https://github.com/acme", NetGitHub, ""},
		{"https://github.com/features", "", "", ""},
		{"https://www.youtube.com/@AcmeHQ", "https://www.youtube.com/@acmehq", NetYouTube, ""},
		{"https://www.youtube.com/channel/UCabcDEF123", "https://www.youtube.com/channel/UCabcDEF123", NetYouTube, ""},
		{"https://www.youtube.com/watch?v=abc", "", "", ""},
		{"https://example.com/in/jane", "", "", ""},
		{"https://notlinkedin.com/in/jane", "", "", ""},
		{"not a url", "", "", ""},
	}
	for _, tt := range tests {
		got, ok := CanonicalSocial(tt.in)
		if got.URL != tt.want || ok != (tt.want != "") || got.Network != tt.network || got.Scope != tt.scope {
			t.Errorf("CanonicalSocial(%q) = %+v, %v; want %q/%s/%s", tt.in, got, ok, tt.want, tt.network, tt.scope)
		}
	}
}

func TestParseAddressText(t *testing.T) {
	tests := []struct {
		name  string
		lines []string
		tld   string
		want  model.AddressFact
		ok    bool
	}{
		{"us", []string{"Acme Inc", "500 Howard St, Suite 4", "San Francisco, CA 94105"}, "",
			model.AddressFact{Street: "500 Howard St, Suite 4", City: "San Francisco", Region: "CA", Postal: "94105", Country: "US"}, true},
		{"uk", []string{"1 Canada Square", "London E14 5AB", "United Kingdom"}, "",
			model.AddressFact{Street: "1 Canada Square", City: "London", Postal: "E14 5AB", Country: "GB"}, true},
		{"de", []string{"Industriestraße 12", "80331 München"}, "DE",
			model.AddressFact{Street: "Industriestraße 12", City: "München", Postal: "80331", Country: "DE"}, true},
		{"ca", []string{"100 King St W", "Toronto, ON M5X 1A9", "Canada"}, "",
			model.AddressFact{Street: "100 King St W", City: "Toronto", Region: "ON", Postal: "M5X 1A9", Country: "CA"}, true},
		{"no postal", []string{"Somewhere nice", "Call us"}, "", model.AddressFact{}, false},
		{"empty", nil, "", model.AddressFact{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseAddressText(tt.lines, tt.tld)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v (%+v)", ok, tt.ok, got)
			}
			if ok && (got.Street != tt.want.Street || got.City != tt.want.City || got.Region != tt.want.Region ||
				got.Postal != tt.want.Postal || got.Country != tt.want.Country) {
				t.Errorf("got %+v\nwant %+v", got, tt.want)
			}
		})
	}
}

func TestPlausibleNameAndTitle(t *testing.T) {
	names := map[string]bool{
		"Jane Doe": true, "Jean-Luc Picard": true, "Maria von Trapp": true, "Tom O'Brien": true,
		"J. R. Tolkien": true, "Søren Kierkegård": true,
		"Our Team": false, "Contact Us": false, "Jane": false, "jane doe": false, "Chief Executive": false,
		"Jane Doe 3rd": false, "Read More": false, "Sales Manager": false, "New York": false,
		"A B": false, "One Two Three Four Five Six": false,
	}
	for in, want := range names {
		if got := plausibleName(in, false); got != want {
			t.Errorf("plausibleName(%q) = %v, want %v", in, got, want)
		}
	}
	titles := map[string]bool{
		"CEO": true, "Head of Engineering": true, "VP, Sales": true, "Co-Founder & CTO": true,
		"Geschäftsführer": true, "Senior Software Engineer": true,
		"We lead with curiosity.": false, "": false, "Loves hiking and coffee": false,
		"She joined the company in 2019 as an engineer and has since grown the platform team to twenty.": false,
	}
	for in, want := range titles {
		if got := LooksLikeTitle(in); got != want {
			t.Errorf("LooksLikeTitle(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestClassifyPath(t *testing.T) {
	tests := map[string]model.PageType{
		"/": model.PageHome, "": model.PageHome, "/index.html": model.PageHome, "/en": model.PageHome, "/de-de/": model.PageHome,
		"/about": model.PageAbout, "/about-us/": model.PageAbout, "/company/history": model.PageAbout,
		"/team": model.PageTeam, "/about/team": model.PageAbout, "/our-team": model.PageTeam, "/leadership": model.PageTeam,
		"/people": model.PageTeam, "/contact": model.PageContact, "/contact-us.html": model.PageContact,
		"/impressum": model.PageContact, "/careers": model.PageCareers, "/jobs/engineer": model.PageCareers,
		"/blog/post-1": model.PageOther, "/products": model.PageOther, "/en/kontakt": model.PageContact,
	}
	for in, want := range tests {
		if got := ClassifyPath(in); got != want {
			t.Errorf("ClassifyPath(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestTechRules(t *testing.T) {
	if DefaultTechRules.Len() < 40 {
		t.Errorf("only %d built-in rules", DefaultTechRules.Len())
	}
	if _, err := ParseTechRules([]byte("- name: X\n  src: ['(']")); err == nil {
		t.Error("expected error for bad regexp")
	}
	if _, err := ParseTechRules([]byte("- category: y")); err == nil {
		t.Error("expected error for missing name")
	}
	custom, err := ParseTechRules([]byte("- name: Custom\n  category: test\n  header: {X-Custom: '^v2'}"))
	if err != nil {
		t.Fatal(err)
	}
	e := Page(Input{URL: "https://a.com/", Body: []byte("<p>hi"), Rules: custom, Headers: map[string][]string{"X-Custom": {"v2.1"}}})
	if len(e.Technologies) != 1 || e.Technologies[0].Value != "Custom" || e.Technologies[0].Confidence != 0.9 {
		t.Errorf("tech = %+v", e.Technologies)
	}
}

func TestTextDensity(t *testing.T) {
	text, scripts := TextDensity([]byte(`<html><head><title>App</title><script>var a = "long long long";</script></head>
		<body><div id="root"></div><script src="/app.js"></script></body></html>`))
	if text != 0 || scripts != 1 {
		t.Errorf("text=%d scripts=%d", text, scripts)
	}
	text, _ = TextDensity([]byte(`<p>Hello world</p>`))
	if text != 11 {
		t.Errorf("text=%d", text)
	}
}

func TestDeterministic(t *testing.T) {
	a := run(t, "https://acme-robotics.com/", "home_jsonld.html")
	for i := 0; i < 5; i++ {
		b := run(t, "https://acme-robotics.com/", "home_jsonld.html")
		if len(a.People) != len(b.People) || len(a.Emails) != len(b.Emails) {
			t.Fatal("non-deterministic output")
		}
		for j := range a.People {
			if a.People[j].Value != b.People[j].Value || a.People[j].Title != b.People[j].Title {
				t.Fatalf("people order differs: %v vs %v", a.People[j], b.People[j])
			}
		}
	}
}

func FuzzPage(f *testing.F) {
	for _, name := range []string{"home_jsonld.html", "team_messy.html", "contact_de.html"} {
		f.Add(fixture(f, name), "https://acme-robotics.com/team")
	}
	f.Add([]byte(`<a href="mailto:a@b.com">x</a><a href="tel:+14155552671">`), "http://x.de/")
	f.Add([]byte(`<script type="application/ld+json">{"@graph":[{"@type":"Person","name":"A B","worksFor":{}}]}</script>`), "https://a.b/about")
	f.Add([]byte(`jane [at] acme [dot] com (at) @ at dot`), "https://a.b/")
	f.Fuzz(func(t *testing.T, body []byte, url string) {
		e := Page(Input{URL: url, Body: body, FetchedAt: fetchedAt})
		if e == nil {
			t.Fatal("nil extraction")
		}
		check := func(f model.Fact) {
			if f.Confidence < 0 || f.Confidence > 1 {
				t.Fatalf("confidence out of range: %+v", f)
			}
			if f.Value == "" || f.Extractor == "" {
				t.Fatalf("empty fact: %+v", f)
			}
		}
		for _, x := range e.Emails {
			check(x.Fact)
			if CleanEmail(x.Value, EmailOptions{}) != x.Value {
				t.Fatalf("invalid email emitted: %q", x.Value)
			}
		}
		for _, x := range e.Phones {
			check(x.Fact)
			if !strings.HasPrefix(x.Value, "+") {
				t.Fatalf("phone not E.164: %q", x.Value)
			}
		}
		for _, x := range e.People {
			check(x.Fact)
		}
		for _, x := range e.Socials {
			check(x.Fact)
		}
		for _, x := range e.CompanyNames {
			check(x)
		}
		for _, l := range e.Links {
			if !strings.HasPrefix(l.URL, "http") {
				t.Fatalf("bad link %q", l.URL)
			}
		}
	})
}

func BenchmarkPage(b *testing.B) {
	pages := []struct{ url, file string }{
		{"https://acme-robotics.com/", "home_jsonld.html"},
		{"https://acme-robotics.com/team", "team_messy.html"},
		{"https://www.mueller-maschinenbau.de/impressum", "contact_de.html"},
		{"https://acme-robotics.com/blog/typical", "typical_page.html"},
	}
	var inputs []Input
	total := 0
	for _, p := range pages {
		body := fixture(b, p.file)
		total += len(body)
		inputs = append(inputs, Input{URL: p.url, Body: body, Status: 200, FetchedAt: fetchedAt})
	}
	b.SetBytes(int64(total / len(inputs)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Page(inputs[i%len(inputs)])
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "pages/sec")
}
