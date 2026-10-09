package extract

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/pkg/model"
)

var orgTypes = toSet(`Organization Corporation LocalBusiness NGO EducationalOrganization
GovernmentOrganization MedicalOrganization OnlineBusiness OnlineStore Store ProfessionalService
LegalService FinancialService InsuranceAgency RealEstateAgent TravelAgency Restaurant Hotel
AutomotiveBusiness HomeAndConstructionBusiness FoodEstablishment HealthAndBeautyBusiness
MedicalBusiness Dentist Physician AccountingService Attorney Notary GeneralContractor
Electrician Plumber EmploymentAgency InternetCafe LodgingBusiness SportsOrganization
NewsMediaOrganization Consortium Airline ResearchOrganization Project CollegeOrUniversity
SoftwareCompany TechnologyCompany`)

// ldTypes returns the schema.org types of a node, without any prefix.
func ldTypes(obj map[string]any) []string {
	var out []string
	add := func(s string) {
		s = strings.TrimPrefix(s, "schema:")
		if i := strings.LastIndexByte(s, '/'); i >= 0 {
			s = s[i+1:]
		}
		if s != "" {
			out = append(out, s)
		}
	}
	switch t := obj["@type"].(type) {
	case string:
		add(t)
	case []any:
		for _, v := range t {
			if s, ok := v.(string); ok {
				add(s)
			}
		}
	}
	return out
}

func isOrg(types []string) bool {
	for _, t := range types {
		if _, ok := orgTypes[t]; ok || strings.HasSuffix(t, "Business") || strings.HasSuffix(t, "Organization") {
			return true
		}
	}
	return false
}

func hasType(types []string, want string) bool {
	for _, t := range types {
		if t == want {
			return true
		}
	}
	return false
}

// ldString renders a JSON-LD value as text: strings directly, numbers
// formatted, and objects via @value or name.
func ldString(v any) string {
	switch t := v.(type) {
	case string:
		return collapse(t)
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case map[string]any:
		if s := ldString(t["@value"]); s != "" {
			return s
		}
		return ldString(t["name"])
	case []any:
		if len(t) > 0 {
			return ldString(t[0])
		}
	}
	return ""
}

// ldList normalizes a value that may be a scalar, an object or an array.
func ldList(v any) []any {
	switch t := v.(type) {
	case nil:
		return nil
	case []any:
		return t
	default:
		return []any{t}
	}
}

var trailingComma = regexp.MustCompile(`,\s*([}\]])`)

// parseLD decodes one script body, tolerating the common breakages: HTML
// comment / CDATA wrappers, raw newlines inside strings and trailing commas.
func parseLD(src string) (any, bool) {
	s := strings.TrimSpace(src)
	s = strings.TrimPrefix(s, "<!--")
	s = strings.TrimSuffix(s, "-->")
	s = strings.TrimPrefix(strings.TrimSpace(s), "//<![CDATA[")
	s = strings.TrimSuffix(strings.TrimSpace(s), "//]]>")
	s = strings.TrimPrefix(strings.TrimSpace(s), "<![CDATA[")
	s = strings.TrimSuffix(strings.TrimSpace(s), "]]>")
	s = strings.TrimSuffix(strings.TrimSpace(s), ";")
	var v any
	if json.Unmarshal([]byte(s), &v) == nil {
		return v, true
	}
	fixed := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == '\t' {
			return ' '
		}
		return r
	}, s)
	fixed = trailingComma.ReplaceAllString(fixed, "$1")
	if json.Unmarshal([]byte(fixed), &v) == nil {
		return v, true
	}
	return nil, false
}

// maxLDNodes bounds traversal of hostile or enormous documents.
const maxLDNodes = 2000

func (st *state) extractJSONLD() {
	var top []map[string]any
	byID := map[string]map[string]any{}
	budget := maxLDNodes
	var index func(v any, depth int)
	index = func(v any, depth int) {
		if depth > 12 || budget <= 0 {
			return
		}
		switch t := v.(type) {
		case []any:
			for _, e := range t {
				index(e, depth+1)
			}
		case map[string]any:
			budget--
			if id, ok := t["@id"].(string); ok && len(t) > 1 {
				byID[id] = t
			}
			for _, e := range t {
				switch e.(type) {
				case []any, map[string]any:
					index(e, depth+1)
				}
			}
		}
	}
	for _, src := range st.p.jsonld {
		v, ok := parseLD(src)
		if !ok {
			continue
		}
		index(v, 0)
		var collect func(v any)
		collect = func(v any) {
			switch t := v.(type) {
			case []any:
				for _, e := range t {
					collect(e)
				}
			case map[string]any:
				if g, ok := t["@graph"]; ok {
					collect(g)
					if _, typed := t["@type"]; !typed {
						return
					}
				}
				top = append(top, t)
			}
		}
		collect(v)
	}
	// deref follows an {"@id": ...} reference to the node that defines it.
	deref := func(v any) map[string]any {
		obj, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		if _, typed := obj["@type"]; !typed {
			if id, ok := obj["@id"].(string); ok {
				if target, ok := byID[id]; ok {
					return target
				}
			}
		}
		return obj
	}
	for _, obj := range top {
		types := ldTypes(obj)
		switch {
		case isOrg(types):
			st.ldOrganization(obj, deref)
		case hasType(types, "Person"):
			jobTitle := ldString(obj["jobTitle"])
			if jobTitle != "" || obj["worksFor"] != nil || st.out.PageType == model.PageTeam || st.out.PageType == model.PageAbout {
				st.ldPerson(obj, "", 0.9)
			}
		case hasType(types, "WebSite"):
			if name := ldString(obj["name"]); name != "" {
				st.out.CompanyNames = append(st.out.CompanyNames, st.fact(name, "jsonld.website", 0.7))
			}
		}
	}
}

func (st *state) ldOrganization(obj map[string]any, deref func(any) map[string]any) {
	const ex = "jsonld.organization"
	if name := ldString(obj["name"]); name != "" {
		st.out.CompanyNames = append(st.out.CompanyNames, st.fact(name, ex, 0.95))
	}
	if legal := ldString(obj["legalName"]); legal != "" {
		st.out.CompanyNames = append(st.out.CompanyNames, st.fact(legal, ex+".legalName", 0.9))
	}
	if d := ldString(obj["description"]); d != "" {
		st.out.Descriptions = append(st.out.Descriptions, st.fact(clip(d, 1000), ex, 0.9))
	}
	if y := yearOf(ldString(obj["foundingDate"])); y != "" {
		st.out.Founded = append(st.out.Founded, st.fact(y, ex, 0.9))
	}
	if n := ldEmployees(obj["numberOfEmployees"]); n != "" {
		st.out.SizeHints = append(st.out.SizeHints, st.fact(n, ex, 0.9))
	}
	for _, key := range []string{"industry", "naics", "knowsAbout"} {
		for _, v := range ldList(obj[key]) {
			if s := ldString(v); s != "" && len(s) < 80 && key != "naics" {
				st.out.Industries = append(st.out.Industries, st.fact(strings.ToLower(s), ex, 0.8))
			}
		}
	}
	for _, v := range ldList(obj["email"]) {
		st.emails.add(strings.TrimPrefix(ldString(v), "mailto:"), ex, 0.95)
	}
	for _, v := range ldList(obj["telephone"]) {
		st.phones.add(ldString(v), ex, 0.95, false)
	}
	for _, v := range ldList(obj["faxNumber"]) {
		st.phones.add(ldString(v), ex, 0.95, true)
	}
	for _, v := range ldList(obj["sameAs"]) {
		st.socials.add(ldString(v), ex, 0.95, "company")
	}
	for _, v := range ldList(obj["address"]) {
		st.ldAddress(v, deref)
	}
	if loc := deref(obj["location"]); loc != nil {
		for _, v := range ldList(loc["address"]) {
			st.ldAddress(v, deref)
		}
	}
	for _, key := range []string{"contactPoint", "contactPoints"} {
		for _, v := range ldList(obj[key]) {
			cp := deref(v)
			if cp == nil {
				continue
			}
			fax := strings.Contains(strings.ToLower(ldString(cp["contactType"])), "fax")
			for _, t := range ldList(cp["telephone"]) {
				st.phones.add(ldString(t), "jsonld.contactPoint", 0.95, fax)
			}
			for _, e := range ldList(cp["email"]) {
				st.emails.add(strings.TrimPrefix(ldString(e), "mailto:"), "jsonld.contactPoint", 0.95)
			}
		}
	}
	for _, rel := range [...][2]string{
		{"founder", "Founder"}, {"founders", "Founder"}, {"employee", ""}, {"employees", ""},
		{"member", ""}, {"members", ""},
	} {
		key, defaultTitle := rel[0], rel[1]
		for _, v := range ldList(obj[key]) {
			if person := deref(v); person != nil && (hasType(ldTypes(person), "Person") || person["jobTitle"] != nil || defaultTitle != "") {
				st.ldPerson(person, defaultTitle, 0.9)
			}
		}
	}
}

func (st *state) ldPerson(obj map[string]any, defaultTitle string, conf float64) {
	name := ldString(obj["name"])
	if name == "" {
		name = collapse(ldString(obj["givenName"]) + " " + ldString(obj["familyName"]))
	}
	name = normalize.PersonName(name)
	if !plausibleName(name, false) {
		return
	}
	title := ldString(obj["jobTitle"])
	if title == "" {
		title = defaultTitle
	}
	pf := model.PersonFact{Fact: st.fact(name, "jsonld.person", conf), Title: normalize.Title(title)}
	for _, v := range ldList(obj["email"]) {
		if a := st.emails.add(strings.TrimPrefix(ldString(v), "mailto:"), "jsonld.person", 0.95); a != "" {
			pf.Emails = append(pf.Emails, a)
		}
	}
	for _, v := range ldList(obj["telephone"]) {
		if n := st.phones.add(ldString(v), "jsonld.person", 0.95, false); n != "" {
			pf.Phones = append(pf.Phones, n)
		}
	}
	for _, key := range []string{"sameAs", "url"} {
		for _, v := range ldList(obj[key]) {
			if pr, ok := CanonicalSocial(ldString(v)); ok && pr.Scope != "company" {
				f := st.fact(pr.URL, "jsonld.person", 0.95)
				pf.Socials = append(pf.Socials, model.SocialFact{Fact: f, Network: pr.Network, Handle: pr.Handle, Scope: "person"})
			}
		}
	}
	st.people.add(pf)
}

func (st *state) ldAddress(v any, deref func(any) map[string]any) {
	if s, ok := v.(string); ok {
		if a, ok := parseAddressText(lines(strings.ReplaceAll(s, ",", "\n")), st.country); ok {
			a.Fact = st.fact(addrLine(a), "jsonld.address", 0.8)
			st.addAddress(a)
		}
		return
	}
	obj := deref(v)
	if obj == nil {
		return
	}
	a := model.AddressFact{
		Street:  ldString(obj["streetAddress"]),
		City:    ldString(obj["addressLocality"]),
		Region:  ldString(obj["addressRegion"]),
		Postal:  ldString(obj["postalCode"]),
		Country: ldString(obj["addressCountry"]),
	}
	if c := normalize.Country(a.Country); c != "" {
		a.Country = c
	}
	if a.Street == "" && a.City == "" && a.Postal == "" && a.Country == "" {
		return
	}
	a.Fact = st.fact(addrLine(a), "jsonld.address", 0.95)
	st.addAddress(a)
}

func ldEmployees(v any) string {
	switch t := v.(type) {
	case float64:
		return strconv.Itoa(int(t))
	case string:
		return collapse(t)
	case map[string]any:
		if val := ldString(t["value"]); val != "" {
			return val
		}
		lo, hi := ldString(t["minValue"]), ldString(t["maxValue"])
		switch {
		case lo != "" && hi != "":
			return lo + "-" + hi
		case lo != "":
			return lo + "+"
		case hi != "":
			return "<" + hi
		}
	}
	return ""
}

var yearRe = regexp.MustCompile(`\b(1[6-9]\d{2}|20\d{2})\b`)

func yearOf(s string) string { return yearRe.FindString(s) }

// clip truncates s to at most n bytes on a rune boundary.
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	for n > 0 && s[n]&0xC0 == 0x80 {
		n--
	}
	return strings.TrimSpace(s[:n])
}
