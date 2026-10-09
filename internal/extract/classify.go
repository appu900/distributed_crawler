package extract

import (
	"net/url"
	"strings"

	"github.com/appu/leadgraph/pkg/model"
)

var pathClasses = []struct {
	typ   model.PageType
	words []string
}{
	{model.PageTeam, []string{"team", "people", "leadership", "management", "staff", "board", "founders", "executives", "our-team", "meet-the-team", "equipe", "mitarbeiter"}},
	{model.PageContact, []string{"contact", "kontakt", "impressum", "imprint", "locations", "offices", "get-in-touch", "contacto", "contatti", "legal-notice"}},
	{model.PageCareers, []string{"careers", "career", "jobs", "join-us", "work-with-us", "karriere", "hiring", "vacancies", "stellenangebote"}},
	{model.PageAbout, []string{"about", "company", "who-we-are", "our-story", "mission", "ueber-uns", "uber-uns", "a-propos", "unternehmen", "sobre", "chi-siamo"}},
}

// WellKnownPaths are probed for every seed in addition to its homepage.
var WellKnownPaths = []string{"/about", "/team", "/contact", "/people", "/leadership", "/company", "/careers", "/impressum"}

// ClassifyPath classifies a URL path by its segments.
func ClassifyPath(path string) model.PageType {
	p := strings.ToLower(strings.Trim(path, "/"))
	if p == "" || p == "index.html" || p == "index.php" || p == "index.htm" || p == "home" {
		return model.PageHome
	}
	segs := strings.Split(p, "/")
	if len(segs) == 1 && len(segs[0]) <= 5 && (len(segs[0]) == 2 || (len(segs[0]) == 5 && segs[0][2] == '-')) {
		return model.PageHome // language root: /en, /de-de
	}
	if len(segs) > 3 {
		segs = segs[:3]
	}
	for _, seg := range segs {
		seg = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSuffix(seg, ".html"), ".php"), ".htm")
		for _, pc := range pathClasses {
			for _, w := range pc.words {
				if seg == w || strings.HasPrefix(seg, w+"-") || strings.HasSuffix(seg, "-"+w) || seg == w+"s" {
					return pc.typ
				}
			}
		}
	}
	return model.PageOther
}

// ClassifyURL classifies a raw URL by its path.
func ClassifyURL(raw string) model.PageType {
	u, err := url.Parse(raw)
	if err != nil {
		return model.PageOther
	}
	return ClassifyPath(u.Path)
}

// classify refines the path-based class with the title and first heading.
func classify(u *url.URL, title, h1 string) model.PageType {
	if t := ClassifyPath(u.Path); t != model.PageOther {
		return t
	}
	text := strings.ToLower(h1 + " | " + title)
	switch {
	case strings.Contains(text, "our team") || strings.Contains(text, "leadership") || strings.Contains(text, "meet the team") || strings.Contains(text, "management team"):
		return model.PageTeam
	case strings.Contains(text, "contact") || strings.Contains(text, "impressum") || strings.Contains(text, "get in touch"):
		return model.PageContact
	case strings.Contains(text, "careers") || strings.Contains(text, "open positions") || strings.Contains(text, "join our team"):
		return model.PageCareers
	case strings.Contains(text, "about us") || strings.Contains(text, "who we are") || strings.Contains(text, "our story"):
		return model.PageAbout
	}
	return model.PageOther
}
