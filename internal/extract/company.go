package extract

import (
	"regexp"
	"sort"
	"strings"

	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/pkg/model"
)

var (
	titleSep    = regexp.MustCompile(`\s+[|\-–—·•:»]\s+|\s*\|\s*`)
	copyrightRe = regexp.MustCompile(`(?i)(?:©|\(c\)|copyright)\s*(?:©\s*)?(?:(?:19|20)\d{2}(?:\s*[-–]\s*(?:19|20)\d{2})?)?[\s,]*([^\n©|]{2,60}?)\s*(?:[.|,]?\s*all rights|\.\s|\.$|\||$)`)
	leadingYear = regexp.MustCompile(`^(?:19|20)\d{2}(?:\s*[-–]\s*(?:19|20)\d{2})?[\s,]*`)
	employeesRe = regexp.MustCompile(`(?i)\b(?:over |more than |approximately |about |~)?(\d{1,3}(?:[,.]\d{3})+|\d{2,6})(\+)?\s+(?:employees|team members|staff members|professionals|people worldwide|mitarbeiter|mitarbeitende|collaborateurs|empleados)\b`)
	foundedRe   = regexp.MustCompile(`(?i)\b(founded|established|est\.|incorporated|gegründet|fondée?|since)\s+(?:in\s+|im jahr\s+|en\s+)?((?:18|19|20)\d{2})\b`)
)

// industries maps an industry label to keywords that indicate it.
var industries = []struct {
	name     string
	keywords []string
}{
	{"software", []string{"saas", "software", "platform", "api", "developer tools", "open source"}},
	{"fintech", []string{"fintech", "payments", "banking", "lending", "insurtech", "financial services"}},
	{"healthcare", []string{"healthcare", "health care", "medical", "clinical", "patients", "telehealth", "biotech", "pharma"}},
	{"ecommerce", []string{"e-commerce", "ecommerce", "online store", "marketplace", "retail"}},
	{"manufacturing", []string{"manufacturing", "industrial", "factory", "machinery", "robotics", "automation"}},
	{"logistics", []string{"logistics", "supply chain", "freight", "shipping", "warehouse", "fleet"}},
	{"marketing", []string{"marketing agency", "advertising", "seo", "branding", "digital agency", "public relations"}},
	{"consulting", []string{"consulting", "consultancy", "advisory", "professional services"}},
	{"cybersecurity", []string{"cybersecurity", "cyber security", "infosec", "threat detection", "zero trust"}},
	{"ai", []string{"artificial intelligence", "machine learning", "deep learning", "generative ai", "llm"}},
	{"education", []string{"education", "edtech", "e-learning", "students", "courses", "training"}},
	{"real estate", []string{"real estate", "proptech", "property management", "commercial property"}},
	{"energy", []string{"renewable", "solar", "energy", "cleantech", "utilities", "wind power"}},
	{"legal", []string{"law firm", "attorneys", "legal services", "litigation", "legaltech"}},
	{"construction", []string{"construction", "architecture", "contractor", "civil engineering"}},
	{"telecom", []string{"telecom", "telecommunications", "5g", "broadband", "connectivity"}},
	{"hr", []string{"recruiting", "recruitment", "staffing", "human resources", "hr software", "payroll"}},
	{"media", []string{"media", "publishing", "entertainment", "streaming", "broadcast"}},
	{"hospitality", []string{"hospitality", "hotel", "travel", "tourism", "restaurant"}},
	{"automotive", []string{"automotive", "vehicles", "electric vehicle", "mobility", "dealership"}},
}

func (st *state) extractCompany() {
	p, out := st.p, st.out
	homeish := out.PageType == model.PageHome || out.PageType == model.PageAbout

	if n := p.meta("og:site_name"); n != "" {
		out.CompanyNames = append(out.CompanyNames, st.fact(clip(n, 120), "meta.og:site_name", 0.8))
	}
	if n := p.meta("application-name", "apple-mobile-web-app-title"); n != "" {
		out.CompanyNames = append(out.CompanyNames, st.fact(clip(n, 120), "meta.application-name", 0.6))
	}
	if p.title != "" {
		segs := titleSep.Split(p.title, -1)
		pick, conf := "", 0.0
		for _, s := range segs {
			s = strings.TrimSpace(s)
			if s == "" {
				continue
			}
			if st.brand != "" && strings.Contains(strings.ReplaceAll(normalize.Fold(s), " ", ""), st.brand) && len(s) <= 60 {
				if pick == "" || len(s) < len(pick) {
					pick, conf = s, 0.7
				}
			}
		}
		if pick == "" && len(segs) > 0 {
			if out.PageType == model.PageHome {
				pick = strings.TrimSpace(segs[0])
			} else if len(segs) > 1 {
				pick = strings.TrimSpace(segs[len(segs)-1])
			}
			conf = 0.4
		}
		if pick != "" && len(pick) <= 80 {
			out.CompanyNames = append(out.CompanyNames, st.fact(pick, "title", conf))
		}
	}
	// Copyright line, searched in the footer when there is one.
	tail := p.text
	if p.footerStart >= 0 && p.footerStart < len(tail) {
		tail = tail[p.footerStart:]
	} else if len(tail) > 3000 {
		tail = tail[len(tail)-3000:]
		for len(tail) > 0 && tail[0]&0xC0 == 0x80 {
			tail = tail[1:]
		}
	}
	if m := copyrightRe.FindStringSubmatch(tail); m != nil {
		name := strings.Trim(leadingYear.ReplaceAllString(strings.TrimSpace(m[1]), ""), " ,.-–")
		if len(name) >= 2 && !strings.EqualFold(name, "all rights reserved") && !strings.HasPrefix(strings.ToLower(name), "all rights") {
			out.CompanyNames = append(out.CompanyNames, st.fact(name, "copyright", 0.6))
		}
	}

	descConf := 0.3
	switch out.PageType {
	case model.PageHome:
		descConf = 0.7
	case model.PageAbout:
		descConf = 0.6
	}
	if d := p.meta("og:description"); d != "" {
		out.Descriptions = append(out.Descriptions, st.fact(clip(d, 1000), "meta.og:description", descConf))
	}
	if d := p.meta("description"); d != "" {
		out.Descriptions = append(out.Descriptions, st.fact(clip(d, 1000), "meta.description", descConf))
	}

	if !homeish && out.PageType != model.PageCareers {
		return
	}
	head := p.text
	if len(head) > 6000 {
		head = head[:6000]
	}
	if m := employeesRe.FindStringSubmatch(head); m != nil {
		n := strings.NewReplacer(",", "", ".", "").Replace(m[1]) + m[2]
		out.SizeHints = append(out.SizeHints, st.fact(n, "text.employees", 0.5))
	}
	if m := foundedRe.FindStringSubmatch(head); m != nil {
		conf := 0.6
		if strings.EqualFold(m[1], "since") {
			conf = 0.4
		}
		out.Founded = append(out.Founded, st.fact(m[2], "text.founded", conf))
	}
	if !homeish {
		return
	}
	corpus := strings.ToLower(p.title + " " + p.meta("description", "og:description") + " " + p.meta("keywords") + " " + clip(head, 4000))
	type score struct {
		name string
		n    int
	}
	var scores []score
	for _, ind := range industries {
		n := 0
		for _, kw := range ind.keywords {
			n += strings.Count(corpus, kw)
		}
		if n >= 2 {
			scores = append(scores, score{ind.name, n})
		}
	}
	sort.SliceStable(scores, func(i, j int) bool { return scores[i].n > scores[j].n })
	for i, s := range scores {
		if i == 3 {
			break
		}
		conf := 0.4 + 0.05*float64(min(s.n, 6))
		out.Industries = append(out.Industries, st.fact(s.name, "keywords", conf))
	}
}
