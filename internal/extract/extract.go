// Package extract turns one HTML document into a model.Extraction. Everything
// here is a pure function of its input: no network, no clock, no globals that
// change. That is what makes re-processing a stored page reproducible.
package extract

import (
	"bytes"
	"net/url"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"

	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

// Input is one fetched page.
type Input struct {
	// URL is the final (post-redirect) normalized URL.
	URL string
	// Body is the document, already decoded to UTF-8.
	Body    []byte
	Headers map[string][]string
	Status  int
	Depth   int
	// FetchedAt becomes extracted_at on every fact, so re-parsing the same
	// fetch yields identical output.
	FetchedAt time.Time
	// DefaultRegion is the ISO country used to parse national phone numbers.
	// Empty derives it from the ccTLD, falling back to FallbackRegion.
	DefaultRegion  string
	FallbackRegion string
	// Rules is the technology rule set; nil uses the built-in one.
	Rules *TechRules
	Email EmailOptions
}

type state struct {
	p            *page
	in           Input
	out          *model.Extraction
	emails       *emailSink
	phones       *phoneSink
	socials      *socialSink
	people       *peopleSink
	personSocial map[string]bool
	// country is the ISO country implied by the ccTLD, if any.
	country string
	// brand is the folded registrable-domain label ("acme" for acme.com).
	brand string
}

func (st *state) fact(value, extractor string, conf float64) model.Fact {
	return model.Fact{Value: value, SourceURL: st.out.URL, Extractor: extractor, Confidence: conf, ExtractedAt: st.in.FetchedAt}
}

// skipExt lists path extensions that are never HTML pages.
var skipExt = toSet(`.pdf .jpg .jpeg .png .gif .svg .webp .ico .css .js .json .zip .gz .tar .rar .7z
.doc .docx .xls .xlsx .ppt .pptx .mp3 .mp4 .avi .mov .wmv .webm .woff .woff2 .ttf .eot .exe .dmg
.apk .iso .csv .rss .atom .ics .vcf .txt .xml .bmp .tiff .psd .ai .eps`)

// Page extracts everything from one document. It never panics on malformed
// HTML and always returns a non-nil result.
func Page(in Input) *model.Extraction {
	out := &model.Extraction{URL: in.URL, Status: in.Status, Depth: in.Depth, FetchedAt: in.FetchedAt, PageType: model.PageOther}
	u, err := url.Parse(in.URL)
	if err != nil || u.Host == "" {
		return out
	}
	out.Host = u.Hostname()
	out.Domain = urlx.Registrable(out.Host)
	out.PageType = ClassifyPath(u.Path)

	root, err := html.Parse(bytes.NewReader(in.Body))
	if err != nil {
		return out
	}
	p := walk(in.Body, root, u)
	p.domain = out.Domain
	out.Title = clip(p.title, 300)
	out.Lang = p.lang
	out.PageType = classify(u, p.title, p.h1)

	robots := strings.ToLower(p.meta("robots") + "," + p.meta("leadgraphbot") + "," + strings.Join(in.Headers["X-Robots-Tag"], ","))
	out.NoIndex = strings.Contains(robots, "noindex") || strings.Contains(robots, "none")
	out.NoFollow = strings.Contains(robots, "nofollow") || strings.Contains(robots, "none")

	if !out.NoFollow {
		extractLinks(p, out)
	}
	if out.NoIndex {
		// The site asked not to be indexed: keep the page record, take no facts.
		return out
	}

	region := in.DefaultRegion
	country := normalize.CountryFromTLD(out.Host)
	if region == "" {
		region = country
	}
	if region == "" {
		region = in.FallbackRegion
	}
	if region == "" {
		region = "US"
	}
	st := &state{p: p, in: in, out: out, country: country, personSocial: map[string]bool{}}
	label := out.Domain
	if i := strings.IndexByte(label, '.'); i > 0 {
		label = label[:i]
	}
	st.brand = strings.ReplaceAll(normalize.Fold(label), " ", "")
	now := func() model.Fact {
		return model.Fact{SourceURL: out.URL, ExtractedAt: in.FetchedAt}
	}
	st.emails = &emailSink{opt: in.Email, best: map[string]model.EmailFact{}, now: now}
	st.phones = &phoneSink{region: region, best: map[string]model.PhoneFact{}, now: now}
	st.socials = &socialSink{best: map[string]model.SocialFact{}, now: now}
	st.people = &peopleSink{byKey: map[string]int{}}

	doc := goquery.NewDocumentFromNode(root)
	st.extractJSONLD()
	st.extractCompany()
	st.extractAddresses(doc)
	st.extractPeople()
	p.extractEmails(st.emails)
	p.extractPhones(st.phones)
	for _, a := range p.anchors {
		if maybeSocial(a.href) && !st.personSocial[canonicalOrEmpty(a.href)] {
			st.socials.add(a.href, "link", 0.85, "company")
		}
	}
	rules := in.Rules
	if rules == nil {
		rules = DefaultTechRules
	}
	st.extractTech(rules)

	out.Emails = capSlice(st.emails.facts(), 200)
	out.Phones = capSlice(st.phones.facts(), 100)
	out.People = st.people.list
	// Profiles that belong to a person are reported on that person only.
	for _, s := range st.socials.facts() {
		if s.Scope == "person" || st.personSocial[s.Value] {
			continue
		}
		out.Socials = append(out.Socials, s)
	}
	out.Socials = capSlice(out.Socials, 50)
	return out
}

func canonicalOrEmpty(href string) string {
	if pr, ok := CanonicalSocial(href); ok {
		return pr.URL
	}
	return ""
}

func capSlice[T any](s []T, n int) []T {
	if len(s) > n {
		return s[:n]
	}
	return s
}

const (
	maxLinks    = 500
	maxOutbound = 200
)

func extractLinks(p *page, out *model.Extraction) {
	seen := map[string]struct{}{}
	outbound := map[string]struct{}{}
	for _, a := range p.anchors {
		href := a.href
		if href[0] == '#' || strings.Contains(a.rel, "nofollow") {
			continue
		}
		if i := strings.IndexByte(href, ':'); i > 0 && i < 12 {
			scheme := strings.ToLower(href[:i])
			if scheme != "http" && scheme != "https" && !strings.ContainsAny(scheme, "/?#") {
				continue
			}
		}
		norm, err := urlx.Normalize(href, p.base)
		if err != nil {
			continue
		}
		u, err := url.Parse(norm)
		if err != nil {
			continue
		}
		domain := urlx.Registrable(u.Hostname())
		if domain != p.domain {
			if len(outbound) < maxOutbound {
				if _, ok := outbound[domain]; !ok {
					outbound[domain] = struct{}{}
					out.OutboundDomains = append(out.OutboundDomains, domain)
				}
			}
			continue
		}
		if i := strings.LastIndexByte(u.Path, '.'); i >= 0 {
			if _, skip := skipExt[strings.ToLower(u.Path[i:])]; skip {
				continue
			}
		}
		if _, dup := seen[norm]; dup || norm == out.URL || len(out.Links) >= maxLinks {
			continue
		}
		seen[norm] = struct{}{}
		out.Links = append(out.Links, model.Link{URL: norm})
	}
}
