// Package resolve turns extractions into graph mutations: deterministic IDs,
// normalization, merge rules, person deduplication, suppression and country
// policy.
package resolve

import (
	"encoding/json"
	"strings"

	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

// Better reports whether candidate should replace current under the merge
// rule: strictly higher confidence wins; at equal confidence the newer fact
// wins. A lower-confidence fact never replaces a higher one.
func Better(candidate, current model.Scalar) bool {
	if current.Value == nil {
		return candidate.Value != nil
	}
	if candidate.Value == nil {
		return false
	}
	if candidate.Confidence != current.Confidence {
		return candidate.Confidence > current.Confidence
	}
	return !candidate.At.Before(current.At)
}

// Best picks the winning fact from a list under the merge rule. Ties on
// confidence and time keep the earlier list element, so the result does not
// depend on map iteration or goroutine timing.
func Best(facts []model.Fact, clean func(string) string) (model.Fact, bool) {
	var best model.Fact
	found := false
	for _, f := range facts {
		if clean != nil {
			f.Value = clean(f.Value)
		}
		if f.Value == "" {
			continue
		}
		if !found || f.Confidence > best.Confidence || (f.Confidence == best.Confidence && f.ExtractedAt.After(best.ExtractedAt)) {
			best, found = f, true
		}
	}
	return best, found
}

func scalar(f model.Fact) model.Scalar {
	return model.Scalar{Value: f.Value, Confidence: f.Confidence, At: f.ExtractedAt}
}

type factRecord struct {
	Field       string  `json:"field"`
	Value       string  `json:"value"`
	SourceURL   string  `json:"source_url"`
	Extractor   string  `json:"extractor"`
	Confidence  float64 `json:"confidence"`
	ExtractedAt string  `json:"extracted_at"`
}

// FactString serializes one fact for a node's `facts` list. The encoding is
// deterministic, so re-processing the same fetch adds nothing new.
func FactString(field string, f model.Fact) string {
	b, _ := json.Marshal(factRecord{
		Field: field, Value: f.Value, SourceURL: f.SourceURL, Extractor: f.Extractor,
		Confidence: f.Confidence, ExtractedAt: f.ExtractedAt.UTC().Format("2006-01-02T15:04:05Z"),
	})
	return string(b)
}

// companyFields maps the extraction's fact lists to Company scalar fields.
func companyFields(ex *model.Extraction) []struct {
	field string
	facts []model.Fact
	clean func(string) string
} {
	return []struct {
		field string
		facts []model.Fact
		clean func(string) string
	}{
		{"name", ex.CompanyNames, normalize.CompanyName},
		{"description", ex.Descriptions, normalize.Text},
		{"industry", ex.Industries, func(s string) string { return strings.ToLower(normalize.Text(s)) }},
		{"size_hint", ex.SizeHints, normalize.Text},
		{"founded", ex.Founded, normalize.Text},
	}
}

// Country evidence strength.
const (
	evidenceNone = iota
	evidencePhone
	evidenceTLD
	evidenceAddress
)

// CountryOf derives the company's country from one extraction: a postal
// address is the strongest evidence, then a country-code TLD, then phone
// numbers that all share one calling region.
func CountryOf(ex *model.Extraction) (iso string, strength int) {
	bestConf := -1.0
	for _, a := range ex.Addresses {
		if c := normalize.Country(a.Country); c != "" && a.Confidence > bestConf {
			iso, bestConf = c, a.Confidence
		}
	}
	if iso != "" {
		return iso, evidenceAddress
	}
	if c := normalize.CountryFromTLD(ex.Host); c != "" {
		return c, evidenceTLD
	}
	region := ""
	for _, p := range ex.Phones {
		r := phoneRegion(p.Value)
		if r == "" || (region != "" && r != region) {
			return "", evidenceNone
		}
		region = r
	}
	if region != "" {
		return region, evidencePhone
	}
	return "", evidenceNone
}

// emailLocalPatterns returns the mailbox names a person with this folded name
// would plausibly have, strongest patterns first. strong marks patterns that
// use both first and last name.
func emailLocalPatterns(nameKey string) (strong, weak []string) {
	t := strings.Fields(nameKey)
	if len(t) < 2 {
		return nil, nil
	}
	first := t[0]
	// "tom o brien" (O'Brien) and "maria von trapp" also write their surname
	// joined: obrien, vontrapp.
	lasts := []string{t[len(t)-1]}
	if len(t) >= 3 {
		lasts = append(lasts, t[len(t)-2]+t[len(t)-1])
	}
	fi := first[:1]
	for _, last := range lasts {
		li := last[:1]
		strong = append(strong,
			first+"."+last, first+"_"+last, first+"-"+last, first+last,
			fi+"."+last, fi+last, first+"."+li, first+li,
			last+"."+first, last+first, last+"."+fi, last+fi,
		)
		weak = append(weak, last)
	}
	weak = append(weak, first)
	return strong, weak
}

// asciiFold maps a folded name to the ASCII form used in mailbox names.
func asciiFold(s string) string {
	r := strings.NewReplacer("ß", "ss", "ø", "o", "æ", "ae", "đ", "d", "ł", "l", "å", "a")
	return r.Replace(s)
}

// MatchEmail finds which known person an address found on the site belongs
// to, by comparing its local part to name patterns. It never invents an
// address: it only links one that was actually published. The result is ""
// unless exactly one person matches.
func MatchEmail(local string, people map[string]string) (personID string, conf float64) {
	local = strings.ToLower(local)
	if i := strings.IndexByte(local, '+'); i > 0 {
		local = local[:i]
	}
	var strongHits, weakHits []string
	for id, nameKey := range people {
		strong, weak := emailLocalPatterns(asciiFold(nameKey))
		for _, p := range strong {
			if p == local {
				strongHits = append(strongHits, id)
				break
			}
		}
		for _, p := range weak {
			if p == local {
				weakHits = append(weakHits, id)
				break
			}
		}
	}
	switch {
	case len(strongHits) == 1:
		return strongHits[0], 0.7
	case len(strongHits) == 0 && len(weakHits) == 1:
		return weakHits[0], 0.5
	}
	return "", 0
}

// sameSite reports whether an email domain belongs to the crawled company.
func sameSite(emailDomain, companyDomain string) bool {
	return urlx.Registrable(emailDomain) == companyDomain
}
