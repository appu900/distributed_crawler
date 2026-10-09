package extract

import (
	"net/url"
	"strings"

	"github.com/nyaruka/phonenumbers"

	"github.com/appu/leadgraph/pkg/model"
)

// NormalizePhone parses raw in the given default region and returns the
// E.164 form and a coarse line type. ok is false for invalid numbers.
func NormalizePhone(raw, region string) (e164, kind string, ok bool) {
	if region == "" {
		region = "US"
	}
	n, err := phonenumbers.Parse(raw, region)
	if err != nil || !phonenumbers.IsValidNumber(n) {
		return "", "", false
	}
	switch phonenumbers.GetNumberType(n) {
	case phonenumbers.MOBILE:
		kind = "mobile"
	case phonenumbers.FIXED_LINE:
		kind = "landline"
	case phonenumbers.FIXED_LINE_OR_MOBILE:
		kind = "landline_or_mobile"
	case phonenumbers.TOLL_FREE:
		kind = "toll_free"
	case phonenumbers.VOIP:
		kind = "voip"
	default:
		kind = "other"
	}
	return phonenumbers.Format(n, phonenumbers.E164), kind, true
}

type phoneSink struct {
	region string
	best   map[string]model.PhoneFact
	list   []string
	now    func() model.Fact
}

func (s *phoneSink) add(raw, extractor string, conf float64, fax bool) string {
	e164, kind, ok := NormalizePhone(raw, s.region)
	if !ok {
		return ""
	}
	if fax {
		kind = "fax"
	}
	if prev, seen := s.best[e164]; seen {
		if prev.Confidence >= conf {
			return e164
		}
	} else {
		s.list = append(s.list, e164)
	}
	f := s.now()
	f.Value, f.Extractor, f.Confidence = e164, extractor, conf
	s.best[e164] = model.PhoneFact{Fact: f, Kind: kind, Raw: collapse(raw)}
	return e164
}

func (s *phoneSink) facts() []model.PhoneFact {
	out := make([]model.PhoneFact, 0, len(s.list))
	for _, k := range s.list {
		out = append(out, s.best[k])
	}
	return out
}

func telNumber(href string) string {
	rest := href[len("tel:"):]
	if i := strings.IndexAny(rest, ";?"); i >= 0 {
		rest = rest[:i]
	}
	if un, err := url.PathUnescape(rest); err == nil {
		rest = un
	}
	return rest
}

var phoneWords = []string{"tel", "phone", "call", "mobile", "cell", "fax", "telefon", "tél", "téléphone",
	"teléfono", "telefono", "fon", "mobil", "hotline", "whatsapp", "t:", "p:", "m:", "f:", "☎", "📞", "handy"}

// phoneContext inspects the text just before a candidate.
func phoneContext(before string) (keyword, fax bool) {
	if len(before) > 28 {
		before = before[len(before)-28:]
	}
	b := strings.ToLower(before)
	for _, w := range phoneWords {
		for from := 0; ; {
			i := strings.Index(b[from:], w)
			if i < 0 {
				break
			}
			i += from
			end := i + len(w)
			// Whole words only: "Bestellnummer" must not match "tel".
			if (i == 0 || !isAlpha(b[i-1])) && (end == len(b) || !isAlpha(b[end])) {
				keyword = true
				break
			}
			from = end
		}
		if keyword {
			break
		}
	}
	tail := b
	if len(tail) > 14 {
		tail = tail[len(tail)-14:]
	}
	fax = strings.Contains(tail, "fax") || strings.HasSuffix(strings.TrimRight(tail, " "), "f:")
	return keyword, fax
}

func (p *page) extractPhones(s *phoneSink) {
	for _, a := range p.anchors {
		if hasPrefixFold(a.href, "tel:") {
			s.add(telNumber(a.href), "tel", 0.9, false)
		} else if hasPrefixFold(a.href, "fax:") {
			s.add(a.href[4:], "tel", 0.9, true)
		}
	}
	textPhones(p.text, s)
}

// maxPhoneCandidates bounds number parsing on hostile pages.
const maxPhoneCandidates = 500

// textPhones scans visible text for digit runs shaped like phone numbers.
// A number without a leading '+' is only trusted when a phone keyword
// precedes it, which keeps dates, prices and IDs out.
func textPhones(text string, s *phoneSink) {
	parsed := 0
	for i := 0; i < len(text) && parsed < maxPhoneCandidates; i++ {
		c := text[i]
		if !(c == '+' || c == '(' || isDigit(c)) {
			continue
		}
		if i > 0 && (isDigit(text[i-1]) || isAlpha(text[i-1]) || text[i-1] == '.' || text[i-1] == '#' || text[i-1] == '/' || text[i-1] == '-') {
			continue
		}
		j, digits, lastDigit := i, 0, i
		for j < len(text) && j-i < 28 {
			d := text[j]
			if isDigit(d) {
				digits++
				lastDigit = j
			} else if !(d == ' ' || d == '-' || d == '.' || d == '(' || d == ')' || d == '/' || (d == '+' && j == i)) {
				break
			}
			j++
		}
		end := lastDigit + 1
		if digits < 7 || digits > 15 || (end < len(text) && isAlpha(text[end])) {
			i = max(i, end-1)
			continue
		}
		cand := text[i:end]
		plus := cand[0] == '+'
		keyword, fax := phoneContext(text[:i])
		if (plus || keyword) && !looksLikeDate(cand) {
			conf := 0.7
			if plus && keyword {
				conf = 0.8
			}
			parsed++
			s.add(cand, "text", conf, fax)
		}
		i = end - 1
	}
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }
func isAlpha(c byte) bool { return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' }

// looksLikeDate rejects dd.mm.yyyy, yyyy-mm-dd and year ranges.
func looksLikeDate(s string) bool {
	parts := strings.FieldsFunc(s, func(r rune) bool { return r == '.' || r == '-' || r == '/' || r == ' ' })
	if len(parts) == 3 {
		l0, l1, l2 := len(parts[0]), len(parts[1]), len(parts[2])
		if l0 <= 2 && l1 <= 2 && (l2 == 4 || l2 == 2) {
			return true
		}
		if l0 == 4 && l1 <= 2 && l2 <= 2 {
			return true
		}
	}
	if len(parts) == 2 && len(parts[0]) == 4 && len(parts[1]) == 4 {
		return true
	}
	return false
}
