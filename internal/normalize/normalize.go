// Package normalize holds text normalization used for entity resolution.
package normalize

import (
	"strings"
	"unicode"

	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
)

// Text applies Unicode NFKC, replaces every whitespace run (including
// non-breaking and zero-width spaces) with one space, and trims.
func Text(s string) string {
	s = norm.NFKC.String(s)
	var sb strings.Builder
	sb.Grow(len(s))
	space := false
	for _, r := range s {
		switch {
		case r == 0x200B || r == 0x200C || r == 0x200D || r == 0xFEFF || r == 0x00AD:
			// zero-width and soft-hyphen characters carry no meaning
		case unicode.IsSpace(r) || unicode.IsControl(r):
			space = true
		default:
			if space && sb.Len() > 0 {
				sb.WriteByte(' ')
			}
			space = false
			sb.WriteRune(r)
		}
	}
	return sb.String()
}

var stripMarks = runes.Remove(runes.In(unicode.Mn))

// Fold returns a comparison key: NFKC, lowercased, diacritics removed,
// punctuation dropped and whitespace collapsed.
func Fold(s string) string {
	s = Text(s)
	if d, _, err := transform.String(transform.Chain(norm.NFD, stripMarks, norm.NFC), s); err == nil {
		s = d
	}
	var sb strings.Builder
	space := false
	for _, r := range strings.ToLower(s) {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			if space && sb.Len() > 0 {
				sb.WriteByte(' ')
			}
			space = false
			sb.WriteRune(r)
		} else {
			space = true
		}
	}
	return sb.String()
}

// legalSuffixes are company-form designators, compared after Fold.
var legalSuffixes = map[string]struct{}{
	"inc": {}, "incorporated": {}, "corp": {}, "corporation": {}, "co": {}, "company": {},
	"ltd": {}, "limited": {}, "llc": {}, "llp": {}, "lp": {}, "plc": {}, "pllc": {}, "pc": {},
	"gmbh": {}, "ag": {}, "kg": {}, "ug": {}, "ohg": {}, "ev": {}, "se": {}, "kgaa": {},
	"sa": {}, "sas": {}, "sarl": {}, "srl": {}, "spa": {}, "sl": {}, "slu": {}, "snc": {},
	"bv": {}, "nv": {}, "vof": {}, "ab": {}, "as": {}, "asa": {}, "oy": {}, "oyj": {}, "aps": {},
	"pty": {}, "pvt": {}, "private": {}, "pte": {}, "sdn": {}, "bhd": {}, "kk": {}, "kft": {},
	"sro": {}, "doo": {}, "ooo": {}, "zoo": {}, "sp": {}, "lda": {}, "ltda": {}, "sae": {},
	"holdings": {}, "holding": {}, "group": {},
}

// multiWordSuffixes are folded suffix phrases removed as a unit.
var multiWordSuffixes = []string{
	"gmbh co kg", "pvt ltd", "private limited", "pty ltd", "pte ltd", "sdn bhd",
	"sp z o o", "s a r l", "s r l", "s p a", "s a", "s l", "b v", "n v", "l l c", "co ltd",
	"s r o", "d o o", "a s", "e v",
}

// CompanyName returns the display form of a company name with legal-form
// suffixes removed ("Acme Inc." → "Acme", "Foo GmbH & Co. KG" → "Foo").
func CompanyName(s string) string {
	s = Text(s)
	s = strings.Trim(s, " ,.;:-–—|")
	for {
		trimmed := stripOneSuffix(s)
		if trimmed == s || trimmed == "" {
			break
		}
		s = trimmed
	}
	return s
}

func stripOneSuffix(s string) string {
	folded := Fold(s)
	for _, mw := range multiWordSuffixes {
		if strings.HasSuffix(folded, " "+mw) {
			return cutTrailingWords(s, strings.Count(mw, " ")+1)
		}
	}
	i := strings.LastIndexAny(folded, " ")
	if i < 0 {
		return s
	}
	if _, ok := legalSuffixes[folded[i+1:]]; ok {
		return cutTrailingWords(s, 1)
	}
	return s
}

// cutTrailingWords removes the last n alphanumeric words (and the punctuation
// around them) from s.
func cutTrailingWords(s string, n int) string {
	r := []rune(s)
	i := len(r)
	for ; n > 0; n-- {
		for i > 0 && !isWordRune(r[i-1]) {
			i--
		}
		for i > 0 && isWordRune(r[i-1]) {
			i--
		}
	}
	return strings.TrimRight(string(r[:i]), " ,.;:-–—|&+")
}

func isWordRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }

// CompanyKey is the comparison key for a company name.
func CompanyKey(s string) string { return Fold(CompanyName(s)) }

var honorifics = map[string]struct{}{
	"mr": {}, "mrs": {}, "ms": {}, "miss": {}, "mx": {}, "dr": {}, "prof": {}, "professor": {},
	"sir": {}, "dame": {}, "herr": {}, "frau": {}, "ing": {}, "dipl": {},
}

var nameSuffixes = map[string]struct{}{
	"phd": {}, "md": {}, "mba": {}, "cpa": {}, "cfa": {}, "esq": {}, "jr": {}, "sr": {},
	"ii": {}, "iii": {}, "iv": {}, "pe": {}, "msc": {}, "bsc": {}, "ma": {}, "jd": {},
}

var lowerParticles = map[string]struct{}{
	"van": {}, "von": {}, "de": {}, "der": {}, "den": {}, "del": {}, "della": {}, "di": {},
	"da": {}, "dos": {}, "das": {}, "la": {}, "le": {}, "du": {}, "bin": {}, "al": {}, "ter": {},
	"ten": {}, "y": {}, "e": {}, "zu": {},
}

// PersonName returns a cleaned display name: honorifics and credential
// suffixes removed, casing repaired when the input is all upper or all lower.
func PersonName(s string) string {
	s = Text(s)
	if i := strings.IndexAny(s, ",("); i > 0 {
		// "Jane Doe, PhD" / "Jane Doe (she/her)"
		rest := Fold(s[i+1:])
		first := rest
		if j := strings.IndexByte(rest, ' '); j > 0 {
			first = rest[:j]
		}
		if _, ok := nameSuffixes[first]; ok || s[i] == '(' {
			s = strings.TrimSpace(s[:i])
		}
	}
	words := strings.Fields(s)
	for len(words) > 1 {
		if _, ok := honorifics[strings.ToLower(strings.TrimRight(words[0], "."))]; !ok {
			break
		}
		words = words[1:]
	}
	for len(words) > 2 {
		last := strings.ToLower(strings.ReplaceAll(strings.Trim(words[len(words)-1], ".,"), ".", ""))
		if _, ok := nameSuffixes[last]; !ok {
			break
		}
		words = words[:len(words)-1]
	}
	joined := strings.Join(words, " ")
	if joined == strings.ToUpper(joined) || joined == strings.ToLower(joined) {
		for i, w := range words {
			words[i] = titleWord(w, i > 0 && i < len(words)-1)
		}
		joined = strings.Join(words, " ")
	}
	return strings.Trim(joined, " ,;:-–—|")
}

func titleWord(w string, middle bool) string {
	lw := strings.ToLower(w)
	if _, ok := lowerParticles[lw]; ok && middle {
		return lw
	}
	// Capitalize after apostrophes and hyphens: o'brien → O'Brien.
	out := []rune(lw)
	up := true
	for i, r := range out {
		if up && unicode.IsLetter(r) {
			out[i] = unicode.ToUpper(r)
			up = false
		}
		if r == '-' || r == '\'' || r == '’' {
			up = true
		}
	}
	s := string(out)
	if strings.HasPrefix(s, "Mc") && len(out) > 2 {
		out[2] = unicode.ToUpper(out[2])
		s = string(out)
	}
	return s
}

// PersonKey is the comparison key for a person name.
func PersonKey(s string) string { return Fold(PersonName(s)) }

// Title normalizes a job title for display.
func Title(s string) string {
	s = Text(s)
	s = strings.Trim(s, " ,;:-–—|/")
	if len(s) > 160 {
		s = s[:160]
	}
	return s
}

// TitleKey is the comparison key for a job title.
func TitleKey(s string) string { return Fold(Title(s)) }

// Seniority maps a job title to a coarse seniority band.
func Seniority(title string) string {
	t := " " + Fold(title) + " "
	has := func(subs ...string) bool {
		for _, s := range subs {
			if strings.Contains(t, " "+s+" ") {
				return true
			}
		}
		return false
	}
	switch {
	case title == "":
		return ""
	case has("founder", "cofounder", "co founder", "owner", "proprietor", "gründer", "inhaber"):
		return "founder"
	case has("ceo", "cto", "cfo", "coo", "cmo", "cio", "cso", "cpo", "cro", "ciso", "chro",
		"chief", "president", "managing director", "geschäftsführer", "geschaftsfuhrer",
		"general manager", "chairman", "chairwoman", "chair", "partner", "managing partner",
		"directeur général", "directeur general", "pdg"):
		if has("vice president") {
			return "vp"
		}
		return "c_level"
	case has("vp", "svp", "evp", "avp", "vice president"):
		return "vp"
	case has("director", "head", "leiter", "directeur", "directrice"):
		return "director"
	case has("manager", "lead", "principal", "supervisor", "team lead"):
		return "manager"
	case has("intern", "trainee", "apprentice", "working student", "werkstudent"):
		return "entry"
	default:
		return "individual"
	}
}
