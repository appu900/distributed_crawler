package extract

import (
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"

	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/pkg/model"
)

var (
	usCityStateZip = regexp.MustCompile(`^(.*?)[,\s]+([A-Z]{2})[,\s]+(\d{5}(?:-\d{4})?)$`)
	euPostalCity   = regexp.MustCompile(`^(?:[A-Z]{1,2}-)?(\d{4,5})\s+(\p{Lu}[\p{L}\-. ]{1,40})$`)
	ukPostcode     = regexp.MustCompile(`\b([A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2})$`)
	caPostal       = regexp.MustCompile(`^(.*?)[,\s]+([A-Z]{2})[,\s]+([A-Z]\d[A-Z]\s?\d[A-Z]\d)$`)
	streetish      = regexp.MustCompile(`\d`)
)

var usStates = toSet(`AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT
NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC PR`)

var caProvinces = toSet(`AB BC MB NB NL NS NT NU ON PE QC SK YT`)

// addrLine renders an address on one line.
func addrLine(a model.AddressFact) string {
	parts := make([]string, 0, 5)
	for _, s := range []string{a.Street, a.City, a.Region, a.Postal, a.Country} {
		if s != "" {
			parts = append(parts, s)
		}
	}
	return strings.Join(parts, ", ")
}

// parseAddressText interprets the lines of a postal address block. It
// recognizes US, Canadian, UK and continental "postal city" layouts and a
// trailing country line. ok is false when no city or postal code was found.
func parseAddressText(ls []string, defaultCountry string) (model.AddressFact, bool) {
	var a model.AddressFact
	if len(ls) == 0 || len(ls) > 8 {
		return a, false
	}
	rest := make([]string, 0, len(ls))
	for _, l := range ls {
		l = strings.Trim(l, " ,;")
		if l == "" || strings.ContainsRune(l, '@') || len(l) > 120 {
			continue
		}
		if c := normalize.Country(l); c != "" && a.Country == "" && len(l) > 2 {
			a.Country = c
			continue
		}
		rest = append(rest, l)
	}
	for i := len(rest) - 1; i >= 0 && a.Postal == ""; i-- {
		l := rest[i]
		if m := usCityStateZip.FindStringSubmatch(l); m != nil {
			if _, ok := usStates[m[2]]; ok {
				a.City, a.Region, a.Postal = strings.Trim(m[1], " ,"), m[2], m[3]
				if a.Country == "" {
					a.Country = "US"
				}
				rest = append(rest[:i], rest[i+1:]...)
				break
			}
		}
		if m := caPostal.FindStringSubmatch(l); m != nil {
			if _, ok := caProvinces[m[2]]; ok {
				a.City, a.Region, a.Postal = strings.Trim(m[1], " ,"), m[2], m[3]
				if a.Country == "" {
					a.Country = "CA"
				}
				rest = append(rest[:i], rest[i+1:]...)
				break
			}
		}
		if m := ukPostcode.FindStringSubmatch(l); m != nil {
			a.Postal = m[1]
			a.City = strings.Trim(strings.TrimSuffix(l, m[1]), " ,")
			if a.Country == "" {
				a.Country = "GB"
			}
			rest = append(rest[:i], rest[i+1:]...)
			if a.City == "" && i > 0 && !streetish.MatchString(rest[i-1]) {
				a.City = rest[i-1]
				rest = rest[:i-1]
			}
			break
		}
		if m := euPostalCity.FindStringSubmatch(l); m != nil {
			a.Postal, a.City = m[1], strings.TrimSpace(m[2])
			rest = append(rest[:i], rest[i+1:]...)
			break
		}
	}
	if a.Postal == "" && a.City == "" {
		return a, false
	}
	for _, l := range rest {
		if streetish.MatchString(l) && !looksLikePhoneLine(l) {
			a.Street = l
			break
		}
	}
	if a.Country == "" {
		a.Country = defaultCountry
	}
	return a, true
}

func looksLikePhoneLine(l string) bool {
	digits := 0
	for i := 0; i < len(l); i++ {
		if isDigit(l[i]) {
			digits++
		}
	}
	low := strings.ToLower(l)
	return digits >= 7 && (strings.HasPrefix(l, "+") || strings.Contains(low, "tel") || strings.Contains(low, "phone") || strings.Contains(low, "fax"))
}

func (st *state) addAddress(a model.AddressFact) {
	key := normalize.Fold(a.Street + "|" + a.City + "|" + a.Postal + "|" + a.Country)
	for i, prev := range st.out.Addresses {
		if normalize.Fold(prev.Street+"|"+prev.City+"|"+prev.Postal+"|"+prev.Country) == key {
			if a.Confidence > prev.Confidence {
				st.out.Addresses[i] = a
			}
			return
		}
	}
	if len(st.out.Addresses) < 20 {
		st.out.Addresses = append(st.out.Addresses, a)
	}
}

var microAddr = "[itemtype*='PostalAddress']"

func (st *state) extractAddresses(doc *goquery.Document) {
	// schema.org microdata.
	doc.Find(microAddr).Each(func(_ int, s *goquery.Selection) {
		prop := func(name string) string {
			el := s.Find("[itemprop='" + name + "']").First()
			if c, ok := el.Attr("content"); ok && c != "" {
				return collapse(c)
			}
			return collapse(el.Text())
		}
		a := model.AddressFact{
			Street: prop("streetAddress"), City: prop("addressLocality"), Region: prop("addressRegion"),
			Postal: prop("postalCode"), Country: prop("addressCountry"),
		}
		if c := normalize.Country(a.Country); c != "" {
			a.Country = c
		}
		if a.City == "" && a.Postal == "" {
			return
		}
		a.Fact = st.fact(addrLine(a), "microdata.address", 0.85)
		st.addAddress(a)
	})
	// <address> elements.
	for _, n := range st.p.addresses {
		if isMicrodata(n) {
			continue
		}
		if a, ok := parseAddressText(lines(textOf(n)), st.country); ok {
			a.Fact = st.fact(addrLine(a), "address.tag", 0.6)
			st.addAddress(a)
		}
	}
}

func isMicrodata(n *html.Node) bool {
	return strings.Contains(attr(n, "itemtype"), "PostalAddress")
}
