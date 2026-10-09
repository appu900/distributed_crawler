package normalize

import "strings"

// countryNames maps folded country names and common aliases to ISO 3166-1
// alpha-2 codes. It covers the countries most often seen on company sites; an
// unknown name yields "".
var countryNames = map[string]string{
	"united states": "US", "united states of america": "US", "usa": "US", "u s a": "US", "u s": "US", "america": "US",
	"united kingdom": "GB", "uk": "GB", "u k": "GB", "great britain": "GB", "england": "GB", "scotland": "GB", "wales": "GB", "northern ireland": "GB",
	"germany": "DE", "deutschland": "DE", "france": "FR", "spain": "ES", "espana": "ES", "italy": "IT", "italia": "IT",
	"netherlands": "NL", "the netherlands": "NL", "nederland": "NL", "holland": "NL", "belgium": "BE", "belgique": "BE", "belgie": "BE",
	"switzerland": "CH", "schweiz": "CH", "suisse": "CH", "austria": "AT", "osterreich": "AT", "ireland": "IE",
	"portugal": "PT", "sweden": "SE", "sverige": "SE", "norway": "NO", "norge": "NO", "denmark": "DK", "danmark": "DK",
	"finland": "FI", "suomi": "FI", "poland": "PL", "polska": "PL", "czech republic": "CZ", "czechia": "CZ",
	"hungary": "HU", "romania": "RO", "bulgaria": "BG", "greece": "GR", "turkey": "TR", "turkiye": "TR",
	"russia": "RU", "russian federation": "RU", "ukraine": "UA", "estonia": "EE", "latvia": "LV", "lithuania": "LT",
	"luxembourg": "LU", "slovakia": "SK", "slovenia": "SI", "croatia": "HR", "serbia": "RS", "iceland": "IS",
	"canada": "CA", "mexico": "MX", "brazil": "BR", "brasil": "BR", "argentina": "AR", "chile": "CL", "colombia": "CO", "peru": "PE",
	"india": "IN", "china": "CN", "peoples republic of china": "CN", "japan": "JP", "south korea": "KR", "korea": "KR", "republic of korea": "KR",
	"singapore": "SG", "hong kong": "HK", "taiwan": "TW", "australia": "AU", "new zealand": "NZ",
	"indonesia": "ID", "malaysia": "MY", "thailand": "TH", "vietnam": "VN", "philippines": "PH", "pakistan": "PK", "bangladesh": "BD", "sri lanka": "LK",
	"united arab emirates": "AE", "uae": "AE", "saudi arabia": "SA", "israel": "IL", "qatar": "QA", "egypt": "EG",
	"south africa": "ZA", "nigeria": "NG", "kenya": "KE", "morocco": "MA", "ghana": "GH",
}

var iso2 = func() map[string]struct{} {
	m := map[string]struct{}{}
	for _, c := range countryNames {
		m[c] = struct{}{}
	}
	for _, c := range strings.Fields("AD AL AM AZ BA BH BY CY GE JO KW KZ LB LI MC MD ME MK MT OM UY VE EC BO PY CR PA DO GT TN DZ ET TZ UG SN CI CM NP KH MM MN UZ IQ IR") {
		m[c] = struct{}{}
	}
	return m
}()

// Country converts a country name or code to an ISO alpha-2 code, or "".
func Country(s string) string {
	s = Text(s)
	if len(s) == 2 {
		up := strings.ToUpper(s)
		if up == "UK" {
			return "GB"
		}
		if _, ok := iso2[up]; ok {
			return up
		}
	}
	if len(s) == 3 {
		switch strings.ToUpper(s) {
		case "USA":
			return "US"
		case "GBR":
			return "GB"
		case "DEU":
			return "DE"
		case "FRA":
			return "FR"
		case "IND":
			return "IN"
		case "CAN":
			return "CA"
		case "AUS":
			return "AU"
		}
	}
	return countryNames[Fold(s)]
}

// genericCCTLDs are country-code TLDs used generically; they say nothing
// about where a company is.
var genericCCTLDs = map[string]struct{}{
	"io": {}, "co": {}, "ai": {}, "me": {}, "tv": {}, "cc": {}, "ly": {}, "to": {}, "fm": {},
	"gg": {}, "sh": {}, "so": {}, "ws": {}, "vc": {}, "gl": {}, "im": {}, "is": {}, "la": {},
	"nu": {}, "st": {}, "tk": {}, "ms": {}, "bz": {}, "ac": {}, "eu": {},
}

// CountryFromTLD returns the ISO country for a hostname's ccTLD, or "" for
// generic TLDs and ccTLDs that are used generically.
func CountryFromTLD(host string) string {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	i := strings.LastIndexByte(host, '.')
	if i < 0 || len(host)-i-1 != 2 {
		return ""
	}
	tld := host[i+1:]
	if _, ok := genericCCTLDs[tld]; ok {
		return ""
	}
	if tld == "uk" {
		return "GB"
	}
	up := strings.ToUpper(tld)
	if _, ok := iso2[up]; ok {
		return up
	}
	return ""
}
