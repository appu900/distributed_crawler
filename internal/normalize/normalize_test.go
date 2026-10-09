package normalize

import (
	"math"
	"testing"
)

func TestText(t *testing.T) {
	tests := map[string]string{
		"  hello   world \n": "hello world",
		"ｆｕｌｌｗｉｄｔｈ":          "fullwidth",
		"zero​width":         "zerowidth",
		"ﬁne":                "fine",
		"":                   "",
	}
	for in, want := range tests {
		if got := Text(in); got != want {
			t.Errorf("Text(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCompanyName(t *testing.T) {
	tests := map[string]string{
		"Acme Inc.":                   "Acme",
		"Acme, Inc":                   "Acme",
		"ACME CORPORATION":            "ACME",
		"Globex GmbH":                 "Globex",
		"Müller GmbH & Co. KG":        "Müller",
		"Initech Pvt. Ltd.":           "Initech",
		"Initech Private Limited":     "Initech",
		"Umbrella S.A.":               "Umbrella",
		"Stark Industries LLC":        "Stark Industries",
		"Wayne Enterprises":           "Wayne Enterprises",
		"Soylent  Co., Ltd.":          "Soylent",
		"Hooli B.V.":                  "Hooli",
		"Inc":                         "Inc",
		"Pied Piper Sp. z o.o.":       "Pied Piper",
		"  Vandelay Industries Ltd  ": "Vandelay Industries",
	}
	for in, want := range tests {
		if got := CompanyName(in); got != want {
			t.Errorf("CompanyName(%q) = %q, want %q", in, got, want)
		}
	}
	if CompanyKey("ACME, Inc.") != CompanyKey("Acme") {
		t.Error("company keys should match across suffix and case")
	}
}

func TestPersonName(t *testing.T) {
	tests := map[string]string{
		"Dr. Jane Doe":            "Jane Doe",
		"JOHN O'BRIEN":            "John O'Brien",
		"maria von trapp":         "Maria von Trapp",
		"Jane Doe, PhD":           "Jane Doe",
		"Jane Doe (she/her)":      "Jane Doe",
		"Mr John Smith Jr.":       "John Smith",
		"RONALD MCDONALD":         "Ronald McDonald",
		"Jean-Luc  Picard":        "Jean-Luc Picard",
		"ANNE-MARIE DUBOIS":       "Anne-Marie Dubois",
		"Søren Kierkegaard":       "Søren Kierkegaard",
		"Prof. Dr. Hans Müller":   "Hans Müller",
		"Jane Doe MBA":            "Jane Doe",
		"Jane Alice Doe MBA":      "Jane Alice Doe",
		"Doe":                     "Doe",
		"  Jane Doe  ":            "Jane Doe",
		"Jane Doe, Head of Sales": "Jane Doe, Head of Sales",
	}
	for in, want := range tests {
		if got := PersonName(in); got != want {
			t.Errorf("PersonName(%q) = %q, want %q", in, got, want)
		}
	}
	if PersonKey("José García") != "jose garcia" {
		t.Errorf("PersonKey = %q", PersonKey("José García"))
	}
}

func TestSeniority(t *testing.T) {
	tests := map[string]string{
		"CEO & Co-Founder":            "founder",
		"Chief Technology Officer":    "c_level",
		"VP of Sales":                 "vp",
		"Senior Vice President, EMEA": "vp",
		"Head of Marketing":           "director",
		"Engineering Manager":         "manager",
		"Software Engineer":           "individual",
		"Marketing Intern":            "entry",
		"Geschäftsführer":             "c_level",
		"":                            "",
		"Managing Director":           "c_level",
		"Director of Engineering":     "director",
	}
	for in, want := range tests {
		if got := Seniority(in); got != want {
			t.Errorf("Seniority(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestJaroWinkler(t *testing.T) {
	tests := []struct {
		a, b string
		want float64
	}{
		{"martha", "marhta", 0.9611},
		{"dixon", "dicksonx", 0.8133},
		{"", "", 1},
		{"a", "", 0},
		{"same", "same", 1},
		{"abc", "xyz", 0},
	}
	for _, tt := range tests {
		if got := JaroWinkler(tt.a, tt.b); math.Abs(got-tt.want) > 0.001 {
			t.Errorf("JaroWinkler(%q,%q) = %.4f, want %.4f", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestSamePerson(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"jane doe", "jane doe", true},
		{"jane doe", "jane a doe", true},
		{"jane doe", "j doe", true},
		{"jonathan smith", "jonathon smith", true},
		{"jane doe", "john doe", false},
		{"jane doe", "jane dow", false},
		{"jane doe", "jane roe", false},
		{"maria garcia", "mario garcia", false},
		{"", "jane doe", false},
		{"katherine johnson", "katharine johnson", true},
	}
	for _, tt := range tests {
		if got := SamePerson(tt.a, tt.b, 0.94); got != tt.want {
			t.Errorf("SamePerson(%q,%q) = %v, want %v (jw=%.3f)", tt.a, tt.b, got, tt.want, JaroWinkler(tt.a, tt.b))
		}
	}
}

func TestCountry(t *testing.T) {
	tests := map[string]string{
		"Germany": "DE", "deutschland": "DE", "US": "US", "usa": "US", "United States": "US",
		"UK": "GB", "United Kingdom": "GB", "España": "ES", "Narnia": "", "": "", "in": "IN",
	}
	for in, want := range tests {
		if got := Country(in); got != want {
			t.Errorf("Country(%q) = %q, want %q", in, got, want)
		}
	}
	tld := map[string]string{
		"acme.de": "DE", "acme.co.uk": "GB", "acme.io": "", "acme.com": "", "acme.fr": "FR", "localhost": "",
	}
	for in, want := range tld {
		if got := CountryFromTLD(in); got != want {
			t.Errorf("CountryFromTLD(%q) = %q, want %q", in, got, want)
		}
	}
}
