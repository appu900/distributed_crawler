package extract

import (
	"strings"
	"testing"
	"time"
)

// Adversarial shapes must stay roughly linear: a hostile page may be 5 MB.
func TestAdversarialInputsAreFast(t *testing.T) {
	cases := map[string]string{
		"deep bold":     strings.Repeat("<b>Jane Doe ", 20000),
		"deep div":      strings.Repeat("<div class=team>", 20000) + "x",
		"many names":    strings.Repeat("<h3>Jane Doe</h3><p>CEO</p>", 20000),
		"many at":       strings.Repeat("a at b dot c at d [at] e ", 40000),
		"many digits":   strings.Repeat("+1 415 555 2671 tel 12 34 56 78 90 ", 30000),
		"many anchors":  strings.Repeat(`<a href="/p?x=1">x</a>`, 50000),
		"nested tables": strings.Repeat("<table><tr><td>Jane Doe<td>CEO", 3000),
		"many li":       strings.Repeat("<li>Jane Doe - CEO", 20000),
		"long line":     "<p>" + strings.Repeat("Jane Doe, CEO ", 50000),
		"addresses":     strings.Repeat("<address>1 Main St<br>Springfield, IL 62701</address>", 10000),
	}
	for name, body := range cases {
		start := time.Now()
		Page(Input{URL: "https://acme.com/team", Body: []byte(body)})
		if d := time.Since(start); d > time.Second {
			t.Errorf("%s: took %v for %d bytes", name, d, len(body))
		} else {
			t.Logf("%s: %v for %d KB", name, d.Round(time.Millisecond), len(body)>>10)
		}
	}
}
