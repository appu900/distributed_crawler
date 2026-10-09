package urlx

import (
	"net/url"
	"testing"
)

func TestNormalize(t *testing.T) {
	base, _ := url.Parse("https://Example.com/a/b/page.html?x=1")
	tests := []struct {
		name, in string
		base     *url.URL
		want     string
		wantErr  bool
	}{
		{"lowercase host and scheme", "HTTP://WWW.Example.COM/Path", nil, "http://www.example.com/Path", false},
		{"empty path becomes slash", "https://example.com", nil, "https://example.com/", false},
		{"strip fragment", "https://example.com/a#team", nil, "https://example.com/a", false},
		{"default https port", "https://example.com:443/a", nil, "https://example.com/a", false},
		{"default http port", "http://example.com:80/", nil, "http://example.com/", false},
		{"keep custom port", "http://example.com:8080/", nil, "http://example.com:8080/", false},
		{"strip tracking", "https://example.com/?utm_source=x&gclid=1&fbclid=2&id=7", nil, "https://example.com/?id=7", false},
		{"only tracking leaves no query", "https://example.com/a?utm_campaign=x", nil, "https://example.com/a", false},
		{"sort query", "https://example.com/?b=2&a=1&a=0", nil, "https://example.com/?a=0&a=1&b=2", false},
		{"relative path", "../team", base, "https://example.com/a/team", false},
		{"relative root", "/contact", base, "https://example.com/contact", false},
		{"protocol relative", "//cdn.example.com/x", base, "https://cdn.example.com/x", false},
		{"dot segments", "https://example.com/a/./b/../c/", nil, "https://example.com/a/c/", false},
		{"idna", "https://bücher.example/straße", nil, "https://xn--bcher-kva.example/stra%C3%9Fe", false},
		{"trailing dot host", "https://example.com./", nil, "https://example.com/", false},
		{"userinfo dropped", "https://user:pw@example.com/", nil, "https://example.com/", false},
		{"mailto rejected", "mailto:a@b.com", nil, "", true},
		{"javascript rejected", "javascript:void(0)", base, "", true},
		{"empty", "  ", nil, "", true},
		{"no host", "https:///path", nil, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Normalize(tt.in, tt.base)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestRegistrable(t *testing.T) {
	tests := map[string]string{
		"www.example.com":       "example.com",
		"a.b.example.co.uk":     "example.co.uk",
		"example.com":           "example.com",
		"127.0.0.1":             "127.0.0.1",
		"127.0.0.1:8080":        "127.0.0.1",
		"localhost":             "localhost",
		"shop.acme.test":        "acme.test",
		"foo.github.io":         "foo.github.io",
		"WWW.Example.COM.":      "example.com",
		"xn--bcher-kva.example": "xn--bcher-kva.example",
	}
	for in, want := range tests {
		if got := Registrable(in); got != want {
			t.Errorf("Registrable(%q) = %q, want %q", in, got, want)
		}
	}
	if !SameSite("blog.example.com", "www.example.com") || SameSite("example.com", "example.org") {
		t.Error("SameSite mismatch")
	}
}

func TestSeedURL(t *testing.T) {
	tests := map[string]string{
		"example.com":             "https://example.com/",
		"http://acme.test":        "http://acme.test/",
		" Example.com/about ":     "https://example.com/about",
		"https://example.com/#x ": "https://example.com/",
	}
	for in, want := range tests {
		got, err := SeedURL(in)
		if err != nil || got != want {
			t.Errorf("SeedURL(%q) = %q, %v; want %q", in, got, err, want)
		}
	}
	if _, err := SeedURL(""); err == nil {
		t.Error("expected error for empty seed")
	}
}

func TestHashStable(t *testing.T) {
	if Hash("a", "b") == Hash("ab") || Hash("a") != Hash("a") || len(Hash("x")) != 32 {
		t.Error("hash properties violated")
	}
}

func FuzzNormalize(f *testing.F) {
	for _, s := range []string{
		"https://example.com/a?b=1#c", "HTTP://EXAMPLE.com:80/../x", "//x.y/z", "/rel?utm_source=1",
		"https://bücher.example/ä", "http://[::1]:8080/", "https://example.com/%2e%2e/%zz", "?q=a;b",
		"https://example.com/a%2Fb", "https://a.b/?a=%20+&A=1",
	} {
		f.Add(s)
	}
	base, _ := url.Parse("https://base.example/dir/")
	f.Fuzz(func(t *testing.T, in string) {
		got, err := Normalize(in, base)
		if err != nil {
			return
		}
		again, err := Normalize(got, nil)
		if err != nil {
			t.Fatalf("normalized output %q (from %q) does not re-normalize: %v", got, in, err)
		}
		if again != got {
			t.Fatalf("not idempotent: %q -> %q -> %q", in, got, again)
		}
		u, err := url.Parse(got)
		if err != nil || u.Host == "" || u.Fragment != "" || u.User != nil {
			t.Fatalf("bad normalized url %q: %v", got, err)
		}
	})
}
