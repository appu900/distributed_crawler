package robots

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type fakeGetter struct {
	status int
	body   string
	err    error
	calls  atomic.Int32
	last   string
}

func (f *fakeGetter) GetRobots(_ context.Context, u string) (int, []byte, error) {
	f.calls.Add(1)
	f.last = u
	return f.status, []byte(f.body), f.err
}

func mustURL(s string) *url.URL {
	u, err := url.Parse(s)
	if err != nil {
		panic(err)
	}
	return u
}

const sample = `
User-agent: *
Disallow: /admin/
Disallow: /search
Allow: /admin/public
Crawl-delay: 2

User-agent: LeadGraphBot
Disallow: /private/
Disallow: /*.pdf$
Disallow: /team?secret=
Crawl-delay: 7

User-agent: BadBot
Disallow: /

Sitemap: https://acme.com/sitemap.xml
Sitemap: https://acme.com/news-sitemap.xml
`

func TestAllowed(t *testing.T) {
	g := &fakeGetter{status: 200, body: sample}
	m := New(g, &MemCache{}, "LeadGraphBot", time.Hour)
	rules, fetched, err := m.Rules(context.Background(), mustURL("https://Acme.com/x"))
	if err != nil || !fetched {
		t.Fatalf("err=%v fetched=%v", err, fetched)
	}
	if g.last != "https://acme.com/robots.txt" {
		t.Errorf("fetched %q", g.last)
	}
	tests := map[string]bool{
		"https://acme.com/":                 true,
		"https://acme.com/team":             true,
		"https://acme.com/private/":         false,
		"https://acme.com/private/x":        false,
		"https://acme.com/privateer":        true,
		"https://acme.com/admin/":           true, // our group, not *
		"https://acme.com/files/a.pdf":      false,
		"https://acme.com/files/a.pdf.html": true,
		"https://acme.com/team?secret=1":    false,
		"https://acme.com/team?page=2":      true,
	}
	for raw, want := range tests {
		if got := rules.Allowed(mustURL(raw)); got != want {
			t.Errorf("Allowed(%s) = %v, want %v", raw, got, want)
		}
	}
	if rules.CrawlDelay != 7*time.Second {
		t.Errorf("crawl delay = %v", rules.CrawlDelay)
	}
	if strings.Join(rules.Sitemaps, ",") != "https://acme.com/sitemap.xml,https://acme.com/news-sitemap.xml" {
		t.Errorf("sitemaps = %v", rules.Sitemaps)
	}
}

func TestWildcardGroupFallback(t *testing.T) {
	g := &fakeGetter{status: 200, body: "User-agent: *\nDisallow: /admin/\nAllow: /admin/public\nCrawl-delay: 2\n"}
	m := New(g, nil, "LeadGraphBot", time.Hour)
	rules, _, _ := m.Rules(context.Background(), mustURL("https://acme.com/"))
	if rules.Allowed(mustURL("https://acme.com/admin/x")) || !rules.Allowed(mustURL("https://acme.com/admin/public")) {
		t.Error("wildcard group not applied")
	}
	if rules.CrawlDelay != 2*time.Second {
		t.Errorf("crawl delay = %v", rules.CrawlDelay)
	}
}

func TestStatusSemantics(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		err         error
		wantAllowed bool
		unavailable bool
	}{
		{"404 allows all", 404, nil, true, false},
		{"403 allows all", 403, nil, true, false},
		{"500 disallows all", 500, nil, false, true},
		{"503 disallows all", 503, nil, false, true},
		{"429 disallows all", 429, nil, false, true},
		{"network error disallows all", 0, errors.New("dial tcp: refused"), false, true},
		{"empty 200 allows all", 200, nil, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := New(&fakeGetter{status: tt.status, err: tt.err}, nil, "LeadGraphBot", time.Hour)
			rules, _, err := m.Rules(context.Background(), mustURL("https://acme.com/"))
			if err != nil {
				t.Fatal(err)
			}
			if got := rules.Allowed(mustURL("https://acme.com/team")); got != tt.wantAllowed {
				t.Errorf("allowed = %v", got)
			}
			if rules.Unavailable != tt.unavailable {
				t.Errorf("unavailable = %v", rules.Unavailable)
			}
		})
	}
}

func TestCaching(t *testing.T) {
	g := &fakeGetter{status: 200, body: "User-agent: *\nDisallow: /x\n"}
	shared := &MemCache{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m1 := New(g, shared, "LeadGraphBot", 24*time.Hour)
	m1.now = func() time.Time { return now }
	for i := 0; i < 5; i++ {
		_, fetched, _ := m1.Rules(context.Background(), mustURL("https://acme.com/p"))
		if fetched != (i == 0) {
			t.Errorf("call %d fetched=%v", i, fetched)
		}
	}
	// Another origin (scheme or port) is a separate robots.txt.
	m1.Rules(context.Background(), mustURL("http://acme.com/"))
	m1.Rules(context.Background(), mustURL("https://acme.com:8443/"))
	if g.calls.Load() != 3 {
		t.Errorf("calls = %d, want 3", g.calls.Load())
	}
	// A second worker reuses the shared cache without fetching.
	m2 := New(g, shared, "LeadGraphBot", 24*time.Hour)
	m2.now = func() time.Time { return now }
	rules, fetched, _ := m2.Rules(context.Background(), mustURL("https://acme.com/"))
	if fetched || rules.Allowed(mustURL("https://acme.com/x")) {
		t.Error("shared cache not used")
	}
	// After the TTL the local entry expires.
	now = now.Add(25 * time.Hour)
	shared.m = nil
	if _, fetched, _ := m1.Rules(context.Background(), mustURL("https://acme.com/")); !fetched {
		t.Error("expected refetch after TTL")
	}
}

func TestErrorsAreCachedBriefly(t *testing.T) {
	g := &fakeGetter{status: 503}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	m := New(g, nil, "LeadGraphBot", 24*time.Hour)
	m.now = func() time.Time { return now }
	m.Rules(context.Background(), mustURL("https://acme.com/"))
	now = now.Add(11 * time.Minute)
	g.status, g.body = 200, ""
	rules, fetched, _ := m.Rules(context.Background(), mustURL("https://acme.com/"))
	if !fetched || rules.Unavailable || !rules.Allowed(mustURL("https://acme.com/a")) {
		t.Error("a failed robots fetch must be retried after the short error TTL")
	}
}

func TestGarbageAndNil(t *testing.T) {
	m := New(&fakeGetter{status: 200, body: "<html>not robots</html>\x00\xff"}, nil, "LeadGraphBot", time.Hour)
	rules, _, err := m.Rules(context.Background(), mustURL("https://acme.com/"))
	if err != nil || !rules.Allowed(mustURL("https://acme.com/a")) {
		t.Errorf("garbage robots should allow: %v", err)
	}
	if _, _, err := m.Rules(context.Background(), &url.URL{}); err == nil {
		t.Error("expected error for empty url")
	}
	var nilRules *Rules
	if !nilRules.Allowed(mustURL("https://a.com/")) {
		t.Error("nil rules allow")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := New(&fakeGetter{status: 200}, nil, "x", time.Hour).Rules(ctx, mustURL("https://b.com/")); err == nil {
		t.Error("cancelled context should error, not cache a failure")
	}
}
