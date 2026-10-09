package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/andybalholm/brotli"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/pkg/model"
)

func testCfg() config.Fetch {
	c := config.Default().Fetch
	c.AllowPrivateNetworks = true
	c.BackoffBase = time.Millisecond
	c.BackoffMax = 5 * time.Millisecond
	c.TotalTimeout = 2 * time.Second
	c.HeaderTimeout = time.Second
	return c
}

// site starts a server and returns a client that resolves host to it.
func site(t *testing.T, host string, h http.Handler, mutate func(*config.Fetch)) (*Client, string) {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cfg := testCfg()
	cfg.HostOverrides = map[string]string{host: srv.Listener.Addr().String()}
	if mutate != nil {
		mutate(&cfg)
	}
	return NewClient(cfg, nil), "http://" + host
}

func TestGetBasics(t *testing.T) {
	var gotUA, gotAE string
	c, base := site(t, "acme.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA, gotAE = r.Header.Get("User-Agent"), r.Header.Get("Accept-Encoding")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, "<p>hello</p>")
	}), nil)
	resp, err := c.Get(context.Background(), base+"/")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != 200 || string(resp.Body) != "<p>hello</p>" || resp.ContentType != "text/html" {
		t.Errorf("resp = %+v", resp)
	}
	if gotUA != "LeadGraphBot/1.0 (+https://example.com/bot)" {
		t.Errorf("user agent = %q", gotUA)
	}
	if gotAE != "gzip, br" {
		t.Errorf("accept-encoding = %q", gotAE)
	}
}

func TestCompressionAndCharset(t *testing.T) {
	latin1 := []byte("<html><head><meta charset=\"iso-8859-1\"></head><body>M\xfcller Stra\xdfe</body></html>")
	c, base := site(t, "acme.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/gzip":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Content-Encoding", "gzip")
			zw := gzip.NewWriter(w)
			zw.Write([]byte("<p>gzipped</p>"))
			zw.Close()
		case "/br":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Content-Encoding", "br")
			bw := brotli.NewWriter(w)
			bw.Write([]byte("<p>brotli</p>"))
			bw.Close()
		case "/latin1-header":
			w.Header().Set("Content-Type", "text/html; charset=ISO-8859-1")
			w.Write([]byte("M\xfcller"))
		case "/latin1-meta":
			w.Header().Set("Content-Type", "text/html")
			w.Write(latin1)
		case "/weird":
			w.Header().Set("Content-Type", "text/html")
			w.Header().Set("Content-Encoding", "zstd")
			w.Write([]byte("x"))
		}
	}), nil)
	ctx := context.Background()
	for path, want := range map[string]string{
		"/gzip": "<p>gzipped</p>", "/br": "<p>brotli</p>", "/latin1-header": "Müller",
	} {
		resp, err := c.Get(ctx, base+path)
		if err != nil || string(resp.Body) != want {
			t.Errorf("%s: body=%q err=%v", path, resp.Body, err)
		}
	}
	resp, err := c.Get(ctx, base+"/latin1-meta")
	if err != nil || !strings.Contains(string(resp.Body), "Müller Straße") {
		t.Errorf("meta charset: %q %v", resp.Body, err)
	}
	if _, err := c.Get(ctx, base+"/weird"); err == nil {
		t.Error("unsupported encoding should error")
	}
}

func TestContentTypeAllowlist(t *testing.T) {
	c, base := site(t, "acme.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/pdf":
			w.Header().Set("Content-Type", "application/pdf")
			w.Write([]byte("%PDF-1.7"))
		case "/xml":
			w.Header().Set("Content-Type", "application/xml")
			w.Write([]byte("<urlset/>"))
		case "/sniff-png":
			w.Header()["Content-Type"] = nil
			w.Write([]byte("\x89PNG\r\n\x1a\n0000000000000000"))
		case "/sniff-html":
			w.Header()["Content-Type"] = nil
			w.Write([]byte("<!DOCTYPE html><html><body>hi</body></html>"))
		}
	}), nil)
	tests := map[string]string{"/pdf": model.SkipContentType, "/xml": "", "/sniff-png": model.SkipContentType, "/sniff-html": ""}
	for path, skip := range tests {
		resp, err := c.Get(context.Background(), base+path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		if resp.SkipReason != skip {
			t.Errorf("%s: skip=%q want %q", path, resp.SkipReason, skip)
		}
		if skip != "" && len(resp.Body) != 0 {
			t.Errorf("%s: body kept for skipped type", path)
		}
	}
}

func TestBodyLimit(t *testing.T) {
	c, base := site(t, "acme.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Write(bytes.Repeat([]byte("a"), 5000))
	}), func(f *config.Fetch) { f.MaxBodyBytes = 1000 })
	resp, err := c.Get(context.Background(), base+"/")
	if err != nil || len(resp.Body) != 1000 || !resp.Truncated {
		t.Errorf("len=%d truncated=%v err=%v", len(resp.Body), resp.Truncated, err)
	}
}

func TestRedirectPolicy(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "other")
	}))
	defer other.Close()
	var c *Client
	c, base := site(t, "acme.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/old":
			http.Redirect(w, r, "/new", http.StatusMovedPermanently)
		case r.URL.Path == "/to-www":
			http.Redirect(w, r, "http://www.acme.test/new", http.StatusFound)
		case r.URL.Path == "/offsite":
			http.Redirect(w, r, "http://evil.test/", http.StatusFound)
		case r.URL.Path == "/to-blocked":
			http.Redirect(w, r, "/private/x", http.StatusFound)
		case strings.HasPrefix(r.URL.Path, "/loop"):
			http.Redirect(w, r, r.URL.Path+"x", http.StatusFound)
		default:
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, "landed "+r.Host+r.URL.Path)
		}
	}), func(f *config.Fetch) {
		f.HostOverrides["evil.test"] = other.Listener.Addr().String()
	})
	c.cfg.HostOverrides["www.acme.test"] = c.cfg.HostOverrides["acme.test"]
	c.SetRedirectCheck(func(_ context.Context, u *url.URL) error {
		if strings.HasPrefix(u.Path, "/private") {
			return ErrRobotsRedirect
		}
		return nil
	})
	ctx := context.Background()

	resp, err := c.Get(ctx, base+"/old")
	if err != nil || resp.FinalURL != base+"/new" || string(resp.Body) != "landed acme.test/new" {
		t.Errorf("same-host redirect: %+v %v", resp, err)
	}
	resp, err = c.Get(ctx, base+"/to-www")
	if err != nil || resp.FinalURL != "http://www.acme.test/new" {
		t.Errorf("same-site redirect: %+v %v", resp, err)
	}
	if _, err = c.Get(ctx, base+"/offsite"); !errors.Is(err, ErrOffDomain) {
		t.Errorf("offsite redirect err = %v", err)
	}
	if _, err = c.Get(ctx, base+"/to-blocked"); !errors.Is(err, ErrRobotsRedirect) {
		t.Errorf("robots redirect err = %v", err)
	}
	if _, err = c.Get(ctx, base+"/loop"); !errors.Is(err, ErrTooManyRedirects) {
		t.Errorf("redirect loop err = %v", err)
	}
	// Under NoFollow a redirect is reported, not followed.
	resp, err = c.Get(NoFollow(ctx), base+"/old")
	if err != nil || resp.Status != http.StatusMovedPermanently || resp.Redirect != base+"/new" || len(resp.Body) != 0 {
		t.Errorf("no-follow: %+v %v", resp, err)
	}
	if resp, err = c.Get(NoFollow(ctx), base+"/offsite"); err != nil || resp.Redirect != "http://evil.test/" {
		t.Errorf("no-follow offsite: %+v %v", resp, err)
	}
	for _, e := range []error{ErrOffDomain, ErrRobotsRedirect, ErrTooManyRedirects} {
		if Retryable(0, fmt.Errorf("wrapped: %w", e)) {
			t.Errorf("%v must not be retried", e)
		}
	}
}

func TestRetriesAndRetryAfter(t *testing.T) {
	var hits atomic.Int32
	c, base := site(t, "acme.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		switch r.URL.Path {
		case "/flaky":
			if n < 3 {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
		case "/429":
			if n == 1 {
				w.Header().Set("Retry-After", "7")
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
		case "/always500":
			w.WriteHeader(500)
			return
		case "/404":
			w.WriteHeader(404)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "ok")
	}), func(f *config.Fetch) { f.BackoffMax = 10 * time.Second })
	var slept []time.Duration
	c.sleep = func(_ context.Context, d time.Duration) error { slept = append(slept, d); return nil }
	var attempts atomic.Int32
	c.OnAttempt = func(int, error, time.Duration) { attempts.Add(1) }
	ctx := context.Background()

	resp, err := c.Fetch(ctx, base+"/flaky")
	if err != nil || resp.Status != 200 || resp.Attempts != 3 || hits.Load() != 3 {
		t.Errorf("flaky: %+v err=%v hits=%d", resp, err, hits.Load())
	}

	hits.Store(0)
	slept = nil
	resp, err = c.Fetch(ctx, base+"/429")
	if err != nil || resp.Status != 200 || len(slept) != 1 || slept[0] != 7*time.Second {
		t.Errorf("429: status=%d slept=%v err=%v", resp.Status, slept, err)
	}

	hits.Store(0)
	resp, err = c.Fetch(ctx, base+"/always500")
	if err != nil || resp.Status != 500 || hits.Load() != int32(c.cfg.MaxRetries)+1 {
		t.Errorf("always500: status=%d hits=%d", resp.Status, hits.Load())
	}

	hits.Store(0)
	resp, _ = c.Fetch(ctx, base+"/404")
	if resp.Status != 404 || hits.Load() != 1 {
		t.Errorf("404 must not be retried: hits=%d", hits.Load())
	}
	if attempts.Load() == 0 {
		t.Error("OnAttempt not called")
	}
}

func TestNetworkErrorRetried(t *testing.T) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close() // nothing listens here any more
	cfg := testCfg()
	cfg.HostOverrides = map[string]string{"dead.test": addr}
	c := NewClient(cfg, nil)
	n := 0
	c.sleep = func(context.Context, time.Duration) error { n++; return nil }
	_, err := c.Fetch(context.Background(), "http://dead.test/")
	if err == nil || n != cfg.MaxRetries {
		t.Errorf("err=%v retries=%d", err, n)
	}
}

func TestSlowHostTimesOut(t *testing.T) {
	c, base := site(t, "slow.test", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(3 * time.Second):
		case <-r.Context().Done():
		}
	}), func(f *config.Fetch) { f.HeaderTimeout = 100 * time.Millisecond; f.MaxRetries = 0 })
	start := time.Now()
	_, err := c.Fetch(context.Background(), base+"/")
	if err == nil || time.Since(start) > 2*time.Second {
		t.Errorf("slow host: err=%v after %v", err, time.Since(start))
	}
}

func TestPrivateAddressesBlocked(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "secret") }))
	defer srv.Close()
	cfg := testCfg()
	cfg.AllowPrivateNetworks = false
	c := NewClient(cfg, nil)
	_, err := c.Get(context.Background(), srv.URL)
	if !errors.Is(err, ErrBlockedAddress) {
		t.Errorf("loopback fetch err = %v", err)
	}
	if Retryable(0, err) {
		t.Error("blocked address must not be retried")
	}
	for ip, public := range map[string]bool{
		"127.0.0.1": false, "10.1.2.3": false, "192.168.1.1": false, "172.16.0.1": false, "169.254.169.254": false,
		"100.64.0.1": false, "::1": false, "fe80::1": false, "fd00::1": false, "0.0.0.0": false,
		"8.8.8.8": true, "2606:4700:4700::1111": true,
	} {
		if got := IsPublicIP(net.ParseIP(ip)); got != public {
			t.Errorf("IsPublicIP(%s) = %v", ip, got)
		}
	}
}

func TestBackoff(t *testing.T) {
	base, maxD := 100*time.Millisecond, 2*time.Second
	for attempt := 1; attempt <= 12; attempt++ {
		ceil := min(base<<(attempt-1), maxD)
		for i := 0; i < 50; i++ {
			d := Backoff(attempt, base, maxD, 0)
			if d < ceil/2 || d > ceil {
				t.Fatalf("attempt %d: %v outside [%v,%v]", attempt, d, ceil/2, ceil)
			}
		}
	}
	if d := Backoff(1, base, maxD, time.Second); d != time.Second {
		t.Errorf("retry-after not honored: %v", d)
	}
	if d := Backoff(1, base, maxD, time.Hour); d != maxD {
		t.Errorf("retry-after not capped: %v", d)
	}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if d := parseRetryAfter(now.Add(90*time.Second).Format(http.TimeFormat), now); d != 90*time.Second {
		t.Errorf("http-date retry-after = %v", d)
	}
	if parseRetryAfter("soon", now) != 0 || parseRetryAfter("-5", now) != 0 || parseRetryAfter("", now) != 0 {
		t.Error("bad retry-after should be 0")
	}
}

func TestBreaker(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	b := NewBreakers(3, time.Minute)
	b.now = func() time.Time { return now }
	for i := 0; i < 2; i++ {
		if b.Failure("a.test") {
			t.Fatal("opened too early")
		}
	}
	if ok, _ := b.Allow("a.test"); !ok {
		t.Fatal("should still be closed")
	}
	if !b.Failure("a.test") || !b.Open("a.test") {
		t.Fatal("should open on the third failure")
	}
	ok, retryAt := b.Allow("a.test")
	if ok || !retryAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("open breaker allowed=%v retryAt=%v", ok, retryAt)
	}
	if ok, _ := b.Allow("b.test"); !ok {
		t.Fatal("other hosts unaffected")
	}
	now = now.Add(61 * time.Second)
	if ok, _ := b.Allow("a.test"); !ok {
		t.Fatal("half-open should admit one probe")
	}
	if ok, _ := b.Allow("a.test"); ok {
		t.Fatal("only one probe at a time")
	}
	if !b.Failure("a.test") {
		t.Fatal("failed probe should re-open")
	}
	if ok, _ := b.Allow("a.test"); ok {
		t.Fatal("re-opened breaker must reject")
	}
	now = now.Add(61 * time.Second)
	b.Allow("a.test")
	b.Success("a.test")
	if ok, _ := b.Allow("a.test"); !ok || b.Open("a.test") {
		t.Fatal("successful probe should close")
	}
	// A success resets the consecutive-failure count.
	b.Failure("c.test")
	b.Failure("c.test")
	b.Success("c.test")
	if b.Failure("c.test") {
		t.Fatal("count should have reset")
	}
}

func TestProxyPool(t *testing.T) {
	if p, err := PoolFromConfig(nil); err != nil || p.Next(nil) != nil {
		t.Errorf("empty config should be direct: %v", err)
	}
	p, err := NewStaticPool([]string{"http://p1:8080", "http://p2:8080"})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for i := 0; i < 10; i++ {
		seen[p.Next(nil).Host]++
	}
	if seen["p1:8080"] != 5 || seen["p2:8080"] != 5 {
		t.Errorf("round robin: %v", seen)
	}
	if _, err := NewStaticPool([]string{"::bad"}); err == nil {
		t.Error("bad proxy url should fail")
	}
}

func TestNeedsRender(t *testing.T) {
	shell := []byte(`<html><head><script src="/app.js"></script></head><body><div id="root"></div></body></html>`)
	full := []byte(`<html><body><script src="/a.js"></script><p>` + strings.Repeat("real content ", 40) + `</p></body></html>`)
	static := []byte(`<html><body><p>tiny</p></body></html>`)
	if !NeedsRender(shell, 200) || NeedsRender(full, 200) || NeedsRender(static, 200) {
		t.Error("render heuristic wrong")
	}
}
