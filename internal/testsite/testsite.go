// Package testsite serves two small company websites used by the tests and
// the demo. Between them they exercise robots.txt, a sitemap, redirects
// (in-site and off-site), a slow page, 429 with Retry-After, a non-HTML
// resource, a noindex page, malformed HTML and a JavaScript-only page.
package testsite

import (
	"embed"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"
)

//go:embed sites
var sites embed.FS

// Hosts served by the fixture.
const (
	Acme   = "acme.test"
	Globex = "globex.test"
)

const acmeRobots = `User-agent: *
Disallow: /private/
Disallow: /members-only

User-agent: LeadGraphBot
Disallow: /private/
Disallow: /members-only
Disallow: /careers

Sitemap: http://acme.test/sitemap.xml
`

const acmeSitemap = `<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>http://acme.test/</loc></url>
  <url><loc>http://acme.test/products</loc></url>
  <url><loc>http://acme.test/private/secret</loc></url>
  <url><loc>http://other.test/not-ours</loc></url>
</urlset>
`

// Site is the fixture handler. It records every request so tests can assert
// on politeness and on what was never fetched.
type Site struct {
	// SlowDelay is how long /slow takes to answer.
	SlowDelay time.Duration
	// FlakyFailures is how many times /flaky answers 429 before succeeding.
	FlakyFailures int

	mu       sync.Mutex
	hits     map[string]int
	requests map[string][]Request
	agents   map[string]int
	inflight map[string]int
	peak     map[string]int
}

// Request is one recorded request.
type Request struct {
	Path string
	At   time.Time
}

func New() *Site {
	return &Site{SlowDelay: 300 * time.Millisecond, FlakyFailures: 2,
		hits: map[string]int{}, requests: map[string][]Request{}, agents: map[string]int{},
		inflight: map[string]int{}, peak: map[string]int{}}
}

func hostOf(r *http.Request) string {
	h := strings.ToLower(r.Host)
	if i := strings.IndexByte(h, ':'); i >= 0 {
		h = h[:i]
	}
	return strings.TrimPrefix(h, "www.")
}

// Hits returns how many times host+path was requested.
func (s *Site) Hits(host, path string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hits[host+path]
}

// Paths returns every requested host+path with its count.
func (s *Site) Paths() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.hits))
	for k, v := range s.hits {
		out[k] = v
	}
	return out
}

// Requests returns the requests seen for a host, in arrival order.
func (s *Site) Requests(host string) []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests[host]...)
}

// Total returns the number of requests received across all hosts.
func (s *Site) Total() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, r := range s.requests {
		n += len(r)
	}
	return n
}

// PeakConcurrency returns the most requests a host ever served at once.
func (s *Site) PeakConcurrency(host string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak[host]
}

// UserAgents returns the User-Agent values seen.
func (s *Site) UserAgents() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int, len(s.agents))
	for k, v := range s.agents {
		out[k] = v
	}
	return out
}

func (s *Site) record(host string, r *http.Request) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hits[host+r.URL.Path]++
	s.requests[host] = append(s.requests[host], Request{Path: r.URL.Path, At: time.Now()})
	s.agents[r.UserAgent()]++
	s.inflight[host]++
	if s.inflight[host] > s.peak[host] {
		s.peak[host] = s.inflight[host]
	}
	return s.hits[host+r.URL.Path]
}

func (s *Site) done(host string) {
	s.mu.Lock()
	s.inflight[host]--
	s.mu.Unlock()
}

func (s *Site) page(w http.ResponseWriter, host, file string) {
	data, err := sites.ReadFile("sites/" + host + "/" + file)
	if err != nil {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Server", "nginx/1.25.3")
	_, _ = w.Write(data)
}

func (s *Site) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	host := hostOf(r)
	n := s.record(host, r)
	defer s.done(host)
	switch host {
	case Acme:
		s.acme(w, r, n)
	case Globex:
		s.globex(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Site) acme(w http.ResponseWriter, r *http.Request, n int) {
	switch r.URL.Path {
	case "/robots.txt":
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprint(w, acmeRobots)
	case "/sitemap.xml":
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, acmeSitemap)
	case "/":
		s.page(w, Acme, "index.html")
	case "/about", "/team", "/contact", "/products", "/hidden", "/malformed", "/app":
		s.page(w, Acme, strings.TrimPrefix(r.URL.Path, "/")+".html")
	case "/blog/automation-guide", "/blog/second-post":
		s.page(w, Acme, "blog-"+strings.TrimPrefix(r.URL.Path, "/blog/")+".html")
	case "/blog/third-post":
		// Depth 4 from the homepage: must never be requested.
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><body>too deep <a href=\"mailto:deep@acme.test\">deep@acme.test</a></body></html>")
	case "/static/app.js":
		// Only a real browser asks for this; the crawler's HTTP client
		// never fetches scripts.
		w.Header().Set("Content-Type", "application/javascript")
		fmt.Fprint(w, `document.getElementById("root").innerHTML = "<h1>Acme App</h1><p>Rendered in the browser. Contact <a href='mailto:app@acme.test'>app@acme.test</a> for access to the Acme customer application portal and its documentation.</p>";`)
	case "/old-team":
		http.Redirect(w, r, "/team", http.StatusMovedPermanently)
	case "/go-offsite":
		http.Redirect(w, r, "http://globex.test/", http.StatusFound)
	case "/brochure.pdf":
		w.Header().Set("Content-Type", "application/pdf")
		fmt.Fprint(w, "%PDF-1.7 fixture")
	case "/slow":
		select {
		case <-time.After(s.SlowDelay):
			w.Header().Set("Content-Type", "text/html")
			fmt.Fprint(w, "<html><head><title>Slow page</title></head><body>finally</body></html>")
		case <-r.Context().Done():
		}
	case "/flaky":
		if n <= s.FlakyFailures {
			w.Header().Set("Retry-After", "0")
			http.Error(w, "slow down", http.StatusTooManyRequests)
			return
		}
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><head><title>Status | Acme Robotics</title></head><body>All systems operational. "+
			"<a href=\"mailto:status@acme.test\">status@acme.test</a></body></html>")
	case "/private/secret", "/members-only", "/careers":
		// Disallowed by robots.txt; a hit here is a compliance failure.
		w.Header().Set("Content-Type", "text/html")
		fmt.Fprint(w, "<html><body><a href=\"mailto:forbidden@acme.test\">forbidden@acme.test</a></body></html>")
	default:
		http.NotFound(w, r)
	}
}

func (s *Site) globex(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/":
		s.page(w, Globex, "index.html")
	case "/team":
		s.page(w, Globex, "team.html")
	default:
		// No robots.txt either: everything is allowed.
		http.NotFound(w, r)
	}
}
