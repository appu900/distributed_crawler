// Package fetch implements the polite HTTP client and the fetch stage worker.
package fetch

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/andybalholm/brotli"
	"golang.org/x/net/html/charset"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

var (
	ErrTooManyRedirects = errors.New("fetch: too many redirects")
	ErrOffDomain        = errors.New("fetch: redirect leaves registrable domain")
	ErrRobotsRedirect   = errors.New("fetch: redirect target disallowed by robots.txt")
	ErrBlockedAddress   = errors.New("fetch: destination is a private or local address")
)

// Response is the outcome of fetching one URL.
type Response struct {
	URL      string
	FinalURL string
	Status   int
	Header   http.Header
	// Body is decompressed and, for text types, transcoded to UTF-8.
	Body        []byte
	ContentType string
	Truncated   bool
	// SkipReason is set when the response was deliberately not read.
	SkipReason string
	// Redirect is the absolute target of a 3xx response that was not
	// followed.
	Redirect   string
	RetryAfter time.Duration
	Attempts   int
}

// RedirectCheck vets a redirect target (robots.txt); a non-nil error stops
// the redirect.
type RedirectCheck func(ctx context.Context, u *url.URL) error

// Client is the crawler's HTTP client.
type Client struct {
	cfg      config.Fetch
	http     *http.Client
	proxies  ProxyPool
	redirect RedirectCheck
	// OnAttempt, if set, observes every HTTP attempt (for metrics).
	OnAttempt func(status int, err error, elapsed time.Duration)
	sleep     func(ctx context.Context, d time.Duration) error
}

// NewClient builds a Client. proxies may be nil.
func NewClient(cfg config.Fetch, proxies ProxyPool) *Client {
	if proxies == nil {
		proxies = NoProxy{}
	}
	c := &Client{cfg: cfg, proxies: proxies, sleep: sleepCtx}
	dialer := &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: 30 * time.Second}
	_, direct := proxies.(NoProxy)
	if !cfg.AllowPrivateNetworks && direct {
		dialer.Control = publicOnly
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if target, ok := override(cfg.HostOverrides, addr); ok {
				addr = target
			}
			return dialer.DialContext(ctx, network, addr)
		},
		Proxy: func(r *http.Request) (*url.URL, error) {
			return proxies.Next(r.URL), nil
		},
		ForceAttemptHTTP2:     true,
		TLSHandshakeTimeout:   cfg.TLSTimeout,
		ResponseHeaderTimeout: cfg.HeaderTimeout,
		ExpectContinueTimeout: time.Second,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   max(cfg.MaxIdleConnsPerHost, 1),
		IdleConnTimeout:       90 * time.Second,
		// Compression is negotiated by hand so brotli can be offered.
		DisableCompression: true,
	}
	c.http = &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if req.Context().Value(noFollowKey{}) != nil {
				return http.ErrUseLastResponse
			}
			if len(via) > cfg.MaxRedirects {
				return ErrTooManyRedirects
			}
			if !urlx.SameSite(req.URL.Hostname(), via[0].URL.Hostname()) {
				return ErrOffDomain
			}
			if c.redirect != nil {
				if err := c.redirect(req.Context(), req.URL); err != nil {
					return err
				}
			}
			return nil
		},
	}
	return c
}

type noFollowKey struct{}

// NoFollow returns a context under which Get reports a redirect instead of
// following it. The fetch stage uses it so that redirect targets go through
// the frontier like any other URL: deduplicated, robots-checked and paced.
func NoFollow(ctx context.Context) context.Context {
	return context.WithValue(ctx, noFollowKey{}, true)
}

// SetRedirectCheck installs the robots check applied to redirect targets.
func (c *Client) SetRedirectCheck(f RedirectCheck) { c.redirect = f }

// CloseIdle releases pooled connections.
func (c *Client) CloseIdle() { c.http.CloseIdleConnections() }

// override maps "host:port" through the configured host overrides. Keys may
// be "host" (any port) or "host:port".
func override(m map[string]string, addr string) (string, bool) {
	if len(m) == 0 {
		return "", false
	}
	if t, ok := m[addr]; ok {
		return t, true
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return "", false
	}
	t, ok := m[host]
	return t, ok
}

// publicOnly is a dialer Control hook that refuses non-public destinations.
// It runs after DNS resolution, so DNS rebinding cannot bypass it.
func publicOnly(_, address string, _ syscall.RawConn) error {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	if ip := net.ParseIP(host); ip == nil || !IsPublicIP(ip) {
		return fmt.Errorf("%w: %s", ErrBlockedAddress, host)
	}
	return nil
}

var cgnat = &net.IPNet{IP: net.IPv4(100, 64, 0, 0), Mask: net.CIDRMask(10, 32)}

// IsPublicIP reports whether ip is a globally routable unicast address.
func IsPublicIP(ip net.IP) bool {
	return !(ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified() || ip.IsMulticast() || ip.IsInterfaceLocalMulticast() || cgnat.Contains(ip))
}

var allowedTypes = map[string]struct{}{
	"text/html": {}, "application/xhtml+xml": {}, "text/plain": {}, "text/xml": {}, "application/xml": {},
}

// AllowedContentType reports whether a media type is one the crawler reads.
func AllowedContentType(mediaType string) bool {
	_, ok := allowedTypes[strings.ToLower(mediaType)]
	return ok
}

// Get performs a single attempt. A response with an HTTP error status is
// returned with a nil error; err is for transport failures and policy stops.
func (c *Client) Get(ctx context.Context, rawURL string) (*Response, error) {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.TotalTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.cfg.UserAgent())
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,text/plain;q=0.8,*/*;q=0.1")
	req.Header.Set("Accept-Encoding", "gzip, br")
	req.Header.Set("Accept-Language", "en,*;q=0.5")

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		c.observe(0, err, start)
		return nil, err
	}
	defer resp.Body.Close()
	out := &Response{
		URL: rawURL, FinalURL: resp.Request.URL.String(), Status: resp.StatusCode, Header: resp.Header,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
	}
	mediaType, params, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	out.ContentType = mediaType

	if resp.StatusCode >= 300 && resp.StatusCode < 400 {
		if loc, lerr := resp.Location(); lerr == nil {
			out.Redirect = loc.String()
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Drain a little so the connection can be reused; the body of an
		// error page is not kept.
		_, _ = io.CopyN(io.Discard, resp.Body, 64<<10)
		c.observe(resp.StatusCode, nil, start)
		return out, nil
	}
	if mediaType != "" && !AllowedContentType(mediaType) {
		out.SkipReason = model.SkipContentType
		c.observe(resp.StatusCode, nil, start)
		return out, nil
	}
	reader, err := decompress(resp.Header.Get("Content-Encoding"), resp.Body)
	if err != nil {
		c.observe(resp.StatusCode, err, start)
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(reader, c.cfg.MaxBodyBytes+1))
	if err != nil && len(body) == 0 {
		c.observe(resp.StatusCode, err, start)
		return nil, fmt.Errorf("fetch: reading body: %w", err)
	}
	if int64(len(body)) > c.cfg.MaxBodyBytes {
		body, out.Truncated = body[:c.cfg.MaxBodyBytes], true
	}
	if mediaType == "" {
		sniffed, _, _ := mime.ParseMediaType(http.DetectContentType(body))
		out.ContentType = sniffed
		if !AllowedContentType(sniffed) {
			out.SkipReason = model.SkipContentType
			c.observe(resp.StatusCode, nil, start)
			return out, nil
		}
	}
	out.Body = toUTF8(body, out.ContentType, params["charset"])
	c.observe(resp.StatusCode, nil, start)
	return out, nil
}

func (c *Client) observe(status int, err error, start time.Time) {
	if c.OnAttempt != nil {
		c.OnAttempt(status, err, time.Since(start))
	}
}

func decompress(encoding string, r io.Reader) (io.Reader, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return r, nil
	case "gzip", "x-gzip":
		return gzip.NewReader(r)
	case "br":
		return brotli.NewReader(r), nil
	case "deflate":
		return flate.NewReader(r), nil
	default:
		return nil, fmt.Errorf("fetch: unsupported content-encoding %q", encoding)
	}
}

// toUTF8 transcodes HTML and plain text using the declared charset, a BOM or
// a <meta> declaration. XML is left as-is; its own decoder reads the prolog.
func toUTF8(body []byte, mediaType, declared string) []byte {
	if mediaType != "text/html" && mediaType != "application/xhtml+xml" && mediaType != "text/plain" {
		return body
	}
	ct := mediaType
	if declared != "" {
		ct += "; charset=" + declared
	}
	r, err := charset.NewReader(bytes.NewReader(body), ct)
	if err != nil {
		return body
	}
	out, err := io.ReadAll(r)
	if err != nil || len(out) == 0 {
		return body
	}
	return out
}

func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

// Retryable reports whether an attempt's outcome is worth another try.
func Retryable(status int, err error) bool {
	if err != nil {
		if errors.Is(err, context.Canceled) || IsPolicyError(err) {
			return false
		}
		return true
	}
	return status == http.StatusTooManyRequests || status >= 500
}

// IsPolicyError reports whether err is a deliberate stop (redirect policy or
// blocked destination) rather than a failure of the remote host.
func IsPolicyError(err error) bool {
	return errors.Is(err, ErrTooManyRedirects) || errors.Is(err, ErrOffDomain) ||
		errors.Is(err, ErrRobotsRedirect) || errors.Is(err, ErrBlockedAddress)
}

// Backoff returns the delay before retry number attempt (1-based): the
// server's Retry-After when given, otherwise exponential with full jitter.
func Backoff(attempt int, base, maxDelay, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return min(retryAfter, maxDelay)
	}
	d := base << min(attempt-1, 20)
	if d <= 0 || d > maxDelay {
		d = maxDelay
	}
	return d/2 + time.Duration(rand.Int64N(int64(d/2)+1))
}

// Fetch gets a URL with retries on 429, 5xx and network errors.
func (c *Client) Fetch(ctx context.Context, rawURL string) (*Response, error) {
	var (
		resp *Response
		err  error
	)
	for attempt := 1; ; attempt++ {
		resp, err = c.Get(ctx, rawURL)
		if resp != nil {
			resp.Attempts = attempt
		}
		status := 0
		var retryAfter time.Duration
		if resp != nil {
			status, retryAfter = resp.Status, resp.RetryAfter
		}
		if !Retryable(status, err) || attempt > c.cfg.MaxRetries || ctx.Err() != nil {
			return resp, err
		}
		if serr := c.sleep(ctx, Backoff(attempt, c.cfg.BackoffBase, c.cfg.BackoffMax, retryAfter)); serr != nil {
			return resp, err
		}
	}
}

// GetRobots implements robots.Getter with a single un-retried request.
func (c *Client) GetRobots(ctx context.Context, rawURL string) (int, []byte, error) {
	resp, err := c.Get(ctx, rawURL)
	if err != nil {
		return 0, nil, err
	}
	if resp.SkipReason != "" {
		// A robots.txt served as a non-text type is treated as absent.
		return http.StatusNotFound, nil, nil
	}
	return resp.Status, resp.Body, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
