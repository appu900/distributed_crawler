// Package urlx normalizes URLs and derives registrable domains.
package urlx

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net"
	"net/url"
	"path"
	"sort"
	"strings"

	"golang.org/x/net/idna"
	"golang.org/x/net/publicsuffix"
)

// MaxURLLength is the longest URL the crawler will accept.
const MaxURLLength = 2048

var (
	ErrUnsupportedScheme = errors.New("urlx: unsupported scheme")
	ErrNoHost            = errors.New("urlx: missing host")
	ErrTooLong           = errors.New("urlx: url too long")
)

// trackingParams are query parameters that never change page content.
var trackingParams = map[string]struct{}{
	"gclid": {}, "gclsrc": {}, "dclid": {}, "gbraid": {}, "wbraid": {}, "fbclid": {},
	"msclkid": {}, "yclid": {}, "igshid": {}, "mc_cid": {}, "mc_eid": {}, "mkt_tok": {},
	"_ga": {}, "_gl": {}, "_hsenc": {}, "_hsmi": {}, "hsctatracking": {}, "__hssc": {},
	"__hstc": {}, "__hsfp": {}, "vero_id": {}, "vero_conv": {}, "oly_enc_id": {},
	"oly_anon_id": {}, "s_cid": {}, "trk": {}, "trkinfo": {}, "phpsessid": {},
	"jsessionid": {}, "sessionid": {}, "sid": {}, "ref_src": {}, "ref_url": {},
}

func isTracking(key string) bool {
	k := strings.ToLower(key)
	if strings.HasPrefix(k, "utm_") || strings.HasPrefix(k, "pk_") || strings.HasPrefix(k, "mtm_") {
		return true
	}
	_, ok := trackingParams[k]
	return ok
}

// Normalize resolves raw against base (which may be nil for absolute URLs) and
// returns the canonical form: lowercase scheme and IDNA host, no default port,
// no fragment or userinfo, cleaned path, tracking parameters removed and the
// remaining query sorted. Normalize is idempotent.
func Normalize(raw string, base *url.URL) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", ErrNoHost
	}
	if len(raw) > MaxURLLength {
		return "", ErrTooLong
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if base != nil {
		u = base.ResolveReference(u)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", ErrUnsupportedScheme
	}
	host, err := NormalizeHost(u.Hostname())
	if err != nil {
		return "", err
	}
	port := u.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	out := url.URL{Scheme: scheme}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port != "" {
		out.Host = host + ":" + port
	} else {
		out.Host = host
	}

	p := u.Path
	if p == "" {
		p = "/"
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	trailing := strings.HasSuffix(p, "/")
	p = path.Clean(p)
	if trailing && p != "/" {
		p += "/"
	}
	out.Path = p

	if u.RawQuery != "" {
		q, err := url.ParseQuery(u.RawQuery)
		if err != nil {
			return "", err
		}
		keys := make([]string, 0, len(q))
		for k := range q {
			if !isTracking(k) {
				keys = append(keys, k)
			}
		}
		sort.Strings(keys)
		var sb strings.Builder
		for _, k := range keys {
			vals := q[k]
			sort.Strings(vals)
			for _, v := range vals {
				if sb.Len() > 0 {
					sb.WriteByte('&')
				}
				sb.WriteString(url.QueryEscape(k))
				if v != "" {
					sb.WriteByte('=')
					sb.WriteString(url.QueryEscape(v))
				}
			}
		}
		out.RawQuery = sb.String()
	}
	s := out.String()
	if len(s) > MaxURLLength {
		return "", ErrTooLong
	}
	return s, nil
}

// NormalizeHost lowercases a hostname, strips a trailing dot and converts
// internationalized names to their ASCII (punycode) form.
func NormalizeHost(host string) (string, error) {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	if host == "" {
		return "", ErrNoHost
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String(), nil
	}
	if strings.HasPrefix(host, ".") || strings.Contains(host, "..") {
		return "", ErrNoHost
	}
	ascii, err := idna.Lookup.ToASCII(host)
	if err != nil {
		return "", err
	}
	if ascii == "" || strings.ContainsAny(ascii, " /\\@?#%") {
		return "", ErrNoHost
	}
	return ascii, nil
}

// Registrable returns the registrable domain (eTLD+1) of a hostname. IP
// addresses and single-label hosts are returned unchanged.
func Registrable(host string) string {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	if net.ParseIP(host) != nil {
		return host
	}
	d, err := publicsuffix.EffectiveTLDPlusOne(host)
	if err != nil {
		return host
	}
	return d
}

// TLD returns the public suffix of a hostname ("co.uk", "com").
func TLD(host string) string {
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if net.ParseIP(host) != nil {
		return ""
	}
	s, _ := publicsuffix.PublicSuffix(host)
	return s
}

// SameSite reports whether two hostnames share a registrable domain.
func SameSite(a, b string) bool { return Registrable(a) == Registrable(b) }

// Hash returns a short stable hex digest of s.
func Hash(parts ...string) string {
	h := sha256.New()
	for i, p := range parts {
		if i > 0 {
			h.Write([]byte{0})
		}
		h.Write([]byte(p))
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

// SeedURL turns a seed line ("example.com", "http://example.com/x") into a
// normalized homepage URL. A bare domain defaults to https.
func SeedURL(seed string) (string, error) {
	seed = strings.TrimSpace(seed)
	if seed == "" {
		return "", ErrNoHost
	}
	if !strings.Contains(seed, "://") {
		seed = "https://" + seed
	}
	return Normalize(seed, nil)
}
