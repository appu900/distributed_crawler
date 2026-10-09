// Package robots fetches, caches and evaluates robots.txt.
package robots

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/temoto/robotstxt"
)

// MaxBody is the most of a robots.txt that is read (Google reads 500 KiB).
const MaxBody = 500 << 10

// Getter performs the HTTP GET for a robots.txt URL. err is non-nil only for
// transport failures; HTTP error statuses are returned as status.
type Getter interface {
	GetRobots(ctx context.Context, url string) (status int, body []byte, err error)
}

// Cache stores raw robots responses so every worker shares one fetch per host.
type Cache interface {
	Get(ctx context.Context, key string) (string, bool, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
}

// Rules is the parsed policy of one origin for our user agent.
type Rules struct {
	group      *robotstxt.Group
	CrawlDelay time.Duration
	Sitemaps   []string
	// Status is the HTTP status robots.txt was served with (0 = unreachable).
	Status int
	// Unavailable is set when the server errored: everything is disallowed
	// until robots.txt can be read.
	Unavailable bool
}

// Allowed reports whether the path (with query) may be fetched.
func (r *Rules) Allowed(u *url.URL) bool {
	if r == nil {
		return true
	}
	if r.Unavailable {
		return false
	}
	if r.group == nil {
		return true
	}
	p := u.EscapedPath()
	if p == "" {
		p = "/"
	}
	if u.RawQuery != "" {
		p += "?" + u.RawQuery
	}
	return r.group.Test(p)
}

type cached struct {
	Status    int       `json:"status"`
	Body      string    `json:"body"`
	FetchedAt time.Time `json:"fetched_at"`
}

type entry struct {
	rules   *Rules
	expires time.Time
}

// Manager answers robots questions for any origin.
type Manager struct {
	getter Getter
	cache  Cache
	agent  string
	ttl    time.Duration
	errTTL time.Duration
	now    func() time.Time

	mu    sync.Mutex
	local map[string]entry
}

// New creates a Manager. agent is the product token matched against
// User-agent lines ("LeadGraphBot"); ttl is how long a result is trusted.
func New(getter Getter, cache Cache, agent string, ttl time.Duration) *Manager {
	errTTL := 10 * time.Minute
	if ttl < errTTL {
		errTTL = ttl
	}
	return &Manager{getter: getter, cache: cache, agent: agent, ttl: ttl, errTTL: errTTL, now: time.Now, local: map[string]entry{}}
}

func origin(u *url.URL) string { return strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host) }

// Rules returns the policy for u's origin. fetched reports whether a network
// request was made, so the caller can pace its next request to that host.
func (m *Manager) Rules(ctx context.Context, u *url.URL) (rules *Rules, fetched bool, err error) {
	if u == nil || u.Host == "" {
		return nil, false, errors.New("robots: url has no host")
	}
	key := origin(u)
	now := m.now()
	m.mu.Lock()
	if e, ok := m.local[key]; ok && now.Before(e.expires) {
		m.mu.Unlock()
		return e.rules, false, nil
	}
	m.mu.Unlock()

	var c cached
	hit := false
	if m.cache != nil {
		if raw, ok, err := m.cache.Get(ctx, key); err == nil && ok && json.Unmarshal([]byte(raw), &c) == nil {
			hit = true
		}
	}
	if !hit {
		status, body, gerr := m.getter.GetRobots(ctx, key+"/robots.txt")
		if ctx.Err() != nil {
			return nil, false, ctx.Err()
		}
		if gerr != nil {
			status, body = 0, nil
		}
		if len(body) > MaxBody {
			body = body[:MaxBody]
		}
		c = cached{Status: status, Body: string(body), FetchedAt: now}
		fetched = true
		if m.cache != nil {
			if raw, err := json.Marshal(c); err == nil {
				_ = m.cache.Set(ctx, key, string(raw), m.ttlFor(status))
			}
		}
	}
	rules = parse(c, m.agent)
	expires := c.FetchedAt.Add(m.ttlFor(c.Status))
	if hit && !expires.After(now) {
		expires = now.Add(time.Minute)
	}
	m.mu.Lock()
	if len(m.local) > 50000 {
		for k, e := range m.local {
			if now.After(e.expires) {
				delete(m.local, k)
			}
		}
	}
	m.local[key] = entry{rules: rules, expires: expires}
	m.mu.Unlock()
	return rules, fetched, nil
}

func (m *Manager) ttlFor(status int) time.Duration {
	if unavailable(status) {
		return m.errTTL
	}
	return m.ttl
}

// unavailable: transport failure, 429 or 5xx. The site could not tell us its
// policy, so nothing is fetched until it can.
func unavailable(status int) bool { return status == 0 || status == 429 || status >= 500 }

func parse(c cached, agent string) *Rules {
	r := &Rules{Status: c.Status}
	switch {
	case unavailable(c.Status):
		r.Unavailable = true
		return r
	case c.Status >= 400:
		// No robots.txt: everything is allowed.
		return r
	}
	data, err := robotstxt.FromStatusAndBytes(200, []byte(c.Body))
	if err != nil {
		// Unparseable file: treat as absent rather than guessing.
		return r
	}
	r.group = data.FindGroup(agent)
	r.CrawlDelay = r.group.CrawlDelay
	for _, s := range data.Sitemaps {
		if s = strings.TrimSpace(s); s != "" {
			r.Sitemaps = append(r.Sitemaps, s)
		}
	}
	return r
}

// RedisCache stores robots responses in Redis under prefix.
type RedisCache struct {
	Client redis.Cmdable
	Prefix string
}

func (c RedisCache) Get(ctx context.Context, key string) (string, bool, error) {
	v, err := c.Client.Get(ctx, c.Prefix+key).Result()
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	return v, err == nil, err
}

func (c RedisCache) Set(ctx context.Context, key, value string, ttl time.Duration) error {
	return c.Client.Set(ctx, c.Prefix+key, value, ttl).Err()
}

// MemCache is an in-process Cache for tests.
type MemCache struct {
	mu sync.Mutex
	m  map[string]string
}

func (c *MemCache) Get(_ context.Context, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[key]
	return v, ok, nil
}

func (c *MemCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]string{}
	}
	c.m[key] = value
	return nil
}
