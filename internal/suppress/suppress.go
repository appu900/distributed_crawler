// Package suppress is the do-not-store list. Emails and people are kept as
// hashes, so the list itself contains no personal data; domains are public
// and kept in the clear.
package suppress

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/normalize"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

// Kinds of suppression entries.
const (
	KindEmail      = "email"       // member: model.EmailID(address)
	KindPerson     = "person"      // member: person ID
	KindPersonName = "person_name" // member: model.PersonNameKey(company, name)
	KindDomain     = "domain"      // member: registrable domain
	// KindGeo holds domains blocked by country policy. It is maintained by
	// the resolver, not by operators.
	KindGeo = "geo"
)

// Kinds lists operator-managed kinds.
var Kinds = []string{KindEmail, KindPerson, KindPersonName, KindDomain}

// List is the suppression list in Redis.
type List struct {
	rdb    redis.UniversalClient
	prefix string

	mu    sync.Mutex
	cache map[string]cached
	ttl   time.Duration
}

type cached struct {
	blocked bool
	expires time.Time
}

func New(rdb redis.UniversalClient, prefix string) *List {
	return &List{rdb: rdb, prefix: prefix, cache: map[string]cached{}, ttl: 3 * time.Second}
}

func (l *List) key(kind string) string { return l.prefix + ":suppress:" + kind }

// Member converts a user-supplied value into the stored member for kind:
// an email address becomes its hash, a domain its registrable form.
func Member(kind, value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("suppress: empty value")
	}
	switch kind {
	case KindEmail:
		if !strings.Contains(value, "@") {
			return "", fmt.Errorf("suppress: %q is not an email address", value)
		}
		return model.EmailID(value), nil
	case KindDomain, KindGeo:
		host := strings.ToLower(value)
		if i := strings.Index(host, "://"); i >= 0 {
			host = host[i+3:]
		}
		if i := strings.IndexAny(host, "/?#"); i >= 0 {
			host = host[:i]
		}
		if host == "" {
			return "", fmt.Errorf("suppress: %q is not a domain", value)
		}
		return urlx.Registrable(host), nil
	case KindPerson, KindPersonName:
		return value, nil
	}
	return "", fmt.Errorf("suppress: unknown kind %q", kind)
}

// PersonNameMember is the member for "anyone with this name at this company".
func PersonNameMember(companyID, name string) string {
	return model.PersonNameKey(companyID, normalize.PersonKey(name))
}

// AddMember stores an already-converted member.
func (l *List) AddMember(ctx context.Context, kind, member string) error {
	l.invalidate()
	return l.rdb.SAdd(ctx, l.key(kind), member).Err()
}

// Add suppresses a value and returns the stored member.
func (l *List) Add(ctx context.Context, kind, value string) (string, error) {
	m, err := Member(kind, value)
	if err != nil {
		return "", err
	}
	return m, l.AddMember(ctx, kind, m)
}

// Remove lifts a suppression and reports whether it existed.
func (l *List) Remove(ctx context.Context, kind, value string) (bool, error) {
	m, err := Member(kind, value)
	if err != nil {
		return false, err
	}
	l.invalidate()
	n, err := l.rdb.SRem(ctx, l.key(kind), m).Result()
	return n > 0, err
}

// RemoveMember removes an already-converted member.
func (l *List) RemoveMember(ctx context.Context, kind, member string) error {
	l.invalidate()
	return l.rdb.SRem(ctx, l.key(kind), member).Err()
}

// HasMembers reports, for each member, whether it is suppressed.
func (l *List) HasMembers(ctx context.Context, kind string, members []string) ([]bool, error) {
	if len(members) == 0 {
		return nil, nil
	}
	args := make([]any, len(members))
	for i, m := range members {
		args[i] = m
	}
	return l.rdb.SMIsMember(ctx, l.key(kind), args...).Result()
}

// Has reports whether a single member is suppressed.
func (l *List) Has(ctx context.Context, kind, member string) (bool, error) {
	return l.rdb.SIsMember(ctx, l.key(kind), member).Result()
}

// Members lists the stored members of a kind.
func (l *List) Members(ctx context.Context, kind string) ([]string, error) {
	return l.rdb.SMembers(ctx, l.key(kind)).Result()
}

// Count returns the number of entries of a kind.
func (l *List) Count(ctx context.Context, kind string) (int64, error) {
	return l.rdb.SCard(ctx, l.key(kind)).Result()
}

func (l *List) invalidate() {
	l.mu.Lock()
	l.cache = map[string]cached{}
	l.mu.Unlock()
}

// DomainBlocked reports whether a registrable domain must not be crawled or
// stored: suppressed by an operator, or blocked by country policy. Answers
// are cached for a few seconds because the frontier asks for every link.
// If Redis cannot be reached the answer is "blocked": fail closed.
func (l *List) DomainBlocked(ctx context.Context, domain string) bool {
	domain = strings.ToLower(domain)
	now := time.Now()
	l.mu.Lock()
	if c, ok := l.cache[domain]; ok && now.Before(c.expires) {
		l.mu.Unlock()
		return c.blocked
	}
	l.mu.Unlock()
	pipe := l.rdb.Pipeline()
	a := pipe.SIsMember(ctx, l.key(KindDomain), domain)
	b := pipe.SIsMember(ctx, l.key(KindGeo), domain)
	if _, err := pipe.Exec(ctx); err != nil {
		return true
	}
	blocked := a.Val() || b.Val()
	l.mu.Lock()
	if len(l.cache) > 100000 {
		l.cache = map[string]cached{}
	}
	l.cache[domain] = cached{blocked: blocked, expires: now.Add(l.ttl)}
	l.mu.Unlock()
	return blocked
}
