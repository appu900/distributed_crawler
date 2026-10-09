package fetch

import (
	"fmt"
	"net/url"
	"sync/atomic"
)

// ProxyPool chooses an outbound proxy per request. Implementations can wrap
// any provider; the crawler ships only a no-op and a static round-robin.
type ProxyPool interface {
	// Next returns the proxy for a request to target, or nil for a direct
	// connection.
	Next(target *url.URL) *url.URL
	// Report tells the pool how a request through proxy went.
	Report(proxy *url.URL, err error)
}

// NoProxy always connects directly.
type NoProxy struct{}

func (NoProxy) Next(*url.URL) *url.URL { return nil }
func (NoProxy) Report(*url.URL, error) {}

// StaticPool rotates through a fixed list of proxies.
type StaticPool struct {
	proxies []*url.URL
	next    atomic.Uint64
}

// NewStaticPool parses proxy URLs ("http://host:port", "socks5://host:port").
func NewStaticPool(urls []string) (*StaticPool, error) {
	p := &StaticPool{}
	for _, raw := range urls {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("fetch: bad proxy url %q", raw)
		}
		p.proxies = append(p.proxies, u)
	}
	if len(p.proxies) == 0 {
		return nil, fmt.Errorf("fetch: empty proxy list")
	}
	return p, nil
}

func (p *StaticPool) Next(*url.URL) *url.URL {
	return p.proxies[(p.next.Add(1)-1)%uint64(len(p.proxies))]
}

func (p *StaticPool) Report(*url.URL, error) {}

// PoolFromConfig returns NoProxy for an empty list.
func PoolFromConfig(urls []string) (ProxyPool, error) {
	if len(urls) == 0 {
		return NoProxy{}, nil
	}
	return NewStaticPool(urls)
}
