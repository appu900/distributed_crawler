package fetch

import (
	"sync"
	"time"
)

type breakerState int

const (
	stateClosed breakerState = iota
	stateOpen
	stateHalfOpen
)

type hostBreaker struct {
	state    breakerState
	failures int
	openedAt time.Time
	probing  bool
	touched  time.Time
}

// Breakers is a set of per-host circuit breakers. A breaker opens after a
// run of consecutive failures, rejects requests for a cooldown, then lets a
// single probe through: success closes it, failure re-opens it.
type Breakers struct {
	threshold int
	cooldown  time.Duration
	now       func() time.Time

	mu    sync.Mutex
	hosts map[string]*hostBreaker
}

func NewBreakers(threshold int, cooldown time.Duration) *Breakers {
	return &Breakers{threshold: threshold, cooldown: cooldown, now: time.Now, hosts: map[string]*hostBreaker{}}
}

// Allow reports whether a request to host may proceed. When it may not,
// retryAt is when the breaker will next admit a probe.
func (b *Breakers) Allow(host string) (ok bool, retryAt time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	h := b.hosts[host]
	if h == nil {
		return true, time.Time{}
	}
	now := b.now()
	h.touched = now
	switch h.state {
	case stateOpen:
		reopen := h.openedAt.Add(b.cooldown)
		if now.Before(reopen) {
			return false, reopen
		}
		h.state, h.probing = stateHalfOpen, true
		return true, time.Time{}
	case stateHalfOpen:
		if h.probing {
			return false, now.Add(b.cooldown / 4)
		}
		h.probing = true
		return true, time.Time{}
	}
	return true, time.Time{}
}

// Success records a healthy response and closes the breaker.
func (b *Breakers) Success(host string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.hosts, host)
}

// Failure records a failed request. It returns true when this failure
// opened (or re-opened) the breaker.
func (b *Breakers) Failure(host string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	h := b.hosts[host]
	if h == nil {
		if len(b.hosts) > 100000 {
			b.evict(now)
		}
		h = &hostBreaker{}
		b.hosts[host] = h
	}
	h.touched = now
	h.failures++
	if h.state == stateHalfOpen || (h.state == stateClosed && h.failures >= b.threshold) {
		h.state, h.openedAt, h.probing = stateOpen, now, false
		return true
	}
	return false
}

// Open reports whether host's breaker is currently rejecting requests.
func (b *Breakers) Open(host string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	h := b.hosts[host]
	return h != nil && h.state == stateOpen && b.now().Before(h.openedAt.Add(b.cooldown))
}

func (b *Breakers) evict(now time.Time) {
	for k, h := range b.hosts {
		if now.Sub(h.touched) > 10*b.cooldown {
			delete(b.hosts, k)
		}
	}
}
