// Package frontier decides what gets fetched and when: URL dedup, per-host
// politeness, priorities, depth and per-domain budgets, and the kill switch.
//
// State lives in Redis:
//
//	{p}:hosts               ZSET host -> next-allowed fetch time (ms)
//	{p}:fq:{host}           ZSET encoded request -> priority (lower first)
//	{p}:seen:{epoch}        Bloom filter or SET of URL hashes
//	{p}:pages:{epoch}:{dom} counter of pages dispatched for a domain
//	{p}:rate:{second}       counter backing the global rate limit
//	{p}:kill                kill switch flag
package frontier

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/extract"
	"github.com/appu/leadgraph/internal/metrics"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

// Outcome of offering a URL to the frontier.
type Outcome int

const (
	Added Outcome = iota
	Duplicate
	QueueFull
	Rejected
)

func (o Outcome) String() string {
	return [...]string{"added", "duplicate", "queue_full", "rejected"}[o]
}

// Frontier is the shared crawl frontier.
type Frontier struct {
	rdb     redis.UniversalClient
	streams *queue.Streams
	prefix  string
	cfg     config.Crawl
	log     *slog.Logger
	bloom   atomic.Int32 // 0 unknown, 1 bloom, 2 plain set
	now     func() time.Time
	// Blocked, if set, vetoes domains (suppression list, country policy).
	Blocked func(ctx context.Context, domain string) bool
}

func New(rdb redis.UniversalClient, streams *queue.Streams, prefix string, cfg config.Crawl, log *slog.Logger) *Frontier {
	if log == nil {
		log = slog.Default()
	}
	return &Frontier{rdb: rdb, streams: streams, prefix: prefix, cfg: cfg, log: log, now: time.Now}
}

func (f *Frontier) hostsKey() string { return f.prefix + ":hosts" }
func (f *Frontier) seenKey() string  { return f.prefix + ":seen:" + f.cfg.Epoch }
func (f *Frontier) killKey() string  { return f.prefix + ":kill" }
func (f *Frontier) queuePrefix() string {
	return f.prefix + ":fq:"
}
func (f *Frontier) pagesPrefix() string { return f.prefix + ":pages:" + f.cfg.Epoch + ":" }

// encode packs a request into a ZSET member. The Lua dispatcher reads the
// leading fields without a JSON parser.
func encode(r model.FetchRequest) string {
	flags := "-"
	if r.Seed {
		flags = "s"
	} else if r.Probe {
		flags = "p"
	}
	return strings.Join([]string{r.Kind, strconv.Itoa(r.Depth), strconv.Itoa(r.Attempt), flags, strconv.Itoa(r.Hops), r.Domain, r.URL}, "\t")
}

// Decode unpacks a member produced by encode.
func Decode(member string) (model.FetchRequest, error) {
	parts := strings.SplitN(member, "\t", 7)
	if len(parts) != 7 {
		return model.FetchRequest{}, fmt.Errorf("frontier: malformed request %q", member)
	}
	depth, err1 := strconv.Atoi(parts[1])
	attempt, err2 := strconv.Atoi(parts[2])
	hops, err3 := strconv.Atoi(parts[4])
	if err1 != nil || err2 != nil || err3 != nil || (parts[0] != model.KindPage && parts[0] != model.KindSitemap) {
		return model.FetchRequest{}, fmt.Errorf("frontier: malformed request %q", member)
	}
	return model.FetchRequest{
		Kind: parts[0], Depth: depth, Attempt: attempt, Seed: parts[3] == "s", Probe: parts[3] == "p",
		Hops: hops, Domain: parts[5], URL: parts[6],
	}, nil
}

// Priority scores a request; lower is fetched first. Homepages and sitemaps
// lead, then the pages that usually carry company and contact data, then
// everything else, with shallower pages first within a class.
func Priority(r model.FetchRequest) float64 {
	class := 5
	switch {
	case r.Kind == model.KindSitemap:
		class = 0
	case r.Seed:
		class = 0
	default:
		switch extract.ClassifyURL(r.URL) {
		case model.PageHome:
			class = 0
		case model.PageAbout, model.PageTeam, model.PageContact, model.PageCareers:
			class = 1
		}
	}
	return float64(class*100 + min(r.Depth, 50))
}

// addScript dedups and enqueues atomically.
// KEYS: seen, host queue, hosts. ARGV: hash, member, score, now, bloom, maxQueue, force, host.
var addScript = redis.NewScript(`
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[6]) then return -1 end
if ARGV[7] ~= '1' then
  if ARGV[5] == '1' then
    if redis.call('BF.ADD', KEYS[1], ARGV[1]) == 0 then return 0 end
  else
    if redis.call('SADD', KEYS[1], ARGV[1]) == 0 then return 0 end
  end
end
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[2])
redis.call('ZADD', KEYS[3], 'NX', ARGV[4], ARGV[8])
return 1
`)

func (f *Frontier) useBloom(ctx context.Context) bool {
	switch f.bloom.Load() {
	case 1:
		return true
	case 2:
		return false
	}
	// Probe for RedisBloom once; fall back to a plain set without it.
	err := f.rdb.Do(ctx, "BF.RESERVE", f.seenKey(), "0.001", "10000000", "EXPANSION", "2").Err()
	if err == nil || strings.Contains(err.Error(), "exists") {
		f.bloom.Store(1)
		f.log.Info("frontier dedup uses RedisBloom")
		return true
	}
	f.bloom.Store(2)
	f.log.Info("RedisBloom not available; frontier dedup uses a plain set", "reason", err.Error())
	return false
}

// Add offers a request. force skips dedup (used for requeues).
func (f *Frontier) Add(ctx context.Context, r model.FetchRequest, force bool) (Outcome, error) {
	u, err := url.Parse(r.URL)
	if err != nil || u.Host == "" {
		metrics.FrontierAdded.WithLabelValues(Rejected.String()).Inc()
		return Rejected, nil
	}
	if r.Domain == "" {
		r.Domain = urlx.Registrable(u.Hostname())
	}
	if r.Kind == "" {
		r.Kind = model.KindPage
	}
	if r.Depth > f.cfg.MaxDepth || (f.Blocked != nil && f.Blocked(ctx, r.Domain)) {
		metrics.FrontierAdded.WithLabelValues(Rejected.String()).Inc()
		return Rejected, nil
	}
	host := strings.ToLower(u.Host)
	bloom, forced := "0", "0"
	if f.useBloom(ctx) {
		bloom = "1"
	}
	if force {
		forced = "1"
	}
	res, err := addScript.Run(ctx, f.rdb,
		[]string{f.seenKey(), f.queuePrefix() + host, f.hostsKey()},
		urlx.Hash(r.URL), encode(r), Priority(r), f.now().UnixMilli(), bloom, f.cfg.MaxQueuePerHost, forced, host,
	).Int()
	if err != nil {
		return Rejected, fmt.Errorf("frontier: add: %w", err)
	}
	out := Added
	switch res {
	case 0:
		out = Duplicate
	case -1:
		out = QueueFull
	}
	metrics.FrontierAdded.WithLabelValues(out.String()).Inc()
	return out, nil
}

// Seed enqueues a seed: its homepage plus the well-known paths.
func (f *Frontier) Seed(ctx context.Context, seed string) (added int, err error) {
	home, err := urlx.SeedURL(seed)
	if err != nil {
		return 0, fmt.Errorf("frontier: seed %q: %w", seed, err)
	}
	u, _ := url.Parse(home)
	domain := urlx.Registrable(u.Hostname())
	if f.Blocked != nil && f.Blocked(ctx, domain) {
		return 0, fmt.Errorf("frontier: seed %q: domain is suppressed or blocked", seed)
	}
	out, err := f.Add(ctx, model.FetchRequest{URL: home, Kind: model.KindPage, Depth: 0, Seed: true, Domain: domain}, false)
	if err != nil {
		return 0, err
	}
	if out == Added {
		added++
	}
	root := &url.URL{Scheme: u.Scheme, Host: u.Host}
	for _, p := range f.cfg.WellKnownPaths {
		probe, err := urlx.Normalize(p, root)
		if err != nil {
			continue
		}
		out, err := f.Add(ctx, model.FetchRequest{URL: probe, Kind: model.KindPage, Depth: 1, Probe: true, Domain: domain}, false)
		if err != nil {
			return added, err
		}
		if out == Added {
			added++
		}
	}
	return added, nil
}

// dispatchScript moves due URLs to the fetch stream.
// KEYS: hosts, fetch stream, kill, rate counter.
// ARGV: now, max hosts, lease ms, max pages, queue prefix, pages prefix, global rps.
// Returns {dispatched, exhausted domains...}.
var dispatchScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[3]) == 1 then return {0} end
local now = tonumber(ARGV[1])
local rps = tonumber(ARGV[7])
local used = tonumber(redis.call('GET', KEYS[4]) or '0')
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[2]))
local sent = 0
local out = {0}
for _, host in ipairs(due) do
  if used >= rps then break end
  local q = ARGV[5] .. host
  local popped = redis.call('ZPOPMIN', q, 1)
  if #popped == 0 then
    redis.call('ZREM', KEYS[1], host)
  else
    local member = popped[1]
    local kind, attempt, domain = string.match(member, '^(%a)\t%d+\t(%d+)\t.\t%d+\t([^\t]*)\t')
    local emit = true
    if kind == 'p' and attempt == '0' then
      local ck = ARGV[6] .. domain
      local c = tonumber(redis.call('GET', ck) or '0')
      if c >= tonumber(ARGV[4]) then
        -- Budget spent: nothing else from this host will be fetched.
        redis.call('DEL', q)
        redis.call('ZREM', KEYS[1], host)
        out[#out + 1] = domain
        emit = false
      else
        redis.call('INCR', ck)
      end
    end
    if emit then
      redis.call('XADD', KEYS[2], '*', 'd', member)
      redis.call('ZADD', KEYS[1], now + tonumber(ARGV[3]), host)
      used = redis.call('INCR', KEYS[4])
      if used == 1 then redis.call('EXPIRE', KEYS[4], 2) end
      sent = sent + 1
    end
  end
end
out[1] = sent
return out
`)

// Dispatch runs one dispatch round and returns how many URLs were emitted.
func (f *Frontier) Dispatch(ctx context.Context) (int, error) {
	now := f.now()
	res, err := dispatchScript.Run(ctx, f.rdb,
		[]string{f.hostsKey(), f.streams.Key(queue.Fetch), f.killKey(), f.prefix + ":rate:" + strconv.FormatInt(now.Unix(), 10)},
		now.UnixMilli(), f.cfg.DispatchBatch, f.cfg.HostLease.Milliseconds(), f.cfg.MaxPagesPerDomain,
		f.queuePrefix(), f.pagesPrefix(), f.cfg.GlobalRPS,
	).Slice()
	if err != nil {
		return 0, fmt.Errorf("frontier: dispatch: %w", err)
	}
	if len(res) == 0 {
		return 0, nil
	}
	n, _ := res[0].(int64)
	for _, d := range res[1:] {
		f.log.Debug("domain page budget exhausted", "domain", d)
	}
	if n > 0 {
		metrics.FrontierDispatched.WithLabelValues().Add(float64(n))
	}
	return int(n), nil
}

// Run dispatches until ctx is cancelled. It holds back while the fetch
// stream is at capacity, so the frontier (which is cheap) absorbs backlog
// instead of the streams.
func (f *Frontier) Run(ctx context.Context, maxFetchBacklog int64) error {
	if err := f.streams.EnsureGroup(ctx, queue.Fetch); err != nil {
		return err
	}
	t := time.NewTicker(f.cfg.DispatchInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if maxFetchBacklog > 0 {
			if n, err := f.streams.Len(ctx, queue.Fetch); err == nil && n >= maxFetchBacklog {
				continue
			}
		}
		// Drain everything that is due before sleeping again.
		for ctx.Err() == nil {
			n, err := f.Dispatch(ctx)
			if err != nil {
				if !errors.Is(err, context.Canceled) {
					f.log.Error("dispatch failed", "error", err)
				}
				break
			}
			if n < f.cfg.DispatchBatch/2 {
				break
			}
		}
		if n, err := f.rdb.ZCard(ctx, f.hostsKey()).Result(); err == nil {
			metrics.FrontierHosts.WithLabelValues().Set(float64(n))
		}
	}
}

// Release ends a host's lease: its next URL may be dispatched at next.
func (f *Frontier) Release(ctx context.Context, host string, next time.Time) error {
	return f.rdb.ZAddXX(ctx, f.hostsKey(), redis.Z{Score: float64(next.UnixMilli()), Member: strings.ToLower(host)}).Err()
}

// Requeue puts a request back (attempt incremented) and holds its host until
// notBefore. The request does not consume page budget again.
func (f *Frontier) Requeue(ctx context.Context, r model.FetchRequest, notBefore time.Time) error {
	r.Attempt++
	if _, err := f.Add(ctx, r, true); err != nil {
		return err
	}
	u, err := url.Parse(r.URL)
	if err != nil {
		return err
	}
	return f.rdb.ZAdd(ctx, f.hostsKey(), redis.Z{Score: float64(notBefore.UnixMilli()), Member: strings.ToLower(u.Host)}).Err()
}

// Return hands back a dispatched request that was never attempted (kill
// switch, shutdown). Its page-budget charge is refunded and its host becomes
// due immediately.
func (f *Frontier) Return(ctx context.Context, r model.FetchRequest) error {
	u, err := url.Parse(r.URL)
	if err != nil {
		return err
	}
	if _, err := f.Add(ctx, r, true); err != nil {
		return err
	}
	if r.Kind == model.KindPage && r.Attempt == 0 {
		if n, err := f.rdb.Decr(ctx, f.pagesPrefix()+r.Domain).Result(); err == nil && n < 0 {
			f.rdb.Set(ctx, f.pagesPrefix()+r.Domain, 0, 0)
		}
	}
	return f.rdb.ZAdd(ctx, f.hostsKey(), redis.Z{Score: float64(f.now().UnixMilli()), Member: strings.ToLower(u.Host)}).Err()
}

// Refund returns one page of budget to a domain. A redirect is not a page:
// the request that only led to another URL does not count.
func (f *Frontier) Refund(ctx context.Context, domain string) {
	if n, err := f.rdb.Decr(ctx, f.pagesPrefix()+domain).Result(); err == nil && n < 0 {
		f.rdb.Set(ctx, f.pagesPrefix()+domain, 0, 0)
	}
}

// CountSitemap charges one sitemap to a domain and reports whether the
// domain is still within its sitemap allowance.
func (f *Frontier) CountSitemap(ctx context.Context, domain string) bool {
	n, err := f.rdb.Incr(ctx, f.prefix+":sitemaps:"+f.cfg.Epoch+":"+domain).Result()
	return err == nil && n <= int64(f.cfg.MaxSitemaps)
}

// DropHost discards everything queued for a host.
func (f *Frontier) DropHost(ctx context.Context, host string) error {
	host = strings.ToLower(host)
	_, err := f.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, f.queuePrefix()+host)
		p.ZRem(ctx, f.hostsKey(), host)
		return nil
	})
	return err
}

// SetKill turns the kill switch on or off. While on, no URL is dispatched
// and fetchers refuse to make requests.
func (f *Frontier) SetKill(ctx context.Context, on bool, reason string) error {
	if on {
		if reason == "" {
			reason = "manual"
		}
		return f.rdb.Set(ctx, f.killKey(), reason+" @ "+f.now().UTC().Format(time.RFC3339), 0).Err()
	}
	return f.rdb.Del(ctx, f.killKey()).Err()
}

// Killed reports whether the kill switch is on, and why.
func (f *Frontier) Killed(ctx context.Context) (bool, string, error) {
	v, err := f.rdb.Get(ctx, f.killKey()).Result()
	if errors.Is(err, redis.Nil) {
		return false, "", nil
	}
	if err != nil {
		return false, "", err
	}
	return true, v, nil
}

// Stats is a snapshot of frontier size.
type Stats struct {
	Hosts  int64
	Queued int64
}

// Stats counts hosts and (approximately, bounded scan) queued URLs.
func (f *Frontier) Stats(ctx context.Context) (Stats, error) {
	var s Stats
	hosts, err := f.rdb.ZRange(ctx, f.hostsKey(), 0, 4999).Result()
	if err != nil {
		return s, err
	}
	s.Hosts, _ = f.rdb.ZCard(ctx, f.hostsKey()).Result()
	for _, h := range hosts {
		n, _ := f.rdb.ZCard(ctx, f.queuePrefix()+h).Result()
		s.Queued += n
	}
	return s, nil
}

// Idle reports whether no host has work queued or leased.
func (f *Frontier) Idle(ctx context.Context) (bool, error) {
	n, err := f.rdb.ZCard(ctx, f.hostsKey()).Result()
	return n == 0, err
}
