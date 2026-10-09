// Package pipeline wires the stages together. The CLI and the end-to-end
// tests build the same Pipeline; only the backing stores differ.
package pipeline

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/erase"
	"github.com/appu/leadgraph/internal/extract"
	"github.com/appu/leadgraph/internal/fetch"
	"github.com/appu/leadgraph/internal/frontier"
	"github.com/appu/leadgraph/internal/graph"
	"github.com/appu/leadgraph/internal/metrics"
	"github.com/appu/leadgraph/internal/parse"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/resolve"
	"github.com/appu/leadgraph/internal/robots"
	"github.com/appu/leadgraph/internal/store"
	"github.com/appu/leadgraph/internal/suppress"
)

// Deps are the external systems a pipeline runs against. Blob and Graph may
// be nil for stages that do not use them.
type Deps struct {
	Cfg      config.Config
	RDB      redis.UniversalClient
	Blob     store.Blob
	Graph    graph.Store
	Renderer fetch.Renderer
	Rules    *extract.TechRules
	Log      *slog.Logger
}

// Pipeline holds the shared components of all stages.
type Pipeline struct {
	Deps
	Streams  *queue.Streams
	Suppress *suppress.List
	Frontier *frontier.Frontier
	Erase    *erase.Service

	rawOnce sync.Once
	raw     *store.BatchWriter
}

// NewRedis builds the Redis client from configuration.
func NewRedis(cfg config.Redis) *redis.Client {
	opts := &redis.Options{Addr: cfg.Addr, Username: cfg.Username, Password: cfg.Password, DB: cfg.DB}
	if cfg.TLS {
		host := cfg.Addr
		if h, _, err := net.SplitHostPort(cfg.Addr); err == nil {
			host = h
		}
		opts.TLSConfig = &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	}
	return redis.NewClient(opts)
}

// New assembles a pipeline.
func New(d Deps) *Pipeline {
	if d.Log == nil {
		d.Log = slog.Default()
	}
	prefix := d.Cfg.Redis.KeyPrefix
	p := &Pipeline{Deps: d}
	p.Streams = queue.NewStreams(d.RDB, prefix, d.Cfg.Queue.MaxLen)
	p.Suppress = suppress.New(d.RDB, prefix)
	p.Frontier = frontier.New(d.RDB, p.Streams, prefix, d.Cfg.Crawl, d.Log)
	p.Frontier.Blocked = p.Suppress.DomainBlocked
	p.Erase = &erase.Service{Suppress: p.Suppress, Blob: d.Blob, RDB: d.RDB, Prefix: prefix, Log: d.Log}
	if d.Graph != nil {
		p.Erase.Graph = d.Graph
	}
	return p
}

// SeedQueueKey is the Redis list external systems push seed domains to.
func (p *Pipeline) SeedQueueKey() string { return p.Cfg.Redis.KeyPrefix + ":seeds" }

// Seed adds seeds to the frontier and returns how many URLs were enqueued.
// Bad or blocked seeds are reported together; good ones are still added.
func (p *Pipeline) Seed(ctx context.Context, seeds []string) (int, error) {
	total := 0
	var errs []error
	for _, s := range seeds {
		n, err := p.Frontier.Seed(ctx, s)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		total += n
	}
	return total, errors.Join(errs...)
}

// ReadSeeds parses a seed list: one domain or URL per line, '#' comments.
func ReadSeeds(r io.Reader) ([]string, error) {
	var out []string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = strings.TrimSpace(line[:i])
		}
		if line != "" {
			out = append(out, line)
		}
	}
	return out, sc.Err()
}

// RunSeedQueue seeds from the Redis list until ctx is cancelled.
func (p *Pipeline) RunSeedQueue(ctx context.Context) error {
	for ctx.Err() == nil {
		res, err := p.RDB.BLPop(ctx, 2*time.Second, p.SeedQueueKey()).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			p.Log.Error("reading seed queue failed", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
			continue
		}
		if len(res) == 2 {
			if n, err := p.Frontier.Seed(ctx, res[1]); err != nil {
				p.Log.Warn("seed rejected", "seed", res[1], "error", err)
			} else {
				p.Log.Info("seed accepted", "seed", res[1], "urls", n)
			}
		}
	}
	return nil
}

// RunFrontier runs the dispatcher and the seed queue reader.
func (p *Pipeline) RunFrontier(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = p.RunSeedQueue(ctx)
	}()
	err := p.Frontier.Run(ctx, p.Cfg.Queue.MaxLen)
	wg.Wait()
	return err
}

func (p *Pipeline) rawWriter() *store.BatchWriter {
	p.rawOnce.Do(func() {
		p.raw = store.NewBatchWriter(p.Blob, store.BatchOptions{
			MaxRecords: p.Cfg.Store.BatchRecords, MaxBytes: p.Cfg.Store.BatchBytes, FlushInterval: p.Cfg.Store.FlushInterval,
		})
	})
	return p.raw
}

// NewFetcher builds a fetch worker.
func (p *Pipeline) NewFetcher() (*fetch.Fetcher, error) {
	if p.Blob == nil {
		return nil, errors.New("pipeline: the fetch stage needs an object store")
	}
	proxies, err := fetch.PoolFromConfig(p.Cfg.Fetch.Proxies)
	if err != nil {
		return nil, err
	}
	client := fetch.NewClient(p.Cfg.Fetch, proxies)
	rb := robots.New(client, robots.RedisCache{Client: p.RDB, Prefix: p.Cfg.Redis.KeyPrefix + ":robots:"},
		p.Cfg.Fetch.BotName, p.Cfg.Fetch.RobotsTTL)
	f := fetch.NewFetcher(p.Cfg, client, rb, p.Frontier, p.Streams, p.rawWriter(), p.Renderer, p.Log)
	f.Blocked = p.Suppress.DomainBlocked
	return f, nil
}

// RunFetch runs the fetch stage.
func (p *Pipeline) RunFetch(ctx context.Context) error {
	f, err := p.NewFetcher()
	if err != nil {
		return err
	}
	return f.Run(ctx)
}

// NewParser builds a parse worker.
func (p *Pipeline) NewParser() (*parse.Worker, error) {
	if p.Blob == nil {
		return nil, errors.New("pipeline: the parse stage needs an object store")
	}
	return parse.NewWorker(p.Cfg, p.RDB, p.Streams, p.Blob, p.Frontier, p.Rules, p.Log), nil
}

// RunParse runs the parse stage.
func (p *Pipeline) RunParse(ctx context.Context) error {
	w, err := p.NewParser()
	if err != nil {
		return err
	}
	return w.Run(ctx)
}

// RunResolve runs the resolve stage.
func (p *Pipeline) RunResolve(ctx context.Context) error {
	return resolve.New(p.Cfg, p.RDB, p.Streams, p.Suppress, p.Log).Run(ctx)
}

// RunWrite runs the graph-writer stage.
func (p *Pipeline) RunWrite(ctx context.Context) error {
	if p.Graph == nil {
		return errors.New("pipeline: the write stage needs a graph store")
	}
	w := graph.NewWriter(p.Cfg, p.Streams, p.Graph, p.Suppress, p.Log)
	w.OnPurge = func(ctx context.Context, rep graph.EraseReport) {
		if _, _, err := p.Erase.CleanupPages(ctx, rep.Pages); err != nil {
			p.Log.Warn("removing stored pages of a purged company failed", "error", err)
		}
	}
	return w.Run(ctx)
}

// RunAll runs every stage in this process until ctx is cancelled. Stages
// shut down in pipeline order so each can drain into the next.
func (p *Pipeline) RunAll(ctx context.Context) error {
	stages := []struct {
		name string
		run  func(context.Context) error
	}{
		{"frontier", p.RunFrontier}, {"fetch", p.RunFetch}, {"parse", p.RunParse},
		{"resolve", p.RunResolve}, {"write", p.RunWrite}, {"lag", p.RunLagCollector},
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errc := make(chan error, len(stages))
	for _, st := range stages {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := st.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
				errc <- fmt.Errorf("%s: %w", st.name, err)
				cancel()
			}
		}()
	}
	wg.Wait()
	close(errc)
	var errs []error
	for err := range errc {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// Lag returns each stream's backlog.
func (p *Pipeline) Lag(ctx context.Context) (map[string]int64, error) {
	out := map[string]int64{}
	for _, st := range append(append([]string{}, queue.Stages...), queue.DLQ) {
		n, err := p.Streams.Len(ctx, st)
		if err != nil {
			return nil, err
		}
		out[st] = n
	}
	return out, nil
}

// RunLagCollector publishes stream lag and DLQ size gauges.
func (p *Pipeline) RunLagCollector(ctx context.Context) error {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		if lag, err := p.Lag(ctx); err == nil {
			for st, n := range lag {
				if st == queue.DLQ {
					metrics.DLQSize.WithLabelValues().Set(float64(n))
				} else {
					metrics.StreamLag.WithLabelValues(st).Set(float64(n))
				}
			}
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// Idle reports whether there is no work anywhere: nothing queued or leased
// in the frontier and nothing in any work stream.
func (p *Pipeline) Idle(ctx context.Context) (bool, error) {
	idle, err := p.Frontier.Idle(ctx)
	if err != nil || !idle {
		return false, err
	}
	for _, st := range queue.Stages {
		n, err := p.Streams.Len(ctx, st)
		if err != nil || n > 0 {
			return false, err
		}
	}
	if n, err := p.RDB.LLen(ctx, p.SeedQueueKey()).Result(); err != nil || n > 0 {
		return false, err
	}
	return true, nil
}

// WaitIdle blocks until the pipeline has been idle for stable, or ctx ends.
// Stages hand work forward before acknowledging it, so a single idle reading
// is already consistent; requiring it to hold filters out the moment between
// a host's lease being released and its queue being inspected.
func (p *Pipeline) WaitIdle(ctx context.Context, stable time.Duration) error {
	var since time.Time
	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
	for {
		idle, err := p.Idle(ctx)
		if err != nil {
			return err
		}
		switch {
		case !idle:
			since = time.Time{}
		case since.IsZero():
			since = time.Now()
		case time.Since(since) >= stable:
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}
