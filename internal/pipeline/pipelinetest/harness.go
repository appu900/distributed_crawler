// Package pipelinetest is the shared end-to-end harness: a fast configuration
// pointed at the fixture sites, and helpers to run a pipeline to completion.
// The in-memory tests and the testcontainers integration tests both use it,
// so the same scenarios run against fake and real stores.
package pipelinetest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/graph"
	"github.com/appu/leadgraph/internal/pipeline"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/testsite"
	"github.com/appu/leadgraph/pkg/model"
)

// Seeds are the fixture sites as seed URLs.
var Seeds = []string{"http://" + testsite.Acme, "http://" + testsite.Globex}

// Logger returns a logger for tests: errors only, unless LEADGRAPH_TEST_LOG
// is set, in which case everything is printed.
func Logger() *slog.Logger {
	level := slog.LevelError
	if os.Getenv("LEADGRAPH_TEST_LOG") != "" {
		level = slog.LevelDebug
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}

// StartSite serves the fixture sites on a local port.
func StartSite(t testing.TB) (*testsite.Site, string) {
	t.Helper()
	site := testsite.New()
	srv := httptest.NewServer(site)
	t.Cleanup(srv.Close)
	return site, srv.Listener.Addr().String()
}

// Config returns a configuration tuned for tests: every fixture host
// resolves to siteAddr, and every delay and timeout is short.
func Config(siteAddr string) config.Config {
	cfg := config.Default()
	cfg.Crawl.MaxDepth = 2
	cfg.Crawl.DefaultDelay = 10 * time.Millisecond
	cfg.Crawl.MaxCrawlDelay = 50 * time.Millisecond
	cfg.Crawl.GlobalRPS = 500
	cfg.Crawl.HostLease = 5 * time.Second
	cfg.Crawl.DispatchInterval = 5 * time.Millisecond

	cfg.Fetch.AllowPrivateNetworks = true
	cfg.Fetch.HostOverrides = map[string]string{
		testsite.Acme: siteAddr, "www." + testsite.Acme: siteAddr, testsite.Globex: siteAddr,
	}
	cfg.Fetch.Workers = 8
	cfg.Fetch.TotalTimeout = 2 * time.Second
	cfg.Fetch.HeaderTimeout = time.Second
	cfg.Fetch.BackoffBase = time.Millisecond
	cfg.Fetch.BackoffMax = 10 * time.Millisecond
	cfg.Fetch.BreakerFailures = 50
	cfg.Fetch.BreakerCooldown = 50 * time.Millisecond
	cfg.Fetch.MaxRequeues = 1

	cfg.Extract.AllowReservedTLDs = true
	cfg.Parse.Workers = 4
	cfg.Resolve.Workers = 2
	cfg.Graph.BatchWait = 20 * time.Millisecond
	cfg.Queue.Block = 20 * time.Millisecond
	cfg.Queue.ClaimMinIdle = 300 * time.Millisecond
	cfg.Queue.DrainTimeout = 5 * time.Second
	cfg.Store.FlushInterval = 10 * time.Millisecond
	cfg.Telemetry.LogLevel = "error"
	return cfg
}

// Runner is a set of stages running in the background.
type Runner struct {
	cancel context.CancelFunc
	wg     sync.WaitGroup
	mu     sync.Mutex
	errs   []error
}

// Go starts one stage.
func (r *Runner) Go(ctx context.Context, run func(context.Context) error) {
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		if err := run(ctx); err != nil && ctx.Err() == nil {
			r.mu.Lock()
			r.errs = append(r.errs, err)
			r.mu.Unlock()
		}
	}()
}

// Stop shuts the stages down gracefully and reports any stage error.
func (r *Runner) Stop(t testing.TB) {
	t.Helper()
	r.cancel()
	r.wg.Wait()
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, err := range r.errs {
		t.Errorf("stage failed: %v", err)
	}
}

// Start runs the given stages (all of them when none are named) until Stop.
func Start(t testing.TB, p *pipeline.Pipeline, stages ...func(context.Context) error) *Runner {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &Runner{cancel: cancel}
	if len(stages) == 0 {
		stages = []func(context.Context) error{p.RunFrontier, p.RunFetch, p.RunParse, p.RunResolve, p.RunWrite}
	}
	for _, st := range stages {
		r.Go(ctx, st)
	}
	t.Cleanup(func() { cancel(); r.wg.Wait() })
	return r
}

// WaitIdle fails the test if the pipeline does not drain in time.
func WaitIdle(t testing.TB, p *pipeline.Pipeline, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := p.WaitIdle(ctx, 400*time.Millisecond); err != nil {
		lag, _ := p.Lag(context.Background())
		fs, _ := p.Frontier.Stats(context.Background())
		t.Fatalf("pipeline did not become idle within %s: %v (stream backlog %v, frontier %+v)", timeout, err, lag, fs)
	}
}

// Crawl seeds the pipeline, runs every stage until idle and stops them.
func Crawl(t testing.TB, p *pipeline.Pipeline, seeds ...string) {
	t.Helper()
	if _, err := p.Seed(context.Background(), seeds); err != nil {
		t.Fatalf("seeding: %v", err)
	}
	r := Start(t, p)
	WaitIdle(t, p, 60*time.Second)
	r.Stop(t)
}

// SnapshotMemory reduces an in-memory graph to what must be identical
// between two runs of the same crawl: which nodes and relationships exist
// and the identifying values they carry. Timestamps and tie-broken scalars
// are left out because they legitimately depend on processing order.
func SnapshotMemory(m *graph.Memory) []string {
	var out []string
	for _, spec := range model.Labels {
		for _, n := range m.Nodes(spec.Label) {
			out = append(out, SnapshotNode(spec.Label, n.Key, n.Value("name"), n.Value("full_name"), n.Value("type"), n.Value("status"), n.Value("skip_reason")))
		}
	}
	for _, spec := range model.Edges {
		for _, e := range m.Edges(spec.Type) {
			if e.FromLabel == spec.From && e.ToLabel == spec.To {
				out = append(out, SnapshotEdge(e.Type, e.FromLabel, e.FromKey, e.ToLabel, e.ToKey))
			}
		}
	}
	sort.Strings(out)
	return out
}

// SnapshotNode renders one node line of a snapshot.
func SnapshotNode(label, key string, name, fullName, typ, status, skipReason any) string {
	line := "node " + label + " " + key
	for _, f := range []struct {
		k string
		v any
	}{{"name", name}, {"full_name", fullName}, {"type", typ}, {"status", status}, {"skip_reason", skipReason}} {
		if f.v != nil && f.v != "" {
			line += fmt.Sprintf(" %s=%v", f.k, f.v)
		}
	}
	return line
}

// SnapshotEdge renders one relationship line of a snapshot.
func SnapshotEdge(typ, fromLabel, fromKey, toLabel, toKey string) string {
	return "edge " + typ + " " + fromLabel + "/" + fromKey + " -> " + toLabel + "/" + toKey
}

// Diff reports every difference between two snapshots as test errors.
func Diff(t testing.TB, want, got []string) {
	t.Helper()
	have := map[string]int{}
	for _, g := range got {
		have[g]++
	}
	wanted := map[string]bool{}
	for _, w := range want {
		wanted[w] = true
		if have[w] == 0 {
			t.Errorf("lost:       %s", w)
		}
	}
	for g, n := range have {
		if !wanted[g] {
			t.Errorf("unexpected: %s", g)
		}
		if n > 1 {
			t.Errorf("duplicated %d times: %s", n, g)
		}
	}
	if len(got) != len(want) {
		t.Errorf("graph has %d entries, the clean run has %d", len(got), len(want))
	}
}

// KillParserMidBatch crawls the fixture sites while killing the parser in the
// middle of its work, then lets a fresh parser finish. p is the pipeline
// under test; doomed must be a second pipeline over the same stores whose
// queue drain timeout is zero, so its in-flight handlers are cut off the way
// a SIGKILL would cut them. The third page the doomed parser handles is fully
// processed (extraction stored, links enqueued, result published) but never
// acknowledged, which is the worst case for both loss and duplication.
func KillParserMidBatch(t testing.TB, p, doomed *pipeline.Pipeline) {
	t.Helper()
	if _, err := p.Seed(context.Background(), Seeds); err != nil {
		t.Fatal(err)
	}
	others := Start(t, p, p.RunFrontier, p.RunFetch, p.RunResolve, p.RunWrite)

	parser, err := doomed.NewParser()
	if err != nil {
		t.Fatal(err)
	}
	ctx, kill := context.WithCancel(context.Background())
	defer kill()
	var handled, unacked atomic.Int32
	parser.Wrap = func(next queue.Handler) queue.Handler {
		return func(hctx context.Context, m queue.Message) error {
			err := next(hctx, m)
			if handled.Add(1) >= 3 {
				unacked.Add(1)
				kill()
				<-hctx.Done()
				return errors.New("killed before ack")
			}
			return err
		}
	}
	died := make(chan struct{})
	go func() { defer close(died); _ = parser.Run(ctx) }()
	select {
	case <-died:
	case <-time.After(60 * time.Second):
		t.Fatal("the parser was never killed (it handled fewer than three pages)")
	}
	pending, _ := p.Streams.Len(context.Background(), queue.Parse)
	if unacked.Load() == 0 || pending == 0 {
		t.Fatalf("the kill left no unacknowledged work (unacked=%d, parse backlog=%d)", unacked.Load(), pending)
	}
	t.Logf("parser killed after handling %d pages: %d processed but unacknowledged, %d messages left in the parse stream",
		handled.Load(), unacked.Load(), pending)

	survivor := Start(t, p, p.RunParse)
	WaitIdle(t, p, 90*time.Second)
	survivor.Stop(t)
	others.Stop(t)
}
