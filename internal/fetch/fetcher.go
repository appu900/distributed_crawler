package fetch

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/extract"
	"github.com/appu/leadgraph/internal/frontier"
	"github.com/appu/leadgraph/internal/metrics"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/robots"
	"github.com/appu/leadgraph/internal/store"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

var tracer = otel.Tracer("leadgraph/fetch")

// Fetcher is the fetch stage: it turns FetchRequests into stored raw records
// and FetchResults, enforcing robots.txt, politeness and the kill switch.
type Fetcher struct {
	cfg      config.Config
	client   *Client
	robots   *robots.Manager
	breakers *Breakers
	frontier *frontier.Frontier
	streams  *queue.Streams
	raw      *store.BatchWriter
	renderer Renderer
	log      *slog.Logger
	// Blocked vetoes domains (suppression list, country policy).
	Blocked func(ctx context.Context, domain string) bool

	killMu      sync.Mutex
	killChecked time.Time
	killed      bool
	now         func() time.Time
}

// NewFetcher wires a fetch worker. renderer may be nil (headless disabled).
func NewFetcher(cfg config.Config, client *Client, rb *robots.Manager, fr *frontier.Frontier,
	streams *queue.Streams, raw *store.BatchWriter, renderer Renderer, log *slog.Logger) *Fetcher {
	if log == nil {
		log = slog.Default()
	}
	f := &Fetcher{
		cfg: cfg, client: client, robots: rb, frontier: fr, streams: streams, raw: raw, renderer: renderer,
		breakers: NewBreakers(cfg.Fetch.BreakerFailures, cfg.Fetch.BreakerCooldown),
		log:      log.With("stage", "fetch"), now: time.Now,
	}
	client.OnAttempt = func(status int, err error, elapsed time.Duration) {
		metrics.FetchAttempts.WithLabelValues(metrics.CodeLabel(status, err)).Inc()
		metrics.FetchDuration.WithLabelValues().Observe(elapsed.Seconds())
	}
	client.SetRedirectCheck(func(ctx context.Context, u *url.URL) error {
		rules, _, err := rb.Rules(ctx, u)
		if err != nil {
			return err
		}
		if !rules.Allowed(u) {
			return ErrRobotsRedirect
		}
		return nil
	})
	return f
}

// Run consumes the fetch stream until ctx is cancelled.
func (f *Fetcher) Run(ctx context.Context) error {
	c := queue.NewConsumer(f.streams, queue.ConsumerConfig{
		Stream: queue.Fetch, Concurrency: f.cfg.Fetch.Workers, ReadBatch: f.cfg.Queue.ReadBatch,
		Block: f.cfg.Queue.Block, ClaimMinIdle: max(f.cfg.Queue.ClaimMinIdle, f.cfg.Crawl.HostLease),
		MaxDeliveries: f.cfg.Queue.MaxDeliveries, DrainTimeout: f.cfg.Queue.DrainTimeout,
	}, f.log)
	err := c.Run(ctx, func(hctx context.Context, m queue.Message) error {
		return f.handle(hctx, ctx.Done(), m)
	})
	f.raw.Flush()
	return err
}

// isKilled reads the kill switch, caching the answer briefly.
func (f *Fetcher) isKilled(ctx context.Context) bool {
	f.killMu.Lock()
	defer f.killMu.Unlock()
	if f.now().Sub(f.killChecked) < 500*time.Millisecond {
		return f.killed
	}
	on, _, err := f.frontier.Killed(ctx)
	if err != nil {
		// If the switch cannot be read, assume it is on: never fetch blind.
		on = true
	}
	f.killed, f.killChecked = on, f.now()
	if on {
		metrics.KillSwitch.WithLabelValues().Set(1)
	} else {
		metrics.KillSwitch.WithLabelValues().Set(0)
	}
	return on
}

func (f *Fetcher) delay(rules *robots.Rules) time.Duration {
	d := f.cfg.Crawl.DefaultDelay
	if rules != nil && rules.CrawlDelay > d {
		d = min(rules.CrawlDelay, f.cfg.Crawl.MaxCrawlDelay)
	}
	return d
}

func (f *Fetcher) publish(ctx context.Context, res model.FetchResult) error {
	payload, err := json.Marshal(res)
	if err != nil {
		return queue.Permanent(err)
	}
	outcome := "fetched"
	if res.SkipReason != "" {
		outcome = res.SkipReason
	} else if res.Status < 200 || res.Status >= 300 {
		outcome = "http_error"
	}
	metrics.FetchPages.WithLabelValues(outcome).Inc()
	return f.streams.Publish(ctx, queue.Parse, payload)
}

func (f *Fetcher) skip(ctx context.Context, req model.FetchRequest, reason string, status int) error {
	f.log.Info("url skipped", "url", req.URL, "reason", reason, "status", status)
	return f.publish(ctx, model.FetchResult{
		URL: req.URL, Kind: req.Kind, Depth: req.Depth, Probe: req.Probe, Status: status,
		FetchedAt: f.now().UTC(), SkipReason: reason,
	})
}

// retryLater requeues a failed request with backoff, or records the failure
// once its requeue allowance is spent. It reports whether the host's lease
// was taken over by the requeue.
func (f *Fetcher) retryLater(ctx context.Context, req model.FetchRequest, notBefore time.Time, reason string, status int) (requeued bool, err error) {
	if req.Attempt < f.cfg.Fetch.MaxRequeues {
		f.log.Info("url requeued", "url", req.URL, "reason", reason, "attempt", req.Attempt+1, "not_before", notBefore)
		return true, f.frontier.Requeue(ctx, req, notBefore)
	}
	return false, f.skip(ctx, req, reason, status)
}

func (f *Fetcher) handle(ctx context.Context, stopping <-chan struct{}, m queue.Message) (err error) {
	req, derr := frontier.Decode(string(m.Payload))
	if derr != nil {
		return queue.Permanent(derr)
	}
	u, perr := url.Parse(req.URL)
	if perr != nil || u.Host == "" {
		return queue.Permanent(fmt.Errorf("fetch: bad url %q", req.URL))
	}
	ctx, span := tracer.Start(ctx, "fetch")
	span.SetAttributes(attribute.String("url", req.URL), attribute.Int("depth", req.Depth))
	defer span.End()

	// The host stays leased until released; every exit path must release it
	// (or hand the lease to a requeue) or the host waits out the lease.
	var rules *robots.Rules
	release := true
	defer func() {
		if release {
			if rerr := f.frontier.Release(context.WithoutCancel(ctx), u.Host, f.now().Add(f.delay(rules))); rerr != nil {
				f.log.Warn("releasing host failed", "host", u.Host, "error", rerr)
			}
		}
	}()

	// Kill switch: give the URL back untouched. The frontier will not
	// dispatch again until the switch is cleared.
	if f.isKilled(ctx) {
		release = false
		return f.frontier.Return(ctx, req)
	}
	// Backpressure: do not fetch what the parsers cannot absorb.
	waited := false
	for {
		n, lerr := f.streams.Len(ctx, queue.Parse)
		if lerr != nil {
			return lerr
		}
		if n < f.cfg.Fetch.ParseLagThreshold {
			break
		}
		if !waited {
			waited = true
			metrics.Backpressure.WithLabelValues().Inc()
		}
		select {
		case <-stopping:
			release = false
			return f.frontier.Return(ctx, req)
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	if f.Blocked != nil && f.Blocked(ctx, req.Domain) {
		metrics.FetchPages.WithLabelValues(model.SkipSuppressed).Inc()
		_ = f.frontier.DropHost(ctx, u.Host)
		release = false
		return nil
	}
	if ok, retryAt := f.breakers.Allow(u.Host); !ok {
		requeued, rerr := f.retryLater(ctx, req, retryAt, model.SkipCircuitOpen, 0)
		release = !requeued
		return rerr
	}

	var fetched bool
	rules, fetched, err = f.robots.Rules(ctx, u)
	if err != nil {
		return err
	}
	if rules.Unavailable {
		// robots.txt could not be read: the site has not told us its
		// policy, so nothing is fetched yet.
		requeued, rerr := f.retryLater(ctx, req, f.now().Add(f.cfg.Fetch.BreakerCooldown), "robots_unavailable", rules.Status)
		release = !requeued
		return rerr
	}
	if !rules.Allowed(u) {
		metrics.RobotsBlocked.WithLabelValues().Inc()
		return f.skip(ctx, req, model.SkipRobots, 0)
	}
	if req.Seed {
		f.enqueueSitemaps(ctx, req, rules)
	}
	if fetched {
		// The robots.txt request was itself a hit on this host.
		if serr := sleepCtx(ctx, f.delay(rules)); serr != nil {
			return serr
		}
	}

	resp, ferr := f.client.Fetch(NoFollow(ctx), req.URL)
	fetchedAt := f.now().UTC()
	switch {
	case ferr != nil && IsPolicyError(ferr):
		reason := model.SkipFetchFailed
		switch {
		case errors.Is(ferr, ErrOffDomain):
			reason = model.SkipOffDomain
		case errors.Is(ferr, ErrTooManyRedirects):
			reason = model.SkipTooManyHops
		case errors.Is(ferr, ErrRobotsRedirect):
			reason = model.SkipRobots
			metrics.RobotsBlocked.WithLabelValues().Inc()
		case errors.Is(ferr, ErrBlockedAddress):
			reason = model.SkipBlocked
		}
		return f.skip(ctx, req, reason, 0)
	case ferr != nil:
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if f.breakers.Failure(u.Host) {
			metrics.BreakerOpened.WithLabelValues().Inc()
			f.log.Warn("circuit breaker opened", "host", u.Host, "error", ferr)
		}
		if req.Seed && u.Scheme == "https" && req.Attempt >= f.cfg.Fetch.MaxRequeues {
			// A seed given without a scheme defaults to https; try http
			// before giving up on the domain.
			alt := *u
			alt.Scheme = "http"
			_, _ = f.frontier.Add(ctx, model.FetchRequest{URL: alt.String(), Kind: model.KindPage, Seed: true, Domain: req.Domain}, false)
		}
		backoff := Backoff(req.Attempt+1, f.cfg.Fetch.BackoffBase*4, f.cfg.Fetch.BreakerCooldown, 0)
		requeued, rerr := f.retryLater(ctx, req, f.now().Add(backoff), model.SkipFetchFailed, 0)
		release = !requeued
		return rerr
	case resp.Status == 429 || resp.Status >= 500:
		if f.breakers.Failure(u.Host) {
			metrics.BreakerOpened.WithLabelValues().Inc()
			f.log.Warn("circuit breaker opened", "host", u.Host, "status", resp.Status)
		}
		wait := Backoff(req.Attempt+1, f.cfg.Fetch.BackoffBase*4, f.cfg.Fetch.BreakerCooldown, resp.RetryAfter)
		requeued, rerr := f.retryLater(ctx, req, f.now().Add(wait), "", resp.Status)
		release = !requeued
		return rerr
	}
	f.breakers.Success(u.Host)

	if resp.Status >= 300 && resp.Status < 400 && resp.Redirect != "" {
		return f.redirect(ctx, req, resp.Redirect)
	}

	final := req.URL
	if n, nerr := urlx.Normalize(resp.FinalURL, nil); nerr == nil {
		final = n
	}
	res := model.FetchResult{
		URL: req.URL, FinalURL: final, Kind: req.Kind, Depth: req.Depth, Probe: req.Probe,
		Status: resp.Status, FetchedAt: fetchedAt, ContentType: resp.ContentType, SkipReason: resp.SkipReason,
	}
	if resp.SkipReason != "" || resp.Status < 200 || resp.Status >= 300 {
		return f.publish(ctx, res)
	}

	body := resp.Body
	if f.renderer != nil && req.Kind == model.KindPage && strings.Contains(resp.ContentType, "html") &&
		NeedsRender(body, f.cfg.Fetch.Headless.MinTextBytes) {
		if rendered, rerr := f.renderer.Render(ctx, final); rerr != nil {
			metrics.HeadlessRender.WithLabelValues("error").Inc()
			f.log.Warn("headless render failed; keeping static response", "url", final, "error", rerr)
		} else if before, _ := extract.TextDensity(body); !moreText(rendered, before) {
			// Rendering added nothing: the static response is the page.
			metrics.HeadlessRender.WithLabelValues("no_gain").Inc()
		} else {
			metrics.HeadlessRender.WithLabelValues("ok").Inc()
			body, res.Rendered = rendered, true
		}
	}
	// Free the host before the (group-committed) store write.
	if rerr := f.frontier.Release(ctx, u.Host, f.now().Add(f.delay(rules))); rerr == nil {
		release = false
	}

	sum := sha256.Sum256(body)
	res.ContentHash = hex.EncodeToString(sum[:])
	ref, aerr := f.raw.Append(ctx, &model.RawRecord{
		URL: req.URL, FinalURL: final, Kind: req.Kind, Status: resp.Status, Headers: resp.Header, FetchedAt: fetchedAt,
		ContentHash: res.ContentHash, Truncated: resp.Truncated, Rendered: res.Rendered, Body: body,
	})
	if aerr != nil {
		return fmt.Errorf("fetch: storing raw response: %w", aerr)
	}
	metrics.FetchBytes.WithLabelValues().Add(float64(len(body)))
	res.S3Key, res.RawOffset, res.RawLength = ref.Key, ref.Offset, ref.Length
	return f.publish(ctx, res)
}

func moreText(rendered []byte, staticText int) bool {
	text, _ := extract.TextDensity(rendered)
	return text > staticText
}

// redirect hands a redirect target to the frontier instead of following it
// in place. The target is then deduplicated against everything already
// crawled, checked against its own robots.txt and fetched under its host's
// politeness delay. Redirects off the registrable domain are not followed.
func (f *Fetcher) redirect(ctx context.Context, req model.FetchRequest, location string) error {
	target, err := urlx.Normalize(location, nil)
	if err != nil {
		return f.skip(ctx, req, model.SkipFetchFailed, 0)
	}
	tu, err := url.Parse(target)
	if err != nil || urlx.Registrable(tu.Hostname()) != req.Domain {
		return f.skip(ctx, req, model.SkipOffDomain, 0)
	}
	if req.Hops >= f.cfg.Fetch.MaxRedirects {
		return f.skip(ctx, req, model.SkipTooManyHops, 0)
	}
	if req.Kind == model.KindPage && req.Attempt == 0 {
		f.frontier.Refund(ctx, req.Domain)
	}
	next := req
	next.URL, next.Hops, next.Attempt = target, req.Hops+1, 0
	metrics.FetchPages.WithLabelValues("redirect").Inc()
	_, err = f.frontier.Add(ctx, next, false)
	return err
}

// enqueueSitemaps adds the sitemaps a seed's robots.txt lists.
func (f *Fetcher) enqueueSitemaps(ctx context.Context, req model.FetchRequest, rules *robots.Rules) {
	for _, sm := range rules.Sitemaps {
		norm, err := urlx.Normalize(sm, nil)
		if err != nil {
			continue
		}
		su, err := url.Parse(norm)
		if err != nil || urlx.Registrable(su.Hostname()) != req.Domain {
			continue
		}
		if !f.frontier.CountSitemap(ctx, req.Domain) {
			return
		}
		if _, err := f.frontier.Add(ctx, model.FetchRequest{URL: norm, Kind: model.KindSitemap, Depth: 1, Domain: req.Domain}, false); err != nil {
			f.log.Warn("enqueueing sitemap failed", "url", norm, "error", err)
		}
	}
}
