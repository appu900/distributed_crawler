package parse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/appu/leadgraph/internal/config"
	"github.com/appu/leadgraph/internal/extract"
	"github.com/appu/leadgraph/internal/frontier"
	"github.com/appu/leadgraph/internal/metrics"
	"github.com/appu/leadgraph/internal/queue"
	"github.com/appu/leadgraph/internal/store"
	"github.com/appu/leadgraph/internal/urlx"
	"github.com/appu/leadgraph/pkg/model"
)

// ExtractorVersion is part of the "already parsed" cache key. Bump it when
// extractor output changes so unchanged pages are parsed again.
const ExtractorVersion = "1"

var tracer = otel.Tracer("leadgraph/parse")

// Worker is the parse stage.
type Worker struct {
	cfg      config.Config
	rdb      redis.UniversalClient
	prefix   string
	streams  *queue.Streams
	blob     store.Blob
	frontier *frontier.Frontier
	rules    *extract.TechRules
	log      *slog.Logger
	// Wrap, if set, decorates the message handler (tests inject faults here).
	Wrap func(queue.Handler) queue.Handler
}

func NewWorker(cfg config.Config, rdb redis.UniversalClient, streams *queue.Streams, blob store.Blob,
	fr *frontier.Frontier, rules *extract.TechRules, log *slog.Logger) *Worker {
	if log == nil {
		log = slog.Default()
	}
	if rules == nil {
		rules = extract.DefaultTechRules
	}
	return &Worker{cfg: cfg, rdb: rdb, prefix: cfg.Redis.KeyPrefix, streams: streams, blob: blob, frontier: fr,
		rules: rules, log: log.With("stage", "parse")}
}

// Run consumes the parse stream until ctx is cancelled.
func (w *Worker) Run(ctx context.Context) error {
	c := queue.NewConsumer(w.streams, queue.ConsumerConfig{
		Stream: queue.Parse, Concurrency: w.cfg.Parse.Workers, ReadBatch: w.cfg.Queue.ReadBatch,
		Block: w.cfg.Queue.Block, ClaimMinIdle: w.cfg.Queue.ClaimMinIdle,
		MaxDeliveries: w.cfg.Queue.MaxDeliveries, DrainTimeout: w.cfg.Queue.DrainTimeout,
	}, w.log)
	h := queue.Handler(w.Handle)
	if w.Wrap != nil {
		h = w.Wrap(h)
	}
	return c.Run(ctx, h)
}

// ParsedKey is the Redis key remembering the content hash a page was last
// parsed with.
func ParsedKey(prefix, pageURL string) string {
	return prefix + ":parsed:" + ExtractorVersion + ":" + urlx.Hash(pageURL)
}

func (w *Worker) parsedKey(pageURL string) string { return ParsedKey(w.prefix, pageURL) }

func (w *Worker) publish(ctx context.Context, ex *model.Extraction) error {
	payload, err := json.Marshal(ex)
	if err != nil {
		return queue.Permanent(err)
	}
	return w.streams.Publish(ctx, queue.Resolve, payload)
}

// Handle processes one FetchResult. It is idempotent: handling the same
// message twice produces the same extraction object and the same downstream
// message content.
func (w *Worker) Handle(ctx context.Context, m queue.Message) error {
	var res model.FetchResult
	if err := json.Unmarshal(m.Payload, &res); err != nil {
		metrics.ParseErrors.WithLabelValues("bad_message").Inc()
		return queue.Permanent(fmt.Errorf("parse: decoding fetch result: %w", err))
	}
	pageURL := res.FinalURL
	if pageURL == "" {
		pageURL = res.URL
	}
	u, err := url.Parse(pageURL)
	if err != nil || u.Host == "" {
		metrics.ParseErrors.WithLabelValues("bad_url").Inc()
		return queue.Permanent(fmt.Errorf("parse: bad url %q", pageURL))
	}
	ctx, span := tracer.Start(ctx, "parse")
	span.SetAttributes(attribute.String("url", pageURL))
	defer span.End()

	page := &model.Extraction{
		URL: pageURL, Host: u.Hostname(), Domain: urlx.Registrable(u.Hostname()),
		PageType: extract.ClassifyPath(u.Path), Status: res.Status, SkipReason: res.SkipReason,
		Depth: res.Depth, Probe: res.Probe, FetchedAt: res.FetchedAt, ContentHash: res.ContentHash,
		RawKey: res.S3Key, RawOffset: res.RawOffset, RawLength: res.RawLength,
	}
	if res.SkipReason != "" || res.Status < 200 || res.Status >= 300 || res.S3Key == "" {
		if res.Kind == model.KindSitemap {
			return nil
		}
		metrics.ParsePages.WithLabelValues("not_fetched").Inc()
		return w.publish(ctx, page)
	}

	rec, err := store.ReadRecord(ctx, w.blob, store.Ref{Key: res.S3Key, Offset: res.RawOffset, Length: res.RawLength})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			metrics.ParseErrors.WithLabelValues("raw_missing").Inc()
			return queue.Permanent(fmt.Errorf("parse: raw record %s is gone", res.S3Key))
		}
		return fmt.Errorf("parse: reading raw record: %w", err)
	}
	if rec.Tombstone {
		metrics.ParsePages.WithLabelValues("erased").Inc()
		return nil
	}
	if res.Kind == model.KindSitemap {
		return w.handleSitemap(ctx, page, rec.Body)
	}

	key := w.parsedKey(pageURL)
	if prev, gerr := w.rdb.Get(ctx, key).Result(); gerr == nil && prev == res.ContentHash && res.ContentHash != "" {
		// Same bytes as last time: nothing new to learn, just refresh the page.
		page.Unchanged = true
		metrics.ParsePages.WithLabelValues("unchanged").Inc()
		return w.publish(ctx, page)
	}

	start := time.Now()
	ex := extract.Page(extract.Input{
		URL: pageURL, Body: rec.Body, Headers: rec.Headers, Status: res.Status, Depth: res.Depth,
		FetchedAt: res.FetchedAt, DefaultRegion: w.cfg.Extract.DefaultRegion, FallbackRegion: w.cfg.Extract.FallbackRegion,
		Rules: w.rules, Email: extract.EmailOptions{AllowReservedTLDs: w.cfg.Extract.AllowReservedTLDs},
	})
	metrics.ParseDuration.WithLabelValues().Observe(time.Since(start).Seconds())
	ex.Probe, ex.ContentHash = res.Probe, res.ContentHash
	ex.RawKey, ex.RawOffset, ex.RawLength = res.S3Key, res.RawOffset, res.RawLength
	ex.ExtractKey = store.ExtractKey(urlx.Hash(pageURL), res.ContentHash)

	if err := store.PutJSON(ctx, w.blob, ex.ExtractKey, ex); err != nil {
		return fmt.Errorf("parse: storing extraction: %w", err)
	}
	if err := w.enqueueLinks(ctx, ex); err != nil {
		return err
	}
	if err := w.publish(ctx, ex); err != nil {
		return err
	}
	// Recorded last: a crash before this line re-parses, which is harmless.
	if err := w.rdb.Set(ctx, key, res.ContentHash, 0).Err(); err != nil {
		w.log.Warn("recording parsed hash failed", "url", pageURL, "error", err)
	}
	count(ex)
	return nil
}

func count(ex *model.Extraction) {
	outcome := "parsed"
	if ex.NoIndex {
		outcome = "noindex"
	}
	metrics.ParsePages.WithLabelValues(outcome).Inc()
	for typ, n := range map[string]int{
		"email": len(ex.Emails), "phone": len(ex.Phones), "social": len(ex.Socials), "person": len(ex.People),
		"address": len(ex.Addresses), "technology": len(ex.Technologies), "company_name": len(ex.CompanyNames),
	} {
		if n > 0 {
			metrics.EntitiesFound.WithLabelValues(typ).Add(float64(n))
		}
	}
}

// enqueueLinks offers in-site links to the frontier, best pages first so a
// bounded host queue keeps the valuable ones.
func (w *Worker) enqueueLinks(ctx context.Context, ex *model.Extraction) error {
	if ex.Depth >= w.cfg.Crawl.MaxDepth || len(ex.Links) == 0 {
		return nil
	}
	reqs := make([]model.FetchRequest, 0, len(ex.Links))
	for _, l := range ex.Links {
		reqs = append(reqs, model.FetchRequest{URL: l.URL, Kind: model.KindPage, Depth: ex.Depth + 1, Domain: ex.Domain})
	}
	sort.SliceStable(reqs, func(i, j int) bool { return frontier.Priority(reqs[i]) < frontier.Priority(reqs[j]) })
	for _, r := range reqs {
		out, err := w.frontier.Add(ctx, r, false)
		if err != nil {
			return fmt.Errorf("parse: enqueueing links: %w", err)
		}
		if out == frontier.QueueFull {
			break
		}
	}
	metrics.LinksFound.WithLabelValues().Add(float64(len(reqs)))
	return nil
}

func (w *Worker) handleSitemap(ctx context.Context, page *model.Extraction, body []byte) error {
	pages, children := ParseSitemap(body, w.cfg.Crawl.MaxSitemapURLs)
	if page.Depth < 2 {
		for _, c := range children {
			norm, err := urlx.Normalize(c, nil)
			if err != nil {
				continue
			}
			cu, err := url.Parse(norm)
			if err != nil || urlx.Registrable(cu.Hostname()) != page.Domain {
				continue
			}
			if !w.frontier.CountSitemap(ctx, page.Domain) {
				break
			}
			if _, err := w.frontier.Add(ctx, model.FetchRequest{URL: norm, Kind: model.KindSitemap, Depth: page.Depth + 1, Domain: page.Domain}, false); err != nil {
				return err
			}
		}
	}
	var reqs []model.FetchRequest
	for _, p := range pages {
		norm, err := urlx.Normalize(p, nil)
		if err != nil {
			continue
		}
		pu, err := url.Parse(norm)
		if err != nil || urlx.Registrable(pu.Hostname()) != page.Domain {
			continue
		}
		reqs = append(reqs, model.FetchRequest{URL: norm, Kind: model.KindPage, Depth: 1, Domain: page.Domain})
	}
	sort.SliceStable(reqs, func(i, j int) bool { return frontier.Priority(reqs[i]) < frontier.Priority(reqs[j]) })
	for _, r := range reqs {
		out, err := w.frontier.Add(ctx, r, false)
		if err != nil {
			return err
		}
		if out == frontier.QueueFull {
			break
		}
	}
	metrics.ParsePages.WithLabelValues("sitemap").Inc()
	return nil
}
