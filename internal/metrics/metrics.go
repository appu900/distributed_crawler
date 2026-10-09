// Package metrics defines the Prometheus metrics of every stage and the HTTP
// endpoint that exposes them.
package metrics

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry holds all LeadGraph metrics.
var Registry = prometheus.NewRegistry()

func counter(name, help string, labels ...string) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "leadgraph", Name: name, Help: help}, labels)
	Registry.MustRegister(c)
	return c
}

func gauge(name, help string, labels ...string) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Namespace: "leadgraph", Name: name, Help: help}, labels)
	Registry.MustRegister(g)
	return g
}

func histogram(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Namespace: "leadgraph", Name: name, Help: help, Buckets: buckets}, labels)
	Registry.MustRegister(h)
	return h
}

var (
	// Frontier.
	FrontierAdded      = counter("frontier_urls_total", "URLs offered to the frontier by outcome.", "outcome")
	FrontierDispatched = counter("frontier_dispatched_total", "URLs handed to the fetch stream.")
	FrontierHosts      = gauge("frontier_hosts", "Hosts with queued URLs.")

	// Fetcher.
	FetchAttempts  = counter("fetch_http_requests_total", "HTTP attempts by status class or error.", "code")
	FetchPages     = counter("fetch_pages_total", "Fetch results by outcome.", "outcome")
	FetchDuration  = histogram("fetch_duration_seconds", "HTTP attempt latency.", prometheus.ExponentialBuckets(0.01, 2, 12))
	FetchBytes     = counter("fetch_bytes_total", "Decoded response bytes stored.")
	RobotsBlocked  = counter("fetch_robots_blocked_total", "URLs skipped because robots.txt disallows them.")
	BreakerOpened  = counter("fetch_breaker_opened_total", "Times a per-host circuit breaker opened.")
	HeadlessRender = counter("fetch_headless_renders_total", "Headless renders by outcome.", "outcome")
	KillSwitch     = gauge("fetch_kill_switch", "1 while the kill switch halts fetching.")
	Backpressure   = counter("fetch_backpressure_waits_total", "Times fetching paused for downstream lag.")

	// Parser.
	ParsePages    = counter("parse_pages_total", "Parsed pages by outcome.", "outcome")
	ParseErrors   = counter("parse_errors_total", "Pages that failed to parse.", "reason")
	ParseDuration = histogram("parse_duration_seconds", "Extraction time per page.", prometheus.ExponentialBuckets(0.0002, 2, 14))
	EntitiesFound = counter("parse_entities_extracted_total", "Entities extracted by type.", "type")
	LinksFound    = counter("parse_links_discovered_total", "In-site links offered to the frontier.")

	// Resolver.
	ResolveMerges     = counter("resolve_merges_total", "Entity merges by kind.", "kind")
	ResolveSuppressed = counter("resolve_suppressed_total", "Entities dropped by the suppression list.", "kind")
	ResolveGeoDropped = counter("resolve_country_dropped_total", "Extractions dropped by country policy.")
	ResolveMutations  = counter("resolve_mutation_rows_total", "Rows emitted to the graph writer.", "kind")

	// Graph writer.
	GraphWriteDuration = histogram("graph_write_duration_seconds", "Transaction latency.", prometheus.ExponentialBuckets(0.002, 2, 14))
	GraphRows          = counter("graph_rows_written_total", "Rows committed by kind.", "kind")
	GraphRetries       = counter("graph_write_retries_total", "Transactions retried after a transient error.")
	GraphErrors        = counter("graph_write_errors_total", "Transactions that failed.")

	// Queues.
	StreamLag     = gauge("stream_lag", "Backlog of a stream (undelivered + pending).", "stream")
	DLQSize       = gauge("dlq_size", "Messages in the dead-letter stream.")
	StreamHandled = counter("stream_messages_total", "Messages handled by outcome.", "stream", "outcome")
	StreamDLQ     = counter("stream_dead_lettered_total", "Messages moved to the DLQ.", "stream")
)

func init() {
	Registry.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
}

// CodeLabel buckets an HTTP status (or transport error) into a label value.
func CodeLabel(status int, err error) string {
	if err != nil || status == 0 {
		return "error"
	}
	if status == 429 || status == 404 || status == 403 || status == 200 {
		return strconv.Itoa(status)
	}
	return strconv.Itoa(status/100) + "xx"
}

// Serve exposes /metrics and /healthz on addr until ctx is cancelled. ready
// reports whether dependencies are reachable.
func Serve(ctx context.Context, addr string, ready func(context.Context) error) error {
	if addr == "" {
		<-ctx.Done()
		return nil
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if ready != nil {
			c, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if err := ready(c); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()
	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
