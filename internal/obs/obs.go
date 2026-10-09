// Package obs sets up structured logging and OpenTelemetry tracing.
package obs

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/appu/leadgraph/internal/config"
)

// NewLogger builds the process logger and installs it as the slog default.
func NewLogger(cfg config.Telemetry, w io.Writer) *slog.Logger {
	if w == nil {
		w = os.Stderr
	}
	var level slog.Level
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		level = slog.LevelInfo
	}
	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	if cfg.LogFormat == "text" {
		h = slog.NewTextHandler(w, opts)
	} else {
		h = slog.NewJSONHandler(w, opts)
	}
	log := slog.New(h).With("service", cfg.ServiceName)
	slog.SetDefault(log)
	return log
}

// InitTracing configures the global tracer provider. With no OTLP endpoint
// configured tracing stays a no-op. The returned function flushes and shuts
// the exporter down.
func InitTracing(ctx context.Context, cfg config.Telemetry, stage string) (shutdown func(context.Context) error, err error) {
	noop := func(context.Context) error { return nil }
	if cfg.OTLPEndpoint == "" {
		return noop, nil
	}
	opts := []otlptracehttp.Option{}
	endpoint := cfg.OTLPEndpoint
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		opts = append(opts, otlptracehttp.WithEndpointURL(endpoint))
	} else {
		opts = append(opts, otlptracehttp.WithEndpoint(endpoint), otlptracehttp.WithInsecure())
	}
	exp, err := otlptracehttp.New(ctx, opts...)
	if err != nil {
		return noop, err
	}
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		attribute.String("service.name", cfg.ServiceName),
		attribute.String("service.namespace", "leadgraph"),
		attribute.String("leadgraph.stage", stage),
	))
	if err != nil {
		res = resource.Default()
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp, sdktrace.WithBatchTimeout(2*time.Second)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(0.1))),
	)
	otel.SetTracerProvider(tp)
	return tp.Shutdown, nil
}
