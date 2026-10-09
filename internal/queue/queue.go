// Package queue connects the pipeline stages with Redis Streams: consumer
// groups, acknowledgement, reclaiming of abandoned messages, a delivery limit
// with a dead-letter stream, and producer-side backpressure.
package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Stage stream names (without the key prefix).
const (
	Fetch   = "fetch"
	Parse   = "parse"
	Resolve = "resolve"
	Write   = "write"
	DLQ     = "dlq"
)

// Stages lists the work streams in pipeline order.
var Stages = []string{Fetch, Parse, Resolve, Write}

const (
	fieldData  = "d"
	fieldTrace = "tp"
)

var propagator = propagation.TraceContext{}

// Message is one stream entry.
type Message struct {
	ID      string
	Stream  string
	Payload []byte
	// Deliveries is how many times the entry has been handed to a consumer.
	Deliveries int64
	trace      string
}

// Streams names and publishes to the pipeline's streams.
type Streams struct {
	rdb    redis.UniversalClient
	prefix string
	maxLen int64
	// poll is how often a blocked producer re-checks the backlog.
	poll time.Duration
}

// NewStreams creates a publisher. maxLen is the backlog at which Publish
// blocks; 0 disables the bound.
func NewStreams(rdb redis.UniversalClient, prefix string, maxLen int64) *Streams {
	return &Streams{rdb: rdb, prefix: prefix, maxLen: maxLen, poll: 50 * time.Millisecond}
}

// Key returns the Redis key of a stream.
func (s *Streams) Key(stream string) string { return s.prefix + ":stream:" + stream }

// Group is the single consumer group of a stream.
func (s *Streams) Group(stream string) string { return stream + "-workers" }

// Client exposes the underlying client.
func (s *Streams) Client() redis.UniversalClient { return s.rdb }

// Len returns a stream's backlog. Consumers delete entries when they ack, so
// the length is exactly undelivered + pending.
func (s *Streams) Len(ctx context.Context, stream string) (int64, error) {
	return s.rdb.XLen(ctx, s.Key(stream)).Result()
}

// Publish appends payload to stream. When the backlog is at the configured
// maximum it waits: the bound is enforced by slowing producers, never by
// trimming, because trimming would discard unprocessed work.
func (s *Streams) Publish(ctx context.Context, stream string, payload []byte) error {
	if s.maxLen > 0 {
		for {
			n, err := s.Len(ctx, stream)
			if err != nil {
				return err
			}
			if n < s.maxLen {
				break
			}
			t := time.NewTimer(s.poll)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
	}
	return s.PublishNow(ctx, stream, payload)
}

// PublishNow appends without waiting for capacity.
func (s *Streams) PublishNow(ctx context.Context, stream string, payload []byte) error {
	values := []any{fieldData, payload}
	carrier := propagation.MapCarrier{}
	propagator.Inject(ctx, carrier)
	if tp := carrier.Get("traceparent"); tp != "" {
		values = append(values, fieldTrace, tp)
	}
	return s.rdb.XAdd(ctx, &redis.XAddArgs{Stream: s.Key(stream), Values: values}).Err()
}

// EnsureGroup creates the stream and its consumer group if missing.
func (s *Streams) EnsureGroup(ctx context.Context, stream string) error {
	err := s.rdb.XGroupCreateMkStream(ctx, s.Key(stream), s.Group(stream), "0").Err()
	if err != nil && !strings.Contains(err.Error(), "BUSYGROUP") {
		return fmt.Errorf("queue: creating group for %s: %w", stream, err)
	}
	return nil
}

func toMessage(stream string, x redis.XMessage, deliveries int64) Message {
	m := Message{ID: x.ID, Stream: stream, Deliveries: deliveries}
	switch v := x.Values[fieldData].(type) {
	case string:
		m.Payload = []byte(v)
	case []byte:
		m.Payload = v
	}
	if tp, ok := x.Values[fieldTrace].(string); ok {
		m.trace = tp
	}
	return m
}

// Context returns ctx carrying the trace context the producer attached.
func (m Message) Context(ctx context.Context) context.Context {
	if m.trace == "" {
		return ctx
	}
	return propagator.Extract(ctx, propagation.MapCarrier{"traceparent": m.trace})
}

// permanent marks an error that retrying cannot fix.
type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent wraps err so the message goes straight to the dead-letter stream
// instead of being redelivered.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanent{err}
}

// IsPermanent reports whether err was wrapped with Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

func init() {
	// Trace context only needs to be set once per process; stages share it.
	otel.SetTextMapPropagator(propagator)
}
