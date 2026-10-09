package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/metrics"
)

// ConsumerConfig tunes one consumer.
type ConsumerConfig struct {
	Stream string
	// Name identifies this consumer in the group; empty derives one from
	// host and pid.
	Name        string
	Concurrency int
	ReadBatch   int
	Block       time.Duration
	// ClaimMinIdle is how long a delivered message may stay unacknowledged
	// before another consumer takes it over. It must exceed the longest
	// handler run, or healthy work gets duplicated.
	ClaimMinIdle  time.Duration
	ClaimInterval time.Duration
	// MaxDeliveries is how many times a message is attempted before it is
	// moved to the dead-letter stream.
	MaxDeliveries int
	// DrainTimeout is how long in-flight handlers get to finish on shutdown.
	DrainTimeout time.Duration
}

// Handler processes one message. Returning nil acknowledges it; an error
// leaves it pending for redelivery; a Permanent error dead-letters it.
type Handler func(ctx context.Context, m Message) error

// BatchHandler processes several messages at once and returns one error per
// message (nil slice = all succeeded).
type BatchHandler func(ctx context.Context, ms []Message) []error

// Consumer reads one stream through its consumer group.
type Consumer struct {
	s   *Streams
	cfg ConsumerConfig
	log *slog.Logger

	mu       sync.Mutex
	inflight map[string]struct{}
	cursor   string
}

// NewConsumer validates the config and fills in defaults.
func NewConsumer(s *Streams, cfg ConsumerConfig, log *slog.Logger) *Consumer {
	if cfg.Name == "" {
		host, _ := os.Hostname()
		cfg.Name = fmt.Sprintf("%s-%d-%d", host, os.Getpid(), time.Now().UnixNano()%100000)
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.ReadBatch <= 0 {
		cfg.ReadBatch = 16
	}
	if cfg.Block <= 0 {
		cfg.Block = 2 * time.Second
	}
	if cfg.ClaimMinIdle <= 0 {
		cfg.ClaimMinIdle = 5 * time.Minute
	}
	if cfg.ClaimInterval <= 0 {
		cfg.ClaimInterval = max(cfg.ClaimMinIdle/4, 50*time.Millisecond)
	}
	if cfg.MaxDeliveries <= 0 {
		cfg.MaxDeliveries = 5
	}
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{s: s, cfg: cfg, log: log.With("stream", cfg.Stream, "consumer", cfg.Name),
		inflight: map[string]struct{}{}, cursor: "0-0"}
}

func (c *Consumer) key() string   { return c.s.Key(c.cfg.Stream) }
func (c *Consumer) group() string { return c.s.Group(c.cfg.Stream) }

func (c *Consumer) errKey() string { return c.s.prefix + ":lasterr:" + c.cfg.Stream }

// read fetches up to count never-delivered messages.
func (c *Consumer) read(ctx context.Context, count int, block time.Duration) ([]Message, error) {
	res, err := c.s.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group: c.group(), Consumer: c.cfg.Name, Streams: []string{c.key(), ">"},
		Count: int64(count), Block: block,
	}).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []Message
	for _, st := range res {
		for _, x := range st.Messages {
			out = append(out, toMessage(c.cfg.Stream, x, 1))
		}
	}
	return out, nil
}

// claim takes over messages that another consumer (or a previous life of
// this one) received but never acknowledged.
func (c *Consumer) claim(ctx context.Context, count int) ([]Message, error) {
	msgs, next, err := c.s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream: c.key(), Group: c.group(), Consumer: c.cfg.Name,
		MinIdle: c.cfg.ClaimMinIdle, Start: c.cursor, Count: int64(count),
	}).Result()
	if err != nil {
		return nil, err
	}
	if next == "" {
		next = "0-0"
	}
	c.cursor = next
	var out []Message
	for _, x := range msgs {
		if x.Values == nil {
			// The entry was deleted while pending; drop the dangling claim.
			c.s.rdb.XAck(ctx, c.key(), c.group(), x.ID)
			continue
		}
		c.mu.Lock()
		_, busy := c.inflight[x.ID]
		c.mu.Unlock()
		if busy {
			continue // still being handled by this very process
		}
		deliveries := int64(2)
		if p, err := c.s.rdb.XPendingExt(ctx, &redis.XPendingExtArgs{
			Stream: c.key(), Group: c.group(), Start: x.ID, End: x.ID, Count: 1,
		}).Result(); err == nil && len(p) == 1 {
			deliveries = p[0].RetryCount
		}
		out = append(out, toMessage(c.cfg.Stream, x, deliveries))
	}
	return out, nil
}

func (c *Consumer) track(id string, on bool) {
	c.mu.Lock()
	if on {
		c.inflight[id] = struct{}{}
	} else {
		delete(c.inflight, id)
	}
	c.mu.Unlock()
}

// ack acknowledges and deletes in one transaction, so the stream length is
// always the true backlog.
func (c *Consumer) ack(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := c.s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.XAck(ctx, c.key(), c.group(), ids...)
		p.XDel(ctx, c.key(), ids...)
		p.HDel(ctx, c.errKey(), ids...)
		return nil
	})
	return err
}

// DeadLetter moves a message to the DLQ stream with its failure context.
func (c *Consumer) deadLetter(ctx context.Context, m Message, reason string) error {
	if reason == "" {
		reason, _ = c.s.rdb.HGet(ctx, c.errKey(), m.ID).Result()
	}
	_, err := c.s.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.XAdd(ctx, &redis.XAddArgs{Stream: c.s.Key(DLQ), Values: []any{
			"stream", m.Stream, "id", m.ID, fieldData, m.Payload, "error", reason,
			"deliveries", m.Deliveries, "failed_at", time.Now().UTC().Format(time.RFC3339),
		}})
		p.XAck(ctx, c.key(), c.group(), m.ID)
		p.XDel(ctx, c.key(), m.ID)
		p.HDel(ctx, c.errKey(), m.ID)
		return nil
	})
	if err == nil {
		metrics.StreamDLQ.WithLabelValues(m.Stream).Inc()
		c.log.Warn("message dead-lettered", "id", m.ID, "deliveries", m.Deliveries, "error", reason)
	}
	return err
}

func clipErr(err error) string {
	s := err.Error()
	if len(s) > 500 {
		s = s[:500]
	}
	return s
}

// settle applies the outcome of handling one message.
func (c *Consumer) settle(ctx context.Context, m Message, err error) {
	switch {
	case err == nil:
		if aerr := c.ack(ctx, m.ID); aerr != nil {
			c.log.Error("ack failed; message will be redelivered", "id", m.ID, "error", aerr)
			metrics.StreamHandled.WithLabelValues(m.Stream, "ack_error").Inc()
			return
		}
		metrics.StreamHandled.WithLabelValues(m.Stream, "ok").Inc()
	case IsPermanent(err) || m.Deliveries >= int64(c.cfg.MaxDeliveries):
		metrics.StreamHandled.WithLabelValues(m.Stream, "failed").Inc()
		if derr := c.deadLetter(ctx, m, clipErr(err)); derr != nil {
			c.log.Error("dead-lettering failed", "id", m.ID, "error", derr)
		}
	default:
		metrics.StreamHandled.WithLabelValues(m.Stream, "retry").Inc()
		c.s.rdb.HSet(ctx, c.errKey(), m.ID, clipErr(err))
		c.log.Warn("handler failed; message left pending for retry", "id", m.ID, "deliveries", m.Deliveries, "error", err)
	}
}

// handlerContext returns a context that outlives ctx by the drain timeout,
// so shutdown lets in-flight work finish and acknowledge.
func (c *Consumer) handlerContext(ctx context.Context) (context.Context, context.CancelFunc) {
	hctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, func() {
		if c.cfg.DrainTimeout <= 0 {
			cancel()
			return
		}
		t := time.NewTimer(c.cfg.DrainTimeout)
		defer t.Stop()
		select {
		case <-t.C:
			cancel()
		case <-hctx.Done():
		}
	})
	return hctx, func() { stop(); cancel() }
}

func pause(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Run consumes until ctx is cancelled, handling up to Concurrency messages
// at a time. It returns after in-flight handlers finish or the drain timeout
// passes.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	if err := c.s.EnsureGroup(ctx, c.cfg.Stream); err != nil {
		return err
	}
	hctx, cancelHandlers := c.handlerContext(ctx)
	defer cancelHandlers()
	sem := make(chan struct{}, c.cfg.Concurrency)
	var wg sync.WaitGroup
	var lastClaim time.Time

	for ctx.Err() == nil {
		free := cap(sem) - len(sem)
		if free == 0 {
			pause(ctx, 5*time.Millisecond)
			continue
		}
		// Never take more than can start immediately: a delivered message
		// that waits in memory is ageing towards a reclaim.
		want := min(free, c.cfg.ReadBatch)
		var msgs []Message
		var err error
		if time.Since(lastClaim) >= c.cfg.ClaimInterval {
			lastClaim = time.Now()
			msgs, err = c.claim(ctx, want)
		}
		if err == nil && len(msgs) == 0 {
			block := c.cfg.Block
			if c.cfg.ClaimInterval < block {
				block = c.cfg.ClaimInterval
			}
			msgs, err = c.read(ctx, want, block)
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			c.log.Error("stream read failed", "error", err)
			pause(ctx, time.Second)
			continue
		}
		for _, m := range msgs {
			if m.Deliveries > int64(c.cfg.MaxDeliveries) {
				if derr := c.deadLetter(hctx, m, ""); derr != nil {
					c.log.Error("dead-lettering failed", "id", m.ID, "error", derr)
				}
				continue
			}
			sem <- struct{}{}
			c.track(m.ID, true)
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				defer c.track(m.ID, false)
				err := safely(func() error { return h(m.Context(hctx), m) })
				c.settle(hctx, m, err)
			}()
		}
	}
	wg.Wait()
	return nil
}

// RunBatch consumes with a single handler invocation per read, for sinks that
// commit many messages in one transaction.
func (c *Consumer) RunBatch(ctx context.Context, h BatchHandler) error {
	if err := c.s.EnsureGroup(ctx, c.cfg.Stream); err != nil {
		return err
	}
	hctx, cancelHandlers := c.handlerContext(ctx)
	defer cancelHandlers()
	var lastClaim time.Time

	for ctx.Err() == nil {
		var msgs []Message
		var err error
		if time.Since(lastClaim) >= c.cfg.ClaimInterval {
			lastClaim = time.Now()
			msgs, err = c.claim(ctx, c.cfg.ReadBatch)
		}
		if err == nil && len(msgs) < c.cfg.ReadBatch {
			block := c.cfg.Block
			if len(msgs) > 0 {
				block = time.Millisecond
			}
			var fresh []Message
			fresh, err = c.read(ctx, c.cfg.ReadBatch-len(msgs), block)
			msgs = append(msgs, fresh...)
		}
		if err != nil && len(msgs) == 0 {
			if ctx.Err() != nil {
				break
			}
			c.log.Error("stream read failed", "error", err)
			pause(ctx, time.Second)
			continue
		}
		batch := msgs[:0]
		for _, m := range msgs {
			if m.Deliveries > int64(c.cfg.MaxDeliveries) {
				if derr := c.deadLetter(hctx, m, ""); derr != nil {
					c.log.Error("dead-lettering failed", "id", m.ID, "error", derr)
				}
				continue
			}
			batch = append(batch, m)
		}
		if len(batch) == 0 {
			continue
		}
		for _, m := range batch {
			c.track(m.ID, true)
		}
		var errs []error
		if perr := safely(func() error { errs = h(hctx, batch); return nil }); perr != nil {
			errs = make([]error, len(batch))
			for i := range errs {
				errs[i] = perr
			}
		}
		var ok []string
		for i, m := range batch {
			var merr error
			if i < len(errs) {
				merr = errs[i]
			}
			if merr == nil {
				ok = append(ok, m.ID)
			} else {
				c.settle(hctx, m, merr)
			}
			c.track(m.ID, false)
		}
		if aerr := c.ack(hctx, ok...); aerr != nil {
			c.log.Error("ack failed; batch will be redelivered", "count", len(ok), "error", aerr)
		} else if len(ok) > 0 {
			metrics.StreamHandled.WithLabelValues(c.cfg.Stream, "ok").Add(float64(len(ok)))
		}
	}
	return nil
}

// safely converts a handler panic into an error so one poisoned message
// cannot take the worker down; it then follows the normal retry/DLQ path.
func safely(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return f()
}
