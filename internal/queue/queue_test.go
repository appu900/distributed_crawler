package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newStreams(t *testing.T, maxLen int64) *Streams {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { rdb.Close() })
	return NewStreams(rdb, "t", maxLen)
}

func fastCfg(stream string) ConsumerConfig {
	return ConsumerConfig{
		Stream: stream, Concurrency: 4, ReadBatch: 8, Block: 20 * time.Millisecond,
		ClaimMinIdle: 60 * time.Millisecond, ClaimInterval: 20 * time.Millisecond,
		MaxDeliveries: 3, DrainTimeout: 2 * time.Second,
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func streamLen(t *testing.T, s *Streams, stream string) int64 {
	t.Helper()
	n, err := s.Len(context.Background(), stream)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPublishConsumeAck(t *testing.T) {
	s := newStreams(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 50; i++ {
		if err := s.Publish(ctx, Parse, []byte(fmt.Sprintf("m%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	seen := map[string]int{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		NewConsumer(s, fastCfg(Parse), nil).Run(ctx, func(_ context.Context, m Message) error {
			mu.Lock()
			seen[string(m.Payload)]++
			mu.Unlock()
			if m.Deliveries != 1 {
				t.Errorf("deliveries = %d", m.Deliveries)
			}
			return nil
		})
	}()
	eventually(t, "all messages handled", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == 50 })
	eventually(t, "stream drained", func() bool { return streamLen(t, s, Parse) == 0 })
	cancel()
	<-done
	for k, n := range seen {
		if n != 1 {
			t.Errorf("%s handled %d times", k, n)
		}
	}
}

func TestRetryThenDeadLetter(t *testing.T) {
	s := newStreams(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Publish(ctx, Parse, []byte("poison"))
	s.Publish(ctx, Parse, []byte("flaky"))
	var poison, flaky atomic.Int32
	var deliveries sync.Map
	go NewConsumer(s, fastCfg(Parse), nil).Run(ctx, func(_ context.Context, m Message) error {
		switch string(m.Payload) {
		case "poison":
			deliveries.Store(poison.Add(1), m.Deliveries)
			return errors.New("cannot parse")
		default:
			if flaky.Add(1) < 2 {
				return errors.New("transient")
			}
			return nil
		}
	})
	eventually(t, "poison dead-lettered", func() bool { return streamLen(t, s, DLQ) == 1 })
	eventually(t, "work stream empty", func() bool { return streamLen(t, s, Parse) == 0 })
	if poison.Load() != 3 {
		t.Errorf("poison attempted %d times, want MaxDeliveries=3", poison.Load())
	}
	for i := int32(1); i <= 3; i++ {
		if d, _ := deliveries.Load(i); d != int64(i) {
			t.Errorf("attempt %d saw deliveries=%v", i, d)
		}
	}
	if flaky.Load() != 2 {
		t.Errorf("flaky attempted %d times, want 2", flaky.Load())
	}
	entries, err := s.rdb.XRange(ctx, s.Key(DLQ), "-", "+").Result()
	if err != nil || len(entries) != 1 {
		t.Fatalf("dlq entries: %v %v", entries, err)
	}
	v := entries[0].Values
	if v["stream"] != Parse || v[fieldData] != "poison" || v["error"] != "cannot parse" || v["deliveries"] != "3" {
		t.Errorf("dlq entry = %v", v)
	}
	if n, _ := s.rdb.HLen(ctx, "t:lasterr:"+Parse).Result(); n != 0 {
		t.Errorf("last-error hash not cleaned: %d", n)
	}
}

func TestPermanentErrorSkipsRetries(t *testing.T) {
	s := newStreams(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Publish(ctx, Parse, []byte("garbage"))
	var n atomic.Int32
	go NewConsumer(s, fastCfg(Parse), nil).Run(ctx, func(context.Context, Message) error {
		n.Add(1)
		return Permanent(errors.New("undecodable"))
	})
	eventually(t, "dead-lettered", func() bool { return streamLen(t, s, DLQ) == 1 })
	time.Sleep(150 * time.Millisecond)
	if n.Load() != 1 {
		t.Errorf("permanent error retried: %d attempts", n.Load())
	}
	if !IsPermanent(fmt.Errorf("wrapped: %w", Permanent(errors.New("x")))) || IsPermanent(errors.New("x")) || Permanent(nil) != nil {
		t.Error("Permanent/IsPermanent semantics")
	}
}

func TestPanicIsRetried(t *testing.T) {
	s := newStreams(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Publish(ctx, Parse, []byte("x"))
	var n atomic.Int32
	go NewConsumer(s, fastCfg(Parse), nil).Run(ctx, func(context.Context, Message) error {
		if n.Add(1) == 1 {
			panic("boom")
		}
		return nil
	})
	eventually(t, "recovered and retried", func() bool { return n.Load() == 2 && streamLen(t, s, Parse) == 0 })
}

func TestBackpressureBlocksProducer(t *testing.T) {
	s := newStreams(t, 5)
	s.poll = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var published atomic.Int32
	go func() {
		for i := 0; i < 20; i++ {
			if s.Publish(ctx, Parse, []byte("m")) != nil {
				return
			}
			published.Add(1)
		}
	}()
	eventually(t, "producer fills the stream", func() bool { return published.Load() == 5 })
	time.Sleep(60 * time.Millisecond)
	if published.Load() != 5 || streamLen(t, s, Parse) != 5 {
		t.Fatalf("producer not blocked at max length: published=%d len=%d", published.Load(), streamLen(t, s, Parse))
	}
	go NewConsumer(s, fastCfg(Parse), nil).Run(ctx, func(context.Context, Message) error { return nil })
	eventually(t, "producer resumes once consumers drain", func() bool { return published.Load() == 20 })
	eventually(t, "drained", func() bool { return streamLen(t, s, Parse) == 0 })

	// A blocked publish gives up when its context ends.
	s2 := newStreams(t, 1)
	s2.Publish(context.Background(), Parse, []byte("a"))
	short, cancel2 := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel2()
	if err := s2.Publish(short, Parse, []byte("b")); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v", err)
	}
}

func TestGracefulShutdownDrainsInFlight(t *testing.T) {
	s := newStreams(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 4; i++ {
		s.Publish(ctx, Parse, []byte("m"))
	}
	started := make(chan struct{}, 4)
	var finished atomic.Int32
	done := make(chan struct{})
	go func() {
		defer close(done)
		NewConsumer(s, fastCfg(Parse), nil).Run(ctx, func(hctx context.Context, _ Message) error {
			started <- struct{}{}
			select {
			case <-time.After(150 * time.Millisecond):
				finished.Add(1)
				return nil
			case <-hctx.Done():
				return hctx.Err()
			}
		})
	}()
	for i := 0; i < 4; i++ {
		<-started
	}
	cancel() // shutdown while all four are mid-flight
	<-done
	if finished.Load() != 4 {
		t.Errorf("in-flight handlers finished = %d, want 4", finished.Load())
	}
	if n := streamLen(t, s, Parse); n != 0 {
		t.Errorf("in-flight work not acknowledged on shutdown: %d left", n)
	}
}

func TestCrashedConsumerWorkIsReclaimed(t *testing.T) {
	s := newStreams(t, 0)
	bg := context.Background()
	const total = 30
	for i := 0; i < total; i++ {
		s.Publish(bg, Parse, []byte(fmt.Sprintf("m%d", i)))
	}
	var mu sync.Mutex
	completed := map[string]int{}

	// Consumer A takes messages and is killed: its handlers never return
	// success, and there is no drain.
	ctxA, killA := context.WithCancel(bg)
	cfgA := fastCfg(Parse)
	cfgA.Name, cfgA.DrainTimeout = "a", 0
	var taken atomic.Int32
	doneA := make(chan struct{})
	go func() {
		defer close(doneA)
		NewConsumer(s, cfgA, nil).Run(ctxA, func(hctx context.Context, _ Message) error {
			taken.Add(1)
			<-hctx.Done()
			return hctx.Err()
		})
	}()
	eventually(t, "A holds messages", func() bool { return taken.Load() >= 4 })
	killA()
	<-doneA
	if n := streamLen(t, s, Parse); n != total {
		t.Fatalf("messages lost by the crash: %d of %d remain", n, total)
	}

	ctxB, stopB := context.WithCancel(bg)
	defer stopB()
	cfgB := fastCfg(Parse)
	cfgB.Name, cfgB.MaxDeliveries = "b", 10
	go NewConsumer(s, cfgB, nil).Run(ctxB, func(_ context.Context, m Message) error {
		mu.Lock()
		completed[string(m.Payload)]++
		mu.Unlock()
		return nil
	})
	eventually(t, "B finishes everything, including A's abandoned messages", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(completed) == total
	})
	eventually(t, "stream empty", func() bool { return streamLen(t, s, Parse) == 0 })
	if n := streamLen(t, s, DLQ); n != 0 {
		t.Errorf("unexpected dead letters: %d", n)
	}
	mu.Lock()
	defer mu.Unlock()
	for k, n := range completed {
		if n != 1 {
			t.Errorf("%s completed %d times", k, n)
		}
	}
}

func TestInFlightMessagesAreNotSelfClaimed(t *testing.T) {
	s := newStreams(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Publish(ctx, Parse, []byte("slow"))
	var calls atomic.Int32
	go NewConsumer(s, fastCfg(Parse), nil).Run(ctx, func(context.Context, Message) error {
		calls.Add(1)
		time.Sleep(300 * time.Millisecond) // five times the claim idle
		return nil
	})
	eventually(t, "done", func() bool { return calls.Load() >= 1 && streamLen(t, s, Parse) == 0 })
	time.Sleep(100 * time.Millisecond)
	if calls.Load() != 1 {
		t.Errorf("slow handler ran %d times; a consumer must not reclaim its own in-flight work", calls.Load())
	}
}

func TestRunBatch(t *testing.T) {
	s := newStreams(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 20; i++ {
		s.Publish(ctx, Write, []byte(fmt.Sprintf("%d", i)))
	}
	var mu sync.Mutex
	okSeen := map[string]int{}
	var batches, maxBatch int
	go NewConsumer(s, fastCfg(Write), nil).RunBatch(ctx, func(_ context.Context, ms []Message) []error {
		mu.Lock()
		defer mu.Unlock()
		batches++
		maxBatch = max(maxBatch, len(ms))
		errs := make([]error, len(ms))
		for i, m := range ms {
			if string(m.Payload) == "7" {
				errs[i] = errors.New("bad row")
				continue
			}
			okSeen[string(m.Payload)]++
		}
		return errs
	})
	eventually(t, "19 written, 1 dead-lettered", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(okSeen) == 19 && streamLen(t, s, DLQ) == 1 && streamLen(t, s, Write) == 0
	})
	mu.Lock()
	defer mu.Unlock()
	if maxBatch < 2 {
		t.Errorf("handler never saw a batch (max %d over %d calls)", maxBatch, batches)
	}
	for k, n := range okSeen {
		if n != 1 {
			t.Errorf("row %s written %d times", k, n)
		}
	}
}

func TestTraceContextTravelsWithMessage(t *testing.T) {
	m := toMessage(Parse, redis.XMessage{ID: "1-0", Values: map[string]any{
		fieldData: "x", fieldTrace: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
	}}, 1)
	if m.trace == "" || string(m.Payload) != "x" {
		t.Fatalf("message = %+v", m)
	}
	if m.Context(context.Background()) == context.Background() {
		t.Error("trace context not extracted")
	}
}
