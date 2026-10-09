// Package dlq inspects and replays the dead-letter stream.
package dlq

import (
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"

	"github.com/appu/leadgraph/internal/queue"
)

// Entry is one dead-lettered message.
type Entry struct {
	ID         string
	Stream     string // the stream it failed in
	OrigID     string
	Error      string
	Deliveries int
	FailedAt   string
	Payload    []byte
}

func toEntry(x redis.XMessage) Entry {
	str := func(k string) string {
		s, _ := x.Values[k].(string)
		return s
	}
	n, _ := strconv.Atoi(str("deliveries"))
	return Entry{ID: x.ID, Stream: str("stream"), OrigID: str("id"), Error: str("error"), Deliveries: n,
		FailedAt: str("failed_at"), Payload: []byte(str("d"))}
}

// Size returns the number of dead letters.
func Size(ctx context.Context, s *queue.Streams) (int64, error) {
	return s.Len(ctx, queue.DLQ)
}

// List returns up to limit entries, oldest first.
func List(ctx context.Context, s *queue.Streams, limit int64) ([]Entry, error) {
	if limit <= 0 {
		limit = 100
	}
	msgs, err := s.Client().XRangeN(ctx, s.Key(queue.DLQ), "-", "+", limit).Result()
	if err != nil {
		return nil, err
	}
	out := make([]Entry, len(msgs))
	for i, m := range msgs {
		out[i] = toEntry(m)
	}
	return out, nil
}

func validStage(stream string) bool {
	for _, st := range queue.Stages {
		if st == stream {
			return true
		}
	}
	return false
}

// Replay moves dead letters back to the stream they failed in, where they
// start again with a fresh delivery count. stream filters by origin ("" for
// all); limit caps how many are replayed (0 for all). Each entry is
// re-published before it is removed, so a crash mid-replay can duplicate a
// message but never lose one; consumers are idempotent.
func Replay(ctx context.Context, s *queue.Streams, stream string, limit int) (int, error) {
	if stream != "" && !validStage(stream) {
		return 0, fmt.Errorf("dlq: unknown stream %q", stream)
	}
	replayed := 0
	cursor := "-"
	for limit <= 0 || replayed < limit {
		msgs, err := s.Client().XRangeN(ctx, s.Key(queue.DLQ), cursor, "+", 100).Result()
		if err != nil {
			return replayed, err
		}
		if len(msgs) == 0 {
			break
		}
		for _, m := range msgs {
			cursor = "(" + m.ID
			e := toEntry(m)
			if stream != "" && e.Stream != stream {
				continue
			}
			if !validStage(e.Stream) {
				continue
			}
			if err := s.PublishNow(ctx, e.Stream, e.Payload); err != nil {
				return replayed, err
			}
			if err := s.Client().XDel(ctx, s.Key(queue.DLQ), m.ID).Err(); err != nil {
				return replayed, err
			}
			replayed++
			if limit > 0 && replayed >= limit {
				break
			}
		}
	}
	return replayed, nil
}

// Purge deletes dead letters (optionally only those of one origin stream).
func Purge(ctx context.Context, s *queue.Streams, stream string) (int, error) {
	purged := 0
	cursor := "-"
	for {
		msgs, err := s.Client().XRangeN(ctx, s.Key(queue.DLQ), cursor, "+", 500).Result()
		if err != nil {
			return purged, err
		}
		if len(msgs) == 0 {
			return purged, nil
		}
		var ids []string
		for _, m := range msgs {
			cursor = "(" + m.ID
			if stream == "" || toEntry(m).Stream == stream {
				ids = append(ids, m.ID)
			}
		}
		if len(ids) > 0 {
			n, err := s.Client().XDel(ctx, s.Key(queue.DLQ), ids...).Result()
			if err != nil {
				return purged, err
			}
			purged += int(n)
		}
	}
}
