package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/appu/leadgraph/pkg/model"
)

func rec(i int) *model.RawRecord {
	return &model.RawRecord{
		URL: fmt.Sprintf("https://acme.com/p%d", i), Status: 200, FetchedAt: time.Unix(1700000000+int64(i), 0).UTC(),
		Headers: map[string][]string{"Content-Type": {"text/html"}}, ContentHash: fmt.Sprintf("h%d", i),
		Body: []byte(strings.Repeat(fmt.Sprintf("<p>page %d jane.doe@acme.com</p>", i), 20)),
	}
}

func TestBatchRoundTrip(t *testing.T) {
	blob := NewMem()
	w := NewBatchWriter(blob, BatchOptions{MaxRecords: 4, FlushInterval: time.Hour})
	ctx := context.Background()
	refs := make([]Ref, 10)
	var wg sync.WaitGroup
	for i := range refs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref, err := w.Append(ctx, rec(i))
			if err != nil {
				t.Errorf("append %d: %v", i, err)
			}
			refs[i] = ref
		}()
	}
	// Two full batches flush by size; the remainder needs an explicit flush.
	deadline := time.Now().Add(5 * time.Second)
	for blob.Len() < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	w.Flush()
	wg.Wait()
	if blob.Len() != 3 {
		t.Fatalf("objects = %d, want 3", blob.Len())
	}
	for i, ref := range refs {
		got, err := ReadRecord(ctx, blob, ref)
		if err != nil {
			t.Fatalf("read %d: %v", i, err)
		}
		if got.URL != rec(i).URL || !bytes.Equal(got.Body, rec(i).Body) || !got.FetchedAt.Equal(rec(i).FetchedAt) {
			t.Errorf("record %d mismatch: %+v", i, got.URL)
		}
	}
	// The object is plain multi-member gzip: zcat yields JSONL.
	keys, _ := blob.List(ctx, RawPrefix)
	total := 0
	for _, k := range keys {
		if !strings.HasSuffix(k, ".jsonl.gz") {
			t.Errorf("key %q", k)
		}
		data, _ := blob.Get(ctx, k)
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		all, err := io.ReadAll(zr)
		if err != nil {
			t.Fatal(err)
		}
		total += strings.Count(string(all), "\n")
		n := 0
		if err := ScanBatch(ctx, blob, k, func(ref Ref, r *model.RawRecord) error {
			n++
			again, err := ReadRecord(ctx, blob, ref)
			if err != nil || again.URL != r.URL {
				t.Errorf("scan ref does not round-trip: %v", err)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Error("empty batch")
		}
	}
	if total != 10 {
		t.Errorf("jsonl lines = %d, want 10", total)
	}
}

func TestFlushByInterval(t *testing.T) {
	blob := NewMem()
	w := NewBatchWriter(blob, BatchOptions{MaxRecords: 1000, FlushInterval: 30 * time.Millisecond})
	start := time.Now()
	ref, err := w.Append(context.Background(), rec(1))
	if err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 25*time.Millisecond {
		t.Error("append returned before the batch was flushed")
	}
	if _, err := ReadRecord(context.Background(), blob, ref); err != nil {
		t.Errorf("record not durable when Append returned: %v", err)
	}
}

func TestAppendReportsWriteFailure(t *testing.T) {
	blob := NewMem()
	blob.FailPut = errors.New("s3 down")
	w := NewBatchWriter(blob, BatchOptions{MaxRecords: 1})
	if _, err := w.Append(context.Background(), rec(1)); err == nil {
		t.Fatal("append must fail when the batch cannot be stored")
	}
	w.Close()
	if _, err := w.Append(context.Background(), rec(2)); err == nil {
		t.Error("append after close must fail")
	}
}

func TestAppendHonorsContext(t *testing.T) {
	w := NewBatchWriter(NewMem(), BatchOptions{MaxRecords: 100, FlushInterval: time.Hour})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := w.Append(ctx, rec(1)); !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("err = %v", err)
	}
	w.Close()
}

func TestTombstoneKeepsOtherOffsets(t *testing.T) {
	blob := NewMem()
	ctx := context.Background()
	w := NewBatchWriter(blob, BatchOptions{MaxRecords: 5, FlushInterval: time.Hour})
	refs := make([]Ref, 5)
	var wg sync.WaitGroup
	var mu sync.Mutex
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ref, err := w.Append(ctx, rec(i))
			if err != nil {
				t.Error(err)
			}
			mu.Lock()
			refs[i] = ref
			mu.Unlock()
		}()
	}
	wg.Wait()
	before, _ := blob.Get(ctx, refs[2].Key)
	if err := Tombstone(ctx, blob, refs[2]); err != nil {
		t.Fatal(err)
	}
	after, _ := blob.Get(ctx, refs[2].Key)
	if len(before) != len(after) {
		t.Fatalf("object size changed: %d -> %d", len(before), len(after))
	}
	if bytes.Contains(after, before[refs[2].Offset:refs[2].Offset+refs[2].Length]) {
		t.Error("erased bytes still present")
	}
	for i, ref := range refs {
		got, err := ReadRecord(ctx, blob, ref)
		if err != nil {
			t.Fatalf("record %d unreadable after tombstone: %v", i, err)
		}
		if i == 2 {
			if !got.Tombstone || got.URL != "" || len(got.Body) != 0 {
				t.Errorf("record 2 not erased: %+v", got)
			}
		} else if got.URL != rec(i).URL || !bytes.Equal(got.Body, rec(i).Body) {
			t.Errorf("record %d damaged", i)
		}
	}
	// Whole-object readers still work and no content survives.
	n := 0
	if err := ScanBatch(ctx, blob, refs[2].Key, func(_ Ref, r *model.RawRecord) error { n++; return nil }); err != nil || n != 5 {
		t.Errorf("scan after tombstone: n=%d err=%v", n, err)
	}
	if err := Tombstone(ctx, blob, refs[2]); err != nil {
		t.Errorf("tombstone must be idempotent: %v", err)
	}
	// zcat of the whole object still works: padding decompresses to nothing.
	zr, err := gzip.NewReader(bytes.NewReader(after))
	if err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(zr)
	if err != nil || strings.Count(string(all), "\n") != 5 || strings.Contains(string(all), "jane.doe@acme.com</p>\"") {
		t.Errorf("multi-member read after tombstone: %d lines, err %v", strings.Count(string(all), "\n"), err)
	}
	if err := Tombstone(ctx, blob, Ref{Key: refs[2].Key, Offset: 0, Length: 1 << 30}); err == nil {
		t.Error("out-of-range tombstone should fail")
	}
	if err := Tombstone(ctx, blob, Ref{Key: "raw/missing", Length: 10}); !errors.Is(err, ErrNotFound) {
		t.Errorf("missing object: %v", err)
	}
}

func TestJSONHelpersAndMem(t *testing.T) {
	blob := NewMem()
	ctx := context.Background()
	key := ExtractKey("abcdef0123", "")
	if key != "extract/ab/abcdef0123/none.json.gz" {
		t.Errorf("key = %q", key)
	}
	in := model.Extraction{URL: "https://acme.com/", Title: "Acme"}
	if err := PutJSON(ctx, blob, key, in); err != nil {
		t.Fatal(err)
	}
	var out model.Extraction
	if err := GetJSON(ctx, blob, key, &out); err != nil || out.Title != "Acme" {
		t.Errorf("out=%+v err=%v", out, err)
	}
	if err := GetJSON(ctx, blob, "nope", &out); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v", err)
	}
	if _, err := blob.GetRange(ctx, key, 0, 1<<20); err == nil {
		t.Error("out-of-bounds range should fail")
	}
	blob.Delete(ctx, key)
	if keys, _ := blob.List(ctx, ""); len(keys) != 0 {
		t.Errorf("keys = %v", keys)
	}
}

// Records range from a few hundred bytes to megabytes that barely compress;
// the tombstone must fit every size exactly.
func TestTombstoneAnySize(t *testing.T) {
	ctx := context.Background()
	noise := make([]byte, 400000)
	x := uint32(1)
	for i := range noise {
		x = x*1664525 + 1013904223
		noise[i] = byte(x >> 24)
	}
	for _, size := range []int{0, 1, 50, 500, 5000, 65000, 65600, 131200, 400000} {
		blob := NewMem()
		w := NewBatchWriter(blob, BatchOptions{MaxRecords: 3, FlushInterval: time.Hour})
		refs := make([]Ref, 3)
		var wg sync.WaitGroup
		for i := range refs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				body := []byte("neighbour")
				if i == 1 {
					body = noise[:size]
				}
				refs[i], _ = w.Append(ctx, &model.RawRecord{URL: fmt.Sprintf("https://a.com/%d", i), Status: 200, Body: body})
			}()
		}
		wg.Wait()
		// Whichever slot the incompressible record landed in, erase it.
		var target Ref
		for _, ref := range refs {
			if r, err := ReadRecord(ctx, blob, ref); err == nil && r.URL == "https://a.com/1" {
				target = ref
			}
		}
		if err := Tombstone(ctx, blob, target); err != nil {
			t.Errorf("size %d (member %d bytes): %v", size, target.Length, err)
			continue
		}
		n, tombs := 0, 0
		if err := ScanBatch(ctx, blob, target.Key, func(ref Ref, r *model.RawRecord) error {
			n++
			if r.Tombstone {
				tombs++
			} else if string(r.Body) != "neighbour" {
				t.Errorf("size %d: neighbour damaged", size)
			}
			return nil
		}); err != nil || n != 3 || tombs != 1 {
			t.Errorf("size %d: scan n=%d tombstones=%d err=%v", size, n, tombs, err)
		}
		for _, ref := range refs {
			if _, err := ReadRecord(ctx, blob, ref); err != nil {
				t.Errorf("size %d: ranged read failed after tombstone: %v", size, err)
			}
		}
	}
}
