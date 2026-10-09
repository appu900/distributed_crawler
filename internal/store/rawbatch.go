package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/appu/leadgraph/pkg/model"
)

// Ref locates one record inside a batch object.
type Ref struct {
	Key    string
	Offset int64
	Length int64
}

// RawPrefix is the key prefix of raw batch objects.
const RawPrefix = "raw/"

// BatchOptions bounds a batch.
type BatchOptions struct {
	MaxRecords    int
	MaxBytes      int
	FlushInterval time.Duration
}

type batch struct {
	key   string
	buf   bytes.Buffer
	count int
	timer *time.Timer
	done  chan struct{}
	err   error
	// flushing is set once, under BatchWriter.mu, by the goroutine that
	// writes the batch.
	flushing bool
}

// BatchWriter appends raw records to WARC-like batch objects: every record
// is its own gzip member holding one JSON line, and an object is the
// concatenation of members. `zcat object` therefore prints JSONL, while a
// single record can be fetched with one ranged GET.
//
// Append is a group commit: it returns only after the batch containing the
// record has been written to the blob store, so a caller that then publishes
// a reference never points at data that could be lost in a crash.
type BatchWriter struct {
	blob Blob
	opt  BatchOptions
	now  func() time.Time

	mu     sync.Mutex
	cur    *batch
	closed bool
	wg     sync.WaitGroup
}

func NewBatchWriter(blob Blob, opt BatchOptions) *BatchWriter {
	if opt.MaxRecords <= 0 {
		opt.MaxRecords = 200
	}
	if opt.MaxBytes <= 0 {
		opt.MaxBytes = 16 << 20
	}
	if opt.FlushInterval <= 0 {
		opt.FlushInterval = 2 * time.Second
	}
	return &BatchWriter{blob: blob, opt: opt, now: time.Now}
}

// gzipExtraMax is the largest gzip header "extra" field.
const gzipExtraMax = 65535

// gzipMember compresses payload into one gzip member. pad > 0 adds that many
// bytes of header padding (an RFC 1952 extra subfield), which is how a
// tombstone is made exactly as long as the record it replaces. pad must be 0
// or in 4..gzipExtraMax.
func gzipMember(payload []byte, pad int) ([]byte, error) {
	var buf bytes.Buffer
	zw, _ := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	if pad > 0 {
		if pad < 4 || pad > gzipExtraMax {
			return nil, fmt.Errorf("store: invalid gzip padding %d", pad)
		}
		extra := make([]byte, pad)
		extra[0], extra[1] = 'L', 'G' // subfield id
		extra[2], extra[3] = byte(pad-4), byte((pad-4)>>8)
		zw.Extra = extra
	}
	if _, err := zw.Write(payload); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func encodeMember(rec *model.RawRecord) ([]byte, error) {
	line, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return gzipMember(append(line, '\n'), 0)
}

// paddedTo returns gzip members totalling exactly n bytes: one holding
// payload, followed by as many empty padding members as needed. Empty
// members decompress to nothing, so readers of the whole object are
// unaffected.
func paddedTo(payload []byte, n int) ([]byte, error) {
	first, err := gzipMember(payload, 4)
	if err != nil {
		return nil, err
	}
	empty, err := gzipMember(nil, 4)
	if err != nil {
		return nil, err
	}
	minFirst, minEmpty := len(first), len(empty)
	if n < minFirst {
		return nil, fmt.Errorf("store: %d bytes is too small to hold a tombstone (%d)", n, minFirst)
	}
	out := make([]byte, 0, n)
	remaining := n
	next, minNext := payload, minFirst
	for remaining > 0 {
		size := min(remaining, minNext-4+gzipExtraMax)
		// Never leave a remainder too small for another member.
		if rest := remaining - size; rest > 0 && rest < minEmpty {
			size -= minEmpty - rest
		}
		member, err := gzipMember(next, size-(minNext-4))
		if err != nil {
			return nil, err
		}
		if len(member) != size {
			return nil, fmt.Errorf("store: padded member is %d bytes, want %d", len(member), size)
		}
		out = append(out, member...)
		remaining -= size
		next, minNext = nil, minEmpty
	}
	return out, nil
}

func (w *BatchWriter) newKey() string {
	var r [6]byte
	_, _ = rand.Read(r[:])
	t := w.now().UTC()
	return fmt.Sprintf("%s%s/%d-%s.jsonl.gz", RawPrefix, t.Format("2006/01/02"), t.UnixMilli(), hex.EncodeToString(r[:]))
}

// Append adds a record and blocks until its batch is durable.
func (w *BatchWriter) Append(ctx context.Context, rec *model.RawRecord) (Ref, error) {
	member, err := encodeMember(rec)
	if err != nil {
		return Ref{}, err
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return Ref{}, errors.New("store: batch writer is closed")
	}
	b := w.cur
	if b == nil {
		b = &batch{key: w.newKey(), done: make(chan struct{})}
		w.cur = b
		w.wg.Add(1)
		b.timer = time.AfterFunc(w.opt.FlushInterval, func() { w.flush(b) })
	}
	ref := Ref{Key: b.key, Offset: int64(b.buf.Len()), Length: int64(len(member))}
	b.buf.Write(member)
	b.count++
	full := b.count >= w.opt.MaxRecords || b.buf.Len() >= w.opt.MaxBytes
	if full {
		// Detach under the lock so no later Append can join a full batch.
		w.cur = nil
	}
	w.mu.Unlock()

	if full {
		w.flush(b)
	}
	select {
	case <-b.done:
		return ref, b.err
	case <-ctx.Done():
		return Ref{}, ctx.Err()
	}
}

// flush writes b once; later calls for the same batch are no-ops.
func (w *BatchWriter) flush(b *batch) {
	w.mu.Lock()
	if b.flushing {
		w.mu.Unlock()
		return
	}
	b.flushing = true
	if w.cur == b {
		w.cur = nil
	}
	b.timer.Stop()
	w.mu.Unlock()
	defer w.wg.Done()

	data := b.buf.Bytes()
	var err error
	for attempt := 0; attempt < 4; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err = w.blob.Put(ctx, b.key, data, "application/gzip")
		cancel()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(attempt+1) * 200 * time.Millisecond)
	}
	b.err = err
	close(b.done)
}

// Flush writes the open batch, if any, and waits for all writes in flight.
func (w *BatchWriter) Flush() {
	w.mu.Lock()
	b := w.cur
	w.mu.Unlock()
	if b != nil {
		w.flush(b)
	}
	w.wg.Wait()
}

// Close flushes and rejects further appends.
func (w *BatchWriter) Close() {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	w.Flush()
}

func decodeMember(member []byte) (*model.RawRecord, error) {
	zr, err := gzip.NewReader(bytes.NewReader(member))
	if err != nil {
		return nil, fmt.Errorf("store: raw record: %w", err)
	}
	zr.Multistream(false)
	line, err := io.ReadAll(zr)
	if err != nil {
		return nil, fmt.Errorf("store: raw record: %w", err)
	}
	var rec model.RawRecord
	if err := json.Unmarshal(line, &rec); err != nil {
		return nil, fmt.Errorf("store: raw record: %w", err)
	}
	return &rec, nil
}

// ReadRecord fetches one record with a ranged read.
func ReadRecord(ctx context.Context, blob Blob, ref Ref) (*model.RawRecord, error) {
	member, err := blob.GetRange(ctx, ref.Key, ref.Offset, ref.Length)
	if err != nil {
		return nil, err
	}
	return decodeMember(member)
}

// ScanBatch calls fn for every record of a batch object, in order.
func ScanBatch(ctx context.Context, blob Blob, key string, fn func(Ref, *model.RawRecord) error) error {
	data, err := blob.Get(ctx, key)
	if err != nil {
		return err
	}
	// bytes.Reader is an io.ByteReader, so gzip does not read ahead and the
	// reader position after a member is exactly that member's end.
	r := bytes.NewReader(data)
	for r.Len() > 0 {
		start := int64(len(data) - r.Len())
		zr, err := gzip.NewReader(r)
		if err != nil {
			return fmt.Errorf("store: %s at %d: %w", key, start, err)
		}
		zr.Multistream(false)
		line, err := io.ReadAll(zr)
		if err != nil {
			return fmt.Errorf("store: %s at %d: %w", key, start, err)
		}
		end := int64(len(data) - r.Len())
		if len(bytes.TrimSpace(line)) == 0 {
			continue // padding left by a tombstone
		}
		var rec model.RawRecord
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("store: %s at %d: %w", key, start, err)
		}
		if err := fn(Ref{Key: key, Offset: start, Length: end - start}, &rec); err != nil {
			return err
		}
	}
	return nil
}

var tombstoneMu sync.Mutex

// Tombstone overwrites one record with a marker of exactly the same byte
// length, so the offsets of every other record in the object stay valid.
// The marker keeps nothing of the record, not even its URL.
func Tombstone(ctx context.Context, blob Blob, ref Ref) error {
	tombstoneMu.Lock()
	defer tombstoneMu.Unlock()
	data, err := blob.Get(ctx, ref.Key)
	if err != nil {
		return err
	}
	if ref.Offset < 0 || ref.Length <= 0 || ref.Offset+ref.Length > int64(len(data)) {
		return fmt.Errorf("store: tombstone range out of bounds for %s", ref.Key)
	}
	if old, err := decodeMember(data[ref.Offset : ref.Offset+ref.Length]); err == nil && old.Tombstone {
		return nil
	}
	// Hand-written so the marker is as small as possible: it must fit inside
	// the smallest record the fetcher can produce.
	member, err := paddedTo([]byte("{\"tombstone\":true}\n"), int(ref.Length))
	if err != nil {
		return fmt.Errorf("%w (record at %s+%d)", err, ref.Key, ref.Offset)
	}
	out := make([]byte, len(data))
	copy(out, data)
	copy(out[ref.Offset:], member)
	return blob.Put(ctx, ref.Key, out, "application/gzip")
}

// ExtractPrefix is the key prefix of extraction objects.
const ExtractPrefix = "extract/"

// ExtractKey is the object key for the extraction of one page version.
func ExtractKey(urlHash, contentHash string) string {
	if contentHash == "" {
		contentHash = "none"
	}
	return ExtractPrefix + urlHash[:2] + "/" + urlHash + "/" + contentHash + ".json.gz"
}

// PutJSON stores v as gzip'd JSON.
func PutJSON(ctx context.Context, blob Blob, key string, v any) error {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if err := json.NewEncoder(zw).Encode(v); err != nil {
		return err
	}
	if err := zw.Close(); err != nil {
		return err
	}
	return blob.Put(ctx, key, buf.Bytes(), "application/gzip")
}

// GetJSON reads gzip'd JSON into v.
func GetJSON(ctx context.Context, blob Blob, key string, v any) error {
	data, err := blob.Get(ctx, key)
	if err != nil {
		return err
	}
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return err
	}
	return json.NewDecoder(zr).Decode(v)
}
