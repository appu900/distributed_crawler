// Package store is the raw object store: an S3-compatible Blob interface and
// the WARC-like batch format layered on top of it.
package store

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
)

// ErrNotFound is returned when a key does not exist.
var ErrNotFound = errors.New("store: object not found")

// Blob is the subset of object storage the pipeline needs.
type Blob interface {
	Put(ctx context.Context, key string, data []byte, contentType string) error
	Get(ctx context.Context, key string) ([]byte, error)
	// GetRange reads length bytes starting at offset.
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)
	Delete(ctx context.Context, key string) error
	List(ctx context.Context, prefix string) ([]string, error)
}

// Mem is an in-memory Blob for tests and local experiments.
type Mem struct {
	mu   sync.RWMutex
	objs map[string][]byte
	// FailPut, when set, makes Put return this error (fault injection).
	FailPut error
}

func NewMem() *Mem { return &Mem{objs: map[string][]byte{}} }

func (m *Mem) Put(_ context.Context, key string, data []byte, _ string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailPut != nil {
		return m.FailPut
	}
	m.objs[key] = append([]byte(nil), data...)
	return nil
}

func (m *Mem) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), b...), nil
}

func (m *Mem) GetRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.objs[key]
	if !ok {
		return nil, ErrNotFound
	}
	if offset < 0 || length < 0 || offset+length > int64(len(b)) {
		return nil, errors.New("store: range out of bounds")
	}
	return append([]byte(nil), b[offset:offset+length]...), nil
}

func (m *Mem) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objs, key)
	return nil
}

func (m *Mem) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var keys []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return keys, nil
}

// Len returns the number of stored objects.
func (m *Mem) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.objs)
}
