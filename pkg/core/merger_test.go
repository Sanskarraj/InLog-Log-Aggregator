package core

import (
	"testing"
)

type mockIterator struct {
	entries []*Entry
	idx     int
}

func (m *mockIterator) Valid() bool {
	return m.idx >= 0 && m.idx < len(m.entries)
}

func (m *mockIterator) Next() {
	if m.Valid() {
		m.idx++
	}
}

func (m *mockIterator) Seek(target []byte) {
	for i, e := range m.entries {
		if string(e.Key) >= string(target) {
			m.idx = i
			return
		}
	}
	m.idx = len(m.entries)
}

func (m *mockIterator) Key() []byte {
	return m.entries[m.idx].Key
}

func (m *mockIterator) Value() []byte {
	return m.entries[m.idx].Value
}

func (m *mockIterator) Entry() *Entry {
	return m.entries[m.idx]
}

func (m *mockIterator) Close() error {
	m.idx = -1
	return nil
}

func TestMergedIteratorDeduplication(t *testing.T) {
	// Stream 1 (higher priority, e.g. MemTable) has key "b" updated, key "c" deleted
	iter1 := &mockIterator{
		entries: []*Entry{
			NewPutEntry([]byte("b"), []byte("v2_new")),
			NewDeleteEntry([]byte("c")),
		},
	}

	// Stream 2 (lower priority, e.g. SSTable) has older keys
	iter2 := &mockIterator{
		entries: []*Entry{
			NewPutEntry([]byte("a"), []byte("v1")),
			NewPutEntry([]byte("b"), []byte("v1_old")),
			NewPutEntry([]byte("c"), []byte("v1_old")),
			NewPutEntry([]byte("d"), []byte("v1")),
		},
	}

	// Merge with dropTombstones = true
	merged := NewMergedIterator([]Iterator{iter1, iter2}, true)
	defer merged.Close()

	var keys []string
	var vals []string

	for merged.Valid() {
		keys = append(keys, string(merged.Key()))
		vals = append(vals, string(merged.Value()))
		merged.Next()
	}

	// Expected keys: "a", "b", "d" ("c" was deleted by tombstone)
	// "b" should have value "v2_new"
	if len(keys) != 3 {
		t.Fatalf("expected 3 keys, got %d: %v", len(keys), keys)
	}

	if keys[0] != "a" || vals[0] != "v1" {
		t.Fatalf("mismatch at key a: got %s, %s", keys[0], vals[0])
	}
	if keys[1] != "b" || vals[1] != "v2_new" {
		t.Fatalf("mismatch at key b: got %s, %s (should be updated value)", keys[1], vals[1])
	}
	if keys[2] != "d" || vals[2] != "v1" {
		t.Fatalf("mismatch at key d: got %s, %s", keys[2], vals[2])
	}
}
