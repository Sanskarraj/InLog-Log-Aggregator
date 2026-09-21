package memtable

import (
	"sync/atomic"

	"github.com/engine/lsm-trees/pkg/core"
)

// MemTable represents an active or immutable in-memory buffer.
type MemTable struct {
	id        uint64
	list      *SkipList
	immutable int32
	maxBytes  int64
}

// New creates a new active MemTable with a given WAL/MemTable ID.
func New(id uint64, maxBytes int64) *MemTable {
	if maxBytes <= 0 {
		maxBytes = 4 * 1024 * 1024 // 4MB default
	}
	return &MemTable{
		id:       id,
		list:     NewSkipList(),
		maxBytes: maxBytes,
	}
}

// ID returns the generation/WAL ID associated with this MemTable.
func (m *MemTable) ID() uint64 {
	return m.id
}

// Put inserts an entry into the MemTable.
func (m *MemTable) Put(entry *core.Entry) {
	m.list.Put(entry)
}

// Get finds an entry in the MemTable.
func (m *MemTable) Get(key []byte) (*core.Entry, bool) {
	return m.list.Get(key)
}

// ShouldFlush returns true if the MemTable has reached its capacity limit.
func (m *MemTable) ShouldFlush() bool {
	return m.list.ByteSize() >= m.maxBytes
}

// MarkImmutable marks this table as read-only.
func (m *MemTable) MarkImmutable() {
	atomic.StoreInt32(&m.immutable, 1)
}

// IsImmutable returns true if writes are disabled.
func (m *MemTable) IsImmutable() bool {
	return atomic.LoadInt32(&m.immutable) == 1
}

// ByteSize returns current memory footprint in bytes.
func (m *MemTable) ByteSize() int64 {
	return m.list.ByteSize()
}

// EntryCount returns total number of entries in the MemTable.
func (m *MemTable) EntryCount() int64 {
	return m.list.Length()
}

// NewIterator returns an iterator over all entries in the MemTable.
func (m *MemTable) NewIterator() *SkipListIterator {
	return m.list.NewIterator()
}
