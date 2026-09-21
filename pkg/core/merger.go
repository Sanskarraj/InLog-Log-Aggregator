package core

import (
	"bytes"
)

type iterItem struct {
	iter     Iterator
	priority int // Lower number = higher priority (0 = active memtable, 1 = imm, etc.)
}

// MergedIterator combines multiple sorted iterators into a single sorted, deduplicated stream
// using a zero-allocation, strongly typed binary min-heap.
type MergedIterator struct {
	iters          []Iterator
	h              []iterItem
	current        *Entry
	currKeyBuf     []byte // Reusable buffer for deduplication key comparison
	dropTombstones bool
	valid          bool
}

// NewMergedIterator creates a multi-way merge iterator across multiple sorted streams.
func NewMergedIterator(iters []Iterator, dropTombstones bool) *MergedIterator {
	m := &MergedIterator{
		iters:          iters,
		h:              make([]iterItem, 0, len(iters)),
		currKeyBuf:     make([]byte, 0, 64),
		dropTombstones: dropTombstones,
	}

	for idx, it := range iters {
		if it != nil && it.Valid() {
			m.push(iterItem{iter: it, priority: idx})
		}
	}

	m.advance()
	return m
}

func (m *MergedIterator) less(i, j int) bool {
	cmp := bytes.Compare(m.h[i].iter.Key(), m.h[j].iter.Key())
	if cmp != 0 {
		return cmp < 0
	}
	if m.h[i].priority != m.h[j].priority {
		return m.h[i].priority < m.h[j].priority
	}
	return m.h[i].iter.Entry().Timestamp > m.h[j].iter.Entry().Timestamp
}

func (m *MergedIterator) swap(i, j int) {
	m.h[i], m.h[j] = m.h[j], m.h[i]
}

func (m *MergedIterator) up(j int) {
	for {
		i := (j - 1) / 2 // parent
		if i == j || !m.less(j, i) {
			break
		}
		m.swap(i, j)
		j = i
	}
}

func (m *MergedIterator) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 {
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && m.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !m.less(j, i) {
			break
		}
		m.swap(i, j)
		i = j
	}
	return i > i0
}

func (m *MergedIterator) push(item iterItem) {
	m.h = append(m.h, item)
	m.up(len(m.h) - 1)
}

func (m *MergedIterator) pop() iterItem {
	n := len(m.h) - 1
	m.swap(0, n)
	m.down(0, n)
	item := m.h[n]
	m.h = m.h[:n]
	return item
}

// advance pulls the next valid deduplicated entry from the typed min-heap without heap allocations.
func (m *MergedIterator) advance() {
	for {
		if len(m.h) == 0 {
			m.valid = false
			m.current = nil
			return
		}

		// Peek root
		candidateEntry := m.h[0].iter.Entry()
		candKey := candidateEntry.Key

		// Reuse buffer to store current key for deduplicating other streams
		m.currKeyBuf = append(m.currKeyBuf[:0], candKey...)

		// Advance root iterator
		m.h[0].iter.Next()
		if m.h[0].iter.Valid() {
			m.down(0, len(m.h))
		} else {
			m.pop()
		}

		// Discard and advance all duplicate versions across other iterators pointing to this same key
		for len(m.h) > 0 && bytes.Equal(m.h[0].iter.Key(), m.currKeyBuf) {
			m.h[0].iter.Next()
			if m.h[0].iter.Valid() {
				m.down(0, len(m.h))
			} else {
				m.pop()
			}
		}

		if m.dropTombstones && candidateEntry.IsTombstone() {
			continue
		}

		m.current = candidateEntry
		m.valid = true
		return
	}
}

// Valid returns true if the iterator points to a valid entry.
func (m *MergedIterator) Valid() bool {
	return m.valid
}

// Next moves to the next unique key in sorted order.
func (m *MergedIterator) Next() {
	if m.Valid() {
		m.advance()
	}
}

// Seek repositions all underlying iterators to target, rebuilds heap, and advances.
func (m *MergedIterator) Seek(target []byte) {
	m.h = m.h[:0]
	for idx, it := range m.iters {
		if it != nil {
			it.Seek(target)
			if it.Valid() {
				m.push(iterItem{iter: it, priority: idx})
			}
		}
	}
	m.advance()
}

// Key returns the current key.
func (m *MergedIterator) Key() []byte {
	if !m.Valid() {
		return nil
	}
	return m.current.Key
}

// Value returns the current value.
func (m *MergedIterator) Value() []byte {
	if !m.Valid() {
		return nil
	}
	return m.current.Value
}

// Entry returns the current entry object.
func (m *MergedIterator) Entry() *Entry {
	if !m.Valid() {
		return nil
	}
	return m.current
}

// Close closes all underlying iterators.
func (m *MergedIterator) Close() error {
	var firstErr error
	for _, it := range m.iters {
		if it != nil {
			if err := it.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	m.h = nil
	m.valid = false
	m.current = nil
	m.currKeyBuf = nil
	return firstErr
}
