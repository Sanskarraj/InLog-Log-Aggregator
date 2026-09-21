package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/engine/lsm-trees/pkg/core"
)

const (
	maxHeight   = 20
	probability = 0.5 // Probability for height generation
)

type node struct {
	entry      *core.Entry
	next       []*node
	inlineNext [2]*node // eliminates slice allocation for 75% of all nodes (height 1 or 2)
}

func newNode(entry *core.Entry, height int) *node {
	n := &node{entry: entry}
	if height <= 2 {
		n.next = n.inlineNext[:height]
	} else {
		n.next = make([]*node, height)
	}
	return n
}

// SkipList is a concurrent-safe, ordered in-memory skip list.
type SkipList struct {
	mu     sync.RWMutex
	head   *node
	height int
	length int64
	bytes  int64
	rnd    *rand.Rand
	rndMu  sync.Mutex
}

// NewSkipList creates an initialized SkipList.
func NewSkipList() *SkipList {
	return &SkipList{
		head:   newNode(nil, maxHeight),
		height: 1,
		rnd:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *SkipList) randomHeight() int {
	s.rndMu.Lock()
	defer s.rndMu.Unlock()

	h := 1
	for h < maxHeight && s.rnd.Float64() < probability {
		h++
	}
	return h
}

// Put inserts or updates an entry in the SkipList.
// If the key already exists, the entry is replaced (or updated if timestamp is newer).
func (s *SkipList) Put(entry *core.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var update [maxHeight]*node // Stack allocated: 0 heap allocations
	curr := s.head

	for i := s.height - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].entry.Key, entry.Key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// Check if key already exists at level 0
	candidate := curr.next[0]
	if candidate != nil && bytes.Equal(candidate.entry.Key, entry.Key) {
		// Key exists: replace entry
		oldSize := candidate.entry.Size()
		candidate.entry = entry
		newSize := entry.Size()
		atomic.AddInt64(&s.bytes, int64(newSize-oldSize))
		return
	}

	// Key does not exist: create new node
	h := s.randomHeight()
	if h > s.height {
		for i := s.height; i < h; i++ {
			update[i] = s.head
		}
		s.height = h
	}

	n := newNode(entry, h)
	for i := 0; i < h; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}

	atomic.AddInt64(&s.length, 1)
	nodeOverhead := 32 + (h * 8) // estimated pointer overhead
	atomic.AddInt64(&s.bytes, int64(entry.Size()+nodeOverhead))
}

// Get finds an entry by key in O(log N) time.
func (s *SkipList) Get(key []byte) (*core.Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	curr := s.head
	for i := s.height - 1; i >= 0; i-- {
		for curr.next[i] != nil && bytes.Compare(curr.next[i].entry.Key, key) < 0 {
			curr = curr.next[i]
		}
	}

	candidate := curr.next[0]
	if candidate != nil && bytes.Equal(candidate.entry.Key, key) {
		return candidate.entry, true
	}
	return nil, false
}

// Length returns total unique key count in the SkipList.
func (s *SkipList) Length() int64 {
	return atomic.LoadInt64(&s.length)
}

// ByteSize returns approximate memory footprint in bytes.
func (s *SkipList) ByteSize() int64 {
	return atomic.LoadInt64(&s.bytes)
}

// Iterator returns an in-order forward iterator.
// Note: While iterating, the caller can call Seek, Next, Valid, Entry.
func (s *SkipList) NewIterator() *SkipListIterator {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Snapshot nodes at level 0 for consistent iteration
	var nodes []*core.Entry
	curr := s.head.next[0]
	for curr != nil {
		nodes = append(nodes, curr.entry)
		curr = curr.next[0]
	}

	return &SkipListIterator{
		entries: nodes,
		idx:     0,
	}
}

// SkipListIterator provides in-order traversal of entries.
type SkipListIterator struct {
	entries []*core.Entry
	idx     int
}

// Valid returns true if the iterator is at a valid item.
func (it *SkipListIterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.entries)
}

// Next moves the iterator to the next entry.
func (it *SkipListIterator) Next() {
	if it.Valid() {
		it.idx++
	}
}

// Seek positions the iterator at the first entry whose key >= target.
func (it *SkipListIterator) Seek(target []byte) {
	low := 0
	high := len(it.entries) - 1
	ans := len(it.entries)

	for low <= high {
		mid := (low + high) / 2
		if bytes.Compare(it.entries[mid].Key, target) >= 0 {
			ans = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}
	it.idx = ans
}

// Entry returns the current entry.
func (it *SkipListIterator) Entry() *core.Entry {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx]
}

// Key returns the current key.
func (it *SkipListIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].Key
}

// Value returns the current value.
func (it *SkipListIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.entries[it.idx].Value
}

// Close releases iterator resources.
func (it *SkipListIterator) Close() error {
	it.entries = nil
	it.idx = -1
	return nil
}
