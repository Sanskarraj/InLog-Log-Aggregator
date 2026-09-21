package sstable

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/engine/lsm-trees/pkg/core"
)

// SSTableReader provides fast point lookups and sequential iteration over an on-disk SSTable.
type SSTableReader struct {
	mu          sync.Mutex
	dir         string
	id          uint64
	meta        Metadata
	index       *SparseIndex
	bloom       *BloomFilter
	dataFile    *os.File
	closed      bool
	bypassBloom bool
}

// OpenReader loads metadata, sparse index, bloom filter, and opens the data file for an SSTable.
func OpenReader(dir string, id uint64) (*SSTableReader, error) {
	// 1. Read metadata
	metaPath := filepath.Join(dir, fmt.Sprintf("sst_%06d.meta", id))
	metaBytes, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read sstable meta file %s: %w", metaPath, err)
	}

	var meta Metadata
	if err := json.Unmarshal(metaBytes, &meta); err != nil {
		return nil, fmt.Errorf("corrupt sstable meta: %w", err)
	}

	// 2. Read sparse index
	indexPath := filepath.Join(dir, fmt.Sprintf("sst_%06d.index", id))
	indexBytes, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read index file %s: %w", indexPath, err)
	}
	index, err := DecodeSparseIndex(indexBytes)
	if err != nil {
		return nil, fmt.Errorf("corrupt sparse index: %w", err)
	}

	// 3. Read bloom filter
	filterPath := filepath.Join(dir, fmt.Sprintf("sst_%06d.filter", id))
	filterBytes, err := os.ReadFile(filterPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read filter file %s: %w", filterPath, err)
	}
	bloom, err := DecodeBloomFilter(filterBytes)
	if err != nil {
		return nil, fmt.Errorf("corrupt bloom filter: %w", err)
	}

	// 4. Open data file
	dataPath := filepath.Join(dir, fmt.Sprintf("sst_%06d.data", id))
	dataFile, err := os.Open(dataPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open data file %s: %w", dataPath, err)
	}

	return &SSTableReader{
		dir:      dir,
		id:       id,
		meta:     meta,
		index:    index,
		bloom:    bloom,
		dataFile: dataFile,
	}, nil
}

// ID returns the SSTable identifier.
func (r *SSTableReader) ID() uint64 {
	return r.id
}

// Level returns the level of this SSTable.
func (r *SSTableReader) Level() int {
	return r.meta.Level
}

// Meta returns the SSTable metadata descriptor.
func (r *SSTableReader) Meta() Metadata {
	return r.meta
}

// SetBypassBloom toggles whether the Bloom filter check is bypassed on Get.
// Useful for benchmarking and empirically proving disk I/O savings from Bloom filter pruning.
func (r *SSTableReader) SetBypassBloom(bypass bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bypassBloom = bypass
}

// Get performs a high-efficiency point lookup for a key:
// 1. Min/Max key range pruning
// 2. Bloom filter check (skips disk seek if negative)
// 3. Sparse index binary search to locate block
// 4. Single block disk read + CRC32 verification
// 5. In-block scan for key
func (r *SSTableReader) Get(key []byte) (*core.Entry, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, false, core.ErrDatabaseClosed
	}

	// 1. Range bounds check
	if len(r.meta.MinKey) > 0 && bytes.Compare(key, r.meta.MinKey) < 0 {
		return nil, false, nil
	}
	if len(r.meta.MaxKey) > 0 && bytes.Compare(key, r.meta.MaxKey) > 0 {
		return nil, false, nil
	}

	// 2. Bloom filter test
	if !r.bypassBloom && !r.bloom.MayContain(key) {
		return nil, false, nil // Guaranteed not in this SSTable!
	}

	// 3. Sparse index binary search
	blockEntry, found := r.index.FindCandidateBlock(key)
	if !found {
		return nil, false, nil
	}

	// 4. Read data block
	blockData := make([]byte, blockEntry.Length)
	if _, err := r.dataFile.ReadAt(blockData, int64(blockEntry.Offset)); err != nil {
		return nil, false, fmt.Errorf("failed to read data block at offset %d: %w", blockEntry.Offset, err)
	}

	// 5. Decode block and verify CRC32
	block, err := DecodeBlock(blockData)
	if err != nil {
		return nil, false, err
	}

	// 6. Find key in block
	for _, entry := range block.Entries {
		if bytes.Equal(entry.Key, key) {
			return entry, true, nil
		}
	}

	return nil, false, nil
}

// Close closes the underlying data file.
func (r *SSTableReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}
	r.closed = true
	return r.dataFile.Close()
}

// NewIterator creates a sequential iterator over the SSTable.
func (r *SSTableReader) NewIterator() (*SSTableIterator, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil, core.ErrDatabaseClosed
	}

	it := &SSTableIterator{
		reader:   r,
		blockIdx: 0,
		entryIdx: 0,
	}

	if err := it.loadBlock(0); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return it, nil
}

// SSTableIterator iterates through all entries in an SSTable in ascending order.
type SSTableIterator struct {
	reader   *SSTableReader
	blockIdx int
	entryIdx int
	currBlock *Block
}

func (it *SSTableIterator) loadBlock(idx int) error {
	if idx < 0 || idx >= len(it.reader.index.Entries) {
		it.currBlock = nil
		return io.EOF
	}

	bEntry := it.reader.index.Entries[idx]
	blockData := make([]byte, bEntry.Length)
	if _, err := it.reader.dataFile.ReadAt(blockData, int64(bEntry.Offset)); err != nil {
		return err
	}

	blk, err := DecodeBlock(blockData)
	if err != nil {
		return err
	}

	it.currBlock = blk
	it.blockIdx = idx
	it.entryIdx = 0
	return nil
}

// Valid returns true if the iterator is currently pointing to a valid entry.
func (it *SSTableIterator) Valid() bool {
	return it.currBlock != nil && it.entryIdx < len(it.currBlock.Entries)
}

// Next moves the iterator to the next key-value entry.
func (it *SSTableIterator) Next() {
	if !it.Valid() {
		return
	}

	it.entryIdx++
	if it.entryIdx >= len(it.currBlock.Entries) {
		// Advance to next block
		_ = it.loadBlock(it.blockIdx + 1)
	}
}

// Seek positions the iterator at the first entry with key >= target.
func (it *SSTableIterator) Seek(target []byte) {
	// Find candidate block in sparse index
	bEntry, found := it.reader.index.FindCandidateBlock(target)
	startBlockIdx := 0
	if found {
		for i, entry := range it.reader.index.Entries {
			if entry.Offset == bEntry.Offset {
				startBlockIdx = i
				break
			}
		}
	}

	for blkIdx := startBlockIdx; blkIdx < len(it.reader.index.Entries); blkIdx++ {
		if err := it.loadBlock(blkIdx); err != nil {
			it.currBlock = nil
			return
		}

		for eIdx, entry := range it.currBlock.Entries {
			if bytes.Compare(entry.Key, target) >= 0 {
				it.entryIdx = eIdx
				return
			}
		}
	}

	it.currBlock = nil
}

// Entry returns the current entry.
func (it *SSTableIterator) Entry() *core.Entry {
	if !it.Valid() {
		return nil
	}
	return it.currBlock.Entries[it.entryIdx]
}

// Key returns the current key.
func (it *SSTableIterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currBlock.Entries[it.entryIdx].Key
}

// Value returns the current value.
func (it *SSTableIterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.currBlock.Entries[it.entryIdx].Value
}

// Close releases the iterator resources.
func (it *SSTableIterator) Close() error {
	it.currBlock = nil
	return nil
}
