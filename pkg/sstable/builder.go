package sstable

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/engine/lsm-trees/pkg/core"
)

// Metadata stores summary information about an SSTable.
type Metadata struct {
	ID         uint64 `json:"id"`
	Level      int    `json:"level"`
	EntryCount int64  `json:"entry_count"`
	DataSize   int64  `json:"data_size"`
	MinKey     []byte `json:"min_key"`
	MaxKey     []byte `json:"max_key"`
	CreatedAt  int64  `json:"created_at"`
}

// BuilderOptions specifies parameters for building an SSTable.
type BuilderOptions struct {
	Dir       string
	ID        uint64
	Level     int
	BlockSize int     // target data block size in bytes (e.g. 4096)
	BloomFp   float64 // false positive probability (e.g. 0.01)
	EstKeys   int     // estimated number of keys for sizing the Bloom filter
}

// SSTableBuilder builds immutable SSTable files on disk.
type SSTableBuilder struct {
	opts         BuilderOptions
	dataFile     *os.File
	dataOffset   uint64
	currBlock    []*core.Entry
	currBlockLen int
	index        *SparseIndex
	bloom        *BloomFilter
	minKey       []byte
	maxKey       []byte
	entryCount   int64
	closed       bool
}

// NewBuilder initializes a new SSTableBuilder.
func NewBuilder(opts BuilderOptions) (*SSTableBuilder, error) {
	if opts.BlockSize <= 0 {
		opts.BlockSize = 4096
	}
	if opts.BloomFp <= 0 {
		opts.BloomFp = 0.01
	}
	if opts.EstKeys <= 0 {
		opts.EstKeys = 1000
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create sstable dir: %w", err)
	}

	dataPath := filepath.Join(opts.Dir, fmt.Sprintf("sst_%06d.data", opts.ID))
	dataFile, err := os.OpenFile(dataPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create data file %s: %w", dataPath, err)
	}

	return &SSTableBuilder{
		opts:       opts,
		dataFile:   dataFile,
		dataOffset: 0,
		index:      NewSparseIndex(),
		bloom:      NewBloomFilter(opts.EstKeys, opts.BloomFp),
	}, nil
}

// Add appends a key-value entry to the SSTable. Entries MUST be added in strictly ascending key order.
func (b *SSTableBuilder) Add(entry *core.Entry) error {
	if b.closed {
		return fmt.Errorf("builder is closed")
	}

	if len(b.minKey) == 0 || bytes.Compare(entry.Key, b.minKey) < 0 {
		b.minKey = make([]byte, len(entry.Key))
		copy(b.minKey, entry.Key)
	}
	if len(b.maxKey) == 0 || bytes.Compare(entry.Key, b.maxKey) > 0 {
		b.maxKey = make([]byte, len(entry.Key))
		copy(b.maxKey, entry.Key)
	}

	b.bloom.Add(entry.Key)
	b.currBlock = append(b.currBlock, entry)
	b.currBlockLen += entry.Size()
	b.entryCount++

	if b.currBlockLen >= b.opts.BlockSize {
		if err := b.flushBlock(); err != nil {
			return err
		}
	}

	return nil
}

func (b *SSTableBuilder) flushBlock() error {
	if len(b.currBlock) == 0 {
		return nil
	}

	block := &Block{Entries: b.currBlock}
	payload := block.Encode()
	length := uint32(len(payload))

	// Record block in sparse index using the first key of the block
	firstKey := b.currBlock[0].Key
	b.index.Add(firstKey, b.dataOffset, length)

	// Write block payload to data file
	if _, err := b.dataFile.Write(payload); err != nil {
		return fmt.Errorf("failed writing data block: %w", err)
	}

	b.dataOffset += uint64(length)
	b.currBlock = nil
	b.currBlockLen = 0
	return nil
}

// Finish flushes any buffered data, writes index, bloom filter, and metadata, and closes files.
func (b *SSTableBuilder) Finish() (*Metadata, error) {
	if b.closed {
		return nil, fmt.Errorf("builder already closed")
	}
	b.closed = true

	// 1. Flush remaining block if any
	if err := b.flushBlock(); err != nil {
		_ = b.dataFile.Close()
		return nil, err
	}

	if err := b.dataFile.Sync(); err != nil {
		_ = b.dataFile.Close()
		return nil, err
	}
	if err := b.dataFile.Close(); err != nil {
		return nil, err
	}

	// 2. Write index file
	indexPath := filepath.Join(b.opts.Dir, fmt.Sprintf("sst_%06d.index", b.opts.ID))
	indexBytes := b.index.Encode()
	if err := os.WriteFile(indexPath, indexBytes, 0644); err != nil {
		return nil, fmt.Errorf("failed writing index file: %w", err)
	}

	// 3. Write bloom filter file
	filterPath := filepath.Join(b.opts.Dir, fmt.Sprintf("sst_%06d.filter", b.opts.ID))
	filterBytes := b.bloom.Encode()
	if err := os.WriteFile(filterPath, filterBytes, 0644); err != nil {
		return nil, fmt.Errorf("failed writing filter file: %w", err)
	}

	// 4. Write metadata file
	meta := &Metadata{
		ID:         b.opts.ID,
		Level:      b.opts.Level,
		EntryCount: b.entryCount,
		DataSize:   int64(b.dataOffset),
		MinKey:     b.minKey,
		MaxKey:     b.maxKey,
		CreatedAt:  time.Now().UnixNano(),
	}

	metaBytes, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed marshaling metadata: %w", err)
	}

	metaPath := filepath.Join(b.opts.Dir, fmt.Sprintf("sst_%06d.meta", b.opts.ID))
	if err := os.WriteFile(metaPath, metaBytes, 0644); err != nil {
		return nil, fmt.Errorf("failed writing meta file: %w", err)
	}

	return meta, nil
}
