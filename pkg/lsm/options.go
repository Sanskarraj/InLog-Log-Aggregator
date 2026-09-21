package lsm

import (
	"time"

	"github.com/engine/lsm-trees/pkg/wal"
)

// Options defines configuration parameters for the LSM-Tree engine.
type Options struct {
	Dir                string
	MemTableSize       int64          // Max bytes per MemTable before freezing and flushing (e.g. 4MB)
	TargetTableSize    int64          // Target size per SSTable (e.g. 2MB)
	BlockSize          int            // Data block size within SSTables (e.g. 4096 bytes)
	BloomFp            float64        // Bloom filter false positive rate (e.g. 0.01)
	SyncPolicy         wal.SyncPolicy // WAL durability policy
	SyncInterval       time.Duration  // Sync interval for wal.SyncBatch
	L0CompactionTrigger int           // Number of L0 tables triggering compaction (default 4)
	AutoCompaction     bool           // Enable background compaction worker
	CompactionInterval time.Duration  // Interval between background compaction checks
}

// DefaultOptions returns standard production defaults.
func DefaultOptions(dir string) Options {
	return Options{
		Dir:                dir,
		MemTableSize:       4 * 1024 * 1024, // 4MB
		TargetTableSize:    2 * 1024 * 1024, // 2MB
		BlockSize:          4096,
		BloomFp:            0.01,
		SyncPolicy:         wal.SyncAlways,
		SyncInterval:       50 * time.Millisecond,
		L0CompactionTrigger: 4,
		AutoCompaction:     true,
		CompactionInterval: 500 * time.Millisecond,
	}
}
