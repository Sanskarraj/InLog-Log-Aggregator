package compactor

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/engine/lsm-trees/pkg/core"
	"github.com/engine/lsm-trees/pkg/sstable"
)

// CompactorStats tracks performance and workload statistics of leveled compaction.
type CompactorStats struct {
	mu               sync.Mutex
	TotalCompactions int64
	BytesRead        int64
	BytesWritten     int64
	TombstonesPurged int64
	LastDuration     time.Duration
}

// Compactor coordinates background leveled merges.
type Compactor struct {
	dir         string
	manifest    *core.Manifest
	picker      *Picker
	targetTable int64 // target SSTable size, e.g. 2MB or 4MB
	stats       CompactorStats
}

// NewCompactor creates a new Compactor instance.
func NewCompactor(dir string, manifest *core.Manifest, cfg LevelConfig, targetTableSize int64) *Compactor {
	if targetTableSize <= 0 {
		targetTableSize = 2 * 1024 * 1024 // 2MB default per table
	}
	return &Compactor{
		dir:         dir,
		manifest:    manifest,
		picker:      NewPicker(cfg),
		targetTable: targetTableSize,
	}
}

// Stats returns a copy of compaction metrics.
func (c *Compactor) Stats() CompactorStats {
	c.stats.mu.Lock()
	defer c.stats.mu.Unlock()
	return CompactorStats{
		TotalCompactions: c.stats.TotalCompactions,
		BytesRead:        c.stats.BytesRead,
		BytesWritten:     c.stats.BytesWritten,
		TombstonesPurged: c.stats.TombstonesPurged,
		LastDuration:     c.stats.LastDuration,
	}
}

// Compact executes a single compaction task if one is eligible.
// It returns the newly created table IDs, deleted table IDs, and any error.
func (c *Compactor) Compact(
	readers map[uint64]*sstable.SSTableReader,
) ([]uint64, []uint64, error) {
	levels := c.manifest.GetAllLevels()
	task := c.picker.PickCompaction(levels, readers)
	if task == nil {
		return nil, nil, nil // No compaction needed
	}

	startTime := time.Now()

	// 1. Gather all input iterators
	allInputs := append([]uint64(nil), task.InputFrom...)
	allInputs = append(allInputs, task.InputTo...)

	var iters []core.Iterator
	var inputBytes int64
	for _, id := range allInputs {
		r, ok := readers[id]
		if !ok {
			return nil, nil, fmt.Errorf("compaction missing reader for table %d", id)
		}
		inputBytes += r.Meta().DataSize
		it, err := r.NewIterator()
		if err != nil {
			return nil, nil, fmt.Errorf("failed opening iterator for table %d: %w", id, err)
		}
		iters = append(iters, it)
	}

	// 2. Merge-sort input streams
	mergeIter := core.NewMergedIterator(iters, task.IsBottom)
	defer mergeIter.Close()

	var newTableIDs []uint64
	var currentBuilder *sstable.SSTableBuilder
	var currentSize int64
	var tombstonesPurged int64
	var outputBytes int64

	startNewTable := func() error {
		newID := c.manifest.NextID()
		b, err := sstable.NewBuilder(sstable.BuilderOptions{
			Dir:       c.dir,
			ID:        newID,
			Level:     task.ToLevel,
			BlockSize: 4096,
			BloomFp:   0.01,
			EstKeys:   5000,
		})
		if err != nil {
			return err
		}
		currentBuilder = b
		currentSize = 0
		newTableIDs = append(newTableIDs, newID)
		return nil
	}

	for mergeIter.Valid() {
		entry := mergeIter.Entry()

		if task.IsBottom && entry.IsTombstone() {
			tombstonesPurged++
			mergeIter.Next()
			continue
		}

		if currentBuilder == nil {
			if err := startNewTable(); err != nil {
				return nil, nil, err
			}
		}

		if err := currentBuilder.Add(entry); err != nil {
			return nil, nil, fmt.Errorf("failed adding entry to compacted table: %w", err)
		}

		entrySize := int64(entry.Size())
		currentSize += entrySize
		outputBytes += entrySize

		if currentSize >= c.targetTable {
			meta, err := currentBuilder.Finish()
			if err != nil {
				return nil, nil, fmt.Errorf("failed finishing compacted table: %w", err)
			}
			currentBuilder = nil
			if meta != nil {
				// Completed SSTable
			}
		}

		mergeIter.Next()
	}

	// Finish trailing table if any
	if currentBuilder != nil {
		_, err := currentBuilder.Finish()
		if err != nil {
			return nil, nil, fmt.Errorf("failed finishing final compacted table: %w", err)
		}
	}

	// 3. Commit version edit to manifest
	var addedMetas []core.TableMeta
	for _, nid := range newTableIDs {
		addedMetas = append(addedMetas, core.TableMeta{ID: nid, Level: task.ToLevel})
	}

	edit := &core.VersionEdit{
		AddedTables:   addedMetas,
		DeletedTables: allInputs,
	}

	if err := c.manifest.LogAndApply(edit); err != nil {
		return nil, nil, fmt.Errorf("failed committing compaction edit: %w", err)
	}

	// Update statistics
	c.stats.mu.Lock()
	c.stats.TotalCompactions++
	c.stats.BytesRead += inputBytes
	c.stats.BytesWritten += outputBytes
	c.stats.TombstonesPurged += tombstonesPurged
	c.stats.LastDuration = time.Since(startTime)
	c.stats.mu.Unlock()

	return newTableIDs, allInputs, nil
}

// RemoveSSTableFiles unlinks the four files (.data, .index, .filter, .meta) associated with an SSTable.
func RemoveSSTableFiles(dir string, id uint64) {
	exts := []string{".data", ".index", ".filter", ".meta"}
	for _, ext := range exts {
		p := filepath.Join(dir, fmt.Sprintf("sst_%06d%s", id, ext))
		_ = os.Remove(p)
	}
}
