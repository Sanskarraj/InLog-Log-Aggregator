package compactor

import (
	"bytes"
	"math"

	"github.com/engine/lsm-trees/pkg/sstable"
)

// LevelConfig specifies capacity guidelines for leveled compaction.
type LevelConfig struct {
	L0CompactionTrigger int   // e.g. 4 tables
	BaseLevelSizeMB     int64 // L1 target size, e.g. 10MB
	LevelMultiplier     int64 // multiplier for higher levels, e.g. 10
	MaxLevels           int   // max levels (e.g. 7)
}

// DefaultLevelConfig provides standard production defaults.
func DefaultLevelConfig() LevelConfig {
	return LevelConfig{
		L0CompactionTrigger: 4,
		BaseLevelSizeMB:     10,
		LevelMultiplier:     10,
		MaxLevels:           7,
	}
}

// CompactionTask encapsulates a scheduled compaction job between Level and Level+1.
type CompactionTask struct {
	FromLevel int
	ToLevel   int
	InputFrom []uint64 // SSTable IDs from FromLevel
	InputTo   []uint64 // Overlapping SSTable IDs from ToLevel
	IsBottom  bool     // True if ToLevel is the deepest active level
}

// Picker evaluates level sizes and selects candidate SSTables for compaction.
type Picker struct {
	config LevelConfig
}

// NewPicker creates a new compaction picker.
func NewPicker(cfg LevelConfig) *Picker {
	return &Picker{config: cfg}
}

// PickCompaction inspects the current levels and readers to find the highest-priority compaction task.
func (p *Picker) PickCompaction(
	levels map[int][]uint64,
	readers map[uint64]*sstable.SSTableReader,
) *CompactionTask {
	// 1. Check Level 0: based on table count
	l0Tables := levels[0]
	if len(l0Tables) >= p.config.L0CompactionTrigger {
		task := &CompactionTask{
			FromLevel: 0,
			ToLevel:   1,
			InputFrom: append([]uint64(nil), l0Tables...),
		}
		task.InputTo = p.findOverlapping(0, l0Tables, 1, levels[1], readers)
		task.IsBottom = p.isBottomLevel(1, levels)
		return task
	}

	// 2. Check higher levels: based on size ratio score
	bestLevel := -1
	bestScore := 1.0

	for lvl := 1; lvl < p.config.MaxLevels; lvl++ {
		tbls := levels[lvl]
		if len(tbls) == 0 {
			continue
		}

		var totalSize int64
		for _, id := range tbls {
			if r, ok := readers[id]; ok {
				totalSize += r.Meta().DataSize
			}
		}

		maxSize := p.maxBytesForLevel(lvl)
		score := float64(totalSize) / float64(maxSize)
		if score > bestScore {
			bestScore = score
			bestLevel = lvl
		}
	}

	if bestLevel == -1 {
		return nil // No level requires compaction right now
	}

	// Pick oldest/first table from bestLevel
	candidateID := levels[bestLevel][0]
	task := &CompactionTask{
		FromLevel: bestLevel,
		ToLevel:   bestLevel + 1,
		InputFrom: []uint64{candidateID},
	}
	task.InputTo = p.findOverlapping(bestLevel, []uint64{candidateID}, bestLevel+1, levels[bestLevel+1], readers)
	task.IsBottom = p.isBottomLevel(bestLevel+1, levels)
	return task
}

func (p *Picker) maxBytesForLevel(lvl int) int64 {
	base := p.config.BaseLevelSizeMB * 1024 * 1024
	multiplier := math.Pow(float64(p.config.LevelMultiplier), float64(lvl-1))
	return int64(float64(base) * multiplier)
}

func (p *Picker) findOverlapping(
	fromLvl int,
	fromTables []uint64,
	toLvl int,
	toTables []uint64,
	readers map[uint64]*sstable.SSTableReader,
) []uint64 {
	if len(toTables) == 0 {
		return nil
	}

	var minKey, maxKey []byte
	for _, id := range fromTables {
		r, ok := readers[id]
		if !ok {
			continue
		}
		if len(minKey) == 0 || bytes.Compare(r.Meta().MinKey, minKey) < 0 {
			minKey = r.Meta().MinKey
		}
		if len(maxKey) == 0 || bytes.Compare(r.Meta().MaxKey, maxKey) > 0 {
			maxKey = r.Meta().MaxKey
		}
	}

	var overlapping []uint64
	for _, id := range toTables {
		r, ok := readers[id]
		if !ok {
			continue
		}
		// Overlap condition: not (r.MaxKey < minKey || r.MinKey > maxKey)
		if bytes.Compare(r.Meta().MaxKey, minKey) < 0 || bytes.Compare(r.Meta().MinKey, maxKey) > 0 {
			continue
		}
		overlapping = append(overlapping, id)
	}

	return overlapping
}

func (p *Picker) isBottomLevel(lvl int, levels map[int][]uint64) bool {
	for l := lvl + 1; l < p.config.MaxLevels; l++ {
		if len(levels[l]) > 0 {
			return false
		}
	}
	return true
}
