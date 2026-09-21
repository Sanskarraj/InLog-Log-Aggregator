package lsm

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/engine/lsm-trees/pkg/compactor"
	"github.com/engine/lsm-trees/pkg/core"
	"github.com/engine/lsm-trees/pkg/memtable"
	"github.com/engine/lsm-trees/pkg/sstable"
	"github.com/engine/lsm-trees/pkg/wal"
)

// EngineStats holds real-time telemetry and engine metrics.
type EngineStats struct {
	ActiveMemBytes            int64                    `json:"active_mem_bytes"`
	ActiveMemEntries          int64                    `json:"active_mem_entries"`
	ImmMemBytes               int64                    `json:"imm_mem_bytes"`
	SSTablesPerLevel          map[int]int              `json:"sstables_per_level"`
	TotalSSTables             int                      `json:"total_sstables"`
	TotalDiskBytes            int64                    `json:"total_disk_bytes"`
	TotalPuts                 int64                    `json:"total_puts"`
	TotalDeletes              int64                    `json:"total_deletes"`
	TotalGets                 int64                    `json:"total_gets"`
	BloomFilterHits           int64                    `json:"bloom_filter_hits"`
	BloomFilterMiss           int64                    `json:"bloom_filter_misses"`
	TotalFlushes              int64                    `json:"total_flushes"`
	CompactionStats           compactor.CompactorStats `json:"compaction_stats"`
	LogicalBytesWritten       int64                    `json:"logical_bytes_written"`
	PhysicalBytesWAL          int64                    `json:"physical_bytes_wal"`
	PhysicalBytesFlush        int64                    `json:"physical_bytes_flush"`
	PhysicalBytesCompaction   int64                    `json:"physical_bytes_compaction"`
	TotalPhysicalBytesWritten int64                    `json:"total_physical_bytes_written"`
	EngineWAF                 float64                  `json:"engine_waf"`
	EngineSAF                 float64                  `json:"engine_saf"`
	ActiveDataBytes           int64                    `json:"active_data_bytes"`
}

// Engine represents the top-level LSM-Tree storage engine.
type Engine struct {
	mu          sync.RWMutex
	dir         string
	opts        Options
	manifest    *Manifest
	activeWAL   *wal.WAL
	activeMem   *memtable.MemTable
	immMem      *memtable.MemTable
	compactor   *compactor.Compactor

	readersMu   sync.RWMutex
	readers     map[uint64]*sstable.SSTableReader
	levelTables [7][]uint64
	bypassBloom bool

	flushLock   sync.Mutex
	flushChan   chan struct{}
	compactChan chan struct{}
	closeChan   chan struct{}
	wg          sync.WaitGroup
	closed      bool

	// Telemetry counters
	puts                int64
	deletes             int64
	gets                int64
	bloomHits           int64
	bloomMisses         int64
	flushes             int64
	logicalBytesWritten int64
	physicalBytesWAL    int64
	physicalBytesFlush  int64
}

// Open initializes or recovers an LSM-Tree storage engine at the specified directory.
func Open(opts Options) (*Engine, error) {
	if opts.Dir == "" {
		return nil, fmt.Errorf("storage directory cannot be empty")
	}
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed creating db dir: %w", err)
	}

	manifest, err := OpenManifest(opts.Dir)
	if err != nil {
		return nil, fmt.Errorf("failed opening manifest: %w", err)
	}

	readers := make(map[uint64]*sstable.SSTableReader)
	for _, tables := range manifest.GetAllLevels() {
		for _, tid := range tables {
			r, err := sstable.OpenReader(opts.Dir, tid)
			if err != nil {
				// Close already opened readers
				for _, openR := range readers {
					_ = openR.Close()
				}
				_ = manifest.Close()
				return nil, fmt.Errorf("failed loading sstable reader %d: %w", tid, err)
			}
			readers[tid] = r
		}
	}

	cfg := compactor.LevelConfig{
		L0CompactionTrigger: opts.L0CompactionTrigger,
		BaseLevelSizeMB:     10,
		LevelMultiplier:     10,
		MaxLevels:           7,
	}
	comp := compactor.NewCompactor(opts.Dir, manifest, cfg, opts.TargetTableSize)

	e := &Engine{
		dir:         opts.Dir,
		opts:        opts,
		manifest:    manifest,
		readers:     readers,
		compactor:   comp,
		flushChan:   make(chan struct{}, 1),
		compactChan: make(chan struct{}, 1),
		closeChan:   make(chan struct{}),
	}
	e.updateLevelTablesUnderLock()

	// Replay any existing WAL files (crash recovery)
	if err := e.recoverWAL(); err != nil {
		_ = manifest.Close()
		return nil, fmt.Errorf("wal recovery failed: %w", err)
	}

	// Initialize active memtable and new active WAL
	if e.activeMem == nil {
		nextWalID := manifest.NextID()
		w, err := wal.Open(nextWalID, wal.Options{
			Dir:          opts.Dir,
			SyncPolicy:   opts.SyncPolicy,
			SyncInterval: opts.SyncInterval,
		})
		if err != nil {
			_ = manifest.Close()
			return nil, fmt.Errorf("failed to open active wal: %w", err)
		}
		e.activeWAL = w
		e.activeMem = memtable.New(nextWalID, opts.MemTableSize)
	}

	// Start background workers
	e.wg.Add(1)
	go e.flushWorker()

	if opts.AutoCompaction {
		e.wg.Add(1)
		go e.compactionWorker()
	}

	return e, nil
}

func (e *Engine) updateLevelTablesUnderLock() {
	for lvl := 0; lvl < 7; lvl++ {
		e.levelTables[lvl] = e.manifest.GetLevelTables(lvl)
	}
}

// SetBypassBloom configures Bloom filter bypassing across all active and future SSTable readers.
// Used for benchmarking to empirically verify disk I/O savings provided by Bloom filtering.
func (e *Engine) SetBypassBloom(bypass bool) {
	e.readersMu.Lock()
	defer e.readersMu.Unlock()
	e.bypassBloom = bypass
	for _, r := range e.readers {
		r.SetBypassBloom(bypass)
	}
}

// recoverWAL replays all uncommitted WAL logs into memory upon restart.
func (e *Engine) recoverWAL() error {
	walIDs, err := wal.ListWALFiles(e.dir)
	if err != nil {
		return err
	}

	if len(walIDs) == 0 {
		return nil
	}

	// Replay each WAL in ascending order
	var latestMem *memtable.MemTable
	var latestWAL *wal.WAL

	for i, wid := range walIDs {
		mem := memtable.New(wid, e.opts.MemTableSize)
		walPath := filepath.Join(e.dir, fmt.Sprintf("wal_%06d.log", wid))

		replayed, err := wal.Recover(walPath, func(entry *Entry) error {
			mem.Put(entry)
			return nil
		})
		if err != nil {
			return fmt.Errorf("recovery error on wal %d: %w", wid, err)
		}

		// If this is not the very last WAL file or if mem reached flush limit, flush it as an SSTable
		if i < len(walIDs)-1 {
			if mem.EntryCount() > 0 {
				if err := e.flushMemTableDirect(mem); err != nil {
					return fmt.Errorf("failed flushing recovered memtable: %w", err)
				}
			}
			_ = os.Remove(walPath)
		} else {
			// Keep the latest WAL as active WAL
			latestMem = mem
			w, err := wal.Open(wid, wal.Options{
				Dir:          e.dir,
				SyncPolicy:   e.opts.SyncPolicy,
				SyncInterval: e.opts.SyncInterval,
			})
			if err != nil {
				return err
			}
			latestWAL = w
			_ = replayed
		}
	}

	e.activeMem = latestMem
	e.activeWAL = latestWAL
	return nil
}

// Put writes or updates a key-value pair.
func (e *Engine) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrDatabaseClosed
	}

	// Write stall: if activeMem needs flushing AND immutable is still flushing,
	// back off briefly so disk I/O can catch up without unbounded MemTable growth.
	for e.activeMem.ShouldFlush() && e.immMem != nil {
		e.mu.Unlock()
		time.Sleep(200 * time.Microsecond)
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return ErrDatabaseClosed
		}
	}

	entry := core.NewPutEntry(key, value)

	// 1. Write to WAL first (durability)
	if err := e.activeWAL.Write(entry); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("wal write failed: %w", err)
	}

	// 2. Write to active MemTable
	e.activeMem.Put(entry)
	atomic.AddInt64(&e.puts, 1)
	entrySize := int64(len(key) + len(value))
	atomic.AddInt64(&e.logicalBytesWritten, entrySize)
	atomic.AddInt64(&e.physicalBytesWAL, int64(8+8+1+2+4)+entrySize)

	// 3. Trigger flush if capacity exceeded
	shouldFlush := e.activeMem.ShouldFlush()
	if shouldFlush && e.immMem == nil {
		e.rotateMemTable()
	}
	e.mu.Unlock()

	if shouldFlush {
		e.triggerFlush()
	}

	return nil
}

// Delete marks a key with a tombstone marker.
func (e *Engine) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}

	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrDatabaseClosed
	}

	// Write stall: if activeMem needs flushing AND immutable is still flushing,
	// back off briefly so disk I/O can catch up without unbounded MemTable growth.
	for e.activeMem.ShouldFlush() && e.immMem != nil {
		e.mu.Unlock()
		time.Sleep(200 * time.Microsecond)
		e.mu.Lock()
		if e.closed {
			e.mu.Unlock()
			return ErrDatabaseClosed
		}
	}

	entry := NewDeleteEntry(key)

	if err := e.activeWAL.Write(entry); err != nil {
		e.mu.Unlock()
		return fmt.Errorf("wal delete failed: %w", err)
	}

	e.activeMem.Put(entry)
	atomic.AddInt64(&e.deletes, 1)
	entrySize := int64(len(key))
	atomic.AddInt64(&e.logicalBytesWritten, entrySize)
	atomic.AddInt64(&e.physicalBytesWAL, int64(8+8+1+2+4)+entrySize)

	shouldFlush := e.activeMem.ShouldFlush()
	if shouldFlush && e.immMem == nil {
		e.rotateMemTable()
	}
	e.mu.Unlock()

	if shouldFlush {
		e.triggerFlush()
	}

	return nil
}

// rotateMemTable converts current active MemTable to immutable MemTable and starts a new one.
func (e *Engine) rotateMemTable() {
	e.activeMem.MarkImmutable()
	e.immMem = e.activeMem
	_ = e.activeWAL.Close()

	nextID := e.manifest.NextID()
	newWAL, err := wal.Open(nextID, wal.Options{
		Dir:          e.dir,
		SyncPolicy:   e.opts.SyncPolicy,
		SyncInterval: e.opts.SyncInterval,
	})
	if err == nil {
		e.activeWAL = newWAL
		e.activeMem = memtable.New(nextID, e.opts.MemTableSize)
	}
}

// Get retrieves the value associated with key.
// Traversal order: Active MemTable -> Immutable MemTable -> Level 0 SSTables (newest to oldest) -> Levels 1..L
func (e *Engine) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrEmptyKey
	}

	atomic.AddInt64(&e.gets, 1)

	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrDatabaseClosed
	}

	// 1. Search Active MemTable
	if entry, found := e.activeMem.Get(key); found {
		e.mu.RUnlock()
		if entry.IsTombstone() {
			return nil, ErrKeyNotFound
		}
		return core.CloneBytes(entry.Value), nil
	}

	// 2. Search Immutable MemTable if present
	if e.immMem != nil {
		if entry, found := e.immMem.Get(key); found {
			e.mu.RUnlock()
			if entry.IsTombstone() {
				return nil, ErrKeyNotFound
			}
			return core.CloneBytes(entry.Value), nil
		}
	}
	e.mu.RUnlock()

	// 3. Search Disk SSTables under dedicated readers lock (zero allocation)
	e.readersMu.RLock()
	defer e.readersMu.RUnlock()

	// Search Level 0 SSTables (newest first, can have overlapping keys)
	l0Tables := e.levelTables[0]
	for i := len(l0Tables) - 1; i >= 0; i-- {
		tid := l0Tables[i]
		r, ok := e.readers[tid]
		if !ok {
			continue
		}

		entry, found, err := r.Get(key)
		if err != nil {
			return nil, err
		}
		if found {
			atomic.AddInt64(&e.bloomHits, 1)
			if entry.IsTombstone() {
				return nil, ErrKeyNotFound
			}
			return core.CloneBytes(entry.Value), nil
		} else {
			atomic.AddInt64(&e.bloomMisses, 1)
		}
	}

	// 4. Search Levels 1..MaxLevels (non-overlapping key ranges per level)
	for lvl := 1; lvl < 7; lvl++ {
		tbls := e.levelTables[lvl]
		if len(tbls) == 0 {
			continue
		}

		// Find candidate SSTable in level whose [MinKey, MaxKey] covers key
		for _, tid := range tbls {
			r, ok := e.readers[tid]
			if !ok {
				continue
			}

			if bytes.Compare(key, r.Meta().MinKey) >= 0 && bytes.Compare(key, r.Meta().MaxKey) <= 0 {
				entry, found, err := r.Get(key)
				if err != nil {
					return nil, err
				}
				if found {
					atomic.AddInt64(&e.bloomHits, 1)
					if entry.IsTombstone() {
						return nil, ErrKeyNotFound
					}
					return core.CloneBytes(entry.Value), nil
				} else {
					atomic.AddInt64(&e.bloomMisses, 1)
				}
				break // Only one SSTable per level can contain the key
			}
		}
	}

	return nil, ErrKeyNotFound
}

// Scan performs a range query from startKey (inclusive) to endKey (inclusive).
// Combines active memtable, immutable memtable, and disk SSTables into a multi-way merge iterator.
func (e *Engine) Scan(startKey, endKey []byte) (*MergedIterator, error) {
	e.mu.RLock()
	if e.closed {
		e.mu.RUnlock()
		return nil, ErrDatabaseClosed
	}

	var iters []Iterator

	// 1. Active MemTable iterator
	iters = append(iters, e.activeMem.NewIterator())

	// 2. Immutable MemTable iterator if present
	if e.immMem != nil {
		iters = append(iters, e.immMem.NewIterator())
	}
	e.mu.RUnlock()

	e.readersMu.RLock()
	defer e.readersMu.RUnlock()

	// 3. Level 0 SSTables (newest to oldest)
	l0Tables := e.levelTables[0]
	for i := len(l0Tables) - 1; i >= 0; i-- {
		tid := l0Tables[i]
		if r, ok := e.readers[tid]; ok {
			it, err := r.NewIterator()
			if err == nil {
				iters = append(iters, it)
			}
		}
	}

	// 4. Levels 1..Max
	for lvl := 1; lvl < 7; lvl++ {
		for _, tid := range e.levelTables[lvl] {
			if r, ok := e.readers[tid]; ok {
				// Only include if key range might overlap [startKey, endKey]
				if len(endKey) > 0 && bytes.Compare(r.Meta().MinKey, endKey) > 0 {
					continue
				}
				if len(startKey) > 0 && bytes.Compare(r.Meta().MaxKey, startKey) < 0 {
					continue
				}
				it, err := r.NewIterator()
				if err == nil {
					iters = append(iters, it)
				}
			}
		}
	}

	merged := NewMergedIterator(iters, true)
	if len(startKey) > 0 {
		merged.Seek(startKey)
	}

	return merged, nil
}

// triggerFlush signals the background worker to flush the immutable memtable.
func (e *Engine) triggerFlush() {
	select {
	case e.flushChan <- struct{}{}:
	default:
	}
}

// flushWorker runs in the background and drains immutable MemTables to disk.
func (e *Engine) flushWorker() {
	defer e.wg.Done()

	for {
		select {
		case <-e.flushChan:
			_ = e.flushImmutable()
		case <-e.closeChan:
			return
		}
	}
}

func (e *Engine) flushImmutable() error {
	e.flushLock.Lock()
	defer e.flushLock.Unlock()

	e.mu.Lock()
	imm := e.immMem
	e.mu.Unlock()

	if imm == nil {
		return nil
	}

	if err := e.flushMemTableDirect(imm); err != nil {
		return err
	}

	e.mu.Lock()
	e.immMem = nil
	hasMoreToFlush := false
	if e.activeMem.ShouldFlush() {
		e.rotateMemTable()
		hasMoreToFlush = true
	}
	e.mu.Unlock()

	// Delete old WAL file associated with flushed memtable
	walPath := filepath.Join(e.dir, fmt.Sprintf("wal_%06d.log", imm.ID()))
	_ = os.Remove(walPath)

	atomic.AddInt64(&e.flushes, 1)

	if hasMoreToFlush {
		e.triggerFlush()
	}

	return nil
}

// flushMemTableDirect writes any MemTable directly into a Level 0 SSTable and updates manifest.
func (e *Engine) flushMemTableDirect(mem *memtable.MemTable) error {
	if mem.EntryCount() == 0 {
		return nil
	}

	sstID := e.manifest.NextID()
	builder, err := sstable.NewBuilder(sstable.BuilderOptions{
		Dir:       e.dir,
		ID:        sstID,
		Level:     0,
		BlockSize: e.opts.BlockSize,
		BloomFp:   e.opts.BloomFp,
		EstKeys:   int(mem.EntryCount()),
	})
	if err != nil {
		return fmt.Errorf("failed creating sstable builder: %w", err)
	}

	iter := mem.NewIterator()
	for iter.Valid() {
		if err := builder.Add(iter.Entry()); err != nil {
			return fmt.Errorf("failed adding entry to sstable: %w", err)
		}
		iter.Next()
	}

	meta, err := builder.Finish()
	if err != nil {
		return fmt.Errorf("failed finishing sstable %d: %w", sstID, err)
	}
	if meta != nil {
		atomic.AddInt64(&e.physicalBytesFlush, meta.DataSize)
	}

	// Open reader for new SSTable
	reader, err := sstable.OpenReader(e.dir, sstID)
	if err != nil {
		return fmt.Errorf("failed opening reader for flushed sstable: %w", err)
	}

	// Commit to manifest
	edit := &VersionEdit{
		AddedTables: []TableMeta{{ID: sstID, Level: 0}},
	}
	if err := e.manifest.LogAndApply(edit); err != nil {
		_ = reader.Close()
		return fmt.Errorf("failed logging flush edit: %w", err)
	}

	e.readersMu.Lock()
	if e.bypassBloom {
		reader.SetBypassBloom(true)
	}
	e.readers[sstID] = reader
	e.updateLevelTablesUnderLock()
	e.readersMu.Unlock()

	if e.opts.AutoCompaction {
		e.triggerCompaction()
	}

	return nil
}

// Flush forces an immediate rotation and flush of the current active MemTable to disk.
func (e *Engine) Flush() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return ErrDatabaseClosed
	}

	if e.activeMem.EntryCount() == 0 && e.immMem == nil {
		e.mu.Unlock()
		return nil
	}

	if e.immMem != nil {
		// Wait for existing immutable to flush first
		e.mu.Unlock()
		if err := e.flushImmutable(); err != nil {
			return err
		}
		e.mu.Lock()
	}

	e.rotateMemTable()
	e.mu.Unlock()

	return e.flushImmutable()
}

// triggerCompaction signals background worker to check and compact saturated levels.
func (e *Engine) triggerCompaction() {
	select {
	case e.compactChan <- struct{}{}:
	default:
	}
}

// compactionWorker runs periodically or on flush notifications to execute background compactions.
func (e *Engine) compactionWorker() {
	defer e.wg.Done()

	interval := e.opts.CompactionInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-e.compactChan:
			// Compact while L0 has >= L0CompactionTrigger tables
			for {
				if err := e.Compact(); err != nil {
					break
				}
				levels := e.manifest.GetAllLevels()
				if len(levels[0]) < e.opts.L0CompactionTrigger {
					break
				}
			}
		case <-ticker.C:
			_ = e.Compact()
		case <-e.closeChan:
			return
		}
	}
}

// Compact triggers a single compaction step across eligible levels.
func (e *Engine) Compact() error {
	e.flushLock.Lock()
	defer e.flushLock.Unlock()

	e.readersMu.RLock()
	readersSnapshot := make(map[uint64]*sstable.SSTableReader, len(e.readers))
	for k, v := range e.readers {
		readersSnapshot[k] = v
	}
	e.readersMu.RUnlock()

	newTables, deletedTables, err := e.compactor.Compact(readersSnapshot)
	if err != nil {
		return fmt.Errorf("compaction error: %w", err)
	}

	if len(newTables) == 0 && len(deletedTables) == 0 {
		return nil // Nothing compacted
	}

	// Open readers for newly created tables
	newReaders := make(map[uint64]*sstable.SSTableReader)
	for _, tid := range newTables {
		r, err := sstable.OpenReader(e.dir, tid)
		if err != nil {
			for _, nr := range newReaders {
				_ = nr.Close()
			}
			return fmt.Errorf("failed opening reader for compacted table %d: %w", tid, err)
		}
		newReaders[tid] = r
	}

	// Update active readers in engine
	e.readersMu.Lock()
	for tid, r := range newReaders {
		if e.bypassBloom {
			r.SetBypassBloom(true)
		}
		e.readers[tid] = r
	}
	for _, tid := range deletedTables {
		if r, ok := e.readers[tid]; ok {
			_ = r.Close()
			delete(e.readers, tid)
		}
	}
	e.updateLevelTablesUnderLock()
	e.readersMu.Unlock()

	// Safely unlink deleted SSTable files from disk
	for _, tid := range deletedTables {
		compactor.RemoveSSTableFiles(e.dir, tid)
	}

	return nil
}

// Stats returns a comprehensive telemetry snapshot of the LSM tree.
func (e *Engine) Stats() EngineStats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	levels := e.manifest.GetAllLevels()
	sstPerLevel := make(map[int]int)
	var totalDisk int64

	for lvl, tbls := range levels {
		sstPerLevel[lvl] = len(tbls)
		for _, tid := range tbls {
			if r, ok := e.readers[tid]; ok {
				totalDisk += r.Meta().DataSize
			}
		}
	}

	var immBytes int64
	if e.immMem != nil {
		immBytes = e.immMem.ByteSize()
	}

	e.readersMu.RLock()
	totalSSTables := len(e.readers)
	e.readersMu.RUnlock()

	compStats := e.compactor.Stats()
	logicalWritten := atomic.LoadInt64(&e.logicalBytesWritten)
	walBytes := atomic.LoadInt64(&e.physicalBytesWAL)
	flushBytes := atomic.LoadInt64(&e.physicalBytesFlush)
	compactBytes := compStats.BytesWritten
	totalPhysical := walBytes + flushBytes + compactBytes

	waf := 1.0
	if logicalWritten > 0 {
		waf = float64(totalPhysical) / float64(logicalWritten)
	}

	activeData := e.activeMem.ByteSize() + immBytes
	for lvl := 1; lvl < 7; lvl++ {
		for _, tid := range levels[lvl] {
			if r, ok := e.readers[tid]; ok {
				activeData += r.Meta().DataSize
			}
		}
	}
	saf := 1.0
	if activeData > 0 && totalDisk > 0 {
		saf = float64(totalDisk) / float64(activeData)
	}

	return EngineStats{
		ActiveMemBytes:            e.activeMem.ByteSize(),
		ActiveMemEntries:          e.activeMem.EntryCount(),
		ImmMemBytes:               immBytes,
		SSTablesPerLevel:          sstPerLevel,
		TotalSSTables:             totalSSTables,
		TotalDiskBytes:            totalDisk,
		TotalPuts:                 atomic.LoadInt64(&e.puts),
		TotalDeletes:              atomic.LoadInt64(&e.deletes),
		TotalGets:                 atomic.LoadInt64(&e.gets),
		BloomFilterHits:           atomic.LoadInt64(&e.bloomHits),
		BloomFilterMiss:           atomic.LoadInt64(&e.bloomMisses),
		TotalFlushes:              atomic.LoadInt64(&e.flushes),
		CompactionStats:           compStats,
		LogicalBytesWritten:       logicalWritten,
		PhysicalBytesWAL:          walBytes,
		PhysicalBytesFlush:        flushBytes,
		PhysicalBytesCompaction:   compactBytes,
		TotalPhysicalBytesWritten: totalPhysical,
		EngineWAF:                 waf,
		EngineSAF:                 saf,
		ActiveDataBytes:           activeData,
	}
}

// Close gracefully closes the engine, flushes pending memtables, stops background workers, and closes files.
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	close(e.closeChan)
	e.mu.Unlock()

	// Wait for background workers
	e.wg.Wait()

	// Flush active WAL
	if e.activeWAL != nil {
		_ = e.activeWAL.Sync()
		_ = e.activeWAL.Close()
	}

	// Close all SSTable readers
	e.readersMu.Lock()
	for _, r := range e.readers {
		_ = r.Close()
	}
	e.readers = nil
	e.readersMu.Unlock()

	// Close manifest
	if e.manifest != nil {
		_ = e.manifest.Close()
	}

	return nil
}
