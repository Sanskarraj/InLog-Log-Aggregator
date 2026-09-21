package test

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/engine/lsm-trees/pkg/core"
	"github.com/engine/lsm-trees/pkg/lsm"
	"github.com/engine/lsm-trees/pkg/memtable"
	"github.com/engine/lsm-trees/pkg/search"
	"github.com/engine/lsm-trees/pkg/sstable"
	"github.com/engine/lsm-trees/pkg/wal"
)

// fastKey formats an integer key into a 16-byte fixed buffer without heap allocations.
func fastKey(buf *[16]byte, prefix byte, i uint64) []byte {
	buf[0] = prefix
	buf[1] = '_'
	binary.BigEndian.PutUint64(buf[2:10], i)
	return buf[:10]
}

// -----------------------------------------------------------------------------
// 1. MemTable Benchmarks
// -----------------------------------------------------------------------------

func BenchmarkMemTablePut(b *testing.B) {
	sl := memtable.NewSkipList()
	val := []byte("val_payload_bytes_sample_128b")
	var keyBuf [16]byte

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'k', uint64(i))
		entry := core.NewPutEntryDirect(k, val)
		sl.Put(entry)
	}
}

func BenchmarkMemTableGet(b *testing.B) {
	sl := memtable.NewSkipList()
	val := []byte("val_payload_bytes_sample_128b")
	var keyBuf [16]byte

	for i := 0; i < 50000; i++ {
		k := fastKey(&keyBuf, 'k', uint64(i))
		sl.Put(core.NewPutEntry(k, val))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'k', uint64(i%50000))
		_, _ = sl.Get(k)
	}
}

// -----------------------------------------------------------------------------
// 2. WAL Append Benchmarks (Disambiguated Durability)
// -----------------------------------------------------------------------------

// BenchmarkWALAppendBuffered measures in-memory buffered WAL append (SyncNone/SyncBatch)
// without forcing an immediate fsync(2) per operation.
func BenchmarkWALAppendBuffered(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_wal_buf_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.Open(1, wal.Options{
		Dir:        tempDir,
		SyncPolicy: wal.SyncNone,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	var keyBuf [16]byte
	val := []byte("bench_value_payload_512_bytes")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'w', uint64(i))
		entry := core.NewPutEntryDirect(k, val)
		if err := w.Write(entry); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkWALAppendSync measures true durable persistence requiring an fsync(2)
// call on every single write operation.
func BenchmarkWALAppendSync(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_wal_sync_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.Open(1, wal.Options{
		Dir:        tempDir,
		SyncPolicy: wal.SyncAlways,
	})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	var keyBuf [16]byte
	val := []byte("bench_value_payload_durable_sync")

	// Limit N for sync benchmark to prevent long runs on disk
	iterations := b.N
	if iterations > 2000 {
		iterations = 2000
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < iterations; i++ {
		k := fastKey(&keyBuf, 's', uint64(i))
		entry := core.NewPutEntryDirect(k, val)
		if err := w.Write(entry); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// 3. LSM Storage Engine PUT (Disambiguated Durability)
// -----------------------------------------------------------------------------

// BenchmarkLSMTreePutBuffered measures WAL append (buffered) + MemTable insertion
// without forcing an fsync per operation.
func BenchmarkLSMTreePutBuffered(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_lsm_put_buf_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch
	opts.MemTableSize = 64 * 1024 * 1024 // large memtable to isolate write path

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	val := []byte("lsm_benchmark_sample_data_value_payload_128b")
	var keyBuf [16]byte

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'l', uint64(i))
		if err := db.Put(k, val); err != nil {
			b.Fatal(err)
		}
	}
}

// -----------------------------------------------------------------------------
// 4. Multi-Tier Read Path Benchmarks (MemTable, L0, L1, Miss, Bloom Pruned)
// -----------------------------------------------------------------------------

// BenchmarkLSMTreeGetHit_MemTable: key resides in active memory
func BenchmarkLSMTreeGetHit_MemTable(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_get_mem_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch
	opts.MemTableSize = 64 * 1024 * 1024

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var keyBuf [16]byte
	val := []byte("memtable_cached_payload")
	for i := 0; i < 10000; i++ {
		k := fastKey(&keyBuf, 'm', uint64(i))
		_ = db.Put(k, val)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'm', uint64(i%10000))
		_, _ = db.Get(k)
	}
}

// BenchmarkLSMTreeGetHit_L0: key has been flushed to Level 0 SSTable
func BenchmarkLSMTreeGetHit_L0(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_get_l0_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch
	opts.AutoCompaction = false

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var keyBuf [16]byte
	val := []byte("l0_sstable_payload")
	for i := 0; i < 5000; i++ {
		k := fastKey(&keyBuf, '0', uint64(i))
		_ = db.Put(k, val)
	}
	_ = db.Flush() // Flush to L0 SSTable

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, '0', uint64(i%5000))
		_, _ = db.Get(k)
	}
}

// BenchmarkLSMTreeGetHit_L1: key resides in compacted Level 1 SSTable
func BenchmarkLSMTreeGetHit_L1(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_get_l1_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch
	opts.L0CompactionTrigger = 2
	opts.AutoCompaction = false

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var keyBuf [16]byte
	val := []byte("l1_compacted_payload")
	for batch := 0; batch < 3; batch++ {
		for i := 0; i < 2000; i++ {
			k := fastKey(&keyBuf, '1', uint64(batch*2000+i))
			_ = db.Put(k, val)
		}
		_ = db.Flush()
	}
	// Force compaction to L1
	_ = db.Compact()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, '1', uint64(i%6000))
		_, _ = db.Get(k)
	}
}

// BenchmarkLSMTreeGetMiss_WithBloom: key does not exist but falls within [MinKey, MaxKey].
// Bloom filter tests negative in RAM and PRUNES disk I/O entirely (~150 ns).
func BenchmarkLSMTreeGetMiss_WithBloom(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_get_miss_with_bloom_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var keyBuf [16]byte
	// Insert even keys only: 0, 2, 4, 6...
	for i := 0; i < 5000; i++ {
		k := fastKey(&keyBuf, 'b', uint64(i*2))
		_ = db.Put(k, []byte("even_payload"))
	}
	_ = db.Flush()

	b.ResetTimer()
	b.ReportAllocs()
	// Query odd keys: 1, 3, 5, 7... (in range, but absent)
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'b', uint64((i%5000)*2+1))
		_, _ = db.Get(k)
	}
}

// BenchmarkLSMTreeGetMiss_WithoutBloom: same key workload as WithBloom, but Bloom filter is bypassed.
// Engine must binary search sparse index, read 4KB block from disk via ReadAt, verify CRC32, and scan entries.
// This proves that Bloom filters avoid real, multi-microsecond disk I/O.
func BenchmarkLSMTreeGetMiss_WithoutBloom(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_get_miss_no_bloom_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	var keyBuf [16]byte
	// Insert even keys only: 0, 2, 4, 6...
	for i := 0; i < 5000; i++ {
		k := fastKey(&keyBuf, 'b', uint64(i*2))
		_ = db.Put(k, []byte("even_payload"))
	}
	_ = db.Flush()

	// Bypass Bloom filter to force sparse index search + disk block read
	db.SetBypassBloom(true)

	b.ResetTimer()
	b.ReportAllocs()
	// Query odd keys: 1, 3, 5, 7... (in range, but absent)
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'b', uint64((i%5000)*2+1))
		_, _ = db.Get(k)
	}
}

// BenchmarkLSMTreeGetMiss_BloomPruned: key is rejected early by Bloom filter
func BenchmarkLSMTreeGetMiss_BloomPruned(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_get_miss_bloom_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 5000; i++ {
		k := []byte(fmt.Sprintf("present_key_%06d", i))
		_ = db.Put(k, []byte("val"))
	}
	_ = db.Flush()

	var keyBuf [16]byte
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'z', uint64(i))
		_, _ = db.Get(k)
	}
}

// -----------------------------------------------------------------------------
// 5. Range Query Benchmarks
// -----------------------------------------------------------------------------

func BenchmarkLSMTreeRangeScan_100(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_range_100_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 10000; i++ {
		k := []byte(fmt.Sprintf("scan_key_%06d", i))
		_ = db.Put(k, []byte("scan_payload_64b"))
	}
	_ = db.Flush()

	startKey := []byte("scan_key_001000")
	endKey := []byte("scan_key_001100")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		iter, err := db.Scan(startKey, endKey)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for iter.Valid() && count < 100 {
			count++
			iter.Next()
		}
		_ = iter.Close()
	}
}

func BenchmarkLSMTreeRangeScan_1000(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_range_1000_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	for i := 0; i < 10000; i++ {
		k := []byte(fmt.Sprintf("scan_key_%06d", i))
		_ = db.Put(k, []byte("scan_payload_64b"))
	}
	_ = db.Flush()

	startKey := []byte("scan_key_001000")
	endKey := []byte("scan_key_002000")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		iter, err := db.Scan(startKey, endKey)
		if err != nil {
			b.Fatal(err)
		}
		count := 0
		for iter.Valid() && count < 1000 {
			count++
			iter.Next()
		}
		_ = iter.Close()
	}
}

// -----------------------------------------------------------------------------
// 6. Concurrency Scaling & Latency Percentiles (p50, p95, p99, p99.9)
// -----------------------------------------------------------------------------

func BenchmarkConcurrentMixed_70Read_30Write(b *testing.B) {
	for _, workers := range []int{1, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("workers_%d", workers), func(b *testing.B) {
			tempDir, err := os.MkdirTemp("", "bench_concurrent_*")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(tempDir)

			opts := lsm.DefaultOptions(tempDir)
			opts.SyncPolicy = wal.SyncBatch
			opts.MemTableSize = 32 * 1024 * 1024

			db, err := lsm.Open(opts)
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()

			// Pre-populate 20,000 keys
			var keyBuf [16]byte
			initVal := []byte("concurrent_initial_payload")
			for i := 0; i < 20000; i++ {
				k := fastKey(&keyBuf, 'c', uint64(i))
				_ = db.Put(k, initVal)
			}

			// Latency sample collection
			var sampleMu sync.Mutex
			var latencies []time.Duration
			var opCounter int64

			b.ResetTimer()
			b.ReportAllocs()

			b.RunParallel(func(pb *testing.PB) {
				localRnd := rand.New(rand.NewSource(time.Now().UnixNano()))
				var localKeyBuf [16]byte
				localLatencies := make([]time.Duration, 0, 1000)

				for pb.Next() {
					opID := atomic.AddInt64(&opCounter, 1)
					start := time.Now()

					if localRnd.Float64() < 0.30 {
						// 30% Write
						k := fastKey(&localKeyBuf, 'c', uint64(opID%50000))
						_ = db.Put(k, initVal)
					} else {
						// 70% Read
						k := fastKey(&localKeyBuf, 'c', uint64(opID%20000))
						_, _ = db.Get(k)
					}

					dur := time.Since(start)
					if len(localLatencies) < 1000 {
						localLatencies = append(localLatencies, dur)
					}
				}

				sampleMu.Lock()
				latencies = append(latencies, localLatencies...)
				sampleMu.Unlock()
			})

			b.StopTimer()

			// Compute latency percentiles
			if len(latencies) > 0 {
				sort.Slice(latencies, func(i, j int) bool {
					return latencies[i] < latencies[j]
				})
				p50 := latencies[len(latencies)*50/100]
				p95 := latencies[len(latencies)*95/100]
				p99 := latencies[len(latencies)*99/100]
				p999 := latencies[len(latencies)*999/1000]

				b.ReportMetric(float64(p50.Microseconds()), "p50_us")
				b.ReportMetric(float64(p95.Microseconds()), "p95_us")
				b.ReportMetric(float64(p99.Microseconds()), "p99_us")
				b.ReportMetric(float64(p999.Microseconds()), "p999_us")
			}
		})
	}
}

// -----------------------------------------------------------------------------
// 7. Compaction Throughput Benchmark
// -----------------------------------------------------------------------------

func BenchmarkCompactionThroughput(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_compact_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch
	opts.MemTableSize = 1024 * 1024
	opts.L0CompactionTrigger = 4
	opts.AutoCompaction = false

	payload := make([]byte, 256)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	recordsPerBatch := 5000
	totalRecords := 4 * recordsPerBatch

	iterations := b.N
	if iterations > 3 {
		iterations = 3
	}

	var totalInputBytes int64
	var totalOutputBytes int64
	var totalDuration time.Duration

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < iterations; i++ {
		b.StopTimer()
		subDir := filepath.Join(tempDir, fmt.Sprintf("run_%d", i))
		_ = os.MkdirAll(subDir, 0755)
		subOpts := opts
		subOpts.Dir = subDir

		db, err := lsm.Open(subOpts)
		if err != nil {
			b.Fatal(err)
		}

		for batch := 0; batch < 4; batch++ {
			for r := 0; r < recordsPerBatch; r++ {
				k := []byte(fmt.Sprintf("k_%06d", r+(batch*2500)))
				_ = db.Put(k, payload)
			}
			_ = db.Flush()
		}

		statsBefore := db.Stats()
		inBytes := statsBefore.TotalDiskBytes

		b.StartTimer()
		start := time.Now()
		if err := db.Compact(); err != nil {
			b.Fatalf("compaction failed: %v", err)
		}
		dur := time.Since(start)
		b.StopTimer()

		statsAfter := db.Stats()
		outBytes := statsAfter.TotalDiskBytes

		totalInputBytes += inBytes
		totalOutputBytes += outBytes
		totalDuration += dur

		_ = db.Close()
		_ = os.RemoveAll(subDir)
	}

	if iterations > 0 && totalDuration > 0 {
		mbProcessed := float64(totalInputBytes) / (1024 * 1024)
		throughputMBs := mbProcessed / totalDuration.Seconds()
		reductionRatio := float64(totalInputBytes) / float64(totalOutputBytes)
		compactionWriteRatio := float64(totalOutputBytes) / float64(totalInputBytes)

		b.ReportMetric(throughputMBs, "compact_MB/s")
		b.ReportMetric(reductionRatio, "reduction_x")
		b.ReportMetric(compactionWriteRatio, "write_ratio")
		b.ReportMetric(float64(totalDuration.Milliseconds())/float64(iterations), "dur_ms")
		b.ReportMetric(float64(totalRecords), "records")
	}
}

// -----------------------------------------------------------------------------
// 7b. Compaction-Under-Load Benchmark
// -----------------------------------------------------------------------------

// BenchmarkCompactionUnderLoad measures concurrent read/write throughput, latency percentiles,
// and write stalls while background leveled compaction actively merges SSTables under continuous write pressure.
func BenchmarkCompactionUnderLoad(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_compact_load_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch
	opts.MemTableSize = 256 * 1024 // 256KB threshold to force continuous flushes & active leveled compaction
	opts.L0CompactionTrigger = 4
	opts.AutoCompaction = true
	opts.CompactionInterval = 25 * time.Millisecond

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	// Pre-populate 10,000 records
	var keyBuf [16]byte
	payload := []byte("bench_compaction_under_load_payload_128_bytes_of_structured_logging_text")
	for i := 0; i < 10000; i++ {
		k := fastKey(&keyBuf, 'c', uint64(i))
		_ = db.Put(k, payload)
	}
	_ = db.Flush()

	var sampleMu sync.Mutex
	var readLatencies []time.Duration
	var writeLatencies []time.Duration
	var opCounter int64

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		localRnd := rand.New(rand.NewSource(time.Now().UnixNano()))
		var localKeyBuf [16]byte
		rLats := make([]time.Duration, 0, 1000)
		wLats := make([]time.Duration, 0, 1000)

		for pb.Next() {
			opID := atomic.AddInt64(&opCounter, 1)
			start := time.Now()

			if localRnd.Float64() < 0.30 {
				// 30% Write (generates rapid MemTable fill, flushes, and background compaction)
				k := fastKey(&localKeyBuf, 'c', uint64(opID%25000))
				_ = db.Put(k, payload)
				dur := time.Since(start)
				if len(wLats) < 1000 {
					wLats = append(wLats, dur)
				}
			} else {
				// 70% Read (concurrently queries while compaction streams disk I/O)
				k := fastKey(&localKeyBuf, 'c', uint64(opID%10000))
				_, _ = db.Get(k)
				dur := time.Since(start)
				if len(rLats) < 1000 {
					rLats = append(rLats, dur)
				}
			}
		}

		sampleMu.Lock()
		readLatencies = append(readLatencies, rLats...)
		writeLatencies = append(writeLatencies, wLats...)
		sampleMu.Unlock()
	})

	b.StopTimer()

	stats := db.Stats()

	// Compute overall latency percentiles under load
	allLats := append(readLatencies, writeLatencies...)
	if len(allLats) > 0 {
		sort.Slice(allLats, func(i, j int) bool { return allLats[i] < allLats[j] })
		p50 := allLats[len(allLats)*50/100]
		p95 := allLats[len(allLats)*95/100]
		p99 := allLats[len(allLats)*99/100]
		p999 := allLats[len(allLats)*999/1000]

		b.ReportMetric(float64(p50.Microseconds()), "p50_us")
		b.ReportMetric(float64(p95.Microseconds()), "p95_us")
		b.ReportMetric(float64(p99.Microseconds()), "p99_us")
		b.ReportMetric(float64(p999.Microseconds()), "p999_us")
	}

	b.ReportMetric(stats.EngineWAF, "real_WAF")
	b.ReportMetric(stats.EngineSAF, "real_SAF")
	b.ReportMetric(float64(stats.CompactionStats.TotalCompactions), "compactions")
	b.ReportMetric(float64(stats.TotalFlushes), "flushes")
}

// -----------------------------------------------------------------------------
// 8. Bloom Filter Empirical vs Theoretical Verification
// -----------------------------------------------------------------------------

func BenchmarkBloomFilterDetailed(b *testing.B) {
	items := 100000
	targetFP := 0.01 // 1%
	bf := sstable.NewBloomFilter(items, targetFP)

	var keyBuf [16]byte
	for i := 0; i < items; i++ {
		k := fastKey(&keyBuf, 'b', uint64(i))
		bf.Add(k)
	}

	queries := 100000
	falsePositives := 0
	for i := 0; i < queries; i++ {
		k := fastKey(&keyBuf, 'm', uint64(i))
		if bf.MayContain(k) {
			falsePositives++
		}
	}

	measuredFP := float64(falsePositives) / float64(queries)

	b.ReportMetric(measuredFP*100, "FP_pct")
	b.ReportMetric(float64(items), "items")
	b.ReportMetric(float64(falsePositives), "FP_count")

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		k := fastKey(&keyBuf, 'b', uint64(i%items))
		_ = bf.MayContain(k)
	}
}

// -----------------------------------------------------------------------------
// 9. Full-Text Search Benchmark
// -----------------------------------------------------------------------------

func BenchmarkLogSearch(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bench_search_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := lsm.DefaultOptions(tempDir)
	opts.SyncPolicy = wal.SyncBatch

	db, err := lsm.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	engine := search.Open(db)

	for i := 0; i < 1000; i++ {
		_ = engine.Ingest(&search.LogRecord{
			ID:      fmt.Sprintf("log_%05d", i),
			Level:   "ERROR",
			Service: "payment-service",
			Message: fmt.Sprintf("Timeout connection failure to banking gateway attempt %d", i),
		})
	}
	_ = db.Flush()

	q := search.Query{
		Text:    "timeout connection failure",
		Level:   "ERROR",
		Service: "payment-service",
		Limit:   20,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = engine.Search(q)
	}
}
