package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/engine/lsm-trees/pkg/wal"
)

func TestEngineLifecycleAndOperations(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "lsm_engine_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := DefaultOptions(tempDir)
	opts.MemTableSize = 10 * 1024 // 10KB to trigger frequent flushes
	opts.SyncPolicy = wal.SyncAlways
	opts.AutoCompaction = false // control manually for testing

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("failed opening engine: %v", err)
	}

	// 1. Insert 300 keys (enough to force at least one flush)
	for i := 0; i < 300; i++ {
		k := []byte(fmt.Sprintf("user_%04d", i))
		v := []byte(fmt.Sprintf("profile_data_%04d", i))
		if err := db.Put(k, v); err != nil {
			t.Fatalf("put failed at %d: %v", i, err)
		}
	}

	// 2. Read back before explicit flush
	val, err := db.Get([]byte("user_0042"))
	if err != nil {
		t.Fatalf("get user_0042 failed: %v", err)
	}
	if !bytes.Equal(val, []byte("profile_data_0042")) {
		t.Fatalf("val mismatch: %s", val)
	}

	// 3. Force Flush
	if err := db.Flush(); err != nil {
		t.Fatalf("flush failed: %v", err)
	}

	// 4. Verify reads after flush from SSTable
	for i := 0; i < 300; i++ {
		k := []byte(fmt.Sprintf("user_%04d", i))
		expected := []byte(fmt.Sprintf("profile_data_%04d", i))
		v, err := db.Get(k)
		if err != nil {
			t.Fatalf("failed reading %s from sstable: %v", k, err)
		}
		if !bytes.Equal(v, expected) {
			t.Fatalf("mismatch at %s: got %s, expected %s", k, v, expected)
		}
	}

	// 5. Delete operation (tombstone)
	if err := db.Delete([]byte("user_0042")); err != nil {
		t.Fatalf("delete failed: %v", err)
	}

	// Key should now be reported not found
	_, err = db.Get([]byte("user_0042"))
	if !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected ErrKeyNotFound, got: %v", err)
	}

	// 6. Range scan: user_0100 to user_0110
	iter, err := db.Scan([]byte("user_0100"), []byte("user_0110"))
	if err != nil {
		t.Fatalf("scan failed: %v", err)
	}
	defer iter.Close()

	var scannedKeys []string
	for iter.Valid() {
		k := string(iter.Key())
		if k > "user_0110" {
			break
		}
		scannedKeys = append(scannedKeys, k)
		iter.Next()
	}

	if len(scannedKeys) != 11 { // 0100 through 0110 inclusive
		t.Fatalf("expected 11 scanned keys, got %d: %v", len(scannedKeys), scannedKeys)
	}

	// 7. Verify stats
	stats := db.Stats()
	if stats.TotalPuts < 300 {
		t.Fatalf("stats total puts mismatch: %d", stats.TotalPuts)
	}
	if stats.TotalFlushes == 0 {
		t.Fatalf("expected at least 1 flush in stats")
	}

	// 8. Close engine
	if err := db.Close(); err != nil {
		t.Fatalf("failed closing engine: %v", err)
	}
}

func TestEngineCompaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "lsm_compaction_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	opts := DefaultOptions(tempDir)
	opts.MemTableSize = 2 * 1024 // 2KB small memtable
	opts.L0CompactionTrigger = 3
	opts.AutoCompaction = false

	db, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}

	// Insert 4 batches, flushing each to create multiple L0 tables
	for b := 0; b < 4; b++ {
		for i := 0; i < 50; i++ {
			k := []byte(fmt.Sprintf("k_%02d_%04d", b, i))
			v := []byte(fmt.Sprintf("v_%02d_%04d", b, i))
			_ = db.Put(k, v)
		}
		_ = db.Flush()
		time.Sleep(10 * time.Millisecond)
	}

	statsBefore := db.Stats()
	if statsBefore.SSTablesPerLevel[0] < 3 {
		t.Fatalf("expected at least 3 L0 tables, got %d", statsBefore.SSTablesPerLevel[0])
	}

	// Trigger compaction
	if err := db.Compact(); err != nil {
		t.Fatalf("compaction failed: %v", err)
	}

	statsAfter := db.Stats()
	t.Logf("L0 tables before: %d, after: %d (Level 1: %d)",
		statsBefore.SSTablesPerLevel[0],
		statsAfter.SSTablesPerLevel[0],
		statsAfter.SSTablesPerLevel[1],
	)

	// Verify all data is still intact and readable
	for b := 0; b < 4; b++ {
		for i := 0; i < 50; i++ {
			k := []byte(fmt.Sprintf("k_%02d_%04d", b, i))
			v, err := db.Get(k)
			if err != nil || v == nil {
				t.Fatalf("missing key after compaction: %s (%v)", k, err)
			}
		}
	}

	_ = db.Close()
}
