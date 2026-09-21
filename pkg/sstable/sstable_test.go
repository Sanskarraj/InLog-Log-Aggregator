package sstable

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/engine/lsm-trees/pkg/core"
)

func TestSSTableBuildAndRead(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "sstable_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	builder, err := NewBuilder(BuilderOptions{
		Dir:       tempDir,
		ID:        1,
		Level:     0,
		BlockSize: 256, // small block size to force multiple blocks
		BloomFp:   0.01,
		EstKeys:   200,
	})
	if err != nil {
		t.Fatalf("failed to create builder: %v", err)
	}

	numEntries := 200
	for i := 0; i < numEntries; i++ {
		k := []byte(fmt.Sprintf("key_%04d", i))
		v := []byte(fmt.Sprintf("val_%04d", i))
		if err := builder.Add(core.NewPutEntry(k, v)); err != nil {
			t.Fatalf("failed adding entry: %v", err)
		}
	}

	meta, err := builder.Finish()
	if err != nil {
		t.Fatalf("failed finishing sstable: %v", err)
	}

	if meta.EntryCount != int64(numEntries) {
		t.Fatalf("expected entry count %d, got %d", numEntries, meta.EntryCount)
	}

	// Open reader
	reader, err := OpenReader(tempDir, 1)
	if err != nil {
		t.Fatalf("failed opening reader: %v", err)
	}
	defer reader.Close()

	// Verify all keys can be read correctly
	for i := 0; i < numEntries; i++ {
		k := []byte(fmt.Sprintf("key_%04d", i))
		expectedVal := []byte(fmt.Sprintf("val_%04d", i))

		entry, found, err := reader.Get(k)
		if err != nil {
			t.Fatalf("lookup error for %s: %v", k, err)
		}
		if !found {
			t.Fatalf("key %s not found in sstable", k)
		}
		if !bytes.Equal(entry.Value, expectedVal) {
			t.Fatalf("val mismatch for %s: got %s, expected %s", k, entry.Value, expectedVal)
		}
	}

	// Verify non-existent keys (out of range and in range)
	_, found, err := reader.Get([]byte("key_9999"))
	if err != nil || found {
		t.Fatalf("expected not found for key_9999")
	}
	_, found, err = reader.Get([]byte("aaa_key"))
	if err != nil || found {
		t.Fatalf("expected not found for aaa_key")
	}

	// Test sequential iterator
	it, err := reader.NewIterator()
	if err != nil {
		t.Fatalf("failed creating iterator: %v", err)
	}
	defer it.Close()

	iterCount := 0
	for it.Valid() {
		expectedKey := fmt.Sprintf("key_%04d", iterCount)
		if string(it.Key()) != expectedKey {
			t.Fatalf("iterator sequence error at %d: expected %s, got %s", iterCount, expectedKey, string(it.Key()))
		}
		iterCount++
		it.Next()
	}

	if iterCount != numEntries {
		t.Fatalf("iterator visited %d entries, expected %d", iterCount, numEntries)
	}

	// Test Seek
	it.Seek([]byte("key_0150"))
	if !it.Valid() || string(it.Key()) != "key_0150" {
		t.Fatalf("seek failed: expected key_0150, got %s", string(it.Key()))
	}
}
