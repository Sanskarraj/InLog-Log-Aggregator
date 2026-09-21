package memtable

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"github.com/engine/lsm-trees/pkg/core"
)

func TestSkipListSequential(t *testing.T) {
	sl := NewSkipList()

	// Insert 1000 items
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key_%05d", i))
		v := []byte(fmt.Sprintf("val_%05d", i))
		sl.Put(core.NewPutEntry(k, v))
	}

	if sl.Length() != 1000 {
		t.Fatalf("expected length 1000, got %d", sl.Length())
	}

	// Verify lookups
	for i := 0; i < 1000; i++ {
		k := []byte(fmt.Sprintf("key_%05d", i))
		entry, found := sl.Get(k)
		if !found {
			t.Fatalf("missing key: %s", k)
		}
		expectedVal := []byte(fmt.Sprintf("val_%05d", i))
		if !bytes.Equal(entry.Value, expectedVal) {
			t.Fatalf("value mismatch: expected %s, got %s", expectedVal, entry.Value)
		}
	}

	// Non-existent key
	if _, found := sl.Get([]byte("non_existent")); found {
		t.Fatalf("expected not found for non_existent key")
	}
}

func TestSkipListConcurrent(t *testing.T) {
	sl := NewSkipList()
	workers := 8
	itemsPerWorker := 500

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				k := []byte(fmt.Sprintf("worker_%02d_key_%04d", workerID, i))
				v := []byte(fmt.Sprintf("worker_%02d_val_%04d", workerID, i))
				sl.Put(core.NewPutEntry(k, v))
			}
		}(w)
	}
	wg.Wait()

	expectedTotal := int64(workers * itemsPerWorker)
	if sl.Length() != expectedTotal {
		t.Fatalf("expected length %d, got %d", expectedTotal, sl.Length())
	}

	// Concurrently read
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < itemsPerWorker; i++ {
				k := []byte(fmt.Sprintf("worker_%02d_key_%04d", workerID, i))
				entry, found := sl.Get(k)
				if !found || entry == nil {
					t.Errorf("worker %d missing key %s", workerID, k)
				}
			}
		}(w)
	}
	wg.Wait()
}

func TestSkipListIterator(t *testing.T) {
	sl := NewSkipList()
	keys := []string{"apple", "banana", "cherry", "date", "elderberry", "fig"}
	for _, k := range keys {
		sl.Put(core.NewPutEntry([]byte(k), []byte("v_"+k)))
	}

	it := sl.NewIterator()
	defer it.Close()

	var collected []string
	for it.Valid() {
		collected = append(collected, string(it.Key()))
		it.Next()
	}

	if len(collected) != len(keys) {
		t.Fatalf("expected %d keys, got %d", len(keys), len(collected))
	}
	for i, k := range keys {
		if collected[i] != k {
			t.Fatalf("order mismatch at %d: expected %s, got %s", i, k, collected[i])
		}
	}

	// Test Seek
	it.Seek([]byte("cherry"))
	if !it.Valid() || string(it.Key()) != "cherry" {
		t.Fatalf("seek failed: expected cherry, got %s", string(it.Key()))
	}

	it.Seek([]byte("czzz"))
	if !it.Valid() || string(it.Key()) != "date" {
		t.Fatalf("seek failed: expected date, got %s", string(it.Key()))
	}
}
