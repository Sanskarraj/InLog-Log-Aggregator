package wal

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/engine/lsm-trees/pkg/core"
)

func TestWALWriteAndRecover(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := Open(1, Options{
		Dir:        tempDir,
		SyncPolicy: SyncAlways,
	})
	if err != nil {
		t.Fatalf("failed to open wal: %v", err)
	}

	numRecords := 500
	for i := 0; i < numRecords; i++ {
		k := []byte(fmt.Sprintf("key_%05d", i))
		v := []byte(fmt.Sprintf("value_%05d", i))
		if err := w.Write(core.NewPutEntry(k, v)); err != nil {
			t.Fatalf("failed write at %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close wal: %v", err)
	}

	// Recover
	recoveredCount := 0
	walPath := filepath.Join(tempDir, "wal_000001.log")
	count, err := Recover(walPath, func(e *core.Entry) error {
		expectedKey := []byte(fmt.Sprintf("key_%05d", recoveredCount))
		if !bytes.Equal(e.Key, expectedKey) {
			return fmt.Errorf("key mismatch at %d: expected %s, got %s", recoveredCount, expectedKey, e.Key)
		}
		recoveredCount++
		return nil
	})

	if err != nil {
		t.Fatalf("recovery error: %v", err)
	}
	if count != numRecords || recoveredCount != numRecords {
		t.Fatalf("expected %d records, got %d (callback %d)", numRecords, count, recoveredCount)
	}
}

func TestWALCorruptTailRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_corrupt_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := Open(2, Options{
		Dir:        tempDir,
		SyncPolicy: SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		_ = w.Write(core.NewPutEntry([]byte(fmt.Sprintf("k%d", i)), []byte(fmt.Sprintf("v%d", i))))
	}
	_ = w.Close()

	walPath := filepath.Join(tempDir, "wal_000002.log")

	// Append corrupt garbage bytes to simulate sudden crash during write
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF, 0x01, 0x02}) // incomplete corrupted header
	_ = f.Close()

	// Recovery should succeed on all 50 valid records and truncate the corrupt tail
	count, err := Recover(walPath, func(e *core.Entry) error {
		return nil
	})
	if err != nil {
		t.Fatalf("recovery failed on corrupt tail: %v", err)
	}
	if count != 50 {
		t.Fatalf("expected 50 recovered records, got %d", count)
	}
}
