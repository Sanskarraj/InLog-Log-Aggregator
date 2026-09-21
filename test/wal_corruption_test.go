package test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/engine/lsm-trees/pkg/core"
	"github.com/engine/lsm-trees/pkg/wal"
)

// TestWAL_TruncatedTail tests recovery when a crash or power cut occurs mid-write at EOF.
// Both truncated frame header (< 8 bytes) and truncated payload (< payloadLen) must be cleanly
// truncated to the last valid record, allowing all committed records to be recovered.
func TestWAL_TruncatedTail(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_trunc_tail_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.Open(1, wal.Options{
		Dir:        tempDir,
		SyncPolicy: wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	totalCommitted := 250
	for i := 0; i < totalCommitted; i++ {
		entry := core.NewPutEntry(
			[]byte(fmt.Sprintf("key_%05d", i)),
			[]byte(fmt.Sprintf("val_payload_committed_%05d", i)),
		)
		if err := w.Write(entry); err != nil {
			t.Fatalf("failed writing entry %d: %v", i, err)
		}
	}
	_ = w.Close()

	walPath := filepath.Join(tempDir, "wal_000001.log")

	// Subtest A: Incomplete 4-byte header at EOF
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.Write([]byte{0xDE, 0xAD, 0xBE, 0xEF}) // partial 4-byte header (expected 8)
	_ = f.Close()

	recoveredKeys := make(map[string][]byte)
	report, err := wal.RecoverWithReport(walPath, func(e *core.Entry) error {
		recoveredKeys[string(e.Key)] = e.Value
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected recovery failure on truncated header: %v", err)
	}

	if report.ValidRecords != totalCommitted {
		t.Fatalf("expected %d valid records, got %d", totalCommitted, report.ValidRecords)
	}
	if !report.CorruptionDetected || report.CorruptionType != "truncated_tail" {
		t.Fatalf("expected truncated_tail corruption detected, got type=%s detected=%v",
			report.CorruptionType, report.CorruptionDetected)
	}

	// Subtest B: Truncated payload at EOF
	// Write a valid 8-byte header claiming 100 bytes, but write only 20 bytes
	f, err = os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var fakeHeader [8]byte
	binary.BigEndian.PutUint32(fakeHeader[0:4], 0x12345678) // fake CRC
	binary.BigEndian.PutUint32(fakeHeader[4:8], 100)        // claims 100-byte payload
	_, _ = f.Write(fakeHeader[:])
	_, _ = f.Write(bytes.Repeat([]byte("A"), 20)) // only write 20 bytes before power cut
	_ = f.Close()

	report, err = wal.RecoverWithReport(walPath, func(e *core.Entry) error {
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected recovery failure on truncated payload: %v", err)
	}
	if report.ValidRecords != totalCommitted {
		t.Fatalf("expected %d valid records, got %d", totalCommitted, report.ValidRecords)
	}
	if !report.FileTruncated {
		t.Fatalf("expected file to be truncated cleanly to last valid record")
	}
}

// TestWAL_BadCRC tests bit-flip detection via CRC32 checksum verification.
func TestWAL_BadCRC(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_bad_crc_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.Open(1, wal.Options{
		Dir:        tempDir,
		SyncPolicy: wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 50; i++ {
		entry := core.NewPutEntry(
			[]byte(fmt.Sprintf("crc_key_%03d", i)),
			[]byte(fmt.Sprintf("crc_val_payload_%03d", i)),
		)
		if err := w.Write(entry); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()

	walPath := filepath.Join(tempDir, "wal_000001.log")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt payload of last record by flipping bytes in payload
	lastRecordPayloadOffset := len(data) - 10
	data[lastRecordPayloadOffset] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	recovered := 0
	report, err := wal.RecoverWithReport(walPath, func(e *core.Entry) error {
		recovered++
		return nil
	})
	if err != nil {
		t.Fatalf("recovery error: %v", err)
	}

	if !report.CorruptionDetected || report.CorruptionType != "bad_crc_tail" {
		t.Fatalf("expected bad_crc_tail, got %s", report.CorruptionType)
	}
	if recovered != 49 {
		t.Fatalf("expected 49 records recovered (all prior to corrupted one), got %d", recovered)
	}
}

// TestWAL_BadLength tests protection against massive or invalid length headers.
// If length is corrupted to 4GB (0xFFFFFFFF) or exceeds MaxRecordPayload,
// the engine must reject it immediately without attempting a fatal OOM heap allocation.
func TestWAL_BadLength(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_bad_len_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.Open(1, wal.Options{
		Dir:        tempDir,
		SyncPolicy: wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 30; i++ {
		entry := core.NewPutEntry(
			[]byte(fmt.Sprintf("len_key_%03d", i)),
			[]byte("sample_payload"),
		)
		if err := w.Write(entry); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()

	walPath := filepath.Join(tempDir, "wal_000001.log")

	// Inject 8-byte frame header with 4GB payload length
	f, err := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}
	var oomHeader [8]byte
	binary.BigEndian.PutUint32(oomHeader[0:4], 0xCAFEBABE)
	binary.BigEndian.PutUint32(oomHeader[4:8], 0xFFFFFFFF) // 4,294,967,295 bytes (4GB)
	_, _ = f.Write(oomHeader[:])
	_ = f.Close()

	recovered := 0
	report, err := wal.RecoverWithReport(walPath, func(e *core.Entry) error {
		recovered++
		return nil
	})
	if err != nil {
		t.Fatalf("recovery failed with error: %v", err)
	}

	if recovered != 30 {
		t.Fatalf("expected 30 valid records recovered, got %d", recovered)
	}
	if !report.CorruptionDetected || report.CorruptionType != "bad_length" {
		t.Fatalf("expected bad_length detection, got %s", report.CorruptionType)
	}
}

// TestWAL_CorruptMiddle tests detection when corruption occurs in the middle of the WAL file
// (e.g. valid records 0..49, corrupted record 50, followed by valid records 51..99).
// The engine must safely recover prefix records 0..49 and diagnose mid-file corruption.
func TestWAL_CorruptMiddle(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "wal_corrupt_mid_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.Open(1, wal.Options{
		Dir:        tempDir,
		SyncPolicy: wal.SyncAlways,
	})
	if err != nil {
		t.Fatal(err)
	}

	var entryOffsets []int64
	var totalBytes int64

	for i := 0; i < 100; i++ {
		entryOffsets = append(entryOffsets, totalBytes)
		entry := core.NewPutEntry(
			[]byte(fmt.Sprintf("mid_key_%04d", i)),
			[]byte(fmt.Sprintf("mid_val_data_payload_%04d", i)),
		)
		enc := entry.EncodeFrameTo(nil)
		totalBytes += int64(len(enc))
		if err := w.Write(entry); err != nil {
			t.Fatal(err)
		}
	}
	_ = w.Close()

	walPath := filepath.Join(tempDir, "wal_000001.log")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt record 50 (which is right in the middle, offsets[50])
	corruptOffset := entryOffsets[50] + 12 // corrupt payload byte of record 50
	data[corruptOffset] ^= 0xEE

	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	recovered := 0
	report, err := wal.RecoverWithReport(walPath, func(e *core.Entry) error {
		recovered++
		return nil
	})
	if err != nil {
		t.Fatalf("unexpected recovery error: %v", err)
	}

	if recovered != 50 {
		t.Fatalf("expected exactly 50 records recovered prior to mid-file corruption, got %d", recovered)
	}
	if !report.CorruptionDetected || report.CorruptionType != "corrupt_middle" {
		t.Fatalf("expected corrupt_middle, got %s", report.CorruptionType)
	}
	if report.CorruptionOffset != entryOffsets[50] {
		t.Fatalf("expected corruption offset %d, got %d", entryOffsets[50], report.CorruptionOffset)
	}
}
