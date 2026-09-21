package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/engine/lsm-trees/pkg/core"
)

var (
	ErrCorruptRecord     = errors.New("wal: corrupt record detected (checksum mismatch)")
	ErrTruncatedRecord   = errors.New("wal: truncated record at end of file")
	ErrBadRecordLength   = errors.New("wal: invalid record length header")
	ErrMidFileCorruption = errors.New("wal: mid-file corruption detected")
)

// MaxRecordPayload defines the maximum allowable single entry size in the WAL (64MB)
// to prevent malicious or corrupted length headers from triggering out-of-memory allocations.
const MaxRecordPayload = 64 * 1024 * 1024

// SyncPolicy defines how WAL writes are flushed and synced to disk.
type SyncPolicy int

const (
	SyncAlways SyncPolicy = iota // fsync after every single write
	SyncBatch                   // fsync periodically or when batch reaches threshold
	SyncNone                    // rely on OS page cache flushing
)

// Options holds WAL configuration.
type Options struct {
	Dir          string
	SyncPolicy   SyncPolicy
	SyncInterval time.Duration // used for SyncBatch
	BufferSize   int           // memory buffer before OS write (default 64KB)
}

// WAL represents an append-only Write-Ahead Log file.
type WAL struct {
	mu           sync.Mutex
	id           uint64
	dir          string
	file         *os.File
	writer       *bufio.Writer
	options      Options
	closed       bool
	stopSyncChan chan struct{}
	syncWg       sync.WaitGroup
	bytesWritten int64
	encodeBuf    []byte
}

// Open creates or opens a WAL file with the given sequence ID.
func Open(id uint64, opts Options) (*WAL, error) {
	if opts.BufferSize <= 0 {
		opts.BufferSize = 64 * 1024
	}
	if opts.SyncInterval <= 0 {
		opts.SyncInterval = 50 * time.Millisecond
	}

	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal directory: %w", err)
	}

	filePath := walFilePath(opts.Dir, id)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal file %s: %w", filePath, err)
	}

	fi, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}

	w := &WAL{
		id:           id,
		dir:          opts.Dir,
		file:         file,
		writer:       bufio.NewWriterSize(file, opts.BufferSize),
		options:      opts,
		bytesWritten: fi.Size(),
		encodeBuf:    make([]byte, 0, 1024),
	}

	if opts.SyncPolicy == SyncBatch {
		w.stopSyncChan = make(chan struct{})
		w.syncWg.Add(1)
		go w.periodicSyncer()
	}

	return w, nil
}

func walFilePath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("wal_%06d.log", id))
}

// ID returns the WAL sequence identifier.
func (w *WAL) ID() uint64 {
	return w.id
}

// Path returns the absolute path of the active WAL file.
func (w *WAL) Path() string {
	return walFilePath(w.dir, w.id)
}

// Write appends an entry to the WAL with CRC32 checksum verification.
// Binary format:
// [CRC32: 4B][PayloadLen: 4B][Payload: Timestamp(8B) | Type(1B) | KeyLen(2B) | ValLen(4B) | Key | Val]
func (w *WAL) Write(entry *core.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return core.ErrDatabaseClosed
	}

	w.encodeBuf = entry.EncodeFrameTo(w.encodeBuf[:0])

	if _, err := w.writer.Write(w.encodeBuf); err != nil {
		return fmt.Errorf("wal write frame error: %w", err)
	}

	w.bytesWritten += int64(len(w.encodeBuf))

	if w.options.SyncPolicy == SyncAlways {
		if err := w.writer.Flush(); err != nil {
			return err
		}
		return w.file.Sync()
	}

	return nil
}

// Sync flushes the buffer to disk and performs an fsync.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close gracefully syncs and closes the WAL.
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	if w.stopSyncChan != nil {
		close(w.stopSyncChan)
		w.syncWg.Wait()
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	var err error
	if w.writer != nil {
		if flushErr := w.writer.Flush(); flushErr != nil && err == nil {
			err = flushErr
		}
	}
	if w.file != nil {
		if syncErr := w.file.Sync(); syncErr != nil && err == nil {
			err = syncErr
		}
		if closeErr := w.file.Close(); closeErr != nil && err == nil {
			err = closeErr
		}
	}
	return err
}

func (w *WAL) periodicSyncer() {
	defer w.syncWg.Done()
	ticker := time.NewTicker(w.options.SyncInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			_ = w.Sync()
		case <-w.stopSyncChan:
			return
		}
	}
}

// RecoveryReport provides detailed telemetry and diagnostics on WAL replay.
type RecoveryReport struct {
	ValidRecords       int    `json:"valid_records"`
	BytesRecovered     int64  `json:"bytes_recovered"`
	CorruptionDetected bool   `json:"corruption_detected"`
	CorruptionType     string `json:"corruption_type,omitempty"` // "truncated_tail", "bad_crc_tail", "corrupt_middle", "bad_length"
	CorruptionOffset   int64  `json:"corruption_offset,omitempty"`
	FileTruncated      bool   `json:"file_truncated"`
}

// RecoverWithReport reads a WAL log file, validates record frames, and replays valid entries.
// It detects and categorizes corruption (truncated tail, bad CRC, bad length, mid-file corruption).
// Tail corruptions from abrupt process death are safely truncated up to the last valid frame.
func RecoverWithReport(filePath string, onEntry func(e *core.Entry) error) (RecoveryReport, error) {
	report := RecoveryReport{}

	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return report, err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return report, err
	}
	fileSize := fi.Size()

	reader := bufio.NewReader(file)
	validCount := 0
	var lastValidOffset int64 = 0

	var header [8]byte
	for {
		currentOffset := lastValidOffset
		n, err := io.ReadFull(reader, header[:])
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // Clean end of file
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				// Truncated header at EOF
				report.CorruptionDetected = true
				report.CorruptionType = "truncated_tail"
				report.CorruptionOffset = currentOffset
				if err := file.Truncate(lastValidOffset); err == nil {
					report.FileTruncated = true
				}
				break
			}
			return report, err
		}

		checksum := binary.BigEndian.Uint32(header[0:4])
		payloadLen := binary.BigEndian.Uint32(header[4:8])

		// OOM Protection & Length Validation:
		// Reject payloads larger than MaxRecordPayload or larger than the entire file.
		if payloadLen > MaxRecordPayload || int64(payloadLen) > fileSize {
			report.CorruptionDetected = true
			report.CorruptionOffset = currentOffset
			report.CorruptionType = "bad_length"

			// Check if corruption is at tail or in the middle
			if currentOffset+8 >= fileSize {
				if err := file.Truncate(lastValidOffset); err == nil {
					report.FileTruncated = true
				}
			} else {
				report.CorruptionType = "bad_length_middle"
			}
			break
		}

		payload := make([]byte, payloadLen)
		nPayload, err := io.ReadFull(reader, payload)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// Incomplete payload written before crash (truncated tail)
				report.CorruptionDetected = true
				report.CorruptionType = "truncated_tail"
				report.CorruptionOffset = currentOffset
				if err := file.Truncate(lastValidOffset); err == nil {
					report.FileTruncated = true
				}
				break
			}
			return report, err
		}

		// CRC32 Checksum Verification
		if crc32.ChecksumIEEE(payload) != checksum {
			report.CorruptionDetected = true
			report.CorruptionOffset = currentOffset

			// Determine if this is a tail corruption or corrupt middle
			if currentOffset+8+int64(nPayload) >= fileSize {
				report.CorruptionType = "bad_crc_tail"
				if err := file.Truncate(lastValidOffset); err == nil {
					report.FileTruncated = true
				}
			} else {
				report.CorruptionType = "corrupt_middle"
			}
			break
		}

		entry, err := core.DecodeEntry(payload)
		if err != nil {
			report.CorruptionDetected = true
			report.CorruptionOffset = currentOffset
			report.CorruptionType = "decode_error"
			if currentOffset+8+int64(nPayload) >= fileSize {
				if err := file.Truncate(lastValidOffset); err == nil {
					report.FileTruncated = true
				}
			}
			break
		}

		if err := onEntry(entry); err != nil {
			return report, fmt.Errorf("recovery handler failed: %w", err)
		}

		validCount++
		lastValidOffset += int64(n + nPayload)
	}

	report.ValidRecords = validCount
	report.BytesRecovered = lastValidOffset
	return report, nil
}

// Recover reads an existing WAL file and replays all valid entries to the handler callback.
// If an uncompleted write is detected at the tail of the log (common during power failure / crash),
// it truncates the file to the last valid record and returns without failing.
func Recover(filePath string, onEntry func(e *core.Entry) error) (int, error) {
	report, err := RecoverWithReport(filePath, onEntry)
	return report.ValidRecords, err
}

// ListWALFiles discovers all WAL log files in the specified directory, sorted by ID.
func ListWALFiles(dir string) ([]uint64, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var ids []uint64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasPrefix(name, "wal_") && strings.HasSuffix(name, ".log") {
			numPart := strings.TrimPrefix(name, "wal_")
			numPart = strings.TrimSuffix(numPart, ".log")
			id, err := strconv.ParseUint(numPart, 10, 64)
			if err == nil {
				ids = append(ids, id)
			}
		}
	}
	return ids, nil
}
