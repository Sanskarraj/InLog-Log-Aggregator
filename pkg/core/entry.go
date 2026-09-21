package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"
)

// Operation types for LSM-Tree mutations
const (
	OpPut    byte = 1
	OpDelete byte = 2 // Tombstone marker
)

var (
	ErrKeyNotFound    = errors.New("key not found")
	ErrKeyDeleted     = errors.New("key deleted (tombstone)")
	ErrEmptyKey       = errors.New("empty key not allowed")
	ErrDatabaseClosed = errors.New("database is closed")
)

// Entry represents a single key-value operation in the LSM tree.
type Entry struct {
	Key       []byte
	Value     []byte
	Timestamp int64 // Nanoseconds Unix timestamp
	Type      byte  // OpPut or OpDelete
}

// NewPutEntry creates an insertion/update entry with cloned byte slices.
func NewPutEntry(key, value []byte) *Entry {
	return &Entry{
		Key:       CloneBytes(key),
		Value:     CloneBytes(value),
		Timestamp: time.Now().UnixNano(),
		Type:      OpPut,
	}
}

// NewPutEntryDirect creates an entry reusing caller-provided byte slices without re-allocating.
func NewPutEntryDirect(key, value []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UnixNano(),
		Type:      OpPut,
	}
}

// NewDeleteEntry creates a tombstone entry.
func NewDeleteEntry(key []byte) *Entry {
	return &Entry{
		Key:       CloneBytes(key),
		Value:     nil,
		Timestamp: time.Now().UnixNano(),
		Type:      OpDelete,
	}
}

// IsTombstone returns true if the entry is a deletion marker.
func (e *Entry) IsTombstone() bool {
	return e.Type == OpDelete
}

// Size returns the approximate memory footprint of the entry in bytes.
func (e *Entry) Size() int {
	return len(e.Key) + len(e.Value) + 16
}

// Compare compares two entries lexicographically by key, and breaks ties by timestamp (descending).
func (e *Entry) Compare(other *Entry) int {
	cmp := bytes.Compare(e.Key, other.Key)
	if cmp != 0 {
		return cmp
	}
	if e.Timestamp > other.Timestamp {
		return -1
	} else if e.Timestamp < other.Timestamp {
		return 1
	}
	return 0
}

// EncodedSize returns the exact byte size needed to serialize the entry.
func (e *Entry) EncodedSize() int {
	return 15 + len(e.Key) + len(e.Value)
}

// EncodeTo writes the entry into dst. If cap(dst) is sufficient, it reuses dst without heap allocations.
func (e *Entry) EncodeTo(dst []byte) []byte {
	needed := e.EncodedSize()
	if cap(dst) >= needed {
		dst = dst[:needed]
	} else {
		dst = make([]byte, needed)
	}

	binary.BigEndian.PutUint64(dst[0:8], uint64(e.Timestamp))
	dst[8] = e.Type
	binary.BigEndian.PutUint16(dst[9:11], uint16(len(e.Key)))
	binary.BigEndian.PutUint32(dst[11:15], uint32(len(e.Value)))

	copy(dst[15:15+len(e.Key)], e.Key)
	copy(dst[15+len(e.Key):], e.Value)
	return dst
}

// EncodeFrameTo writes the complete WAL frame ([CRC32: 4B][PayloadLen: 4B][Payload...]) into dst without allocating.
func (e *Entry) EncodeFrameTo(dst []byte) []byte {
	pLen := e.EncodedSize()
	total := 8 + pLen
	if cap(dst) >= total {
		dst = dst[:total]
	} else {
		dst = make([]byte, total)
	}

	payload := dst[8:total]
	binary.BigEndian.PutUint64(payload[0:8], uint64(e.Timestamp))
	payload[8] = e.Type
	binary.BigEndian.PutUint16(payload[9:11], uint16(len(e.Key)))
	binary.BigEndian.PutUint32(payload[11:15], uint32(len(e.Value)))
	copy(payload[15:15+len(e.Key)], e.Key)
	copy(payload[15+len(e.Key):], e.Value)

	checksum := crc32.ChecksumIEEE(payload)
	binary.BigEndian.PutUint32(dst[0:4], checksum)
	binary.BigEndian.PutUint32(dst[4:8], uint32(pLen))

	return dst
}

// Encode serializes the entry to a newly allocated byte slice.
func (e *Entry) Encode() []byte {
	return e.EncodeTo(nil)
}

// DecodeEntry deserializes an entry from a byte buffer.
func DecodeEntry(data []byte) (*Entry, error) {
	if len(data) < 15 {
		return nil, errors.New("insufficient buffer for entry header")
	}

	ts := int64(binary.BigEndian.Uint64(data[0:8]))
	opType := data[8]
	kLen := int(binary.BigEndian.Uint16(data[9:11]))
	vLen := int(binary.BigEndian.Uint32(data[11:15]))

	if len(data) < 15+kLen+vLen {
		return nil, errors.New("buffer corrupted or truncated")
	}

	key := make([]byte, kLen)
	copy(key, data[15:15+kLen])

	var val []byte
	if vLen > 0 {
		val = make([]byte, vLen)
		copy(val, data[15+kLen:15+kLen+vLen])
	}

	return &Entry{
		Key:       key,
		Value:     val,
		Timestamp: ts,
		Type:      opType,
	}, nil
}

// DecodeEntryZeroCopy deserializes an entry without allocating new byte slices for Key and Value.
// The returned entry's Key and Value reference the provided data slice directly.
func DecodeEntryZeroCopy(data []byte) (*Entry, error) {
	if len(data) < 15 {
		return nil, errors.New("insufficient buffer for entry header")
	}

	ts := int64(binary.BigEndian.Uint64(data[0:8]))
	opType := data[8]
	kLen := int(binary.BigEndian.Uint16(data[9:11]))
	vLen := int(binary.BigEndian.Uint32(data[11:15]))

	if len(data) < 15+kLen+vLen {
		return nil, errors.New("buffer corrupted or truncated")
	}

	key := data[15 : 15+kLen]
	var val []byte
	if vLen > 0 {
		val = data[15+kLen : 15+kLen+vLen]
	}

	return &Entry{
		Key:       key,
		Value:     val,
		Timestamp: ts,
		Type:      opType,
	}, nil
}

// CloneBytes allocates and returns an exact copy of a byte slice.
func CloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}
