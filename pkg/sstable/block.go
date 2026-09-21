package sstable

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/engine/lsm-trees/pkg/core"
)

var (
	ErrBlockChecksumMismatch = errors.New("sstable: block CRC32 checksum mismatch")
	ErrBlockCorrupt          = errors.New("sstable: corrupt data block")
)

// Block holds a sequence of sorted entries packed into a memory chunk.
type Block struct {
	Entries []*core.Entry
}

// Encode serializes all entries in the block followed by a 4-byte CRC32 trailer.
func (b *Block) Encode() []byte {
	var payload []byte
	for _, entry := range b.Entries {
		data := entry.Encode()
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))
		payload = append(payload, lenBuf[:]...)
		payload = append(payload, data...)
	}

	checksum := crc32.ChecksumIEEE(payload)
	var trailer [4]byte
	binary.BigEndian.PutUint32(trailer[:], checksum)

	return append(payload, trailer[:]...)
}

// DecodeBlock deserializes and validates a block payload including its CRC32 trailer.
func DecodeBlock(data []byte) (*Block, error) {
	if len(data) < 4 {
		return nil, ErrBlockCorrupt
	}

	payloadLen := len(data) - 4
	payload := data[:payloadLen]
	expectedChecksum := binary.BigEndian.Uint32(data[payloadLen:])

	actualChecksum := crc32.ChecksumIEEE(payload)
	if actualChecksum != expectedChecksum {
		return nil, ErrBlockChecksumMismatch
	}

	// 1. Count entries in block first (fast linear scan over cached 4KB payload)
	count := 0
	scanOff := 0
	for scanOff < payloadLen {
		if scanOff+4 > payloadLen {
			return nil, ErrBlockCorrupt
		}
		entryLen := int(binary.BigEndian.Uint32(payload[scanOff : scanOff+4]))
		scanOff += 4 + entryLen
		if scanOff > payloadLen {
			return nil, ErrBlockCorrupt
		}
		count++
	}

	// 2. Preallocate all entry values in a single continuous array
	entriesBuf := make([]core.Entry, count)
	entries := make([]*core.Entry, count)

	offset := 0
	idx := 0
	for offset < payloadLen {
		entryLen := int(binary.BigEndian.Uint32(payload[offset : offset+4]))
		offset += 4

		entryBytes := payload[offset : offset+entryLen]
		if len(entryBytes) < 15 {
			return nil, errors.New("insufficient buffer for entry header")
		}

		kLen := int(binary.BigEndian.Uint16(entryBytes[9:11]))
		vLen := int(binary.BigEndian.Uint32(entryBytes[11:15]))
		if len(entryBytes) < 15+kLen+vLen {
			return nil, errors.New("buffer corrupted or truncated")
		}

		entriesBuf[idx].Timestamp = int64(binary.BigEndian.Uint64(entryBytes[0:8]))
		entriesBuf[idx].Type = entryBytes[8]
		entriesBuf[idx].Key = entryBytes[15 : 15+kLen]
		if vLen > 0 {
			entriesBuf[idx].Value = entryBytes[15+kLen : 15+kLen+vLen]
		}

		entries[idx] = &entriesBuf[idx]
		idx++
		offset += entryLen
	}

	return &Block{Entries: entries}, nil
}
