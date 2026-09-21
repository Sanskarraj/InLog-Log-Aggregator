package sstable

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// IndexEntry maps a block's initial key to its byte location in the data file.
type IndexEntry struct {
	Key    []byte
	Offset uint64
	Length uint32
}

// SparseIndex is an in-memory index table enabling binary search over SSTable blocks.
type SparseIndex struct {
	Entries []IndexEntry
}

// NewSparseIndex creates an empty SparseIndex.
func NewSparseIndex() *SparseIndex {
	return &SparseIndex{}
}

// Add appends a new index entry.
func (idx *SparseIndex) Add(key []byte, offset uint64, length uint32) {
	k := make([]byte, len(key))
	copy(k, key)
	idx.Entries = append(idx.Entries, IndexEntry{
		Key:    k,
		Offset: offset,
		Length: length,
	})
}

// FindCandidateBlock finds the block where the key may reside using binary search.
// It returns the last block whose first key is <= target key.
func (idx *SparseIndex) FindCandidateBlock(key []byte) (IndexEntry, bool) {
	if len(idx.Entries) == 0 {
		return IndexEntry{}, false
	}

	low := 0
	high := len(idx.Entries) - 1
	bestIdx := -1

	for low <= high {
		mid := (low + high) / 2
		cmp := bytes.Compare(idx.Entries[mid].Key, key)
		if cmp <= 0 {
			bestIdx = mid
			low = mid + 1 // try to find a closer starting key
		} else {
			high = mid - 1
		}
	}

	if bestIdx == -1 {
		// All blocks start with keys > target key; could be in first block if sparse index is upper-bound,
		// but since it records lower-bound (first key), it cannot be in this SSTable.
		return IndexEntry{}, false
	}

	return idx.Entries[bestIdx], true
}

// Encode serializes the sparse index to bytes.
// Format:
// [Count: 4B]
// For each entry:
//   [KeyLen: 2B][Key bytes][Offset: 8B][Length: 4B]
func (idx *SparseIndex) Encode() []byte {
	var buf []byte
	var countBuf [4]byte
	binary.BigEndian.PutUint32(countBuf[:], uint32(len(idx.Entries)))
	buf = append(buf, countBuf[:]...)

	for _, entry := range idx.Entries {
		kLen := uint16(len(entry.Key))
		var header [2]byte
		binary.BigEndian.PutUint16(header[:], kLen)
		buf = append(buf, header[:]...)
		buf = append(buf, entry.Key...)

		var loc [12]byte
		binary.BigEndian.PutUint64(loc[0:8], entry.Offset)
		binary.BigEndian.PutUint32(loc[8:12], entry.Length)
		buf = append(buf, loc[:]...)
	}

	return buf
}

// DecodeSparseIndex deserializes a sparse index from a byte slice.
func DecodeSparseIndex(data []byte) (*SparseIndex, error) {
	if len(data) < 4 {
		return nil, errors.New("sparse index data too short")
	}

	count := int(binary.BigEndian.Uint32(data[0:4]))
	entries := make([]IndexEntry, 0, count)
	offset := 4

	for i := 0; i < count; i++ {
		if offset+2 > len(data) {
			return nil, errors.New("sparse index corrupted at key length")
		}
		kLen := int(binary.BigEndian.Uint16(data[offset : offset+2]))
		offset += 2

		if offset+kLen+12 > len(data) {
			return nil, fmt.Errorf("sparse index corrupted at entry %d", i)
		}

		key := make([]byte, kLen)
		copy(key, data[offset:offset+kLen])
		offset += kLen

		blockOffset := binary.BigEndian.Uint64(data[offset : offset+8])
		blockLen := binary.BigEndian.Uint32(data[offset+8 : offset+12])
		offset += 12

		entries = append(entries, IndexEntry{
			Key:    key,
			Offset: blockOffset,
			Length: blockLen,
		})
	}

	return &SparseIndex{Entries: entries}, nil
}
