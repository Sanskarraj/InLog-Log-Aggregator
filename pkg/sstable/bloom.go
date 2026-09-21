package sstable

import (
	"encoding/binary"
	"errors"
	"hash/fnv"
	"math"
)

// BloomFilter is a probabilistic data structure for set membership testing.
type BloomFilter struct {
	bitmap []byte
	bits   uint64
	k      uint32 // number of hash functions
}

// NewBloomFilter creates a BloomFilter optimized for n items and false positive rate fp.
func NewBloomFilter(n int, fp float64) *BloomFilter {
	if n <= 0 {
		n = 1000
	}
	if fp <= 0 || fp >= 1.0 {
		fp = 0.01 // 1% default
	}

	// m = - (n * ln(fp)) / (ln(2)^2)
	m := -float64(n) * math.Log(fp) / (math.Ln2 * math.Ln2)
	bits := uint64(math.Ceil(m))
	if bits%8 != 0 {
		bits = (bits/8 + 1) * 8
	}
	if bits < 64 {
		bits = 64
	}

	// k = (m / n) * ln(2)
	k := uint32(math.Round(float64(bits) / float64(n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}

	bytesLen := bits / 8
	return &BloomFilter{
		bitmap: make([]byte, bytesLen),
		bits:   bits,
		k:      k,
	}
}

// doubleHash generates two high-dispersion 64-bit hashes using FNV-64a and SplitMix64 mixing.
func (bf *BloomFilter) doubleHash(key []byte) (uint64, uint64) {
	hasher := fnv.New64a()
	_, _ = hasher.Write(key)
	h := hasher.Sum64()

	// SplitMix64 mixing step
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31

	h1 := uint64(uint32(h))
	h2 := uint64(uint32(h >> 32))
	if h2 == 0 {
		h2 = 1
	}

	return h1, h2
}

// Add inserts a key into the Bloom Filter.
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bf.doubleHash(key)
	for i := uint32(0); i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % bf.bits
		bf.bitmap[idx/8] |= 1 << (idx % 8)
	}
}

// MayContain returns false if the key is definitely not in the set,
// or true if the key might be in the set.
func (bf *BloomFilter) MayContain(key []byte) bool {
	if bf.bits == 0 || len(bf.bitmap) == 0 {
		return true
	}
	h1, h2 := bf.doubleHash(key)
	for i := uint32(0); i < bf.k; i++ {
		idx := (h1 + uint64(i)*h2) % bf.bits
		if (bf.bitmap[idx/8] & (1 << (idx % 8))) == 0 {
			return false // Definitely not present
		}
	}
	return true
}

// Encode serializes the bloom filter to bytes:
// [Bits: 8B][K: 4B][Bitmap length: 4B][Bitmap bytes...]
func (bf *BloomFilter) Encode() []byte {
	buf := make([]byte, 8+4+4+len(bf.bitmap))
	binary.BigEndian.PutUint64(buf[0:8], bf.bits)
	binary.BigEndian.PutUint32(buf[8:12], bf.k)
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(bf.bitmap)))
	copy(buf[16:], bf.bitmap)
	return buf
}

// DecodeBloomFilter deserializes a bloom filter from a byte slice.
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 16 {
		return nil, errors.New("bloom filter buffer too short")
	}
	bits := binary.BigEndian.Uint64(data[0:8])
	k := binary.BigEndian.Uint32(data[8:12])
	bLen := binary.BigEndian.Uint32(data[12:16])

	if len(data) < 16+int(bLen) {
		return nil, errors.New("bloom filter payload truncated")
	}

	bitmap := make([]byte, bLen)
	copy(bitmap, data[16:16+int(bLen)])

	return &BloomFilter{
		bitmap: bitmap,
		bits:   bits,
		k:      k,
	}, nil
}
