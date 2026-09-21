package sstable

import (
	"fmt"
	"testing"
)

func TestBloomFilterAccuracy(t *testing.T) {
	n := 10000
	fpTarget := 0.01 // 1% false positive rate
	bf := NewBloomFilter(n, fpTarget)

	// Add n keys
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("item_key_%06d", i))
		bf.Add(key)
	}

	// 1. Verify 100% True Positive (Zero false negatives)
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("item_key_%06d", i))
		if !bf.MayContain(key) {
			t.Fatalf("false negative detected for key %s", key)
		}
	}

	// 2. Measure False Positive rate on missing keys
	falsePositives := 0
	testMissing := 10000
	for i := 0; i < testMissing; i++ {
		missingKey := []byte(fmt.Sprintf("absent_item_%06d", i))
		if bf.MayContain(missingKey) {
			falsePositives++
		}
	}

	actualFp := float64(falsePositives) / float64(testMissing)
	t.Logf("Measured Bloom Filter False Positive Rate: %.4f (Target: %.2f)", actualFp, fpTarget)
	if actualFp > 0.03 { // Allow slight statistical tolerance above 1%
		t.Fatalf("false positive rate too high: got %.4f, target %.2f", actualFp, fpTarget)
	}

	// 3. Test serialization / deserialization roundtrip
	encoded := bf.Encode()
	decoded, err := DecodeBloomFilter(encoded)
	if err != nil {
		t.Fatalf("failed decoding bloom filter: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("item_key_%06d", i))
		if !decoded.MayContain(key) {
			t.Fatalf("decoded bloom filter missing key %s", key)
		}
	}
}
