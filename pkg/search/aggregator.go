package search

import (
	"sort"
	"time"
)

// Bucket represents an aggregated time interval.
type Bucket struct {
	Timestamp int64            `json:"timestamp"` // Bucket start in Unix nanoseconds
	Count     int64            `json:"count"`
	ByLevel   map[string]int64 `json:"by_level"`
}

// AggregationResult summarizes log frequency over time.
type AggregationResult struct {
	IntervalMs int64     `json:"interval_ms"`
	Buckets    []*Bucket `json:"buckets"`
	Total      int64     `json:"total"`
}

// ParseInterval converts duration string (e.g. "1s", "1m", "1h") to nanoseconds.
func ParseInterval(intervalStr string) int64 {
	d, err := time.ParseDuration(intervalStr)
	if err != nil || d <= 0 {
		return int64(time.Minute) // Default 1 minute
	}
	return int64(d)
}

// AggregateGroups gathers matching logs into discrete time buckets.
func AggregateGroups(logs []*LogRecord, intervalNano int64) *AggregationResult {
	if intervalNano <= 0 {
		intervalNano = int64(time.Minute)
	}

	bucketMap := make(map[int64]*Bucket)
	var total int64

	for _, log := range logs {
		bucketStart := (log.Timestamp / intervalNano) * intervalNano
		b, exists := bucketMap[bucketStart]
		if !exists {
			b = &Bucket{
				Timestamp: bucketStart,
				ByLevel:   make(map[string]int64),
			}
			bucketMap[bucketStart] = b
		}
		b.Count++
		b.ByLevel[log.Level]++
		total++
	}

	var buckets []*Bucket
	for _, b := range bucketMap {
		buckets = append(buckets, b)
	}

	// Sort chronologically
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Timestamp < buckets[j].Timestamp
	})

	return &AggregationResult{
		IntervalMs: intervalNano / 1e6,
		Buckets:    buckets,
		Total:      total,
	}
}
