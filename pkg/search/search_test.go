package search

import (
	"os"
	"testing"
	"time"

	"github.com/engine/lsm-trees/pkg/lsm"
)

func TestSearchEngineEndToEnd(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "search_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	db, err := lsm.Open(lsm.DefaultOptions(tempDir))
	if err != nil {
		t.Fatalf("failed opening lsm engine: %v", err)
	}
	defer db.Close()

	engine := Open(db)

	baseTime := time.Now().Add(-10 * time.Minute).UnixNano()

	sampleLogs := []*LogRecord{
		{
			ID:        "log-001",
			Timestamp: baseTime + int64(1*time.Minute),
			Level:     "INFO",
			Service:   "auth-service",
			Message:   "User alice successfully logged in via OAuth",
		},
		{
			ID:        "log-002",
			Timestamp: baseTime + int64(2*time.Minute),
			Level:     "WARN",
			Service:   "payment-service",
			Message:   "Database connection slow: latency spike 450ms",
		},
		{
			ID:        "log-003",
			Timestamp: baseTime + int64(3*time.Minute),
			Level:     "ERROR",
			Service:   "payment-service",
			Message:   "Payment processing connection timeout on gateway",
		},
		{
			ID:        "log-004",
			Timestamp: baseTime + int64(4*time.Minute),
			Level:     "ERROR",
			Service:   "order-service",
			Message:   "Failed to submit order due to connection timeout",
		},
		{
			ID:        "log-005",
			Timestamp: baseTime + int64(5*time.Minute),
			Level:     "DEBUG",
			Service:   "auth-service",
			Message:   "Cache lookup for session token hit",
		},
	}

	count, err := engine.IngestBatch(sampleLogs)
	if err != nil {
		t.Fatalf("ingest batch failed: %v", err)
	}
	if count != len(sampleLogs) {
		t.Fatalf("expected %d ingested, got %d", len(sampleLogs), count)
	}

	// 1. Single-term search: "timeout" -> should match log-003 and log-004
	res, err := engine.Search(Query{Text: "timeout"})
	if err != nil {
		t.Fatalf("search timeout failed: %v", err)
	}
	if res.TotalHits != 2 {
		t.Fatalf("expected 2 matches for 'timeout', got %d", res.TotalHits)
	}

	// 2. Multi-term intersection (AND): "connection timeout payment" -> should match only log-003
	resMulti, err := engine.Search(Query{Text: "connection timeout payment"})
	if err != nil {
		t.Fatalf("multi-term search failed: %v", err)
	}
	if resMulti.TotalHits != 1 || resMulti.Logs[0].ID != "log-003" {
		t.Fatalf("expected 1 match (log-003) for multi-term AND, got %d", resMulti.TotalHits)
	}

	// 3. Level filter: Level="ERROR" -> log-003 and log-004
	resErr, err := engine.Search(Query{Level: "ERROR"})
	if err != nil {
		t.Fatalf("level search failed: %v", err)
	}
	if resErr.TotalHits != 2 {
		t.Fatalf("expected 2 ERROR logs, got %d", resErr.TotalHits)
	}

	// 4. Combined query: Text="timeout", Service="order-service" -> only log-004
	resCombined, err := engine.Search(Query{Text: "timeout", Service: "order-service"})
	if err != nil {
		t.Fatalf("combined search failed: %v", err)
	}
	if resCombined.TotalHits != 1 || resCombined.Logs[0].ID != "log-004" {
		t.Fatalf("expected 1 match (log-004), got %d", resCombined.TotalHits)
	}

	// 5. Test Aggregations by 1-minute intervals
	agg, err := engine.Aggregate(Query{}, "1m")
	if err != nil {
		t.Fatalf("aggregate failed: %v", err)
	}
	if agg.Total != 5 {
		t.Fatalf("expected 5 total aggregated logs, got %d", agg.Total)
	}
	if len(agg.Buckets) == 0 {
		t.Fatalf("expected at least 1 bucket, got 0")
	}
}
