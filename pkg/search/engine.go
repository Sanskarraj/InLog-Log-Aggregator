package search

import (
	"bytes"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/engine/lsm-trees/pkg/lsm"
)

// SearchEngine coordinates log parsing, inverted secondary indexing, and query evaluation over an LSM-Tree.
type SearchEngine struct {
	mu  sync.RWMutex
	db  *lsm.Engine
	dir string
}

// Open initializes the SearchEngine backed by the given LSM engine.
func Open(db *lsm.Engine) *SearchEngine {
	return &SearchEngine{
		db: db,
	}
}

// Ingest indexes and persists a single log record into the LSM tree.
func (s *SearchEngine) Ingest(record *LogRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}

	payload, err := record.ToJSON()
	if err != nil {
		return fmt.Errorf("failed marshaling log record: %w", err)
	}

	// 1. Store primary document: doc:<id>
	docKey := fmt.Sprintf("doc:%s", record.ID)
	if err := s.db.Put([]byte(docKey), payload); err != nil {
		return fmt.Errorf("failed storing document: %w", err)
	}

	// 2. Index timestamp for chronological range scans: idx:time:<ts_nano>:<id>
	timeKey := fmt.Sprintf("idx:time:%020d:%s", record.Timestamp, record.ID)
	_ = s.db.Put([]byte(timeKey), nil)

	// 3. Index level: idx:lvl:<level>:<ts_nano>:<id>
	lvlKey := fmt.Sprintf("idx:lvl:%s:%020d:%s", record.Level, record.Timestamp, record.ID)
	_ = s.db.Put([]byte(lvlKey), nil)

	// 4. Index service: idx:svc:<service>:<ts_nano>:<id>
	svcKey := fmt.Sprintf("idx:svc:%s:%020d:%s", record.Service, record.Timestamp, record.ID)
	_ = s.db.Put([]byte(svcKey), nil)

	// 5. Inverted index for message text terms: term:<token>:<ts_nano>:<id>
	tokens := Tokenize(record.Message)
	for _, token := range tokens {
		termKey := fmt.Sprintf("term:%s:%020d:%s", token, record.Timestamp, record.ID)
		_ = s.db.Put([]byte(termKey), nil)
	}

	// 6. Custom field index
	for k, v := range record.Fields {
		fKey := fmt.Sprintf("fld:%s=%s:%020d:%s", k, v, record.Timestamp, record.ID)
		_ = s.db.Put([]byte(fKey), nil)
	}

	return nil
}

// IngestBatch ingests a slice of log records sequentially.
func (s *SearchEngine) IngestBatch(records []*LogRecord) (int, error) {
	count := 0
	for _, rec := range records {
		if err := s.Ingest(rec); err != nil {
			return count, err
		}
		count++
	}
	return count, nil
}

// GetByID retrieves a single log record by its unique ID.
func (s *SearchEngine) GetByID(id string) (*LogRecord, error) {
	docKey := fmt.Sprintf("doc:%s", id)
	val, err := s.db.Get([]byte(docKey))
	if err != nil {
		return nil, err
	}
	return ParseLogRecord(val)
}

// Search executes a query against the inverted index and LSM storage.
func (s *SearchEngine) Search(q Query) (*SearchResult, error) {
	start := time.Now()

	fromTime := q.FromTime
	if fromTime <= 0 {
		fromTime = 0
	}
	toTime := q.ToTime
	if toTime <= 0 {
		toTime = 1<<62 - 1 // Max future timestamp
	}

	var candidateIDs []string
	var err error

	// Determine optimal index scan strategy
	tokens := Tokenize(q.Text)
	if len(tokens) > 0 {
		// Multi-term intersection using inverted index
		candidateIDs, err = s.intersectTerms(tokens, fromTime, toTime)
	} else if q.Level != "" {
		// Level index scan
		candidateIDs, err = s.scanPrefixIDs(
			fmt.Sprintf("idx:lvl:%s:%020d:", strings.ToUpper(q.Level), fromTime),
			fmt.Sprintf("idx:lvl:%s:%020d:\xff", strings.ToUpper(q.Level), toTime),
		)
	} else if q.Service != "" {
		// Service index scan
		candidateIDs, err = s.scanPrefixIDs(
			fmt.Sprintf("idx:svc:%s:%020d:", q.Service, fromTime),
			fmt.Sprintf("idx:svc:%s:%020d:\xff", q.Service, toTime),
		)
	} else {
		// General time index scan
		candidateIDs, err = s.scanPrefixIDs(
			fmt.Sprintf("idx:time:%020d:", fromTime),
			fmt.Sprintf("idx:time:%020d:\xff", toTime),
		)
	}

	if err != nil {
		return nil, fmt.Errorf("index scan failed: %w", err)
	}

	// Fetch primary documents and apply remaining filters
	var matchedLogs []*LogRecord
	for _, id := range candidateIDs {
		rec, err := s.GetByID(id)
		if err != nil {
			continue // Deleted or missing
		}

		// Filter by Level
		if q.Level != "" && !strings.EqualFold(rec.Level, q.Level) {
			continue
		}
		// Filter by Service
		if q.Service != "" && !strings.EqualFold(rec.Service, q.Service) {
			continue
		}
		// Filter by Host
		if q.Host != "" && !strings.EqualFold(rec.Host, q.Host) {
			continue
		}
		// Filter by Time
		if rec.Timestamp < fromTime || rec.Timestamp > toTime {
			continue
		}
		// Filter by Custom Fields
		matchFields := true
		for k, v := range q.Fields {
			if rec.Fields == nil || rec.Fields[k] != v {
				matchFields = false
				break
			}
		}
		if !matchFields {
			continue
		}

		matchedLogs = append(matchedLogs, rec)
	}

	totalHits := int64(len(matchedLogs))

	// Sort logs descending by timestamp (newest first)
	sort.Slice(matchedLogs, func(i, j int) bool {
		return matchedLogs[i].Timestamp > matchedLogs[j].Timestamp
	})

	// Apply pagination
	limit := q.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := q.Offset
	if offset < 0 {
		offset = 0
	}

	var pagedLogs []*LogRecord
	if offset < len(matchedLogs) {
		end := offset + limit
		if end > len(matchedLogs) {
			end = len(matchedLogs)
		}
		pagedLogs = matchedLogs[offset:end]
	}

	tookMs := float64(time.Since(start).Microseconds()) / 1000.0
	return &SearchResult{
		TotalHits: totalHits,
		TookMs:    tookMs,
		Logs:      pagedLogs,
	}, nil
}

// Aggregate performs time-bucketed histogram aggregation over the query results.
func (s *SearchEngine) Aggregate(q Query, intervalStr string) (*AggregationResult, error) {
	intervalNano := ParseInterval(intervalStr)
	// Query all matching logs without pagination limit
	q.Limit = 100000
	q.Offset = 0

	res, err := s.Search(q)
	if err != nil {
		return nil, err
	}

	return AggregateGroups(res.Logs, intervalNano), nil
}

func (s *SearchEngine) intersectTerms(tokens []string, fromTime, toTime int64) ([]string, error) {
	if len(tokens) == 0 {
		return nil, nil
	}

	// Fetch IDs for first token
	startKey := fmt.Sprintf("term:%s:%020d:", tokens[0], fromTime)
	endKey := fmt.Sprintf("term:%s:%020d:\xff", tokens[0], toTime)
	ids, err := s.scanPrefixIDs(startKey, endKey)
	if err != nil {
		return nil, err
	}

	commonMap := make(map[string]bool)
	for _, id := range ids {
		commonMap[id] = true
	}

	// Intersect with remaining tokens
	for i := 1; i < len(tokens); i++ {
		tKeyStart := fmt.Sprintf("term:%s:%020d:", tokens[i], fromTime)
		tKeyEnd := fmt.Sprintf("term:%s:%020d:\xff", tokens[i], toTime)
		tIDs, err := s.scanPrefixIDs(tKeyStart, tKeyEnd)
		if err != nil {
			return nil, err
		}

		currentSet := make(map[string]bool)
		for _, id := range tIDs {
			currentSet[id] = true
		}

		for id := range commonMap {
			if !currentSet[id] {
				delete(commonMap, id)
			}
		}
		if len(commonMap) == 0 {
			break
		}
	}

	var result []string
	for id := range commonMap {
		result = append(result, id)
	}
	return result, nil
}

func (s *SearchEngine) scanPrefixIDs(startKey, endKey string) ([]string, error) {
	iter, err := s.db.Scan([]byte(startKey), []byte(endKey))
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var ids []string
	seen := make(map[string]struct{})

	for iter.Valid() {
		k := string(iter.Key())
		if bytes.Compare(iter.Key(), []byte(endKey)) > 0 {
			break
		}

		parts := strings.Split(k, ":")
		if len(parts) >= 4 {
			id := parts[len(parts)-1]
			if _, exists := seen[id]; !exists {
				seen[id] = struct{}{}
				ids = append(ids, id)
			}
		}

		iter.Next()
	}

	return ids, nil
}
