package server

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/engine/lsm-trees/pkg/lsm"
	"github.com/engine/lsm-trees/pkg/search"
)

type APIHandler struct {
	db     *lsm.Engine
	search *search.SearchEngine
}

func NewAPIHandler(db *lsm.Engine, s *search.SearchEngine) *APIHandler {
	return &APIHandler{
		db:     db,
		search: s,
	}
}

// Ingest handles single or batch log ingestion via JSON array or NDJSON lines.
func (h *APIHandler) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var logs []*search.LogRecord

	// Check if JSON array
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '[' {
		if err := json.Unmarshal(trimmed, &logs); err != nil {
			http.Error(w, fmt.Sprintf("Invalid JSON array: %v", err), http.StatusBadRequest)
			return
		}
	} else if len(trimmed) > 0 && trimmed[0] == '{' {
		// Single JSON object or NDJSON
		if bytes.Contains(trimmed, []byte("\n")) {
			scanner := bufio.NewScanner(bytes.NewReader(trimmed))
			for scanner.Scan() {
				line := bytes.TrimSpace(scanner.Bytes())
				if len(line) == 0 {
					continue
				}
				rec, err := search.ParseLogRecord(line)
				if err != nil {
					http.Error(w, fmt.Sprintf("Invalid NDJSON line: %v", err), http.StatusBadRequest)
					return
				}
				logs = append(logs, rec)
			}
		} else {
			rec, err := search.ParseLogRecord(trimmed)
			if err != nil {
				http.Error(w, fmt.Sprintf("Invalid log JSON: %v", err), http.StatusBadRequest)
				return
			}
			logs = append(logs, rec)
		}
	} else {
		http.Error(w, "Unsupported body payload", http.StatusBadRequest)
		return
	}

	count, err := h.search.IngestBatch(logs)
	if err != nil {
		http.Error(w, fmt.Sprintf("Ingestion failed: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"ingested": count,
	})
}

// Search handles log searches with text queries, level/service filters, and time bounds.
func (h *APIHandler) handleSearch(w http.ResponseWriter, r *http.Request) {
	var q search.Query

	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&q); err != nil {
			http.Error(w, "Invalid query payload", http.StatusBadRequest)
			return
		}
	} else if r.Method == http.MethodGet {
		q.Text = r.URL.Query().Get("query")
		q.Level = r.URL.Query().Get("level")
		q.Service = r.URL.Query().Get("service")
		q.Host = r.URL.Query().Get("host")

		if sinceStr := r.URL.Query().Get("since"); sinceStr != "" {
			d, err := time.ParseDuration(sinceStr)
			if err == nil {
				q.FromTime = time.Now().Add(-d).UnixNano()
			}
		}

		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			q.Limit, _ = strconv.Atoi(limitStr)
		}
	} else {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	res, err := h.search.Search(q)
	if err != nil {
		http.Error(w, fmt.Sprintf("Search error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

// Aggregate returns time-bucketed counts.
func (h *APIHandler) handleAggregate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var q search.Query
	q.Text = r.URL.Query().Get("query")
	q.Level = r.URL.Query().Get("level")
	q.Service = r.URL.Query().Get("service")

	interval := r.URL.Query().Get("interval")
	if interval == "" {
		interval = "1m"
	}

	if sinceStr := r.URL.Query().Get("since"); sinceStr != "" {
		d, err := time.ParseDuration(sinceStr)
		if err == nil {
			q.FromTime = time.Now().Add(-d).UnixNano()
		}
	}

	agg, err := h.search.Aggregate(q, interval)
	if err != nil {
		http.Error(w, fmt.Sprintf("Aggregation error: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(agg)
}

// GetLog retrieves a log document by ID.
func (h *APIHandler) handleGetLog(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/api/v1/logs/")
	if id == "" {
		http.Error(w, "Log ID required", http.StatusBadRequest)
		return
	}

	rec, err := h.search.GetByID(id)
	if err != nil {
		if errors.Is(err, lsm.ErrKeyNotFound) {
			http.Error(w, "Log not found", http.StatusNotFound)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(rec)
}

// KV Handlers
func (h *APIHandler) handleKV(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/api/v1/kv/")
	if key == "" {
		http.Error(w, "Key required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		val, err := h.db.Get([]byte(key))
		if err != nil {
			if errors.Is(err, lsm.ErrKeyNotFound) {
				http.Error(w, "Key not found", http.StatusNotFound)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(val)

	case http.MethodPut, http.MethodPost:
		val, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "Failed to read body", http.StatusBadRequest)
			return
		}
		defer r.Body.Close()

		if err := h.db.Put([]byte(key), val); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"status":"created"}`))

	case http.MethodDelete:
		if err := h.db.Delete([]byte(key)); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"deleted"}`))

	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// KV Range Scan
func (h *APIHandler) handleKVScan(w http.ResponseWriter, r *http.Request) {
	startKey := r.URL.Query().Get("start")
	endKey := r.URL.Query().Get("end")
	limitStr := r.URL.Query().Get("limit")

	limit := 100
	if limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	iter, err := h.db.Scan([]byte(startKey), []byte(endKey))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer iter.Close()

	type kvPair struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	var results []kvPair
	for iter.Valid() && len(results) < limit {
		if len(endKey) > 0 && string(iter.Key()) > endKey {
			break
		}
		results = append(results, kvPair{
			Key:   string(iter.Key()),
			Value: string(iter.Value()),
		})
		iter.Next()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(results)
}

// Manual Flush
func (h *APIHandler) handleFlush(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.db.Flush(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"flushed"}`))
}

// Manual Compaction
func (h *APIHandler) handleCompact(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := h.db.Compact(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"compacted"}`))
}

// Prometheus-compatible Metrics endpoint
func (h *APIHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	stats := h.db.Stats()

	var b strings.Builder
	fmt.Fprintf(&b, "# HELP lsm_active_mem_bytes Memory size of active memtable in bytes\n")
	fmt.Fprintf(&b, "# TYPE lsm_active_mem_bytes gauge\n")
	fmt.Fprintf(&b, "lsm_active_mem_bytes %d\n\n", stats.ActiveMemBytes)

	fmt.Fprintf(&b, "# HELP lsm_active_mem_entries Number of entries in active memtable\n")
	fmt.Fprintf(&b, "# TYPE lsm_active_mem_entries gauge\n")
	fmt.Fprintf(&b, "lsm_active_mem_entries %d\n\n", stats.ActiveMemEntries)

	fmt.Fprintf(&b, "# HELP lsm_immutable_mem_bytes Memory size of immutable memtable\n")
	fmt.Fprintf(&b, "# TYPE lsm_immutable_mem_bytes gauge\n")
	fmt.Fprintf(&b, "lsm_immutable_mem_bytes %d\n\n", stats.ImmMemBytes)

	fmt.Fprintf(&b, "# HELP lsm_total_sstables Total on-disk SSTable files\n")
	fmt.Fprintf(&b, "# TYPE lsm_total_sstables gauge\n")
	fmt.Fprintf(&b, "lsm_total_sstables %d\n\n", stats.TotalSSTables)

	fmt.Fprintf(&b, "# HELP lsm_total_disk_bytes Total size of all SSTable data on disk\n")
	fmt.Fprintf(&b, "# TYPE lsm_total_disk_bytes gauge\n")
	fmt.Fprintf(&b, "lsm_total_disk_bytes %d\n\n", stats.TotalDiskBytes)

	fmt.Fprintf(&b, "# HELP lsm_puts_total Total write mutations processed\n")
	fmt.Fprintf(&b, "# TYPE lsm_puts_total counter\n")
	fmt.Fprintf(&b, "lsm_puts_total %d\n\n", stats.TotalPuts)

	fmt.Fprintf(&b, "# HELP lsm_deletes_total Total delete tombstones processed\n")
	fmt.Fprintf(&b, "# TYPE lsm_deletes_total counter\n")
	fmt.Fprintf(&b, "lsm_deletes_total %d\n\n", stats.TotalDeletes)

	fmt.Fprintf(&b, "# HELP lsm_gets_total Total point lookups\n")
	fmt.Fprintf(&b, "# TYPE lsm_gets_total counter\n")
	fmt.Fprintf(&b, "lsm_gets_total %d\n\n", stats.TotalGets)

	fmt.Fprintf(&b, "# HELP lsm_bloom_hits_total Total positive Bloom filter checks\n")
	fmt.Fprintf(&b, "# TYPE lsm_bloom_hits_total counter\n")
	fmt.Fprintf(&b, "lsm_bloom_hits_total %d\n\n", stats.BloomFilterHits)

	fmt.Fprintf(&b, "# HELP lsm_bloom_misses_total Total negative Bloom filter checks (disk seeks avoided)\n")
	fmt.Fprintf(&b, "# TYPE lsm_bloom_misses_total counter\n")
	fmt.Fprintf(&b, "lsm_bloom_misses_total %d\n\n", stats.BloomFilterMiss)

	fmt.Fprintf(&b, "# HELP lsm_flushes_total Total memtable to SSTable flushes\n")
	fmt.Fprintf(&b, "# TYPE lsm_flushes_total counter\n")
	fmt.Fprintf(&b, "lsm_flushes_total %d\n\n", stats.TotalFlushes)

	fmt.Fprintf(&b, "# HELP lsm_compactions_total Total leveled compaction routines completed\n")
	fmt.Fprintf(&b, "# TYPE lsm_compactions_total counter\n")
	fmt.Fprintf(&b, "lsm_compactions_total %d\n\n", stats.CompactionStats.TotalCompactions)

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}
