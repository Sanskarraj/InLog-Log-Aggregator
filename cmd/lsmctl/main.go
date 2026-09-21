package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/engine/lsm-trees/pkg/search"
)

func main() {
	if len(os.Args) < 2 {
		printHelp()
		os.Exit(1)
	}

	subcommand := os.Args[1]
	args := os.Args[2:]

	switch subcommand {
	case "ingest":
		handleIngest(args)
	case "search":
		handleSearch(args)
	case "aggregate":
		handleAggregate(args)
	case "put":
		handlePut(args)
	case "get":
		handleGet(args)
	case "delete":
		handleDelete(args)
	case "scan":
		handleScan(args)
	case "stats":
		handleStats(args)
	case "flush":
		handleFlush(args)
	case "compact":
		handleCompact(args)
	case "help", "-h", "--help":
		printHelp()
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n\n", subcommand)
		printHelp()
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Println(`lsmctl - Enterprise CLI for LSM-Tree Log Aggregator & Search Engine

USAGE:
  lsmctl <subcommand> [options] [arguments]

SUBCOMMANDS:
  ingest       Ingest log entries from file or stdin (JSON/NDJSON)
  search       Search logs by terms, level, service, and time range
  aggregate    Generate time-bucketed log volume histograms
  put          Store a low-level key-value pair
  get          Retrieve a value by key
  delete       Mark a key with a tombstone deletion
  scan         Execute a range scan across keys
  stats        Inspect engine telemetry, level distributions, and metrics
  flush        Trigger immediate MemTable rotation and flush to SSTable
  compact      Trigger background leveled compaction routine

GLOBAL OPTIONS:
  --server     Base URL of the lsmd daemon (default: http://localhost:8080)

EXAMPLES:
  lsmctl ingest --file=sample_logs.json
  lsmctl search --query="connection timeout" --level=ERROR --since=1h
  lsmctl aggregate --interval=1m --since=24h
  lsmctl put user:100 '{"name": "Alice"}'
  lsmctl get user:100
  lsmctl scan user:100 user:200
  lsmctl stats`)
}

func handleIngest(args []string) {
	fs := flag.NewFlagSet("ingest", flag.ExitOnError)
	file := fs.String("file", "", "Path to file containing logs (JSON array or NDJSON). Omit to read from stdin.")
	serverURL := fs.String("server", "http://localhost:8080", "Server base URL")
	_ = fs.Parse(args)

	var reader io.Reader
	if *file != "" {
		f, err := os.Open(*file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to open file: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()
		reader = f
	} else {
		reader = os.Stdin
	}

	data, err := io.ReadAll(reader)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to read input: %v\n", err)
		os.Exit(1)
	}

	resp, err := http.Post(*serverURL+"/api/v1/logs/ingest", "application/json", bytes.NewReader(data))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Ingest request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "Ingest failed (HTTP %d): %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}

	fmt.Printf("Ingestion successful: %s\n", string(body))
}

func handleSearch(args []string) {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	query := fs.String("query", "", "Full-text search keywords")
	level := fs.String("level", "", "Log level filter (DEBUG, INFO, WARN, ERROR)")
	service := fs.String("service", "", "Service name filter")
	since := fs.String("since", "24h", "Time window (e.g. 15m, 1h, 24h)")
	limit := fs.Int("limit", 20, "Max results to display")
	serverURL := fs.String("server", "http://localhost:8080", "Server base URL")
	_ = fs.Parse(args)

	var q search.Query
	q.Text = *query
	q.Level = *level
	q.Service = *service
	q.Limit = *limit

	if *since != "" {
		d, err := time.ParseDuration(*since)
		if err == nil {
			q.FromTime = time.Now().Add(-d).UnixNano()
		}
	}

	payload, _ := json.Marshal(q)
	resp, err := http.Post(*serverURL+"/api/v1/logs/search", "application/json", bytes.NewReader(payload))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Search request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var result search.SearchResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintf(os.Stderr, "Failed decoding response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Found %d matches (took %.2f ms):\n\n", result.TotalHits, result.TookMs)
	for _, l := range result.Logs {
		t := time.Unix(0, l.Timestamp).Format("2006-01-02 15:04:05.000")
		fmt.Printf("[%s] [%-5s] [%-12s] %s (id: %s)\n", t, l.Level, l.Service, l.Message, l.ID[:8])
	}
}

func handleAggregate(args []string) {
	fs := flag.NewFlagSet("aggregate", flag.ExitOnError)
	interval := fs.String("interval", "1m", "Aggregation bucket interval (e.g. 10s, 1m, 5m, 1h)")
	since := fs.String("since", "1h", "Time duration to look back")
	query := fs.String("query", "", "Filter query")
	level := fs.String("level", "", "Filter level")
	serverURL := fs.String("server", "http://localhost:8080", "Server base URL")
	_ = fs.Parse(args)

	reqURL := fmt.Sprintf("%s/api/v1/logs/aggregate?interval=%s&since=%s&query=%s&level=%s",
		*serverURL, *interval, *since, *query, *level)

	resp, err := http.Get(reqURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Aggregate request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var result search.AggregationResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		fmt.Fprintf(os.Stderr, "Failed decoding response: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\n--- Log Aggregation (Interval: %s | Total: %d logs) ---\n\n", *interval, result.Total)
	maxCount := int64(1)
	for _, b := range result.Buckets {
		if b.Count > maxCount {
			maxCount = b.Count
		}
	}

	for _, b := range result.Buckets {
		t := time.Unix(0, b.Timestamp).Format("15:04:05")
		barLen := int((b.Count * 40) / maxCount)
		if barLen < 1 && b.Count > 0 {
			barLen = 1
		}
		bar := strings.Repeat("█", barLen)

		var breakdown []string
		for lvl, cnt := range b.ByLevel {
			breakdown = append(breakdown, fmt.Sprintf("%s:%d", lvl, cnt))
		}

		fmt.Printf("%s | %-40s | %4d logs  (%s)\n", t, bar, b.Count, strings.Join(breakdown, ", "))
	}
	fmt.Println()
}

func defaultServerURL() string {
	if u := os.Getenv("LSM_SERVER"); u != "" {
		return strings.TrimRight(u, "/")
	}
	return "http://localhost:8080"
}

func handlePut(args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	serverURL := fs.String("server", defaultServerURL(), "Server base URL")
	_ = fs.Parse(args)
	rem := fs.Args()

	if len(rem) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: lsmctl put [--server=URL] <key> <value>\n")
		os.Exit(1)
	}
	key := rem[0]
	val := rem[1]

	req, _ := http.NewRequest(http.MethodPut, *serverURL+"/api/v1/kv/"+key, strings.NewReader(val))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Put failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated {
		fmt.Println("OK")
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Put failed (%d): %s\n", resp.StatusCode, string(body))
	}
}

func handleGet(args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	serverURL := fs.String("server", defaultServerURL(), "Server base URL")
	_ = fs.Parse(args)
	rem := fs.Args()

	if len(rem) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: lsmctl get [--server=URL] <key>\n")
		os.Exit(1)
	}
	key := rem[0]

	resp, err := http.Get(*serverURL + "/api/v1/kv/" + key)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Get failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		fmt.Println("(nil)")
		return
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Get failed (%d): %s\n", resp.StatusCode, string(body))
		return
	}

	body, _ := io.ReadAll(resp.Body)
	fmt.Println(string(body))
}

func handleDelete(args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	serverURL := fs.String("server", defaultServerURL(), "Server base URL")
	_ = fs.Parse(args)
	rem := fs.Args()

	if len(rem) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: lsmctl delete [--server=URL] <key>\n")
		os.Exit(1)
	}
	key := rem[0]

	req, _ := http.NewRequest(http.MethodDelete, *serverURL+"/api/v1/kv/"+key, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Delete failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		fmt.Println("DELETED")
	} else {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "Delete failed (%d): %s\n", resp.StatusCode, string(body))
	}
}

func handleScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	serverURL := fs.String("server", defaultServerURL(), "Server base URL")
	limit := fs.Int("limit", 100, "Max keys to scan")
	_ = fs.Parse(args)
	rem := fs.Args()

	if len(rem) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: lsmctl scan [--server=URL] [--limit=N] <startKey> <endKey>\n")
		os.Exit(1)
	}
	startKey := rem[0]
	endKey := rem[1]

	reqURL := fmt.Sprintf("%s/api/v1/kv/scan?start=%s&end=%s&limit=%d", *serverURL, startKey, endKey, *limit)
	resp, err := http.Get(reqURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Scan failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	var pairs []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pairs); err != nil {
		fmt.Fprintf(os.Stderr, "Failed decoding scan results: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Scanned %d keys:\n", len(pairs))
	for _, p := range pairs {
		fmt.Printf("  %-25s => %s\n", p.Key, p.Value)
	}
}

func handleStats(args []string) {
	fs := flag.NewFlagSet("stats", flag.ExitOnError)
	serverURL := fs.String("server", defaultServerURL(), "Server base URL")
	_ = fs.Parse(args)

	resp, err := http.Get(*serverURL + "/metrics")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Stats request failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Println("=== LSM Engine Telemetry Metrics ===")
	fmt.Println(string(body))
}

func handleFlush(args []string) {
	fs := flag.NewFlagSet("flush", flag.ExitOnError)
	serverURL := fs.String("server", defaultServerURL(), "Server base URL")
	_ = fs.Parse(args)

	resp, err := http.Post(*serverURL+"/api/v1/db/flush", "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Flush failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	fmt.Println("Flush completed successfully")
}

func handleCompact(args []string) {
	fs := flag.NewFlagSet("compact", flag.ExitOnError)
	serverURL := fs.String("server", defaultServerURL(), "Server base URL")
	_ = fs.Parse(args)

	resp, err := http.Post(*serverURL+"/api/v1/db/compact", "application/json", nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Compact failed: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	fmt.Println("Compaction triggered successfully")
}
