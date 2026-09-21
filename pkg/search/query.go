package search

// Query encapsulates search filters and pagination parameters.
type Query struct {
	Text     string            `json:"text,omitempty"`      // Free-text query terms (AND match)
	Level    string            `json:"level,omitempty"`     // Exact log level filter (e.g. ERROR)
	Service  string            `json:"service,omitempty"`   // Filter by service name
	Host     string            `json:"host,omitempty"`      // Filter by host
	FromTime int64             `json:"from_time,omitempty"` // Start Unix timestamp in nanoseconds
	ToTime   int64             `json:"to_time,omitempty"`   // End Unix timestamp in nanoseconds
	Fields   map[string]string `json:"fields,omitempty"`    // Custom key-value field filters
	Limit    int               `json:"limit,omitempty"`     // Max results to return
	Offset   int               `json:"offset,omitempty"`    // Number of matches to skip
}

// SearchResult holds the output of a query execution.
type SearchResult struct {
	TotalHits int64        `json:"total_hits"`
	TookMs    float64      `json:"took_ms"`
	Logs      []*LogRecord `json:"logs"`
}
