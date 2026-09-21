package search

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// LogRecord represents a structured enterprise log entry.
type LogRecord struct {
	ID        string            `json:"id"`
	Timestamp int64             `json:"timestamp"` // Unix timestamp in nanoseconds
	Level     string            `json:"level"`     // DEBUG, INFO, WARN, ERROR, FATAL
	Service   string            `json:"service"`   // Service / microservice name
	Host      string            `json:"host,omitempty"`
	Message   string            `json:"message"`
	Fields    map[string]string `json:"fields,omitempty"`
}

// GenerateID produces a random 16-byte hex unique identifier.
func GenerateID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Validate checks required fields and initializes defaults if absent.
func (l *LogRecord) Validate() error {
	if l.ID == "" {
		l.ID = GenerateID()
	}
	if l.Timestamp <= 0 {
		l.Timestamp = time.Now().UnixNano()
	}
	if l.Level == "" {
		l.Level = "INFO"
	}
	l.Level = strings.ToUpper(strings.TrimSpace(l.Level))
	if l.Message == "" {
		return fmt.Errorf("log message cannot be empty")
	}
	if l.Service == "" {
		l.Service = "default"
	}
	return nil
}

// ToJSON serializes the log record to JSON bytes.
func (l *LogRecord) ToJSON() ([]byte, error) {
	return json.Marshal(l)
}

// ParseLogRecord deserializes a log record from JSON bytes.
func ParseLogRecord(data []byte) (*LogRecord, error) {
	var l LogRecord
	if err := json.Unmarshal(data, &l); err != nil {
		return nil, err
	}
	if err := l.Validate(); err != nil {
		return nil, err
	}
	return &l, nil
}
