package server

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/engine/lsm-trees/pkg/lsm"
	"github.com/engine/lsm-trees/pkg/search"
)

// Server encapsulates the HTTP API and routing logic.
type Server struct {
	httpServer *http.Server
	logger     *slog.Logger
	db         *lsm.Engine
	search     *search.SearchEngine
}

// Config holds web server options.
type Config struct {
	Addr         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewServer configures and initializes a new HTTP server.
func NewServer(cfg Config, db *lsm.Engine, s *search.SearchEngine, logger *slog.Logger) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":8080"
	}
	if cfg.ReadTimeout <= 0 {
		cfg.ReadTimeout = 10 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}

	h := NewAPIHandler(db, s)
	mux := http.NewServeMux()

	// Log ingestion & search endpoints
	mux.HandleFunc("/api/v1/logs/ingest", h.handleIngest)
	mux.HandleFunc("/api/v1/logs/search", h.handleSearch)
	mux.HandleFunc("/api/v1/logs/aggregate", h.handleAggregate)
	mux.HandleFunc("/api/v1/logs/", h.handleGetLog)

	// Low-level KV store endpoints
	mux.HandleFunc("/api/v1/kv/scan", h.handleKVScan)
	mux.HandleFunc("/api/v1/kv/", h.handleKV)

	// Admin / Maintenance endpoints
	mux.HandleFunc("/api/v1/db/flush", h.handleFlush)
	mux.HandleFunc("/api/v1/db/compact", h.handleCompact)

	// Observability & Health endpoints
	mux.HandleFunc("/metrics", h.handleMetrics)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})

	loggedHandler := LoggingMiddleware(logger)(mux)

	return &Server{
		httpServer: &http.Server{
			Addr:         cfg.Addr,
			Handler:      loggedHandler,
			ReadTimeout:  cfg.ReadTimeout,
			WriteTimeout: cfg.WriteTimeout,
		},
		logger: logger,
		db:     db,
		search: s,
	}
}

// Start begins listening on the configured network address.
func (s *Server) Start() error {
	s.logger.Info("starting lsm log aggregator server", "addr", s.httpServer.Addr)
	if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server failed: %w", err)
	}
	return nil
}

// Shutdown gracefully shuts down the HTTP server.
func (s *Server) Shutdown(ctx context.Context) error {
	s.logger.Info("shutting down http server")
	return s.httpServer.Shutdown(ctx)
}
