package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/engine/lsm-trees/pkg/lsm"
	"github.com/engine/lsm-trees/pkg/search"
	"github.com/engine/lsm-trees/pkg/server"
	"github.com/engine/lsm-trees/pkg/wal"
)

func main() {
	var (
		dataDir     = flag.String("data-dir", "./data", "Directory to store LSM database files")
		port        = flag.Int("port", 8080, "Port for the HTTP API server")
		memSizeMB   = flag.Int("mem-size-mb", 4, "MemTable threshold size in megabytes")
		syncPolicy  = flag.String("sync-policy", "always", "WAL sync policy: always, batch, none")
		autoCompact = flag.Bool("auto-compact", true, "Enable background leveled compaction")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	logger.Info("starting LSM-Tree Log Aggregator & Search Engine daemon",
		"data_dir", *dataDir,
		"port", *port,
		"mem_size_mb", *memSizeMB,
		"sync_policy", *syncPolicy,
		"auto_compact", *autoCompact,
	)

	// Resolve WAL sync policy
	var sp wal.SyncPolicy
	switch strings.ToLower(*syncPolicy) {
	case "batch":
		sp = wal.SyncBatch
	case "none":
		sp = wal.SyncNone
	default:
		sp = wal.SyncAlways
	}

	opts := lsm.DefaultOptions(filepath.Clean(*dataDir))
	opts.MemTableSize = int64(*memSizeMB) * 1024 * 1024
	opts.SyncPolicy = sp
	opts.AutoCompaction = *autoCompact

	// Open LSM core engine
	db, err := lsm.Open(opts)
	if err != nil {
		logger.Error("failed to open LSM engine", "error", err)
		os.Exit(1)
	}

	// Open Search & Aggregator layer
	searchEngine := search.Open(db)

	// Initialize API server
	srv := server.NewServer(
		server.Config{
			Addr:         fmt.Sprintf(":%d", *port),
			ReadTimeout:  10 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		db,
		searchEngine,
		logger,
	)

	// Start server in background
	serverErrChan := make(chan error, 1)
	go func() {
		if err := srv.Start(); err != nil {
			serverErrChan <- err
		}
	}()

	// Listen for termination signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		logger.Info("received termination signal", "signal", sig.String())
	case err := <-serverErrChan:
		logger.Error("server encountered fatal error", "error", err)
	}

	// Graceful shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error("http server graceful shutdown error", "error", err)
	}

	logger.Info("closing LSM-Tree database engine...")
	if err := db.Close(); err != nil {
		logger.Error("error closing LSM engine", "error", err)
	}

	logger.Info("daemon shutdown complete")
}
