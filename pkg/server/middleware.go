package server

import (
	"log/slog"
	"net/http"
	"time"
)

// LoggingMiddleware logs incoming HTTP requests with method, path, duration, and status code.
func LoggingMiddleware(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, statusCode: http.StatusOK}
			next.ServeHTTP(sw, r)
			duration := time.Since(start)

			logger.Info("http request",
				"method", r.Method,
				"path", r.URL.Path,
				"status", sw.statusCode,
				"duration_ms", float64(duration.Microseconds())/1000.0,
				"remote_addr", r.RemoteAddr,
			)
		})
	}
}

type statusWriter struct {
	http.ResponseWriter
	statusCode int
}

func (w *statusWriter) WriteHeader(code int) {
	w.statusCode = code
	w.ResponseWriter.WriteHeader(code)
}
