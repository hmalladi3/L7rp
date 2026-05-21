package middleware

import (
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// AccessLog emits one structured slog record per completed request, at a level
// chosen by the response status code:
//
//	2xx, 3xx → info
//	4xx      → warn
//	5xx      → error
//
// The log record carries a documented field set so operators can build stable
// dashboards. Field names are part of the project's public contract; renaming
// one is a breaking change.
//
// The middleware sits inside RequestID (so request_id is available) and
// inside PanicRecover (so a recovered panic still produces an access record
// — it sees status 500 from the recovery).
func AccessLog(routeName string) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &accessRecorder{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rec, r)
			duration := time.Since(start)

			level := slog.LevelInfo
			switch {
			case rec.status >= 500:
				level = slog.LevelError
			case rec.status >= 400:
				level = slog.LevelWarn
			}

			attrs := []slog.Attr{
				slog.String("event", "request_completed"),
				slog.String("route", routeName),
				slog.String("request_id", RequestIDFromContext(r.Context())),
				slog.String("method", r.Method),
				slog.String("host", r.Host),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.status),
				slog.Int64("duration_ms", duration.Milliseconds()),
				slog.Int64("bytes_out", rec.bytes),
				slog.String("client_ip", clientIP(r)),
			}
			if outcome := OutcomeFromContext(r.Context()); outcome != nil && outcome.Chosen != nil {
				attrs = append(attrs, slog.String("upstream", outcome.Chosen.URL.String()))
			}

			slog.LogAttrs(r.Context(), level, "request_completed", attrs...)
		})
	}
}

// accessRecorder is a thin wrapper that captures status + bytes-written without
// buffering the response body — the upstream stream still flows straight to
// the client.
type accessRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (a *accessRecorder) WriteHeader(code int) {
	if !a.wroteHeader {
		a.status = code
		a.wroteHeader = true
	}
	a.ResponseWriter.WriteHeader(code)
}

func (a *accessRecorder) Write(p []byte) (int, error) {
	if !a.wroteHeader {
		a.wroteHeader = true
	}
	n, err := a.ResponseWriter.Write(p)
	a.bytes += int64(n)
	return n, err
}

// clientIP extracts the bare client IP from a request's RemoteAddr. Strips the
// trailing :port. (X-Forwarded-For inspection lives in rate-limit and header-
// transform middlewares, which honor a per-route trust hop count; the access
// log just reports the directly-connected peer.)
func clientIP(r *http.Request) string {
	addr := r.RemoteAddr
	if i := strings.LastIndexByte(addr, ':'); i >= 0 {
		return addr[:i]
	}
	return addr
}
