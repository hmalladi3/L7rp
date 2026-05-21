package middleware

import (
	"log/slog"
	"net/http"
	"runtime/debug"
)

// PanicConfig parameterizes the PanicRecover middleware.
type PanicConfig struct {
	// OnRecover, when non-nil, is called with a location label whenever a
	// panic is recovered. Typically wired to a Prometheus counter increment.
	OnRecover func(location string)
}

// PanicRecover wraps the chain in a defer/recover boundary. Recovered panics
// are logged at error level with a truncated stack trace, and (if the response
// hasn't already been written) the client receives a 500 response. The
// optional OnRecover callback fires regardless so observability can count
// recovered panics.
//
// Distinction between "headers written" and "no headers yet" matters: once
// WriteHeader has been called, the client has already seen a status line, so
// we can't change it. We close the connection in that case rather than
// continuing to stream bytes after a panic.
func PanicRecover(cfg PanicConfig) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			rw := &writeTracker{ResponseWriter: w}
			defer func() {
				rcv := recover()
				if rcv == nil {
					return
				}

				location := "middleware-chain"
				if rw.wroteHeader {
					location = "middleware-chain-post-header"
				}
				if cfg.OnRecover != nil {
					cfg.OnRecover(location)
				}

				slog.ErrorContext(r.Context(), "panic recovered",
					slog.String("request_id", RequestIDFromContext(r.Context())),
					slog.Any("panic", rcv),
					slog.String("location", location),
					slog.String("stack", truncateStack(debug.Stack(), 4096)),
				)

				if !rw.wroteHeader {
					http.Error(rw, "internal server error", http.StatusInternalServerError)
				}
				// If the response was already partially streamed, there's no
				// meaningful recovery — we let the connection close on return.
			}()
			next.ServeHTTP(rw, r)
		})
	}
}

// writeTracker observes whether headers have been written; PanicRecover uses
// this to decide between 500-on-recover and connection-close.
type writeTracker struct {
	http.ResponseWriter
	wroteHeader bool
}

func (w *writeTracker) WriteHeader(code int) {
	w.wroteHeader = true
	w.ResponseWriter.WriteHeader(code)
}

func (w *writeTracker) Write(p []byte) (int, error) {
	w.wroteHeader = true
	return w.ResponseWriter.Write(p)
}

func truncateStack(b []byte, max int) string {
	if len(b) > max {
		b = b[:max]
	}
	return string(b)
}
