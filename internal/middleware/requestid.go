package middleware

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"net/http"
)

// RequestID is the outer-most-relevant middleware: every request gets a
// stable identifier that downstream middlewares and logs can correlate on.
// If the inbound request already carries an `X-Request-ID`, we honor it;
// otherwise we generate a fresh 64-bit identifier from crypto/rand and emit
// it as a base32-encoded string (13 characters, no padding — short enough to
// fit in log lines without dominating them).
//
// The request ID is installed on the request context for downstream readers
// (access log, upstream proxy, tracing) and copied to the response headers
// so the client can correlate the same request in its own logs.
func RequestID() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := r.Header.Get("X-Request-ID")
			if id == "" {
				id = newRequestID()
			}
			ctx := context.WithValue(r.Context(), requestIDKey{}, id)
			w.Header().Set("X-Request-ID", id)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

type requestIDKey struct{}

// RequestIDFromContext returns the request ID installed by the RequestID
// middleware, or an empty string when no such middleware ran upstream.
func RequestIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(requestIDKey{}).(string)
	return v
}

// newRequestID generates an 8-byte random identifier and encodes it as base32
// without padding. crypto/rand is used (not math/rand) because the ID may be
// surfaced to the client and we don't want it to be guessable.
func newRequestID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(b[:])
}
