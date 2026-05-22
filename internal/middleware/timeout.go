package middleware

import (
	"context"
	"net/http"
	"time"
)

// Timeout wraps incoming requests in a derived context with the given
// deadline. When the deadline fires, downstream handlers see their context
// cancel and unwind; the upstream proxy in the terminal handler surfaces a
// 502 to the client.
//
// A zero or negative timeout disables the middleware (no-op).
func Timeout(total time.Duration) Middleware {
	if total <= 0 {
		return func(next http.Handler) http.Handler { return next }
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), total)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
