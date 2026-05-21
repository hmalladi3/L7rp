// Package middleware defines the request-processing decorators that wrap each
// route's terminal upstream-proxy handler. The Middleware type is the canonical
// Go decorator signature; chains are composed at config-load time and held as
// immutable per-route handlers in the *Config.
package middleware

import "net/http"

// Middleware is the canonical Go middleware type — a decorator that wraps the
// next handler and returns a new handler. Each route's chain is a slice of
// these, composed into a single http.Handler at config-load time.
type Middleware func(next http.Handler) http.Handler

// Chain composes a list of middlewares into a single decorator, applying them
// in order: the first middleware in the slice is the outermost in the chain,
// the last middleware wraps the terminal handler. Calling the returned
// Middleware with the terminal handler produces the per-route handler.
func Chain(mws ...Middleware) Middleware {
	return func(next http.Handler) http.Handler {
		for i := len(mws) - 1; i >= 0; i-- {
			next = mws[i](next)
		}
		return next
	}
}
