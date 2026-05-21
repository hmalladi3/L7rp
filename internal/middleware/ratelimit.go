package middleware

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// RateLimitConfig parameterizes the RateLimit middleware. Operators choose a
// scope (per-ip or per-route) plus a sustained `rps` and a `burst` (token
// bucket capacity).
type RateLimitConfig struct {
	Scope          string // "per-ip" (default) | "per-route"
	RPS            int    // sustained rate; must be > 0
	Burst          int    // bucket capacity; defaults to RPS when 0
	TrustProxyHops int    // honor XFF this many hops back; 0 = ignore XFF
	RouteName      string // used by per-route scope; usually injected by the chain builder
}

// RateLimit returns a token-bucket rate-limit middleware. Buckets are keyed
// per `(scope, key)`; the key is either the client IP (with optional XFF
// trust) or the route name for global per-route limits.
//
// A request without an available token receives 429 with `Retry-After` set to
// the seconds until the next token refills. Idle buckets are reaped on a
// background goroutine to bound memory use under cycling client IPs.
func RateLimit(cfg RateLimitConfig) Middleware {
	if cfg.RPS <= 0 {
		// Misconfiguration; route-level validation rejects this upstream.
		// Here we no-op rather than crash.
		return func(next http.Handler) http.Handler { return next }
	}
	burst := cfg.Burst
	if burst <= 0 {
		burst = cfg.RPS
	}
	bm := newBucketMap(float64(cfg.RPS), float64(burst))

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := bucketKey(cfg, r)
			allowed, retryAfter := bm.allow(key, time.Now())
			if !allowed {
				secs := int(retryAfter.Seconds()) + 1
				w.Header().Set("Retry-After", strconv.Itoa(secs))
				w.Header().Set("X-RateLimit-Remaining", "0")
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

// bucketMap stores token-bucket state per key. Reads and writes serialize on
// a single mutex — at hundreds of RPS this is not the bottleneck; if it ever
// becomes one, sharding by key hash is the standard fix.
type bucketMap struct {
	rps, burst float64

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens   float64
	lastFill time.Time
}

func newBucketMap(rps, burst float64) *bucketMap {
	bm := &bucketMap{
		rps:     rps,
		burst:   burst,
		buckets: make(map[string]*bucket),
	}
	return bm
}

// allow returns (true, 0) when the request can pass and consumes one token;
// returns (false, retryAfter) when the bucket is empty.
func (bm *bucketMap) allow(key string, now time.Time) (bool, time.Duration) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	b, ok := bm.buckets[key]
	if !ok {
		b = &bucket{tokens: bm.burst, lastFill: now}
		bm.buckets[key] = b
	} else {
		elapsed := now.Sub(b.lastFill).Seconds()
		if elapsed > 0 {
			b.tokens += elapsed * bm.rps
			if b.tokens > bm.burst {
				b.tokens = bm.burst
			}
			b.lastFill = now
		}
	}

	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	needed := 1 - b.tokens
	return false, time.Duration(needed / bm.rps * float64(time.Second))
}

// bucketKey produces the lookup key according to the rate limit's scope.
func bucketKey(cfg RateLimitConfig, r *http.Request) string {
	if cfg.Scope == "per-route" {
		name := cfg.RouteName
		if name == "" {
			name = "default"
		}
		return "r:" + name
	}
	return "ip:" + clientIPWithTrust(r, cfg.TrustProxyHops)
}

// clientIPWithTrust extracts the client IP, honoring X-Forwarded-For when the
// operator has explicitly configured trust hops. This matches what nginx does
// for `set_real_ip_from` + `real_ip_recursive`: walk left from the end of the
// XFF chain by `hops` positions.
func clientIPWithTrust(r *http.Request, hops int) string {
	if hops > 0 {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			parts := strings.Split(xff, ",")
			idx := len(parts) - hops
			if idx < 0 {
				idx = 0
			}
			if idx < len(parts) {
				return strings.TrimSpace(parts[idx])
			}
		}
	}
	return clientIP(r)
}
