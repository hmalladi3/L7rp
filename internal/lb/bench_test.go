package lb

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"
)

// Selector benchmarks measure the per-Pick cost of each algorithm. They share
// a pool of 8 eligible upstreams with default circuit breakers, which is a
// reasonable production point on the curve (most pools are 2–16 upstreams).
//
// Run with: go test -bench=. -benchmem -benchtime=2s ./internal/lb

func benchPool(b *testing.B, n int) []*Upstream {
	b.Helper()
	pool := make([]*Upstream, n)
	for i := 0; i < n; i++ {
		u := mustParseURL(b, "http://upstream-"+strconv.Itoa(i))
		pool[i] = &Upstream{
			URL:     u,
			Weight:  1,
			Breaker: NewCircuitBreaker(DefaultCircuitBreakerConfig()),
		}
		pool[i].Eligible.Store(true)
	}
	return pool
}

func BenchmarkRoundRobin_Pick(b *testing.B) {
	pool := benchPool(b, 8)
	sel := NewRoundRobin(pool)
	req := httptest.NewRequest("GET", "/", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sel.Pick(context.Background(), req, PickHint{})
	}
}

func BenchmarkWeightedRR_Pick(b *testing.B) {
	pool := benchPool(b, 8)
	for i, u := range pool {
		u.Weight = i + 1
	}
	sel := NewWeightedRoundRobin(pool)
	req := httptest.NewRequest("GET", "/", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sel.Pick(context.Background(), req, PickHint{})
	}
}

func BenchmarkLeastConnections_Pick(b *testing.B) {
	pool := benchPool(b, 8)
	for i, u := range pool {
		u.InFlight.Store(int64(i)) // varied so there's no tiebreak
	}
	sel := NewLeastConnections(pool)
	req := httptest.NewRequest("GET", "/", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sel.Pick(context.Background(), req, PickHint{})
	}
}

func BenchmarkP2C_Pick(b *testing.B) {
	pool := benchPool(b, 8)
	for i, u := range pool {
		u.LatencyEWMA.Store(uint64((i + 1) * int(time.Millisecond)))
	}
	sel := NewP2C(pool, 5*time.Second)
	req := httptest.NewRequest("GET", "/", nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sel.Pick(context.Background(), req, PickHint{})
	}
}

func BenchmarkConsistentHashBounded_Pick(b *testing.B) {
	pool := benchPool(b, 8)
	sel := NewConsistentHashBounded(pool, 128, 0.25, HeaderKey("X-Key"))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Key", "tenant-1234")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = sel.Pick(context.Background(), req, PickHint{})
	}
}

func BenchmarkCircuitBreaker_State_Closed(b *testing.B) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cb.State()
	}
}

func BenchmarkCircuitBreaker_Record_Closed(b *testing.B) {
	cb := NewCircuitBreaker(DefaultCircuitBreakerConfig())
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cb.Record(OutcomeSuccess)
	}
}

func mustParseURL(tb testing.TB, raw string) *url.URL {
	tb.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		tb.Fatalf("parse %q: %v", raw, err)
	}
	return u
}
