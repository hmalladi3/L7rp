package lb

import (
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"
)

// testUpstream constructs an eligible Upstream with a Closed circuit breaker.
func testUpstream(t *testing.T, raw string) *Upstream {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("bad URL %q: %v", raw, err)
	}
	up := &Upstream{
		URL:     u,
		Weight:  1,
		Breaker: NewCircuitBreaker(DefaultCircuitBreakerConfig()),
	}
	up.Eligible.Store(true)
	return up
}

// testPool constructs n eligible upstreams with URLs http://u0 ... http://u{n-1}.
func testPool(t *testing.T, n int) []*Upstream {
	t.Helper()
	pool := make([]*Upstream, n)
	for i := range pool {
		pool[i] = testUpstream(t, fmt.Sprintf("http://u%d", i))
	}
	return pool
}

// mockClock is a goroutine-safe stub clock for tests that need deterministic
// time advancement (notably the circuit-breaker state transitions).
type mockClock struct {
	mu  sync.Mutex
	now time.Time
}

func newMockClock(start time.Time) *mockClock {
	return &mockClock{now: start}
}

func (c *mockClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *mockClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}
