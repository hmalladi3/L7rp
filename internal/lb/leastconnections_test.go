package lb

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestLC_PicksLowestInflight(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	pool[0].InFlight.Store(5)
	pool[1].InFlight.Store(2)
	pool[2].InFlight.Store(8)

	lc := NewLeastConnections(pool)
	req := httptest.NewRequest("GET", "/", nil)

	for i := 0; i < 100; i++ {
		u, _ := lc.Pick(context.Background(), req, PickHint{})
		if u != pool[1] {
			t.Fatalf("iter %d: picked %s (inflight=%d), want pool[1] (inflight=2)",
				i, u.URL, u.InFlight.Load())
		}
	}
}

func TestLC_TieBreakIsRandom(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	// All same inflight → all tied.
	for _, u := range pool {
		u.InFlight.Store(1)
	}

	lc := NewLeastConnections(pool)
	req := httptest.NewRequest("GET", "/", nil)

	counts := make(map[*Upstream]int)
	for i := 0; i < 3000; i++ {
		u, _ := lc.Pick(context.Background(), req, PickHint{})
		counts[u]++
	}

	// Each of 3 tied upstreams should get roughly 1000 picks.
	for _, u := range pool {
		if counts[u] < 800 || counts[u] > 1200 {
			t.Errorf("upstream %s: %d picks; expected ~1000 ±200 (uniform random tie-break)", u.URL, counts[u])
		}
	}
}

func TestLC_SkipsIneligible(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	pool[0].InFlight.Store(1) // lowest, but ineligible
	pool[0].Eligible.Store(false)
	pool[1].InFlight.Store(5)
	pool[2].InFlight.Store(3)

	lc := NewLeastConnections(pool)
	req := httptest.NewRequest("GET", "/", nil)

	for i := 0; i < 50; i++ {
		u, _ := lc.Pick(context.Background(), req, PickHint{})
		if u != pool[2] {
			t.Fatalf("iter %d: picked %s, want pool[2] (lowest eligible)", i, u.URL)
		}
	}
}

func TestLC_NoEligibleReturnsError(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 2)
	for _, u := range pool {
		u.Eligible.Store(false)
	}
	lc := NewLeastConnections(pool)
	_, err := lc.Pick(context.Background(), httptest.NewRequest("GET", "/", nil), PickHint{})
	if err != ErrNoEligibleUpstream {
		t.Errorf("err = %v, want ErrNoEligibleUpstream", err)
	}
}
