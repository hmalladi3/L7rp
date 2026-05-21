package lb

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestRR_DistributesEvenly(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 4)
	rr := NewRoundRobin(pool)
	req := httptest.NewRequest("GET", "/", nil)

	counts := make(map[*Upstream]int)
	for i := 0; i < 4000; i++ {
		u, err := rr.Pick(context.Background(), req, PickHint{})
		if err != nil {
			t.Fatal(err)
		}
		counts[u]++
	}

	for _, u := range pool {
		if counts[u] != 1000 {
			t.Errorf("upstream %s: %d picks, want exactly 1000", u.URL, counts[u])
		}
	}
}

func TestRR_SkipsIneligible(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	pool[1].Eligible.Store(false)

	rr := NewRoundRobin(pool)
	req := httptest.NewRequest("GET", "/", nil)

	for i := 0; i < 100; i++ {
		u, _ := rr.Pick(context.Background(), req, PickHint{})
		if u == pool[1] {
			t.Errorf("picked ineligible upstream at iteration %d", i)
		}
	}
}

func TestRR_NoEligibleReturnsError(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 2)
	for _, u := range pool {
		u.Eligible.Store(false)
	}
	rr := NewRoundRobin(pool)

	_, err := rr.Pick(context.Background(), httptest.NewRequest("GET", "/", nil), PickHint{})
	if err != ErrNoEligibleUpstream {
		t.Errorf("err = %v, want ErrNoEligibleUpstream", err)
	}
}

func TestRR_HonorsExcludeHint(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	rr := NewRoundRobin(pool)

	req := httptest.NewRequest("GET", "/", nil)
	for i := 0; i < 100; i++ {
		u, _ := rr.Pick(context.Background(), req, PickHint{Exclude: []*Upstream{pool[0], pool[1]}})
		if u != pool[2] {
			t.Errorf("got %v, want pool[2]", u)
		}
	}
}
