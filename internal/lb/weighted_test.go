package lb

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestWRR_RatioFollowsWeights(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	pool[0].Weight = 5
	pool[1].Weight = 1
	pool[2].Weight = 1

	wrr := NewWeightedRoundRobin(pool)
	req := httptest.NewRequest("GET", "/", nil)

	counts := make(map[*Upstream]int)
	const picks = 7000 // multiple of total weight (7) for exact distribution
	for i := 0; i < picks; i++ {
		u, err := wrr.Pick(context.Background(), req, PickHint{})
		if err != nil {
			t.Fatal(err)
		}
		counts[u]++
	}

	want := map[*Upstream]int{
		pool[0]: 5000,
		pool[1]: 1000,
		pool[2]: 1000,
	}
	for u, expected := range want {
		if counts[u] != expected {
			t.Errorf("upstream weight=%d: %d picks, want exactly %d (smooth WRR is deterministic)",
				u.Weight, counts[u], expected)
		}
	}
}

// Smooth WRR interleaves picks rather than emitting bursts of the same
// upstream. With weights 5:1:1, expect each non-heavy upstream to appear no
// later than every 7 picks. This is what distinguishes smooth WRR from naive
// weighted expansion.
func TestWRR_InterleavesNotBursts(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	pool[0].Weight = 5
	pool[1].Weight = 1
	pool[2].Weight = 1

	wrr := NewWeightedRoundRobin(pool)
	req := httptest.NewRequest("GET", "/", nil)

	// Find the longest consecutive run of the heavy upstream. Smooth WRR
	// guarantees runs no longer than ceil(weight_heavy / 1) — strictly less
	// than the total weight. A naive expansion would produce a run of 5.
	var (
		lastPick *Upstream
		runLen   int
		maxRun   int
	)
	for i := 0; i < 1000; i++ {
		u, _ := wrr.Pick(context.Background(), req, PickHint{})
		if u == lastPick {
			runLen++
			if runLen > maxRun {
				maxRun = runLen
			}
		} else {
			runLen = 1
			lastPick = u
		}
	}
	if maxRun >= 5 {
		t.Errorf("max consecutive run = %d; smooth WRR should not produce naive bursts of length ≥ heavy weight", maxRun)
	}
}

func TestWRR_SkipsZeroWeight(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	pool[0].Weight = 1
	pool[1].Weight = 0 // skipped
	pool[2].Weight = 1

	wrr := NewWeightedRoundRobin(pool)
	req := httptest.NewRequest("GET", "/", nil)

	for i := 0; i < 200; i++ {
		u, _ := wrr.Pick(context.Background(), req, PickHint{})
		if u == pool[1] {
			t.Errorf("picked zero-weight upstream at iteration %d", i)
		}
	}
}

func TestWRR_NoEligibleReturnsError(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 2)
	for _, u := range pool {
		u.Eligible.Store(false)
	}
	wrr := NewWeightedRoundRobin(pool)
	_, err := wrr.Pick(context.Background(), httptest.NewRequest("GET", "/", nil), PickHint{})
	if err != ErrNoEligibleUpstream {
		t.Errorf("err = %v, want ErrNoEligibleUpstream", err)
	}
}
