package lb

import (
	"context"
	"fmt"
	"math"
	"net/http/httptest"
	"strconv"
	"testing"
)

func TestCHBL_SameKeyLandsOnSameUpstream(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 5)
	ch := NewConsistentHashBounded(pool, 128, 0.25, HeaderKey("X-Tenant"))

	first := pickWithHeader(t, ch, "X-Tenant", "tenant-A")

	for i := 0; i < 100; i++ {
		got := pickWithHeader(t, ch, "X-Tenant", "tenant-A")
		if got != first {
			t.Fatalf("same key landed on different upstream: %v vs %v (iter %d)", first.URL, got.URL, i)
		}
	}
}

func TestCHBL_DifferentKeysSpreadAcrossPool(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	ch := NewConsistentHashBounded(pool, 128, 0.25, HeaderKey("X-Key"))

	picks := make(map[*Upstream]bool)
	for i := 0; i < 1000; i++ {
		got := pickWithHeader(t, ch, "X-Key", strconv.Itoa(i))
		picks[got] = true
		if len(picks) == len(pool) {
			break
		}
	}

	if len(picks) < len(pool) {
		t.Errorf("only %d of %d upstreams received any key; CH appears degenerate", len(picks), len(pool))
	}
}

// CH-BL with uniform keys should spread roughly evenly. The tolerance is
// generous (30%) because virtual-node placement is inherently bumpy at
// modest virtual-node counts.
func TestCHBL_DistributionRoughlyUniform(t *testing.T) {
	t.Parallel()

	const (
		nUpstreams = 10
		nKeys      = 20_000
		tolerance  = 0.30
	)

	pool := testPool(t, nUpstreams)
	ch := NewConsistentHashBounded(pool, 256, 0.25, HeaderKey("X-Key"))

	counts := make(map[*Upstream]int)
	for i := 0; i < nKeys; i++ {
		u := pickWithHeader(t, ch, "X-Key", strconv.Itoa(i))
		counts[u]++
	}

	expected := nKeys / nUpstreams
	for _, u := range pool {
		c := counts[u]
		diff := math.Abs(float64(c-expected)) / float64(expected)
		if diff > tolerance {
			t.Errorf("upstream %s: %d picks (expected ~%d, off by %.1f%%)", u.URL, c, expected, diff*100)
		}
	}
}

// The bounded-loads rule: an upstream may not exceed (1+ε) × meanInflight.
// Drive the same key repeatedly with inflight rising on the chosen upstream;
// the algorithm must redirect to a different upstream once the cap is hit.
func TestCHBL_BoundedLoadsForcesRedirect(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	ch := NewConsistentHashBounded(pool, 128, 0.25, HeaderKey("X-Key"))

	const key = "hot-key"
	first := pickWithHeader(t, ch, "X-Key", key)
	first.InFlight.Add(1)

	// Mean inflight is now 1/3 ≈ 0.33. Cap = ceil((1+0.25) × 0.33) = ceil(0.42) = 1.
	// First upstream is at its cap; next pick of the same key must walk past.
	second := pickWithHeader(t, ch, "X-Key", key)
	if second == first {
		t.Errorf("expected walk past saturated upstream; got same upstream %s (inflight=%d)",
			first.URL, first.InFlight.Load())
	}
}

// NOTE: A test exercising the "ring fully walked without finding a candidate
// under the cap" fallback path is omitted intentionally. With the cap derived
// from the eligible-set mean (cap = ⌈(1+ε)·mean⌉), it is arithmetically
// impossible for every upstream to exceed the cap simultaneously — at least
// one upstream's inflight is always ≤ mean ≤ cap. The fallback is defensive
// coding for pathological states (e.g., extreme cap underflow during a
// concurrent write storm) that ordinary state cannot produce.

func TestCHBL_FallbackToClientIPWhenHeaderMissing(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	ch := NewConsistentHashBounded(pool, 128, 0.25, HeaderKey("X-Missing"))

	req1 := httptest.NewRequest("GET", "/", nil)
	req1.RemoteAddr = "203.0.113.10:11111"
	req2 := httptest.NewRequest("GET", "/", nil)
	req2.RemoteAddr = "203.0.113.10:22222" // same IP, different port

	u1, err := ch.Pick(context.Background(), req1, PickHint{})
	if err != nil {
		t.Fatal(err)
	}
	u2, err := ch.Pick(context.Background(), req2, PickHint{})
	if err != nil {
		t.Fatal(err)
	}

	if u1 != u2 {
		t.Errorf("same client IP landed on different upstreams (header fallback broken): %s vs %s", u1.URL, u2.URL)
	}
}

func TestCHBL_NoEligibleReturnsError(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 3)
	for _, u := range pool {
		u.Eligible.Store(false)
	}

	ch := NewConsistentHashBounded(pool, 128, 0.25, HeaderKey("X-Key"))
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Key", "k")

	_, err := ch.Pick(context.Background(), req, PickHint{})
	if err != ErrNoEligibleUpstream {
		t.Errorf("err = %v, want ErrNoEligibleUpstream", err)
	}
}

func TestCHBL_ExcludeHintAdvancesRingWalk(t *testing.T) {
	t.Parallel()

	pool := testPool(t, 4)
	ch := NewConsistentHashBounded(pool, 128, 0.25, HeaderKey("X-Key"))

	const key = "shard-key"
	first := pickWithHeader(t, ch, "X-Key", key)

	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set("X-Key", key)
	got, err := ch.Pick(context.Background(), req, PickHint{Exclude: []*Upstream{first}})
	if err != nil {
		t.Fatal(err)
	}
	if got == first {
		t.Errorf("exclude hint ignored: got excluded upstream %s", first.URL)
	}
}

func TestCHBL_StabilityAcrossVirtualNodeCounts(t *testing.T) {
	t.Parallel()

	// With more virtual nodes per upstream, distribution variance decreases.
	// This is a sanity test for the construction — both configurations should
	// still produce a valid pick.
	pool := testPool(t, 5)
	for _, vnodes := range []int{1, 16, 128, 512} {
		t.Run(fmt.Sprintf("vnodes=%d", vnodes), func(t *testing.T) {
			ch := NewConsistentHashBounded(pool, vnodes, 0.25, HeaderKey("X-Key"))
			u := pickWithHeader(t, ch, "X-Key", "anything")
			if u == nil {
				t.Fatal("got nil upstream")
			}
		})
	}
}

// helper
func pickWithHeader(t *testing.T, ch Selector, header, value string) *Upstream {
	t.Helper()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(header, value)
	u, err := ch.Pick(context.Background(), req, PickHint{})
	if err != nil {
		t.Fatalf("Pick: %v", err)
	}
	return u
}
