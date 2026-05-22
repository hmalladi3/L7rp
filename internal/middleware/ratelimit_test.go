package middleware

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"
)

func TestRateLimit_AllowsWithinBurst(t *testing.T) {
	t.Parallel()

	handler := RateLimit(RateLimitConfig{RPS: 5, Burst: 5})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "1.2.3.4:5678"
		handler.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Errorf("burst request %d rejected (status %d)", i+1, w.Code)
		}
	}
}

func TestRateLimit_BlocksOverBurst(t *testing.T) {
	t.Parallel()

	handler := RateLimit(RateLimitConfig{RPS: 1, Burst: 2})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	makeReq := func() *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = "1.2.3.4:5678"
		handler.ServeHTTP(w, r)
		return w
	}

	// Burst of 2 admits.
	if c := makeReq().Code; c != 200 {
		t.Fatalf("1st request: status = %d, want 200", c)
	}
	if c := makeReq().Code; c != 200 {
		t.Fatalf("2nd request: status = %d, want 200", c)
	}
	// 3rd is rejected.
	rejected := makeReq()
	if rejected.Code != http.StatusTooManyRequests {
		t.Errorf("3rd request: status = %d, want 429", rejected.Code)
	}
	if rejected.Header().Get("Retry-After") == "" {
		t.Error("rejected response missing Retry-After header")
	}
	if got, _ := strconv.Atoi(rejected.Header().Get("Retry-After")); got <= 0 {
		t.Errorf("Retry-After = %q, want positive integer", rejected.Header().Get("Retry-After"))
	}
}

func TestRateLimit_RefillsOverTime(t *testing.T) {
	t.Parallel()

	bm := newBucketMap(10, 2) // 10 tokens/s, burst 2

	// Drain burst at t=0.
	now := time.Now()
	allowed, _ := bm.allow("k", now)
	if !allowed {
		t.Fatal("burst 1 should admit")
	}
	allowed, _ = bm.allow("k", now)
	if !allowed {
		t.Fatal("burst 2 should admit")
	}

	// Immediate 3rd is blocked.
	allowed, _ = bm.allow("k", now)
	if allowed {
		t.Fatal("3rd at t=0 should block")
	}

	// 200ms later: 10 RPS × 0.2s = 2 tokens refilled.
	later := now.Add(200 * time.Millisecond)
	allowed, _ = bm.allow("k", later)
	if !allowed {
		t.Error("after 200ms (2 tokens refilled), request should admit")
	}
}

func TestRateLimit_PerIPScope(t *testing.T) {
	t.Parallel()

	handler := RateLimit(RateLimitConfig{RPS: 1, Burst: 1, Scope: "per-ip"})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))

	// Two distinct client IPs each get their own bucket.
	for _, ip := range []string{"1.1.1.1:1", "2.2.2.2:1"} {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = ip
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != 200 {
			t.Errorf("ip %s: first request rejected (status %d)", ip, w.Code)
		}
	}

	// Each IP's second request is blocked.
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "1.1.1.1:1"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 429 {
		t.Errorf("second request from same IP: status = %d, want 429", w.Code)
	}
}

func TestRateLimit_PerRouteScope(t *testing.T) {
	t.Parallel()

	handler := RateLimit(RateLimitConfig{
		Scope:     "per-route",
		RPS:       1,
		Burst:     1,
		RouteName: "api",
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))

	// Different IPs share the bucket under per-route scope.
	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "1.1.1.1:1"
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 200 {
		t.Fatal("burst should admit")
	}

	r = httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "2.2.2.2:1" // different IP
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Code != 429 {
		t.Errorf("per-route bucket should block second request even from different IP: status = %d", w.Code)
	}
}

func TestRateLimit_TrustProxyHopsHonorsXFF(t *testing.T) {
	t.Parallel()

	cases := []struct {
		hops   int
		xff    string
		remote string
		want   string
	}{
		// trust=0: ignore XFF, use remote
		{0, "10.0.0.1, 192.168.1.1", "203.0.113.1:1234", "203.0.113.1"},
		// trust=1: take last XFF entry (rightmost, set by the immediately upstream proxy)
		{1, "10.0.0.1, 192.168.1.1", "203.0.113.1:1234", "192.168.1.1"},
		// trust=2: walk back 2
		{2, "10.0.0.1, 192.168.1.1", "203.0.113.1:1234", "10.0.0.1"},
		// trust=99: clamps to first entry
		{99, "10.0.0.1, 192.168.1.1", "203.0.113.1:1234", "10.0.0.1"},
	}
	for _, c := range cases {
		r := httptest.NewRequest("GET", "/", nil)
		r.RemoteAddr = c.remote
		r.Header.Set("X-Forwarded-For", c.xff)
		got := clientIPWithTrust(r, c.hops)
		if got != c.want {
			t.Errorf("hops=%d xff=%q remote=%q → %q, want %q", c.hops, c.xff, c.remote, got, c.want)
		}
	}
}

func TestRateLimit_ZeroRPSIsPassThrough(t *testing.T) {
	t.Parallel()

	called := false
	handler := RateLimit(RateLimitConfig{RPS: 0})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(200)
	}))

	for i := 0; i < 100; i++ {
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
	}
	if !called {
		t.Error("zero-RPS rate limit should be a pass-through (no enforcement)")
	}
}
