//go:build integration

package integration

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

// TestResilience_HealthMonitorRemovesDeadUpstream kills a backend and
// verifies that the proxy's active probes notice and the upstream is
// removed from selection. With only one upstream in the pool, subsequent
// requests should return 503 (no eligible upstream).
func TestResilience_HealthMonitorRemovesDeadUpstream(t *testing.T) {
	be := startBackend(t, "be1")
	dataPort := freePort(t)
	dataAddr := fmt.Sprintf("127.0.0.1:%d", dataPort)

	cfg := fmt.Sprintf(`
listeners:
  - name: http
    bind: "%s"
upstream_pools:
  - name: backend
    selector: { algorithm: round-robin }
    upstreams:
      - url: %s
    health:
      active:
        path: /healthz
        interval: 100ms
        timeout: 50ms
        healthy_threshold: 2
        unhealthy_threshold: 2
routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: backend
`, dataAddr, be.URL())

	p := startProxy(t, cfg)
	if !waitFor(2*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s == 200
	}) {
		t.Fatalf("backend never reachable initially")
	}

	// Take the backend down (Close stops the listener accepting).
	be.Close()

	// Wait for probes to mark it ineligible. Once that happens, requests
	// should return 503 (no eligible upstream).
	if !waitFor(5*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s == 503
	}) {
		t.Errorf("upstream never marked ineligible after backend died; metrics:\n%s",
			p.metricLine(t, "proxy_health_eligibility_transitions_total"))
	}
}

// TestResilience_BreakerTripsOnSustained5xx pumps 5xx responses through
// the proxy and verifies the per-upstream circuit breaker eventually
// trips, causing the proxy to short-circuit to 503 before reaching the
// backend.
//
// The CB defaults are min_observations=20 and failure_ratio>0.5, so we send
// ~30 requests at 100% 5xx to guarantee a trip.
func TestResilience_BreakerTripsOnSustained5xx(t *testing.T) {
	be := startBackend(t, "be1")
	dataPort := freePort(t)
	dataAddr := fmt.Sprintf("127.0.0.1:%d", dataPort)

	cfg := fmt.Sprintf(`
listeners:
  - name: http
    bind: "%s"
upstream_pools:
  - name: backend
    selector: { algorithm: round-robin }
    upstreams:
      - url: %s
    health:
      active:
        path: /healthz
        interval: 200ms
        timeout: 100ms
        healthy_threshold: 2
        unhealthy_threshold: 50  # don't let active health interfere
routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: backend
`, dataAddr, be.URL())

	p := startProxy(t, cfg)
	waitFor(2*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s == 200
	})

	// Have the backend start returning 500s for non-/healthz.
	be.SetStatus(500)

	// Send enough requests to trip the breaker (min_observations=20, all
	// failing). Count how many returned the upstream's 500 vs the proxy's
	// 503 (short-circuit). After the trip, we should see 503s.
	var got500, got503 int
	for i := 0; i < 60; i++ {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		switch s {
		case 500:
			got500++
		case 503:
			got503++
		}
		time.Sleep(10 * time.Millisecond)
	}

	if got503 == 0 {
		t.Errorf("breaker never short-circuited: got %d × 500, %d × 503; transitions metric: %s",
			got500, got503, p.metricLine(t, "proxy_circuit_breaker_transitions_total"))
	}
}

// TestResilience_PassiveScoringEjectsUpstream pumps errors against a pool with
// two upstreams and verifies that passive scoring eventually marks the
// 5xx-emitter ineligible — traffic concentrates on the healthy one.
func TestResilience_PassiveScoringEjectsUpstream(t *testing.T) {
	be1 := startBackend(t, "be1")
	be2 := startBackend(t, "be2")
	dataPort := freePort(t)
	dataAddr := fmt.Sprintf("127.0.0.1:%d", dataPort)

	cfg := fmt.Sprintf(`
listeners:
  - name: http
    bind: "%s"
upstream_pools:
  - name: backend
    selector: { algorithm: round-robin }
    upstreams:
      - url: %s
      - url: %s
    health:
      active:
        path: /healthz
        interval: 500ms
        timeout: 100ms
        healthy_threshold: 2
        unhealthy_threshold: 50  # disable active for this test
      passive:
        error_threshold: 0.3
        half_life: 200ms
routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: backend
`, dataAddr, be1.URL(), be2.URL())

	p := startProxy(t, cfg)
	waitFor(2*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s == 200
	})

	// Make be2 return errors.
	be2.SetStatus(503)

	// Drive enough traffic to accumulate passive scoring on be2.
	for i := 0; i < 50; i++ {
		curl(t, dataAddr, "localhost", "/")
		time.Sleep(15 * time.Millisecond)
	}

	// After ejection, all traffic should land on be1.
	be1Count := 0
	beOtherCount := 0
	for i := 0; i < 30; i++ {
		_, hdr, _ := curl(t, dataAddr, "localhost", "/")
		switch hdr.Get("X-Backend") {
		case "be1":
			be1Count++
		case "be2":
			beOtherCount++
		}
	}

	if beOtherCount > 5 {
		t.Errorf("be2 received %d/30 requests after passive ejection; want ≈0", beOtherCount)
	}
	if be1Count < 20 {
		// Also acceptable: all requests 503'd if both upstreams got ejected.
		// Check the metric to know which happened.
		line := p.metricLine(t, "proxy_health_eligibility_transitions_total")
		if !strings.Contains(line, "passive") && be1Count < 20 {
			t.Errorf("be1 received %d/30 requests; expected concentrated traffic", be1Count)
		}
	}
}
