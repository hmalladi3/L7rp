//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"
)

// TestSmoke_BasicProxy is the simplest end-to-end: one listener, one pool,
// one backend. A request reaches the proxy and gets back the backend's body.
// If this fails, everything else in the suite fails — investigate this first.
func TestSmoke_BasicProxy(t *testing.T) {
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
      active: { path: /healthz, interval: 200ms, timeout: 100ms, healthy_threshold: 2, unhealthy_threshold: 3 }
routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: backend
`, dataAddr, be.URL())

	p := startProxy(t, cfg)

	// Wait for at least one probe to mark the upstream eligible.
	if !waitFor(2*time.Second, func() bool {
		status, _, _ := curl(t, dataAddr, "localhost", "/")
		return status == 200
	}) {
		t.Fatalf("backend never reachable through proxy")
	}

	status, hdr, body := curl(t, dataAddr, "localhost", "/test")
	if status != 200 {
		t.Errorf("status = %d, want 200", status)
	}
	if hdr.Get("X-Backend") != "be1" {
		t.Errorf("X-Backend = %q, want %q", hdr.Get("X-Backend"), "be1")
	}
	if body != "be1" {
		t.Errorf("body = %q, want %q", body, "be1")
	}
	if hdr.Get("X-Request-Id") == "" {
		t.Error("X-Request-Id missing (request-id middleware did not run)")
	}

	// At least one proxy_requests_total counter should have advanced.
	line := p.metricLine(t, "proxy_requests_total")
	if line == "" {
		t.Error("proxy_requests_total not present in /metrics output")
	}
}
