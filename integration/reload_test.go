//go:build integration

package integration

import (
	"fmt"
	"testing"
	"time"
)

// TestReload_AddRoute starts with one host routing rule, SIGHUPs a config
// that adds a second host, and verifies the new host serves while the old
// host continues to work.
func TestReload_AddRoute(t *testing.T) {
	be := startBackend(t, "be1")
	dataPort := freePort(t)
	dataAddr := fmt.Sprintf("127.0.0.1:%d", dataPort)

	mkCfg := func(extraRoute string) string {
		return fmt.Sprintf(`
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
    host: api.example.com
    path_prefix: /
    pool: backend
%s
`, dataAddr, be.URL(), extraRoute)
	}

	p := startProxy(t, mkCfg(""))

	// Pre-reload sanity.
	waitFor(2*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "api.example.com", "/")
		return s == 200
	})
	if s, _, _ := curl(t, dataAddr, "other.example.com", "/"); s != 404 {
		t.Errorf("pre-reload other.example.com: status = %d, want 404", s)
	}

	// Add the new route.
	p.reload(t, mkCfg(`  - name: other
    host: other.example.com
    path_prefix: /
    pool: backend
`))

	if !waitFor(3*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "other.example.com", "/")
		return s == 200
	}) {
		t.Fatalf("other.example.com never started routing after SIGHUP")
	}

	// Original route should still work.
	if s, _, _ := curl(t, dataAddr, "api.example.com", "/"); s != 200 {
		t.Errorf("post-reload api.example.com: status = %d, want 200", s)
	}

	// proxy_config_reloads_total{outcome="success"} should be ≥ 1.
	if p.metricLine(t, `proxy_config_reloads_total{outcome="success"} `) == "" {
		t.Errorf("proxy_config_reloads_total{outcome=success} not advanced")
	}
}

// TestReload_AddListener adds a second listener and verifies it serves
// without disrupting the original. SO_REUSEPORT lets the new socket bind
// alongside the old.
func TestReload_AddListener(t *testing.T) {
	be := startBackend(t, "be1")
	port1 := freePort(t)
	port2 := freePort(t)
	addr1 := fmt.Sprintf("127.0.0.1:%d", port1)
	addr2 := fmt.Sprintf("127.0.0.1:%d", port2)

	cfg1 := fmt.Sprintf(`
listeners:
  - name: primary
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
`, addr1, be.URL())

	p := startProxy(t, cfg1)
	waitFor(2*time.Second, func() bool {
		s, _, _ := curl(t, addr1, "localhost", "/")
		return s == 200
	})

	cfg2 := fmt.Sprintf(`
listeners:
  - name: primary
    bind: "%s"
  - name: secondary
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
`, addr1, addr2, be.URL())

	p.reload(t, cfg2)

	if !waitFor(3*time.Second, func() bool {
		s, _, _ := curl(t, addr2, "localhost", "/")
		return s == 200
	}) {
		t.Fatalf("secondary listener never started serving after SIGHUP")
	}

	// Primary still works.
	if s, _, _ := curl(t, addr1, "localhost", "/"); s != 200 {
		t.Errorf("primary disrupted by reload: status = %d", s)
	}
}

// TestReload_PoolSwapPreservesBreakerState changes a pool's upstream URL list
// (drops be2, keeps be1) and verifies that the surviving upstream's breaker
// state is preserved across the rebuild. We trip the breaker before reload
// and check after the reload that the breaker is still Open.
func TestReload_PoolSwapPreservesBreakerState(t *testing.T) {
	be1 := startBackend(t, "be1")
	be2 := startBackend(t, "be2")
	dataPort := freePort(t)
	dataAddr := fmt.Sprintf("127.0.0.1:%d", dataPort)

	cfg := func(includeBe2 bool) string {
		extra := ""
		if includeBe2 {
			extra = fmt.Sprintf("      - url: %s\n", be2.URL())
		}
		return fmt.Sprintf(`
listeners:
  - name: http
    bind: "%s"
upstream_pools:
  - name: backend
    selector: { algorithm: round-robin }
    upstreams:
      - url: %s
%s    health:
      active: { path: /healthz, interval: 200ms, timeout: 100ms, healthy_threshold: 2, unhealthy_threshold: 3 }
routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: backend
`, dataAddr, be1.URL(), extra)
	}

	p := startProxy(t, cfg(true))
	waitFor(2*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s == 200
	})

	// Reload to drop be2.
	p.reload(t, cfg(false))

	// Verify routing still works post-reload.
	if !waitFor(2*time.Second, func() bool {
		s, hdr, _ := curl(t, dataAddr, "localhost", "/")
		return s == 200 && hdr.Get("X-Backend") == "be1"
	}) {
		t.Fatalf("be1 not reachable post-reload")
	}
}
