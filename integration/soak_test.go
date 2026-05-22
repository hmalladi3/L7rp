//go:build soak

package integration

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestSoak_ReloadUnderLoad drives steady traffic through the proxy and triggers
// repeated SIGHUP reloads. Asserts zero 5xx responses across the reload window —
// SIGHUP-driven config swaps must not interrupt in-flight or new requests.
func TestSoak_ReloadUnderLoad(t *testing.T) {
	be := startBackend(t, "be1")
	dataPort := freePort(t)
	dataAddr := fmt.Sprintf("127.0.0.1:%d", dataPort)

	mkCfg := func(routeName string) string {
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
  - name: %s
    host: localhost
    path_prefix: /
    pool: backend
`, dataAddr, be.URL(), routeName)
	}

	p := startProxy(t, mkCfg("api-v0"))

	// Wait until the first request lands successfully.
	if !waitFor(2*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s == 200
	}) {
		t.Fatalf("proxy never reachable")
	}

	// Drive sustained traffic from 32 concurrent clients for 10 seconds.
	// One shared transport so the test process doesn't exhaust local ephemeral
	// ports against the proxy — we care about proxy stability, not client
	// behavior.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr := &http.Transport{
		MaxIdleConns:        128,
		MaxIdleConnsPerHost: 128,
		IdleConnTimeout:     30 * time.Second,
	}
	defer tr.CloseIdleConnections()
	cli := &http.Client{Transport: tr, Timeout: 2 * time.Second}

	var (
		ok       atomic.Int64
		bad      atomic.Int64 // 5xx including 503
		errs     atomic.Int64 // connection errors
		clientWG sync.WaitGroup
	)

	const concurrency = 32
	clientWG.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer clientWG.Done()
			for ctx.Err() == nil {
				req, _ := http.NewRequestWithContext(ctx, "GET", "http://"+dataAddr+"/", nil)
				req.Host = "localhost"
				resp, err := cli.Do(req)
				if err != nil {
					if ctx.Err() == nil {
						errs.Add(1)
					}
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode >= 500 {
					bad.Add(1)
				} else {
					ok.Add(1)
				}
			}
		}()
	}

	// Reload every 500ms with an alternating route name to force a real swap.
	reloadWG := sync.WaitGroup{}
	reloadWG.Add(1)
	go func() {
		defer reloadWG.Done()
		i := 0
		tick := time.NewTicker(500 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				i++
				p.reload(t, mkCfg(fmt.Sprintf("api-v%d", i)))
			}
		}
	}()

	clientWG.Wait()
	reloadWG.Wait()

	total := ok.Load() + bad.Load() + errs.Load()
	t.Logf("soak/reload: ok=%d 5xx=%d errs=%d total=%d", ok.Load(), bad.Load(), errs.Load(), total)

	if total < 1000 {
		t.Errorf("only drove %d requests in 10s; suspicious", total)
	}
	if bad.Load() > 0 {
		t.Errorf("reload caused 5xx responses: %d", bad.Load())
	}
	if errs.Load() > 5 {
		// A handful of connection errors right around the reload boundary
		// can be tolerated since the listener accepts on the new socket
		// before the old socket fully drains. But a wall of errors means
		// reload is dropping connections.
		t.Errorf("reload caused %d connection errors", errs.Load())
	}
}

// TestSoak_TimeoutEnforcement points the proxy at an upstream that sleeps far
// longer than the configured total route timeout. Asserts every request returns
// promptly with the expected gateway-timeout status, and that goroutines do not
// pile up across requests.
func TestSoak_TimeoutEnforcement(t *testing.T) {
	be := startSlowBackend(t, 3*time.Second)
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
      active: { path: /healthz, interval: 100ms, timeout: 50ms, healthy_threshold: 1, unhealthy_threshold: 100 }
routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: backend
    timeouts:
      total: 500ms
`, dataAddr, be.URL())

	p := startProxy(t, cfg)

	// Wait for the upstream to become eligible and the first request to
	// actually attempt a dispatch (the metric line appearing proves we got
	// past the short-circuit). Until then every response is 503 and there's
	// no timeout regime to measure.
	if !waitFor(3*time.Second, func() bool {
		curl(t, dataAddr, "localhost", "/")
		return p.metricLine(t, "proxy_upstream_requests_total") != ""
	}) {
		t.Fatalf("upstream never became eligible — no dispatch metric observed")
	}

	baseline := runtime.NumGoroutine()

	const N = 20
	var got504, got503, gotOther int
	for i := 0; i < N; i++ {
		start := time.Now()
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		elapsed := time.Since(start)
		switch s {
		case http.StatusGatewayTimeout: // 504
			got504++
		case http.StatusServiceUnavailable: // 503 — passive ejection short-circuit
			got503++
		default:
			gotOther++
			t.Errorf("request %d: unexpected status = %d (want 504 or 503)", i, s)
		}
		// Total budget is 500ms; allow generous slack for scheduling on a
		// busy CI runner.
		if elapsed > 1500*time.Millisecond {
			t.Errorf("request %d exceeded budget: took %s, total timeout was 500ms", i, elapsed)
		}
	}
	// We expect at least one 504 before passive scoring kicks in and starts
	// short-circuiting with 503.
	if got504 == 0 {
		t.Errorf("no 504 responses observed; want gateway-timeout from the total budget firing (got %d×503, %d×other)", got503, gotOther)
	}

	// Let any short-lived goroutines wind down.
	time.Sleep(500 * time.Millisecond)
	runtime.GC()

	leaked := runtime.NumGoroutine() - baseline
	if leaked > 10 {
		t.Errorf("goroutine count grew by %d after %d timed-out requests (baseline=%d)", leaked, N, baseline)
	}

	// Sanity: the proxy did try to dispatch upstream at least once before
	// passive scoring kicked in.
	if p.metricLine(t, "proxy_upstream_requests_total") == "" {
		t.Errorf("no upstream_requests metric observed")
	}
}

// TestSoak_BackendRestart kills the backend mid-stream and restarts it later
// on the same port. The proxy should serve 5xx during the gap (health-monitor
// marks the upstream ineligible) and recover automatically once the backend
// comes back and the probe succeeds again.
func TestSoak_BackendRestart(t *testing.T) {
	be := startBackend(t, "be1")
	bePort := be.port()
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
      active: { path: /healthz, interval: 200ms, timeout: 100ms, healthy_threshold: 2, unhealthy_threshold: 2 }
routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: backend
`, dataAddr, be.URL())

	startProxy(t, cfg)

	// Phase 1: confirm the proxy is serving.
	if !waitFor(2*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s == 200
	}) {
		t.Fatalf("proxy never reachable initially")
	}

	// Phase 2: kill the backend; expect 5xx within a few probe intervals.
	be.Close()
	if !waitFor(3*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s >= 500
	}) {
		t.Fatalf("proxy still serving 200 after backend died")
	}

	// Phase 3: bring a fresh backend up on the same port; expect the proxy
	// to recover within a few probe intervals (healthy_threshold=2 × 200ms).
	be2 := startBackendOnPort(t, "be1-reborn", bePort)
	if !waitFor(3*time.Second, func() bool {
		s, _, _ := curl(t, dataAddr, "localhost", "/")
		return s == 200
	}) {
		t.Errorf("proxy never recovered after backend restart on port %d", bePort)
	}
	_ = be2
}

// TestSoak_ConnectTimeoutFires points the proxy at TEST-NET-1 (RFC 5737,
// guaranteed unroutable) with a 200ms connect_timeout and asserts the proxy
// gives up within budget rather than blocking on the kernel's much longer
// default connect timeout (~75s on macOS).
func TestSoak_ConnectTimeoutFires(t *testing.T) {
	dataPort := freePort(t)
	dataAddr := fmt.Sprintf("127.0.0.1:%d", dataPort)

	// 192.0.2.1 is in TEST-NET-1; packets are unroutable so the dial hangs
	// until the configured timeout (or kernel default) fires.
	cfg := fmt.Sprintf(`
listeners:
  - name: http
    bind: "%s"
upstream_pools:
  - name: backend
    selector: { algorithm: round-robin }
    connect_timeout: 200ms
    upstreams:
      - url: http://192.0.2.1:80
routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: backend
`, dataAddr)

	startProxy(t, cfg)

	start := time.Now()
	s, _, _ := curl(t, dataAddr, "localhost", "/")
	elapsed := time.Since(start)

	// Anywhere in the 5xx family is fine — the point is that we get an
	// answer quickly rather than waiting for the OS to give up.
	if s < 500 || s > 599 {
		t.Errorf("status = %d, want 5xx", s)
	}
	// Budget: 200ms timeout + some slack for the request lifecycle. macOS CI
	// schedulers can be slow; allow 2s before we call this a real regression.
	if elapsed > 2*time.Second {
		t.Errorf("connect timeout did not fire: request took %s, want <= 2s for 200ms timeout", elapsed)
	}
}
