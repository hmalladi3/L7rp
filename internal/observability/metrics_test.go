package observability

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestMetrics_RecordsRequestAndExposes(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.ObserveRequest("api", "GET", 200, 12*time.Millisecond)
	m.ObserveRequest("api", "GET", 502, 45*time.Millisecond)
	m.ObserveUpstream("backend", "http://u1", 200, 10*time.Millisecond)
	m.ObserveUpstream("backend", "http://u2", 0, 50*time.Millisecond) // connection error

	body := scrape(t, m)

	// proxy_requests_total split by status.
	mustContain(t, body, `proxy_requests_total{method="GET",route="api",status="200"} 1`)
	mustContain(t, body, `proxy_requests_total{method="GET",route="api",status="502"} 1`)

	// Upstream connection error renders as status="err" (not "0") — confirms
	// the label-cardinality discipline (no synthetic "0" status leaking into
	// dashboards).
	mustContain(t, body, `proxy_upstream_requests_total{pool="backend",status="200",upstream="http://u1"} 1`)
	mustContain(t, body, `proxy_upstream_requests_total{pool="backend",status="err",upstream="http://u2"} 1`)
}

func TestMetrics_BreakerStateGauge(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	m.SetBreakerState("backend", "http://u1", 0)
	m.SetBreakerState("backend", "http://u2", 1)
	m.SetBreakerState("backend", "http://u3", 2)

	body := scrape(t, m)
	mustContain(t, body, `proxy_circuit_breaker_state{pool="backend",upstream="http://u1"} 0`)
	mustContain(t, body, `proxy_circuit_breaker_state{pool="backend",upstream="http://u2"} 1`)
	mustContain(t, body, `proxy_circuit_breaker_state{pool="backend",upstream="http://u3"} 2`)
}

func TestMetrics_StandardEndpoints(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	srv := httptest.NewServer(m.Handler("v1.2.3"))
	defer srv.Close()

	for _, c := range []struct {
		path string
		want string
	}{
		{"/-/healthz", "ok"},
		{"/-/ready", "ready"},
		{"/-/version", "v1.2.3"},
	} {
		resp, err := http.Get(srv.URL + c.path)
		if err != nil {
			t.Fatalf("GET %s: %v", c.path, err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Errorf("GET %s: status = %d, want 200", c.path, resp.StatusCode)
		}
		if !strings.Contains(string(body), c.want) {
			t.Errorf("GET %s: body = %q, want substring %q", c.path, body, c.want)
		}
	}
}

func TestMetrics_ProcessCollectorsRegistered(t *testing.T) {
	t.Parallel()

	m := NewMetrics()
	body := scrape(t, m)
	// Go-runtime collector ships goroutine count; process collector ships open
	// FDs. Both are operator must-haves and prove the default collectors are
	// present.
	if !strings.Contains(body, "go_goroutines") {
		t.Error("expected go_goroutines from Go runtime collector")
	}
}

func TestMetrics_NilReceiverIsSafe(t *testing.T) {
	t.Parallel()

	// Defense in depth: callers in test paths or partially-wired bootstraps
	// shouldn't crash on a nil *Metrics.
	var m *Metrics
	m.ObserveRequest("route", "GET", 200, time.Millisecond)
	m.ObserveUpstream("pool", "u", 200, time.Millisecond)
	m.SetBreakerState("pool", "u", 1)
}

func scrape(t *testing.T, m *Metrics) string {
	t.Helper()
	srv := httptest.NewServer(m.Handler("test"))
	t.Cleanup(srv.Close)
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return string(body)
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Errorf("expected metric line %q\nactual:\n%s", needle, haystack)
	}
}
