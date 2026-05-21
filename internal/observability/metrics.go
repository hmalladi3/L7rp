// Package observability provides the proxy's Prometheus metrics surface and
// the small HTTP server that exposes /metrics, /-/healthz, /-/ready, and
// /-/version. Tracing and structured-logging helpers will land alongside.
//
// Label cardinality discipline: only `route`, `pool`, `upstream`, `listener`,
// `method`, and `status` are accepted as label values, and each comes from a
// bounded set known at config-load time. There is no `path`, `client_ip`, or
// `request_id` label — the dominant Prometheus failure mode is cardinality
// blowup, and we close off the surface.
package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the proxy's Prometheus collectors. Constructed once at
// startup; the registry is owned by the Metrics instance so tests can spin up
// isolated registries without colliding with the process-wide DefaultRegisterer.
type Metrics struct {
	Registry *prometheus.Registry

	Requests           *prometheus.CounterVec
	RequestDuration    *prometheus.HistogramVec
	UpstreamRequests   *prometheus.CounterVec
	UpstreamDuration   *prometheus.HistogramVec
	UpstreamInflight   *prometheus.GaugeVec
	BreakerState       *prometheus.GaugeVec
	BreakerTransitions *prometheus.CounterVec
	HealthTransitions  *prometheus.CounterVec
	NoRoute            *prometheus.CounterVec
	Panics             *prometheus.CounterVec
	HedgeFired         *prometheus.CounterVec
	HedgeWinner        *prometheus.CounterVec
	RetryAttempts      *prometheus.CounterVec
	ConfigReloads      *prometheus.CounterVec
	ReloadDuration     prometheus.Histogram
}

// NewMetrics constructs the metric set and registers it on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	// Standard Go process collectors give operators "for free" the goroutine
	// count, GC stats, and FD usage they expect.
	reg.MustRegister(
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	m := &Metrics{Registry: reg}

	// Latency buckets favor sub-millisecond resolution at the low end —
	// appropriate for an L7 proxy where most overhead is dispatch and
	// most upstream latency is in the tens-to-hundreds of ms.
	durationBuckets := []float64{
		0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
	}

	m.Requests = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_requests_total", Help: "Total requests received, labeled by route, method, and downstream response status."},
		[]string{"route", "method", "status"},
	)
	m.RequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "proxy_request_duration_seconds", Help: "End-to-end request duration including upstream call and retries.", Buckets: durationBuckets},
		[]string{"route", "method"},
	)
	m.UpstreamRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_upstream_requests_total", Help: "Upstream request attempts by pool, upstream, and resulting status (or outcome=err/cancel)."},
		[]string{"pool", "upstream", "status"},
	)
	m.UpstreamDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{Name: "proxy_upstream_duration_seconds", Help: "Per-attempt upstream call duration.", Buckets: durationBuckets},
		[]string{"pool", "upstream"},
	)
	m.UpstreamInflight = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "proxy_upstream_inflight", Help: "Currently in-flight requests per upstream."},
		[]string{"pool", "upstream"},
	)
	m.BreakerState = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{Name: "proxy_circuit_breaker_state", Help: "Circuit breaker state: 0=closed, 1=open, 2=half-open."},
		[]string{"pool", "upstream"},
	)
	m.BreakerTransitions = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_circuit_breaker_transitions_total", Help: "Circuit breaker state transition count."},
		[]string{"pool", "upstream", "to"},
	)
	m.HealthTransitions = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_health_eligibility_transitions_total", Help: "Upstream eligibility transitions from active or passive health."},
		[]string{"pool", "upstream", "to", "reason"},
	)
	m.NoRoute = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_no_route_total", Help: "Requests that matched no route (404)."},
		[]string{"listener"},
	)
	m.Panics = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_panics_total", Help: "Recovered panics by location."},
		[]string{"location"},
	)
	m.HedgeFired = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_hedge_fired_total", Help: "Hedge dispatches by route."},
		[]string{"route"},
	)
	m.HedgeWinner = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_hedge_winner_total", Help: "Which side won the hedge race."},
		[]string{"route", "winner"},
	)
	m.RetryAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_retry_attempts_total", Help: "Retry attempts beyond the original by route and outcome."},
		[]string{"route", "outcome"},
	)
	m.ConfigReloads = prometheus.NewCounterVec(
		prometheus.CounterOpts{Name: "proxy_config_reloads_total", Help: "Configuration reload attempts by outcome."},
		[]string{"outcome"},
	)
	m.ReloadDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{Name: "proxy_config_reload_duration_seconds", Help: "Time to complete a configuration reload.", Buckets: durationBuckets},
	)

	reg.MustRegister(
		m.Requests, m.RequestDuration,
		m.UpstreamRequests, m.UpstreamDuration, m.UpstreamInflight,
		m.BreakerState, m.BreakerTransitions,
		m.HealthTransitions, m.NoRoute, m.Panics,
		m.HedgeFired, m.HedgeWinner, m.RetryAttempts,
		m.ConfigReloads, m.ReloadDuration,
	)
	return m
}

// ObserveRequest records the standard end-of-request metrics.
func (m *Metrics) ObserveRequest(route, method string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	m.Requests.WithLabelValues(route, method, strconv.Itoa(status)).Inc()
	m.RequestDuration.WithLabelValues(route, method).Observe(duration.Seconds())
}

// ObserveUpstream records the per-attempt upstream metrics. The status is the
// HTTP status code returned by the upstream; for connection errors / timeouts
// the caller passes 0 and we map to "err".
func (m *Metrics) ObserveUpstream(pool, upstream string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	statusLabel := "err"
	if status > 0 {
		statusLabel = strconv.Itoa(status)
	}
	m.UpstreamRequests.WithLabelValues(pool, upstream, statusLabel).Inc()
	m.UpstreamDuration.WithLabelValues(pool, upstream).Observe(duration.Seconds())
}

// SetBreakerState updates the gauge representing the breaker's current state.
// 0 = closed, 1 = open, 2 = half-open.
func (m *Metrics) SetBreakerState(pool, upstream string, state int) {
	if m == nil {
		return
	}
	m.BreakerState.WithLabelValues(pool, upstream).Set(float64(state))
}

// Handler returns an http.Handler that serves /metrics, /-/healthz, /-/ready,
// and /-/version on the supplied mux. The metrics listener is separated from
// the data listeners so scrape traffic can't compete for request-handling
// goroutines.
func (m *Metrics) Handler(version string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/-/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/-/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/-/version", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(version + "\n"))
	})
	return mux
}
