// Package health implements active HTTP probing for upstream eligibility.
//
// Each upstream pool runs one supervisor goroutine that owns the pool's
// probe cadence. Pools are isolated: a misbehaving probe in one pool cannot
// block another pool's liveness signal. Probes use a dedicated http.Client
// separate from the upstream-traffic Transport so probe traffic doesn't
// compete with real requests for connection slots.
//
// The hysteresis rules are conservative: an upstream transitions to
// "eligible" only after `healthy_threshold` consecutive successful probes,
// and to "ineligible" only after `unhealthy_threshold` consecutive failures.
// New upstreams begin ineligible until they prove healthy.
package health

import (
	"context"
	"log/slog"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/harimalladi/l7rp/internal/config"
	"github.com/harimalladi/l7rp/internal/lb"
)

// Monitor runs active health probes for one upstream pool and (optionally)
// tracks passive EWMA failure scoring from real upstream-traffic outcomes.
//
// Active probes and passive scoring observe different signals on different
// time scales:
//
//   - Active: persistent state, probes at a configured interval, recovery
//     requires sustained probe success.
//   - Passive: per-request signal, EWMA-decayed over a configurable half-life;
//     can take an upstream OUT of rotation but cannot put it BACK in.
//
// The one-way valve for passive scoring is deliberate: an upstream marked
// passively-ineligible receives no traffic, so its passive EWMA can't decay.
// Only active probes can confirm recovery and restore eligibility.
type Monitor struct {
	poolName string
	pool     []*lb.Upstream
	cfg      *config.ActiveHealthConfig
	passive  *config.PassiveHealthConfig
	client   *http.Client

	// states is parallel to pool, holding per-upstream probe-result counters
	// plus the passive scoring EWMA state.
	states []*probeState

	now func() time.Time
	rng *rand.Rand

	// OnTransition is invoked when an upstream's eligibility flag flips.
	// Hooked up by main to drive observability counters.
	OnTransition func(poolName string, upstream *lb.Upstream, eligible bool, reason string)
}

type probeState struct {
	mu        sync.Mutex
	successes int
	failures  int

	// Passive scoring fields. Updated by RecordOutcome under the same mu.
	errEWMA     float64
	lastPassive time.Time
}

// NewMonitor constructs a Monitor for the given pool. Returns nil when neither
// active nor passive configuration is provided — pools fully opt out of the
// health monitor and rely on the circuit breaker alone.
//
// Passive scoring requires active probes (config validation rejects
// the inverse). The constructor accepts both so the wiring lives in one place;
// callers may pass nil for passive when only active probes are desired.
func NewMonitor(poolName string, pool []*lb.Upstream, cfg *config.ActiveHealthConfig, passive *config.PassiveHealthConfig) *Monitor {
	if cfg == nil {
		return nil
	}
	states := make([]*probeState, len(pool))
	for i := range states {
		states[i] = &probeState{}
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}
	seed := uint64(time.Now().UnixNano())
	return &Monitor{
		poolName: poolName,
		pool:     pool,
		cfg:      cfg,
		passive:  passive,
		client: &http.Client{
			Timeout: timeout,
			// Dedicated transport — does NOT share the upstream-traffic pool.
			Transport: &http.Transport{
				MaxIdleConns:        len(pool),
				MaxIdleConnsPerHost: 1,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		states: states,
		now:    time.Now,
		rng:    rand.New(rand.NewPCG(seed, seed^0xc0ffee)),
	}
}

// RecordOutcome feeds a per-request outcome from the upstream client into the
// passive EWMA score. No-op when passive scoring is not configured.
//
// `isError` is true for connection errors, upstream timeouts, and 5xx
// responses. 4xx responses are not errors for passive scoring purposes
// (they're typically the client's fault, not the upstream's). Cancellations
// (hedge canceled, client disconnect) should also be filtered out by the
// caller before calling this — they're not upstream failures.
func (m *Monitor) RecordOutcome(u *lb.Upstream, latency time.Duration, isError bool) {
	if m == nil || m.passive == nil {
		return
	}

	idx := -1
	for i, up := range m.pool {
		if up == u {
			idx = i
			break
		}
	}
	if idx < 0 {
		return
	}

	state := m.states[idx]
	state.mu.Lock()
	defer state.mu.Unlock()

	now := m.now()
	halfLife := m.passive.HalfLife
	if halfLife <= 0 {
		halfLife = 30 * time.Second
	}

	// Time-weighted alpha — same formulation as the P2C selector's EWMA.
	alpha := 1.0
	if !state.lastPassive.IsZero() {
		dt := now.Sub(state.lastPassive)
		if dt < 0 {
			dt = 0
		}
		alpha = 1 - math.Exp(-float64(dt)/float64(halfLife))
	}
	state.lastPassive = now

	sample := 0.0
	if isError {
		sample = 1.0
	}
	state.errEWMA = alpha*sample + (1-alpha)*state.errEWMA

	threshold := m.passive.ErrorThreshold
	if threshold <= 0 {
		threshold = 0.5
	}

	if state.errEWMA > threshold && u.Eligible.Load() {
		u.Eligible.Store(false)
		// Reset active counters so recovery requires a fresh batch of
		// consecutive probe successes from a clean slate — the one-way valve.
		state.successes = 0
		state.failures = 0
		slog.Warn("upstream ineligible (passive scoring)",
			slog.String("pool", m.poolName),
			slog.String("upstream", u.URL.String()),
			slog.Float64("err_ewma", state.errEWMA),
			slog.Float64("threshold", threshold))
		if m.OnTransition != nil {
			m.OnTransition(m.poolName, u, false, "passive_threshold_breached")
		}
	}
}

// Run is the supervisor loop. Returns when ctx is canceled. Pool upstreams
// start ineligible; they become eligible only after `healthy_threshold`
// consecutive probe successes.
func (m *Monitor) Run(ctx context.Context) {
	if m == nil {
		return
	}

	interval := m.cfg.Interval
	if interval <= 0 {
		interval = 2 * time.Second
	}

	// New upstreams begin ineligible — guard against routing traffic to
	// untested backends.
	for _, u := range m.pool {
		u.Eligible.Store(false)
	}

	// Jittered initial probes per upstream to avoid synchronized bursts.
	for i := range m.pool {
		jitter := time.Duration(m.rng.Int64N(int64(interval)))
		go m.probeAfter(ctx, i, jitter)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for i := range m.pool {
				go m.probeAfter(ctx, i, 0)
			}
		}
	}
}

func (m *Monitor) probeAfter(ctx context.Context, idx int, delay time.Duration) {
	if delay > 0 {
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
	m.probe(ctx, idx)
}

func (m *Monitor) probe(ctx context.Context, idx int) {
	u := m.pool[idx]
	timeout := m.cfg.Timeout
	if timeout <= 0 {
		timeout = time.Second
	}

	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	probeURL := *u.URL
	if m.cfg.Path != "" {
		probeURL.Path = m.cfg.Path
	}

	method := m.cfg.Method
	if method == "" {
		method = http.MethodGet
	}

	req, err := http.NewRequestWithContext(probeCtx, method, probeURL.String(), nil)
	if err != nil {
		m.recordResult(idx, false, "build-request:"+err.Error())
		return
	}

	resp, err := m.client.Do(req)
	if err != nil {
		m.recordResult(idx, false, "request:"+err.Error())
		return
	}
	defer resp.Body.Close()

	if !statusAccepted(resp.StatusCode, m.cfg.ExpectedStatus) {
		m.recordResult(idx, false, "status:"+strconv.Itoa(resp.StatusCode))
		return
	}
	m.recordResult(idx, true, "status:"+strconv.Itoa(resp.StatusCode))
}

func statusAccepted(got int, expected []int) bool {
	if len(expected) == 0 {
		return got == http.StatusOK
	}
	for _, code := range expected {
		if got == code {
			return true
		}
	}
	return false
}

func (m *Monitor) recordResult(idx int, success bool, reason string) {
	healthyTh := m.cfg.HealthyThreshold
	if healthyTh <= 0 {
		healthyTh = 2
	}
	unhealthyTh := m.cfg.UnhealthyThreshold
	if unhealthyTh <= 0 {
		unhealthyTh = 3
	}

	state := m.states[idx]
	state.mu.Lock()
	if success {
		state.successes++
		state.failures = 0
	} else {
		state.failures++
		state.successes = 0
	}
	successes, failures := state.successes, state.failures
	state.mu.Unlock()

	u := m.pool[idx]

	if success && successes >= healthyTh && !u.Eligible.Load() {
		u.Eligible.Store(true)
		slog.Info("upstream eligible (active probes)",
			"pool", m.poolName, "upstream", u.URL.String(),
			"consecutive_success", successes, "reason", reason)
		if m.OnTransition != nil {
			m.OnTransition(m.poolName, u, true, reason)
		}
		return
	}
	if !success && failures >= unhealthyTh && u.Eligible.Load() {
		u.Eligible.Store(false)
		slog.Warn("upstream ineligible (active probes)",
			"pool", m.poolName, "upstream", u.URL.String(),
			"consecutive_failure", failures, "reason", reason)
		if m.OnTransition != nil {
			m.OnTransition(m.poolName, u, false, reason)
		}
	}
}
