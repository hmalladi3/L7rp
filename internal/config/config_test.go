package config

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestManager_LoadReturnsCurrentConfig is the basic contract: after construction,
// Load returns the initial *Config; after a swap, Load returns the new one.
func TestManager_LoadReturnsCurrentConfig(t *testing.T) {
	t.Parallel()

	cfg1 := &Config{}
	m := NewManager(cfg1)
	if got := m.Load(); got != cfg1 {
		t.Errorf("Load returned %p, want %p", got, cfg1)
	}

	cfg2 := &Config{}
	m.swap(cfg2)
	if got := m.Load(); got != cfg2 {
		t.Errorf("after swap, Load returned %p, want %p", got, cfg2)
	}
}

// Concurrent readers must always observe a coherent *Config — never a partial
// swap. With atomic.Pointer this is guaranteed at the language level; this
// test exercises the race detector to catch any accidental non-atomic mutation
// added in future revisions. Both readers and the swapper run in goroutines
// for a fixed wall-clock window so the Go scheduler interleaves them.
func TestManager_ConcurrentReadDuringSwapIsRaceClean(t *testing.T) {
	t.Parallel()

	cfgA := &Config{Listeners: []ListenerConfig{{Name: "a"}}}
	cfgB := &Config{Listeners: []ListenerConfig{{Name: "b"}}}
	m := NewManager(cfgA)

	var (
		wg       sync.WaitGroup
		stop     atomic.Bool
		observed atomic.Int64
		swaps    atomic.Int64
	)

	// Readers.
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				c := m.Load()
				if c != cfgA && c != cfgB {
					t.Errorf("observed unknown *Config pointer: %p", c)
					return
				}
				observed.Add(1)
			}
		}()
	}

	// Swapper.
	wg.Add(1)
	go func() {
		defer wg.Done()
		i := int64(0)
		for !stop.Load() {
			if i%2 == 0 {
				m.swap(cfgA)
			} else {
				m.swap(cfgB)
			}
			i++
		}
		swaps.Store(i)
	}()

	time.Sleep(50 * time.Millisecond)
	stop.Store(true)
	wg.Wait()

	if observed.Load() == 0 {
		t.Error("no reads observed; test logic broken")
	}
	if swaps.Load() == 0 {
		t.Error("no swaps performed; test logic broken")
	}
}

// In-flight requests must continue against their captured snapshot even after
// a swap. The Manager's contract is: Load is called once at request entry and
// the returned pointer is used for the rest of the request's lifetime.
func TestManager_InFlightSnapshotSurvivesSwap(t *testing.T) {
	t.Parallel()

	cfgA := &Config{Listeners: []ListenerConfig{{Name: "a"}}}
	cfgB := &Config{Listeners: []ListenerConfig{{Name: "b"}}}
	m := NewManager(cfgA)

	snapshot := m.Load()
	m.swap(cfgB)

	if snapshot != cfgA {
		t.Error("captured snapshot was mutated by swap (immutability violated)")
	}
	if len(snapshot.Listeners) != 1 || snapshot.Listeners[0].Name != "a" {
		t.Errorf("snapshot listener changed: got %v", snapshot.Listeners)
	}
}

func TestValidate_RejectsDuplicateRoutes(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Pools: []PoolConfig{{Name: "p"}},
		Routes: []RouteConfig{
			{Name: "r1", Host: "x", PathPrefix: "/", Pool: "p"},
			{Name: "r2", Host: "x", PathPrefix: "/", Pool: "p"}, // duplicate tuple
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error on duplicate routes")
	}
}

func TestValidate_RejectsUnknownPool(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Pools: []PoolConfig{{Name: "real"}},
		Routes: []RouteConfig{
			{Name: "r", Host: "x", PathPrefix: "/", Pool: "missing"},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error on unknown pool reference")
	}
}

func TestValidate_RejectsLowTLSMinVersion(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Listeners: []ListenerConfig{
			{Name: "l", Bind: "0.0.0.0:443", TLS: &TLSConfig{MinVersion: "1.0"}},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error on TLS < 1.2")
	}
}

func TestValidate_AcceptsTLS12And13(t *testing.T) {
	t.Parallel()

	for _, v := range []string{"1.2", "1.3"} {
		cfg := &Config{
			Listeners: []ListenerConfig{
				{Name: "l", Bind: ":443", TLS: &TLSConfig{MinVersion: v}},
			},
		}
		if err := Validate(cfg); err != nil {
			t.Errorf("TLS %s rejected unexpectedly: %v", v, err)
		}
	}
}

func TestValidate_RejectsOverlappingBindAddresses(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Listeners: []ListenerConfig{
			{Name: "a", Bind: "0.0.0.0:443"},
			{Name: "b", Bind: "0.0.0.0:443"},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error on bind collision")
	}
}

func TestValidate_RejectsUnknownSelectorAlgorithm(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Pools: []PoolConfig{
			{Name: "p", Selector: SelectorConfig{Algorithm: "random-spinning-wheel"}},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error on unknown algorithm")
	}
}

func TestValidate_ConsistentHashRequiresHashKey(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Pools: []PoolConfig{
			{Name: "p", Selector: SelectorConfig{Algorithm: "consistent-hash-bounded"}},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error: CH-BL requires hash_key")
	}
}

func TestValidate_RejectsCRLFInHeaderValue(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Pools: []PoolConfig{{Name: "p"}},
		Routes: []RouteConfig{
			{
				Name: "r", Host: "x", PathPrefix: "/", Pool: "p",
				Middleware: []MiddlewareConfig{
					{HeaderTransform: &HeaderTransformConfig{
						AddRequest: map[string]string{"X-Bad": "value\r\ninjected: yes"},
					}},
				},
			},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error on CRLF in header value")
	}
}

func TestValidate_PassiveHealthRequiresActive(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Pools: []PoolConfig{
			{
				Name: "p",
				Health: HealthConfig{
					Passive: &PassiveHealthConfig{ErrorThreshold: 0.5},
					// Active is nil — passive can't restore eligibility alone.
				},
			},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error: passive requires active")
	}
}

func TestValidate_ProbeIntervalMustExceedTimeout(t *testing.T) {
	t.Parallel()

	cfg := &Config{
		Pools: []PoolConfig{
			{
				Name: "p",
				Health: HealthConfig{
					Active: &ActiveHealthConfig{
						Path:     "/",
						Interval: 100 * time.Millisecond,
						Timeout:  200 * time.Millisecond, // > interval
					},
				},
			},
		},
	}
	if err := Validate(cfg); err == nil {
		t.Error("expected error: interval must be > timeout")
	}
}

func TestLoad_RejectsInvalidYAML(t *testing.T) {
	t.Parallel()

	_, err := Load(strings.NewReader("not: valid: yaml: ::: structure"))
	if err == nil {
		t.Error("expected YAML parse error")
	}
}

func TestLoad_ValidConfigParses(t *testing.T) {
	t.Parallel()

	src := `
listeners:
  - name: http
    bind: "0.0.0.0:8080"
    timeouts:
      read_header: 5s
      read: 30s
      write: 30s
      idle: 120s

upstream_pools:
  - name: api
    selector:
      algorithm: p2c-ewma
    upstreams:
      - url: http://localhost:9001
      - url: http://localhost:9002
    health:
      active:
        path: /healthz
        interval: 2s
        timeout: 1s
        healthy_threshold: 2
        unhealthy_threshold: 3

routes:
  - name: api
    host: localhost
    path_prefix: /
    pool: api
    timeouts:
      upstream_connect: 1s
      upstream_response: 30s
      total: 35s
`
	cfg, err := Load(strings.NewReader(src))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Listeners) != 1 || cfg.Listeners[0].Name != "http" {
		t.Errorf("listeners not parsed correctly: %v", cfg.Listeners)
	}
	if len(cfg.Pools) != 1 || cfg.Pools[0].Name != "api" {
		t.Errorf("pools not parsed correctly: %v", cfg.Pools)
	}
}

func TestValidationError_ErrorString(t *testing.T) {
	t.Parallel()

	e := &ValidationError{Path: "listeners[0].bind", Line: 5, Msg: "invalid address"}
	got := e.Error()
	if !strings.Contains(got, "listeners[0].bind") || !strings.Contains(got, "line 5") || !strings.Contains(got, "invalid address") {
		t.Errorf("ValidationError string missing fields: %q", got)
	}
}
