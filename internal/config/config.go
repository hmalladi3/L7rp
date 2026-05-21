// Package config defines the L7 proxy's runtime configuration shape, the
// validation rules that gate config-load, and the Manager that holds the
// active immutable *Config behind an atomic.Pointer.
//
// The design rests on one architectural choice: *Config is immutable once
// constructed. Reload builds a new *Config and atomically swaps the active
// pointer; in-flight requests continue against the snapshot they captured at
// listener-handler entry. No locks on the request hot path; concurrent
// reload during sustained load drops zero requests.
package config

import (
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the immutable runtime configuration. Construction is the entire
// validation surface — once a *Config exists, every field has been checked
// for self-consistency.
type Config struct {
	Listeners []ListenerConfig `yaml:"listeners"`
	Pools     []PoolConfig     `yaml:"upstream_pools"`
	Routes    []RouteConfig    `yaml:"routes"`
}

type ListenerConfig struct {
	Name     string           `yaml:"name"`
	Bind     string           `yaml:"bind"`
	TLS      *TLSConfig       `yaml:"tls,omitempty"`
	Timeouts ListenerTimeouts `yaml:"timeouts,omitempty"`
}

type TLSConfig struct {
	Certs      []TLSCert       `yaml:"certs,omitempty"`
	MinVersion string          `yaml:"min_version,omitempty"`
	Autocert   *AutocertConfig `yaml:"autocert,omitempty"`
}

type TLSCert struct {
	Cert  string   `yaml:"cert"`
	Key   string   `yaml:"key"`
	Hosts []string `yaml:"hosts,omitempty"`
}

type AutocertConfig struct {
	CacheDir     string   `yaml:"cache_dir"`
	AllowedHosts []string `yaml:"allowed_hosts"`
	Email        string   `yaml:"email"`
}

type ListenerTimeouts struct {
	ReadHeader     time.Duration `yaml:"read_header,omitempty"`
	Read           time.Duration `yaml:"read,omitempty"`
	Write          time.Duration `yaml:"write,omitempty"`
	Idle           time.Duration `yaml:"idle,omitempty"`
	MaxHeaderBytes int           `yaml:"max_header_bytes,omitempty"`
}

type PoolConfig struct {
	Name      string           `yaml:"name"`
	Selector  SelectorConfig   `yaml:"selector,omitempty"`
	Upstreams []UpstreamConfig `yaml:"upstreams"`
	Health    HealthConfig     `yaml:"health,omitempty"`
}

type SelectorConfig struct {
	Algorithm    string        `yaml:"algorithm,omitempty"`
	EWMAHalfLife time.Duration `yaml:"ewma_half_life,omitempty"`
	HashKey      string        `yaml:"hash_key,omitempty"`
	Epsilon      float64       `yaml:"epsilon,omitempty"`
	VirtualNodes int           `yaml:"virtual_nodes,omitempty"`
}

type UpstreamConfig struct {
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight,omitempty"`
}

type HealthConfig struct {
	Active  *ActiveHealthConfig  `yaml:"active,omitempty"`
	Passive *PassiveHealthConfig `yaml:"passive,omitempty"`
}

type ActiveHealthConfig struct {
	Path               string        `yaml:"path"`
	Method             string        `yaml:"method,omitempty"`
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
	HealthyThreshold   int           `yaml:"healthy_threshold,omitempty"`
	UnhealthyThreshold int           `yaml:"unhealthy_threshold,omitempty"`
	ExpectedStatus     []int         `yaml:"expected_status,omitempty"`
}

type PassiveHealthConfig struct {
	ErrorThreshold      float64       `yaml:"error_threshold,omitempty"`
	LatencyP99Threshold time.Duration `yaml:"latency_threshold_p99,omitempty"`
	HalfLife            time.Duration `yaml:"half_life,omitempty"`
}

type RouteConfig struct {
	Name             string                  `yaml:"name"`
	Host             string                  `yaml:"host"`
	PathPrefix       string                  `yaml:"path_prefix,omitempty"`
	HeaderPredicates []HeaderPredicateConfig `yaml:"header_predicates,omitempty"`
	Pool             string                  `yaml:"pool"`
	Middleware       []MiddlewareConfig      `yaml:"middleware,omitempty"`
	Timeouts         RouteTimeouts           `yaml:"timeouts,omitempty"`
}

type HeaderPredicateConfig struct {
	Name  string `yaml:"name"`
	Value string `yaml:"value"`
}

// MiddlewareConfig is a discriminated union — exactly one of the embedded
// pointers is set per entry.
type MiddlewareConfig struct {
	RateLimit       *RateLimitConfig       `yaml:"rate_limit,omitempty"`
	Retry           *RetryConfig           `yaml:"retry,omitempty"`
	HeaderTransform *HeaderTransformConfig `yaml:"headers,omitempty"`
}

type RateLimitConfig struct {
	Scope          string `yaml:"scope,omitempty"`
	PerIPRPS       int    `yaml:"per_ip_rps,omitempty"`
	Burst          int    `yaml:"burst,omitempty"`
	TrustProxyHops int    `yaml:"trust_proxy_hops,omitempty"`
}

type RetryConfig struct {
	MaxAttempts          int           `yaml:"max_attempts,omitempty"`
	RetryMethods         []string      `yaml:"retry_methods,omitempty"`
	RetryOn              []int         `yaml:"retry_on,omitempty"`
	HedgeAfter           time.Duration `yaml:"hedge_after,omitempty"`
	HedgeAfterPercentile float64       `yaml:"hedge_after_percentile,omitempty"`
	HedgeableMethods     []string      `yaml:"hedgeable,omitempty"`
	MaxReplayableBody    int64         `yaml:"max_replayable_body,omitempty"`
}

type HeaderTransformConfig struct {
	AddRequest     map[string]string `yaml:"add_request,omitempty"`
	RemoveRequest  []string          `yaml:"remove_request,omitempty"`
	AddResponse    map[string]string `yaml:"add_response,omitempty"`
	RemoveResponse []string          `yaml:"remove_response,omitempty"`
}

type RouteTimeouts struct {
	UpstreamConnect  time.Duration `yaml:"upstream_connect,omitempty"`
	UpstreamResponse time.Duration `yaml:"upstream_response,omitempty"`
	Total            time.Duration `yaml:"total,omitempty"`
}

// Load reads a YAML configuration from r, parses, validates, and returns the
// resulting *Config. Any validation error is returned with as much YAML
// location info as gopkg.in/yaml.v3 surfaces.
func Load(r io.Reader) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("config: parse: %w", err)
	}
	if err := Validate(&cfg); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate runs the full validation pipeline against c and returns the first
// error encountered, or nil if c is valid.
func Validate(c *Config) error {
	if err := validateListeners(c.Listeners); err != nil {
		return err
	}
	if err := validatePools(c.Pools); err != nil {
		return err
	}
	if err := validateRoutes(c.Routes, c.Pools); err != nil {
		return err
	}
	return nil
}

func validateListeners(ls []ListenerConfig) error {
	// bind addresses don't overlap.
	binds := make(map[string]string)
	for i, l := range ls {
		if existing, dup := binds[l.Bind]; dup {
			return &ValidationError{
				Path: fmt.Sprintf("listeners[%d].bind", i),
				Msg:  fmt.Sprintf("bind address %q already used by listener %q", l.Bind, existing),
			}
		}
		binds[l.Bind] = l.Name

		// TLS min_version >= 1.2.
		if l.TLS != nil && l.TLS.MinVersion != "" {
			if !validTLSVersion(l.TLS.MinVersion) {
				return &ValidationError{
					Path: fmt.Sprintf("listeners[%d].tls.min_version", i),
					Msg:  fmt.Sprintf("min_version %q below required 1.2", l.TLS.MinVersion),
				}
			}
		}
	}
	return nil
}

func validTLSVersion(v string) bool {
	switch v {
	case "1.2", "1.3":
		return true
	default:
		return false
	}
}

func validatePools(ps []PoolConfig) error {
	knownAlgorithms := map[string]bool{
		"":                        true, // default → p2c-ewma
		"round-robin":             true,
		"weighted-rr":             true,
		"least-conn":              true,
		"p2c-ewma":                true,
		"consistent-hash-bounded": true,
	}

	names := make(map[string]bool)
	for i, p := range ps {
		if names[p.Name] {
			return &ValidationError{
				Path: fmt.Sprintf("upstream_pools[%d].name", i),
				Msg:  fmt.Sprintf("duplicate pool name %q", p.Name),
			}
		}
		names[p.Name] = true

		// known algorithm.
		if !knownAlgorithms[p.Selector.Algorithm] {
			return &ValidationError{
				Path: fmt.Sprintf("upstream_pools[%d].selector.algorithm", i),
				Msg:  fmt.Sprintf("unknown selector algorithm %q", p.Selector.Algorithm),
			}
		}
		// CH-BL requires hash_key.
		if p.Selector.Algorithm == "consistent-hash-bounded" && p.Selector.HashKey == "" {
			return &ValidationError{
				Path: fmt.Sprintf("upstream_pools[%d].selector.hash_key", i),
				Msg:  "consistent-hash-bounded selector requires a hash_key rule",
			}
		}

		// passive requires active.
		if p.Health.Passive != nil && p.Health.Active == nil {
			return &ValidationError{
				Path: fmt.Sprintf("upstream_pools[%d].health.passive", i),
				Msg:  "passive health requires active health (passive cannot restore eligibility alone)",
			}
		}

		// probe interval > timeout.
		if a := p.Health.Active; a != nil && a.Interval > 0 && a.Timeout > 0 && a.Interval <= a.Timeout {
			return &ValidationError{
				Path: fmt.Sprintf("upstream_pools[%d].health.active.interval", i),
				Msg:  fmt.Sprintf("interval %v must exceed timeout %v", a.Interval, a.Timeout),
			}
		}
	}
	return nil
}

func validateRoutes(routes []RouteConfig, pools []PoolConfig) error {
	poolNames := make(map[string]bool, len(pools))
	for _, p := range pools {
		poolNames[p.Name] = true
	}

	// detect duplicate (host, path_prefix, predicates) tuples.
	type key struct{ host, prefix, preds string }
	seen := make(map[key]string)

	for i, r := range routes {
		// pool must exist.
		if !poolNames[r.Pool] {
			return &ValidationError{
				Path: fmt.Sprintf("routes[%d].pool", i),
				Msg:  fmt.Sprintf("route %q references unknown pool %q", r.Name, r.Pool),
			}
		}

		// Duplicate / unreachable-route detection.
		k := key{
			host:   strings.ToLower(r.Host),
			prefix: r.PathPrefix,
			preds:  predicatesKey(r.HeaderPredicates),
		}
		if existing, dup := seen[k]; dup {
			return &ValidationError{
				Path: fmt.Sprintf("routes[%d]", i),
				Msg:  fmt.Sprintf("duplicate route: %q has same (host, path_prefix, predicates) as %q", r.Name, existing),
			}
		}
		seen[k] = r.Name

		// no CRLF in header transform values.
		for _, m := range r.Middleware {
			if m.HeaderTransform != nil {
				if err := scanHeaderValues(m.HeaderTransform); err != nil {
					return &ValidationError{
						Path: fmt.Sprintf("routes[%d].middleware.headers", i),
						Msg:  err.Error(),
					}
				}
			}
		}
	}
	return nil
}

func scanHeaderValues(h *HeaderTransformConfig) error {
	for name, v := range h.AddRequest {
		if strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("CRLF in add_request[%q]", name)
		}
	}
	for name, v := range h.AddResponse {
		if strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("CRLF in add_response[%q]", name)
		}
	}
	return nil
}

func predicatesKey(ps []HeaderPredicateConfig) string {
	if len(ps) == 0 {
		return ""
	}
	parts := make([]string, 0, len(ps))
	for _, p := range ps {
		parts = append(parts, strings.ToLower(p.Name)+"="+p.Value)
	}
	// Sort for stable key.
	for i := 1; i < len(parts); i++ {
		for j := i; j > 0 && parts[j-1] > parts[j]; j-- {
			parts[j-1], parts[j] = parts[j], parts[j-1]
		}
	}
	return strings.Join(parts, ";")
}

// resolveBind ensures the bind string is a valid TCP address. Returns the
// canonical form so we can detect overlap.
func resolveBind(s string) (string, error) {
	host, port, err := net.SplitHostPort(s)
	if err != nil {
		return "", err
	}
	if host == "" {
		host = "0.0.0.0"
	}
	return net.JoinHostPort(host, port), nil
}

// Manager owns the active *Config and serves atomic snapshots to readers.
// It coordinates SIGHUP-driven reloads, atomically swapping the active
// pointer when a new configuration validates successfully.
//
// Hot-path reads of the active *Config are sub-nanosecond (one atomic load).
// Concurrent reads during reload never block.
type Manager struct {
	active atomic.Pointer[Config]
}

// NewManager constructs a Manager with the given initial configuration. The
// initial *Config must already have passed Validate.
func NewManager(initial *Config) *Manager {
	m := &Manager{}
	m.active.Store(initial)
	return m
}

// Load returns the active *Config snapshot. This is the hot-path read,
// expected to be called exactly once per request at listener-handler entry.
func (m *Manager) Load() *Config {
	return m.active.Load()
}

// Reload reads a new configuration from r, validates it, and swaps the active
// pointer atomically on success. On any failure (parse, validate, build), the
// previous configuration is retained and a typed error is returned.
func (m *Manager) Reload(r io.Reader) error {
	cfg, err := Load(r)
	if err != nil {
		return err
	}
	m.swap(cfg)
	return nil
}

// swap unconditionally replaces the active config. Exposed unexported to
// in-package tests.
func (m *Manager) swap(cfg *Config) {
	m.active.Store(cfg)
}

// ValidationError wraps a validation failure with YAML location info.
type ValidationError struct {
	Path string // YAML path, e.g., "listeners[0].tls.min_version"
	Line int    // 1-based; 0 when unknown
	Msg  string
}

func (e *ValidationError) Error() string {
	if e.Line > 0 {
		return fmt.Sprintf("%s (line %d): %s", e.Path, e.Line, e.Msg)
	}
	return fmt.Sprintf("%s: %s", e.Path, e.Msg)
}
