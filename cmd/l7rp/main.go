// Command l7rp is the L7 reverse proxy and load balancer.
//
// Usage:
//
//	l7rp --config /etc/l7rp/config.yaml
//	l7rp --check --config /path/to/config.yaml
//	l7rp --version
//
// The proxy loads its configuration from YAML, validates it, builds the
// per-pool selectors and per-route middleware chains, then starts one TCP
// listener per configured listener entry. Shutdown on SIGINT/SIGTERM drains
// in-flight requests with a 30-second deadline.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/harimalladi/l7rp/internal/config"
	"github.com/harimalladi/l7rp/internal/health"
	"github.com/harimalladi/l7rp/internal/lb"
	"github.com/harimalladi/l7rp/internal/listener"
	"github.com/harimalladi/l7rp/internal/middleware"
	"github.com/harimalladi/l7rp/internal/observability"
	"github.com/harimalladi/l7rp/internal/router"
	"github.com/harimalladi/l7rp/internal/upstream"
)

// Version metadata; set via -ldflags during release builds.
var (
	Version   = "dev"
	GitCommit = "unknown"
)

func main() {
	if err := mainErr(); err != nil {
		fmt.Fprintln(os.Stderr, "l7rp:", err)
		os.Exit(1)
	}
}

func mainErr() error {
	var (
		configPath         = flag.String("config", "/etc/l7rp/config.yaml", "path to YAML config")
		checkOnly          = flag.Bool("check", false, "validate config and exit")
		showVer            = flag.Bool("version", false, "print version and exit")
		logLevel           = flag.String("log-level", "info", "log level: debug | info | warn | error")
		metricsBind        = flag.String("metrics-bind", "127.0.0.1:9090", "address for the /metrics endpoint; empty to disable")
		tracingEndpoint    = flag.String("tracing-endpoint", "", "OTLP gRPC endpoint (e.g., 'localhost:4317'); empty disables tracing export but keeps W3C propagation")
		tracingSampleRatio = flag.Float64("tracing-sample-ratio", 0.01, "head-based sampling ratio (0.0-1.0) for traces with no inbound parent decision")
		tracingInsecure    = flag.Bool("tracing-insecure", true, "use insecure gRPC for OTLP exporter (set false when collector requires TLS)")
	)
	flag.Parse()

	if *showVer {
		fmt.Printf("l7rp %s (%s)\n", Version, GitCommit)
		return nil
	}

	setupLogging(*logLevel)

	cfg, err := loadConfig(*configPath)
	if err != nil {
		return err
	}

	if *checkOnly {
		fmt.Printf("config OK: %d listener(s), %d pool(s), %d route(s)\n",
			len(cfg.Listeners), len(cfg.Pools), len(cfg.Routes))
		return nil
	}

	return serve(cfg, *configPath, *metricsBind, observability.TracingConfig{
		Endpoint:    *tracingEndpoint,
		ServiceName: "l7rp",
		SampleRatio: *tracingSampleRatio,
		Insecure:    *tracingInsecure,
	})
}

func loadConfig(path string) (*config.Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open config %s: %w", path, err)
	}
	defer f.Close()

	cfg, err := config.Load(f)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}
	return cfg, nil
}

func setupLogging(level string) {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: lvl})
	slog.SetDefault(slog.New(h))
}

// serve builds the runtime from cfg and runs until SIGINT/SIGTERM. SIGHUP
// triggers a config reload: routes and middleware can be updated in place;
// pool and listener definitions require a restart.
func serve(cfg *config.Config, configPath, metricsBind string, tracingCfg observability.TracingConfig) error {
	metrics := observability.NewMetrics()

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tracingShutdown, err := observability.SetupTracing(rootCtx, tracingCfg)
	if err != nil {
		return fmt.Errorf("tracing setup: %w", err)
	}
	defer func() {
		shutdownCtx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = tracingShutdown(shutdownCtx)
	}()

	rt, err := buildRuntime(rootCtx, cfg, metrics)
	if err != nil {
		return err
	}

	// Spin up the metrics endpoint on its own listener.
	var metricsSrv *http.Server
	if metricsBind != "" {
		metricsSrv = &http.Server{
			Addr:              metricsBind,
			Handler:           metrics.Handler(Version),
			ReadHeaderTimeout: 2 * time.Second,
			WriteTimeout:      5 * time.Second,
		}
		go func() {
			slog.Info("metrics listener serving", "addr", metricsBind)
			if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics listener error", "err", err)
			}
		}()
	}

	// Start every data listener in its own goroutine. Per-pool monitor
	// goroutines were already spawned inside buildRuntime so they can be
	// individually cancelled on SIGHUP.
	for _, ln := range rt.listeners {
		serveListener(rt, ln)
	}

	// Signal handling: SIGHUP reloads in place; SIGINT/SIGTERM begin drain.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	var shutdownSig os.Signal
sigLoop:
	for {
		s := <-sigCh
		switch s {
		case syscall.SIGHUP:
			reloadConfig(rt, configPath, metrics)
		default:
			shutdownSig = s
			break sigLoop
		}
	}
	slog.Info("shutdown signal received, draining", "signal", shutdownSig.String())

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer drainCancel()

	for _, ln := range rt.listeners {
		if err := ln.Shutdown(drainCtx); err != nil {
			slog.Error("listener shutdown error", "name", ln.Name, "err", err)
		}
	}
	if metricsSrv != nil {
		_ = metricsSrv.Shutdown(drainCtx)
	}
	cancel() // stop all health monitor goroutines (per-pool contexts derive from this)

	// Wait for each pool's monitor goroutine to exit before returning.
	for _, p := range rt.pools {
		if p.monitorDone != nil {
			<-p.monitorDone
		}
	}
	rt.listenerWG.Wait()
	slog.Info("l7rp stopped")
	return nil
}

// serveListener spawns the serve goroutine for a single listener and adds it
// to the runtime's per-listener WaitGroup. Used both at startup (buildRuntime
// → here) and on reload (when a new or replaced listener appears).
func serveListener(rt *runtime, ln *listener.Listener) {
	rt.listenerWG.Add(1)
	go func() {
		defer rt.listenerWG.Done()
		if err := ln.Serve(); err != nil {
			slog.Error("listener stopped with error", "name", ln.Name, "err", err)
		}
	}()
}

// runtime is the materialized object graph. The router is held behind an
// atomic.Pointer so that SIGHUP can swap in a fresh route table without
// disrupting listener sockets or in-flight requests.
//
// listeners is keyed by name so reload can identify specifically which
// listeners are new, changed, or removed. SO_REUSEPORT lets a new instance
// of a listener bind to the same (host, port) tuple while the previous
// instance is still draining — zero-downtime even for bind changes.
type runtime struct {
	listeners map[string]*listener.Listener
	pools     map[string]*runtimePool // mutated only by the SIGHUP serializer
	metrics   *observability.Metrics

	rootCtx context.Context // parent context for all per-pool monitor goroutines
	cfg     *config.Config  // last applied config
	router  atomic.Pointer[router.Router]

	// listenerWG tracks per-listener serve goroutines so shutdown can wait
	// for them to fully drain. Reload adds entries for new listeners; the
	// removed/replaced listeners' goroutines exit naturally on Shutdown.
	listenerWG *sync.WaitGroup
}

// newUpstreamTransport builds an http.Transport tuned for reverse-proxy use.
// The defaults from http.DefaultTransport are designed for general-purpose
// clients — in particular MaxIdleConnsPerHost=2, which causes ephemeral-port
// exhaustion under load because every request beyond the second forces a
// fresh dial. The values below give comfortable connection reuse at the cost
// of slightly higher memory.
//
// connectTimeout caps the dial; zero falls back to a sensible default.
// tlsCfg is applied to https:// upstreams; nil leaves Go's default behavior
// (system roots, no client cert, SNI from the URL host).
func newUpstreamTransport(connectTimeout time.Duration, tlsCfg *tls.Config) *http.Transport {
	if connectTimeout <= 0 {
		connectTimeout = 5 * time.Second
	}
	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   connectTimeout,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSClientConfig:       tlsCfg,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   connectTimeout,
		ExpectContinueTimeout: 1 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
	}
}

// loadUpstreamTLS materializes a *tls.Config from the YAML block. Returns
// (nil, nil) when the block is nil — caller treats that as "use Go defaults".
func loadUpstreamTLS(cfg *config.UpstreamTLSConfig) (*tls.Config, error) {
	if cfg == nil {
		return nil, nil
	}
	out := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         cfg.ServerName,
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // operator opt-in for self-signed dev backends
	}
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read upstream TLS CA %s: %w", cfg.CAFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("upstream TLS CA %s: no certificates parsed", cfg.CAFile)
		}
		out.RootCAs = pool
	}
	if cfg.ClientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.ClientCertFile, cfg.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("load mTLS client cert/key: %w", err)
		}
		out.Certificates = []tls.Certificate{cert}
	}
	return out, nil
}

// runtimePool bundles a pool's selector, upstreams, and (optional) health
// monitor with a per-pool cancellation handle. Per-pool cancellation lets
// SIGHUP gracefully retire monitors for pools that are removed or changed
// without disturbing other pools.
//
// transport is owned by the pool because connect_timeout is per-pool — each
// pool gets its own connection pool, scoped to the pool's upstream URLs.
type runtimePool struct {
	name          string
	upstreams     []*lb.Upstream
	selector      lb.Selector
	transport     *http.Transport
	monitor       *health.Monitor
	monitorCancel context.CancelFunc
	monitorDone   chan struct{} // closed when the monitor goroutine returns
}

func buildRuntime(rootCtx context.Context, cfg *config.Config, metrics *observability.Metrics) (*runtime, error) {
	rt := &runtime{
		pools:      make(map[string]*runtimePool, len(cfg.Pools)),
		listeners:  make(map[string]*listener.Listener, len(cfg.Listeners)),
		metrics:    metrics,
		cfg:        cfg,
		rootCtx:    rootCtx,
		listenerWG: &sync.WaitGroup{},
	}

	for _, p := range cfg.Pools {
		rp, err := buildPool(rootCtx, p, nil, metrics)
		if err != nil {
			return nil, fmt.Errorf("pool %q: %w", p.Name, err)
		}
		rt.pools[p.Name] = rp
	}

	routes, err := buildRoutes(cfg.Routes, rt.pools, metrics)
	if err != nil {
		return nil, err
	}

	rtr, err := router.NewRouter(routes)
	if err != nil {
		return nil, fmt.Errorf("router: %w", err)
	}
	rt.router.Store(rtr)

	if err := buildListeners(cfg.Listeners, rt, metrics); err != nil {
		return nil, err
	}

	return rt, nil
}

// buildPool materializes a single pool: upstream values, selector, and the
// per-pool health monitor goroutine. When `existingUpstreams` is non-nil
// (SIGHUP rebuild path), upstream pointers are reused for matching URLs so
// circuit-breaker state, EWMA latencies, and in-flight counters survive the
// reload — only the selector and monitor are rebuilt.
func buildPool(rootCtx context.Context, p config.PoolConfig, existingUpstreams []*lb.Upstream, metrics *observability.Metrics) (*runtimePool, error) {
	upstreams, err := buildUpstreamsReusing(p.Upstreams, existingUpstreams)
	if err != nil {
		return nil, err
	}
	sel, err := buildSelector(p.Selector, upstreams)
	if err != nil {
		return nil, fmt.Errorf("selector: %w", err)
	}
	tlsCfg, err := loadUpstreamTLS(p.UpstreamTLS)
	if err != nil {
		return nil, fmt.Errorf("upstream_tls: %w", err)
	}

	rp := &runtimePool{
		name:      p.Name,
		upstreams: upstreams,
		selector:  sel,
		transport: newUpstreamTransport(p.ConnectTimeout, tlsCfg),
	}

	if mon := health.NewMonitor(p.Name, upstreams, p.Health.Active, p.Health.Passive, tlsCfg); mon != nil {
		mon.OnTransition = func(poolName string, up *lb.Upstream, eligible bool, reason string) {
			to := "ineligible"
			if eligible {
				to = "eligible"
			}
			metrics.HealthTransitions.WithLabelValues(poolName, up.URL.String(), to, reason).Inc()
		}
		rp.monitor = mon
		monCtx, cancel := context.WithCancel(rootCtx)
		rp.monitorCancel = cancel
		rp.monitorDone = make(chan struct{})
		go func() {
			defer close(rp.monitorDone)
			mon.Run(monCtx)
		}()
	} else {
		// No active probing — start eligible so traffic can flow. Breaker is
		// the only safety net in this configuration.
		for _, u := range upstreams {
			u.Eligible.Store(true)
		}
		rp.monitorCancel = func() {}
	}

	return rp, nil
}

// buildUpstreamsReusing constructs upstream values for a pool, preserving any
// existing *lb.Upstream pointer whose URL is unchanged in the new config.
// Preserving the pointer preserves breaker state, EWMA latencies, and the
// in-flight counter — critical for not "amnesia"-resetting a healthy pool on
// a route-only reload.
func buildUpstreamsReusing(cfgs []config.UpstreamConfig, existing []*lb.Upstream) ([]*lb.Upstream, error) {
	byURL := make(map[string]*lb.Upstream, len(existing))
	for _, u := range existing {
		byURL[u.URL.String()] = u
	}

	out := make([]*lb.Upstream, 0, len(cfgs))
	for _, uc := range cfgs {
		u, err := url.Parse(uc.URL)
		if err != nil {
			return nil, fmt.Errorf("parse upstream URL %q: %w", uc.URL, err)
		}
		weight := uc.Weight
		if weight <= 0 {
			weight = 1
		}
		if reused, ok := byURL[u.String()]; ok {
			reused.Weight = weight // weight is the only mutable field we let move
			out = append(out, reused)
			continue
		}
		up := &lb.Upstream{
			URL:     u,
			Weight:  weight,
			Breaker: lb.NewCircuitBreaker(lb.DefaultCircuitBreakerConfig()),
		}
		out = append(out, up)
	}
	return out, nil
}

func buildSelector(cfg config.SelectorConfig, upstreams []*lb.Upstream) (lb.Selector, error) {
	algo := cfg.Algorithm
	if algo == "" {
		algo = "p2c-ewma"
	}
	switch algo {
	case "round-robin":
		return lb.NewRoundRobin(upstreams), nil
	case "weighted-rr":
		return lb.NewWeightedRoundRobin(upstreams), nil
	case "least-conn":
		return lb.NewLeastConnections(upstreams), nil
	case "p2c-ewma":
		half := cfg.EWMAHalfLife
		if half <= 0 {
			half = 5 * time.Second
		}
		return lb.NewP2C(upstreams, half), nil
	case "consistent-hash-bounded":
		eps := cfg.Epsilon
		if eps <= 0 {
			eps = 0.25
		}
		vnodes := cfg.VirtualNodes
		if vnodes <= 0 {
			vnodes = 128
		}
		extract := parseHashKey(cfg.HashKey)
		return lb.NewConsistentHashBounded(upstreams, vnodes, eps, extract), nil
	}
	return nil, fmt.Errorf("unknown algorithm %q", algo)
}

func parseHashKey(spec string) lb.HashKeyExtractor {
	switch {
	case spec == "client_ip" || spec == "":
		return lb.ClientIPKey
	case strings.HasPrefix(spec, "header:"):
		return lb.HeaderKey(strings.TrimPrefix(spec, "header:"))
	case strings.HasPrefix(spec, "cookie:"):
		return lb.CookieKey(strings.TrimPrefix(spec, "cookie:"))
	default:
		return lb.ClientIPKey
	}
}

func buildRoutes(cfgs []config.RouteConfig, pools map[string]*runtimePool, metrics *observability.Metrics) ([]*router.Route, error) {
	routes := make([]*router.Route, 0, len(cfgs))
	for _, rc := range cfgs {
		pool, ok := pools[rc.Pool]
		if !ok {
			return nil, fmt.Errorf("route %q references unknown pool %q", rc.Name, rc.Pool)
		}

		// Terminal handler is the upstream proxy. Wire passive scoring when
		// the pool has a monitor configured for it.
		terminal := upstream.NewProxy(pool.name, pool.selector, pool.transport, metrics)
		if pool.monitor != nil {
			terminal = terminal.WithPassiveRecorder(pool.monitor.RecordOutcome)
		}

		// Compose middleware chain in canonical order (operators choose
		// *which* middlewares apply, not in what order).
		mws := buildMiddleware(rc, metrics)
		handler := middleware.Chain(mws...)(terminal)

		// Wrap to observe end-to-end request metrics (outermost so it sees
		// the post-panic-recovery status).
		instrumented := observingHandler(handler, rc.Name, metrics)

		preds := make([]router.HeaderPredicate, 0, len(rc.HeaderPredicates))
		for _, p := range rc.HeaderPredicates {
			preds = append(preds, router.HeaderPredicate{Name: p.Name, Value: p.Value})
		}

		routes = append(routes, &router.Route{
			Name:             rc.Name,
			HostPattern:      rc.Host,
			PathPrefix:       rc.PathPrefix,
			HeaderPredicates: preds,
			Handler:          instrumented,
		})
	}
	return routes, nil
}

// buildMiddleware assembles the per-route middleware chain in canonical order.
// Universal middlewares (request-id, access-log, panic-recovery) wrap every
// route; rate-limit, retry, header-transform are conditional on per-route
// configuration.
func buildMiddleware(rc config.RouteConfig, metrics *observability.Metrics) []middleware.Middleware {
	// Slot the configured middlewares by kind so we can place them in
	// canonical order regardless of declaration order in YAML.
	var rateLimitMW, retryMW, headerMW, compressMW middleware.Middleware
	for _, mw := range rc.Middleware {
		switch {
		case mw.RateLimit != nil:
			rateLimitMW = middleware.RateLimit(toRateLimitConfig(*mw.RateLimit, rc.Name))
		case mw.Retry != nil:
			retryMW = middleware.Retry(toRetryConfig(*mw.Retry))
		case mw.HeaderTransform != nil:
			headerMW = middleware.HeaderTransform(toHeaderTransformConfig(*mw.HeaderTransform))
		case mw.Compress != nil:
			compressMW = middleware.Compress(middleware.CompressConfig{
				MinBytes:         mw.Compress.MinBytes,
				SkipContentTypes: mw.Compress.SkipContentTypes,
			})
		}
	}

	panicCfg := middleware.PanicConfig{
		OnRecover: func(location string) {
			metrics.Panics.WithLabelValues(location).Inc()
		},
	}

	ordered := []middleware.Middleware{
		middleware.RequestID(),
		middleware.AccessLog(rc.Name),
		middleware.PanicRecover(panicCfg),
	}
	if rateLimitMW != nil {
		ordered = append(ordered, rateLimitMW)
	}
	// Total-budget timeout sits inside rate-limit (so denied requests don't
	// consume budget) and outside retry (so retries share one wallclock
	// budget instead of stacking).
	if rc.Timeouts.Total > 0 {
		ordered = append(ordered, middleware.Timeout(rc.Timeouts.Total))
	}
	if retryMW != nil {
		ordered = append(ordered, retryMW)
	}
	if headerMW != nil {
		ordered = append(ordered, headerMW)
	}
	// Compression sits closest to the upstream proxy so it sees the final
	// response body (after header transforms have run) and so its
	// ResponseWriter wrapper is what the proxy writes to.
	if compressMW != nil {
		ordered = append(ordered, compressMW)
	}
	return ordered
}

func toRateLimitConfig(c config.RateLimitConfig, routeName string) middleware.RateLimitConfig {
	return middleware.RateLimitConfig{
		Scope:          c.Scope,
		RPS:            c.PerIPRPS,
		Burst:          c.Burst,
		TrustProxyHops: c.TrustProxyHops,
		RouteName:      routeName,
	}
}

func toHeaderTransformConfig(c config.HeaderTransformConfig) middleware.HeaderTransformConfig {
	return middleware.HeaderTransformConfig{
		AddRequest:     c.AddRequest,
		RemoveRequest:  c.RemoveRequest,
		AddResponse:    c.AddResponse,
		RemoveResponse: c.RemoveResponse,
	}
}

func toRetryConfig(c config.RetryConfig) middleware.RetryConfig {
	out := middleware.RetryConfig{
		MaxAttempts:          c.MaxAttempts,
		RetryMethods:         c.RetryMethods,
		RetryOn:              c.RetryOn,
		HedgeAfter:           c.HedgeAfter,
		HedgeAfterPercentile: c.HedgeAfterPercentile,
		HedgeableMethods:     c.HedgeableMethods,
		MaxReplayableBody:    c.MaxReplayableBody,
	}
	if len(out.RetryMethods) == 0 {
		out.RetryMethods = []string{"GET", "HEAD", "PUT", "DELETE"}
	}
	if len(out.RetryOn) == 0 {
		out.RetryOn = []int{502, 503, 504}
	}
	if len(out.HedgeableMethods) == 0 {
		out.HedgeableMethods = []string{"GET", "HEAD"}
	}
	if out.MaxReplayableBody == 0 {
		out.MaxReplayableBody = 64 * 1024
	}
	if out.MaxAttempts == 0 {
		out.MaxAttempts = 1
	}
	return out
}

// buildListeners materializes each listener entry, populating rt.listeners
// in place. Returns an error on the first bind failure; partial-success is
// not supported (the process exits on bind failure at startup).
func buildListeners(cfgs []config.ListenerConfig, rt *runtime, metrics *observability.Metrics) error {
	for _, lc := range cfgs {
		ln, err := newListenerForRuntime(lc, rt, metrics)
		if err != nil {
			return err
		}
		rt.listeners[lc.Name] = ln
	}
	return nil
}

// newListenerForRuntime is the single point of listener construction: it
// builds the per-listener handler closure (which reads the atomic router
// pointer per request) and instantiates the Listener.
func newListenerForRuntime(lc config.ListenerConfig, rt *runtime, metrics *observability.Metrics) (*listener.Listener, error) {
	listenerName := lc.Name
	return listener.New(lc, http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Trace context extraction first: inbound W3C `traceparent` (when
		// present) becomes the parent of the local root span.
		ctx := observability.ExtractRequestContext(req)
		ctx, span := observability.Tracer().Start(ctx, "proxy.request",
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.request.method", req.Method),
				attribute.String("url.path", req.URL.Path),
				attribute.String("server.address", req.Host),
				attribute.String("proxy.listener", listenerName),
			),
		)
		defer span.End()
		req = req.WithContext(ctx)

		// Atomic router load per request: SIGHUP can swap the pointer
		// without affecting in-flight requests (they each see whichever
		// snapshot was active at their entry).
		activeRouter := rt.router.Load()
		m, ok := activeRouter.Match(req)
		if !ok {
			metrics.NoRoute.WithLabelValues(listenerName).Inc()
			span.SetAttributes(attribute.Int("http.response.status_code", 404))
			http.NotFound(w, req)
			return
		}
		span.SetAttributes(observability.AttrRoute.String(m.Route.Name))
		m.Route.Handler.ServeHTTP(w, req)
	}))
}

// reloadConfig handles SIGHUP: re-read the config, diff pools and routes
// against the live runtime, retire any pools that disappeared, build any
// new pools, rebuild pools whose definitions changed (reusing upstream
// pointers for unchanged URLs so circuit-breaker state survives), then
// atomically swap the router.
//
// Listener definitions cannot change on reload — sockets are already bound
// and re-binding to potentially-conflicting addresses isn't safe inside a
// running process. Listener-change reloads are rejected with a clear error.
func reloadConfig(rt *runtime, configPath string, metrics *observability.Metrics) {
	start := time.Now()
	defer func() {
		metrics.ReloadDuration.Observe(time.Since(start).Seconds())
	}()

	newCfg, err := loadConfig(configPath)
	if err != nil {
		slog.Error("reload failed: parse/validate", "err", err)
		metrics.ConfigReloads.WithLabelValues("fail").Inc()
		return
	}

	// Diff listeners. SO_REUSEPORT lets a new listener bind to the same
	// (host, port) before the previous one finishes draining, so config
	// changes apply with no observable downtime.
	listenerChanges, err := diffListeners(rt, newCfg.Listeners, metrics)
	if err != nil {
		slog.Error("reload failed: listener diff", "err", err)
		metrics.ConfigReloads.WithLabelValues("fail").Inc()
		return
	}

	// Diff pools. Track which old pools we kept so we can retire the rest.
	newPools := make(map[string]*runtimePool, len(newCfg.Pools))
	oldPoolByName := indexPoolsByName(rt.cfg.Pools)

	var added, rebuilt, kept []string

	for _, p := range newCfg.Pools {
		existing, hadOld := rt.pools[p.Name]
		oldCfg, hadOldCfg := oldPoolByName[p.Name]

		if hadOld && hadOldCfg && reflect.DeepEqual(oldCfg, p) {
			// Unchanged pool — reuse intact.
			newPools[p.Name] = existing
			kept = append(kept, p.Name)
			continue
		}

		// New or changed: retire the old (if any) and build fresh.
		if hadOld {
			existing.monitorCancel()
			// Wait briefly for the old monitor to exit before starting the
			// replacement — otherwise two monitors briefly probe the same
			// upstream pointer on reuse.
			if existing.monitorDone != nil {
				select {
				case <-existing.monitorDone:
				case <-time.After(time.Second):
					slog.Warn("old pool monitor did not exit within 1s; proceeding with new monitor", "pool", p.Name)
				}
			}
			// Drop the old transport's idle connections so they don't linger
			// past their owning pool.
			if existing.transport != nil {
				existing.transport.CloseIdleConnections()
			}
			rebuilt = append(rebuilt, p.Name)
		} else {
			added = append(added, p.Name)
		}

		var existingUpstreams []*lb.Upstream
		if hadOld {
			existingUpstreams = existing.upstreams
		}
		rp, err := buildPool(rt.rootCtx, p, existingUpstreams, metrics)
		if err != nil {
			slog.Error("reload failed: build pool", "pool", p.Name, "err", err)
			metrics.ConfigReloads.WithLabelValues("fail").Inc()
			return
		}
		newPools[p.Name] = rp
	}

	// Retire pools that aren't in the new config.
	var removed []string
	for name, pool := range rt.pools {
		if _, kept := newPools[name]; !kept {
			pool.monitorCancel()
			if pool.transport != nil {
				pool.transport.CloseIdleConnections()
			}
			removed = append(removed, name)
		}
	}

	// Build the new route table against the new pool map.
	routes, err := buildRoutes(newCfg.Routes, newPools, metrics)
	if err != nil {
		slog.Error("reload failed: build routes", "err", err)
		metrics.ConfigReloads.WithLabelValues("fail").Inc()
		return
	}
	newRouter, err := router.NewRouter(routes)
	if err != nil {
		slog.Error("reload failed: build router", "err", err)
		metrics.ConfigReloads.WithLabelValues("fail").Inc()
		return
	}

	rt.pools = newPools
	rt.router.Store(newRouter)
	rt.cfg = newCfg

	// Apply listener changes only after the new router is live, so freshly-
	// added listeners route via the new config from their very first request.
	listenerChanges.apply(rt)

	metrics.ConfigReloads.WithLabelValues("success").Inc()
	slog.Info("config reloaded",
		slog.Int("routes", len(newCfg.Routes)),
		slog.Any("pools_added", added),
		slog.Any("pools_rebuilt", rebuilt),
		slog.Any("pools_kept", kept),
		slog.Any("pools_removed", removed),
		slog.Any("listeners_added", listenerChanges.added),
		slog.Any("listeners_replaced", listenerChanges.replaced),
		slog.Any("listeners_removed", listenerChanges.removed),
		slog.Int64("duration_ms", time.Since(start).Milliseconds()))
}

// listenerPlan is the deferred set of changes the listener diff produced.
// We build new listeners *before* taking the router-swap path so reload can
// fail cleanly (no partial state) and apply them *after* the router is live.
type listenerPlan struct {
	added    []string                      // names of brand-new listeners
	replaced []string                      // names of in-place-replaced listeners
	removed  []string                      // names of removed listeners
	newOnes  map[string]*listener.Listener // freshly-built listeners (added + replaced)
	retired  []*listener.Listener          // old instances to drain
}

func diffListeners(rt *runtime, newCfgs []config.ListenerConfig, metrics *observability.Metrics) (*listenerPlan, error) {
	plan := &listenerPlan{newOnes: make(map[string]*listener.Listener)}

	newByName := make(map[string]config.ListenerConfig, len(newCfgs))
	for _, lc := range newCfgs {
		newByName[lc.Name] = lc
	}
	oldByName := indexListenersByName(rt.cfg.Listeners)

	// Identify removed and replaced listeners up front.
	for _, oldLc := range rt.cfg.Listeners {
		newLc, exists := newByName[oldLc.Name]
		if !exists {
			plan.removed = append(plan.removed, oldLc.Name)
			plan.retired = append(plan.retired, rt.listeners[oldLc.Name])
			continue
		}
		if reflect.DeepEqual(oldLc, newLc) {
			continue // unchanged
		}
		// Replacement: build the new instance now so we can fail before
		// retiring the old.
		ln, err := newListenerForRuntime(newLc, rt, metrics)
		if err != nil {
			return nil, fmt.Errorf("listener %q: %w", newLc.Name, err)
		}
		plan.replaced = append(plan.replaced, newLc.Name)
		plan.newOnes[newLc.Name] = ln
		plan.retired = append(plan.retired, rt.listeners[oldLc.Name])
	}

	// New listeners — build now, start in apply.
	for _, newLc := range newCfgs {
		if _, existed := oldByName[newLc.Name]; existed {
			continue
		}
		ln, err := newListenerForRuntime(newLc, rt, metrics)
		if err != nil {
			return nil, fmt.Errorf("listener %q: %w", newLc.Name, err)
		}
		plan.added = append(plan.added, newLc.Name)
		plan.newOnes[newLc.Name] = ln
	}

	return plan, nil
}

// apply commits the listener plan after the router swap. New listeners
// start first (SO_REUSEPORT lets them bind on the same port as a draining
// predecessor); then we kick off shutdown for retired listeners and update
// rt.listeners. Drain of retired listeners is fire-and-forget — the per-
// listener Serve goroutine clears itself out and the listenerWG accounts
// for it on process shutdown.
func (p *listenerPlan) apply(rt *runtime) {
	// Start all new listeners.
	for name, ln := range p.newOnes {
		rt.listeners[name] = ln
		serveListener(rt, ln)
	}

	// Retire the old ones in the background — Shutdown returns when the
	// listener stops accepting and in-flight requests finish (up to the
	// drain timeout). Done() unblocks once Serve has returned, which closes
	// the listener's socket and lets the kernel reclaim the (host, port).
	if len(p.retired) == 0 {
		return
	}
	go func() {
		drainCtx, cancel := context.WithTimeout(rt.rootCtx, 30*time.Second)
		defer cancel()
		for _, oldLn := range p.retired {
			if oldLn == nil {
				continue
			}
			if err := oldLn.Shutdown(drainCtx); err != nil {
				slog.Error("listener drain error", "name", oldLn.Name, "err", err)
			}
		}
		for _, oldLn := range p.retired {
			if oldLn == nil {
				continue
			}
			select {
			case <-oldLn.Done():
			case <-drainCtx.Done():
				slog.Warn("listener did not drain in time", "name", oldLn.Name)
			}
		}
	}()
	// Remove retired from rt.listeners (do this synchronously so a subsequent
	// reload sees a consistent map immediately).
	for _, oldLn := range p.retired {
		if oldLn != nil {
			delete(rt.listeners, oldLn.Name)
		}
	}
}

func indexListenersByName(lcs []config.ListenerConfig) map[string]config.ListenerConfig {
	m := make(map[string]config.ListenerConfig, len(lcs))
	for _, lc := range lcs {
		m[lc.Name] = lc
	}
	return m
}

// indexPoolsByName builds a map of pool name → config, for diffing during
// reload.
func indexPoolsByName(pools []config.PoolConfig) map[string]config.PoolConfig {
	m := make(map[string]config.PoolConfig, len(pools))
	for _, p := range pools {
		m[p.Name] = p
	}
	return m
}

// observingHandler wraps a handler to record end-to-end request timing and
// status into the proxy_requests_total / proxy_request_duration_seconds
// counters.
func observingHandler(next http.Handler, route string, metrics *observability.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		metrics.ObserveRequest(route, r.Method, sw.status, time.Since(start))
	})
}

// statusWriter captures the response status for instrumentation. It deliberately
// does not buffer the response body — observation runs at end-of-request only.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (s *statusWriter) WriteHeader(code int) {
	if !s.wroteHeader {
		s.status = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusWriter) Write(p []byte) (int, error) {
	if !s.wroteHeader {
		s.wroteHeader = true
	}
	return s.ResponseWriter.Write(p)
}
