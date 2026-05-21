package middleware

import (
	"net/http"
	"strings"
)

// HeaderTransformConfig parameterizes the HeaderTransform middleware.
//
// AddRequest and AddResponse set or replace headers; RemoveRequest and
// RemoveResponse delete them. CRLF injection in declared add-values is
// rejected at config-load by the config package; this middleware also
// enforces the rule at runtime as defense in depth.
//
// DisableForwardedFor opts out of the X-Forwarded-* augmentation
// (X-Forwarded-For/Proto/Host, X-Real-IP). Operators who handle these
// themselves upstream can disable our defaults here.
type HeaderTransformConfig struct {
	AddRequest          map[string]string
	RemoveRequest       []string
	AddResponse         map[string]string
	RemoveResponse      []string
	DisableForwardedFor bool
	TrustProxyHops      int
}

// HeaderTransform applies the configured header rewrites to the request and
// response, and (by default) sets the X-Forwarded-* headers that downstream
// services rely on.
//
// The middleware sits between retry and upstream-proxy; the same transforms
// apply on every retry attempt with consistent XFF values (they're computed
// from request state, which doesn't mutate across attempts).
func HeaderTransform(cfg HeaderTransformConfig) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			stripHopByHopHeaders(r.Header)

			for _, name := range cfg.RemoveRequest {
				r.Header.Del(name)
			}
			for name, value := range cfg.AddRequest {
				if !validHeaderValue(value) {
					continue // CRLF defense
				}
				r.Header.Set(name, value)
			}

			if !cfg.DisableForwardedFor {
				setForwardedHeaders(r, cfg.TrustProxyHops)
			}

			rw := &responseHeaderRewriter{ResponseWriter: w, cfg: &cfg}
			next.ServeHTTP(rw, r)
		})
	}
}

// hopByHopHeaderNames are the headers RFC 7230 §6.1 designates as not
// proxyable end-to-end. They get stripped before we hand the request off to
// the upstream. (Upgrade is intentionally absent — the WebSocket upgrade
// path needs it to propagate.)
var hopByHopHeaderNames = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"TE",
	"Trailer",
	"Transfer-Encoding",
}

func stripHopByHopHeaders(h http.Header) {
	// Per RFC 7230, the Connection header lists additional headers to treat
	// as hop-by-hop. Strip them first, then the standard set.
	if conn := h.Get("Connection"); conn != "" {
		for _, name := range strings.Split(conn, ",") {
			h.Del(strings.TrimSpace(name))
		}
	}
	for _, name := range hopByHopHeaderNames {
		h.Del(name)
	}
}

// validHeaderValue rejects CR or LF anywhere in the value — the basic
// header-injection defense. We strip values rather than rejecting requests
// because the configured value comes from the operator (already validated at
// config-load); this is belt-and-suspenders for any dynamic value path that
// might land here in v1.x.
func validHeaderValue(v string) bool {
	return !strings.ContainsAny(v, "\r\n")
}

// setForwardedHeaders applies the conventional X-Forwarded-* augmentation.
// X-Forwarded-For appends the client IP to any existing chain; the others
// are set only when absent so an upstream-side proxy can override.
func setForwardedHeaders(r *http.Request, trustHops int) {
	cIP := clientIPWithTrust(r, trustHops)
	if cIP == "" {
		return
	}

	if existing := r.Header.Get("X-Forwarded-For"); existing != "" {
		r.Header.Set("X-Forwarded-For", existing+", "+cIP)
	} else {
		r.Header.Set("X-Forwarded-For", cIP)
	}
	if r.Header.Get("X-Real-IP") == "" {
		r.Header.Set("X-Real-IP", cIP)
	}
	if r.Header.Get("X-Forwarded-Host") == "" && r.Host != "" {
		r.Header.Set("X-Forwarded-Host", r.Host)
	}

	proto := "http"
	if r.TLS != nil {
		proto = "https"
	}
	r.Header.Set("X-Forwarded-Proto", proto)
}

// responseHeaderRewriter applies the configured response-side header
// transforms just before the status line is written.
type responseHeaderRewriter struct {
	http.ResponseWriter
	cfg     *HeaderTransformConfig
	applied bool
}

func (r *responseHeaderRewriter) WriteHeader(code int) {
	r.applyOnce()
	r.ResponseWriter.WriteHeader(code)
}

func (r *responseHeaderRewriter) Write(p []byte) (int, error) {
	r.applyOnce()
	return r.ResponseWriter.Write(p)
}

func (r *responseHeaderRewriter) applyOnce() {
	if r.applied {
		return
	}
	r.applied = true
	for _, name := range r.cfg.RemoveResponse {
		r.Header().Del(name)
	}
	for name, value := range r.cfg.AddResponse {
		if !validHeaderValue(value) {
			continue
		}
		r.Header().Set(name, value)
	}
}
