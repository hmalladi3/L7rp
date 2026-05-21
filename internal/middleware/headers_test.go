package middleware

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHeaders_AddAndRemoveRequest(t *testing.T) {
	t.Parallel()

	var seen http.Header
	handler := HeaderTransform(HeaderTransformConfig{
		AddRequest:    map[string]string{"X-Service": "api-v2"},
		RemoveRequest: []string{"X-Internal"},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Internal", "secret")
	r.RemoteAddr = "1.2.3.4:5678"
	handler.ServeHTTP(httptest.NewRecorder(), r)

	if seen.Get("X-Service") != "api-v2" {
		t.Errorf("AddRequest not applied: %q", seen.Get("X-Service"))
	}
	if seen.Get("X-Internal") != "" {
		t.Errorf("RemoveRequest not applied: %q", seen.Get("X-Internal"))
	}
}

func TestHeaders_AddAndRemoveResponse(t *testing.T) {
	t.Parallel()

	handler := HeaderTransform(HeaderTransformConfig{
		AddResponse:    map[string]string{"X-Frame-Options": "DENY"},
		RemoveResponse: []string{"Server"},
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Server", "leaky-server/1.0")
		w.WriteHeader(200)
	}))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))

	if got := w.Header().Get("X-Frame-Options"); got != "DENY" {
		t.Errorf("AddResponse not applied: %q", got)
	}
	if got := w.Header().Get("Server"); got != "" {
		t.Errorf("RemoveResponse not applied: %q", got)
	}
}

func TestHeaders_CRLFInjectionDefended(t *testing.T) {
	t.Parallel()

	var seen http.Header
	handler := HeaderTransform(HeaderTransformConfig{
		AddRequest: map[string]string{"X-Bad": "value\r\nX-Smuggled: yes"},
	})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "1.2.3.4:5678"
	handler.ServeHTTP(httptest.NewRecorder(), r)

	if seen.Get("X-Bad") != "" {
		t.Errorf("CRLF-tainted X-Bad was set: %q", seen.Get("X-Bad"))
	}
	if seen.Get("X-Smuggled") != "" {
		t.Errorf("smuggled header materialized: %q", seen.Get("X-Smuggled"))
	}
}

func TestHeaders_HopByHopStripped(t *testing.T) {
	t.Parallel()

	var seen http.Header
	handler := HeaderTransform(HeaderTransformConfig{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "1.2.3.4:5678"
	r.Header.Set("Connection", "close, X-Custom-Hop")
	r.Header.Set("Keep-Alive", "timeout=5")
	r.Header.Set("Proxy-Authorization", "Basic xyz")
	r.Header.Set("X-Custom-Hop", "should-be-stripped")
	r.Header.Set("X-Keep-Me", "should-stay")

	handler.ServeHTTP(httptest.NewRecorder(), r)

	for _, dropped := range []string{"Connection", "Keep-Alive", "Proxy-Authorization", "X-Custom-Hop"} {
		if seen.Get(dropped) != "" {
			t.Errorf("hop-by-hop header %q not stripped: %q", dropped, seen.Get(dropped))
		}
	}
	if seen.Get("X-Keep-Me") != "should-stay" {
		t.Errorf("non-hop-by-hop header was wrongly stripped: %q", seen.Get("X-Keep-Me"))
	}
}

func TestHeaders_ForwardedHeadersSetByDefault(t *testing.T) {
	t.Parallel()

	var seen http.Header
	handler := HeaderTransform(HeaderTransformConfig{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.Host = "api.example.com"
	r.RemoteAddr = "203.0.113.7:54321"
	handler.ServeHTTP(httptest.NewRecorder(), r)

	if got := seen.Get("X-Forwarded-For"); got != "203.0.113.7" {
		t.Errorf("X-Forwarded-For = %q, want 203.0.113.7", got)
	}
	if got := seen.Get("X-Real-IP"); got != "203.0.113.7" {
		t.Errorf("X-Real-IP = %q, want 203.0.113.7", got)
	}
	if got := seen.Get("X-Forwarded-Host"); got != "api.example.com" {
		t.Errorf("X-Forwarded-Host = %q, want api.example.com", got)
	}
	if got := seen.Get("X-Forwarded-Proto"); got != "http" {
		t.Errorf("X-Forwarded-Proto = %q, want http", got)
	}
}

func TestHeaders_ForwardedHeadersAppendedToExistingChain(t *testing.T) {
	t.Parallel()

	var seen http.Header
	handler := HeaderTransform(HeaderTransformConfig{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.Host = "api.example.com"
	r.RemoteAddr = "203.0.113.7:54321"
	r.Header.Set("X-Forwarded-For", "10.0.0.1, 10.0.0.2")
	handler.ServeHTTP(httptest.NewRecorder(), r)

	if got := seen.Get("X-Forwarded-For"); got != "10.0.0.1, 10.0.0.2, 203.0.113.7" {
		t.Errorf("XFF = %q, want %q", got, "10.0.0.1, 10.0.0.2, 203.0.113.7")
	}
}

func TestHeaders_HTTPSProtoWhenTLS(t *testing.T) {
	t.Parallel()

	var seen http.Header
	handler := HeaderTransform(HeaderTransformConfig{})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "1.2.3.4:5678"
	r.TLS = &tls.ConnectionState{} // simulate TLS termination
	handler.ServeHTTP(httptest.NewRecorder(), r)

	if got := seen.Get("X-Forwarded-Proto"); got != "https" {
		t.Errorf("X-Forwarded-Proto under TLS = %q, want https", got)
	}
}

func TestHeaders_DisableForwardedForSkipsXFF(t *testing.T) {
	t.Parallel()

	var seen http.Header
	handler := HeaderTransform(HeaderTransformConfig{DisableForwardedFor: true})(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
	}))

	r := httptest.NewRequest("GET", "/", nil)
	r.RemoteAddr = "203.0.113.7:54321"
	handler.ServeHTTP(httptest.NewRecorder(), r)

	if seen.Get("X-Forwarded-For") != "" {
		t.Errorf("XFF should not be set when DisableForwardedFor=true; got %q", seen.Get("X-Forwarded-For"))
	}
	if seen.Get("X-Real-IP") != "" {
		t.Errorf("X-Real-IP should not be set when DisableForwardedFor=true; got %q", seen.Get("X-Real-IP"))
	}
}
