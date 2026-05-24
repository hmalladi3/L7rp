package middleware

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

const repeatedBody = "This is the body that will be compressed. It needs to be reasonably long to exceed the default minimum-bytes threshold so the middleware decides compression is worthwhile.\n"

func makeHandler(body string, contentType string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if contentType != "" {
			w.Header().Set("Content-Type", contentType)
		}
		w.Header().Set("Content-Length", "9999") // intentionally wrong; should be stripped on compress
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	})
}

func TestCompress_GzipAccepted(t *testing.T) {
	t.Parallel()

	h := Compress(CompressConfig{})(makeHandler(strings.Repeat(repeatedBody, 10), "text/plain"))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if got := w.Header().Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	if w.Header().Get("Content-Length") != "" {
		t.Errorf("Content-Length should be stripped when body is compressed")
	}
	if got := w.Header().Get("Vary"); got != "Accept-Encoding" {
		t.Errorf("Vary = %q, want Accept-Encoding", got)
	}

	gr, err := gzip.NewReader(w.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	decoded, err := io.ReadAll(gr)
	if err != nil {
		t.Fatalf("read decoded: %v", err)
	}
	want := strings.Repeat(repeatedBody, 10)
	if string(decoded) != want {
		t.Errorf("decoded body mismatch (%d vs %d bytes)", len(decoded), len(want))
	}
}

func TestCompress_BrotliPreferredOverGzip(t *testing.T) {
	t.Parallel()

	h := Compress(CompressConfig{})(makeHandler(strings.Repeat(repeatedBody, 10), "text/plain"))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip, br")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if got := w.Header().Get("Content-Encoding"); got != "br" {
		t.Fatalf("Content-Encoding = %q, want br (brotli should beat gzip on a tie)", got)
	}
	dec := brotli.NewReader(bytes.NewReader(w.Body.Bytes()))
	decoded, err := io.ReadAll(dec)
	if err != nil {
		t.Fatalf("brotli decode: %v", err)
	}
	if !strings.HasPrefix(string(decoded), "This is the body") {
		t.Errorf("decoded prefix unexpected: %q", string(decoded[:32]))
	}
}

func TestCompress_ZstdAccepted(t *testing.T) {
	t.Parallel()

	h := Compress(CompressConfig{})(makeHandler(strings.Repeat(repeatedBody, 10), "text/plain"))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "zstd")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if got := w.Header().Get("Content-Encoding"); got != "zstd" {
		t.Fatalf("Content-Encoding = %q, want zstd", got)
	}
	dec, err := zstd.NewReader(bytes.NewReader(w.Body.Bytes()))
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer dec.Close()
	decoded, _ := io.ReadAll(dec)
	if !strings.HasPrefix(string(decoded), "This is the body") {
		t.Errorf("decoded prefix unexpected: %q", string(decoded[:32]))
	}
}

func TestCompress_QValueRespected(t *testing.T) {
	t.Parallel()

	h := Compress(CompressConfig{})(makeHandler(strings.Repeat(repeatedBody, 10), "text/plain"))

	// Client says "I accept br but really do not want it; gzip is fine".
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "br;q=0.1, gzip;q=1.0")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if got := w.Header().Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip (higher q-value wins)", got)
	}
}

func TestCompress_BelowThresholdPassthrough(t *testing.T) {
	t.Parallel()

	h := Compress(CompressConfig{MinBytes: 1024})(makeHandler("short", "text/plain"))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip, br, zstd")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if got := w.Header().Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q, want empty for sub-threshold body", got)
	}
	if w.Body.String() != "short" {
		t.Errorf("body = %q, want %q", w.Body.String(), "short")
	}
}

func TestCompress_SkipsAlreadyCompressedContentType(t *testing.T) {
	t.Parallel()

	h := Compress(CompressConfig{})(makeHandler(strings.Repeat("x", 4096), "image/png"))

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "gzip, br")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if got := w.Header().Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q, want empty for image/png", got)
	}
}

func TestCompress_NoAcceptEncodingPassthrough(t *testing.T) {
	t.Parallel()

	body := strings.Repeat(repeatedBody, 10)
	h := Compress(CompressConfig{})(makeHandler(body, "text/plain"))

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if got := w.Header().Get("Content-Encoding"); got != "" {
		t.Errorf("Content-Encoding = %q, want empty when client doesn't ask", got)
	}
	if w.Body.String() != body {
		t.Errorf("body changed despite no Accept-Encoding")
	}
}

func TestCompress_PreservesPreExistingEncoding(t *testing.T) {
	t.Parallel()

	// Simulate an upstream that pre-compressed its response.
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		// Body is opaque to us; just send something.
		_, _ = w.Write([]byte(strings.Repeat("opaque", 200)))
	})
	h := Compress(CompressConfig{})(handler)

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Accept-Encoding", "br")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if got := w.Header().Get("Content-Encoding"); got != "gzip" {
		t.Errorf("Content-Encoding = %q, want gzip (upstream encoding preserved)", got)
	}
}
