package middleware

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

// captureLogs swaps the default slog handler for one writing JSON into the
// returned buffer for the duration of the test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	prior := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(prior) })
	return &buf
}

func TestAccessLog_EmitsOneRecord(t *testing.T) {
	buf := captureLogs(t)
	handler := AccessLog("api")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("hello"))
	}))

	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest("GET", "/v1/orders", nil))

	rec := parseLastJSON(t, buf)
	if got, want := rec["msg"], "request_completed"; got != want {
		t.Errorf("msg = %v, want %q", got, want)
	}
	if got, want := rec["route"], "api"; got != want {
		t.Errorf("route = %v, want %q", got, want)
	}
	if got := rec["status"]; got != float64(200) {
		t.Errorf("status = %v, want 200", got)
	}
	if got := rec["bytes_out"]; got != float64(5) {
		t.Errorf("bytes_out = %v, want 5", got)
	}
	if got, want := rec["path"], "/v1/orders"; got != want {
		t.Errorf("path = %v, want %q", got, want)
	}
}

func TestAccessLog_LevelByStatus(t *testing.T) {
	cases := []struct {
		status    int
		wantLevel string
	}{
		{200, "INFO"},
		{301, "INFO"},
		{404, "WARN"},
		{500, "ERROR"},
		{503, "ERROR"},
	}
	for _, c := range cases {
		buf := captureLogs(t)
		handler := AccessLog("api")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(c.status)
		}))
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))
		rec := parseLastJSON(t, buf)
		if rec["level"] != c.wantLevel {
			t.Errorf("status %d: level = %v, want %q", c.status, rec["level"], c.wantLevel)
		}
	}
}

func TestAccessLog_IncludesRequestIDWhenPresent(t *testing.T) {
	buf := captureLogs(t)
	chain := RequestID()(AccessLog("api")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	})))

	w := httptest.NewRecorder()
	chain.ServeHTTP(w, httptest.NewRequest("GET", "/", nil))

	rec := parseLastJSON(t, buf)
	if rec["request_id"] == nil || rec["request_id"] == "" {
		t.Error("request_id field missing from access-log record")
	}
}

func TestAccessLog_DefaultStatus200WhenHandlerWritesNothing(t *testing.T) {
	buf := captureLogs(t)
	handler := AccessLog("api")(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// no explicit WriteHeader; Go's default is 200
	}))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	rec := parseLastJSON(t, buf)
	if rec["status"] != float64(200) {
		t.Errorf("status = %v, want 200", rec["status"])
	}
}

// parseLastJSON extracts the last JSON record written to buf. slog's JSON
// handler emits one record per line terminated by '\n', so we find the last
// non-empty line and decode it.
func parseLastJSON(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	lines := bytes.Split(bytes.TrimRight(buf.Bytes(), "\n"), []byte{'\n'})
	if len(lines) == 0 || len(lines[len(lines)-1]) == 0 {
		t.Fatalf("no log records captured; buffer: %q", buf.String())
	}
	last := lines[len(lines)-1]
	var rec map[string]any
	if err := json.Unmarshal(last, &rec); err != nil {
		t.Fatalf("parse JSON %q: %v", last, err)
	}
	return rec
}
