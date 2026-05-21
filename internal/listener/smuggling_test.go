package listener

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSmuggling_RejectsCLAndTETogether(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Content-Length", "5")
	r.Header.Set("Transfer-Encoding", "chunked")

	if err := checkSmuggling(r); err == nil {
		t.Error("expected rejection when both CL and TE present")
	}
}

func TestSmuggling_RejectsMultipleCLHeaders(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Add("Content-Length", "5")
	r.Header.Add("Content-Length", "10")

	if err := checkSmuggling(r); err == nil {
		t.Error("expected rejection on multiple Content-Length headers")
	}
}

func TestSmuggling_RejectsCommaSeparatedCL(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Content-Length", "0, 200")

	if err := checkSmuggling(r); err == nil {
		t.Error("expected rejection on comma-separated Content-Length")
	}
}

func TestSmuggling_RejectsNonNumericCL(t *testing.T) {
	t.Parallel()

	cases := []string{"abc", "-5", " "}
	for _, cl := range cases {
		r := httptest.NewRequest("POST", "/", nil)
		r.Header.Set("Content-Length", cl)
		if err := checkSmuggling(r); err == nil {
			t.Errorf("Content-Length=%q: expected rejection", cl)
		}
	}
}

func TestSmuggling_RejectsUnsupportedTransferEncoding(t *testing.T) {
	t.Parallel()

	cases := []string{
		"gzip",
		"chunked, gzip",
		"x-custom",
	}
	for _, te := range cases {
		r := httptest.NewRequest("POST", "/", nil)
		r.Header.Set("Transfer-Encoding", te)
		if err := checkSmuggling(r); err == nil {
			t.Errorf("Transfer-Encoding=%q: expected rejection", te)
		}
	}
}

func TestSmuggling_AcceptsChunked(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Transfer-Encoding", "chunked")
	if err := checkSmuggling(r); err != nil {
		t.Errorf("chunked TE should be accepted; got %v", err)
	}
}

func TestSmuggling_AcceptsCleanRequest(t *testing.T) {
	t.Parallel()

	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Content-Length", "5")
	if err := checkSmuggling(r); err != nil {
		t.Errorf("clean POST should be accepted; got %v", err)
	}
}

func TestSmugglingHandler_Returns400(t *testing.T) {
	t.Parallel()

	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
	})
	h := smugglingHandler(next)

	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("Content-Length", "5")
	r.Header.Set("Transfer-Encoding", "chunked")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
	if called {
		t.Error("downstream handler should NOT run when smuggling check fails")
	}
}

func TestSmugglingHandler_PassesCleanRequest(t *testing.T) {
	t.Parallel()

	called := false
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		called = true
		w.WriteHeader(200)
	})
	h := smugglingHandler(next)

	r := httptest.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)

	if !called {
		t.Error("clean request was rejected; expected downstream to run")
	}
	if w.Code != 200 {
		t.Errorf("status = %d, want 200", w.Code)
	}
}
