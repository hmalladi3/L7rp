package router

import (
	"net/http/httptest"
	"testing"
)

func buildRouter(t *testing.T, routes []*Route) *Router {
	t.Helper()
	r, err := NewRouter(routes)
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return r
}

func TestRouter_ExactHostMatch(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "api", HostPattern: "api.example.com", PathPrefix: "/"},
	})

	req := httptest.NewRequest("GET", "http://api.example.com/", nil)
	req.Host = "api.example.com"

	m, ok := r.Match(req)
	if !ok || m.Route.Name != "api" {
		t.Errorf("expected match on api.example.com; got match=%v ok=%v", m, ok)
	}
}

func TestRouter_WildcardHostMatchesOneLabel(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "wild", HostPattern: "*.example.com", PathPrefix: "/"},
	})

	cases := []struct {
		host  string
		match bool
	}{
		{"api.example.com", true},
		{"www.example.com", true},
		{"v2.api.example.com", false}, // wildcard matches one label
		{"example.com", false},        // bare host doesn't match wildcard
	}
	for _, c := range cases {
		req := httptest.NewRequest("GET", "/", nil)
		req.Host = c.host
		_, ok := r.Match(req)
		if ok != c.match {
			t.Errorf("host %q: match = %v, want %v", c.host, ok, c.match)
		}
	}
}

func TestRouter_ExactBeatsWildcard(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "wild", HostPattern: "*.example.com", PathPrefix: "/"},
		{Name: "specific", HostPattern: "api.example.com", PathPrefix: "/"},
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "api.example.com"

	m, ok := r.Match(req)
	if !ok || m.Route.Name != "specific" {
		t.Errorf("exact match should win over wildcard; got %v", m)
	}
}

func TestRouter_LongestPathPrefixWins(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "v1", HostPattern: "api.example.com", PathPrefix: "/v1/"},
		{Name: "v1-orders", HostPattern: "api.example.com", PathPrefix: "/v1/orders/"},
	})

	req := httptest.NewRequest("GET", "/v1/orders/42", nil)
	req.Host = "api.example.com"

	m, ok := r.Match(req)
	if !ok || m.Route.Name != "v1-orders" {
		t.Errorf("longest prefix should win; got %v", m)
	}
}

func TestRouter_HeaderPredicates(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{
			Name:             "tenant-a",
			HostPattern:      "api.example.com",
			PathPrefix:       "/",
			HeaderPredicates: []HeaderPredicate{{Name: "X-Tenant", Value: "a"}},
		},
		{
			Name:             "tenant-b",
			HostPattern:      "api.example.com",
			PathPrefix:       "/",
			HeaderPredicates: []HeaderPredicate{{Name: "X-Tenant", Value: "b"}},
		},
		{
			Name:        "default",
			HostPattern: "api.example.com",
			PathPrefix:  "/",
		},
	})

	cases := []struct {
		tenant string
		want   string
	}{
		{"a", "tenant-a"},
		{"b", "tenant-b"},
		{"c", "default"},
		{"", "default"},
	}
	for _, c := range cases {
		req := httptest.NewRequest("GET", "/", nil)
		req.Host = "api.example.com"
		if c.tenant != "" {
			req.Header.Set("X-Tenant", c.tenant)
		}
		m, ok := r.Match(req)
		if !ok || m.Route.Name != c.want {
			t.Errorf("X-Tenant=%q: got %v, want %s", c.tenant, m, c.want)
		}
	}
}

func TestRouter_HostPortStripped(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "x", HostPattern: "api.example.com", PathPrefix: "/"},
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "api.example.com:443"

	m, ok := r.Match(req)
	if !ok || m.Route.Name != "x" {
		t.Errorf("port should be stripped; got %v ok=%v", m, ok)
	}
}

func TestRouter_HostCaseInsensitive(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "x", HostPattern: "api.example.com", PathPrefix: "/"},
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "API.EXAMPLE.COM"

	m, ok := r.Match(req)
	if !ok || m.Route.Name != "x" {
		t.Errorf("hosts should be case-insensitive; got %v", m)
	}
}

func TestRouter_PathCaseSensitive(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "lower", HostPattern: "x", PathPrefix: "/lower/"},
	})

	req := httptest.NewRequest("GET", "/LOWER/foo", nil)
	req.Host = "x"

	if _, ok := r.Match(req); ok {
		t.Error("paths should be case-sensitive; matched anyway")
	}
}

func TestRouter_EncodedSlashDoesNotSplitSegments(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "users", HostPattern: "x", PathPrefix: "/users/"},
		{Name: "users-admin", HostPattern: "x", PathPrefix: "/users/admin"},
	})

	// /users/admin matches "users-admin".
	req := httptest.NewRequest("GET", "/users/admin", nil)
	req.Host = "x"
	if m, _ := r.Match(req); m == nil || m.Route.Name != "users-admin" {
		t.Errorf("/users/admin: got %v, want users-admin", m)
	}

	// /users/%2Fadmin is one segment "%2Fadmin" — does NOT route to users-admin.
	req = httptest.NewRequest("GET", "/users/%2Fadmin", nil)
	req.Host = "x"
	if m, _ := r.Match(req); m == nil || m.Route.Name != "users" {
		t.Errorf("/users/%%2Fadmin: encoded slash split incorrectly; got %v, want users", m)
	}
}

func TestRouter_PrefixWithoutTrailingSlash(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "v1", HostPattern: "x", PathPrefix: "/v1"},
	})

	cases := map[string]bool{
		"/v1":     true,
		"/v1/":    true,
		"/v1/foo": true,
		"/v1x":    false, // matches must respect segment boundary
		"/v":      false,
	}
	for path, want := range cases {
		req := httptest.NewRequest("GET", path, nil)
		req.Host = "x"
		_, ok := r.Match(req)
		if ok != want {
			t.Errorf("path %q: match = %v, want %v", path, ok, want)
		}
	}
}

func TestRouter_PrefixWithTrailingSlash(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "v1", HostPattern: "x", PathPrefix: "/v1/"},
	})

	cases := map[string]bool{
		"/v1/":    true,
		"/v1/foo": true,
		"/v1":     false, // bare /v1 does NOT match /v1/
	}
	for path, want := range cases {
		req := httptest.NewRequest("GET", path, nil)
		req.Host = "x"
		_, ok := r.Match(req)
		if ok != want {
			t.Errorf("path %q: match = %v, want %v", path, ok, want)
		}
	}
}

func TestRouter_NoMatchReturnsFalse(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "x", HostPattern: "x.com", PathPrefix: "/"},
	})

	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "y.com"

	if _, ok := r.Match(req); ok {
		t.Error("expected no match")
	}
}

func TestRouter_PathTailPopulated(t *testing.T) {
	t.Parallel()

	r := buildRouter(t, []*Route{
		{Name: "v1", HostPattern: "x", PathPrefix: "/v1"},
	})

	req := httptest.NewRequest("GET", "/v1/orders/42", nil)
	req.Host = "x"

	m, ok := r.Match(req)
	if !ok {
		t.Fatal("no match")
	}
	if m.PathTail != "/orders/42" {
		t.Errorf("PathTail = %q, want %q", m.PathTail, "/orders/42")
	}
}

func TestRouter_RejectsDuplicateRoutes(t *testing.T) {
	t.Parallel()

	routes := []*Route{
		{Name: "r1", HostPattern: "x", PathPrefix: "/"},
		{Name: "r2", HostPattern: "x", PathPrefix: "/"},
	}
	if _, err := NewRouter(routes); err == nil {
		t.Error("expected error on duplicate (host, path, predicates) tuple")
	}
}

func TestRouter_RejectsUnreachablePredicatedRoute(t *testing.T) {
	t.Parallel()

	// No-predicate route in front makes the predicated route unreachable.
	routes := []*Route{
		{Name: "open", HostPattern: "x", PathPrefix: "/"},
		{
			Name: "tenant", HostPattern: "x", PathPrefix: "/",
			HeaderPredicates: []HeaderPredicate{{Name: "X-Tenant", Value: "a"}},
		},
	}
	if _, err := NewRouter(routes); err == nil {
		t.Error("expected error: predicated route is unreachable behind no-predicate route")
	}
}
