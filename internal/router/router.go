// Package router implements the host + path + header request matcher.
//
// The router is built from a list of Route values into an immutable tree at
// config-load time. Matching is a deterministic descent: host first, then
// path, then header predicates. Precedence rules are explicit:
//
//   - Longest exact-label host suffix wins over wildcards.
//   - Within a host bucket, longest path prefix wins.
//   - Within a (host, path_prefix) bucket, predicate-bearing routes are
//     evaluated in configuration order; the first whose predicates all match
//     wins; a no-predicate route at the same key is the catch-all and must
//     be placed last (validation enforces this).
//
// Wildcards are only allowed at the leftmost host label and match exactly one
// label, per RFC 6125. Hosts are matched case-insensitively; paths are
// case-sensitive (HTTP-correct). Encoded slashes (%2F) do not split path
// segments.
//
// Construction validates the route list and returns an error on ambiguity
// (duplicate (host, path_prefix, predicates) tuples) or on a no-predicate
// route placed before a predicate-bearing route at the same (host, path).
package router

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
)

// Route is one entry in the routing table.
type Route struct {
	Name             string
	HostPattern      string // "api.example.com" or "*.example.com"
	PathPrefix       string
	HeaderPredicates []HeaderPredicate
	Handler          http.Handler // pre-composed middleware chain (set by config build)
}

// HeaderPredicate matches against the request headers. Exact-match by default.
type HeaderPredicate struct {
	Name  string
	Value string
}

// RouteMatch is the routing decision plus auxiliary data the upstream layer
// uses for path-prefix rewriting.
type RouteMatch struct {
	Route    *Route
	PathTail string // request path with PathPrefix removed
	Host     string // canonicalized: lowercased, port stripped
}

// Router is the immutable host+path+header matching tree built at config-load
// time. After construction, a Router is safe for concurrent reads.
type Router struct {
	// hostExact maps canonicalized hosts to their route bucket.
	hostExact map[string][]*Route

	// hostWildcards holds wildcard hosts indexed by the suffix (everything
	// after the leftmost "*."). Sorted by suffix length desc so the longest
	// match wins.
	hostWildcards []wildcardEntry
}

type wildcardEntry struct {
	suffix string // e.g., ".example.com" (no leading wildcard)
	routes []*Route
}

// NewRouter builds an immutable router from the given route list. Routes are
// validated and indexed.
func NewRouter(routes []*Route) (*Router, error) {
	if err := validate(routes); err != nil {
		return nil, err
	}

	r := &Router{
		hostExact: make(map[string][]*Route),
	}

	wildcardMap := make(map[string][]*Route) // suffix → routes
	for _, route := range routes {
		host := strings.ToLower(route.HostPattern)
		if strings.HasPrefix(host, "*.") {
			suffix := host[1:] // strip leading "*", keep the leading "."
			wildcardMap[suffix] = append(wildcardMap[suffix], route)
		} else {
			r.hostExact[host] = append(r.hostExact[host], route)
		}
	}

	// Sort each bucket: longest path prefix first; predicate-bearing first
	// within the same prefix.
	for h := range r.hostExact {
		sortRoutes(r.hostExact[h])
	}
	for suffix, list := range wildcardMap {
		sortRoutes(list)
		r.hostWildcards = append(r.hostWildcards, wildcardEntry{suffix: suffix, routes: list})
	}
	// Sort wildcards by suffix length desc so longest match is tried first.
	sort.Slice(r.hostWildcards, func(i, j int) bool {
		return len(r.hostWildcards[i].suffix) > len(r.hostWildcards[j].suffix)
	})

	return r, nil
}

// sortRoutes sorts a host's routes by path prefix length desc, then by
// predicate-presence (predicated routes first within the same prefix).
func sortRoutes(routes []*Route) {
	sort.SliceStable(routes, func(i, j int) bool {
		li, lj := len(routes[i].PathPrefix), len(routes[j].PathPrefix)
		if li != lj {
			return li > lj
		}
		// Same path length: predicated routes come first (catch-all last).
		return len(routes[i].HeaderPredicates) > len(routes[j].HeaderPredicates)
	})
}

// Match selects the route for a request. Returns the match and true on
// success; nil and false when no route matches.
func (r *Router) Match(req *http.Request) (*RouteMatch, bool) {
	host := canonicalizeHost(req.Host)
	path := req.URL.EscapedPath() // preserves %2F as %2F (does not decode)
	if path == "" {
		path = "/"
	}

	// Exact host match first.
	if list, ok := r.hostExact[host]; ok {
		if m := matchInBucket(list, req, path, host); m != nil {
			return m, true
		}
	}

	// Wildcard host fallback, longest-suffix first.
	for _, w := range r.hostWildcards {
		if !matchesWildcard(host, w.suffix) {
			continue
		}
		if m := matchInBucket(w.routes, req, path, host); m != nil {
			return m, true
		}
	}

	return nil, false
}

// matchesWildcard reports whether host matches a wildcard pattern with the
// given suffix (e.g., ".example.com"). The wildcard "*" matches exactly one
// label.
func matchesWildcard(host, suffix string) bool {
	if !strings.HasSuffix(host, suffix) {
		return false
	}
	// Everything before the suffix must be exactly one label (no dots).
	prefix := host[:len(host)-len(suffix)]
	if prefix == "" {
		return false // wildcard requires a label to match
	}
	return !strings.Contains(prefix, ".")
}

// matchInBucket walks a pre-sorted route bucket and returns the first match
// against path + predicates, or nil.
func matchInBucket(routes []*Route, req *http.Request, path, host string) *RouteMatch {
	for _, route := range routes {
		if !pathMatchesPrefix(path, route.PathPrefix) {
			continue
		}
		if !predicatesMatch(route.HeaderPredicates, req.Header) {
			continue
		}
		tail := stripPrefix(path, route.PathPrefix)
		return &RouteMatch{
			Route:    route,
			PathTail: tail,
			Host:     host,
		}
	}
	return nil
}

// pathMatchesPrefix implements the prefix semantics:
//   - "/v1"  matches "/v1", "/v1/", "/v1/foo" — not "/v1x" or "/v".
//   - "/v1/" matches "/v1/", "/v1/foo"        — not "/v1".
func pathMatchesPrefix(path, prefix string) bool {
	if strings.HasSuffix(prefix, "/") {
		return strings.HasPrefix(path, prefix)
	}
	// Prefix without trailing slash: equal, or path[len(prefix)] == '/'.
	if path == prefix {
		return true
	}
	if len(path) > len(prefix) && strings.HasPrefix(path, prefix) && path[len(prefix)] == '/' {
		return true
	}
	return false
}

// stripPrefix returns the request path with the route prefix removed.
func stripPrefix(path, prefix string) string {
	if strings.HasSuffix(prefix, "/") {
		// Keep one leading slash on the tail.
		return "/" + strings.TrimPrefix(path, prefix)
	}
	tail := strings.TrimPrefix(path, prefix)
	if tail == "" {
		return "/"
	}
	return tail
}

// predicatesMatch reports whether every predicate in the list is satisfied by
// the request headers. An empty predicate list matches always.
func predicatesMatch(predicates []HeaderPredicate, h http.Header) bool {
	for _, p := range predicates {
		values := h.Values(p.Name)
		matched := false
		for _, v := range values {
			if v == p.Value {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

// canonicalizeHost lowercases and strips the port suffix.
func canonicalizeHost(h string) string {
	h = strings.ToLower(h)
	if i := strings.LastIndexByte(h, ':'); i >= 0 {
		// Avoid stripping IPv6 brackets — for v1 we accept only IPv4/host.
		// IPv6 in Host header uses [::1]:port form; LastIndexByte(":") still
		// finds the port separator correctly in that form.
		h = h[:i]
	}
	return h
}

// validate checks the route list for the conditions documented at the package
// level: no duplicates and no unreachable predicated routes.
func validate(routes []*Route) error {
	// Detect exact-duplicate tuples.
	type key struct {
		host, prefix, preds string
	}
	seen := make(map[key]string)
	for _, r := range routes {
		k := key{host: strings.ToLower(r.HostPattern), prefix: r.PathPrefix, preds: predicatesKey(r.HeaderPredicates)}
		if name, dup := seen[k]; dup {
			return fmt.Errorf("router: duplicate route (host=%q, path_prefix=%q, predicates=%q): %q and %q",
				r.HostPattern, r.PathPrefix, k.preds, name, r.Name)
		}
		seen[k] = r.Name
	}

	// Detect unreachable predicated routes: a no-predicate route at (host,
	// prefix) placed before a predicated route at the same (host, prefix).
	type hp struct {
		host, prefix string
	}
	openCatchAll := make(map[hp]string)
	for _, r := range routes {
		h := hp{host: strings.ToLower(r.HostPattern), prefix: r.PathPrefix}
		if len(r.HeaderPredicates) == 0 {
			openCatchAll[h] = r.Name
			continue
		}
		if name, blocked := openCatchAll[h]; blocked {
			return fmt.Errorf("router: predicated route %q is unreachable behind catch-all route %q at (host=%q, path_prefix=%q)",
				r.Name, name, r.HostPattern, r.PathPrefix)
		}
	}
	return nil
}

// predicatesKey produces a stable string representation of a predicate list
// for duplicate detection.
func predicatesKey(ps []HeaderPredicate) string {
	if len(ps) == 0 {
		return ""
	}
	out := make([]string, 0, len(ps))
	for _, p := range ps {
		out = append(out, strings.ToLower(p.Name)+"="+p.Value)
	}
	sort.Strings(out)
	return strings.Join(out, ";")
}
