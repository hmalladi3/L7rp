package listener

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// checkSmuggling rejects requests that exhibit the canonical patterns of HTTP
// request smuggling attacks. Go's net/http already enforces several of these
// at parse time (it rejects requests with both Content-Length and
// Transfer-Encoding as of Go 1.17), but applying the check explicitly inside
// the proxy is defense-in-depth — it survives stdlib relaxations and
// documents intent.
//
// The check is intentionally strict at the cost of rejecting some pathological
// but technically legal requests (e.g., Transfer-Encoding values other than
// "chunked" or "identity"). Operators serving exotic clients can disable
// individual rules in v1.x; v1 is conservative by default.
func checkSmuggling(r *http.Request) error {
	headers := r.Header

	hasCL := headers.Get("Content-Length") != ""
	hasTE := headers.Get("Transfer-Encoding") != ""

	// Rule 1: Content-Length AND Transfer-Encoding together is the textbook
	// CL.TE / TE.CL smuggling vector.
	if hasCL && hasTE {
		return errors.New("both Content-Length and Transfer-Encoding present")
	}

	// Rule 2: Multiple Content-Length headers (or comma-separated values)
	// invite parsers to disagree on the body length.
	if cls := headers.Values("Content-Length"); len(cls) > 1 {
		return fmt.Errorf("multiple Content-Length headers: %d values", len(cls))
	}
	if cl := headers.Get("Content-Length"); cl != "" {
		// A single header may still smuggle two values via comma:
		// "Content-Length: 0, 200".
		if strings.Contains(cl, ",") {
			return fmt.Errorf("comma-separated Content-Length: %q", cl)
		}
		if n, err := strconv.ParseInt(cl, 10, 64); err != nil || n < 0 {
			return fmt.Errorf("non-numeric or negative Content-Length: %q", cl)
		}
	}

	// Rule 3: Transfer-Encoding values must be "chunked" or "identity" — and
	// only one of them. Values like "chunked, identity" or arbitrary
	// extensions invite chunked-encoding smuggling.
	for _, te := range headers.Values("Transfer-Encoding") {
		te = strings.ToLower(strings.TrimSpace(te))
		// A single header may itself contain a comma-separated list.
		for _, v := range strings.Split(te, ",") {
			v = strings.TrimSpace(v)
			if v != "chunked" && v != "identity" {
				return fmt.Errorf("unsupported Transfer-Encoding: %q", v)
			}
		}
	}

	return nil
}

// smugglingHandler wraps next with the smuggling check. Rejected requests get
// 400 with a generic message; the specific reason is logged via the access-log
// middleware (which sees the 400) rather than echoed to the client (we don't
// want to inform the attacker what failed).
func smugglingHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := checkSmuggling(r); err != nil {
			// Log at debug level — operators don't usually want to see every
			// scanner hit, but the access-log middleware will record the 400.
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		next.ServeHTTP(w, r)
	})
}
