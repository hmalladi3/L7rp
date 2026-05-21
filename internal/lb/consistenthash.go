package lb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"net/http"
	"sort"
	"sync"
)

// HashKeyExtractor extracts the bytes to be hashed for consistent-hash
// selection. Built-in extractors include HeaderKey, CookieKey, and
// ClientIPKey; operators compose these via configuration.
type HashKeyExtractor func(req *http.Request) []byte

// HeaderKey returns an extractor that reads the named header. If the header
// is absent, the extractor returns nil and the selector falls back to client
// IP.
func HeaderKey(name string) HashKeyExtractor {
	return func(req *http.Request) []byte {
		v := req.Header.Get(name)
		if v == "" {
			return nil
		}
		return []byte(v)
	}
}

// CookieKey returns an extractor that reads the named cookie's value.
func CookieKey(name string) HashKeyExtractor {
	return func(req *http.Request) []byte {
		c, err := req.Cookie(name)
		if err != nil {
			return nil
		}
		return []byte(c.Value)
	}
}

// ClientIPKey extracts the request's client IP for hashing. It serves both
// as a primary extractor (when the operator configures hash_key: client_ip)
// and as the fallback when a header or cookie extractor returns nil.
func ClientIPKey(req *http.Request) []byte {
	ra := req.RemoteAddr
	if i := lastColon(ra); i >= 0 {
		ra = ra[:i]
	}
	return []byte(ra)
}

func lastColon(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == ':' {
			return i
		}
	}
	return -1
}

// ConsistentHashBounded implements consistent hashing on a 64-bit ring with
// the Mirrokni-Thorup-Zadimoghaddam bounded-loads adjustment.
//
// Construction places each upstream at `virtualNodes` positions on the ring,
// computed by hashing (URL || index) per virtual node. A pick proceeds as:
//
//  1. Extract the hash key (with fallback to client IP if the primary extractor
//     yields nil).
//  2. Hash the key to a ring position h.
//  3. Walk clockwise. For each eligible upstream encountered, check whether
//     admitting one more request would exceed (1 + ε) × meanInflight; if not,
//     return that upstream.
//  4. If the entire ring is walked without finding a candidate under the cap,
//     return the eligible upstream with the lowest InFlight (the fallback rule).
//
// The bounded-loads invariant prevents the textbook consistent-hash failure
// mode where one "hot" hash range piles requests on a single shard while
// other shards sit idle.
type ConsistentHashBounded struct {
	upstreams []*Upstream
	ring      []ringEntry // sorted by pos
	epsilon   float64
	extract   HashKeyExtractor

	mu sync.Mutex // reserved for future ring rebuilds; not held on hot path
}

type ringEntry struct {
	pos      uint64
	upstream *Upstream
}

// NewConsistentHashBounded constructs a CH-BL selector. The ring is built once
// at construction; reads in Pick are lock-free against ring state.
func NewConsistentHashBounded(pool []*Upstream, virtualNodes int, epsilon float64, extract HashKeyExtractor) *ConsistentHashBounded {
	ch := &ConsistentHashBounded{
		upstreams: pool,
		epsilon:   epsilon,
		extract:   extract,
	}

	ring := make([]ringEntry, 0, len(pool)*virtualNodes)
	for _, u := range pool {
		urlBytes := []byte(u.URL.String())
		for i := 0; i < virtualNodes; i++ {
			ring = append(ring, ringEntry{
				pos:      hashVirtualNode(urlBytes, i),
				upstream: u,
			})
		}
	}
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].pos < ring[j].pos
	})
	ch.ring = ring
	return ch
}

// hashVirtualNode produces a stable ring position for (upstream URL, virtual
// node index). SHA256-truncated is used (not a faster non-cryptographic hash
// like FNV-1a) because typical inputs are short and structurally similar —
// "http://u0", "http://u1", … — and weaker hashes leave large gaps on the
// ring that produce visibly skewed distribution.
func hashVirtualNode(url []byte, index int) uint64 {
	h := sha256.New()
	h.Write(url)
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], uint32(index))
	h.Write(buf[:])
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}

// hashKey produces a 64-bit ring position for a request's extracted key.
// Same SHA256-truncated rationale as hashVirtualNode.
func hashKey(key []byte) uint64 {
	sum := sha256.Sum256(key)
	return binary.BigEndian.Uint64(sum[:8])
}

// Pick implements the bounded-loads consistent-hash selection.
func (ch *ConsistentHashBounded) Pick(ctx context.Context, req *http.Request, hint PickHint) (*Upstream, error) {
	eligible, release := eligibleSet(ch.upstreams, hint)
	defer release()

	switch len(eligible) {
	case 0:
		return nil, ErrNoEligibleUpstream
	case 1:
		return eligible[0], nil
	}

	// Extract hash key with fallback to client IP.
	key := ch.extract(req)
	if key == nil {
		key = ClientIPKey(req)
	}
	pos := hashKey(key)

	// Compute mean inflight over the eligible set, then derive the cap. The
	// floor at 1 ensures that an idle pool can still admit the first request.
	var totalInflight int64
	for _, u := range eligible {
		totalInflight += u.InFlight.Load()
	}
	meanInflight := float64(totalInflight) / float64(len(eligible))
	capacity := int64(math.Ceil((1 + ch.epsilon) * meanInflight))
	if capacity < 1 {
		capacity = 1
	}

	// Build a quick lookup of the eligible set for the ring walk. The map
	// allocation is the price for not maintaining a parallel eligible-projected
	// ring; for typical pool sizes (≤ a few dozen upstreams) it's negligible.
	eligibleLookup := make(map[*Upstream]struct{}, len(eligible))
	for _, u := range eligible {
		eligibleLookup[u] = struct{}{}
	}

	// Binary search for the first ring position ≥ pos; that's the clockwise
	// starting point.
	n := len(ch.ring)
	start := sort.Search(n, func(i int) bool { return ch.ring[i].pos >= pos })
	if start == n {
		start = 0
	}

	// Walk clockwise. Skip ring entries whose upstreams are ineligible or
	// already inspected. Return the first one within the load cap.
	visited := make(map[*Upstream]struct{}, len(eligible))
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		u := ch.ring[idx].upstream
		if _, ok := eligibleLookup[u]; !ok {
			continue
		}
		if _, seen := visited[u]; seen {
			continue
		}
		visited[u] = struct{}{}

		if u.InFlight.Load()+1 <= capacity {
			return u, nil
		}
	}

	// Fallback: every eligible upstream is over the cap. Return the one with
	// the lowest InFlight.
	var best *Upstream
	bestInflight := int64(math.MaxInt64)
	for _, u := range eligible {
		if v := u.InFlight.Load(); v < bestInflight {
			best = u
			bestInflight = v
		}
	}
	return best, nil
}
