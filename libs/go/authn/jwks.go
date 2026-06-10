package authn

import (
	"context"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"
)

// jwksTTL is how long a fetched JWKS document is trusted before the next
// lookup forces a refresh. Google rotates its signing keys on the order of
// days and serves generous Cache-Control max-age values; 12h keeps us well
// inside the overlap window during which old and new kids are both published.
const jwksTTL = 12 * time.Hour

// jwksMaxBody bounds the JWKS response read — the real document is ~1.6 KiB,
// so 1 MiB is pure paranoia against a misconfigured GOOGLE_JWKS_URL pointing
// at something enormous.
const jwksMaxBody = 1 << 20

// jwksCache is a kid-indexed cache of the RSA public keys published at a JWKS
// URL. Lookups are served from memory while the document is inside its TTL;
// an unknown kid (Google key rotation) or an expired document triggers a
// refetch. Refetches are single-flight: concurrent lookups that all miss
// serialize on fetchMu and every waiter after the first re-checks the cache
// instead of issuing its own HTTP request.
type jwksCache struct {
	url    string
	client *http.Client

	mu        sync.RWMutex // guards keys + fetchedAt
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time

	fetchMu sync.Mutex // single-flight: at most one in-flight JWKS fetch
}

func newJWKSCache(url string) *jwksCache {
	return &jwksCache{
		url:    url,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// key returns the RSA public key for kid, refreshing the JWKS document when
// the kid is unknown or the cached document has outlived its TTL.
func (c *jwksCache) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	if pub, ok := c.lookup(kid); ok {
		return pub, nil
	}

	// Miss (unknown kid or stale document) — refresh, single-flight.
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

	// Another goroutine may have refreshed while we waited on fetchMu;
	// re-check before issuing our own request.
	if pub, ok := c.lookup(kid); ok {
		return pub, nil
	}

	if err := c.fetch(ctx); err != nil {
		return nil, err
	}

	c.mu.RLock()
	pub, ok := c.keys[kid]
	c.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("jwks: no key for kid %q after refresh", kid)
	}
	return pub, nil
}

// lookup serves a kid from the cache, honoring the TTL. A hit on a stale
// document is treated as a miss so the caller refreshes.
func (c *jwksCache) lookup(kid string) (*rsa.PublicKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if time.Since(c.fetchedAt) > jwksTTL {
		return nil, false
	}
	pub, ok := c.keys[kid]
	return pub, ok
}

// jwk is one entry of the RFC 7517 key set as Google publishes it.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"` // base64url modulus
	E   string `json:"e"` // base64url exponent
}

// fetch downloads and parses the JWKS document, replacing the cached key map.
// Callers must hold fetchMu.
func (c *jwksCache) fetch(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.url, nil)
	if err != nil {
		return fmt.Errorf("jwks: build request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("jwks: fetch %s: %w", c.url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("jwks: fetch %s: unexpected status %d", c.url, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, jwksMaxBody))
	if err != nil {
		return fmt.Errorf("jwks: read body: %w", err)
	}

	var doc struct {
		Keys []jwk `json:"keys"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return fmt.Errorf("jwks: decode document: %w", err)
	}

	keys := make(map[string]*rsa.PublicKey, len(doc.Keys))
	for _, k := range doc.Keys {
		// RS256-only policy: skip non-RSA entries and entries explicitly
		// labelled with a different algorithm. An absent alg is tolerated —
		// the verifier enforces RS256 on the token side regardless.
		if k.Kty != "RSA" || k.Kid == "" || (k.Alg != "" && k.Alg != "RS256") {
			continue
		}
		pub, err := rsaPublicKey(k)
		if err != nil {
			return fmt.Errorf("jwks: parse key %q: %w", k.Kid, err)
		}
		keys[k.Kid] = pub
	}
	if len(keys) == 0 {
		return fmt.Errorf("jwks: document at %s contains no usable RS256 keys", c.url)
	}

	c.mu.Lock()
	c.keys = keys
	c.fetchedAt = time.Now()
	c.mu.Unlock()
	return nil
}

// rsaPublicKey converts the base64url (n, e) pair of a JWK into *rsa.PublicKey.
func rsaPublicKey(k jwk) (*rsa.PublicKey, error) {
	nBytes, err := base64.RawURLEncoding.DecodeString(k.N)
	if err != nil {
		return nil, fmt.Errorf("decode modulus: %w", err)
	}
	eBytes, err := base64.RawURLEncoding.DecodeString(k.E)
	if err != nil {
		return nil, fmt.Errorf("decode exponent: %w", err)
	}
	if len(nBytes) == 0 || len(eBytes) == 0 || len(eBytes) > 8 {
		return nil, fmt.Errorf("implausible modulus/exponent lengths (%d, %d)", len(nBytes), len(eBytes))
	}
	e := 0
	for _, b := range eBytes {
		e = e<<8 | int(b)
	}
	if e < 3 {
		return nil, fmt.Errorf("implausible public exponent %d", e)
	}
	return &rsa.PublicKey{N: new(big.Int).SetBytes(nBytes), E: e}, nil
}
