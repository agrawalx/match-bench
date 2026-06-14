// Package authn defines shared library behavior for jwks.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
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

const jwksTTL = 12 * time.Hour

const jwksMaxBody = 1 << 20

// jwksCache groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type jwksCache struct {
	url    string
	client *http.Client

	mu        sync.RWMutex
	keys      map[string]*rsa.PublicKey
	fetchedAt time.Time

	fetchMu sync.Mutex
}

// newJWKSCache performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newJWKSCache(url string) *jwksCache {
	return &jwksCache{
		url:    url,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// key applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *jwksCache) key(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	if pub, ok := c.lookup(kid); ok {
		return pub, nil
	}

	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

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

// lookup applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (c *jwksCache) lookup(kid string) (*rsa.PublicKey, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if time.Since(c.fetchedAt) > jwksTTL {
		return nil, false
	}
	pub, ok := c.keys[kid]
	return pub, ok
}

// jwk groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type jwk struct {
	Kty string `json:"kty"`
	Kid string `json:"kid"`
	Use string `json:"use"`
	Alg string `json:"alg"`
	N   string `json:"n"` // base64url modulus
	E   string `json:"e"` // base64url exponent
}

// fetch applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// rsaPublicKey performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
