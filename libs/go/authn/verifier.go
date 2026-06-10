// Package authn defines shared library behavior for verifier.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package authn

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	DefaultJWKSURL = "https://www.googleapis.com/oauth2/v3/certs"

	EnvJWKSURL = "GOOGLE_JWKS_URL"

	leeway = 60 * time.Second
)

var ErrInvalidToken = errors.New("invalid token")

var issuers = map[string]bool{
	"accounts.google.com":         true,
	"https://accounts.google.com": true,
}

// Verifier groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Verifier struct {
	audience string
	jwks     *jwksCache
	parser   *jwt.Parser
}

// NewVerifier performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewVerifier(clientID string) (*Verifier, error) {
	url := os.Getenv(EnvJWKSURL)
	if url == "" {
		url = DefaultJWKSURL
	}
	return NewVerifierWithJWKSURL(clientID, url)
}

// NewVerifierWithJWKSURL performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func NewVerifierWithJWKSURL(clientID, jwksURL string) (*Verifier, error) {
	if clientID == "" {
		return nil, errors.New("authn: client ID (audience) must not be empty")
	}
	if jwksURL == "" {
		return nil, errors.New("authn: JWKS URL must not be empty")
	}
	return &Verifier{
		audience: clientID,
		jwks:     newJWKSCache(jwksURL),
		parser: jwt.NewParser(
			jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
			jwt.WithAudience(clientID),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
			jwt.WithLeeway(leeway),
		),
	}, nil
}

// Verify applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (v *Verifier) Verify(ctx context.Context, rawToken string) (string, error) {
	claims := &jwt.RegisteredClaims{}
	_, err := v.parser.ParseWithClaims(rawToken, claims, func(t *jwt.Token) (any, error) {
		kid, _ := t.Header["kid"].(string)
		if kid == "" {
			return nil, errors.New("missing kid header")
		}
		return v.jwks.key(ctx, kid)
	})
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidToken, err)
	}
	if !issuers[claims.Issuer] {
		return "", fmt.Errorf("%w: unexpected issuer %q", ErrInvalidToken, claims.Issuer)
	}
	if claims.Subject == "" {
		return "", fmt.Errorf("%w: empty sub claim", ErrInvalidToken)
	}
	return claims.Subject, nil
}
