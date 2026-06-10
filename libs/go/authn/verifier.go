// Package authn verifies Google ID tokens for the platform's HTTP services.
//
// The platform's PlatformToken IS the raw Google ID token (auth-api mints no
// token of its own), so the enforcement point — submission-api — must verify
// against Google's published JWKS. Acceptance policy:
//
//   - signature: RS256 only, key selected by the token's kid from the JWKS at
//     GOOGLE_JWKS_URL (default: Google's oauth2/v3/certs). alg:none and every
//     non-RS256 algorithm are rejected outright.
//   - iss ∈ {accounts.google.com, https://accounts.google.com} — Google has
//     historically emitted both forms.
//   - aud == the configured OAuth client ID.
//   - exp required and iat honored, both with 60s leeway for clock skew.
//
// The JWKS document is cached kid-indexed for 12h and refreshed single-flight
// on an unknown kid, so Google's key rotations are picked up without a
// restart and a burst of cold-cache requests costs one HTTP fetch.
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
	// DefaultJWKSURL is Google's published JWKS endpoint for ID-token
	// signing keys.
	DefaultJWKSURL = "https://www.googleapis.com/oauth2/v3/certs"

	// EnvJWKSURL overrides the JWKS endpoint — used by tests (httptest
	// fake) and air-gapped dev environments.
	EnvJWKSURL = "GOOGLE_JWKS_URL"

	// leeway absorbs clock skew between Google and our pods on exp/iat/nbf
	// checks.
	leeway = 60 * time.Second
)

// ErrInvalidToken is the sentinel wrapped around every verification failure.
// Callers should map errors.Is(err, ErrInvalidToken) to HTTP 401.
var ErrInvalidToken = errors.New("invalid token")

// issuers is the closed set of acceptable iss values.
var issuers = map[string]bool{
	"accounts.google.com":         true,
	"https://accounts.google.com": true,
}

// Verifier validates Google ID tokens against a JWKS endpoint and a fixed
// audience. Safe for concurrent use.
type Verifier struct {
	audience string
	jwks     *jwksCache
	parser   *jwt.Parser
}

// NewVerifier builds a Verifier for the given OAuth client ID, reading the
// JWKS endpoint from GOOGLE_JWKS_URL (default: Google's published endpoint).
func NewVerifier(clientID string) (*Verifier, error) {
	url := os.Getenv(EnvJWKSURL)
	if url == "" {
		url = DefaultJWKSURL
	}
	return NewVerifierWithJWKSURL(clientID, url)
}

// NewVerifierWithJWKSURL builds a Verifier with an explicit JWKS endpoint.
// An empty clientID is refused — verification must fail closed, never
// degrade into accepting any audience.
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
			// The whole acceptance policy lives here, not in handlers:
			// RS256-only kills alg:none and algorithm-confusion attacks;
			// the rest is standard Google ID-token validation.
			jwt.WithValidMethods([]string{jwt.SigningMethodRS256.Alg()}),
			jwt.WithAudience(clientID),
			jwt.WithExpirationRequired(),
			jwt.WithIssuedAt(),
			jwt.WithLeeway(leeway),
		),
	}, nil
}

// Verify checks rawToken against the policy in the package doc and returns
// the token's sub claim — the stable Google account ID the platform uses as
// contestant_id. Every failure wraps ErrInvalidToken.
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
	// jwt/v5 only validates a single expected issuer, and Google emits two
	// forms — enforce the closed set here.
	if !issuers[claims.Issuer] {
		return "", fmt.Errorf("%w: unexpected issuer %q", ErrInvalidToken, claims.Issuer)
	}
	if claims.Subject == "" {
		return "", fmt.Errorf("%w: empty sub claim", ErrInvalidToken)
	}
	return claims.Subject, nil
}
