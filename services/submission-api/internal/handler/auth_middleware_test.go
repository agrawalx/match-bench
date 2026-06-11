// Package handler defines tests for auth middleware test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/iicpc/libs/authn"
)

const testGoogleClientID = "submission-api-test.apps.googleusercontent.com"

// tokenSigner groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type tokenSigner struct {
	key  *rsa.PrivateKey
	kid  string
	jwks *httptest.Server
}

// newTokenSigner performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func newTokenSigner(t *testing.T) *tokenSigner {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	s := &tokenSigner{key: key, kid: "itest-kid"}
	s.jwks = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		doc := map[string]any{"keys": []map[string]string{{
			"kty": "RSA",
			"kid": s.kid,
			"use": "sig",
			"alg": "RS256",
			"n":   base64.RawURLEncoding.EncodeToString(key.PublicKey.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.PublicKey.E)).Bytes()),
		}}}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(s.jwks.Close)
	return s
}

// verifier applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *tokenSigner) verifier(t *testing.T) *authn.Verifier {
	t.Helper()
	v, err := authn.NewVerifierWithJWKSURL(testGoogleClientID, s.jwks.URL)
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	return v
}

// token applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *tokenSigner) token(t *testing.T, sub string, mutate func(jwt.MapClaims)) string {
	t.Helper()
	now := time.Now()
	claims := jwt.MapClaims{
		"iss": "https://accounts.google.com",
		"aud": testGoogleClientID,
		"sub": sub,
		"iat": now.Add(-time.Minute).Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}
	if mutate != nil {
		mutate(claims)
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = s.kid
	signed, err := tok.SignedString(s.key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

// request applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (s *tokenSigner) request(t *testing.T, method, target, sub string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	req.Header.Set("Authorization", "Bearer "+s.token(t, sub, nil))
	return req
}

// forgedRequest performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func forgedRequest(method, target, sub string) *http.Request {
	req := httptest.NewRequest(method, target, nil)
	header := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none"}`))
	payload, _ := json.Marshal(map[string]string{"sub": sub})
	req.Header.Set("Authorization", "Bearer "+header+"."+base64.RawURLEncoding.EncodeToString(payload)+".")
	return req
}

// TestRequireContestant performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestRequireContestant(t *testing.T) {
	signer := newTokenSigner(t)
	mw := RequireContestant(signer.verifier(t), slog.Default())
	protected := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(contestantIDFromContext(r.Context())))
	}))

	tests := []struct {
		name     string
		request  func(t *testing.T) *http.Request
		wantCode int
		wantBody string
	}{
		{
			name: "valid signed token",
			request: func(t *testing.T) *http.Request {
				return signer.request(t, http.MethodGet, "/protected", "contestant-1")
			},
			wantCode: http.StatusOK,
			wantBody: "contestant-1",
		},
		{
			name: "no authorization header",
			request: func(t *testing.T) *http.Request {
				return httptest.NewRequest(http.MethodGet, "/protected", nil)
			},
			wantCode: http.StatusUnauthorized,
		},
		{
			name: "non-bearer authorization header",
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/protected", nil)
				req.Header.Set("Authorization", "Basic dXNlcjpwYXNz")
				return req
			},
			wantCode: http.StatusUnauthorized,
		},
		{
			name: "forged alg:none token",
			request: func(t *testing.T) *http.Request {
				return forgedRequest(http.MethodGet, "/protected", "victim")
			},
			wantCode: http.StatusUnauthorized,
		},
		{
			name: "expired token",
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/protected", nil)
				tok := signer.token(t, "contestant-1", func(c jwt.MapClaims) {
					c["exp"] = time.Now().Add(-2 * time.Minute).Unix()
				})
				req.Header.Set("Authorization", "Bearer "+tok)
				return req
			},
			wantCode: http.StatusUnauthorized,
		},
		{
			name: "wrong audience",
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/protected", nil)
				tok := signer.token(t, "contestant-1", func(c jwt.MapClaims) {
					c["aud"] = "someone-else.apps.googleusercontent.com"
				})
				req.Header.Set("Authorization", "Bearer "+tok)
				return req
			},
			wantCode: http.StatusUnauthorized,
		},
		{
			name: "wrong issuer",
			request: func(t *testing.T) *http.Request {
				req := httptest.NewRequest(http.MethodGet, "/protected", nil)
				tok := signer.token(t, "contestant-1", func(c jwt.MapClaims) {
					c["iss"] = "https://evil.example.com"
				})
				req.Header.Set("Authorization", "Bearer "+tok)
				return req
			},
			wantCode: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			protected.ServeHTTP(rec, tt.request(t))
			if rec.Code != tt.wantCode {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, tt.wantCode, rec.Body.String())
			}
			if tt.wantCode == http.StatusOK && rec.Body.String() != tt.wantBody {
				t.Fatalf("body = %q, want %q (context must carry the verified sub)", rec.Body.String(), tt.wantBody)
			}
		})
	}
}
