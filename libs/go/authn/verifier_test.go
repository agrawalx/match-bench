package authn

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const testClientID = "test-client-id.apps.googleusercontent.com"

// testJWKS serves a JWKS document over httptest and counts fetches so cache
// behaviour (TTL reuse, refresh-on-unknown-kid, no refetch on a kid hit) is
// assertable from tests.
type testJWKS struct {
	srv     *httptest.Server
	fetches atomic.Int64

	mu   sync.Mutex
	keys map[string]*rsa.PublicKey
}

func newTestJWKS(t *testing.T) *testJWKS {
	t.Helper()
	j := &testJWKS{keys: map[string]*rsa.PublicKey{}}
	j.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		j.fetches.Add(1)
		j.mu.Lock()
		defer j.mu.Unlock()
		type jwk struct {
			Kty string `json:"kty"`
			Kid string `json:"kid"`
			Use string `json:"use"`
			Alg string `json:"alg"`
			N   string `json:"n"`
			E   string `json:"e"`
		}
		var doc struct {
			Keys []jwk `json:"keys"`
		}
		for kid, pub := range j.keys {
			doc.Keys = append(doc.Keys, jwk{
				Kty: "RSA",
				Kid: kid,
				Use: "sig",
				Alg: "RS256",
				N:   base64.RawURLEncoding.EncodeToString(pub.N.Bytes()),
				E:   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(pub.E)).Bytes()),
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(doc)
	}))
	t.Cleanup(j.srv.Close)
	return j
}

func (j *testJWKS) setKey(kid string, pub *rsa.PublicKey) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.keys[kid] = pub
}

func newRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	return key
}

func signRS256(t *testing.T, key *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	tok.Header["kid"] = kid
	signed, err := tok.SignedString(key)
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}
	return signed
}

// googleClaims is a valid Google ID-token claim set; tests mutate single
// fields to produce each rejection case.
func googleClaims(sub string) jwt.MapClaims {
	now := time.Now()
	return jwt.MapClaims{
		"iss": "https://accounts.google.com",
		"aud": testClientID,
		"sub": sub,
		"iat": now.Add(-time.Minute).Unix(),
		"exp": now.Add(time.Hour).Unix(),
	}
}

// TestVerify covers the full acceptance matrix for Google ID tokens: RS256
// only (alg:none and HS256 rejected), iss restricted to the two Google forms,
// aud == configured client ID, exp/iat with 60s leeway, and signatures checked
// against the JWKS key for the token's kid.
func TestVerify(t *testing.T) {
	key := newRSAKey(t)
	jwks := newTestJWKS(t)
	jwks.setKey("kid-1", &key.PublicKey)

	v, err := NewVerifierWithJWKSURL(testClientID, jwks.srv.URL)
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	tests := []struct {
		name    string
		token   func(t *testing.T) string
		wantSub string
		wantErr bool
	}{
		{
			name:    "valid token https issuer",
			token:   func(t *testing.T) string { return signRS256(t, key, "kid-1", googleClaims("sub-https")) },
			wantSub: "sub-https",
		},
		{
			name: "valid token bare issuer",
			token: func(t *testing.T) string {
				c := googleClaims("sub-bare")
				c["iss"] = "accounts.google.com"
				return signRS256(t, key, "kid-1", c)
			},
			wantSub: "sub-bare",
		},
		{
			name: "expired within 60s leeway",
			token: func(t *testing.T) string {
				c := googleClaims("sub-leeway")
				c["exp"] = time.Now().Add(-30 * time.Second).Unix()
				return signRS256(t, key, "kid-1", c)
			},
			wantSub: "sub-leeway",
		},
		{
			name: "expired beyond leeway",
			token: func(t *testing.T) string {
				c := googleClaims("sub-expired")
				c["exp"] = time.Now().Add(-2 * time.Minute).Unix()
				return signRS256(t, key, "kid-1", c)
			},
			wantErr: true,
		},
		{
			name: "missing exp",
			token: func(t *testing.T) string {
				c := googleClaims("sub-noexp")
				delete(c, "exp")
				return signRS256(t, key, "kid-1", c)
			},
			wantErr: true,
		},
		{
			name: "iat in the future beyond leeway",
			token: func(t *testing.T) string {
				c := googleClaims("sub-iat")
				c["iat"] = time.Now().Add(5 * time.Minute).Unix()
				return signRS256(t, key, "kid-1", c)
			},
			wantErr: true,
		},
		{
			name: "wrong audience",
			token: func(t *testing.T) string {
				c := googleClaims("sub-aud")
				c["aud"] = "some-other-client.apps.googleusercontent.com"
				return signRS256(t, key, "kid-1", c)
			},
			wantErr: true,
		},
		{
			name: "foreign issuer",
			token: func(t *testing.T) string {
				c := googleClaims("sub-iss")
				c["iss"] = "https://evil.example.com"
				return signRS256(t, key, "kid-1", c)
			},
			wantErr: true,
		},
		{
			name: "empty sub",
			token: func(t *testing.T) string {
				return signRS256(t, key, "kid-1", googleClaims(""))
			},
			wantErr: true,
		},
		{
			name: "alg none forgery",
			token: func(t *testing.T) string {
				tok := jwt.NewWithClaims(jwt.SigningMethodNone, googleClaims("sub-none"))
				signed, err := tok.SignedString(jwt.UnsafeAllowNoneSignatureType)
				if err != nil {
					t.Fatalf("sign none token: %v", err)
				}
				return signed
			},
			wantErr: true,
		},
		{
			name: "HS256 forgery",
			token: func(t *testing.T) string {
				tok := jwt.NewWithClaims(jwt.SigningMethodHS256, googleClaims("sub-hs"))
				tok.Header["kid"] = "kid-1"
				signed, err := tok.SignedString([]byte("guessable-secret"))
				if err != nil {
					t.Fatalf("sign hs256 token: %v", err)
				}
				return signed
			},
			wantErr: true,
		},
		{
			name: "signature from a different key under a known kid",
			token: func(t *testing.T) string {
				return signRS256(t, newRSAKey(t), "kid-1", googleClaims("sub-forged-sig"))
			},
			wantErr: true,
		},
		{
			name: "unknown kid even after refresh",
			token: func(t *testing.T) string {
				return signRS256(t, key, "kid-unknown", googleClaims("sub-unknown-kid"))
			},
			wantErr: true,
		},
		{
			name:    "garbage token",
			token:   func(t *testing.T) string { return "not.a.jwt" },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub, err := v.Verify(context.Background(), tt.token(t))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("Verify() = %q, want error", sub)
				}
				return
			}
			if err != nil {
				t.Fatalf("Verify() error = %v, want nil", err)
			}
			if sub != tt.wantSub {
				t.Fatalf("Verify() sub = %q, want %q", sub, tt.wantSub)
			}
		})
	}
}

// TestVerifyCachesJWKS asserts that a kid hit inside the TTL does NOT refetch
// the JWKS document — one fetch serves many verifications.
func TestVerifyCachesJWKS(t *testing.T) {
	key := newRSAKey(t)
	jwks := newTestJWKS(t)
	jwks.setKey("kid-1", &key.PublicKey)

	v, err := NewVerifierWithJWKSURL(testClientID, jwks.srv.URL)
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	for i := 0; i < 5; i++ {
		if _, err := v.Verify(context.Background(), signRS256(t, key, "kid-1", googleClaims("sub"))); err != nil {
			t.Fatalf("Verify() #%d error = %v", i, err)
		}
	}
	if got := jwks.fetches.Load(); got != 1 {
		t.Fatalf("JWKS fetches = %d, want 1 (kid hits must be served from cache)", got)
	}
}

// TestVerifyRefreshesOnUnknownKid simulates Google's key rotation: a token
// signed by a kid that is absent from the cached document must trigger one
// refetch, after which the rotated key verifies.
func TestVerifyRefreshesOnUnknownKid(t *testing.T) {
	key1 := newRSAKey(t)
	jwks := newTestJWKS(t)
	jwks.setKey("kid-1", &key1.PublicKey)

	v, err := NewVerifierWithJWKSURL(testClientID, jwks.srv.URL)
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}

	if _, err := v.Verify(context.Background(), signRS256(t, key1, "kid-1", googleClaims("sub-1"))); err != nil {
		t.Fatalf("Verify() with kid-1: %v", err)
	}

	// Rotate: the JWKS endpoint now also serves kid-2.
	key2 := newRSAKey(t)
	jwks.setKey("kid-2", &key2.PublicKey)

	sub, err := v.Verify(context.Background(), signRS256(t, key2, "kid-2", googleClaims("sub-2")))
	if err != nil {
		t.Fatalf("Verify() with rotated kid-2: %v", err)
	}
	if sub != "sub-2" {
		t.Fatalf("Verify() sub = %q, want sub-2", sub)
	}
	if got := jwks.fetches.Load(); got != 2 {
		t.Fatalf("JWKS fetches = %d, want 2 (initial + one refresh on unknown kid)", got)
	}
}

// TestNewVerifierRequiresClientID rejects construction with an empty audience —
// an unset GOOGLE_CLIENT_ID must fail closed, never verify-against-nothing.
func TestNewVerifierRequiresClientID(t *testing.T) {
	if _, err := NewVerifierWithJWKSURL("", "http://127.0.0.1:1/jwks"); err == nil {
		t.Fatal("NewVerifierWithJWKSURL(\"\", ...) = nil error, want error")
	}
}

// TestNewVerifierJWKSURLFromEnv: GOOGLE_JWKS_URL overrides the default Google
// endpoint so tests and air-gapped dev environments can point at a fake.
func TestNewVerifierJWKSURLFromEnv(t *testing.T) {
	key := newRSAKey(t)
	jwks := newTestJWKS(t)
	jwks.setKey("kid-env", &key.PublicKey)
	t.Setenv(EnvJWKSURL, jwks.srv.URL)

	v, err := NewVerifier(testClientID)
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	sub, err := v.Verify(context.Background(), signRS256(t, key, "kid-env", googleClaims("sub-env")))
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
	if sub != "sub-env" {
		t.Fatalf("Verify() sub = %q, want sub-env", sub)
	}
}
