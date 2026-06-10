package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
)

// TokenVerifier is the slice of libs/go/authn.Verifier the middleware needs:
// verify a raw bearer token, return the stable subject (Google `sub`) it
// authenticates. Kept as a local interface so handler tests can fake it and
// the handler package does not couple to the authn implementation.
type TokenVerifier interface {
	Verify(ctx context.Context, rawToken string) (string, error)
}

// contestantIDKeyType keys the verified contestant ID in the request context.
// Unexported struct type — collision-proof against other packages' context keys.
type contestantIDKeyType struct{}

var contestantIDKey contestantIDKeyType

func withContestantID(ctx context.Context, contestantID string) context.Context {
	return context.WithValue(ctx, contestantIDKey, contestantID)
}

// contestantIDFromContext returns the contestant ID the auth middleware
// stored in the request context, or "" when no middleware ran. Handlers keep
// their own ""-means-401 guards so a route accidentally mounted without the
// middleware fails closed instead of serving unauthenticated traffic.
func contestantIDFromContext(ctx context.Context) string {
	contestantID, _ := ctx.Value(contestantIDKey).(string)
	return contestantID
}

// RequireContestant is the chi middleware in front of every contestant-facing
// route. It extracts the Bearer token, verifies it as a Google ID token —
// RS256 signature against Google's JWKS, iss/aud/exp/iat; see libs/go/authn —
// and stores the verified sub in the request context for handlers to read.
//
// Missing token and failed verification are both 401. This replaces the
// pre-Critical-1 contestantIDFromRequest, which base64-decoded the payload
// with NO verification and accepted alg:none forgeries.
func RequireContestant(v TokenVerifier, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := bearerToken(r)
			if token == "" {
				writeError(w, http.StatusUnauthorized, "authentication required")
				return
			}
			sub, err := v.Verify(r.Context(), token)
			if err != nil {
				// 401 with a generic message; the specific failure
				// (signature, exp, aud, ...) goes to logs only.
				log.WarnContext(r.Context(), "rejected bearer token", "error", err)
				writeError(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}
			next.ServeHTTP(w, r.WithContext(withContestantID(r.Context(), sub)))
		})
	}
}

// InsecureTrustSubClaim is the AUTH_REQUIRED=false twin of RequireContestant:
// it trusts the token's unverified sub claim, exactly the pre-fix behaviour.
// Dev and test environments only — main.go refuses to select it unless
// AUTH_REQUIRED is explicitly set to false, and logs loudly when it is.
func InsecureTrustSubClaim(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sub := unverifiedSubClaim(bearerToken(r))
			if sub == "" {
				writeError(w, http.StatusUnauthorized, "authentication required")
				return
			}
			next.ServeHTTP(w, r.WithContext(withContestantID(r.Context(), sub)))
		})
	}
}

// bearerToken extracts the RFC 6750 Bearer token, or "" when absent/malformed.
func bearerToken(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	token, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(token)
}

// unverifiedSubClaim base64-decodes the JWT payload and returns its sub claim
// WITHOUT any signature/iss/aud/exp validation. This is the Critical-1 bug
// preserved deliberately — and exclusively — for InsecureTrustSubClaim's
// dev-mode. Never call it from a production path.
func unverifiedSubClaim(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Sub string `json:"sub"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return strings.TrimSpace(claims.Sub)
}
