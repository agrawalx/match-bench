// Package handler implements auth behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package handler

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
)

// TokenVerifier defines the behavior expected by this package boundary.
// Implementations should preserve the caller-visible contract.
type TokenVerifier interface {
	Verify(ctx context.Context, rawToken string) (string, error)
}

// contestantIDKeyType groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type contestantIDKeyType struct{}

var contestantIDKey contestantIDKeyType

// withContestantID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func withContestantID(ctx context.Context, contestantID string) context.Context {
	return context.WithValue(ctx, contestantIDKey, contestantID)
}

// contestantIDFromContext performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func contestantIDFromContext(ctx context.Context) string {
	contestantID, _ := ctx.Value(contestantIDKey).(string)
	return contestantID
}

// RequireContestant performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
				log.WarnContext(r.Context(), "rejected bearer token", "error", err)
				writeError(w, http.StatusUnauthorized, "invalid or expired token")
				return
			}
			next.ServeHTTP(w, r.WithContext(withContestantID(r.Context(), sub)))
		})
	}
}

// OptionalContestant disables authentication: it never rejects a request. The
// contestant identity is taken from an unverified bearer-token `sub` claim when one
// is present, otherwise it falls back to defaultID. Use only when AUTH_REQUIRED=false.
func OptionalContestant(defaultID string, log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			sub := unverifiedSubClaim(bearerToken(r))
			if sub == "" {
				sub = defaultID
			}
			next.ServeHTTP(w, r.WithContext(withContestantID(r.Context(), sub)))
		})
	}
}

// InsecureTrustSubClaim performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// bearerToken performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func bearerToken(r *http.Request) string {
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	token, ok := strings.CutPrefix(auth, "Bearer ")
	if !ok {
		return ""
	}
	return strings.TrimSpace(token)
}

// unverifiedSubClaim performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
