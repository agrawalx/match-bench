package main

import (
	"testing"
)

// loadConfig must fail when no redirect allowlist is configured — the same
// treatment as a missing client ID. An auth-api booted without an allowlist
// would otherwise forward arbitrary attacker-chosen redirect_uri values into
// the Google token exchange.
func TestLoadConfigRequiresAllowedRedirects(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr bool
	}{
		{
			name: "missing allowlist fails",
			env: map[string]string{
				"GOOGLE_CLIENT_ID":     "id",
				"GOOGLE_CLIENT_SECRET": "secret",
			},
			wantErr: true,
		},
		{
			name: "whitespace-only allowlist fails",
			env: map[string]string{
				"GOOGLE_CLIENT_ID":             "id",
				"GOOGLE_CLIENT_SECRET":         "secret",
				"GOOGLE_ALLOWED_REDIRECT_URIS": " , ,",
			},
			wantErr: true,
		},
		{
			name: "google allowlist ok",
			env: map[string]string{
				"GOOGLE_CLIENT_ID":             "id",
				"GOOGLE_CLIENT_SECRET":         "secret",
				"GOOGLE_ALLOWED_REDIRECT_URIS": "https://contest.example.com/auth/callback",
			},
			wantErr: false,
		},
		{
			name: "oauth fallback allowlist ok",
			env: map[string]string{
				"GOOGLE_CLIENT_ID":            "id",
				"GOOGLE_CLIENT_SECRET":        "secret",
				"OAUTH_ALLOWED_REDIRECT_URIS": "https://contest.example.com/auth/callback",
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear every config-relevant var, then apply the case's env.
			for _, key := range []string{
				"GOOGLE_CLIENT_ID", "OAUTH_CLIENT_ID",
				"GOOGLE_CLIENT_SECRET", "OAUTH_CLIENT_SECRET",
				"GOOGLE_ALLOWED_REDIRECT_URIS", "OAUTH_ALLOWED_REDIRECT_URIS",
			} {
				t.Setenv(key, "")
			}
			for key, value := range tt.env {
				t.Setenv(key, value)
			}
			_, err := loadConfig()
			if (err != nil) != tt.wantErr {
				t.Fatalf("loadConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// redirectAllowed must fail closed: an empty allowlist (zero-value config that
// bypassed loadConfig) rejects everything instead of allowing everything.
func TestRedirectAllowedFailClosed(t *testing.T) {
	tests := []struct {
		name        string
		allowed     map[string]struct{}
		redirectURI string
		want        bool
	}{
		{
			name:        "empty set rejects",
			allowed:     map[string]struct{}{},
			redirectURI: "https://contest.example.com/auth/callback",
			want:        false,
		},
		{
			name:        "nil set rejects",
			allowed:     nil,
			redirectURI: "https://contest.example.com/auth/callback",
			want:        false,
		},
		{
			name:        "listed uri allowed",
			allowed:     map[string]struct{}{"https://contest.example.com/auth/callback": {}},
			redirectURI: "https://contest.example.com/auth/callback",
			want:        true,
		},
		{
			name:        "unlisted uri rejected",
			allowed:     map[string]struct{}{"https://contest.example.com/auth/callback": {}},
			redirectURI: "https://evil.example.com/auth/callback",
			want:        false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &server{cfg: config{allowedRedirects: tt.allowed}}
			if got := s.redirectAllowed(tt.redirectURI); got != tt.want {
				t.Fatalf("redirectAllowed(%q) = %v, want %v", tt.redirectURI, got, tt.want)
			}
		})
	}
}
