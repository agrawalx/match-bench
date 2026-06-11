// Package main defines tests for main test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"testing"
)

// TestLoadConfigRequiresAllowedRedirects performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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

// TestRedirectAllowedFailClosed performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
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
