package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type config struct {
	port             string
	clientID         string
	clientSecret     string
	tokenURL         string
	refreshCookie    string
	cookieSecure     bool
	cookieSameSite   http.SameSite
	requestTimeout   time.Duration
	allowedRedirects map[string]struct{}
}

type server struct {
	cfg    config
	client *http.Client
}

type tokenRequest struct {
	Code         string `json:"code"`
	CodeVerifier string `json:"code_verifier"`
	ClientID     string `json:"client_id"`
	RedirectURI  string `json:"redirect_uri"`
	Nonce        string `json:"nonce"`
}

type googleTokenResponse struct {
	AccessToken  string `json:"access_token,omitempty"`
	ExpiresIn    int64  `json:"expires_in,omitempty"`
	IDToken      string `json:"id_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
	TokenType    string `json:"token_type,omitempty"`
	Scope        string `json:"scope,omitempty"`
	Error        string `json:"error,omitempty"`
	ErrorMessage string `json:"error_description,omitempty"`
}

type tokenResponse struct {
	AccessToken   string       `json:"access_token,omitempty"`
	ExpiresIn     int64        `json:"expires_in"`
	IDToken       string       `json:"id_token,omitempty"`
	PlatformToken string       `json:"platform_token,omitempty"`
	User          *userProfile `json:"user,omitempty"`
}

type userProfile struct {
	Sub           string  `json:"sub"`
	Email         string  `json:"email"`
	EmailVerified bool    `json:"emailVerified"`
	DisplayName   string  `json:"displayName"`
	AvatarURL     *string `json:"avatarUrl"`
	ContestantID  string  `json:"contestantId"`
}

type idTokenClaims struct {
	Sub           string `json:"sub"`
	Email         string `json:"email"`
	EmailVerified bool   `json:"email_verified"`
	Name          string `json:"name"`
	Picture       string `json:"picture"`
	Nonce         string `json:"nonce"`
}

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	s := &server{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.requestTimeout,
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", ok)
	mux.HandleFunc("/ready", ok)
	mux.HandleFunc("/token", s.handleToken)
	mux.HandleFunc("/refresh", s.handleRefresh)
	mux.HandleFunc("/logout", s.handleLogout)

	httpServer := &http.Server{
		Addr:              ":" + cfg.port,
		Handler:           securityHeaders(mux),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	listener, err := net.Listen("tcp", httpServer.Addr)
	if err != nil {
		log.Fatal(err)
	}

	// Graceful shutdown: on SIGTERM (kubelet) or SIGINT, stop accepting new
	// connections and drain in-flight token exchanges before exiting, so a
	// rolling deploy never cuts off a login mid-exchange.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.Serve(listener)
	}()
	log.Printf("auth-api listening on %s", listener.Addr())

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatal(err)
		}
	case <-ctx.Done():
		stop()
		log.Printf("shutdown signal received; draining connections")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("graceful shutdown failed: %v", err)
		}
	}
	log.Printf("auth-api stopped")
}

func (s *server) handleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	var req tokenRequest
	if err := readJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request")
		return
	}
	clientID := firstNonEmpty(req.ClientID, s.cfg.clientID)
	if clientID != s.cfg.clientID {
		writeError(w, http.StatusBadRequest, "client_id_mismatch")
		return
	}
	if req.Code == "" || req.CodeVerifier == "" || req.RedirectURI == "" {
		writeError(w, http.StatusBadRequest, "missing_oauth_fields")
		return
	}
	if !s.redirectAllowed(req.RedirectURI) {
		writeError(w, http.StatusBadRequest, "redirect_uri_not_allowed")
		return
	}

	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {req.Code},
		"code_verifier": {req.CodeVerifier},
		"client_id":     {s.cfg.clientID},
		"client_secret": {s.cfg.clientSecret},
		"redirect_uri":  {req.RedirectURI},
	}
	token, err := s.exchange(form)
	if err != nil {
		log.Printf("token exchange failed: %v", err)
		writeError(w, http.StatusBadGateway, "exchange_failed")
		return
	}
	if token.IDToken == "" {
		writeError(w, http.StatusBadGateway, "missing_id_token")
		return
	}
	claims, err := parseClaims(token.IDToken)
	if err != nil {
		writeError(w, http.StatusBadGateway, "invalid_id_token")
		return
	}
	if req.Nonce != "" && claims.Nonce != "" && req.Nonce != claims.Nonce {
		writeError(w, http.StatusBadGateway, "nonce_mismatch")
		return
	}
	if token.RefreshToken != "" {
		s.setRefreshCookie(w, token.RefreshToken, false)
	}
	writeJSON(w, http.StatusOK, responseFromToken(token, claims))
}

func (s *server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	cookie, err := r.Cookie(s.cfg.refreshCookie)
	if err != nil || cookie.Value == "" {
		writeError(w, http.StatusUnauthorized, "missing_refresh_token")
		return
	}
	form := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {cookie.Value},
		"client_id":     {s.cfg.clientID},
		"client_secret": {s.cfg.clientSecret},
	}
	token, err := s.exchange(form)
	if err != nil {
		log.Printf("refresh exchange failed: %v", err)
		writeError(w, http.StatusUnauthorized, "refresh_failed")
		return
	}
	if token.IDToken == "" {
		writeError(w, http.StatusBadGateway, "missing_id_token")
		return
	}
	claims, err := parseClaims(token.IDToken)
	if err != nil {
		writeError(w, http.StatusBadGateway, "invalid_id_token")
		return
	}
	writeJSON(w, http.StatusOK, responseFromToken(token, claims))
}

func (s *server) handleLogout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed")
		return
	}
	s.setRefreshCookie(w, "", true)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) exchange(form url.Values) (googleTokenResponse, error) {
	req, err := http.NewRequest(http.MethodPost, s.cfg.tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return googleTokenResponse{}, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := s.client.Do(req)
	if err != nil {
		return googleTokenResponse{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return googleTokenResponse{}, err
	}
	var token googleTokenResponse
	if err := json.Unmarshal(body, &token); err != nil {
		return googleTokenResponse{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 || token.Error != "" {
		return googleTokenResponse{}, fmt.Errorf("%s: %s", firstNonEmpty(token.Error, resp.Status), token.ErrorMessage)
	}
	return token, nil
}

func responseFromToken(token googleTokenResponse, claims idTokenClaims) tokenResponse {
	user := userProfile{
		Sub:           claims.Sub,
		Email:         claims.Email,
		EmailVerified: claims.EmailVerified,
		DisplayName:   firstNonEmpty(claims.Name, claims.Email, "Contestant"),
		ContestantID:  claims.Sub,
	}
	if claims.Picture != "" {
		user.AvatarURL = &claims.Picture
	}
	return tokenResponse{
		AccessToken:   token.AccessToken,
		ExpiresIn:     token.ExpiresIn,
		IDToken:       token.IDToken,
		PlatformToken: token.IDToken,
		User:          &user,
	}
}

func parseClaims(idToken string) (idTokenClaims, error) {
	parts := strings.Split(idToken, ".")
	if len(parts) != 3 {
		return idTokenClaims{}, errors.New("bad jwt segment count")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return idTokenClaims{}, err
	}
	var claims idTokenClaims
	if err := json.Unmarshal(payload, &claims); err != nil {
		return idTokenClaims{}, err
	}
	if claims.Sub == "" {
		return idTokenClaims{}, errors.New("missing sub")
	}
	return claims, nil
}

// redirectAllowed is fail-closed: loadConfig guarantees a non-empty allowlist,
// and an empty set here (a zero-value config that bypassed loadConfig) must
// reject every redirect_uri rather than forward arbitrary attacker-chosen
// values into the token exchange.
func (s *server) redirectAllowed(redirectURI string) bool {
	_, ok := s.cfg.allowedRedirects[redirectURI]
	return ok
}

func (s *server) setRefreshCookie(w http.ResponseWriter, value string, clear bool) {
	maxAge := 30 * 24 * 60 * 60
	expires := time.Now().Add(time.Duration(maxAge) * time.Second)
	if clear {
		maxAge = -1
		expires = time.Unix(0, 0)
	}
	http.SetCookie(w, &http.Cookie{
		Name:     s.cfg.refreshCookie,
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		Secure:   s.cfg.cookieSecure,
		SameSite: s.cfg.cookieSameSite,
		MaxAge:   maxAge,
		Expires:  expires,
	})
}

func loadConfig() (config, error) {
	secure := envBool("AUTH_COOKIE_SECURE", false)
	cookieName := os.Getenv("AUTH_REFRESH_COOKIE_NAME")
	if cookieName == "" {
		if secure {
			cookieName = "__Host-iicpc_google_refresh"
		} else {
			cookieName = "iicpc_google_refresh"
		}
	}
	cfg := config{
		port:           envOr("PORT", "8080"),
		clientID:       firstNonEmpty(os.Getenv("GOOGLE_CLIENT_ID"), os.Getenv("OAUTH_CLIENT_ID")),
		clientSecret:   firstNonEmpty(os.Getenv("GOOGLE_CLIENT_SECRET"), os.Getenv("OAUTH_CLIENT_SECRET")),
		tokenURL:       envOr("GOOGLE_TOKEN_URL", "https://oauth2.googleapis.com/token"),
		refreshCookie:  cookieName,
		cookieSecure:   secure,
		cookieSameSite: sameSiteFromEnv(envOr("AUTH_COOKIE_SAMESITE", "lax")),
		requestTimeout: durationFromEnv("AUTH_GOOGLE_TIMEOUT", 10*time.Second),
		allowedRedirects: redirectSet(firstNonEmpty(
			os.Getenv("GOOGLE_ALLOWED_REDIRECT_URIS"),
			os.Getenv("OAUTH_ALLOWED_REDIRECT_URIS"),
		)),
	}
	if cfg.clientID == "" {
		return cfg, errors.New("GOOGLE_CLIENT_ID or OAUTH_CLIENT_ID is required")
	}
	if cfg.clientSecret == "" {
		return cfg, errors.New("GOOGLE_CLIENT_SECRET or OAUTH_CLIENT_SECRET is required")
	}
	if len(cfg.allowedRedirects) == 0 {
		return cfg, errors.New("GOOGLE_ALLOWED_REDIRECT_URIS or OAUTH_ALLOWED_REDIRECT_URIS is required")
	}
	return cfg, nil
}

func redirectSet(raw string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, item := range strings.Split(raw, ",") {
		value := strings.TrimSpace(item)
		if value != "" {
			out[value] = struct{}{}
		}
	}
	return out
}

func sameSiteFromEnv(value string) http.SameSite {
	switch strings.ToLower(value) {
	case "strict":
		return http.SameSiteStrictMode
	case "none":
		return http.SameSiteNoneMode
	default:
		return http.SameSiteLaxMode
	}
}

func durationFromEnv(key string, def time.Duration) time.Duration {
	value := os.Getenv(key)
	if value == "" {
		return def
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return def
	}
	return parsed
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func ok(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		next.ServeHTTP(w, r)
	})
}

func envOr(key, def string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return def
}

func envBool(key string, def bool) bool {
	switch strings.ToLower(os.Getenv(key)) {
	case "true", "1", "yes":
		return true
	case "false", "0", "no":
		return false
	default:
		return def
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
