# IICPC Frontend

Next.js 14 App Router frontend for the IICPC algorithm benchmarking platform.

## Development

```bash
npm install
npm run dev
```

The dev server proxies `/api/leaderboard/v1/*`, `/api/submission/v1/*`, and `/api/auth/*`
to the local backend services configured in `.env.local`.

## Authentication

Google OAuth uses Authorization Code + PKCE. The browser stores only temporary PKCE
material in `sessionStorage`; platform tokens remain in React memory, and refresh
tokens are handled by the auth API as `HttpOnly` cookies.

Frontend `.env.local` needs only public OAuth values:

```bash
NEXT_PUBLIC_GOOGLE_CLIENT_ID=<google-web-client-id>
NEXT_PUBLIC_GOOGLE_REDIRECT_URI=http://localhost:3000/auth/callback
AUTH_API_URL=http://localhost:8083
```

The Google client secret belongs only to `services/auth-api`. From the repo root:

```bash
PORT=8083 \
GOOGLE_CLIENT_ID=<google-web-client-id> \
GOOGLE_CLIENT_SECRET=<google-web-client-secret> \
GOOGLE_ALLOWED_REDIRECT_URIS=http://localhost:3000/auth/callback \
AUTH_COOKIE_SECURE=false \
go run ./services/auth-api
```
