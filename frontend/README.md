# IICPC Frontend

Next.js 14 App Router frontend for the IICPC algorithm benchmarking platform.

## Development

For benchmark testing, prefer the production-shaped Kind plane. From the
repository root, stop host services that can accidentally create runs in the
local Compose plane:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  stop leaderboard-api submission-api

pkill -f 'go run ./services/build-worker/cmd/worker' || true
```

Then expose the in-cluster submission API:

```bash
kubectl port-forward svc/submission-api 8082:80 -n platform --context kind-iicpc-k8s-test
kubectl port-forward svc/leaderboard-api 8081:8080 -n platform --context kind-iicpc-k8s-test
```

Start the frontend against those same Kind APIs:

```bash
npm install
LEADERBOARD_API_URL=http://localhost:8081 \
SUBMISSION_API_URL=http://localhost:8082 \
npm run dev
```

After switching to Kind, submit a fresh zip. Run IDs created against host-local
Compose Postgres are not visible to the Kind API or Kind runners.

For a local Compose-only smoke test, start the local platform APIs before the
frontend:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  up -d --build leaderboard-api submission-api
```

```bash
npm install
LEADERBOARD_API_URL=http://localhost:8081 \
SUBMISSION_API_URL=http://localhost:8082 \
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
