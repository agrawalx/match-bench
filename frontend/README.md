# IICPC Frontend

Real-time leaderboard + per-run analytics dashboard for the IICPC HFT benchmark.
Vite + React + TypeScript, TanStack Table (sortable leaderboard), uPlot (canvas
latency/throughput charts), and the browser `EventSource` for live SSE updates.

It is a pure projection of `leaderboard-api` — no business logic; it renders what
the scorer decided.

## Views

- **Leaderboard** — sortable table (rank, team, peak sustained TPS, p99 @ peak,
  correctness, rank delta, DQ), live-updating via SSE with rank-delta arrows;
  click a row to open the run detail.
- **Run detail** — the three sessions (constant / spike / ramp), each with
  latency-percentile and throughput/error uPlot timelines, KPI cards, and the
  violation log. Only `p99 service_time` is labelled *scored*; everything else is
  *diagnostic*.
- **Platform health** — the Prometheus-backed self-observation panels.

## Data source

- **Offline (default):** with no `VITE_API_BASE` set, the app serves built-in
  fixtures and simulates live movement. It runs and is fully clickable with no
  backend — used for local development and verification.
- **Live:** set `VITE_API_BASE` to a reachable `leaderboard-api`, e.g.

  ```sh
  kubectl -n platform port-forward svc/leaderboard-api 8080:8080
  VITE_API_BASE=http://localhost:8080 npm run dev
  ```

  In production the app is built with same-origin `/api` and nginx
  (`nginx.conf`) proxies `/api/*` (including the `/api/events` SSE stream) to
  `leaderboard-api`.

## Develop

```sh
npm install
npm run dev          # http://localhost:5173 (fixtures)
npm run build        # typecheck + static build to dist/
```

## Verify (headless)

`node verify.mjs` drives the built app with headless Chromium (Playwright):
navigates every view, sorts, opens a clean run and a DQ run, switches tabs,
captures live SSE movement, and asserts there are no console errors. Screenshots
land in `/tmp/fe-shots`.

## Deploy

```sh
docker build -t frontend:local .          # static SPA behind unprivileged nginx
kubectl apply -f ../k8s/platform/frontend/
```

Deploys to the `platform` namespace (Deployment + Service + NetworkPolicy). The
netpol allows ingress from the ingress controller and egress to `leaderboard-api`
+ DNS only.
