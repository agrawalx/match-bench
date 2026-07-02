#!/usr/bin/env bash
# Port-forward the user-facing endpoints. Leave this running in its own terminal.
set -euo pipefail
pkill -f "kubectl port-forward" 2>/dev/null || true
sleep 1
kubectl -n platform      port-forward svc/frontend         8080:8080  >/tmp/pf-frontend.log 2>&1 &
kubectl -n observability port-forward svc/grafana          3000:3000  >/tmp/pf-grafana.log  2>&1 &
kubectl -n observability port-forward svc/prometheus       9090:9090  >/tmp/pf-prom.log     2>&1 &
kubectl -n platform      port-forward svc/submission-api   8088:80    >/tmp/pf-sapi.log     2>&1 &
kubectl -n platform      port-forward svc/leaderboard-api  8090:8080  >/tmp/pf-lb.log       2>&1 &
sleep 2
cat <<EOF

  Frontend (leaderboard/runs)  http://localhost:8080
  Grafana  (dashboards)        http://localhost:3000   (admin / admin)
  Prometheus                   http://localhost:9090
  submission-api (for submit)  http://localhost:8088
  leaderboard-api (raw API)    http://localhost:8090

  Grafana datasources: Prometheus (throughput/health/kafka lag),
  TimescaleDB (latency: SELECT time, p99_ns, tps_1s FROM metrics ORDER BY time).
EOF
