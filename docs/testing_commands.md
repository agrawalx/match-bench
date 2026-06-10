# Testing Commands

This file is only for test and validation commands. For local startup commands, use `docs/local-run.md`. For AWS deployment commands, use `docs/aws-deployment.md`.

Run commands from the repository root unless a command changes directories.

## Shared Integration Environment

Start dependencies first:

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d
```

Then export:

```bash
export DATABASE_URL='postgres://iicpc:iicpc@localhost:5433/iicpc?sslmode=disable'
export TIMESCALE_URL='postgres://iicpc:iicpc@localhost:5434/metrics'
export REDIS_ADDR='localhost:6379'
export REDIS_URL='redis://localhost:6379'
export KAFKA_BROKERS='localhost:9092,localhost:9095,localhost:9096'
export IICPC_INTEGRATION=1
```

## Go

All Go tests:

```bash
env GOCACHE=/tmp/iicpc-go-build-cache go test \
  ./libs/go/... \
  ./schemas/go/... \
  ./services/auth-api/... \
  ./services/submission-api/... \
  ./services/build-worker/... \
  ./services/bot-fleet-controller/... \
  ./services/sandbox-orchestrator/... \
  ./services/correctness-validator/... \
  ./services/leaderboard-api/... \
  ./services/score-computer/...
```

Race tests:

```bash
env GOCACHE=/tmp/iicpc-go-build-cache go test -race -count=1 \
  ./libs/go/... \
  ./schemas/go/... \
  ./services/auth-api/... \
  ./services/submission-api/... \
  ./services/build-worker/... \
  ./services/bot-fleet-controller/... \
  ./services/sandbox-orchestrator/... \
  ./services/correctness-validator/... \
  ./services/leaderboard-api/... \
  ./services/score-computer/...
```

Vet:

```bash
env GOCACHE=/tmp/iicpc-go-build-cache go vet \
  ./libs/go/... \
  ./schemas/go/... \
  ./services/auth-api/... \
  ./services/submission-api/... \
  ./services/build-worker/... \
  ./services/bot-fleet-controller/... \
  ./services/sandbox-orchestrator/... \
  ./services/correctness-validator/... \
  ./services/leaderboard-api/... \
  ./services/score-computer/...
```

Service-specific tests:

```bash
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/auth-api/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/submission-api/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/build-worker/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/bot-fleet-controller/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/sandbox-orchestrator/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/correctness-validator/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/leaderboard-api/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/score-computer/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./libs/go/...
env GOCACHE=/tmp/iicpc-go-build-cache go test ./schemas/go/...
```

Correctness validator integration:

```bash
KAFKA_BROKERS=localhost:9092 \
DATABASE_URL='postgres://iicpc:iicpc@localhost:5433/iicpc?sslmode=disable' \
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/correctness-validator/... -run Integration -v
```

Score computer integration:

```bash
IICPC_INTEGRATION=1 \
DATABASE_URL='postgres://iicpc:iicpc@localhost:5433/iicpc?sslmode=disable' \
TIMESCALE_URL='postgres://iicpc:iicpc@localhost:5434/metrics' \
REDIS_ADDR=localhost:6379 \
KAFKA_BROKERS=localhost:9092 \
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/score-computer/... -v
```

Leaderboard API integration:

```bash
IICPC_INTEGRATION=1 \
DATABASE_URL='postgres://iicpc:iicpc@localhost:5433/iicpc?sslmode=disable' \
TIMESCALE_URL='postgres://iicpc:iicpc@localhost:5434/metrics' \
REDIS_ADDR=localhost:6379 \
KAFKA_BROKERS=localhost:9092 \
env GOCACHE=/tmp/iicpc-go-build-cache go test ./services/leaderboard-api/... -v
```

## Rust

Format check:

```bash
cargo fmt --all -- --check
```

Workspace check:

```bash
cargo check --workspace
```

Workspace tests:

```bash
cargo test --workspace
```

Telemetry ingester integration:

```bash
KAFKA_BROKERS=localhost:9092 \
TIMESCALE_URL='postgres://iicpc:iicpc@localhost:5434/metrics' \
REDIS_URL=redis://localhost:6379 \
cargo test -p iicpc-telemetry-ingester --test integration -- --nocapture
```

Bot fleet Kafka integration:

```bash
KAFKA_BROKERS=localhost:9092 \
cargo test -p iicpc-bot-fleet --test kafka_integration -- --nocapture
```

eBPF latency tests:

```bash
cargo test -p iicpc-ebpf-latency
```

Privileged eBPF tests:

```bash
sudo -E cargo test -p iicpc-ebpf-latency --test real_ebpf -- --ignored --nocapture
sudo -E cargo test -p iicpc-ebpf-latency --test mtu_clamp -- --ignored --nocapture
```

## Frontend

```bash
cd frontend
npm install
npm run lint
npm run test
npm run build
```

## Smoke Checks

Compose:

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml ps
curl -f http://localhost:9000/minio/health/ready
curl -f http://localhost:8081/healthz
curl -f http://localhost:8082/healthz
```

Kubernetes:

```bash
kubectl get pods -A
kubectl -n data wait --for=condition=Ready pod -l app=kafka --timeout=420s
kubectl -n platform rollout status deployment/auth-api --timeout=300s
kubectl -n platform rollout status deployment/submission-api --timeout=300s
kubectl -n platform rollout status deployment/leaderboard-api --timeout=300s
kubectl -n platform rollout status deployment/frontend --timeout=300s
kubectl -n build rollout status deployment/spawner --timeout=300s
kubectl -n sandbox rollout status deployment/sandbox-orchestrator --timeout=300s
kubectl -n benchmark rollout status deployment/bot-fleet-controller --timeout=300s
kubectl -n benchmark rollout status deployment/bot-fleet-worker --timeout=300s
kubectl -n benchmark rollout status deployment/telemetry-ingester --timeout=300s
kubectl -n benchmark rollout status deployment/correctness-validator --timeout=300s
kubectl -n benchmark rollout status deployment/score-computer --timeout=300s
```
