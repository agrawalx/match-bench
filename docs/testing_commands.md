# Testing Commands

This file is only for test and validation commands. For local startup commands, use `docs/local-run.md`. For AWS deployment commands, use `docs/aws-deployment.md`.

Run commands from the repository root unless a command changes directories.

## Shared Integration Environment

Start the full local validation stack:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  -f docker-compose.observability.yml \
  up -d --build
```

This starts PostgreSQL, TimescaleDB, Redis, MinIO, Kafka, Kafka topic initialization, the compose-hosted platform APIs, Prometheus, Grafana, Loki, and the PostgreSQL exporter.

For tests that only need shared dependencies and run services from source, use the lighter stack:

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d
```

Then export:

```bash
export DATABASE_URL='postgres://<db-user>:<db-password>@localhost:5433/iicpc?sslmode=disable'
export TIMESCALE_URL='postgres://<metrics-user>:<metrics-password>@localhost:5434/metrics'
export REDIS_ADDR='localhost:6379'
export REDIS_URL='redis://localhost:6379'
export KAFKA_BROKERS='localhost:9092,localhost:9095,localhost:9096'
export IICPC_INTEGRATION=1
```

## Go

All Go tests:

```bash
go test \
  ./libs/go/... \
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
go test ./services/auth-api/...
go test ./services/submission-api/...
go test ./services/build-worker/...
go test ./services/bot-fleet-controller/...
go test ./services/sandbox-orchestrator/...
go test ./services/correctness-validator/...
go test ./services/leaderboard-api/...
go test ./services/score-computer/...
go test ./libs/go/...
```

Race-focused tests:

```bash
go test -race -count=1 ./services/bot-fleet-controller/internal/controller -run TestSessionManagerConcurrentAccess
go test -race -count=1 ./services/sandbox-orchestrator/internal/store -run TestSlotStoreConcurrentAccess
```

Correctness validator integration:

```bash
KAFKA_BROKERS=localhost:9092,localhost:9095,localhost:9096 \
DATABASE_URL='postgres://<db-user>:<db-password>@localhost:5433/iicpc?sslmode=disable' \
go test \
  ./services/correctness-validator \
  ./services/correctness-validator/internal/source \
  ./services/correctness-validator/internal/store \
  -run Integration -v
```

Score computer integration:

```bash
IICPC_INTEGRATION=1 \
DATABASE_URL='postgres://<db-user>:<db-password>@localhost:5433/iicpc?sslmode=disable' \
TIMESCALE_URL='postgres://<metrics-user>:<metrics-password>@localhost:5434/metrics' \
REDIS_ADDR=localhost:6379 \
KAFKA_BROKERS=localhost:9092,localhost:9095,localhost:9096 \
go test ./services/score-computer -v
```

Leaderboard API integration:

```bash
IICPC_INTEGRATION=1 \
DATABASE_URL='postgres://<db-user>:<db-password>@localhost:5433/iicpc?sslmode=disable' \
TIMESCALE_URL='postgres://<metrics-user>:<metrics-password>@localhost:5434/metrics' \
REDIS_ADDR=localhost:6379 \
KAFKA_BROKERS=localhost:9092,localhost:9095,localhost:9096 \
go test ./services/leaderboard-api -v
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

Rust unit tests:

```bash
cargo test -p iicpc-bot-fleet
cargo test -p iicpc-telemetry-ingester
cargo test -p iicpc-ebpf-latency
```

Telemetry ingester integration:

```bash
KAFKA_BROKERS=localhost:9092 \
TIMESCALE_URL='postgres://<metrics-user>:<metrics-password>@localhost:5434/metrics' \
REDIS_URL=redis://localhost:6379 \
cargo test -p iicpc-telemetry-ingester --test integration -- --nocapture
```

Bot fleet Kafka integration:

```bash
KAFKA_BROKERS=localhost:9092 \
cargo test -p iicpc-bot-fleet --test kafka_integration -- --nocapture
```

Privileged eBPF tests:

```bash
IICPC_REAL_EBPF_STRICT=1 sudo -E env "PATH=$PATH" \
  cargo test -p iicpc-ebpf-latency --test real_ebpf -- --ignored --nocapture
IICPC_REAL_EBPF_STRICT=1 sudo -E env "PATH=$PATH" \
  cargo test -p iicpc-ebpf-latency --test mtu_clamp -- --ignored --nocapture
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

Full Docker Compose stack:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  -f docker-compose.observability.yml \
  ps
```

Core dependency health:

```bash
curl -f http://localhost:9000/minio/health/ready
docker compose -f docker-compose.yml exec postgres pg_isready -U iicpc -d iicpc
docker compose -f docker-compose.yml exec timescaledb pg_isready -U iicpc -d metrics
docker compose -f docker-compose.yml exec redis redis-cli ping
docker compose -f docker-compose.yml -f docker-compose.kafka.yml exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --list
```

Platform API health:

```bash
curl -f http://localhost:8081/healthz
curl -f http://localhost:8081/ready
curl -f http://localhost:8082/health
curl -f http://localhost:8082/ready
```

Observability health:

```bash
curl -f http://localhost:9090/-/ready
curl -f http://localhost:3005/api/health
curl -f http://localhost:3100/ready
curl -f http://localhost:9187/metrics
```

Compose logs for failures:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  -f docker-compose.observability.yml \
  logs --tail=120
```

Stop the full Docker Compose stack:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  -f docker-compose.observability.yml \
  down
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
