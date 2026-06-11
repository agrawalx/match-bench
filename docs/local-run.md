# Local Run Guide

This guide starts the project locally with Docker Compose for dependencies and source commands for individual services.

## Prerequisites

- Docker and Docker Compose
- Go matching `go.work`
- Rust stable
- Node.js 22 and npm
- `kubectl` if you run services that create Kubernetes Pods or Jobs

## Environment Files

```bash
cp .env.example .env
cp frontend/.env.example frontend/.env.local
```

## Start Core Dependencies

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml up -d
```

This starts PostgreSQL, TimescaleDB, Redis, MinIO, Kafka, and Kafka topic initialization.

## Optional Platform APIs In Compose

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  up -d --build
```

This starts `leaderboard-api` and `submission-api` in containers.

## Optional Observability

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.observability.yml \
  up -d
```

## Local Endpoints

| Component | Endpoint |
|---|---|
| PostgreSQL | `localhost:5433` |
| TimescaleDB | `localhost:5434` |
| Redis | `localhost:6379` |
| Kafka | `localhost:9092,localhost:9095,localhost:9096` |
| MinIO API | `http://localhost:9000` |
| MinIO console | `http://localhost:9001` |
| leaderboard-api | `http://localhost:8081` |
| submission-api | `http://localhost:8082` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3005` |
| Loki | `http://localhost:3100` |
| postgres-exporter | `http://localhost:9187/metrics` |

## Shared Shell Environment

Use this when running services from source:

```bash
export DATABASE_URL='postgres://<db-user>:<db-password>@localhost:5433/iicpc?sslmode=disable'
export TIMESCALE_URL='postgres://<metrics-user>:<metrics-password>@localhost:5434/metrics'
export REDIS_ADDR='localhost:6379'
export REDIS_URL='redis://localhost:6379'
export KAFKA_BROKERS='localhost:9092,localhost:9095,localhost:9096'
export MINIO_ENDPOINT='localhost:9000'
export MINIO_ACCESS_KEY='<minio-access-key>'
export MINIO_SECRET_KEY='<minio-secret-key>'
export MINIO_BUCKET='submissions'
export MINIO_CREATE_BUCKET_IF_MISSING='true'
```

## Platform Services

Auth API:

```bash
PORT=8083 \
GOOGLE_CLIENT_ID='<google-client-id>' \
GOOGLE_CLIENT_SECRET='<google-client-secret>' \
GOOGLE_ALLOWED_REDIRECT_URIS=http://localhost:3000/auth/callback \
AUTH_COOKIE_SECURE=false \
go run ./services/auth-api
```

Submission API:

```bash
PORT=8082 AUTH_REQUIRED=false go run ./services/submission-api
```

Leaderboard API:

```bash
PORT=8081 PROMETHEUS_URL=http://localhost:9090 go run ./services/leaderboard-api
```

## Benchmark And Scoring Services

Bot fleet controller:

```bash
PORT=8084 go run ./services/bot-fleet-controller
```

Bot fleet worker:

```bash
cargo run -p iicpc-bot-fleet
```

Telemetry ingester:

```bash
cargo run -p iicpc-telemetry-ingester
```

Correctness validator:

```bash
PORT=8085 go run ./services/correctness-validator
```

Score computer:

```bash
PORT=8086 go run ./services/score-computer
```

## Build And Sandbox Services

Build worker:

```bash
METRICS_PORT=9102 go run ./services/build-worker/cmd/worker
```

Build spawner:

```bash
METRICS_PORT=9103 go run ./services/build-worker/cmd/spawner
```

Sandbox orchestrator:

```bash
PORT=8087 K8S_NAMESPACE=sandbox CAPTURE_ENABLED=false go run ./services/sandbox-orchestrator
```

`build-worker/cmd/spawner` and `sandbox-orchestrator` require Kubernetes access for real build jobs and algo pod allocation.

## eBPF Latency Capture

Run eBPF capture only on Linux with the required privileges and target interface.

```bash
sudo -E KAFKA_BROKERS=localhost:9092 \
  SESSION_ID=local-session \
  CONTESTANT_ID=local-contestant \
  EBPF_IFACE=<interface> \
  cargo run -p iicpc-ebpf-latency
```

## Frontend

```bash
cd frontend
npm install
LEADERBOARD_API_URL=http://localhost:8081 \
SUBMISSION_API_URL=http://localhost:8082 \
AUTH_API_URL=http://localhost:8083 \
npm run dev
```

Open `http://localhost:3000`.

## Metrics Ports

Most services expose Prometheus metrics on a separate listener. Assign unique ports when running several services on the host.

```bash
METRICS_PORT=9102 go run ./services/build-worker/cmd/worker
METRICS_ADDR=0.0.0.0:9104 go run ./services/score-computer
METRICS_PORT=9105 cargo run -p iicpc-bot-fleet
```

## Local Smoke Checks

```bash
docker compose -f docker-compose.yml -f docker-compose.kafka.yml ps
curl -f http://localhost:9000/minio/health/ready
curl -f http://localhost:8081/healthz
curl -f http://localhost:8082/healthz
```

## Stop Local Stack

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  -f docker-compose.observability.yml \
  down
```

Remove volumes as well:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.kafka.yml \
  -f docker-compose.platform.yml \
  -f docker-compose.observability.yml \
  down -v
```
