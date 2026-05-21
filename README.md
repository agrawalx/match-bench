# iicpc

A competitive programming platform where contestants submit trading algorithm code.
The platform builds their container image, runs security scans, deploys it in an isolated pod,
sends synthetic orders to test latency and correctness, and scores the result.

---

## Repo layout

```
iicpc/
│
├── schemas/                        # Shared types across all services
│   ├── go/                         # Go — Kafka message types, status constants
│   └── rust/                       # Rust — (add when first Rust service needs shared types)
│
├── services/                       # One folder per microservice
│   ├── submission-api/             # Go — receives ZIP uploads, kicks off build pipeline
│   ├── build-worker/               # Go — builds, scans, and promotes contestant images
│   ├── bot-fleet/                  # Rust — sends synthetic orders, timestamps ACKs (coming)
│   ├── validation-engine/          # Rust — scores latency + correctness from Kafka (coming)
│   └── telemetry-engine/           # Go — streams status updates to frontend via SSE (coming)
│
├── k8s/                            # Kubernetes manifests — one folder per namespace
│   ├── data/                       # Stateful services: postgres, kafka, minio
│   ├── platform/                   # User-facing: submission-api, telemetry-engine, spawner
│   ├── build/                      # Build Job pods: kaniko, trivy, syft
│   ├── sandbox/                    # Contestant algorithm pods (gVisor isolated)
│   └── benchmark/                  # Bot-worker pods: send orders, measure latency
│
├── docker-compose.yml              # Local dev only — spins up data services
├── go.work                         # Ties all Go modules together
└── .env.example                    # Source of truth for all environment variables
```

---

## Service structure

Every service follows the same layout regardless of language.

**Go service:**
```
services/<name>/
├── Dockerfile
├── go.mod                          # module github.com/iicpc/<name>
├── main.go                         # single binary
└── internal/
    ├── handler/                    # HTTP layer (if HTTP service)
    ├── consumer/                   # Kafka consumer (if event-driven)
    ├── publisher/                  # Kafka producer (if publishes events)
    ├── store/                      # postgres.go, minio.go
    └── <domain>/                   # service-specific logic
```

If a service has multiple binaries (e.g. build-worker has spawner + runner + worker):
```
services/<name>/
└── cmd/
    ├── <binary-1>/main.go
    └── <binary-2>/main.go
```

**Rust service:**
```
services/<name>/
├── Dockerfile
├── Cargo.toml                      # [package] name = "iicpc-<name>"
└── src/
    ├── main.rs
    ├── handler/                    # HTTP layer (if HTTP service)
    ├── consumer/                   # Kafka consumer (if event-driven)
    ├── publisher/                  # Kafka producer
    ├── store/                      # postgres.rs, minio.rs
    └── <domain>/                   # service-specific logic
```

---

## k8s namespace structure

Every namespace folder contains everything needed to deploy that tier.
`kubectl apply -f k8s/<namespace>/` brings up the whole tier.

```
k8s/<namespace>/
├── namespace.yaml
├── network-policy.yaml
└── <service>/
    ├── deployment.yaml             # or statefulset.yaml for stateful services
    ├── service.yaml
    └── secret.yaml                 # placeholder values only — never real credentials
```

---

## Naming conventions

| Thing | Convention | Example |
|---|---|---|
| Services | kebab-case | `bot-fleet`, `validation-engine` |
| Go modules | `github.com/iicpc/<name>` | `github.com/iicpc/build-worker` |
| Rust crates | `iicpc-<name>` | `iicpc-bot-fleet` |
| Go packages | single word, no underscores | `handler`, `consumer`, `store` |
| Rust modules | snake_case | `order_handler`, `latency_store` |
| Kafka topics | `<domain>.<entity>.<event>` | `submission.build.requested` |
| MinIO paths | `<entity>/<id>/<artifact>` | `submissions/{id}/build.log` |
| k8s resources | kebab-case, matches service name | `submission-api`, `build-spawner` |
| Docker images | `harbor.example.com/iicpc/<name>:<git-sha>` | tag is always a git SHA, never `latest` in prod |

---

## Rules

**One module per service.** Each service has its own `go.mod` or `Cargo.toml`.
Services never import each other directly.

**`internal/` for everything (Go) / private modules (Rust).** Nothing inside a service
is importable by another service. If something needs to be shared, it goes in `schemas/`.

**`schemas/` is the only cross-service contract.** Kafka message types and shared constants
live here. Go services import `schemas/go`; Rust services import `schemas/rust`.
Each language subdirectory is its own module.

**Dockerfiles build from repo root.** The Go workspace and Rust workspace both need the
full repo context. All `docker build` commands run from `/` of the repo:
```bash
docker build -f services/<name>/Dockerfile .
```

**k8s manifests mirror the namespace.** All resources for a namespace live inside
`k8s/<namespace>/`. Applying a single folder deploys the entire tier.

**`docker-compose.yml` is for data services only.** Application services run with
`go run` or `cargo run` locally. Compose only provides the backing infrastructure
(postgres, minio, kafka).

**Secrets never in the repo.** `secret.yaml` files contain `changeme` placeholders
with a comment showing the `kubectl` command to generate real values.
Real credentials go in `.env` (gitignored) locally, or injected at deploy time in k8s.

**`.env.example` is the contract.** Every environment variable any service reads must
be documented there with a comment explaining what it does. Add your vars there first
before writing the code that reads them.

---

## Running locally

```bash
# 1. Start backing services
docker-compose up -d

# 2. Set up environment
cp .env.example .env
# edit .env with real values if needed

# 3. Run a service (Go)
go run ./services/submission-api

# 4. Run a service (Rust)
cargo run -p iicpc-bot-fleet

# 5. Submit a test ZIP
curl -X POST http://localhost:8080/submit \
  -F "file=@test.zip" \
  -F "language=go" \
  -F "protocol=tcp" \
  -F "port=8080" \
  -F "team_name=team1"
```

**Prerequisites:** Go 1.23+, Rust (latest stable), Docker, `kubectl`
