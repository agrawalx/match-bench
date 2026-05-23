# Build from repo root:
#   docker build -f services/build-worker/Dockerfile .
#
# Must build from root because the Go workspace (go.work) spans multiple modules.
#
# This image contains only the spawner and fetcher binaries.
# Build Jobs use official images directly: gcr.io/kaniko-project/executor,
# aquasec/trivy, and anchore/syft — no need to bundle them here.

FROM golang:1.25 AS builder
WORKDIR /workspace
COPY go.work go.work.sum* ./
COPY schemas/go ./schemas/go
COPY libs/go ./libs/go
COPY services/submission-api/go.mod services/submission-api/go.sum* ./services/submission-api/
COPY services/build-worker ./services/build-worker
WORKDIR /workspace/services/build-worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/spawner ./cmd/spawner
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/fetcher ./cmd/fetcher

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/spawner /usr/local/bin/spawner
COPY --from=builder /bin/fetcher /usr/local/bin/fetcher
ENTRYPOINT ["/usr/local/bin/spawner"]
