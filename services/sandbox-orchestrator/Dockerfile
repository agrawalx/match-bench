# Build from repo root:
#   docker build -f services/sandbox-orchestrator/Dockerfile .
#
# Must build from root because the Go workspace (go.work) spans multiple modules.

FROM golang:1.25 AS builder
WORKDIR /workspace
COPY go.work go.work.sum* ./
COPY schemas/go ./schemas/go
COPY libs/go ./libs/go
# Other module manifests are needed so go.work resolves cleanly during build.
COPY services/submission-api/go.mod services/submission-api/go.sum* ./services/submission-api/
COPY services/build-worker/go.mod services/build-worker/go.sum* ./services/build-worker/
COPY services/sandbox-orchestrator ./services/sandbox-orchestrator
WORKDIR /workspace/services/sandbox-orchestrator
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/sandbox-orchestrator .

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/sandbox-orchestrator /usr/local/bin/sandbox-orchestrator
ENTRYPOINT ["/usr/local/bin/sandbox-orchestrator"]
