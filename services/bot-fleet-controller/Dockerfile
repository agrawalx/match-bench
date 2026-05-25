# Build from repo root:
#   docker build -f services/bot-fleet-controller/Dockerfile .
#
# Must build from root because the Go workspace (go.work) spans multiple modules.

FROM golang:1.25 AS builder
WORKDIR /workspace
COPY go.work go.work.sum* ./
COPY schemas/go ./schemas/go
COPY libs/go ./libs/go
COPY services/submission-api/go.mod services/submission-api/go.sum* ./services/submission-api/
COPY services/build-worker/go.mod services/build-worker/go.sum* ./services/build-worker/
COPY services/sandbox-orchestrator/go.mod services/sandbox-orchestrator/go.sum* ./services/sandbox-orchestrator/
COPY services/bot-fleet-controller ./services/bot-fleet-controller
WORKDIR /workspace/services/bot-fleet-controller
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/bot-fleet-controller .

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/bot-fleet-controller /usr/local/bin/bot-fleet-controller
ENTRYPOINT ["/usr/local/bin/bot-fleet-controller"]
