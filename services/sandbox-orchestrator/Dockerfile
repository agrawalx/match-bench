# Build from repo root:
#   docker build -f services/sandbox-orchestrator/Dockerfile .
#
# Built in module mode (GOWORK=off), NOT workspace mode. The module's go.mod
# replace directives point at ../../libs/go and ../../schemas/go, so only those two
# siblings plus this service are needed — adding modules to go.work can never break
# this build. go.sum pins every dependency, so the build stays reproducible. Build
# context is the repo root (COPY paths are repo-relative).

FROM golang:1.25 AS builder
ENV GOWORK=off
WORKDIR /workspace
COPY schemas/go ./schemas/go
COPY libs/go ./libs/go
COPY services/sandbox-orchestrator ./services/sandbox-orchestrator
WORKDIR /workspace/services/sandbox-orchestrator
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/sandbox-orchestrator .

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/sandbox-orchestrator /usr/local/bin/sandbox-orchestrator
ENTRYPOINT ["/usr/local/bin/sandbox-orchestrator"]
