# Build from repo root:
#   docker build -f services/build-worker/Dockerfile .
#
# Built in module mode (GOWORK=off), NOT workspace mode. The module's go.mod
# replace directives point at ../../libs/go and ../../schemas/go, so only those two
# siblings plus this service are needed — adding modules to go.work can never break
# this build. go.sum pins every dependency, so the build stays reproducible. Build
# context is the repo root (COPY paths are repo-relative).
#
# This image contains only the spawner and fetcher binaries.
# Build Jobs use official images directly: gcr.io/kaniko-project/executor,
# aquasec/trivy, and anchore/syft — no need to bundle them here.

FROM golang:1.25 AS builder
ENV GOWORK=off
WORKDIR /workspace
COPY schemas/go ./schemas/go
COPY libs/go ./libs/go
COPY services/build-worker ./services/build-worker
WORKDIR /workspace/services/build-worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/spawner ./cmd/spawner
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/fetcher ./cmd/fetcher

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/spawner /usr/local/bin/spawner
COPY --from=builder /bin/fetcher /usr/local/bin/fetcher
# Non-root UID so deployments/Job init-containers with runAsNonRoot=true pass
# admission. The fetcher writes only to an emptyDir, writable by any UID.
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/spawner"]
