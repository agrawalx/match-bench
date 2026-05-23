# Build from repo root:
#   docker build -f services/submission-api/Dockerfile .
#
# Must build from root because the Go workspace (go.work) spans multiple modules.

FROM golang:1.25 AS builder
WORKDIR /workspace
COPY go.work go.work.sum* ./
COPY schemas/go ./schemas/go
COPY libs/go ./libs/go
COPY services/build-worker/go.mod services/build-worker/go.sum* ./services/build-worker/
COPY services/submission-api ./services/submission-api
WORKDIR /workspace/services/submission-api
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/submission-api .

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/submission-api /usr/local/bin/submission-api
ENTRYPOINT ["/usr/local/bin/submission-api"]
