# Build stage — compiles all three binaries
FROM golang:1.23 AS builder
WORKDIR /workspace
COPY go.work go.work.sum* ./
COPY schemas/go ./schemas/go
COPY services/build-worker ./services/build-worker
WORKDIR /workspace/services/build-worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/worker  ./cmd/worker
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/spawner ./cmd/spawner
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/runner  ./cmd/runner

# Pull tool binaries from official images
FROM aquasec/trivy:latest   AS trivy-src
FROM anchore/syft:latest    AS syft-src
FROM gcr.io/kaniko-project/executor:latest AS kaniko-src

# Final image
FROM debian:12-slim

# SSL certs (needed by kaniko for registry pushes)
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Tool binaries
COPY --from=trivy-src   /usr/local/bin/trivy    /usr/local/bin/trivy
COPY --from=syft-src    /syft                   /usr/local/bin/syft
COPY --from=kaniko-src  /kaniko/executor        /kaniko/executor

# Our binaries
COPY --from=builder /bin/worker  /usr/local/bin/worker
COPY --from=builder /bin/spawner /usr/local/bin/spawner
COPY --from=builder /bin/runner  /usr/local/bin/runner

# Default: local dev worker
ENTRYPOINT ["/usr/local/bin/worker"]
