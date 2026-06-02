# Build from repo root:
#   docker build -f services/correctness-validator/Dockerfile .
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
COPY services/correctness-validator ./services/correctness-validator
WORKDIR /workspace/services/correctness-validator
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/correctness-validator .

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/correctness-validator /usr/local/bin/correctness-validator
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/correctness-validator"]
