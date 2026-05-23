FROM golang:1.23-bookworm AS builder
WORKDIR /app
COPY . .
RUN go build -C services/submission-api -trimpath -ldflags="-s -w" -o /out/submission-api .

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /out/submission-api /usr/local/bin/submission-api
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/submission-api"]
