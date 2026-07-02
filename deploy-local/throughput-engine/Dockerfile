# Build the throughput ack-engine (static Go binary) and run it on distroless.
# Go 1.25 sets GOMAXPROCS from the cgroup CPU quota automatically, so the engine
# spawns one epoll reactor per allotted core (not per host core) — no CFS throttling.
FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY src ./src
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /throughput-engine ./src

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /throughput-engine /throughput-engine
EXPOSE 8080
ENTRYPOINT ["/throughput-engine"]
