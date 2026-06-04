FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.work go.work.sum ./
COPY libs/go ./libs/go
COPY schemas/go ./schemas/go
COPY services/leaderboard-api ./services/leaderboard-api
WORKDIR /src/services/leaderboard-api
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/leaderboard-api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/leaderboard-api /leaderboard-api
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/leaderboard-api"]
