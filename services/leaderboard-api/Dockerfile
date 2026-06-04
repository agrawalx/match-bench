FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.work go.work.sum ./
COPY libs/go ./libs/go
COPY schemas/go ./schemas/go
COPY services/leaderboard-api ./services/leaderboard-api
WORKDIR /src/services/leaderboard-api
# GOWORK=off: build in module mode via go.mod replace directives (which point at
# the copied libs/go + schemas/go). The repo go.work `use`s all 8 services, but
# only this one is in the build context, so workspace mode would fail.
RUN GOWORK=off CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/leaderboard-api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/leaderboard-api /leaderboard-api
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/leaderboard-api"]
