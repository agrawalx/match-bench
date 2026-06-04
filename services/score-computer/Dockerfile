FROM golang:1.25-bookworm AS build
WORKDIR /src
COPY go.work go.work.sum ./
COPY libs/go ./libs/go
COPY schemas/go ./schemas/go
COPY services/score-computer ./services/score-computer
WORKDIR /src/services/score-computer
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/score-computer .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/score-computer /score-computer
USER nonroot:nonroot
EXPOSE 8080 9090
ENTRYPOINT ["/score-computer"]
