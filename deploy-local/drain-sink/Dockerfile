FROM golang:1.25-alpine AS build
WORKDIR /app
COPY go.mod ./
COPY src ./src
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /drain-sink ./src
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /drain-sink /drain-sink
EXPOSE 8080
ENTRYPOINT ["/drain-sink"]
