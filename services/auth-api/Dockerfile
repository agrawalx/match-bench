FROM golang:1.25-alpine AS build
WORKDIR /src
COPY services/auth-api/go.mod ./
RUN go mod download
COPY services/auth-api/ ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/auth-api .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/auth-api /auth-api
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/auth-api"]
