// Package dockerfile renders safe, platform-controlled Dockerfiles for
// contestant submissions handled by the build worker.
//
// The package centralizes language-specific build templates so uploaded source
// archives are converted into predictable runtime images. It also validates
// interpolated values before rendering so build metadata cannot become a
// Dockerfile injection vector.
package dockerfile

import (
	"bytes"
	"fmt"
	"regexp"
	"text/template"
)

var validTarget = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// templateVars stores the values interpolated into language-specific templates.
// Keep this type narrow because values are copied directly into Dockerfile text.
type templateVars struct {
	Target string
	Port   int
}

var cppTemplate = template.Must(template.New("cpp").Parse(
	`FROM ubuntu:22.04 AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake build-essential ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY . .
RUN cmake -B out -DCMAKE_BUILD_TYPE=Release && \
    cmake --build out --target {{.Target}} -j$(nproc)

FROM ubuntu:22.04
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/out/{{.Target}} /app/{{.Target}}
EXPOSE {{.Port}}
ENTRYPOINT ["/app/{{.Target}}"]
`))

var rustTemplate = template.Must(template.New("rust").Parse(
	`FROM rust:1.80-slim AS builder
WORKDIR /build
COPY . .
RUN cargo build --release --bin {{.Target}}

FROM debian:12-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /build/target/release/{{.Target}} /app/{{.Target}}
EXPOSE {{.Port}}
ENTRYPOINT ["/app/{{.Target}}"]
`))

var goTemplate = template.Must(template.New("go").Parse(
	`FROM golang:1.23-alpine AS builder
WORKDIR /build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/{{.Target}} ./src

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=builder /bin/{{.Target}} /app/{{.Target}}
EXPOSE {{.Port}}
ENTRYPOINT ["/app/{{.Target}}"]
`))

// Generate renders a Dockerfile for a supported contestant language.
// It validates the target name before template execution and returns the
// complete Dockerfile text for the build pipeline to package.
func Generate(language, _ string, target string, port int) (string, error) {
	if !validTarget.MatchString(target) {
		return "", fmt.Errorf("invalid build target %q: must match %s", target, validTarget.String())
	}
	vars := templateVars{Target: target, Port: port}
	var tmpl *template.Template
	switch language {
	case "cpp":
		tmpl = cppTemplate
	case "rust":
		tmpl = rustTemplate
	case "go":
		tmpl = goTemplate
	default:
		return "", fmt.Errorf("unsupported language: %q", language)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, vars); err != nil {
		return "", fmt.Errorf("render dockerfile template: %w", err)
	}
	return buf.String(), nil
}
