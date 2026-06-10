// Package config implements config behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// Config groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Config struct {
	Port                 string
	MetadataDatabaseURL  string
	TimescaleDatabaseURL string
	RedisAddr            string
	KafkaBrokers         []string
	KafkaGroup           string
	PrometheusURL        string
	MetricsAddr          string
}

// Load performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func Load(log *slog.Logger) Config {
	return Config{
		Port:                 envOr("PORT", "8080"),
		MetadataDatabaseURL:  mustEnv("DATABASE_URL", log),
		TimescaleDatabaseURL: mustEnv("TIMESCALE_URL", log),
		RedisAddr:            envOr("REDIS_ADDR", "redis.data.svc:6379"),
		KafkaBrokers:         parseBrokers(mustEnv("KAFKA_BROKERS", log)),
		KafkaGroup:           sseConsumerGroup(),
		PrometheusURL:        envOr("PROMETHEUS_URL", "http://prometheus.observability.svc:9090"),
		MetricsAddr:          envOr("METRICS_ADDR", "0.0.0.0:9090"),
	}
}

// sseConsumerGroup performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func sseConsumerGroup() string {
	base := envOr("KAFKA_GROUP", "leaderboard-api-sse")
	pod := os.Getenv("POD_NAME")
	if pod == "" {
		if host, err := os.Hostname(); err == nil && host != "" {
			pod = host
		} else {
			pod = fmt.Sprintf("pid-%d", os.Getpid())
		}
	}
	return base + "-" + pod
}

// envOr performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// mustEnv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func mustEnv(key string, log *slog.Logger) string {
	v := os.Getenv(key)
	if v == "" {
		log.Error("required env var missing", "var", key)
		os.Exit(1)
	}
	return v
}

// parseBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func parseBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
