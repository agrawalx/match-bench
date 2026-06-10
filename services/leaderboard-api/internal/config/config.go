package config

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

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

// sseConsumerGroup returns a per-pod Kafka consumer group so EVERY replica
// receives EVERY leaderboard update. The SSE broker only fans out to its own
// connected clients, so a shared group would split the topic's partitions
// across replicas and leave roughly half of the clients snapshot-then-silent.
// POD_NAME comes from the downward API; the hostname (== pod name on k8s) is
// the fallback when it is unset (e.g. local runs).
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

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func mustEnv(key string, log *slog.Logger) string {
	v := os.Getenv(key)
	if v == "" {
		log.Error("required env var missing", "var", key)
		os.Exit(1)
	}
	return v
}

func parseBrokers(s string) []string {
	var out []string
	for _, b := range strings.Split(s, ",") {
		if b = strings.TrimSpace(b); b != "" {
			out = append(out, b)
		}
	}
	return out
}
