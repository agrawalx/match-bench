package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Port                 string
	MetadataDatabaseURL  string
	TimescaleDatabaseURL string
	RedisAddr            string
	KafkaBrokers         []string
	StatusGroup          string
	CorrectnessGroup     string
	Concurrency          int
	LeaderboardKey       string
	MetricsAddr          string
}

func Load(log *slog.Logger) Config {
	return Config{
		Port:                 envOr("PORT", "8080"),
		MetadataDatabaseURL:  mustEnv("DATABASE_URL", log),
		TimescaleDatabaseURL: mustEnv("TIMESCALE_URL", log),
		RedisAddr:            envOr("REDIS_ADDR", "redis.data.svc:6379"),
		KafkaBrokers:         parseBrokers(mustEnv("KAFKA_BROKERS", log)),
		StatusGroup:          envOr("KAFKA_STATUS_GROUP", "score-computer"),
		CorrectnessGroup:     envOr("KAFKA_CORRECTNESS_GROUP", "score-computer-correctness"),
		Concurrency:          envInt("SCORER_CONCURRENCY", 4),
		LeaderboardKey:       envOr("LEADERBOARD_KEY", "leaderboard:global"),
		MetricsAddr:          envOr("METRICS_ADDR", "0.0.0.0:9090"),
	}
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

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
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
