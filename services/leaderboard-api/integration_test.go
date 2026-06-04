package main

import (
	"os"
	"testing"
)

func TestIntegrationEnvironmentDocumented(t *testing.T) {
	if os.Getenv("IICPC_INTEGRATION") != "1" {
		t.Skip("set IICPC_INTEGRATION=1 with DATABASE_URL, TIMESCALE_URL, REDIS_ADDR, and KAFKA_BROKERS to run leaderboard-api integration tests")
	}
}
