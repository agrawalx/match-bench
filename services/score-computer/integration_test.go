// Package main defines tests for integration test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"os"
	"testing"
)

// TestIntegrationEnvironmentDocumented performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestIntegrationEnvironmentDocumented(t *testing.T) {
	if os.Getenv("IICPC_INTEGRATION") != "1" {
		t.Skip("set IICPC_INTEGRATION=1 with DATABASE_URL, TIMESCALE_URL, REDIS_ADDR, and KAFKA_BROKERS to run score-computer integration tests")
	}
}
