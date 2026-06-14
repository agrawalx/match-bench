// Package main starts the loadgen-seed service.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/iicpc/schemas/topics"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	targets := parseTargets(getenv("LOADGEN_TARGETS", "20000,60000,150000"))
	conns := getenvInt("LOADGEN_CONNS", 256)
	retailConns := getenvInt("LOADGEN_RETAIL_CONNS", 0)
	instConns := getenvInt("LOADGEN_INST_CONNS", 0)
	durationS := getenvInt("LOADGEN_DURATION_S", 120)
	if conns <= 0 || durationS <= 0 || len(targets) == 0 || retailConns < 0 || instConns < 0 {
		fmt.Fprintln(os.Stderr, "invalid LOADGEN_CONNS / LOADGEN_DURATION_S / LOADGEN_TARGETS / LOADGEN_RETAIL_CONNS / LOADGEN_INST_CONNS")
		os.Exit(1)
	}
	durationNs := uint64(time.Duration(durationS) * time.Second)

	for i, target := range targets {
		sortOrder := i + 1
		name := fmt.Sprintf("load-%dk", target/1000)
		tasks := buildScenario(uint32(target), uint32(conns), uint32(retailConns), uint32(instConns), durationNs)
		j, err := json.Marshal(tasks)
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal %s: %v\n", name, err)
			os.Exit(1)
		}
		fmt.Printf("UPDATE scenarios SET name='%s', duration_ns=%d, sort_order=%d, task_specs=$JSON$%s$JSON$ WHERE sort_order=%d;\n",
			name, durationNs, sortOrder, string(j), sortOrder)
	}
}

// buildLean performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func buildLean(target, conns uint32, durationNs uint64) []topics.TaskSpec {
	base := target / conns
	rem := target % conns
	tasks := make([]topics.TaskSpec, 0, conns)
	for i := uint32(0); i < conns; i++ {
		rps := base
		if i < rem {
			rps++
		}
		if rps == 0 {
			continue // target < conns: skip the zero-rps tail (validate_spec rejects 0)
		}
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        i,
			Profile:       "hft",
			TargetRPS:     rps,
			StartOffsetNs: 0,
			DurationNs:    durationNs,
			MarketPct:     0, // pure NewOrderSingle limit orders — no cancel/replace
			CancelPct:     0,
			ReplacePct:    0,
		})
	}
	return tasks
}

// Per-profile order rates and order-type mix, mirroring
// services/submission-api/internal/scenarios/builder.go so loadgen tasks behave
// bot-for-bot like the realistic-mix scenarios. HFT stays pure new-limit
// (buildLean) so it drives the headline throughput number; a small *fixed*
// count of retail/institutional tasks layers on the market + cancel/replace
// code paths without the 5-orders/s retail profile exploding the task count
// (the percentage builder yields 25k+ retail tasks at a 500k budget).
const (
	rpsPerRetail        uint32 = 5
	rpsPerInstitutional uint32 = 300

	retailMarketPct  uint8 = 65
	retailCancelPct  uint8 = 5
	retailReplacePct uint8 = 0

	instMarketPct  uint8 = 20
	instCancelPct  uint8 = 0
	instReplacePct uint8 = 0
)

// buildScenario assembles one scenario's task list: hftConns pure-new HFT tasks
// (the throughput driver, carrying the full `target` rps) followed by fixed
// counts of retail and institutional tasks at their profile-standard rates.
// TaskIDs are contiguous and unique across the three profiles.
func buildScenario(target, hftConns, retailConns, instConns uint32, durationNs uint64) []topics.TaskSpec {
	tasks := buildLean(target, hftConns, durationNs)
	tasks = append(tasks, buildProfile(uint32(len(tasks)), "retail", rpsPerRetail, retailConns,
		durationNs, retailMarketPct, retailCancelPct, retailReplacePct)...)
	tasks = append(tasks, buildProfile(uint32(len(tasks)), "institutional", rpsPerInstitutional, instConns,
		durationNs, instMarketPct, instCancelPct, instReplacePct)...)
	return tasks
}

// buildProfile emits `conns` identical tasks for one profile at a fixed rate,
// numbering TaskIDs from startID. Returns an empty slice when conns == 0.
func buildProfile(startID uint32, profile string, rps, conns uint32, durationNs uint64,
	marketPct, cancelPct, replacePct uint8) []topics.TaskSpec {
	tasks := make([]topics.TaskSpec, 0, conns)
	for i := uint32(0); i < conns; i++ {
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        startID + i,
			Profile:       profile,
			TargetRPS:     rps,
			StartOffsetNs: 0,
			DurationNs:    durationNs,
			MarketPct:     marketPct,
			CancelPct:     cancelPct,
			ReplacePct:    replacePct,
		})
	}
	return tasks
}

// parseTargets performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func parseTargets(s string) []int {
	var out []int
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 {
			continue
		}
		out = append(out, n)
	}
	return out
}

// getenv performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// getenvInt performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
