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
	durationS := getenvInt("LOADGEN_DURATION_S", 120)
	if conns <= 0 || durationS <= 0 || len(targets) == 0 {
		fmt.Fprintln(os.Stderr, "invalid LOADGEN_CONNS / LOADGEN_DURATION_S / LOADGEN_TARGETS")
		os.Exit(1)
	}
	durationNs := uint64(time.Duration(durationS) * time.Second)

	for i, target := range targets {
		sortOrder := i + 1
		name := fmt.Sprintf("load-%dk", target/1000)
		tasks := buildLean(uint32(target), uint32(conns), durationNs)
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
			continue
		}
		tasks = append(tasks, topics.TaskSpec{
			TaskID:        i,
			Profile:       "hft",
			TargetRPS:     rps,
			StartOffsetNs: 0,
			DurationNs:    durationNs,
			MarketPct:     0,
			CancelPct:     0,
			ReplacePct:    0,
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
