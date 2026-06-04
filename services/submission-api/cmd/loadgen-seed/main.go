// Command loadgen-seed emits SQL that turns the three scenario rows into a lean
// single-pod load ramp: three "constant" tests at rising target RPS, each made of
// a FIXED number of connections (LOADGEN_CONNS) firing pure NewOrderSingle limit
// orders (no market/cancel/replace). Unlike the realistic 60/25/15 mix, the
// connection count stays small and fixed as RPS climbs (the mix would need
// thousands of retail sockets at high RPS), so the only thing scaling is
// per-connection rate — ideal for finding one pod's generation ceiling.
//
// It UPDATEs the existing rows by sort_order (1,2,3), preserving scenario_id so
// foreign keys from past runs stay valid. StartBenchmark runs all three rows
// serially, so one trigger = an automatic 3-step ramp.
//
// Env (all optional):
//
//	LOADGEN_TARGETS    comma-separated aggregate RPS levels (default "20000,60000,150000")
//	LOADGEN_CONNS      connections per test (default 256)
//	LOADGEN_DURATION_S seconds per test (default 120)
//
//	GOWORK=off go run ./cmd/loadgen-seed | psql ...
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
		// $JSON$-quote the task_specs literal; JSON never contains the tag.
		fmt.Printf("UPDATE scenarios SET name='%s', duration_ns=%d, sort_order=%d, task_specs=$JSON$%s$JSON$ WHERE sort_order=%d;\n",
			name, durationNs, sortOrder, string(j), sortOrder)
	}
}

// buildLean spreads `target` RPS across `conns` connections (the remainder goes
// to the first `target % conns` connections, +1 rps each) so the per-test
// aggregate hits `target` exactly. All tasks are pure-limit HFT-profile orders.
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

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func getenvInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
