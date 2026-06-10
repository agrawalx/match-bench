// Package main starts the sweepseed service.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/iicpc/submission-api/internal/scenarios"
)

// main performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func main() {
	type sweep struct {
		oldName string
		newName string
		rps     uint32
		order   int
	}
	sweeps := []sweep{
		{"constant", "constant-25k", 25000, 1},
		{"spike", "constant-50k", 50000, 2},
		{"ramp", "constant-100k", 100000, 3},
	}

	for _, s := range sweeps {
		cfg := scenarios.DefaultConfig()
		cfg.ConstantDuration = 300 * time.Second
		cfg.ConstantTotalRPS = s.rps
		cfg.SpikePeakRPS = s.rps

		rows, err := scenarios.BuildAll(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "build %s: %v\n", s.newName, err)
			os.Exit(1)
		}
		var c scenarios.ScenarioRow
		for _, r := range rows {
			if r.Name == "constant" {
				c = r
			}
		}
		j, err := json.Marshal(c.TaskSpecs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "marshal %s: %v\n", s.newName, err)
			os.Exit(1)
		}
		fmt.Printf("UPDATE scenarios SET name='%s', duration_ns=%d, sort_order=%d, task_specs=$JSON$%s$JSON$ WHERE name='%s';\n",
			s.newName, c.DurationNs, s.order, string(j), s.oldName)
	}
}
