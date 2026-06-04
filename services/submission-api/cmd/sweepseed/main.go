// Command sweepseed emits SQL that repurposes the three existing scenario rows
// (constant, spike, ramp) into a constant-RPS capacity sweep: three 300s
// constant tests at 25k / 50k / 100k orders/sec. It reuses the scenarios
// builder so the task_specs (participant mix, per-bot rates) are identical to a
// real run — only the total RPS and duration differ.
//
// It UPDATEs the existing rows in place (preserving scenario_id, so foreign keys
// from past runs stay valid) rather than inserting new ones. Output is SQL on
// stdout; pipe it into psql.
//
//	GOWORK=off go run ./cmd/sweepseed | psql ...
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/iicpc/submission-api/internal/scenarios"
)

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
		// BuildAll validates the whole config (incl. spike peak >= baseline), so
		// raise the spike peak to the sweep RPS even though we only use the
		// constant row.
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
		// $JSON$-quote the task_specs literal; JSON never contains the tag.
		fmt.Printf("UPDATE scenarios SET name='%s', duration_ns=%d, sort_order=%d, task_specs=$JSON$%s$JSON$ WHERE name='%s';\n",
			s.newName, c.DurationNs, s.order, string(j), s.oldName)
	}
}
