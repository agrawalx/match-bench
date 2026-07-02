package main

import (
	"testing"

	"github.com/iicpc/submission-api/internal/scenarios"
)

// TestFilterScenarios covers the SEED_SCENARIOS allowlist: only named scenarios
// survive, order is preserved, and unknown/blank entries are ignored rather than
// erroring.
func TestFilterScenarios(t *testing.T) {
	rows := []scenarios.ScenarioRow{
		{Name: "constant"},
		{Name: "spike"},
		{Name: "ramp"},
	}

	cases := []struct {
		name string
		csv  string
		want []string
	}{
		{"single", "constant", []string{"constant"}},
		{"subset preserves order", "ramp,constant", []string{"constant", "ramp"}},
		{"whitespace tolerated", " constant , spike ", []string{"constant", "spike"}},
		{"unknown ignored", "constant,bogus", []string{"constant"}},
		{"none match", "bogus", nil},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := filterScenarios(rows, c.csv)
			if len(got) != len(c.want) {
				t.Fatalf("len = %d (%v), want %d (%v)", len(got), names(got), len(c.want), c.want)
			}
			for i, r := range got {
				if r.Name != c.want[i] {
					t.Errorf("row %d = %q, want %q", i, r.Name, c.want[i])
				}
			}
		})
	}
}

func names(rows []scenarios.ScenarioRow) []string {
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = r.Name
	}
	return out
}
