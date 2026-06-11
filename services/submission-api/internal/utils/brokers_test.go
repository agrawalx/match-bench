// Package utils defines tests for brokers test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package utils

import (
	"reflect"
	"testing"
)

// TestParseBrokers performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestParseBrokers(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "empty",
			input: "",
			want:  []string{},
		},
		{
			name:  "single",
			input: "localhost:9092",
			want:  []string{"localhost:9092"},
		},
		{
			name:  "comma separated",
			input: "broker1:9092,broker2:9092",
			want:  []string{"broker1:9092", "broker2:9092"},
		},
		{
			name:  "trims whitespace and skips empties",
			input: " broker1:9092, ,broker2:9092, ",
			want:  []string{"broker1:9092", "broker2:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ParseBrokers(tt.input); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("ParseBrokers(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}
