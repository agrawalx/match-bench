// Package model defines tests for participant test.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package model

import "testing"

// TestParticipantOf performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestParticipantOf(t *testing.T) {
	cases := []struct {
		orderID string
		want    string
	}{
		{"sess_42_7_O", "42"},
		{"sess_42_7_M", "42"},
		{"sess_42_7_C", "42"},
		{"sess_42_7_R", "42"},
		{"my_long_sess_99_1234_O", "99"},
		{"S_3_0_O", "3"},
	}
	for _, c := range cases {
		if got := ParticipantOf(c.orderID); got != c.want {
			t.Errorf("ParticipantOf(%q) = %q, want %q", c.orderID, got, c.want)
		}
	}
}

// TestParticipantOfMalformedIsWholeID performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func TestParticipantOfMalformedIsWholeID(t *testing.T) {
	for _, id := range []string{"", "noseparators", "a_b"} {
		if got := ParticipantOf(id); got != id {
			t.Errorf("ParticipantOf(%q) = %q, want the whole id %q", id, got, id)
		}
	}
}
