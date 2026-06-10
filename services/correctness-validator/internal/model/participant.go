// Package model implements participant behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package model

import "strings"

// ParticipantOf performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func ParticipantOf(orderID string) string {
	parts := strings.Split(orderID, "_")
	if len(parts) < 4 {
		return orderID
	}
	return parts[len(parts)-3]
}
