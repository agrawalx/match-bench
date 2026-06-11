// Package errors implements errors behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package errors

import "errors"

var (
	ErrSlotNotFound      = errors.New("slot not found")
	ErrSlotImageMismatch = errors.New("slot exists with different image")
	ErrSlotInternal      = errors.New("internal slot error")
	ErrInvalidRequest    = errors.New("invalid request")
)
