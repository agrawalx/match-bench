package errors

import "errors"

var (
	ErrSlotNotFound      = errors.New("slot not found")
	ErrSlotImageMismatch = errors.New("slot exists with different image")
	ErrSlotInternal      = errors.New("internal slot error")
	ErrInvalidRequest    = errors.New("invalid request")
)
