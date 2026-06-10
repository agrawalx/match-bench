// Package model implements order behavior.
//
// This file is part of the IICPC benchmarking platform and keeps its
// responsibilities local to the surrounding package. It should be read with
// the service-level design in design.md for broader operational context.
package model

// Flow groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Flow struct {
	SrcIP   uint32
	SrcPort uint16
}

// Less applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (f Flow) Less(o Flow) bool {
	if f.SrcIP != o.SrcIP {
		return f.SrcIP < o.SrcIP
	}
	return f.SrcPort < o.SrcPort
}

type Side int

const (
	Buy Side = iota
	Sell
)

// SideFrom performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func SideFrom(s string) Side {
	if s == "SELL" {
		return Sell
	}
	return Buy
}

type Kind int

const (
	NewLimit Kind = iota
	NewMarket
	Cancel
	Replace
)

// KindFrom performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func KindFrom(payloadType, ordType string) Kind {
	switch payloadType {
	case "CANCEL":
		return Cancel
	case "REPLACE":
		return Replace
	default: // NEW
		if ordType == "MARKET" {
			return NewMarket
		}
		return NewLimit
	}
}

// String applies behavior for its receiver performs the package-specific operation described by its name.
// It keeps validation, side effects, and returned values within this package's contract.
func (k Kind) String() string {
	switch k {
	case NewLimit:
		return "NewLimit"
	case NewMarket:
		return "NewMarket"
	case Cancel:
		return "Cancel"
	case Replace:
		return "Replace"
	default:
		return "Unknown"
	}
}

// Response groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Response struct {
	ExecType  string
	FillQty   uint64
	FillPrice uint64 // fixed-point, scaled by TelemetryPriceScale
	T7Ns      uint64
}

// Order groups the state and dependencies used by this package.
// Keep this type aligned with the runtime contract around it.
type Order struct {
	OrderID     string
	Flow        Flow
	TCPSeq      uint32
	T3Ns        uint64 // request ingress (shared across the order's responses)
	EffectiveT3 uint64 // computed by replay ordering (HOL promotion)
	Side        Side
	Price       int64
	Qty         uint64
	Kind        Kind
	OrigOrderID string // referenced order for cancel/replace (from acked tag 41)
	Responses   []Response
}
