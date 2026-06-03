// Package model holds the validator's in-memory domain types: a per-order record
// joined from orders.sent (price/qty/side/type) and orders.acked (flow/t3/fills),
// the reference order book's view of an order, and the violation types.
package model

// Flow identifies one TCP connection by the bot (client) tuple — the same on both
// directions, as the eBPF capture reports it.
type Flow struct {
	SrcIP   uint32
	SrcPort uint16
}

// Less gives a deterministic total order over flows for the replay tiebreaker.
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

func SideFrom(s string) Side {
	if s == "SELL" {
		return Sell
	}
	return Buy
}

// Kind is the request type, derived from payload_type × ord_type.
type Kind int

const (
	NewLimit Kind = iota
	NewMarket
	Cancel
	Replace
)

// KindFrom maps the wire enums (payload_type "NEW"|"CANCEL"|"REPLACE",
// ord_type "LIMIT"|"MARKET") to a request Kind.
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

// Response is one contestant-reported execution report for an order, from
// orders.acked. exec_type is the raw FIX tag 150/39 value ("0","1","2","4","8","F").
type Response struct {
	ExecType  string
	FillQty   uint64
	FillPrice uint64 // fixed-point, scaled by TelemetryPriceScale
	T7Ns      uint64
}

// Order is one logical order, joined from its sent event and its acked events.
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
