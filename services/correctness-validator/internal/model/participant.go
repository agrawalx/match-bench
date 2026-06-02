package model

import "strings"

// ParticipantOf extracts the participant identity (bot_id) embedded in an order_id.
//
// The bot fleet (bot-fleet src/fix.rs) constructs every ClOrdID as
// "{session_id}_{bot_id}_{seq}_{suffix}", where suffix is one of O/M/C/R and the
// session_id may itself contain underscores. The bot_id is therefore the token two
// positions before the final suffix (index len-3 when split on "_").
//
// We use bot_id — NOT the FIX SenderCompID, which is the uniform "IICPC-BOT" for
// every bot and so can never distinguish maker from taker. A self-trade is a fill
// whose maker and taker order_ids resolve to the same participant.
//
// An order_id we cannot parse (too few segments) yields the whole id unchanged, so
// two unparseable ids never spuriously compare equal as a self-trade.
func ParticipantOf(orderID string) string {
	parts := strings.Split(orderID, "_")
	if len(parts) < 4 {
		return orderID
	}
	return parts[len(parts)-3]
}
