package model

import "testing"

// order_id format (bot-fleet src/fix.rs): "{session_id}_{bot_id}_{seq}_{suffix}"
// where suffix is one of O/M/C/R and session_id may itself contain underscores.
// The participant is the bot_id — the token two positions before the suffix.
func TestParticipantOf(t *testing.T) {
	cases := []struct {
		orderID string
		want    string
	}{
		{"sess_42_7_O", "42"},        // simple session id
		{"sess_42_7_M", "42"},        // market suffix
		{"sess_42_7_C", "42"},        // cancel suffix
		{"sess_42_7_R", "42"},        // replace suffix
		{"my_long_sess_99_1234_O", "99"}, // session id contains underscores
		{"S_3_0_O", "3"},
	}
	for _, c := range cases {
		if got := ParticipantOf(c.orderID); got != c.want {
			t.Errorf("ParticipantOf(%q) = %q, want %q", c.orderID, got, c.want)
		}
	}
}

func TestParticipantOfMalformedIsWholeID(t *testing.T) {
	// A self-trade requires two parsed participants to be EQUAL. To avoid false
	// self-trades on ids we can't parse, an unparseable id must yield a token that
	// won't spuriously collide — we return the whole id.
	for _, id := range []string{"", "noseparators", "a_b"} {
		if got := ParticipantOf(id); got != id {
			t.Errorf("ParticipantOf(%q) = %q, want the whole id %q", id, got, id)
		}
	}
}
