#!/usr/bin/env bash
# Make one submission against the local stack and trigger its benchmark.
# Requires deploy-local/forward.sh running (submission-api on :8088).
# AUTH_REQUIRED=false locally, so an unsigned dev token is accepted.
set -euo pipefail
API="${API:-http://localhost:8088}"

# dev bearer token: unsigned, carries sub=devuser (insecure middleware trusts it)
PAYLOAD=$(printf '{"sub":"devuser"}' | basenc --base64url 2>/dev/null | tr -d '=' || printf '{"sub":"devuser"}' | base64 | tr '+/' '-_' | tr -d '=')
TOKEN="eyJhbGciOiJub25lIn0.${PAYLOAD}."
AUTH="Authorization: Bearer ${TOKEN}"

# minimal REST echo "matching engine" (responds 200 to every request)
WORK=$(mktemp -d); mkdir -p "$WORK/src"
cat > "$WORK/benchmark.yaml" <<'EOF'
protocol: REST
language: go
build:
  type: go
  target: algo
port: 8080
team_name: devtest
EOF
cat > "$WORK/go.mod" <<'EOF'
module github.com/test/algo
go 1.23
EOF
cat > "$WORK/src/main.go" <<'EOF'
package main

import (
	"encoding/json"
	"net/http"
)

// Minimal REST "matching engine": for every order it returns an execution
// report echoing cl_ord_id (so the eBPF capture can pair request<->response by
// cl_ord_id and emit orders.acked) with a full fill at the order's price/qty.
func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var o struct {
			ClOrdID string      `json:"cl_ord_id"`
			Qty     json.Number `json:"qty"`
			Price   json.Number `json:"price"`
		}
		dec := json.NewDecoder(r.Body)
		dec.UseNumber()
		_ = dec.Decode(&o)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"cl_ord_id":  o.ClOrdID,
			"exec_type":  "2", // FILL
			"fill_qty":   o.Qty,
			"fill_price": o.Price,
			"liquidity":  2, // taker: fills immediately on arrival (FIX 851=2)
		})
	})
	http.ListenAndServe(":8080", nil)
}
EOF
( cd "$WORK" && zip -qr /tmp/iicpc-sub.zip . )
echo ">> submitting /tmp/iicpc-sub.zip"
RESP=$(curl -s -H "$AUTH" -F file=@/tmp/iicpc-sub.zip "$API/submit")
echo "$RESP"
SID=$(echo "$RESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('submission_id',''))" 2>/dev/null || true)
[ -z "$SID" ] && { echo "no submission_id (already submitted? check response above)"; exit 1; }
echo ">> submission_id=$SID  — waiting for build to reach 'ready' (Kaniko build + scan)..."
for i in $(seq 1 120); do
  ST=$(curl -s -H "$AUTH" "$API/submissions/$SID" | python3 -c "import sys,json;print(json.load(sys.stdin).get('status',''))" 2>/dev/null || true)
  echo "   [$i] status=$ST"
  [ "$ST" = "ready" ] && break
  [ "$ST" = "failed" ] && { echo "BUILD FAILED — kubectl logs deploy/spawner -n build; kubectl get jobs -n build"; exit 1; }
  sleep 5
done
echo ">> triggering benchmark (3 sessions: constant/spike/ramp, low RPS)"
BRESP=$(curl -s -X POST -H "$AUTH" "$API/submissions/$SID/benchmark")
echo "$BRESP"
RG=$(echo "$BRESP" | python3 -c "import sys,json;print(json.load(sys.stdin).get('run_group_id',''))" 2>/dev/null || true)
cat <<EOF

>> run_group_id=$RG
   Watch it run:
     kubectl get pods -A -w           # algo-* + capture-* appear in sandbox ns
     Grafana  http://localhost:3000   # throughput (Prometheus) + latency (TimescaleDB)
     Frontend http://localhost:8080   # leaderboard / run detail
EOF
