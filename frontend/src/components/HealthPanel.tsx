import { useEffect, useState } from "react";
import { getHealthPanel, type HealthPanel as HealthPanelT } from "../api";

const PANEL_DESC: Record<string, string> = {
  kafka_consumer_lag: "Per-topic consumer-group lag across the pipeline.",
  timescaledb_write_rate: "Metric rows/s landing in the TimescaleDB hypertable.",
  capture_rate_vs_orders_sent: "eBPF orders.acked rate vs orders.sent (should track ~1.0).",
  bot_pod_count: "KEDA-scaled bot-fleet worker pods and their scaling state.",
};

export default function HealthPanel() {
  const [h, setH] = useState<HealthPanelT | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    getHealthPanel()
      .then((r) => alive && (setH(r), setLoading(false)))
      .catch(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, []);

  return (
    <div className="card" data-testid="health-panel">
      <h2>Platform health</h2>
      <div className="hint">
        The platform observing itself. Backed by{" "}
        <span className="mono">{h?.source ?? "prometheus"}</span>
        {h?.prometheus_url ? <span className="muted"> · {h.prometheus_url}</span> : null}.
      </div>
      {loading ? (
        <div className="empty">Loading…</div>
      ) : (
        <div className="panel-list">
          {(h?.panels ?? []).map((p) => (
            <div className="p" key={p} data-testid={`panel-${p}`}>
              <div className="n">{p}</div>
              <div className="d">{PANEL_DESC[p] ?? "Prometheus-backed panel."}</div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
