import { useEffect, useState } from "react";
import type uPlot from "uplot";
import {
  fmtMicros,
  fmtMillis,
  fmtPct,
  fmtTPS,
  getRunDetail,
  nsToMicros,
  type MetricPoint,
  type RunDetail as RunDetailT,
  type SessionDetail,
} from "../api";
import Uplot from "./Uplot";

const C = { accent: "#4493f8", green: "#3fb950", amber: "#d29922", red: "#f85149", muted: "#8b98a8" };

function relSeconds(tl: MetricPoint[]): number[] {
  if (tl.length === 0) return [];
  const t0 = tl[0].time_unix_ns;
  return tl.map((p) => (p.time_unix_ns - t0) / 1e9);
}

function latencyChart(s: SessionDetail) {
  const tl = s.timeline ?? [];
  const xs = relSeconds(tl);
  // null (not 0) for missing/zero samples — a log y-axis can't range over
  // non-positive values, and old runs predate the rt_* columns (all zero).
  const lv = (ns: number) => (ns > 0 ? nsToMicros(ns) : null);
  const data: uPlot.AlignedData = [
    xs,
    tl.map((p) => lv(p.p50_ns)),
    tl.map((p) => lv(p.p99_ns)),
    tl.map((p) => lv(p.rt_p99_ns)),
  ];
  const opts: Omit<uPlot.Options, "width" | "height"> = {
    // log y-axis: round-trip (r9-t0) can be orders of magnitude above
    // service_time (t7-t3) under coordinated omission.
    scales: { x: { time: false }, y: { distr: 3 } },
    axes: [
      { stroke: C.muted, grid: { stroke: "#222c38" }, label: "seconds" },
      { stroke: C.muted, grid: { stroke: "#222c38" }, label: "µs (log)" },
    ],
    series: [
      { label: "t" },
      { label: "p50 service (diag)", stroke: C.muted, width: 1 },
      { label: "p99 service_time t7-t3 (scored)", stroke: C.accent, width: 2 },
      { label: "p99 round-trip r9-t0 (CO delay)", stroke: C.green, width: 2 },
    ],
  };
  return { data, opts };
}

function tpsChart(s: SessionDetail) {
  const tl = s.timeline ?? [];
  const xs = relSeconds(tl);
  const data: uPlot.AlignedData = [
    xs,
    tl.map((p) => p.tps_1s),
    tl.map((p) => p.error_rate * 100),
  ];
  const opts: Omit<uPlot.Options, "width" | "height"> = {
    scales: { x: { time: false }, err: { auto: true } },
    axes: [
      { stroke: C.muted, grid: { stroke: "#222c38" }, label: "seconds" },
      { stroke: C.muted, grid: { stroke: "#222c38" }, label: "TPS" },
      { stroke: C.muted, side: 1, scale: "err", label: "err %" },
    ],
    series: [
      { label: "t" },
      { label: "tps_1s (diag)", stroke: C.green, width: 2 },
      { label: "error % (diag)", stroke: C.red, width: 1, scale: "err" },
    ],
  };
  return { data, opts };
}

export default function RunDetail({
  runGroupID,
  onBack,
}: {
  runGroupID: string;
  onBack: () => void;
}) {
  const [d, setD] = useState<RunDetailT | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    let alive = true;
    setLoading(true);
    getRunDetail(runGroupID)
      .then((r) => alive && (setD(r), setLoading(false)))
      .catch(() => alive && setLoading(false));
    return () => {
      alive = false;
    };
  }, [runGroupID]);

  const score = d?.score ?? null;

  return (
    <div data-testid="run-detail">
      <button className="back-btn" onClick={onBack} data-testid="back">
        ← Back to leaderboard
      </button>

      <div className="card">
        <h2>
          Run {runGroupID}{" "}
          {score?.team_name ? <span className="muted">· {score.team_name}</span> : null}
        </h2>
        <div className="hint">
          Only <span className="badge scored">p99 service_time</span> is the scored metric; tps,
          p50/p90, and error rate are <span className="badge diag">diagnostic</span>.
        </div>
        {score && (
          <div className="kpis">
            <div className="kpi">
              <div className="label">Peak sustained TPS</div>
              <div className="val">{fmtTPS(score.peak_sustained_tps)}</div>
            </div>
            <div className="kpi">
              <div className="label">p99 @ peak</div>
              <div className="val">{fmtMicros(score.p99_ns_at_peak_tps)}</div>
            </div>
            <div className="kpi">
              <div className="label">Total correctness</div>
              <div className="val">{fmtPct(score.total_correctness)}</div>
            </div>
            <div className="kpi">
              <div className="label">Spike recovery</div>
              <div className="val">{fmtMillis(score.spike_recovery_ns)}</div>
            </div>
          </div>
        )}
        {score?.disqualified && (
          <div style={{ marginTop: 8 }}>
            <span className="badge dq">DISQUALIFIED</span>{" "}
            <span className="muted">{score.disqualification_code}</span>
          </div>
        )}
      </div>

      {loading ? (
        <div className="card empty">Loading run…</div>
      ) : (
        (d?.sessions ?? []).map((s) => {
          const lat = latencyChart(s);
          const tp = tpsChart(s);
          return (
            <div className="card" key={s.session_id} data-testid={`session-${s.scenario}`}>
              <h2>
                <span className="scenario-tag">{s.scenario}</span>{" "}
                <span className="muted" style={{ fontSize: 12 }}>
                  {s.status} · {(s.timeline ?? []).length}s
                </span>
              </h2>
              <div className="chart-grid">
                <div>
                  <div className="chart-legend">Latency percentiles (µs)</div>
                  <Uplot data={lat.data} options={lat.opts} />
                </div>
                <div>
                  <div className="chart-legend">Throughput and error rate</div>
                  <Uplot data={tp.data} options={tp.opts} />
                </div>
              </div>
            </div>
          );
        })
      )}

      <div className="card" data-testid="violations">
        <h2>Violation log</h2>
        <div className="hint">
          Matching-engine violations detected by the reference order-book replay.
        </div>
        {(d?.violations ?? []).length > 0 ? (
          <table>
            <thead>
              <tr>
                <th>Type</th>
                <th>Order</th>
                <th>Session</th>
                <th>Detail</th>
              </tr>
            </thead>
            <tbody>
              {(d?.violations ?? []).map((v, i) => (
                <tr key={i} style={{ cursor: "default" }}>
                  <td>
                    <span className="badge dq">{v.violation_type}</span>
                  </td>
                  <td className="mono">{v.order_id}</td>
                  <td className="mono muted">{v.session_id}</td>
                  <td className="muted">{v.detail}</td>
                </tr>
              ))}
            </tbody>
          </table>
        ) : (
          <div className="empty">No violations — clean run.</div>
        )}
      </div>
    </div>
  );
}
