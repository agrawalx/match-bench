import { useEffect, useState } from "react";
import type uPlot from "uplot";
import {
  fmtMicros,
  fmtPct,
  fmtTPS,
  getChart,
  getLive,
  nsToMicros,
  type ActiveRun,
  type MetricPoint,
} from "../api";
import Uplot from "./Uplot";

const C = { accent: "#4493f8", green: "#3fb950", amber: "#d29922", red: "#f85149", muted: "#8b98a8" };

function statusClass(s: string): string {
  if (s === "running") return "ok";
  if (s === "completed") return "scored";
  if (s === "failed") return "dq";
  return "diag"; // requested / deploying / waiting_ready / barrier_fired
}

function p99Chart(points: MetricPoint[]) {
  const t0 = points.length ? points[0].time_unix_ns : 0;
  const xs = points.map((p) => (p.time_unix_ns - t0) / 1e9);
  // null (not 0) for missing samples — a log y-axis can't range over
  // non-positive values.
  const lv = (ns: number) => (ns > 0 ? nsToMicros(ns) : null);
  const data: uPlot.AlignedData = [
    xs,
    points.map((p) => lv(p.p99_ns)),
    points.map((p) => lv(p.rt_p99_ns)),
  ];
  const opts: Omit<uPlot.Options, "width" | "height"> = {
    // log y-axis: service_time (~ms) and round-trip (often ~seconds under
    // coordinated omission) differ by orders of magnitude, so a linear axis
    // would hide the service line entirely.
    scales: { x: { time: false }, y: { distr: 3 } },
    axes: [
      { stroke: C.muted, grid: { stroke: "#222c38" }, label: "seconds" },
      { stroke: C.muted, grid: { stroke: "#222c38" }, label: "p99 µs (log)" },
    ],
    series: [
      { label: "t" },
      { label: "p99 service_time (t7-t3, scored)", stroke: C.accent, width: 2 },
      { label: "p99 round-trip (r9-t0, w/ CO delay)", stroke: C.amber, width: 2 },
    ],
  };
  return { data, opts };
}

function SessionLive({ scenario, status, points }: { scenario: string; status: string; points: MetricPoint[] }) {
  const last = points.length ? points[points.length - 1] : null;
  const ch = p99Chart(points);
  return (
    <div className="live-session" data-testid={`live-session-${scenario}`}>
      <div className="live-session-head">
        <span className="scenario-tag">{scenario}</span>
        <span className={`badge ${statusClass(status)}`}>{status}</span>
      </div>
      <div className="live-kpis live-kpis-4">
        <div>
          <div className="label">p99 service</div>
          <div className="val" style={{ color: C.accent }}>{last ? fmtMicros(last.p99_ns) : "—"}</div>
        </div>
        <div>
          <div className="label">p99 round-trip</div>
          <div className="val" style={{ color: C.amber }}>{last ? fmtMicros(last.rt_p99_ns) : "—"}</div>
        </div>
        <div>
          <div className="label">tps</div>
          <div className="val">{last ? fmtTPS(Math.round(last.tps_1s)) : "—"}</div>
        </div>
        <div>
          <div className="label">err</div>
          <div className="val">{last ? fmtPct(last.error_rate) : "—"}</div>
        </div>
      </div>
      {points.length > 1 ? (
        <Uplot data={ch.data} options={ch.opts} height={300} />
      ) : (
        <div className="empty" style={{ padding: 24 }}>waiting for metrics…</div>
      )}
    </div>
  );
}

export default function LiveRuns() {
  const [runs, setRuns] = useState<ActiveRun[]>([]);
  const [charts, setCharts] = useState<Record<string, MetricPoint[]>>({});
  const [loaded, setLoaded] = useState(false);

  useEffect(() => {
    let alive = true;
    async function tick() {
      try {
        const live = await getLive();
        if (!alive) return;
        setRuns(live.runs);
        setLoaded(true);
        const sessions = live.runs.flatMap((r) => r.sessions.map((s) => s.session_id));
        const entries = await Promise.all(
          sessions.map((sid) =>
            getChart(sid)
              .then((c) => [sid, c.points ?? []] as const)
              .catch(() => [sid, [] as MetricPoint[]] as const),
          ),
        );
        if (!alive) return;
        setCharts(Object.fromEntries(entries));
      } catch {
        /* transient — keep last good state */
      }
    }
    tick();
    const t = setInterval(tick, 2000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, []);

  return (
    <div data-testid="live-runs">
      <div className="card">
        <h2>Tests in progress</h2>
        <div className="hint">
          Live per-test latency while the benchmark runs — refreshes every 2s. Each card is one
          contestant; the p99 service-time (the scored metric) updates per second, grouped by test
          (constant / spike / ramp).
        </div>
      </div>

      {!loaded ? (
        <div className="card empty">Loading…</div>
      ) : runs.length === 0 ? (
        <div className="card empty">No tests running right now. Trigger a benchmark to see it live here.</div>
      ) : (
        runs.map((r) => (
          <div className="card" key={r.run_group_id} data-testid={`live-run-${r.run_group_id}`}>
            <h2>
              {r.team_name || "—"}{" "}
              <span className="muted mono" style={{ fontSize: 12 }}>· {r.run_group_id}</span>
            </h2>
            <div className="live-grid">
              {r.sessions.map((s) => (
                <SessionLive
                  key={s.session_id}
                  scenario={s.scenario}
                  status={s.status}
                  points={charts[s.session_id] ?? []}
                />
              ))}
            </div>
          </div>
        ))
      )}
    </div>
  );
}
