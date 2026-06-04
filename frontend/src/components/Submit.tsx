import { useEffect, useRef, useState } from "react";
import {
  getRunGroupStatus,
  getSubmission,
  submitCode,
  triggerBenchmark,
  type RunGroup,
  type Submission,
} from "../api";

const TERMINAL_SUB = new Set(["ready", "failed"]);
const TERMINAL_RUN = new Set(["completed", "failed"]);

function subBadge(s: string): string {
  if (s === "ready") return "ok";
  if (s === "failed") return "dq";
  return "diag"; // uploaded / building / scanned / sbom_ready
}
function runBadge(s: string): string {
  if (s === "completed") return "scored";
  if (s === "failed") return "dq";
  if (s === "running") return "ok";
  return "diag";
}

const YAML_EXAMPLE = `# benchmark.yaml (at the root of your zip)
team_name: your-team
language: rust          # rust | go | cpp
protocol: FIX           # FIX | REST | WS
port: 9898              # port your server listens on
build:
  type: cargo           # cargo | go | cmake
  target: my-engine     # binary to produce`;

export default function Submit({ onViewRun }: { onViewRun: (runGroupID: string) => void }) {
  const [sub, setSub] = useState<Submission | null>(null);
  const [group, setGroup] = useState<RunGroup | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [trackId, setTrackId] = useState("");
  const [drag, setDrag] = useState(false);
  const fileRef = useRef<HTMLInputElement>(null);

  // Poll submission status until it reaches a terminal state (ready/failed).
  useEffect(() => {
    if (!sub || TERMINAL_SUB.has(sub.status)) return;
    const id = sub.submission_id;
    const t = setInterval(async () => {
      try {
        const s = await getSubmission(id);
        setSub(s);
      } catch {
        /* transient; keep last */
      }
    }, 2000);
    return () => clearInterval(t);
  }, [sub]);

  // Poll the run-group once a benchmark is triggered.
  useEffect(() => {
    if (!group || TERMINAL_RUN.has(group.status)) return;
    const id = group.run_group_id;
    const t = setInterval(async () => {
      try {
        setGroup(await getRunGroupStatus(id));
      } catch {
        /* transient */
      }
    }, 2000);
    return () => clearInterval(t);
  }, [group]);

  async function doUpload(file: File) {
    setErr(null);
    setGroup(null);
    setBusy(true);
    try {
      if (!file.name.endsWith(".zip")) throw new Error("please upload a .zip file");
      setSub(await submitCode(file));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function doTrack() {
    const id = trackId.trim();
    if (!id) return;
    setErr(null);
    setGroup(null);
    setBusy(true);
    try {
      setSub(await getSubmission(id));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  async function doTrigger() {
    if (!sub) return;
    setErr(null);
    setBusy(true);
    try {
      setGroup(await triggerBenchmark(sub.submission_id));
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  }

  function reset() {
    setSub(null);
    setGroup(null);
    setErr(null);
    setTrackId("");
  }

  return (
    <div data-testid="submit-view">
      <div className="card">
        <h2>Submit your engine</h2>
        <div className="hint">
          Upload a <code>.zip</code> containing your source and a{" "}
          <code>benchmark.yaml</code> at the root. The platform builds it in a sandbox,
          then you trigger the benchmark and watch your run live.
        </div>

        {err && (
          <div className="submit-err" data-testid="submit-error">
            {err}
          </div>
        )}

        {!sub ? (
          <>
            <div
              className={`dropzone${drag ? " drag" : ""}`}
              data-testid="dropzone"
              onClick={() => fileRef.current?.click()}
              onDragOver={(e) => {
                e.preventDefault();
                setDrag(true);
              }}
              onDragLeave={() => setDrag(false)}
              onDrop={(e) => {
                e.preventDefault();
                setDrag(false);
                const f = e.dataTransfer.files?.[0];
                if (f) doUpload(f);
              }}
            >
              <input
                ref={fileRef}
                type="file"
                accept=".zip"
                data-testid="file-input"
                style={{ display: "none" }}
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  if (f) doUpload(f);
                }}
              />
              <div className="dz-icon">⤓</div>
              <div>{busy ? "Uploading…" : "Drop your .zip here, or click to browse"}</div>
            </div>

            <div className="track-row">
              <input
                className="track-input"
                placeholder="…or track an existing submission_id"
                data-testid="track-input"
                value={trackId}
                onChange={(e) => setTrackId(e.target.value)}
                onKeyDown={(e) => e.key === "Enter" && doTrack()}
              />
              <button className="back-btn" onClick={doTrack} disabled={busy} data-testid="track-btn">
                Track
              </button>
            </div>

            <details className="yaml-help">
              <summary>benchmark.yaml format</summary>
              <pre>{YAML_EXAMPLE}</pre>
            </details>
          </>
        ) : (
          <div className="sub-card" data-testid="submission-card">
            <div className="sub-head">
              <span className="mono">{sub.submission_id}</span>
              <span className={`badge ${subBadge(sub.status)}`} data-testid="sub-status">
                {sub.status}
              </span>
            </div>
            <div className="sub-meta">
              <span>team: <b>{sub.team_name || "—"}</b></span>
              <span>lang: <b>{sub.language}</b></span>
              <span>protocol: <b>{sub.protocol}</b></span>
              <span>port: <b>{sub.port}</b></span>
            </div>
            {!TERMINAL_SUB.has(sub.status) && (
              <div className="hint" style={{ marginTop: 8 }}>
                Building in the sandbox… status updates automatically.
              </div>
            )}
            {sub.status === "failed" && (
              <div className="hint" style={{ color: "var(--red)", marginTop: 8 }}>
                Build failed — check your benchmark.yaml and source, then re-submit.
              </div>
            )}
            <div className="sub-actions">
              <button
                className="primary-btn"
                onClick={doTrigger}
                disabled={busy || sub.status !== "ready" || !!group}
                data-testid="run-btn"
              >
                {group ? "Run started" : "Run Benchmark"}
              </button>
              <button className="back-btn" onClick={reset} data-testid="reset-btn">
                Submit another
              </button>
            </div>
          </div>
        )}
      </div>

      {group && (
        <div className="card" data-testid="rungroup-card">
          <h2>
            Run started{" "}
            <span className="muted mono" style={{ fontSize: 12 }}>· {group.run_group_id}</span>{" "}
            <span className={`badge ${runBadge(group.status)}`}>{group.status}</span>
          </h2>
          <div className="hint">
            One session per scenario. Watch the percentiles live, or open the full run detail.
          </div>
          <table>
            <thead>
              <tr>
                <th>Scenario</th>
                <th>Session</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {(group.runs ?? []).map((r) => (
                <tr key={r.session_id} style={{ cursor: "default" }}>
                  <td><span className="scenario-tag">{r.scenario_name || "—"}</span></td>
                  <td className="mono muted">{r.session_id}</td>
                  <td>
                    <span className={`badge ${runBadge(r.status)}`}>{r.status}</span>
                    {r.message ? <span className="muted"> · {r.message}</span> : null}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <div className="sub-actions">
            <button
              className="primary-btn"
              onClick={() => onViewRun(group.run_group_id)}
              data-testid="view-run-btn"
            >
              View run detail →
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
