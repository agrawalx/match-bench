import { useState } from "react";
import Leaderboard from "./components/Leaderboard";
import RunDetail from "./components/RunDetail";
import HealthPanel from "./components/HealthPanel";
import LiveRuns from "./components/LiveRuns";

type View = "leaderboard" | "live" | "health";

export default function App() {
  const [view, setView] = useState<View>("leaderboard");
  const [runGroup, setRunGroup] = useState<string | null>(null);

  return (
    <div className="shell">
      <header className="top">
        <div>
          <div className="title">IICPC — HFT Benchmark Leaderboard</div>
          <div className="sub">
            Ranked by peak sustained TPS, gated on latency and correctness
          </div>
        </div>
        <span className="mode-pill live" data-testid="mode-pill">
          live
        </span>
      </header>

      {runGroup === null && (
        <nav className="tabs">
          <button
            className={view === "leaderboard" ? "active" : ""}
            onClick={() => setView("leaderboard")}
            data-testid="tab-leaderboard"
          >
            Leaderboard
          </button>
          <button
            className={view === "live" ? "active" : ""}
            onClick={() => setView("live")}
            data-testid="tab-live"
          >
            Live Tests
          </button>
          <button
            className={view === "health" ? "active" : ""}
            onClick={() => setView("health")}
            data-testid="tab-health"
          >
            Platform Health
          </button>
        </nav>
      )}

      {runGroup !== null ? (
        <RunDetail runGroupID={runGroup} onBack={() => setRunGroup(null)} />
      ) : view === "leaderboard" ? (
        <Leaderboard onSelect={(id) => setRunGroup(id)} />
      ) : view === "live" ? (
        <LiveRuns />
      ) : (
        <HealthPanel />
      )}
    </div>
  );
}
