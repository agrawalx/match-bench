import { useState } from "react";
import Leaderboard from "./components/Leaderboard";
import RunDetail from "./components/RunDetail";
import LiveRuns from "./components/LiveRuns";
import Submit from "./components/Submit";

type View = "submit" | "leaderboard" | "live";

const TABS: { id: View; label: string }[] = [
  { id: "submit", label: "Submit" },
  { id: "leaderboard", label: "Leaderboard" },
  { id: "live", label: "Live Tests" },
];

export default function App() {
  const [view, setView] = useState<View>("submit");
  const [runGroup, setRunGroup] = useState<string | null>(null);

  return (
    <div className="shell">
      <header className="top">
        <div>
          <div className="title">IICPC — HFT Benchmark Platform</div>
          <div className="sub">
            Submit your engine, run it against the load scenarios, watch it live
          </div>
        </div>
        <span className="mode-pill live" data-testid="mode-pill">
          live
        </span>
      </header>

      {runGroup === null && (
        <nav className="tabs">
          {TABS.map((t) => (
            <button
              key={t.id}
              className={view === t.id ? "active" : ""}
              onClick={() => setView(t.id)}
              data-testid={`tab-${t.id}`}
            >
              {t.label}
            </button>
          ))}
        </nav>
      )}

      {runGroup !== null ? (
        <RunDetail runGroupID={runGroup} onBack={() => setRunGroup(null)} />
      ) : view === "submit" ? (
        <Submit onViewRun={(id) => setRunGroup(id)} />
      ) : view === "leaderboard" ? (
        <Leaderboard onSelect={(id) => setRunGroup(id)} />
      ) : (
        <LiveRuns />
      )}
    </div>
  );
}
