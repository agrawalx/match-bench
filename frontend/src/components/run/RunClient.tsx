/**
 * This file defines frontend behavior for RunClient.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useEffect, useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import {
  ArrowLeft,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Clock3,
  History,
  LoaderCircle,
  Search,
  XCircle,
} from "lucide-react";
import { getRunGroups, getRunGroupStatus } from "@/api/submission";
import { platformConfig } from "@/config/platform";
import { ErrorBanner } from "@/components/common/ErrorBanner";
import { Tabs } from "@/components/common/Tabs";
import { useRunDetail } from "@/hooks/useRunDetail";
import { formatLatencyUs } from "@/utils/format";
import type { HdrScenario, HdrSeries } from "@/utils/hdr";
import { HdrPercentileChart } from "./HdrPercentileChart";
import { LatencyTimeline } from "./LatencyTimeline";
import { RunHeader } from "./RunHeader";
import { SessionCards } from "./SessionCards";
import { ThroughputChart } from "./ThroughputChart";
import { ViolationSummary } from "./ViolationSummary";
import styles from "./RunClient.module.css";

/**
 * RunClient renders the "My Runs" history list and the per-run detail view. It
 * keeps inputs, side effects, and returned values within this module's contract.
 */
export function RunClient({
  initialRunGroupId,
}: {
  initialRunGroupId?: string;
}) {
  const router = useRouter();
  const [search, setSearch] = useState("");
  const [selectedSession, setSelectedSession] = useState<string | null>(null);
  const [distOpen, setDistOpen] = useState(true);

  const detailMode = Boolean(initialRunGroupId);

  const historyQuery = useQuery({
    queryKey: ["all-run-history", search],
    enabled: !detailMode,
    queryFn: () =>
      getRunGroups({
        search,
        limit: platformConfig.leaderboardLimit,
      }),
    refetchInterval: (query) =>
      query.state.data?.run_groups.some(
        (group) => group.status === "requested" || group.status === "running",
      )
        ? 2500
        : false,
  });

  const history = useMemo(
    () => historyQuery.data?.run_groups ?? [],
    [historyQuery.data?.run_groups],
  );

  const { data, hdrByScenario, matchByScenario, isLoading, error } =
    useRunDetail(detailMode ? (initialRunGroupId ?? null) : null);

  const runGroupQuery = useQuery({
    queryKey: ["run-group-status", initialRunGroupId],
    enabled: detailMode,
    queryFn: () => getRunGroupStatus(initialRunGroupId ?? ""),
    refetchInterval: (query) => {
      const phase = query.state.data?.status;
      return phase === "requested" || phase === "running" ? 2500 : false;
    },
  });

  // Default the scenario tab to the first session once the run loads.
  useEffect(() => {
    if (data?.sessions?.length && selectedSession === null) {
      setSelectedSession(data.sessions[0].session_id);
    }
  }, [data, selectedSession]);

  const showRunGroupState =
    Boolean(runGroupQuery.data) &&
    (runGroupQuery.data?.status !== "completed" ||
      runGroupQuery.data.runs.some((run) => run.status !== "completed"));

  if (detailMode) {
    const sessions = data?.sessions ?? [];
    const activeId =
      sessions.find((s) => s.session_id === selectedSession)?.session_id ??
      sessions[0]?.session_id ??
      "";
    const activeSession = sessions.find((s) => s.session_id === activeId);
    const hdr = hdrByScenario?.find((h) => h.sessionId === activeId);
    const match = matchByScenario?.find((h) => h.sessionId === activeId);

    return (
      <section className={styles.page}>
        <div className={styles.heading}>
          <button
            type="button"
            className={styles.backLink}
            onClick={() => router.push("/run")}
          >
            <ArrowLeft size={14} strokeWidth={2} />
            <span>Back to all runs</span>
          </button>
        </div>
        {runGroupQuery.data && showRunGroupState && (
          <PendingRunCard runGroup={runGroupQuery.data} />
        )}
        {runGroupQuery.isLoading && !data && (
          <p className={styles.mono}>Loading benchmark run…</p>
        )}
        {runGroupQuery.error && !data && (
          <ErrorBanner
            message={
              runGroupQuery.error instanceof Error
                ? runGroupQuery.error.message
                : "Run status failed to load"
            }
          />
        )}
        {isLoading && !data && (
          <p className={styles.mono}>Loading run telemetry…</p>
        )}
        {error && !runGroupQuery.data && (
          <ErrorBanner
            message={error instanceof Error ? error.message : "Run failed to load"}
          />
        )}
        {data && (
          <>
            <RunHeader run={data} />
            {sessions.length > 0 && (
              <>
                <Tabs
                  ariaLabel="Scenario"
                  tabs={sessions.map((s) => ({
                    key: s.session_id,
                    label: s.scenario || "scenario",
                  }))}
                  active={activeId}
                  onChange={setSelectedSession}
                />
                {activeSession && (
                  <>
                    <div className={styles.sectionLabel}>TIMELINES · PRIMARY</div>
                    <div className={styles.timelines}>
                      <LatencyTimeline
                        points={activeSession.timeline}
                        metric="service"
                      />
                      <LatencyTimeline
                        points={activeSession.timeline}
                        metric="response"
                      />
                      <ThroughputChart points={activeSession.timeline} />
                    </div>

                    <Distributions
                      hdr={hdr}
                      match={match}
                      open={distOpen}
                      onToggle={() => setDistOpen((v) => !v)}
                    />

                    <div className={styles.sectionLabel}>SCENARIO SUMMARY</div>
                    <SessionCards sessions={sessions} />

                    <ViolationSummary
                      counts={data.violation_counts}
                      sessionId={activeSession.session_id}
                    />
                  </>
                )}
              </>
            )}
          </>
        )}
      </section>
    );
  }

  return (
    <section className={styles.page}>
      <div className={styles.heading}>
        <h1>All runs</h1>
        <p>Every benchmark run across all contestants — search by contestant or team.</p>
      </div>
      <div className={styles.searchBar}>
        <Search size={15} strokeWidth={2} className={styles.searchIcon} />
        <input
          className={styles.searchInput}
          type="search"
          placeholder="Search contestant or team…"
          value={search}
          onChange={(event) => setSearch(event.target.value)}
        />
      </div>
      {historyQuery.error ? (
        <ErrorBanner message="Run history is unavailable because submission-api is not reachable." />
      ) : historyQuery.isLoading ? (
        <p className={styles.mono}>Loading runs…</p>
      ) : history.length === 0 ? (
        <div className={styles.empty}>
          <span className={styles.emptyIcon}>
            <History size={28} strokeWidth={1.5} />
          </span>
          <strong>
            {search ? "No runs match your search" : "No benchmark runs yet"}
          </strong>
          <p>
            {search
              ? "Try a different contestant or team name."
              : "Submit an algorithm and start a benchmark run to populate this list."}
          </p>
        </div>
      ) : (
        <div className={styles.tableCard}>
          <div className={`${styles.tableRow} ${styles.tableHead}`}>
            <span>RUN GROUP</span>
            <span>CONTESTANT</span>
            <span>STATUS</span>
            <span className={styles.num}>SCENARIOS</span>
            <span className={styles.num}>CREATED</span>
          </div>
          {history.map((row) => (
            <button
              key={row.run_group_id}
              type="button"
              className={styles.tableRow}
              onClick={() =>
                router.push(
                  `/run?run_group_id=${encodeURIComponent(row.run_group_id)}`,
                )
              }
            >
              <span className={styles.runId}>{row.run_group_id}</span>
              <span className={styles.contestant}>
                {row.team_name || row.contestant_id || "—"}
              </span>
              <span>
                <span
                  className={`${styles.statusBadge} ${styles[`status_${row.status}`] ?? ""}`}
                >
                  {row.status}
                </span>
              </span>
              <span className={styles.num}>{row.runs.length}</span>
              <span className={`${styles.num} ${styles.timestamp}`}>
                {formatTimestamp(row.created_at)}
              </span>
            </button>
          ))}
        </div>
      )}
    </section>
  );
}

/**
 * keyPercentiles pulls a few headline percentile values out of the service HDR
 * series for the Distributions summary column.
 */
function keyPercentiles(series?: HdrSeries): { k: string; v: string }[] {
  if (!series) return [];
  const at = (pct: number): string | null => {
    const point = series.points.find((p) => p.percentile === pct);
    return point ? formatLatencyUs(point.value_us) : null;
  };
  return [
    { k: "p50", v: at(50) ?? formatLatencyUs(series.p50_us) },
    { k: "p90", v: at(90) ?? "—" },
    { k: "p99", v: at(99) ?? formatLatencyUs(series.p99_us) },
    { k: "p99.9", v: at(99.9) ?? "—" },
    { k: "p99.99", v: at(99.99) ?? "—" },
  ];
}

/**
 * Distributions renders the secondary, collapsible HDR block: service/response
 * percentile curves, match-latency curve, and a key-percentile readout.
 */
function Distributions({
  hdr,
  match,
  open,
  onToggle,
}: {
  hdr?: HdrScenario;
  match?: HdrScenario;
  open: boolean;
  onToggle: () => void;
}) {
  const svc = hdr?.series ?? [];
  const matchSeries = match?.series ?? [];
  const pctRows = keyPercentiles(svc[0]);

  return (
    <section className={styles.dist}>
      <button
        type="button"
        className={styles.distHead}
        onClick={onToggle}
        aria-expanded={open}
      >
        {open ? (
          <ChevronDown size={15} strokeWidth={2} className={styles.distChevron} />
        ) : (
          <ChevronRight size={15} strokeWidth={2} className={styles.distChevron} />
        )}
        <span className={styles.distTitle}>Distributions</span>
        <span className={styles.distCaption}>
          HDR percentiles · match latency — secondary
        </span>
      </button>
      {open && (
        <div className={styles.distGrid}>
          <HdrPercentileChart
            series={svc}
            title="Service / response HDR"
            caption="latency by percentile"
            height={200}
          />
          <HdrPercentileChart
            series={matchSeries}
            title="Match latency HDR"
            caption="taker fills, by percentile"
            height={200}
          />
          <div className={styles.pctCard}>
            <div className={styles.pctTitle}>KEY PERCENTILES</div>
            {pctRows.length === 0 ? (
              <div className={styles.pctEmpty}>No HDR data for this scenario.</div>
            ) : (
              pctRows.map((r) => (
                <div key={r.k} className={styles.pctRow}>
                  <span className={styles.pctKey}>{r.k}</span>
                  <span className={styles.pctVal}>{r.v}</span>
                </div>
              ))
            )}
          </div>
        </div>
      )}
    </section>
  );
}

/**
 * PendingRunCard performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function PendingRunCard({
  runGroup,
}: {
  runGroup: Awaited<ReturnType<typeof getRunGroupStatus>>;
}) {
  const terminal =
    runGroup.status === "completed" || runGroup.status === "failed";
  const running = runGroup.status === "running";
  const requested = runGroup.status === "requested";
  const requestedAgeMs = requested
    ? Date.now() - Date.parse(runGroup.created_at)
    : 0;
  const stalled = requestedAgeMs > 20_000;
  const Icon =
    runGroup.status === "completed"
      ? CheckCircle2
      : runGroup.status === "failed"
        ? XCircle
        : running
          ? LoaderCircle
          : Clock3;

  return (
    <section className={styles.pendingRun}>
      <div className={styles.pendingHeader}>
        <span className={running ? styles.statusIconActive : styles.statusIcon}>
          <Icon size={20} strokeWidth={1.8} />
        </span>
        <div>
          <strong>{runGroup.status}</strong>
          <span>
            {requested
              ? "Waiting for a benchmark runner to claim this run"
              : `Run group ${runGroup.run_group_id}`}
          </span>
        </div>
      </div>
      <div className={styles.stageTrack} aria-label="Benchmark run progress">
        {["requested", "running", terminal ? runGroup.status : "completed"].map(
          (stage, index) => {
            const active =
              runGroup.status === stage ||
              (stage === "completed" && runGroup.status === "failed");
            const passed =
              (stage === "requested" &&
                ["running", "completed", "failed"].includes(runGroup.status)) ||
              (stage === "running" &&
                ["completed", "failed"].includes(runGroup.status));
            return (
              <span
                key={`${stage}-${index}`}
                className={`${styles.stage} ${active ? styles.stageActive : ""} ${passed ? styles.stagePassed : ""}`}
              >
                {stage}
              </span>
            );
          },
        )}
      </div>
      {stalled && (
        <p className={styles.stalledNotice}>
          No runner has consumed this benchmark request yet. This usually means a
          host-vs-cluster split brain: submission-api wrote the run to one
          Kafka/Postgres stack while bot-fleet-controller is listening to another.
        </p>
      )}
      <div className={styles.sessionList}>
        {runGroup.runs.map((run) => (
          <div key={run.session_id} className={styles.sessionRow}>
            <span>{run.scenario_name || run.scenario_id}</span>
            <strong>{run.status}</strong>
            <small>{run.session_id}</small>
            {run.message && <em>{run.message}</em>}
          </div>
        ))}
      </div>
    </section>
  );
}

/**
 * formatTimestamp performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function formatTimestamp(iso: string): string {
  try {
    const d = new Date(iso);
    return d.toLocaleString("en-GB", {
      day: "2-digit",
      month: "short",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      hour12: false,
    });
  } catch {
    return iso;
  }
}
