/**
 * This file defines frontend behavior for HomeClient.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useMemo } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { ArrowUpRight, Plus } from "lucide-react";
import { useLeaderboard } from "@/hooks/useLeaderboard";
import { getRunGroups } from "@/api/submission";
import { platformConfig } from "@/config/platform";
import { formatNumber } from "@/utils/format";
import styles from "./HomeClient.module.css";

/**
 * HomeClient is the landing overview: top-ranked teams and the viewer's recent
 * runs, both from data the platform already serves. Season/fleet KPIs are
 * intentionally omitted — there is no API for them. It keeps inputs, side
 * effects, and returned values within this module's contract.
 */
export function HomeClient() {
  const router = useRouter();
  const { data: leaderboard } = useLeaderboard();

  const topTeams = useMemo(
    () =>
      [...(leaderboard?.rows ?? [])]
        .sort((a, b) => a.rank - b.rank)
        .slice(0, 5),
    [leaderboard?.rows],
  );

  const historyQuery = useQuery({
    queryKey: ["home-run-history"],
    queryFn: () =>
      getRunGroups({ limit: platformConfig.leaderboardLimit }),
  });
  const recentRuns = (historyQuery.data?.run_groups ?? []).slice(0, 5);

  return (
    <section className={styles.page}>
      <div className={styles.headRow}>
        <div>
          <h1>Overview</h1>
          <p>Live matching-engine benchmark standings and your recent runs.</p>
        </div>
        <Link href="/submit" className={styles.newRun}>
          <Plus size={14} strokeWidth={2.2} />
          New run
        </Link>
      </div>

      <div className={styles.grid}>
        <div className={styles.card}>
          <header className={styles.cardHead}>
            <span className={styles.cardTitle}>Top teams</span>
            <Link href="/leaderboard" className={styles.viewAll}>
              View all <ArrowUpRight size={13} strokeWidth={2} />
            </Link>
          </header>
          {topTeams.length === 0 ? (
            <div className={styles.empty}>No ranked teams yet.</div>
          ) : (
            topTeams.map((row) => (
              <button
                key={row.contestant_id}
                type="button"
                className={styles.teamRow}
                onClick={() =>
                  row.run_group_id &&
                  router.push(
                    `/run?run_group_id=${encodeURIComponent(row.run_group_id)}`,
                  )
                }
              >
                <span className={styles.rank}>{row.rank}</span>
                <span className={styles.team}>
                  {row.team_name || "Untitled team"}
                </span>
                <span className={styles.metric}>
                  {formatNumber(row.peak_sustained_tps)}
                  <span className={styles.metricUnit}> tps</span>
                </span>
              </button>
            ))
          )}
        </div>

        <div className={styles.card}>
          <header className={styles.cardHead}>
            <span className={styles.cardTitle}>Recent runs</span>
            <Link href="/run" className={styles.viewAll}>
              My runs <ArrowUpRight size={13} strokeWidth={2} />
            </Link>
          </header>
          {recentRuns.length === 0 ? (
            <div className={styles.empty}>No runs yet.</div>
          ) : (
            recentRuns.map((run) => (
              <button
                key={run.run_group_id}
                type="button"
                className={styles.runRow}
                onClick={() =>
                  router.push(
                    `/run?run_group_id=${encodeURIComponent(run.run_group_id)}`,
                  )
                }
              >
                <span
                  className={`${styles.dot} ${styles[`dot_${run.status}`] ?? ""}`}
                  aria-hidden="true"
                />
                <span className={styles.runId}>
                  {run.team_name || run.contestant_id || run.run_group_id}
                </span>
                <span className={styles.runScen}>
                  {run.runs.length} scenario{run.runs.length === 1 ? "" : "s"}
                </span>
                <span className={styles.runWhen}>{relativeTime(run.created_at)}</span>
              </button>
            ))
          )}
        </div>
      </div>
    </section>
  );
}

/**
 * relativeTime renders a compact "3m / 2h / 4d ago" style stamp for a run's
 * created_at, falling back to the raw string if it cannot be parsed.
 */
function relativeTime(iso: string): string {
  const ms = Date.now() - Date.parse(iso);
  if (!Number.isFinite(ms)) return iso;
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 60) return `${s}s ago`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ago`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ago`;
  return `${Math.floor(h / 24)}d ago`;
}
