/**
 * This file defines frontend behavior for LeaderboardClient.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useMemo, useState } from "react";
import { motion } from "framer-motion";
import { useSearchParams } from "next/navigation";
import { ErrorBanner } from "@/components/common/ErrorBanner";
import { useAuth } from "@/auth/useAuth";
import { platformConfig } from "@/config/platform";
import { useLeaderboard } from "@/hooks/useLeaderboard";
import type { LeaderboardEntry } from "@/types/leaderboard";
import { LeaderboardControls } from "./LeaderboardControls";
import { ScoringRules } from "./ScoringRules";
import { LeaderboardTable } from "./LeaderboardTable";
import styles from "./LeaderboardClient.module.css";

/**
 * SortBy describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export type SortBy =
  | "rank"
  | "peak_sustained_tps"
  | "p99_ns_at_peak_tps"
  | "spike_recovery_ns"
  | "total_correctness";
/**
 * SortOrder describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export type SortOrder = "asc" | "desc";

/**
 * LeaderboardClient performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function LeaderboardClient() {
  const [sortBy, setSortBy] = useState<SortBy>("rank");
  const [sortOrder, setSortOrder] = useState<SortOrder>("asc");
  const [page, setPage] = useState(1);
  const { user } = useAuth();
  const searchParams = useSearchParams();
  const { data, isLoading, error, sseStatus, flashedRows } = useLeaderboard();
  const authError = searchParams.get("auth_error");

  const rows = useMemo(() => {
    const sorted = [...(data?.rows ?? [])].sort((a, b) => {
      const left = a[sortBy] as number;
      const right = b[sortBy] as number;
      return sortOrder === "asc" ? left - right : right - left;
    });
    return sorted.slice(
      (page - 1) * platformConfig.pageSize,
      page * platformConfig.pageSize,
    );
  }, [data?.rows, page, sortBy, sortOrder]);

  const totalPages = Math.max(
    1,
    Math.ceil((data?.rows.length ?? rows.length) / platformConfig.pageSize),
  );

  return (
    <motion.section
      className={styles.page}
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.22, ease: "easeOut" }}
    >
      <div className={styles.heading}>
        <h1>LEADERBOARD</h1>
        <p>Ranked benchmark results across active IICPC sessions.</p>
      </div>
      <ScoringRules />
      {error && (
        <ErrorBanner message="Leaderboard API is unavailable. Start or connect the platform leaderboard service to load live standings." />
      )}
      {authError && (
        <ErrorBanner
          message={`Google sign-in failed: ${authError}. Check auth-api and OAuth environment configuration.`}
        />
      )}
      <LeaderboardControls
        sortBy={sortBy}
        sortOrder={sortOrder}
        sessionId={platformConfig.sessionId}
        live={sseStatus === "open"}
        onSortBy={setSortBy}
        onSortOrder={() =>
          setSortOrder((value) => (value === "asc" ? "desc" : "asc"))
        }
      />
      <LeaderboardTable
        rows={rows as LeaderboardEntry[]}
        loading={isLoading && !error}
        ownContestantId={user?.contestantId}
        flashedRows={flashedRows}
        sortBy={sortBy}
        sortOrder={sortOrder}
      />
      <div className={styles.pagination}>
        <button
          type="button"
          disabled={page === 1}
          onClick={() => setPage((value) => Math.max(1, value - 1))}
        >
          Prev
        </button>
        <span>
          Page {page} of {totalPages}
        </span>
        <button
          type="button"
          disabled={page === totalPages}
          onClick={() => setPage((value) => Math.min(totalPages, value + 1))}
        >
          Next
        </button>
      </div>
    </motion.section>
  );
}
