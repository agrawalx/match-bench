/**
 * This file defines frontend behavior for page.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { Suspense } from "react";
import { LeaderboardClient } from "@/components/leaderboard/LeaderboardClient";

export const metadata = { title: "Leaderboard - IICPC" };

export default function LeaderboardPage() {
  return (
    <Suspense fallback={null}>
      <LeaderboardClient />
    </Suspense>
  );
}
