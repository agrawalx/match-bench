/**
 * This file defines frontend behavior for page.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { Suspense } from "react";
import { LeaderboardClient } from "@/components/leaderboard/LeaderboardClient";

export const metadata = { title: "Leaderboard - IICPC" };

/**
 * LeaderboardPage renders the live standings view inside a suspense boundary.
 * It lets the client component own query params, streaming updates, and table
 * interactions.
 */
export default function LeaderboardPage() {
  return (
    <Suspense fallback={null}>
      <LeaderboardClient />
    </Suspense>
  );
}
