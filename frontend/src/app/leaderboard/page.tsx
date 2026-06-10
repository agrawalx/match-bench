import { Suspense } from 'react';
import { LeaderboardClient } from '@/components/leaderboard/LeaderboardClient';

export const metadata = { title: 'Leaderboard - IICPC' };

export default function LeaderboardPage() {
  return (
    <Suspense fallback={null}>
      <LeaderboardClient />
    </Suspense>
  );
}
