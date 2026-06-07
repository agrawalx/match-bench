export function rankTone(rank: number): 'first' | 'podium' | 'top' | 'rest' {
  if (rank === 1) return 'first';
  if (rank <= 3) return 'podium';
  if (rank <= 10) return 'top';
  return 'rest';
}
