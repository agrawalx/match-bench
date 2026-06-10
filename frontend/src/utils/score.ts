/**
 * This file defines frontend behavior for score.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
/**
 * rankTone performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function rankTone(rank: number): "first" | "podium" | "top" | "rest" {
  if (rank === 1) return "first";
  if (rank <= 3) return "podium";
  if (rank <= 10) return "top";
  return "rest";
}
