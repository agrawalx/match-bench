/**
 * This file defines frontend behavior for page.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { redirect } from "next/navigation";

/**
 * HomePage redirects the root route into the primary leaderboard experience.
 * It keeps the app entry point stable while letting the leaderboard own the
 * first interactive screen.
 */
export default function HomePage() {
  redirect("/leaderboard");
}
