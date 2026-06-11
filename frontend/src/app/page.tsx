/**
 * This file defines frontend behavior for page.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { redirect } from "next/navigation";

export default function HomePage() {
  redirect("/leaderboard");
}
