/**
 * This file defines frontend behavior for page.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { HomeClient } from "@/components/home/HomeClient";

/**
 * HomePage renders the overview landing: top teams and recent runs.
 * It keeps the app entry point stable while surfacing the most useful
 * at-a-glance state from data the platform already serves.
 */
export default function HomePage() {
  return <HomeClient />;
}
