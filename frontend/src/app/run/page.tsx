/**
 * This file defines frontend behavior for page.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { RunClient } from "@/components/run/RunClient";

export const metadata = { title: "My Run - IICPC" };

export default function RunPage({
  searchParams,
}: {
  searchParams?: { run_group_id?: string };
}) {
  return <RunClient initialRunGroupId={searchParams?.run_group_id} />;
}
