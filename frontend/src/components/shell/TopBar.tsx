/**
 * This file defines frontend behavior for TopBar.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { usePathname } from "next/navigation";
import styles from "./TopBar.module.css";

const TITLES: Record<string, string> = {
  "/": "Overview",
  "/leaderboard": "Leaderboard",
  "/run": "All runs",
  "/submit": "Submit engine",
};

/**
 * pageTitle maps the current route to a top-bar heading, falling back to the
 * first path segment for nested routes.
 */
function pageTitle(pathname: string): string {
  if (TITLES[pathname]) return TITLES[pathname];
  const seg = pathname.split("/").filter(Boolean)[0];
  if (seg && TITLES[`/${seg}`]) return TITLES[`/${seg}`];
  return "match-bench";
}

/**
 * TopBar shows the current page title. Auth is disabled, so there is no account
 * control. It keeps inputs, side effects, and returned values within this
 * module's contract.
 */
export function TopBar() {
  const pathname = usePathname();
  return (
    <header className={styles.bar}>
      <span className={styles.title}>{pageTitle(pathname)}</span>
      <span className={styles.brandTag}>match-bench</span>
    </header>
  );
}
