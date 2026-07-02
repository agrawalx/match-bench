/**
 * This file defines frontend behavior for Shell.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { SideNav } from "./SideNav";
import { TopBar } from "./TopBar";
import styles from "./Shell.module.css";

/**
 * Shell lays out the persistent app chrome: a fixed sidebar for primary
 * navigation and a top bar over the scrolling content column. No ambient
 * animation — the chrome stays out of the way of the measurement data.
 */
export function Shell({ children }: { children: React.ReactNode }) {
  return (
    <div className={styles.layout}>
      <SideNav />
      <div className={styles.main}>
        <TopBar />
        <main className={styles.content}>{children}</main>
      </div>
    </div>
  );
}
