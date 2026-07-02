/**
 * This file defines frontend behavior for Tabs.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import styles from "./Tabs.module.css";

export interface TabItem {
  key: string;
  label: string;
}

/**
 * Tabs is a segmented control used for scenario switching (run detail) and
 * category switching (leaderboard). It keeps inputs, side effects, and returned
 * values within this module's contract.
 */
export function Tabs({
  tabs,
  active,
  onChange,
  ariaLabel,
}: {
  tabs: TabItem[];
  active: string;
  onChange: (key: string) => void;
  ariaLabel?: string;
}) {
  return (
    <div className={styles.tabs} role="tablist" aria-label={ariaLabel}>
      {tabs.map((tab) => (
        <button
          key={tab.key}
          type="button"
          role="tab"
          aria-selected={active === tab.key}
          className={`${styles.tab} ${active === tab.key ? styles.active : ""}`}
          onClick={() => onChange(tab.key)}
        >
          {tab.label}
        </button>
      ))}
    </div>
  );
}
