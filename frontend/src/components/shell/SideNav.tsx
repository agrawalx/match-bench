/**
 * This file defines frontend behavior for SideNav.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { Activity, LayoutGrid, Trophy, Upload } from "lucide-react";
import styles from "./SideNav.module.css";

const links = [
  { href: "/", icon: LayoutGrid, label: "Overview" },
  { href: "/leaderboard", icon: Trophy, label: "Leaderboard" },
  { href: "/run", icon: Activity, label: "All runs" },
  { href: "/submit", icon: Upload, label: "Submit" },
] as const;

/**
 * isActive marks a nav link active for its own route and, for non-root links,
 * any nested path beneath it (e.g. /run?run_group_id=… keeps "My Runs" lit).
 */
function isActive(pathname: string, href: string): boolean {
  if (href === "/") return pathname === "/";
  return pathname === href || pathname.startsWith(`${href}/`);
}

/**
 * SideNav is the persistent left navigation: brand mark, primary links, and a
 * season footer. It keeps inputs, side effects, and returned values within this
 * module's contract.
 */
export function SideNav() {
  const pathname = usePathname();

  return (
    <aside className={styles.nav}>
      <div className={styles.brand}>
        <span className={styles.mark}>M</span>
        <span className={styles.wordmark}>match-bench</span>
      </div>
      <nav className={styles.links} aria-label="Primary">
        {links.map((link) => {
          const active = isActive(pathname, link.href);
          const Icon = link.icon;
          return (
            <Link
              key={link.href}
              className={`${styles.item} ${active ? styles.active : ""}`}
              href={link.href}
              aria-current={active ? "page" : undefined}
            >
              <Icon size={16} strokeWidth={1.8} className={styles.icon} />
              {link.label}
            </Link>
          );
        })}
      </nav>
      <div className={styles.season}>
        <div className={styles.seasonLabel}>SEASON</div>
        <div className={styles.seasonValue}>2026 · Finals</div>
      </div>
    </aside>
  );
}
