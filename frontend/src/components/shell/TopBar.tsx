/**
 * This file defines frontend behavior for TopBar.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { Gauge, Send, Trophy } from "lucide-react";
import { GoogleButton } from "@/auth/GoogleButton";
import { useAuth } from "@/auth/useAuth";
import { UserMenu } from "./UserMenu";
import styles from "./TopBar.module.css";

/**
 * TopBar performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function TopBar() {
  const { status, user } = useAuth();
  const pathname = usePathname();
  const authenticated = status === "authenticated" && user;

  return (
    <header className={styles.bar}>
      <div className={styles.left}>
        <span className={styles.logo}>IICPC</span>
        <span className={styles.separator}>|</span>
        <span className={styles.session}>
          <span className={styles.liveDot} />
          SESSION&nbsp;GLOBAL
        </span>
      </div>
      <nav className={styles.tabs} aria-label="Primary">
        {[
          { href: "/leaderboard", label: "Leaderboard", icon: Trophy },
          { href: "/submit", label: "Submit", icon: Send },
          { href: "/run", label: "My Run", icon: Gauge },
        ].map((item) => (
          <Link
            key={item.href}
            className={`${styles.tab} ${pathname === item.href ? styles.activeTab : ""}`}
            href={item.href}
          >
            <item.icon size={14} strokeWidth={1.8} aria-hidden="true" />
            {item.label}
          </Link>
        ))}
      </nav>
      <div className={styles.right}>
        {authenticated ? (
          <UserMenu>
            {user.avatarUrl ? (
              <Image
                className={styles.avatarImage}
                src={user.avatarUrl}
                alt=""
                width={28}
                height={28}
              />
            ) : (
              <span className={styles.avatar}>
                {initials(user.displayName)}
              </span>
            )}
            <span className={styles.name}>{user.displayName}</span>
          </UserMenu>
        ) : (
          <GoogleButton />
        )}
      </div>
    </header>
  );
}

/**
 * initials performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function initials(name: string): string {
  return name
    .split(/\s+/)
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0]?.toUpperCase())
    .join("");
}
