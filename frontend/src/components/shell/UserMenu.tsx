/**
 * This file defines frontend behavior for UserMenu.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useState } from "react";
import { useAuth } from "@/auth/useAuth";
import styles from "./UserMenu.module.css";

/**
 * UserMenu performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function UserMenu({ children }: { children: React.ReactNode }) {
  const { user, signOut } = useAuth();
  const [open, setOpen] = useState(false);

  return (
    <div className={styles.wrap}>
      <button
        className={styles.trigger}
        type="button"
        onClick={() => setOpen((value) => !value)}
      >
        {children}
        <span className={styles.chevron}>▾</span>
      </button>
      {open && user && (
        <div className={styles.menu}>
          <strong>{user.displayName}</strong>
          <span>{user.email}</span>
          <button type="button" onClick={() => void signOut()}>
            Sign out
          </button>
        </div>
      )}
    </div>
  );
}
