/**
 * This file defines frontend behavior for Shell.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { PacketFlowCanvas } from "@/components/canvas/PacketFlowCanvas";
import { TopBar } from "./TopBar";
import styles from "./Shell.module.css";

/**
 * Shell performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function Shell({ children }: { children: React.ReactNode }) {
  return (
    <>
      <PacketFlowCanvas aria-hidden="true" />
      <span className="sr-only">
        Ambient animation showing data flowing through the IICPC benchmark
        pipeline
      </span>
      <div className={styles.layout}>
        <TopBar />
        <div className={styles.body}>
          <main className={styles.content}>{children}</main>
        </div>
      </div>
    </>
  );
}
