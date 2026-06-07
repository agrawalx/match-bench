'use client';

import { PacketFlowCanvas } from '@/components/canvas/PacketFlowCanvas';
import { TopBar } from './TopBar';
import styles from './Shell.module.css';

export function Shell({ children }: { children: React.ReactNode }) {
  return (
    <>
      <PacketFlowCanvas aria-hidden="true" />
      <span className="sr-only">Ambient animation showing data flowing through the IICPC benchmark pipeline</span>
      <div className={styles.layout}>
        <TopBar />
        <div className={styles.body}>
          <main className={styles.content}>{children}</main>
        </div>
      </div>
    </>
  );
}
