'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { useState } from 'react';
import { useAuth } from '@/auth/useAuth';
import styles from './SideNav.module.css';

const links = [
  { href: '/leaderboard', icon: '#', label: 'Leaderboard' },
  { href: '/submit', icon: '^', label: 'Submit' },
  { href: '/run', icon: '[]', label: 'My Run' },
] as const;

export function SideNav() {
  const pathname = usePathname();
  const { status } = useAuth();
  const [tip, setTip] = useState(false);
  const authenticated = status === 'authenticated';

  return (
    <aside className={styles.nav}>
      {links.map((link) => {
        const locked = link.href === '/submit' && !authenticated;
        const active = pathname === link.href;
        if (locked) {
          return (
            <button
              key={link.href}
              className={`${styles.item} ${active ? styles.active : ''}`}
              type="button"
              onClick={() => {
                setTip(true);
                setTimeout(() => setTip(false), 2000);
              }}
            >
              <span className={styles.icon}>{link.icon}</span>
              <span className={styles.label}>{link.label}</span>
              <span className={styles.lock}>LOCK</span>
              {tip && <span className={styles.tip}>Sign in to submit</span>}
            </button>
          );
        }
        return (
          <Link key={link.href} className={`${styles.item} ${active ? styles.active : ''}`} href={link.href}>
            <span className={styles.icon}>{link.icon}</span>
            <span className={styles.label}>{link.label}</span>
          </Link>
        );
      })}
    </aside>
  );
}
