/**
 * This file defines frontend behavior for page.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useEffect, useRef } from "react";
import { useRouter } from "next/navigation";
import { useAuth } from "@/auth/useAuth";
import styles from "./callback.module.css";

export default function CallbackPage() {
  const { handleCallback } = useAuth();
  const router = useRouter();
  const handledRef = useRef(false);

  useEffect(() => {
    if (handledRef.current) return;
    handledRef.current = true;

    const params = new URLSearchParams(window.location.search);
    const code = params.get("code");
    const state = params.get("state");
    const error = params.get("error");
    const errorDescription = params.get("error_description");

    if (error) {
      router.replace(
        `/leaderboard?auth_error=${encodeURIComponent(errorDescription ?? error)}`,
      );
      return;
    }

    if (!code || !state) {
      router.replace("/leaderboard?auth_error=missing_params");
      return;
    }

    handleCallback(code, state)
      .then(() => {
        const returnTo =
          sessionStorage.getItem("iicpc_return_to") ?? "/leaderboard";
        sessionStorage.removeItem("iicpc_return_to");
        router.replace(returnTo);
      })
      .catch((caught: unknown) => {
        const message =
          caught instanceof Error ? caught.message : "exchange_failed";
        router.replace(
          `/leaderboard?auth_error=${encodeURIComponent(message)}`,
        );
      });
  }, [handleCallback, router]);

  return (
    <div className={styles.container}>
      <div className={styles.spinner} aria-hidden="true" />
      <p className={styles.label}>Authenticating with Google...</p>
    </div>
  );
}
