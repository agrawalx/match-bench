/**
 * This file defines frontend behavior for layout.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import "@fontsource/geist-sans/400.css";
import "@fontsource/geist-sans/500.css";
import "@fontsource/geist-mono/400.css";
import "@fontsource/geist-mono/500.css";
import "./globals.css";
import type { Metadata } from "next";
import { QueryProvider } from "@/providers/QueryProvider";
import { Shell } from "@/components/shell/Shell";

export const metadata: Metadata = {
  title: "IICPC - Algorithm Benchmarking",
  description:
    "High-frequency trading algorithm benchmarking competition platform.",
};

/**
 * RootLayout wires global providers and the application shell around pages.
 * Auth is disabled platform-wide, so only query state and navigation are
 * provided to every route rendered by the frontend app.
 */
export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <QueryProvider>
          <Shell>{children}</Shell>
        </QueryProvider>
      </body>
    </html>
  );
}
