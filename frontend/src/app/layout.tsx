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
import { AuthProvider } from "@/auth/AuthProvider";
import { QueryProvider } from "@/providers/QueryProvider";
import { Shell } from "@/components/shell/Shell";

export const metadata: Metadata = {
  title: "IICPC - Algorithm Benchmarking",
  description:
    "High-frequency trading algorithm benchmarking competition platform.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body>
        <QueryProvider>
          <AuthProvider>
            <Shell>{children}</Shell>
          </AuthProvider>
        </QueryProvider>
      </body>
    </html>
  );
}
