/**
 * This file defines frontend behavior for page.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { SubmitClient } from "@/components/submit/SubmitClient";

export const metadata = { title: "Submit - IICPC" };

export default function SubmitPage() {
  return <SubmitClient />;
}
