/**
 * This file defines frontend behavior for submission.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
/**
 * BackendSubmissionPhase describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export type BackendSubmissionPhase =
  | "uploaded"
  | "building"
  | "scanned"
  | "sbom_ready"
  | "ready"
  | "failed";

/**
 * SubmissionPhase describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export type SubmissionPhase =
  | "queued"
  | "building"
  | "scanning"
  | "promoting"
  | "ready"
  | "failed";

/**
 * SubmissionStatus describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface SubmissionStatus {
  submission_id: string;
  contestant_id?: string;
  status: SubmissionPhase;
  created_at: string;
  updated_at?: string;
  build_logs?: string;
}

/**
 * BackendSubmissionStatus describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export interface BackendSubmissionStatus extends Omit<
  SubmissionStatus,
  "status"
> {
  status: BackendSubmissionPhase;
}
