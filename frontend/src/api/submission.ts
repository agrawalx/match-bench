/**
 * This file defines frontend behavior for submission.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { apiFetch } from "./client";
import { platformConfig } from "@/config/platform";
import type { RunGroupHistoryResponse, RunGroupStatus } from "@/types/run";
import type {
  BackendSubmissionPhase,
  BackendSubmissionStatus,
  SubmissionPhase,
  SubmissionStatus,
} from "@/types/submission";

/**
 * uploadSubmission performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function uploadSubmission(
  file: File,
  token: string,
  onProgress: (pct: number) => void,
): Promise<{ submission_id: string; reused?: boolean }> {
  return new Promise((resolve, reject) => {
    const form = new FormData();
    form.append("file", file);

    const xhr = new XMLHttpRequest();
    xhr.open("POST", platformConfig.endpoints.submission.create);
    xhr.setRequestHeader("Authorization", `Bearer ${token}`);
    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable)
        onProgress(Math.round((event.loaded / event.total) * 100));
    };
    xhr.onerror = () => reject(new Error("upload_failed"));
    xhr.onload = () => {
      if (xhr.status === 409) {
        const body = parseUploadError(xhr.responseText);
        if (body.submission_id) {
          resolve({ submission_id: body.submission_id, reused: true });
          return;
        }
      }
      if (xhr.status === 413) {
        reject(
          new Error(
            "Submission is too large: the platform accepts uploads up to 100 MB.",
          ),
        );
        return;
      }
      if (xhr.status < 200 || xhr.status >= 300) {
        const body = parseUploadError(xhr.responseText);
        reject(new Error(body.error ?? body.message ?? "Upload failed"));
        return;
      }
      resolve(JSON.parse(xhr.responseText) as { submission_id: string });
    };
    xhr.send(form);
  });
}

/**
 * getSubmissionStatus performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export async function getSubmissionStatus(
  submissionId: string,
  token: string,
): Promise<SubmissionStatus> {
  const status = await apiFetch<BackendSubmissionStatus>(
    platformConfig.endpoints.submission.detail(submissionId),
    { token },
  );
  return { ...status, status: normalizeSubmissionStatus(status.status) };
}

/**
 * createRun performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function createRun(
  submissionId: string,
  token: string,
): Promise<RunGroupStatus> {
  return apiFetch<RunGroupStatus>(
    platformConfig.endpoints.submission.benchmark(submissionId),
    { method: "POST", token },
  );
}

/**
 * getRunGroupStatus performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function getRunGroupStatus(
  runGroupId: string,
  token: string,
): Promise<RunGroupStatus> {
  return apiFetch<RunGroupStatus>(
    platformConfig.endpoints.submission.runGroup(runGroupId),
    { token },
  );
}

/**
 * getRunGroups performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function getRunGroups(
  params: { contestantId?: string; submissionIds?: string[]; limit?: number },
  token: string,
): Promise<RunGroupHistoryResponse> {
  const query = new URLSearchParams();
  for (const submissionId of params.submissionIds ?? []) {
    query.append("submission_id", submissionId);
  }
  if (params.limit) query.set("limit", String(params.limit));
  const suffix = query.toString();
  return apiFetch<RunGroupHistoryResponse>(
    `${platformConfig.endpoints.submission.runGroups}${suffix ? `?${suffix}` : ""}`,
    { token },
  );
}

/**
 * normalizeSubmissionStatus performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function normalizeSubmissionStatus(
  status: BackendSubmissionPhase | string,
): SubmissionPhase {
  switch (status) {
    case "uploaded":
      return "queued";
    case "scanned":
      return "scanning";
    case "sbom_ready":
      return "promoting";
    case "building":
    case "ready":
    case "failed":
      return status;
    default:
      return "queued";
  }
}

/**
 * parseUploadError performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function parseUploadError(text: string): {
  error?: string;
  message?: string;
  submission_id?: string;
} {
  try {
    return JSON.parse(text) as {
      error?: string;
      message?: string;
      submission_id?: string;
    };
  } catch {
    return { error: text || "Upload failed" };
  }
}
