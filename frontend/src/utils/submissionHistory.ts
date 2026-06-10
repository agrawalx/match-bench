/**
 * This file defines frontend behavior for submissionHistory.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
const STORAGE_KEY = "iicpc_submission_ids";
const LAST_RUN_GROUP_KEY = "iicpc_last_run_group_id";

/**
 * rememberSubmissionId performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function rememberSubmissionId(submissionId: string, ownerId = ""): void {
  if (!submissionId || typeof window === "undefined") return;
  const key = scopedKey(STORAGE_KEY, ownerId);
  const ids = getRememberedSubmissionIds(ownerId);
  const next = [submissionId, ...ids.filter((id) => id !== submissionId)].slice(
    0,
    50,
  );
  setStoredValue(key, JSON.stringify(next));
}

/**
 * getRememberedSubmissionIds performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function getRememberedSubmissionIds(ownerId = ""): string[] {
  if (typeof window === "undefined") return [];
  try {
    const parsed = JSON.parse(
      getStoredValue(scopedKey(STORAGE_KEY, ownerId)) ?? "[]",
    ) as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.filter(
      (value): value is string => typeof value === "string" && value.length > 0,
    );
  } catch {
    return [];
  }
}

/**
 * rememberRunGroupId performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function rememberRunGroupId(runGroupId: string, ownerId = ""): void {
  if (!runGroupId || typeof window === "undefined") return;
  setStoredValue(scopedKey(LAST_RUN_GROUP_KEY, ownerId), runGroupId);
}

/**
 * getRememberedRunGroupId performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function getRememberedRunGroupId(ownerId = ""): string | null {
  if (typeof window === "undefined") return null;
  const runGroupId = getStoredValue(scopedKey(LAST_RUN_GROUP_KEY, ownerId));
  return runGroupId && runGroupId.length > 0 ? runGroupId : null;
}

/**
 * scopedKey performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function scopedKey(key: string, ownerId: string): string {
  return ownerId ? `${key}:${ownerId}` : key;
}

/**
 * getStoredValue performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function getStoredValue(key: string): string | null {
  try {
    return (
      window.localStorage.getItem(key) ?? window.sessionStorage.getItem(key)
    );
  } catch {
    return window.sessionStorage.getItem(key);
  }
}

/**
 * setStoredValue performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function setStoredValue(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value);
  } catch {}
  window.sessionStorage.setItem(key, value);
}
