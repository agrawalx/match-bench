const STORAGE_KEY = 'iicpc_submission_ids';
const LAST_RUN_GROUP_KEY = 'iicpc_last_run_group_id';

export function rememberSubmissionId(submissionId: string): void {
  if (!submissionId || typeof window === 'undefined') return;
  const ids = getRememberedSubmissionIds();
  const next = [submissionId, ...ids.filter((id) => id !== submissionId)].slice(0, 50);
  setStoredValue(STORAGE_KEY, JSON.stringify(next));
}

export function getRememberedSubmissionIds(): string[] {
  if (typeof window === 'undefined') return [];
  try {
    const parsed = JSON.parse(getStoredValue(STORAGE_KEY) ?? '[]') as unknown;
    if (!Array.isArray(parsed)) return [];
    return parsed.filter((value): value is string => typeof value === 'string' && value.length > 0);
  } catch {
    return [];
  }
}

export function rememberRunGroupId(runGroupId: string): void {
  if (!runGroupId || typeof window === 'undefined') return;
  setStoredValue(LAST_RUN_GROUP_KEY, runGroupId);
}

export function getRememberedRunGroupId(): string | null {
  if (typeof window === 'undefined') return null;
  const runGroupId = getStoredValue(LAST_RUN_GROUP_KEY);
  return runGroupId && runGroupId.length > 0 ? runGroupId : null;
}

function getStoredValue(key: string): string | null {
  try {
    return window.localStorage.getItem(key) ?? window.sessionStorage.getItem(key);
  } catch {
    return window.sessionStorage.getItem(key);
  }
}

function setStoredValue(key: string, value: string): void {
  try {
    window.localStorage.setItem(key, value);
  } catch {
    // Ignore storage quota/privacy errors; sessionStorage still preserves this tab.
  }
  window.sessionStorage.setItem(key, value);
}
