const STORAGE_KEY = 'iicpc_submission_ids';
const LAST_RUN_GROUP_KEY = 'iicpc_last_run_group_id';

export function rememberSubmissionId(submissionId: string, ownerId = ''): void {
	if (!submissionId || typeof window === 'undefined') return;
	const key = scopedKey(STORAGE_KEY, ownerId);
	const ids = getRememberedSubmissionIds(ownerId);
	const next = [submissionId, ...ids.filter((id) => id !== submissionId)].slice(0, 50);
	setStoredValue(key, JSON.stringify(next));
}

export function getRememberedSubmissionIds(ownerId = ''): string[] {
	if (typeof window === 'undefined') return [];
	try {
		const parsed = JSON.parse(getStoredValue(scopedKey(STORAGE_KEY, ownerId)) ?? '[]') as unknown;
		if (!Array.isArray(parsed)) return [];
		return parsed.filter((value): value is string => typeof value === 'string' && value.length > 0);
	} catch {
		return [];
	}
}

export function rememberRunGroupId(runGroupId: string, ownerId = ''): void {
	if (!runGroupId || typeof window === 'undefined') return;
	setStoredValue(scopedKey(LAST_RUN_GROUP_KEY, ownerId), runGroupId);
}

export function getRememberedRunGroupId(ownerId = ''): string | null {
	if (typeof window === 'undefined') return null;
	const runGroupId = getStoredValue(scopedKey(LAST_RUN_GROUP_KEY, ownerId));
	return runGroupId && runGroupId.length > 0 ? runGroupId : null;
}

function scopedKey(key: string, ownerId: string): string {
	return ownerId ? `${key}:${ownerId}` : key;
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
