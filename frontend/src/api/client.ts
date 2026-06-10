/**
 * This file defines frontend behavior for client.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
/**
 * ApiError describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */
export class ApiError extends Error {
  constructor(
    public readonly status: number,
    message: string,
    public readonly requestId?: string,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

/**
 * apiFetch performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export async function apiFetch<T>(
  path: string,
  options: RequestInit & { token?: string } = {},
): Promise<T> {
  const base = process.env.NEXT_PUBLIC_API_BASE ?? "";
  const headers = new Headers(options.headers);
  const hasBody = options.body !== undefined;

  if (hasBody && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  if (options.token) {
    headers.set("Authorization", `Bearer ${options.token}`);
  }

  const response = await fetch(`${base}${path}`, {
    ...options,
    headers,
    credentials: options.credentials ?? "same-origin",
  });

  if (!response.ok) {
    const body = (await response
      .json()
      .catch(() => ({ message: response.statusText }))) as {
      message?: string;
      error?: string;
    };
    throw new ApiError(
      response.status,
      body.message ?? body.error ?? response.statusText,
      response.headers.get("x-request-id") ?? undefined,
    );
  }

  if (response.status === 204) return undefined as T;
  return response.json() as Promise<T>;
}
