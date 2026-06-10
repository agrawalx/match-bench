/**
 * This file defines frontend behavior for pkce.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
/**
 * generateVerifier performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export async function generateVerifier(): Promise<string> {
  const bytes = new Uint8Array(96);
  crypto.getRandomValues(bytes);
  return base64url(bytes);
}

/**
 * generateChallenge performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export async function generateChallenge(verifier: string): Promise<string> {
  const digest = await crypto.subtle.digest(
    "SHA-256",
    new TextEncoder().encode(verifier),
  );
  return base64url(new Uint8Array(digest));
}

/**
 * generateState performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function generateState(): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  return Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join(
    "",
  );
}

/**
 * generateNonce performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function generateNonce(): string {
  return generateState();
}

/**
 * base64url performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
function base64url(bytes: Uint8Array): string {
  const text = Array.from(bytes, (byte) => String.fromCharCode(byte)).join("");
  return btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
}
