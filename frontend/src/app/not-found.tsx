/**
 * This file defines frontend behavior for not found.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
export default function NotFound() {
  return (
    <section>
      <h1>404</h1>
      <p>Route not found.</p>
    </section>
  );
}
