/**
 * This file defines tests for format.test.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { describe, expect, it } from "vitest";
import { formatLatencyNs } from "./format";

describe("formatLatencyNs", () => {
  it("formats edge values", () => {
    expect(formatLatencyNs(0)).toBe("0 us");
    expect(formatLatencyNs(1_500_000_000)).toBe("1.50 s");
    expect(formatLatencyNs(Number.NaN)).toBe("-");
  });
});
