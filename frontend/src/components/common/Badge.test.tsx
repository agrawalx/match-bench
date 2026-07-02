/**
 * This file defines tests for Badge.test.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { Badge, type BadgeVariant } from "./Badge";

describe("Badge", () => {
  it("renders every variant", () => {
    const variants: BadgeVariant[] = [
      "scored",
      "running",
      "failed",
      "disqualified",
      "ready",
      "requested",
      "completed",
      "building",
      "queued",
    ];
    variants.forEach((variant) => {
      render(<Badge variant={variant} />);
      // Badge renders the raw variant text; capitalization is CSS-only, so the
      // DOM text content stays lowercase.
      expect(screen.getByText(variant)).toBeInTheDocument();
    });
  });
});
