/**
 * This file defines tests for submission.test.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { uploadSubmission } from "./submission";

/**
 * FakeXHR describes structured data exchanged by this module.
 * Keep this shape aligned with API and component expectations.
 */

class FakeXHR {
  static instances: FakeXHR[] = [];
  upload: { onprogress: ((event: ProgressEvent) => void) | null } = {
    onprogress: null,
  };
  status = 0;
  responseText = "";
  onload: (() => void) | null = null;
  onerror: (() => void) | null = null;
  open = vi.fn();
  setRequestHeader = vi.fn();
  send = vi.fn();

  constructor() {
    FakeXHR.instances.push(this);
  }
}

describe("uploadSubmission", () => {
  beforeEach(() => {
    FakeXHR.instances = [];
    vi.stubGlobal("XMLHttpRequest", FakeXHR);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("rejects an oversized upload (413) with a friendly message", async () => {
    const promise = uploadSubmission(
      new File(["x"], "algo.tar.gz"),
      "token",
      () => {},
    );
    const xhr = FakeXHR.instances[0];
    xhr.status = 413;
    xhr.responseText =
      "<html><head><title>413 Request Entity Too Large</title></head></html>";
    xhr.onload?.();
    await expect(promise).rejects.toThrow(/too large.*100\s?MB/i);
  });

  it("resolves a successful upload", async () => {
    const promise = uploadSubmission(
      new File(["x"], "algo.tar.gz"),
      "token",
      () => {},
    );
    const xhr = FakeXHR.instances[0];
    xhr.status = 201;
    xhr.responseText = JSON.stringify({ submission_id: "sub-1" });
    xhr.onload?.();
    await expect(promise).resolves.toEqual({ submission_id: "sub-1" });
  });
});
