/**
 * This file defines frontend behavior for SubmitClient.
 * It is part of the IICPC frontend and keeps UI, API, or test behavior
 * scoped to this module so callers can rely on stable boundaries.
 */
"use client";

import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { AlertTriangle, Play, Send, ShieldCheck } from "lucide-react";
import toast from "react-hot-toast";
import {
  createRun,
  getSubmissionStatus,
  uploadSubmission,
} from "@/api/submission";
import { defaultContestantId, platformConfig } from "@/config/platform";
import type { SubmissionStatus } from "@/types/submission";
import {
  rememberRunGroupId,
  rememberSubmissionId,
} from "@/utils/submissionHistory";
import { BuildTimeline } from "./BuildTimeline";
import { DropZone } from "./DropZone";
import { UploadProgress } from "./UploadProgress";
import styles from "./SubmitClient.module.css";

/**
 * SubmitClient performs the module-specific operation described by its name.
 * It keeps inputs, side effects, and returned values within this module's contract.
 */
export function SubmitClient() {
  const router = useRouter();
  const [file, setFile] = useState<File | null>(null);
  const [progress, setProgress] = useState(0);
  const [submissionId, setSubmissionId] = useState<string | null>(null);
  const [optimisticStatus, setOptimisticStatus] =
    useState<SubmissionStatus | null>(null);
  const [fileError, setFileError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const [isDuplicate, setIsDuplicate] = useState(false);
  const ownerId = defaultContestantId();
  const duplicateError =
    fileError?.toLowerCase().includes("duplicate") ?? false;

  const statusQuery = useQuery<SubmissionStatus>({
    queryKey: ["submission", submissionId],
    enabled: Boolean(submissionId),
    queryFn: () => getSubmissionStatus(submissionId ?? ""),
    refetchInterval: (query) => {
      const phase = query.state.data?.status;
      return phase === "queued" ||
        phase === "building" ||
        phase === "scanning" ||
        phase === "promoting"
        ? 3000
        : false;
    },
  });

  const upload = useMutation({
    mutationFn: async () => {
      if (!file) throw new Error("missing_file");
      if (!file.name.endsWith(".zip"))
        throw new Error("Only .zip bundles are accepted");
      if (file.size > platformConfig.uploadMaxBytes) {
        throw new Error(
          `File exceeds ${Math.round(platformConfig.uploadMaxBytes / 1024 / 1024)} MB limit`,
        );
      }
      return uploadSubmission(file, setProgress);
    },
    onSuccess: (result) => {
      const now = new Date().toISOString();
      rememberSubmissionId(result.submission_id, ownerId);
      setSubmissionId(result.submission_id);
      setOptimisticStatus({
        submission_id: result.submission_id,
        contestant_id: ownerId || undefined,
        status: "queued",
        created_at: now,
        updated_at: now,
      });
      setProgress(100);
      setIsDuplicate(Boolean(result.reused));
      setStatusMessage(
        result.reused
          ? "Duplicate submission detected. Existing submission loaded."
          : null,
      );
      if (result.reused) {
        toast(
          "Duplicate submission detected. Loaded the existing submission instead.",
          {
            icon: <AlertTriangle size={18} strokeWidth={2} />,
            duration: 9000,
            style: {
              borderColor: "rgba(255, 214, 0, 0.5)",
            },
          },
        );
      } else {
        toast.success("Bundle uploaded. Build pipeline started.");
      }
    },
    onError: (error) => {
      const message = error instanceof Error ? error.message : "Upload failed";
      const duplicate = message.toLowerCase().includes("duplicate");
      setIsDuplicate(duplicate);
      setStatusMessage(null);
      setFileError(message);
      if (duplicate) {
        toast(
          "Duplicate submission detected. Change the ZIP contents and upload again.",
          {
            icon: <AlertTriangle size={18} strokeWidth={2} />,
            duration: 9000,
            style: {
              borderColor: "rgba(255, 214, 0, 0.5)",
            },
          },
        );
      } else {
        toast.error(message);
      }
    },
  });

  const run = useMutation({
    mutationFn: async () => {
      if (!submissionId) throw new Error("missing_submission");
      return createRun(submissionId);
    },
    onSuccess: (result) => {
      rememberSubmissionId(result.submission_id, ownerId);
      rememberRunGroupId(result.run_group_id, ownerId);
      router.push(
        `/run?run_group_id=${encodeURIComponent(result.run_group_id)}`,
      );
    },
    onError: (error) => {
      toast.error(
        error instanceof Error ? error.message : "Benchmark run failed",
      );
    },
  });

  const timelineStatus = statusQuery.data ?? optimisticStatus;
  const terminalReady = timelineStatus?.status === "ready";

  return (
    <section className={styles.page}>
      <div className={styles.heading}>
        <div>
          <h1>Submit engine</h1>
          <p>
            Upload a zip bundle for build, scan, promotion, and benchmark
            execution.
          </p>
        </div>
        <div className={styles.headingMeta}>
          <ShieldCheck size={16} strokeWidth={1.8} />
          <span>
            {Math.round(platformConfig.uploadMaxBytes / 1024 / 1024)} MB max
            .zip
          </span>
        </div>
      </div>
      <div className={styles.workspace}>
        <div className={styles.primary}>
          <UploadProgress progress={progress} />
          {(isDuplicate || duplicateError) && (
            <div className={styles.duplicateWarning}>
              <div className={styles.duplicateIcon}>
                <AlertTriangle size={20} strokeWidth={2} />
              </div>
              <div className={styles.duplicateText}>
                <strong>Duplicate Submission Detected</strong>
                {submissionId ? (
                  <>
                    <p>
                      This exact ZIP bundle (matching SHA256) was already
                      uploaded. The platform loaded the existing submission, so
                      any run uses the previously built version.
                    </p>
                    <small>
                      To build a new version, make a trivial edit, re-zip the
                      bundle, and upload again.
                    </small>
                  </>
                ) : (
                  <>
                    <p>
                      This ZIP matches a bundle that is already in the platform
                      cache, so it cannot be accepted as a new submission.
                    </p>
                    <small>
                      Use one of the fresh sample ZIPs or change any
                      source/comment before re-zipping.
                    </small>
                  </>
                )}
              </div>
            </div>
          )}
          <div className={styles.dropWrap}>
            <DropZone
              file={file}
              error={null}
              onFile={(next) => {
                setFile(next);
                setSubmissionId(null);
                setOptimisticStatus(null);
                setFileError(null);
                setStatusMessage(null);
                setIsDuplicate(false);
                setProgress(0);
              }}
            />
          </div>
          <div className={styles.actions}>
            <button
              type="button"
              disabled={!file || upload.isPending}
              onClick={() => upload.mutate()}
            >
              <Send size={16} strokeWidth={1.8} />
              <span>{upload.isPending ? "Uploading" : "Submit Bundle"}</span>
            </button>
            {terminalReady && (
              <button
                type="button"
                disabled={run.isPending}
                onClick={() => run.mutate()}
              >
                <Play size={16} strokeWidth={1.8} />
                <span>Start Benchmark Run</span>
              </button>
            )}
          </div>
          {(fileError || statusMessage || run.error) && (
            <div
              className={
                (fileError && !duplicateError) || run.error
                  ? styles.actionError
                  : styles.actionNotice
              }
              role="status"
            >
              <AlertTriangle size={16} strokeWidth={2} aria-hidden="true" />
              <span>
                {fileError ??
                  statusMessage ??
                  (run.error instanceof Error
                    ? run.error.message
                    : "Benchmark run failed")}
              </span>
            </div>
          )}
        </div>
        <aside className={styles.statusPanel} aria-label="Submission status">
          <BuildTimeline status={timelineStatus ?? null} />
          {!timelineStatus && (
            <div className={styles.placeholder}>
              <ShieldCheck size={24} strokeWidth={1.6} aria-hidden="true" />
              <strong>Pipeline status</strong>
              <span>
                Upload a bundle to watch queue, build, scan, and promotion
                states here.
              </span>
            </div>
          )}
        </aside>
      </div>
    </section>
  );
}
