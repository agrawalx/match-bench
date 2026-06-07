'use client';

import { useState } from 'react';
import { useMutation, useQuery } from '@tanstack/react-query';
import { useRouter } from 'next/navigation';
import { motion } from 'framer-motion';
import { LogIn, Play, Send, ShieldCheck } from 'lucide-react';
import { GoogleButton } from '@/auth/GoogleButton';
import { useAuth } from '@/auth/useAuth';
import { createRun, getSubmissionStatus, uploadSubmission } from '@/api/submission';
import { platformConfig } from '@/config/platform';
import type { SubmissionStatus } from '@/types/submission';
import { rememberRunGroupId, rememberSubmissionId } from '@/utils/submissionHistory';
import { BuildTimeline } from './BuildTimeline';
import { DropZone } from './DropZone';
import { UploadProgress } from './UploadProgress';
import styles from './SubmitClient.module.css';

export function SubmitClient() {
  const { status, getToken, signIn } = useAuth();
  const router = useRouter();
  const [file, setFile] = useState<File | null>(null);
  const [progress, setProgress] = useState(0);
  const [submissionId, setSubmissionId] = useState<string | null>(null);
  const [fileError, setFileError] = useState<string | null>(null);
  const [statusMessage, setStatusMessage] = useState<string | null>(null);
  const token = getToken();

  const statusQuery = useQuery<SubmissionStatus>({
    queryKey: ['submission', submissionId],
    enabled: Boolean(submissionId && token),
    queryFn: () => getSubmissionStatus(submissionId ?? '', token ?? ''),
    refetchInterval: (query) => {
      const phase = query.state.data?.status;
      return phase === 'queued' || phase === 'building' || phase === 'scanning' || phase === 'promoting' ? 3000 : false;
    },
  });

  const upload = useMutation({
    mutationFn: async () => {
      if (!file || !token) throw new Error('missing_file');
      if (!file.name.endsWith('.zip')) throw new Error('Only .zip bundles are accepted');
      if (file.size > platformConfig.uploadMaxBytes) {
        throw new Error(`File exceeds ${Math.round(platformConfig.uploadMaxBytes / 1024 / 1024)} MB limit`);
      }
      return uploadSubmission(file, token, setProgress);
    },
    onSuccess: (result) => {
      rememberSubmissionId(result.submission_id);
      setSubmissionId(result.submission_id);
      setProgress(100);
      setStatusMessage(result.reused ? 'Bundle already exists. Loaded the existing submission.' : null);
    },
    onError: (error) => {
      setStatusMessage(null);
      setFileError(error instanceof Error ? error.message : 'Upload failed');
    },
  });

  const run = useMutation({
    mutationFn: async () => {
      if (!submissionId || !token) throw new Error('missing_submission');
      return createRun(submissionId, token);
    },
    onSuccess: (result) => {
      rememberSubmissionId(result.submission_id);
      rememberRunGroupId(result.run_group_id);
      router.push(`/run?run_group_id=${encodeURIComponent(result.run_group_id)}`);
    },
  });

  const authenticated = status === 'authenticated';
  const terminalReady = statusQuery.data?.status === 'ready';

  return (
    <motion.section
      className={styles.page}
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.22, ease: 'easeOut' }}
    >
      <div className={styles.heading}>
        <div>
          <h1>SUBMIT</h1>
          <p>Upload a zip bundle for build, scan, promotion, and benchmark execution.</p>
        </div>
        <div className={styles.headingMeta}>
          <ShieldCheck size={16} strokeWidth={1.8} />
          <span>{Math.round(platformConfig.uploadMaxBytes / 1024 / 1024)} MB max .zip</span>
        </div>
      </div>
      <div className={styles.workspace}>
        <motion.div className={styles.primary} layout>
          {!authenticated && (
            <div className={styles.authNotice}>
              <LogIn size={18} strokeWidth={1.8} aria-hidden="true" />
              <div>
                <strong>Sign in required for submission</strong>
                <span>You can prepare a bundle here, but upload starts after Google sign-in.</span>
              </div>
              <GoogleButton />
            </div>
          )}
          <UploadProgress progress={progress} />
          <div className={styles.dropWrap}>
            <DropZone
              file={file}
              error={null}
              onFile={(next) => {
                setFile(next);
                setFileError(null);
                setStatusMessage(null);
                setProgress(0);
              }}
            />
          </div>
          <div className={styles.actions}>
            <button
              type="button"
              disabled={!file || upload.isPending || status === 'redirecting'}
              onClick={() => {
                if (!authenticated) {
                  void signIn();
                  return;
                }
                upload.mutate();
              }}
            >
              {authenticated ? <Send size={16} strokeWidth={1.8} /> : <LogIn size={16} strokeWidth={1.8} />}
              <span>{upload.isPending ? 'Uploading' : authenticated ? 'Submit Bundle' : 'Sign in to Submit'}</span>
            </button>
            {terminalReady && (
              <button type="button" disabled={run.isPending} onClick={() => run.mutate()}>
                <Play size={16} strokeWidth={1.8} />
                <span>Start Benchmark Run</span>
              </button>
            )}
          </div>
          {(fileError || statusMessage || run.error) && (
            <p className={fileError || run.error ? styles.actionError : styles.actionNotice}>
              {fileError ?? statusMessage ?? (run.error instanceof Error ? run.error.message : 'Benchmark run failed')}
            </p>
          )}
        </motion.div>
        <motion.aside className={styles.statusPanel} aria-label="Submission status" layout>
          <BuildTimeline status={statusQuery.data ?? null} />
          {!statusQuery.data && (
            <div className={styles.placeholder}>
              <ShieldCheck size={24} strokeWidth={1.6} aria-hidden="true" />
              <strong>Pipeline status</strong>
              <span>Upload a bundle to watch queue, build, scan, and promotion states here.</span>
            </div>
          )}
        </motion.aside>
      </div>
    </motion.section>
  );
}
