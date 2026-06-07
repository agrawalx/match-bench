'use client';

import { useEffect, useMemo, useState } from 'react';
import { useQuery } from '@tanstack/react-query';
import { motion } from 'framer-motion';
import { useRouter } from 'next/navigation';
import { Brackets, CheckCircle2, Clock3, History, LoaderCircle, XCircle } from 'lucide-react';
import { GoogleButton } from '@/auth/GoogleButton';
import { useAuth } from '@/auth/useAuth';
import { getRunGroups, getRunGroupStatus } from '@/api/submission';
import { platformConfig } from '@/config/platform';
import { ErrorBanner } from '@/components/common/ErrorBanner';
import { useRunDetail } from '@/hooks/useRunDetail';
import {
  getRememberedRunGroupId,
  getRememberedSubmissionIds,
  rememberRunGroupId,
  rememberSubmissionId,
} from '@/utils/submissionHistory';
import { LatencyHistogram } from './LatencyHistogram';
import { RunHeader } from './RunHeader';
import { SessionCards } from './SessionCards';
import { ThroughputChart } from './ThroughputChart';
import styles from './RunClient.module.css';

export function RunClient({ initialRunGroupId }: { initialRunGroupId?: string }) {
  const { status, user, getToken } = useAuth();
  const router = useRouter();
  const token = getToken();
  const [selectedRunGroupId, setSelectedRunGroupId] = useState<string | null>(null);
  const [rememberedSubmissionIds, setRememberedSubmissionIds] = useState<string[]>([]);

  useEffect(() => {
    if (initialRunGroupId) {
      rememberRunGroupId(initialRunGroupId);
      setSelectedRunGroupId(initialRunGroupId);
      return;
    }
    setSelectedRunGroupId(getRememberedRunGroupId());
  }, [initialRunGroupId]);

  useEffect(() => {
    setRememberedSubmissionIds(getRememberedSubmissionIds());
  }, []);

  const historyQuery = useQuery({
    queryKey: ['my-run-history', user?.contestantId, rememberedSubmissionIds],
    enabled: status === 'authenticated',
    queryFn: () =>
      getRunGroups(
        {
          contestantId: user?.contestantId || user?.sub,
          submissionIds: rememberedSubmissionIds,
          limit: platformConfig.leaderboardLimit,
        },
        token ?? '',
      ),
    refetchInterval: (query) =>
      query.state.data?.run_groups.some((group) => group.status === 'requested' || group.status === 'running') ? 2500 : false,
  });

  const history = useMemo(() => historyQuery.data?.run_groups ?? [], [historyQuery.data?.run_groups]);

  const { data, histogram, throughput, isLoading, error } = useRunDetail(selectedRunGroupId);
  const runGroupQuery = useQuery({
    queryKey: ['run-group-status', selectedRunGroupId],
    enabled: Boolean(selectedRunGroupId && token),
    queryFn: () => getRunGroupStatus(selectedRunGroupId ?? '', token ?? ''),
    refetchInterval: (query) => {
      const phase = query.state.data?.status;
      return phase === 'requested' || phase === 'running' ? 2500 : false;
    },
  });
  const authenticated = status === 'authenticated';
  const showRunGroupState =
    Boolean(runGroupQuery.data) &&
    (runGroupQuery.data?.status !== 'completed' ||
      runGroupQuery.data.runs.some((run) => run.status !== 'completed'));

  useEffect(() => {
    const submissionId = runGroupQuery.data?.submission_id;
    if (!submissionId) return;
    rememberSubmissionId(submissionId);
    setRememberedSubmissionIds(getRememberedSubmissionIds());
  }, [runGroupQuery.data?.submission_id]);

  if (!authenticated) {
    return (
      <motion.section className={styles.page} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.22, ease: 'easeOut' }}>
        <div className={styles.heading}>
          <h1>MY RUN</h1>
          <p>Run history and benchmark telemetry for your submitted algorithms.</p>
        </div>
        <div className={styles.empty}>
          <span className={styles.emptyIcon}><Brackets size={32} strokeWidth={1.5} /></span>
          <strong>Sign in to load your run history</strong>
          <p>The page stays available, but personal runs require your contestant identity.</p>
          <GoogleButton />
        </div>
      </motion.section>
    );
  }

  if (historyQuery.isLoading && !selectedRunGroupId) return <p className={styles.mono}>Loading run history...</p>;

  if (historyQuery.error && !selectedRunGroupId) {
    return (
      <motion.section className={styles.page} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.22, ease: 'easeOut' }}>
        <div className={styles.heading}>
          <h1>MY RUN</h1>
          <p>Run history and benchmark telemetry for your submitted algorithms.</p>
        </div>
        <ErrorBanner message="Run history is unavailable because leaderboard-api is not reachable." />
      </motion.section>
    );
  }

  if (history.length === 0 && !selectedRunGroupId) {
    return (
      <motion.section className={styles.page} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.22, ease: 'easeOut' }}>
        <div className={styles.heading}>
          <h1>MY RUN</h1>
          <p>Run history and benchmark telemetry for your submitted algorithms.</p>
        </div>
        <div className={styles.empty}>
          <span className={styles.emptyIcon}><History size={32} strokeWidth={1.5} /></span>
          <strong>No benchmark runs yet</strong>
          <p>Submit an algorithm and start a benchmark run to populate this history.</p>
        </div>
      </motion.section>
    );
  }

  return (
    <motion.section className={styles.page} initial={{ opacity: 0, y: 8 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.22, ease: 'easeOut' }}>
      <div className={styles.heading}>
        <h1>MY RUN</h1>
        <p>Run history and benchmark telemetry for your submitted algorithms.</p>
      </div>
      <div className={styles.history}>
        {selectedRunGroupId && !history.some((row) => row.run_group_id === selectedRunGroupId) && (
          <button className={`${styles.historyItem} ${styles.selected}`} type="button">
            <span>{selectedRunGroupId}</span>
            <strong>{runGroupQuery.data?.status ?? 'requested'}</strong>
            <small>Current benchmark run</small>
          </button>
        )}
        {history.map((row) => {
          const selectRun = () => {
            rememberRunGroupId(row.run_group_id);
            setSelectedRunGroupId(row.run_group_id);
            router.replace(`/run?run_group_id=${encodeURIComponent(row.run_group_id)}`);
          };
          return (
            <button
              key={row.run_group_id}
              className={`${styles.historyItem} ${selectedRunGroupId === row.run_group_id ? styles.selected : ''}`}
              type="button"
              onClick={selectRun}
            >
              <span>{row.run_group_id}</span>
              <strong>{row.status}</strong>
              <small>{row.runs.length} scenarios / {row.submission_id}</small>
            </button>
          );
        })}
      </div>
      {runGroupQuery.data && showRunGroupState && <PendingRunCard runGroup={runGroupQuery.data} />}
      {runGroupQuery.isLoading && selectedRunGroupId && !data && <p className={styles.mono}>Loading current benchmark run...</p>}
      {runGroupQuery.error && selectedRunGroupId && !data && (
        <ErrorBanner message={runGroupQuery.error instanceof Error ? runGroupQuery.error.message : 'Run status failed to load'} />
      )}
      {isLoading && !data && <p className={styles.mono}>Loading selected run...</p>}
      {error && !runGroupQuery.data && <ErrorBanner message={error instanceof Error ? error.message : 'Run failed to load'} />}
      {data && (
        <>
          <RunHeader run={data} />
          <div className={styles.charts}>
            <LatencyHistogram histogram={histogram} run={data} />
            <ThroughputChart throughput={throughput} run={data} />
          </div>
          <SessionCards sessions={data.sessions} />
        </>
      )}
    </motion.section>
  );
}

function PendingRunCard({ runGroup }: { runGroup: Awaited<ReturnType<typeof getRunGroupStatus>> }) {
  const terminal = runGroup.status === 'completed' || runGroup.status === 'failed';
  const running = runGroup.status === 'running';
  const requested = runGroup.status === 'requested';
  const requestedAgeMs = requested ? Date.now() - Date.parse(runGroup.created_at) : 0;
  const stalled = requestedAgeMs > 20_000;
  const Icon = runGroup.status === 'completed' ? CheckCircle2 : runGroup.status === 'failed' ? XCircle : running ? LoaderCircle : Clock3;

  return (
    <section className={styles.pendingRun}>
      <div className={styles.pendingHeader}>
        <span className={running ? styles.statusIconActive : styles.statusIcon}>
          <Icon size={22} strokeWidth={1.8} />
        </span>
        <div>
          <strong>{runGroup.status}</strong>
          <span>{requested ? 'Waiting for a benchmark runner to claim this run' : `Run group ${runGroup.run_group_id}`}</span>
        </div>
      </div>
      <div className={styles.stageTrack} aria-label="Benchmark run progress">
        {['requested', 'running', terminal ? runGroup.status : 'completed'].map((stage, index) => {
          const active = runGroup.status === stage || (stage === 'completed' && runGroup.status === 'failed');
          const passed =
            (stage === 'requested' && ['running', 'completed', 'failed'].includes(runGroup.status)) ||
            (stage === 'running' && ['completed', 'failed'].includes(runGroup.status));
          return (
            <span key={`${stage}-${index}`} className={`${styles.stage} ${active ? styles.stageActive : ''} ${passed ? styles.stagePassed : ''}`}>
              {stage}
            </span>
          );
        })}
      </div>
      {stalled && (
        <p className={styles.stalledNotice}>
          No runner has consumed this benchmark request yet. Check that submission-api and bot-fleet-controller are using the same Kafka and Postgres stack.
        </p>
      )}
      <div className={styles.sessionList}>
        {runGroup.runs.map((run) => (
          <div key={run.session_id} className={styles.sessionRow}>
            <span>{run.scenario_name || run.scenario_id}</span>
            <strong>{run.status}</strong>
            <small>{run.session_id}</small>
            {run.message && <em>{run.message}</em>}
          </div>
        ))}
      </div>
    </section>
  );
}
