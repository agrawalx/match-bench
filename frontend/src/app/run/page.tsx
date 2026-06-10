import { RunClient } from '@/components/run/RunClient';

export const metadata = { title: 'My Run - IICPC' };

export default function RunPage({ searchParams }: { searchParams?: { run_group_id?: string } }) {
  return <RunClient initialRunGroupId={searchParams?.run_group_id} />;
}
