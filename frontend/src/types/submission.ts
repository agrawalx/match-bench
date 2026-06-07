export type BackendSubmissionPhase =
  | 'uploaded'
  | 'building'
  | 'scanned'
  | 'sbom_ready'
  | 'ready'
  | 'failed';

export type SubmissionPhase = 'queued' | 'building' | 'scanning' | 'promoting' | 'ready' | 'failed';

export interface SubmissionStatus {
  submission_id: string;
  contestant_id?: string;
  status: SubmissionPhase;
  created_at: string;
  updated_at?: string;
  build_logs?: string;
}

export interface BackendSubmissionStatus extends Omit<SubmissionStatus, 'status'> {
  status: BackendSubmissionPhase;
}
