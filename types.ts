// Shared types for worker-harness extension

export type WorkerStatus = "online" | "offline" | "draining";
export type JobStatus = "pending" | "starting" | "running" | "done" | "failed";
export type JobPriority = "flexible" | "normal" | "blocking" | "time_critical";
export type ExecutionState = "active" | "stopping" | "termination_unknown";
export type TerminationReason = "user_cancelled" | "gpu_idle_timeout" | "user_disabled" | null;

/** Explicit shared projection: never includes worker addresses or process details. */
export interface PublicWorker {
  id: string;
  name: string;
  status: WorkerStatus;
  gpu_count: number;
  gpu_names: string[];
  gpu_vram_gb: number[];
  gpu_used_vram_gb: number[];
  gpu_utilization_percent: Array<number | null>;
  gpu_available: Array<boolean | null>;
  cpu_cores: number;
  data_paths: string[];
}

export interface OwnedJob {
  id: string;
  worker_id: string | null;
  worker_name: string | null;
  name: string;
  command: string;
  status: JobStatus;
  execution_state: ExecutionState;
  exit_code: number | null;
  kind: string;
  owner_user_id: string;
  owner_display_name: string;
  submitted_by_actor_id: string;
  submitted_at: number;
  submission_sequence: number;
  priority: JobPriority;
  priority_note: string;
  deadline_at: number | null;
  expected_seconds: number;
  gpu_count: number;
  gpu_indices: number[];
  queue_managed: boolean;
  started_at: number | null;
  finished_at: number | null;
  termination_reason: TerminationReason;
  stop_requested_at: number | null;
  origin_session_id: string | null;
  delay_history?: DelayEntry[];
  stdout?: string;
}

export interface DelayEntry {
  event_id: string;
  created_at: number;
  delay_seconds: number | null;
  moved_job_id: string;
  moved_job_name: string;
  moved_job_owner_display_name: string;
  actor_user_id: string;
  actor_display_name: string;
  reason: string;
}

export interface QueueJob {
  id: string;
  name: string;
  owner_user_id: string;
  owner_display_name: string;
  worker_id: string;
  worker_name: string;
  status: JobStatus;
  execution_state: ExecutionState;
  position: number;
  submitted_at: number;
  submission_sequence: number;
  priority: JobPriority;
  priority_note: string;
  deadline_at: number | null;
  expected_seconds: number;
  gpu_count: number;
  started_at: number | null;
  termination_reason: TerminationReason;
  stop_requested_at: number | null;
  estimated_start_at: number | null;
  estimated_finish_at: number | null;
  estimate_status: "known" | "unknown";
  estimate_explanation: string;
  delay_history: DelayEntry[];
}

export interface QueueBoard {
  workers: Array<{
    worker_id: string;
    worker_name: string;
    revision: number;
    jobs: QueueJob[];
    cpu_jobs: QueueJob[];
  }>;
  as_of: number;
}

export interface Impact {
  job_id: string;
  name: string;
  owner_user_id: string;
  owner_display_name: string;
  before_position: number;
  after_position: number;
  before_start_at: number | null;
  after_start_at: number | null;
  delay_seconds: number | null;
}

export interface QueueEvent {
  id: string;
  event_type: "reorder" | "worker_move" | "job_updated";
  worker_id: string;
  job_id: string;
  job_name: string;
  job_owner_user_id: string;
  job_owner_display_name: string;
  actor_user_id: string;
  actor_display_name: string;
  actor_id: string;
  reason: string;
  created_at: number;
  old_revision: number;
  new_revision: number;
  old_order: string[];
  new_order: string[];
  impacts: Impact[];
}

export type QueuePreview =
  | { changed: false }
  | { changed: true; preview_id: string; revision: number; expires_at: number;
      before_order: string[]; after_order: string[]; impacts: Impact[] };
export interface QueueMoveResult { event: QueueEvent; revision: number }
export interface QueueHistory { events: QueueEvent[]; next_before_event_id: string | null }

export interface UsageRow {
  owner_user_id: string;
  owner_display_name: string;
  worker_id: string;
  worker_name: string;
  job_seconds: number;
  cpu_job_seconds: number;
  allocated_gpu_seconds: number;
  running_jobs: number;
  provisional: boolean;
  historical_estimate: boolean;
}
export interface UsageResult { rows: UsageRow[]; as_of: number; provisional: boolean }

export interface Tunnel {
  id: string;
  worker_id: string;
  worker_name: string;
  local_port: number;
  remote_port: number;
  service_name: string;
  bind_host: string;
  owner_user_id: string;
  created_at: number;
}

export interface WorkersSummary {
  total: number;
  online: number;
  offline: number;
  draining: number;
}

export interface WorkersPruneResult {
  removed: number;
  minutes: number;
}

export interface PriorityFields {
  priority?: JobPriority;
  priority_note?: string;
  deadline_at?: number | null;
}
export interface StartJobRequest extends PriorityFields {
  worker_id: string;
  command: string;
  name: string;
  expected_seconds: number;
  no_pty?: boolean;
  sync?: boolean;
  sync_timeout?: number;
}
export interface EnqueueJobRequest extends PriorityFields {
  worker_id: string;
  command: string;
  name: string;
  expected_seconds: number;
  gpu_count: number;
  no_pty?: boolean;
}
export interface UpdateJobRequest extends PriorityFields {
  expected_seconds?: number;
}
export interface UpdateQueuedJobRequest extends UpdateJobRequest {
  worker_id?: string;
  name?: string;
  gpu_count?: number;
}

export interface JobLogsResult {
  job_id: string;
  tail: number | null;
  head: number | null;
  logs: string;
}

export interface StopJobResult {
  job_id: string;
  stopped: boolean;
  already_terminal: boolean;
  status: string | null;
  execution_state: ExecutionState;
}

export interface AddTunnelRequest {
  worker_id: string;
  local_port: number;
  remote_port: number;
  name?: string;
}

export interface RemoveTunnelResponse {
  removed: boolean;
  tunnel_id: string;
}

export interface MarimoSession {
  id: string;
  worker_id: string;
  worker_name?: string | null;
  notebook_path: string;
  environment: string;
  job_id: string;
  owner_user_id: string;
  url: string;
  status: "ready" | "stopped";
  created_at: number;
}

export interface MarimoCreateRequest {
  worker_id: string;
  notebook_path: string;
  environment: string;
  ready_timeout?: number;
}

export interface RemoveMarimoResponse {
  session_id: string;
  removed: boolean;
}

export interface MarimoKernelSession {
  path?: string;
  filename?: string;
  [key: string]: unknown;
}

export interface MarimoExecutionResult {
  marimo_session_id: string;
  kernel_session_id: string;
  /** True when this call had to create the kernel, so its namespace starts empty. */
  kernel_created: boolean;
  success: boolean;
  stdout: string;
  stderr: string;
  output: unknown;
}

export type DataPaths = Record<string, Array<{ worker_id: string; worker_name: string }>>;

export interface DataCopyResult {
  transfer_id: string;
  job_id: string;
  source_worker: string;
  source_path: string;
  destination_worker: string;
  destination_path: string;
  expires_in_seconds: number;
}

export interface FileTransferResult {
  worker_id: string;
  path: string;
  size: number;
  content_b64?: string;
}

export type PiSessionState =
  | "queued"
  | "starting"
  | "working"
  | "idle"
  | "blocked"
  | "stopped"
  | "failed"
  | "termination_unknown";

export interface PiSession {
  id: string;
  worker_id: string | null;
  parent_session_id: string | null;
  session_type: "interactive" | "global-router";
  state: PiSessionState;
  task: string;
  cwd: string;
  tmux_session: string;
  detail: string;
  role: "" | "orchestrator" | "pm" | "task";
  question: string;
  meta: Record<string, unknown>;
  created_at: number;
  updated_at: number;
}

export interface Project {
  name: string;
  machine: string;
  repo: string;
  remote: string;
  base_branch: string;
}

export interface ApiErrorBody {
  error?: {
    code: string;
    message: string;
    detail: unknown;
  };
  detail?: unknown;
}
