// Shared types for worker-harness extension

export type WorkerStatus = "online" | "offline" | "draining";
export type JobStatus = "pending" | "starting" | "running" | "done" | "failed";

export interface Worker {
  id: string;
  name: string;
  worker_ip: string;
  dns_name: string;
  ssh_user: string;
  harness_dir: string;
  gpu_count: number;
  gpu_names: string[];
  gpu_vram_gb: number[];
  gpu_used_vram_gb: number[];
  gpu_busy?: Array<boolean | null>;
  cpu_cores: number;
  total_ram_gb: number;
  used_ram_gb: number;
  total_disk_gb: number;
  used_disk_gb: number;
  status: WorkerStatus;
  last_heartbeat_ts: number;
  created_at: number;
  gpu_device_ids?: string[];
  slurm?: SlurmWorkerInfo | null;
}

export interface GpuInfo {
  name: string;
  vram_total_gb: number;
  vram_used_gb: number;
  uuid?: string | null;
}

export interface WorkerDetail extends Worker {
  gpus: GpuInfo[];
  active_ports: Array<{ local: number; remote: number; service: string }>;
  active_jobs: Array<{ job_id: string; tmux_session: string; status: string }>;
}

export interface Job {
  id: string;
  worker_id: string | null;
  worker_name?: string | null;
  name: string;
  tmux_session: string;
  command: string;
  status: JobStatus;
  queue_managed: boolean;
  exit_code: number | null;
  pty_enabled: boolean;
  kind: "ssh";
  origin_session_id: string | null;
  report_revision: number;
  expected_seconds: number;
  gpu_count: number;
  gpu_indices: number[];
  queue_order: number;
  started_at: number;
  finished_at: number | null;
  stdout?: string; // only present in sync mode
  notify_recipient_id?: string | null;
  submission_key?: string | null;
}

export interface Tunnel {
  id: string;
  worker_id: string;
  worker_name?: string;
  local_port: number;
  remote_port: number;
  service_name: string;
  pid: number;
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

export interface StartJobRequest {
  worker_id: string;
  command: string;
  name?: string;
  no_pty?: boolean;
  sync?: boolean;
  sync_timeout?: number;
  notify_recipient_id?: string;
}

export interface QueuedJob extends Job {
  position: number;
}

export interface EnqueueJobRequest {
  worker_id: string;
  command: string;
  name: string;
  expected_seconds: number;
  gpu_count?: number;
  no_pty?: boolean;
  notify_recipient_id?: string;
  submission_key?: string;
}

export interface UpdateQueuedJobRequest {
  worker_id?: string;
  position?: number;
  name?: string;
  expected_seconds?: number;
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
}

/** Client-side targets are not registered workers or immediately free capacity. */
export interface ClusterTarget {
  id: `cluster:${string}`;
  name: string;
  kind: "cluster";
  cluster_id: string;
  compute_kind: "gpu" | "cpu";
  partition: string;
  description: string;
  shared_path: string;
  resource_classes: {
    gpu_type?: string;
    max_gpus: number;
    max_cpus?: number;
    max_memory_mb?: number;
    max_walltime_seconds: number;
  };
  availability: {
    status: "unknown" | "fresh" | "stale";
    observed_at: number | null;
    detail: string;
    [key: string]: unknown;
  };
}

export type ComputeTarget = (Worker & { kind: "worker" }) | ClusterTarget;

export interface SlurmLaunchPolicy {
  cluster_id: string;
  storage_id: string;
  compute_kind: "gpu" | "cpu";
  cpus: number;
  memory_mb: number;
  walltime_seconds: number;
  failure_grace_seconds: number;
  warning_seconds: number;
  termination_grace_seconds: number;
}

export interface SlurmWorkerInfo extends SlurmLaunchPolicy {
  request_id: string;
  recipient_id: string;
  initial_job_id?: string | null;
  slurm_job_id?: string | null;
  scheduler_state?: string | null;
  scheduler_reason?: string | null;
  observed_at?: number | null;
  submission_attempted: boolean;
  submission_failed: boolean;
  cancel_requested: boolean;
  submitted_at?: number | null;
  started_at?: number | null;
  finished_at?: number | null;
  failure_deadline?: number | null;
  last_useful_started_at?: number | null;
  last_useful_started_monotonic?: number | null;
  last_useful_boot_id?: string | null;
}

export interface SlurmReservationRequest {
  request_id: string;
  name: string;
  gpu_count: number;
  recipient_id: string;
  registration_token_hash: string;
  slurm: SlurmLaunchPolicy;
}

export interface RunnerResult {
  exit_code: number;
  descendants_exited: true;
  started_at?: number;
  started_monotonic?: number;
  finished_at: number;
  finished_monotonic: number;
  boot_id: string;
  termination_reason?: string | null;
}

export interface SlurmObservation {
  slurm_job_id?: string;
  scheduler_state?: string;
  scheduler_reason?: string;
  observed_at: number;
  submission_attempted?: boolean;
  submission_failed?: boolean;
  cancel_requested?: boolean;
  cancel_if_empty?: boolean;
  recovered_results?: Array<{ job_id: string; result: RunnerResult }>;
}

export interface SlurmEnqueueOptions {
  project_path: string;
  environment_path?: string;
  cpus?: number;
  memory_mb?: number;
  walltime_seconds: number;
  checkpoint_signal?: "USR1" | "TERM";
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
  tunnel_id: string;
  local_port: number;
  remote_port: number;
  bind_host: string;
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
