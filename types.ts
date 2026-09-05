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
}

export interface GpuInfo {
  name: string;
  vram_total_gb: number;
  vram_used_gb: number;
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
