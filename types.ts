// Shared types for worker-harness extension

export type WorkerStatus = "online" | "offline" | "draining";
export type JobStatus = "pending" | "running" | "done" | "failed";

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
  worker_name?: string;
  tmux_session: string;
  command: string;
  name?: string;
  status: JobStatus;
  exit_code: number | null;
  pty_enabled: boolean;
  kind: "ssh" | "delegated";
  origin_session_id: string | null;
  report_revision: number;
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
  | "stopped"
  | "failed"
  | "termination_unknown";

export interface PiSession {
  id: string;
  worker_id: string | null;
  parent_session_id: string | null;
  session_type: "interactive" | "delegated" | "global-router";
  state: PiSessionState;
  task: string;
  cwd: string;
  tmux_session: string;
  detail: string;
  created_at: number;
  updated_at: number;
}

export interface PiDelegation {
  id: string;
  parent_session_id: string | null;
  worker_id: string;
  child_session_id: string;
  task: string;
  state: PiSessionState;
  timeout_seconds: number;
  created_at: number;
  completed_at: number;
}

export interface PiSessionEvent {
  id: string;
  session_id: string;
  event_type: string;
  payload: Record<string, unknown>;
  created_at: number;
}

export interface PiDelegationCreateResult {
  delegation_id: string;
  child_session_id: string;
  state: PiSessionState;
  status_url: string;
  /** Present only for a sync delegation request. */
  settled?: boolean;
  session?: PiSession | null;
  delegation?: PiDelegation | null;
  events?: PiSessionEvent[];
}

export interface ApiErrorBody {
  error: {
    code: string;
    message: string;
    detail: unknown;
  };
}
