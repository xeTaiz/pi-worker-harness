/**
 * Grouped worker-harness tools.
 *
 * The grouped tools (`wh_read` and `wh_dispatch`) are the ONLY way the
 * extension exposes worker-harness operations to Pi's tool registry.
 * They are intentionally action-based so that subagent configs can grant
 * a full RO/RW tier with one name (`tools: ["wh_read"]` /
 * `tools: ["wh_dispatch"]`) instead of having to enumerate every individual
 * operation.
 *
 * Ordinary keys receive only the shared scheduling and owned-resource surface.
 * Internal Pi credentials retain their role-scoped orchestration workflows.
 *
 *   wh_read     — role-scoped read-only inspection
 *   wh_dispatch — role-scoped mutations and Python-capable Marimo operations
 */
import { Type, type Static } from "typebox";
import { Text } from "@earendil-works/pi-tui";
import type { ExtensionAPI, Theme } from "@earendil-works/pi-coding-agent";
import { StringEnum } from "../utils.ts";
import { events } from "../events.ts";
import { gpuAvailability, workerGpuStatus } from "../gpu-status.ts";
import type { GpuModelStatus } from "../gpu-status.ts";
import type { PublicWorker, QueueBoard } from "../types.ts";
import {
  availableGpuWorkers,
  buildDataCatalog,
  formatDataCatalog,
  formatAvailableGpus,
  formatQueueOverview,
  formatOwnedJob,
  formatWorkerOverview,
} from "../agent-output.ts";
import type { DataCatalog } from "../agent-output.ts";
import {
  ApiError,
  addTunnel,
  askPm,
  copyData,
  createMarimo,
  dispatchTask,
  downloadFile,
  enqueueJob,
  getJobLogs,
  getQueueHistory,
  getUsage,
  getCredentialIdentity,
  getMarimo,
  getOrchestratorUrl,
  getPiSession,
  getWorker,
  getWorkerSummary,
  listDataPaths,
  listJobs,
  listMarimo,
  listPiSessions,
  listProjects,
  listQueue,
  listTunnels,
  listWorkers,
  notifyPm,
  pruneWorkers,
  previewQueueMove,
  moveQueueJob,
  removeMarimo,
  removeTunnel,
  sendOrchestratorMessage,
  sendProjectMessage,
  sendSessionMessage,
  startJob,
  stopJob,
  submitPr,
  teardownTask,
  updateQueuedJob,
  updateJob,
  uploadFile,
} from "../api.ts";
import { subscribeJobLogs } from "../sse.ts";
import { ensureKernelSession, executeMarimoCode, hydrateNotebook } from "../marimo.ts";

// ── shared helpers ─────────────────────────────────────

function toToolError(err: unknown): Error {
  if (err instanceof ApiError) {
    return new Error(`${err.message}${err.detail ? ` (${JSON.stringify(err.detail)})` : ""}`);
  }
  return err instanceof Error ? err : new Error(String(err));
}

function requireField<T>(value: T | undefined, name: string): T {
  if (value === undefined || value === null || value === "") {
    throw new Error(`Missing required parameter for action: ${name}`);
  }
  return value;
}

function positiveInteger(value: number | undefined, name: string): number {
  const required = requireField(value, name);
  if (!Number.isInteger(required) || required < 1) throw new Error(`${name} must be a positive integer`);
  return required;
}

function nonblank(value: string | undefined, name: string): string {
  const required = requireField(value, name).trim();
  if (!required) throw new Error(`${name} must not be blank`);
  return required;
}

function validatePriority(params: Static<typeof whDispatchParams>): void {
  if (params.priority !== undefined && !["flexible", "normal", "blocking", "time_critical"].includes(params.priority)) {
    throw new Error("Invalid priority");
  }
  if (params.priority_note !== undefined && params.priority_note.length > 1000) {
    throw new Error("priority_note must be at most 1000 characters");
  }
  if (params.priority === "time_critical" && !params.priority_note?.trim()) {
    throw new Error("time_critical requires a nonblank priority_note");
  }
  if (params.deadline_at !== undefined && params.deadline_at !== null && !Number.isFinite(params.deadline_at)) {
    throw new Error("deadline_at must be epoch seconds or null");
  }
}

/** Run a local command synchronously (used by grant_git_access for gh API). */
async function runLocal(
  cmd: string[],
  opts?: { cwd?: string; timeout?: number },
): Promise<{ stdout: string; stderr: string; returncode: number }> {
  const { execSync } = await import("node:child_process");
  try {
    const stdout = execSync(cmd.join(" "), {
      cwd: opts?.cwd,
      timeout: opts?.timeout ?? 15_000,
      encoding: "utf-8",
      stdio: ["pipe", "pipe", "pipe"],
      maxBuffer: 1024 * 1024,
    });
    return { stdout: stdout.toString(), stderr: "", returncode: 0 };
  } catch (err: any) {
    return {
      stdout: err.stdout?.toString() ?? "",
      stderr: err.stderr?.toString() ?? err.message ?? String(err),
      returncode: err.status ?? 1,
    };
  }
}

/** Parse "owner/repo" from a GitHub remote URL. */
function parseRepoFromRemote(url: string): string | null {
  const sshMatch = url.match(/github\.com[:/]([^/]+)\/([^/\s]+?)(?:\.git)?$/);
  if (sshMatch) return `${sshMatch[1]}/${sshMatch[2]}`;
  return null;
}

function truncate(value: string, max: number): string {
  if (value.length <= max) return value;
  return value.slice(0, max - 1) + "…";
}

// ── call-summary helpers (renderCall) ──────────────────

/** Short, parameterised summary for wh_read calls — shows the action and key args. */
function readCallSummary(args: Static<typeof whReadParams>): string {
  const action = args.action as string | undefined;
  switch (action) {
    case "list_workers":
    case "available_gpus":
    case "list_tunnels":
    case "get_worker_summary":
      return "";
    case "list_queue":
      return args.worker_id ? `worker=${args.worker_id}` : "";
    case "list_data":
      return args.query ? `query=${truncate(args.query, 60)}` : "";
    case "get_worker":
      return args.worker_id ?? "";
    case "list_jobs": {
      const parts: string[] = [];
      if (args.worker_id) parts.push(`worker=${args.worker_id}`);
      if (args.status) parts.push(`status=${args.status}`);
      if (args.origin_session_id) parts.push(`origin=${args.origin_session_id}`);
      return parts.join(" ");
    }
    case "get_job_logs": {
      const parts: string[] = [args.job_id ?? ""];
      if (args.tail !== undefined) parts.push(`tail=${args.tail}`);
      if (args.head !== undefined) parts.push(`head=${args.head}`);
      if (args.follow) parts.push("follow");
      return parts.filter(Boolean).join(" ");
    }
    case "pi_sessions":
      return args.worker_id ?? "";
    case "fleet_status":
    case "list_projects":
      return "";
    default:
      return "";
  }
}

/** Short, parameterised summary for wh_dispatch calls. */
function dispatchCallSummary(args: Static<typeof whDispatchParams>): string {
  const action = args.action as string | undefined;
  switch (action) {
    case "data_copy":
      return `${args.src_worker}:${truncate(args.src_path ?? "", 40)} → ${args.dst_worker}:${truncate(args.dst_path ?? "", 40)}`;
    case "exec": {
      const parts: string[] = [args.worker_id ?? ""];
      if (args.name) parts.push(`name=${args.name}`);
      if (args.sync) parts.push("sync");
      parts.push(`$ ${truncate(args.command ?? "", 60)}`);
      return parts.filter(Boolean).join(" ");
    }
    case "enqueue":
      return [
        args.worker_id ?? "",
        args.name ? `name=${args.name}` : "",
        args.gpu_count !== undefined ? `gpus=${args.gpu_count}` : "gpus=1",
        args.expected_seconds !== undefined ? `expected=${args.expected_seconds}s` : "",
      ].filter(Boolean).join(" ");
    case "update_queued_job":
      return [
        args.job_id ?? "",
        args.worker_id ? `worker=${args.worker_id}` : "",
        args.update_status ? `status=${args.update_status}` : "",
        args.name ? `name=${args.name}` : "",
        args.gpu_count !== undefined ? `gpus=${args.gpu_count}` : "",
        args.expected_seconds !== undefined ? `expected=${args.expected_seconds}s` : "",
      ].filter(Boolean).join(" ");
    case "stop_job":
      return args.job_id ?? "";
    case "add_tunnel":
      return `${args.worker_id} :${args.remote_port}→localhost:${args.local_port}${args.name ? ` (${args.name})` : ""}`;
    case "remove_tunnel":
      return args.tunnel_id ?? "";
    case "list_marimo":
      return args.worker_id ?? "";
    case "get_marimo":
    case "stop_marimo":
    case "execute_marimo":
      return args.marimo_session_id ?? "";
    case "start_marimo":
      return `${args.worker_id}:${truncate(args.notebook_path ?? "", 60)}`;
    case "workers_prune":
      return args.minutes !== undefined ? `${args.minutes}m` : "";
    case "upload_file":
    case "download_file":
      return `${args.worker_id}:${truncate(args.path ?? "", 60)}`;
    case "ask_pm":
      return truncate(args.question ?? "", 60);
    case "notify_pm":
      return truncate(args.note ?? "", 60);
    case "dispatch_task":
      return `${args.branch ?? ""} ${truncate(args.briefing ?? "", 60)}`.trim();
    case "answer":
      return `${args.task_session_id ?? ""} ${truncate(args.text ?? "", 60)}`.trim();
    case "submit_pr":
    case "teardown_task":
      return args.task_session_id ?? "";
    case "escalate":
      return truncate(args.text ?? "", 60);
    case "pm_send":
      return `${args.project ?? ""} ${truncate(args.message ?? "", 60)}`.trim();
    case "grant_git_access":
      return [args.worker_id, args.repo ?? ""].filter(Boolean).join(" ");
    default:
      return "";
  }
}

/** Build a single-line `wh_read  <action>  <summary>` call header. */
function buildReadHeader(args: Static<typeof whReadParams>, theme: Theme): string {
  let text = theme.fg("toolTitle", theme.bold("wh_read")) + " ";
  text += theme.fg("accent", (args.action as string | undefined) ?? "?");
  const summary = readCallSummary(args);
  if (summary) text += theme.fg("toolOutput", `  ${summary}`);
  return text;
}

/** Build a single-line `wh_dispatch  <action>  <summary>` call header. */
function buildDispatchHeader(args: Static<typeof whDispatchParams>, theme: Theme): string {
  let text = theme.fg("toolTitle", theme.bold("wh_dispatch")) + " ";
  text += theme.fg("accent", (args.action as string | undefined) ?? "?");
  const summary = dispatchCallSummary(args);
  if (summary) text += theme.fg("toolOutput", `  ${summary}`);
  return text;
}
function workerStatusEmoji(status: PublicWorker["status"]): string {
  if (status === "online") return "🟢";
  if (status === "draining") return "🟡";
  return "🔴";
}

function gpuStatusColor(status: GpuModelStatus): "success" | "warning" | "error" {
  const availability = gpuAvailability(status);
  if (availability === "free") return "success";
  if (availability === "partial") return "warning";
  return "error";
}

export function buildWorkerList(workers: PublicWorker[], theme: Theme): string {
  if (workers.length === 0) return theme.fg("dim", "No workers");
  return workers.map((worker) => {
    const gpus = workerGpuStatus(worker).map((status) =>
      theme.fg(gpuStatusColor(status), `${status.model}: ${status.busy}/${status.total}`),
    ).join(" · ") || "—";
    return `${workerStatusEmoji(worker.status)} ${theme.fg("toolOutput", worker.name)} | GPUs: ${gpus}`;
  }).join("\n");
}


// ── parameter schemas ──────────────────────────────────

/**
 * wh_read — read-only inspection.
 *
 * Each `action` only uses a subset of the optional fields. The runtime
 * enforces required-for-action fields via `requireField`; the schema keeps
 * all fields optional so Pi can validate the union of all action params
 * without complaining about absent siblings.
 */
const whReadParams = Type.Object({
  action: StringEnum([
    "list_workers",
    "available_gpus",
    "get_worker",
    "get_worker_summary",
    "list_data",
    "list_jobs",
    "list_queue",
    "get_job_logs",
    "list_tunnels",
    "get_usage",
    "get_queue_history",
    "pi_sessions",
    "fleet_status",
    "list_projects",
  ]),
  worker_id: Type.Optional(Type.String({ description: "Worker ID or name" })),
  query: Type.Optional(
    Type.String({ description: "Case-insensitive substring filter for list_data paths" }),
  ),
  job_id: Type.Optional(Type.String({ description: "Job ID" })),
  user_id: Type.Optional(Type.String({ description: "Usage owner user ID" })),
  before_event_id: Type.Optional(Type.String({ description: "Queue history pagination cursor" })),
  limit: Type.Optional(Type.Number({ minimum: 1, maximum: 200, description: "Queue history page size" })),
  origin_session_id: Type.Optional(
    Type.String({ description: "Child Pi session ID that originated the job" }),
  ),
  status: Type.Optional(StringEnum(["pending", "starting", "running", "done", "failed"])),
  tail: Type.Optional(Type.Number({ description: "Last N log lines. Default 10" })),
  head: Type.Optional(
    Type.Number({ description: "First N log lines. Mutually exclusive with tail" }),
  ),
  follow: Type.Optional(Type.Boolean({ description: "Stream logs in real time" })),
});

/**
 * wh_dispatch — mutations.
 *
 * Each `action` only uses a subset of the optional fields. Required-for-action
 * fields are enforced at runtime via `requireField`.
 */
const whDispatchParams = Type.Object({
  action: StringEnum([
    "data_copy",
    "exec",
    "stop_job",
    "enqueue",
    "update_queued_job",
    "preview_queue_move",
    "move_queued_job",
    "add_tunnel",
    "remove_tunnel",
    "workers_prune",
    "upload_file",
    "download_file",
    "grant_git_access",
    "list_marimo",
    "get_marimo",
    "start_marimo",
    "stop_marimo",
    "execute_marimo",
    "ask_pm",
    "notify_pm",
    "dispatch_task",
    "answer",
    "submit_pr",
    "teardown_task",
    "escalate",
    "pm_send",
  ]),
  // data_copy
  src_worker: Type.Optional(Type.String({ description: "Source worker ID or name for data_copy" })),
  src_path: Type.Optional(Type.String({ description: "Advertised absolute source path for data_copy" })),
  dst_worker: Type.Optional(Type.String({ description: "Destination worker ID or name for data_copy" })),
  dst_path: Type.Optional(Type.String({ description: "Absolute destination path for data_copy" })),
  ttl_seconds: Type.Optional(Type.Number({ description: "Temporary source export TTL in seconds (default 21600)" })),
  // exec
  worker_id: Type.Optional(Type.String({ description: "Worker ID or name" })),
  command: Type.Optional(Type.String({
    description:
      "Shell command to execute on the worker. Workers have uv, build-essential, cmake, ninja and the full CUDA toolkit (nvcc 12.6, cudnn, headers) pre-installed and on PATH. " +
      "For Python deps: create a venv with `uv venv $WH_DIR/harness/<name> && source $WH_DIR/harness/<name>/bin/activate && uv pip install <pkgs>`. " +
      "For extra CUDA libraries (cuBLAS, cuDNN, NCCL, …) install via `uv pip install nvidia-cublas-cu12 nvidia-cudnn-cu12 nvidia-nccl-cu12 …`. " +
      "Use /code for repos, /data/shared for fleet-shared storage, /data/local for worker-local storage, and ~ for home files.",
  })),
  name: Type.Optional(Type.String({ description: "Job or service label" })),
  no_pty: Type.Optional(Type.Boolean({ description: "Disable PTY" })),
  sync: Type.Optional(Type.Boolean({
    description: "Block until command finishes, return stdout (exec action). Default: false",
  })),
  sync_timeout: Type.Optional(Type.Number({ description: "Timeout in seconds for sync mode (default 120)" })),
  expected_seconds: Type.Optional(Type.Integer({
    minimum: 1,
    description: "Required positive integer duration for exec/enqueue; scheduling information only",
  })),
  gpu_count: Type.Optional(Type.Integer({
    minimum: 1,
    description: "Required positive integer GPUs for enqueue; forbidden for CPU-only exec",
  })),
  priority: Type.Optional(StringEnum(["flexible", "normal", "blocking", "time_critical"])),
  priority_note: Type.Optional(Type.String({ maxLength: 1000, description: "Required nonblank note for time_critical priority" })),
  deadline_at: Type.Optional(Type.Union([Type.Number(), Type.Null()])),
  update_status: Type.Optional({
    ...StringEnum(["pending", "active"]),
    description: "Required for update_queued_job: pending updates queue metadata; active updates starting/running duration, priority, note, deadline only. Never reorders.",
  }),
  before_job_id: Type.Optional(Type.Union([Type.String(), Type.Null()])),
  expected_revision: Type.Optional(Type.Integer({ minimum: 0, description: "Queue revision from a freshly fetched board" })),
  reason: Type.Optional(Type.String({ minLength: 1, maxLength: 1000, description: "Human reason for previewing a queue move" })),
  preview_id: Type.Optional(Type.String({ description: "Preview to commit; after 409 re-fetch queue and re-preview, never blind-retry" })),
  // stop_job / remove_tunnel
  job_id: Type.Optional(Type.String({ description: "Job ID" })),
  tunnel_id: Type.Optional(Type.String({ description: "Tunnel ID" })),
  // add_tunnel
  local_port: Type.Optional(Type.Number({ minimum: 1024, maximum: 65535, description: "Raw TCP listener port on trusted Tailnet. Target service must authenticate." })),
  remote_port: Type.Optional(Type.Number({ minimum: 1, maximum: 65535, description: "Remote worker service port; authentication is the service owner's responsibility" })),
  // workers_prune
  minutes: Type.Optional(
    Type.Number({ description: "Stale worker threshold in minutes (default 5)" }),
  ),
  // upload_file / download_file
  path: Type.Optional(Type.String({ description: "Remote file path on worker (for file transfer)" })),
  content_b64: Type.Optional(Type.String({ description: "Base64-encoded file content (for upload_file)" })),
  max_bytes: Type.Optional(Type.Number({ description: "Max bytes to download (default 10MB)" })),
  // Hierarchical agent fleet
  question: Type.Optional(Type.String({ description: "Question for this task's project manager" })),
  note: Type.Optional(Type.String({ description: "Non-blocking note for this task's project manager" })),
  branch: Type.Optional(Type.String({ description: "Requested task branch name" })),
  briefing: Type.Optional(Type.String({ description: "Complete task briefing" })),
  task_session_id: Type.Optional(Type.String({ description: "Task session ID owned by this project manager" })),
  text: Type.Optional(Type.String({ description: "Message text" })),
  summary: Type.Optional(Type.String({ description: "Six-section pull request summary" })),
  force: Type.Optional(Type.Boolean({ description: "Allow teardown before a pull request exists" })),
  project: Type.Optional(Type.String({ description: "Configured project name" })),
  message: Type.Optional(Type.String({ description: "Message for the project manager" })),
  // grant_git_access
  repo: Type.Optional(Type.String({
    description: 'GitHub repo in "owner/repo" format. If omitted, detected from the current directory\'s git remote.',
  })),
  // managed Marimo lifecycle and direct kernel execution
  marimo_session_id: Type.Optional(Type.String({ description: "Managed Marimo session ID" })),
  notebook_path: Type.Optional(Type.String({ description: "Absolute notebook path on the worker" })),
  environment: Type.Optional(Type.String({ description: "Absolute Python environment path on the worker" })),
  ready_timeout: Type.Optional(Type.Number({
    minimum: 1,
    maximum: 300,
    description: "Marimo server readiness timeout in seconds (1–300)",
  })),
  code: Type.Optional(Type.String({ description: "Python code to execute in the notebook kernel" })),
});

// ── registration ───────────────────────────────────────
// OMP consumes loadMode; standard Pi safely ignores the extra runtime property.
// Spreading it avoids requiring standard Pi's ToolDefinition type to declare it.
const essentialToolPresentation = { loadMode: "essential" as const };


export function registerGroupedTools(
  pi: ExtensionAPI,
) {
  const role = process.env.WH_SESSION_ROLE?.trim() ?? "";
  let readActions: string[];
  let dispatchActions: string[];
  let readDescription: string;
  let dispatchDescription: string;

  const computeReadActions = [
    "list_workers",
    "available_gpus",
    "get_worker",
    "get_worker_summary",
    "list_data",
    "list_jobs",
    "list_queue",
    "get_job_logs",
    "get_usage",
    "get_queue_history",
  ];
  const computeDispatchActions = [
    "data_copy",
    "exec",
    "stop_job",
    "enqueue",
    "update_queued_job",
    "preview_queue_move",
    "move_queued_job",
    "list_marimo",
    "get_marimo",
    "start_marimo",
    "stop_marimo",
    "execute_marimo",
  ];

  if (role === "orchestrator") {
    readActions = ["fleet_status", "list_projects"];
    dispatchActions = ["pm_send"];
    readDescription =
      "Inspect the hierarchical agent fleet or configured projects. The orchestrator holds no project detail and routes project work to one project manager.";
    dispatchDescription =
      "Send work to a project's manager; the orchestrator never edits project files or uses worker compute directly.";
  } else if (role === "pm") {
    readActions = [...computeReadActions, "pi_sessions"];
    dispatchActions = [
      ...computeDispatchActions,
      "dispatch_task",
      "answer",
      "submit_pr",
      "teardown_task",
      "escalate",
    ];
    readDescription =
      "Inspect worker compute, data, jobs, managed Marimo sessions, and fleet sessions needed to manage this project's task agents.";
    dispatchDescription =
      "Run compute and managed Marimo sessions or manage task agents for this project. The project manager must review the worktree diff before submit_pr, and escalates cross-project or operator decisions.";
  } else if (role === "task") {
    readActions = computeReadActions;
    dispatchActions = [...computeDispatchActions, "ask_pm", "notify_pm"];
    readDescription =
      "Inspect worker compute, data, jobs, and the shared queue needed to complete the assigned task.";
    dispatchDescription =
      "Run compute or managed Marimo sessions, or contact this task's project manager. A task agent works only in its allocated worktree, never pushes, and never opens a pull request.";
  } else if (role === "") {
    readActions = [...computeReadActions, "list_tunnels"];
    dispatchActions = [
      "exec", "enqueue", "update_queued_job", "preview_queue_move", "move_queued_job",
      "stop_job", "add_tunnel", "remove_tunnel",
      "list_marimo", "get_marimo", "start_marimo", "stop_marimo", "execute_marimo",
    ];
    readDescription =
      "Token-efficient read-only Worker Harness inspection. Inspect list_queue before scheduling GPU work and use the narrowest action.";
    dispatchDescription =
      "Owner-scoped CPU jobs, scheduled GPU work, queue moves, tunnels and managed Marimo. Raw TCP forwards are reachable by anyone on the trusted Tailnet: the target service must authenticate. Use wh_read for inspection; never invoke the Worker Harness CLI/API directly.";
  } else {
    console.warn(`[pi-worker-harness] Unknown WH_SESSION_ROLE ${JSON.stringify(role)}; grouped tools disabled`);
    return;
  }

  const readPromptSnippet = role === "orchestrator"
    ? "wh_read: inspect fleet_status or list_projects before routing work to a project manager."
    : "wh_read (RO): inspect list_queue before scheduling GPU work; use available_gpus for telemetry availability; list_workers only for fleet overview; get_worker only for one chosen worker; list_data(query) for shallow /data/shared, /data/local, and /code discovery. Filter list_jobs/logs. Never invoke the wh CLI/API directly.";
  const dispatchPromptSnippet = role === "orchestrator"
    ? "wh_dispatch: use pm_send to route a request to exactly one configured project's manager."
    : "wh_dispatch: inspect wh_read list_queue before enqueueing GPU work. exec is CPU/setup only (no gpu_count); both require name and expected_seconds, and enqueue requires positive gpu_count. Queue moves require preview then commit; after any 409 re-fetch and re-preview, never blind-retry. Priority/deadline never reorder. Raw TCP tunnels are reachable by anyone on the trusted Tailnet; target-service authentication is your responsibility. Use wh_dispatch for all five managed Marimo actions. Never invoke the wh CLI/API directly.";

  const readProperties = { ...whReadParams.properties };
  if (role === "") delete (readProperties as Record<string, unknown>).origin_session_id;
  const dispatchProperties = { ...whDispatchParams.properties };
  if (role === "") {
    for (const field of [
      "src_worker", "src_path", "dst_worker", "dst_path", "ttl_seconds",
      "minutes", "path", "content_b64", "max_bytes", "question", "note", "branch",
      "briefing", "task_session_id", "text", "summary", "force", "project", "message", "repo",
    ]) delete (dispatchProperties as Record<string, unknown>)[field];
  }
  const roleReadParams = Type.Object({
    ...readProperties,
    action: StringEnum(readActions),
  }, { additionalProperties: false });
  const roleDispatchParams = Type.Object({
    ...dispatchProperties,
    action: StringEnum(dispatchActions),
  }, { additionalProperties: false });

  pi.registerTool({
    name: "wh_read",
    label: "Worker Harness Read",
    ...essentialToolPresentation,
    description: readDescription,
    promptSnippet: readPromptSnippet,
    parameters: roleReadParams,
    renderCall(args, theme, context) {
      const text = (context.lastComponent as Text | undefined) ?? new Text("", 0, 0);
      text.setText(buildReadHeader(args, theme));
      return text;
    },
    renderResult(result, _options, theme) {
      const details = result.details as {
        workers?: PublicWorker[];
        queue?: QueueBoard;
        data?: DataCatalog;
      } | undefined;
      if (details?.queue) {
        return new Text(formatQueueOverview(details.queue), 0, 0);
      }
      if (Array.isArray(details?.workers)) {
        return new Text(buildWorkerList(details.workers, theme), 0, 0);
      }
      if (details?.data) {
        return new Text(formatDataCatalog(details.data), 0, 0);
      }
      const text = result.content.find((item) => item.type === "text");
      return new Text(text?.type === "text" ? text.text : "", 0, 0);
    },
    async execute(_toolCallId, params, signal, onUpdate) {
      const {
        action,
        worker_id,
        job_id,
        origin_session_id,
        status,
        query,
        tail,
        head,
        follow,
      } = params;
      try {
        if (typeof action !== "string" || !readActions.includes(action)) throw new Error(`Action ${action} is not available for role ${role || "operator"}`);
        switch (action) {
          case "list_workers": {
            const workers = await listWorkers();
            return {
              content: [{ type: "text", text: formatWorkerOverview(workers) }],
              details: { workers },
            };
          }
          case "available_gpus": {
            const workers = await listWorkers();
            return {
              content: [{ type: "text", text: formatAvailableGpus(workers) }],
              details: { workers: availableGpuWorkers(workers) },
            };
          }
          case "get_worker": {
            const worker = await getWorker(requireField(worker_id, "worker_id"));
            return {
              content: [{ type: "text", text: JSON.stringify(worker, null, 2) }],
              details: { worker },
            };
          }
          case "get_worker_summary": {
            const summary = await getWorkerSummary();
            return {
              content: [{ type: "text", text: JSON.stringify(summary, null, 2) }],
              details: { summary },
            };
          }
          case "list_data": {
            const catalog = buildDataCatalog(await listDataPaths(), query);
            return {
              content: [{ type: "text", text: JSON.stringify(catalog) }],
              details: { data: catalog, query: query ?? "" },
            };
          }
          case "list_jobs": {
            if (role === "" && origin_session_id !== undefined) throw new Error("origin_session_id is internal-only");
            const jobs = await listJobs({ worker_id, status, origin_session_id });
            return {
              content: [{ type: "text", text: JSON.stringify(jobs, null, 2) }],
              details: { jobs },
            };
          }
          case "list_queue": {
            const queue = await listQueue(worker_id);
            return {
              content: [{ type: "text", text: formatQueueOverview(queue) }],
              details: { queue },
            };
          }
          case "get_usage": {
            const usage = await getUsage({ worker_id, user_id: params.user_id });
            return {
              content: [{ type: "text", text: `${JSON.stringify(usage, null, 2)}\nAllocated wall/GPU time, not utilization or quota.` }],
              details: { usage },
            };
          }
          case "get_queue_history": {
            const history = await getQueueHistory({
              worker_id, job_id, before_event_id: params.before_event_id, limit: params.limit,
            });
            return {
              content: [{ type: "text", text: JSON.stringify(history, null, 2) }],
              details: { history },
            };
          }
          case "get_job_logs": {
            const id = requireField(job_id, "job_id");
            const credentialIdentity = getCredentialIdentity();
            if (follow) {
              if (head !== undefined) {
                throw new Error("head is not supported in follow mode");
              }

              const lines: string[] = [];
              const timeoutController = new AbortController();
              const timeout = setTimeout(() => timeoutController.abort(), 60_000);

              let abortListener: (() => void) | null = null;
              if (signal) {
                const onAbort = () => timeoutController.abort();
                signal.addEventListener("abort", onAbort, { once: true });
                abortListener = () => signal.removeEventListener("abort", onAbort);
              }

              const sub = subscribeJobLogs(id, {
                tail: tail ?? 50,
                pollSeconds: 1,
                onLine: (line) => {
                  if (getCredentialIdentity() !== credentialIdentity) {
                    timeoutController.abort();
                    sub.stop();
                    return;
                  }
                  lines.push(line);
                  onUpdate?.({ content: [{ type: "text", text: line + "\n" }] });
                  if (line.trim()) {
                    events.emit("worker-harness:log-line", { job_id: id, line, credentialIdentity });
                  }
                },
              });

              try {
                await sub.done;
              } catch (err) {
                const isAbortError =
                  (err instanceof Error && err.name === "AbortError") ||
                  (!!err &&
                    typeof err === "object" &&
                    "name" in err &&
                    (err as { name?: string }).name === "AbortError");
                if (!isAbortError) throw err;
              } finally {
                clearTimeout(timeout);
                abortListener?.();
                sub.stop();
              }

              if (getCredentialIdentity() !== credentialIdentity) throw new Error("Credentials changed while following job logs; discarded old identity output");
              return {
                content: [
                  {
                    type: "text",
                    text: (lines.length > 0 ? lines.join("\n") + "\n" : "") + "[Follow mode ended]",
                  },
                ],
                details: { lines },
              };
            }

            const logs = await getJobLogs(id, { tail, head });
            if (getCredentialIdentity() !== credentialIdentity) throw new Error("Credentials changed while fetching job logs; discarded old identity output");
            const text = typeof logs.logs === "string" ? logs.logs : JSON.stringify(logs, null, 2);
            return {
              content: [{ type: "text", text }],
              details: logs,
            };
          }
          case "list_tunnels": {
            const tunnels = await listTunnels();
            return {
              content: [{ type: "text", text: JSON.stringify(tunnels, null, 2) }],
              details: { tunnels },
            };
          }
          case "pi_sessions":
          case "fleet_status": {
            const sessions = await listPiSessions(worker_id);
            return {
              content: [{ type: "text", text: JSON.stringify(sessions, null, 2) }],
              details: { sessions },
            };
          }
          case "list_projects": {
            const projects = await listProjects();
            return {
              content: [{ type: "text", text: JSON.stringify(projects, null, 2) }],
              details: { projects },
            };
          }
          default:
            throw new Error(`Unknown action: ${String(action)}`);
        }
      } catch (err) {
        throw toToolError(err);
      }
    },
  });

  pi.registerTool({
    name: "wh_dispatch",
    label: "Worker Harness Dispatch",
    ...essentialToolPresentation,
    description: dispatchDescription,
    promptSnippet: dispatchPromptSnippet,
    parameters: roleDispatchParams,
    renderCall(args, theme, context) {
      const text = (context.lastComponent as Text | undefined) ?? new Text("", 0, 0);
      text.setText(buildDispatchHeader(args, theme));
      return text;
    },
    async execute(_toolCallId, params, signal, onUpdate) {
      const {
        action,
        worker_id,
        src_worker,
        src_path,
        dst_worker,
        dst_path,
        ttl_seconds,
        command,
        name,
        no_pty,
        job_id,
        tunnel_id,
        local_port,
        remote_port,
        minutes,
        path,
        content_b64,
        max_bytes,
        sync,
        sync_timeout,
        expected_seconds,
        gpu_count,
        priority,
        priority_note,
        deadline_at,
        update_status,
        before_job_id,
        expected_revision,
        reason,
        preview_id,
        repo,
        marimo_session_id,
        notebook_path,
        environment,
        ready_timeout,
        code,
        question,
        note,
        branch,
        briefing,
        task_session_id,
        text,
        summary,
        force,
        project,
        message,
      } = params;

      try {
        if (typeof action !== "string" || !dispatchActions.includes(action)) throw new Error(`Action ${action} is not available for role ${role || "operator"}`);
        switch (action) {
          case "data_copy": {
            const result = await copyData({
              src_worker: requireField(src_worker, "src_worker"),
              src_path: requireField(src_path, "src_path"),
              dst_worker: requireField(dst_worker, "dst_worker"),
              dst_path: requireField(dst_path, "dst_path"),
              ttl_seconds,
            });
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [{
                type: "text",
                text: `Data copy started: ${result.job_id} (${result.source_worker}:${result.source_path} → ${result.destination_worker}:${result.destination_path})`,
              }],
              details: result,
            };
          }
          case "exec": {
            if (gpu_count !== undefined) throw new Error("exec is CPU-only; gpu_count is not allowed. Use enqueue for GPU work.");
            validatePriority(params);
            const resolvedWorkerId = requireField(worker_id, "worker_id");
            const resolvedCommand = requireField(command, "command");
            const credentialIdentity = getCredentialIdentity();
            const job = await startJob({
              worker_id: resolvedWorkerId,
              command: resolvedCommand,
              name: nonblank(name, "name"),
              expected_seconds: positiveInteger(expected_seconds, "expected_seconds"),
              priority, priority_note, deadline_at,
              no_pty,
              sync,
              sync_timeout,
            });
            if (getCredentialIdentity() !== credentialIdentity) throw new Error("Credentials changed while starting the job; discarded old identity output");
            events.emit("worker-harness:job-started", { job, credentialIdentity });
            events.emit("worker-harness:refresh", undefined);

            // Sync mode: return the command output directly
            if (sync && job.stdout !== undefined) {
              const text =
                job.status === "done"
                  ? `${job.stdout || "(no output)"}\n${formatOwnedJob(job)}`
                  : `[${job.status}] ${job.stdout}\n${formatOwnedJob(job)}`.trim();
              return {
                content: [{ type: "text", text }],
                details: { job },
              };
            }

            return {
              content: [
                {
                  type: "text",
                  text: `Job started: ${formatOwnedJob(job)}`,
                },
              ],
              details: { job },
            };
          }
          case "enqueue": {
            validatePriority(params);
            const job = await enqueueJob({
              worker_id: requireField(worker_id, "worker_id"),
              command: requireField(command, "command"),
              name: nonblank(name, "name"),
              expected_seconds: positiveInteger(expected_seconds, "expected_seconds"),
              gpu_count: positiveInteger(gpu_count, "gpu_count"),
              priority, priority_note, deadline_at,
              no_pty,
            });
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [{
                type: "text",
                text: `Job queued: ${formatOwnedJob(job)}`,
              }],
              details: { job },
            };
          }
          case "update_queued_job": {
            const resolvedJobId = requireField(job_id, "job_id");
            if ("position" in params) throw new Error("position is not supported; use preview_queue_move then move_queued_job");
            if (update_status !== "pending" && update_status !== "active") throw new Error("update_queued_job requires update_status pending or active");
            validatePriority(params);
            if (expected_seconds !== undefined) positiveInteger(expected_seconds, "expected_seconds");
            if (gpu_count !== undefined) positiveInteger(gpu_count, "gpu_count");
            if (name !== undefined) nonblank(name, "name");
            if (update_status === "active" && (worker_id !== undefined || name !== undefined || gpu_count !== undefined)) {
              throw new Error("Active updates allow only expected_seconds, priority, priority_note and deadline_at");
            }
            if ([worker_id, name, expected_seconds, gpu_count, priority, priority_note, deadline_at].every((value) => value === undefined)) {
              throw new Error("update_queued_job requires at least one metadata field");
            }
            const metadata = { expected_seconds, priority, priority_note, deadline_at };
            const job = update_status === "active"
              ? await updateJob(resolvedJobId, metadata)
              : await updateQueuedJob(resolvedJobId, { ...metadata, worker_id, name, gpu_count });
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [{
                type: "text",
                text: `Job updated: ${formatOwnedJob(job)}`,
              }],
              details: { job },
            };
          }
          case "preview_queue_move": {
            const revision = requireField(expected_revision, "expected_revision");
            if (!Number.isInteger(revision) || revision < 0) throw new Error("expected_revision must be a nonnegative integer");
            if (before_job_id === undefined) throw new Error("before_job_id is required; use null to move to the end");
            const trimmedReason = nonblank(reason, "reason");
            if (trimmedReason.length > 1000) throw new Error("reason must be at most 1000 characters");
            const preview = await previewQueueMove(requireField(job_id, "job_id"), {
              before_job_id, expected_revision: revision, reason: trimmedReason,
            });
            return {
              content: [{ type: "text", text: JSON.stringify(preview, null, 2) }],
              details: { preview },
            };
          }
          case "move_queued_job": {
            const revision = requireField(expected_revision, "expected_revision");
            if (!Number.isInteger(revision) || revision < 0) throw new Error("expected_revision must be a nonnegative integer");
            const result = await moveQueueJob(requireField(job_id, "job_id"), {
              preview_id: nonblank(preview_id, "preview_id"), expected_revision: revision,
            });
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
              details: result,
            };
          }
          case "stop_job": {
            const resolvedJobId = requireField(job_id, "job_id");
            const result = await stopJob(resolvedJobId);
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [
                {
                  type: "text",
                  text: result.stopped
                    ? `Job stopped: ${resolvedJobId} | status=${result.status} | execution_state=${result.execution_state}`
                    : `Failed to stop job: ${resolvedJobId}`,
                },
              ],
              details: result,
            };
          }
          case "add_tunnel": {
            const resolvedWorkerId = requireField(worker_id, "worker_id");
            const resolvedLocalPort = requireField(local_port, "local_port");
            const resolvedRemotePort = requireField(remote_port, "remote_port");
            const tunnel = await addTunnel({
              worker_id: resolvedWorkerId,
              local_port: resolvedLocalPort,
              remote_port: resolvedRemotePort,
              name,
            });
            events.emit("worker-harness:refresh", undefined);
            const orchestratorUrl = getOrchestratorUrl();
            const orchestratorHost = (() => {
              try {
                return new URL(orchestratorUrl).hostname;
              } catch {
                return orchestratorUrl;
              }
            })();
            return {
              content: [
                {
                  type: "text",
                  text:
                    `Tunnel started: ${tunnel.bind_host}:${tunnel.local_port} → ${tunnel.worker_name ?? tunnel.worker_id}:${tunnel.remote_port}\n` +
                    `Tunnel ID: ${tunnel.id}\n` +
                    `Service: ${tunnel.service_name}\n` +
                    `Access: ${orchestratorHost}:${tunnel.local_port}\nRaw TCP is reachable by anyone on the trusted Tailnet. The target service must authenticate; this is your responsibility.`,
                },
              ],
              details: { tunnel, access: { orchestrator_url: orchestratorUrl, host: orchestratorHost, port: tunnel.local_port } },
            };
          }
          case "remove_tunnel": {
            const resolvedTunnelId = requireField(tunnel_id, "tunnel_id");
            const result = await removeTunnel(resolvedTunnelId);
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [
                {
                  type: "text",
                  text: result.removed
                    ? `Tunnel removed: ${resolvedTunnelId}`
                    : `Failed to remove tunnel: ${resolvedTunnelId}`,
                },
              ],
              details: result,
            };
          }
          case "list_marimo": {
            const sessions = await listMarimo(worker_id);
            return {
              content: [{ type: "text", text: JSON.stringify(sessions, null, 2) }],
              details: { sessions },
            };
          }
          case "get_marimo": {
            const session = await getMarimo(requireField(marimo_session_id, "marimo_session_id"));
            return {
              content: [{ type: "text", text: JSON.stringify(session, null, 2) }],
              details: { session },
            };
          }
          case "start_marimo": {
            const session = await createMarimo({
              worker_id: requireField(worker_id, "worker_id"),
              notebook_path: requireField(notebook_path, "notebook_path"),
              environment: requireField(environment, "environment"),
              ...(ready_timeout === undefined ? {} : { ready_timeout }),
            });
            events.emit("worker-harness:refresh", undefined);
            // Create the kernel and run the saved notebook, so code executes
            // immediately and the first browser to open the URL sees outputs.
            let kernelSessionId: string | null = null;
            let kernelError: string | null = null;
            let cellsRun: number | null = null;
            try {
              kernelSessionId = (await ensureKernelSession(session, { signal })).id;
              cellsRun = await hydrateNotebook(session, { signal });
            } catch (err) {
              kernelError = err instanceof Error ? err.message : String(err);
            }
            const notice = kernelError !== null
              ? `\nKernel warm-up failed: ${kernelError}\nexecute_marimo will retry creating a kernel.`
              : `\nKernel ready (${kernelSessionId}), ${cellsRun} saved cell(s) run. ` +
                "execute_marimo works now, and the first browser to open the URL resumes this kernel.";
            return {
              content: [{ type: "text", text: `${JSON.stringify(session, null, 2)}${notice}` }],
              details: { session, kernel_session_id: kernelSessionId, cells_run: cellsRun, kernel_error: kernelError },
            };
          }
          case "stop_marimo": {
            const result = await removeMarimo(requireField(marimo_session_id, "marimo_session_id"));
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
              details: result,
            };
          }
          case "execute_marimo": {
            const sessionId = requireField(marimo_session_id, "marimo_session_id");
            const session = await getMarimo(sessionId);
            const result = await executeMarimoCode(
              session,
              requireField(code, "code"),
              {
                signal,
                onStdout: (text) => onUpdate?.({ content: [{ type: "text", text }] }),
                onStderr: (text) => onUpdate?.({
                  content: [{ type: "text", text: `[stderr] ${text}` }],
                }),
              },
            );
            const sections: string[] = [];
            if (result.kernel_created) {
              sections.push(
                `Created a new kernel (${result.kernel_session_id}); its namespace started empty.`,
              );
            }
            if (result.stdout) sections.push(result.stdout);
            if (result.stderr) sections.push(`[stderr]\n${result.stderr}`);
            if (result.output !== undefined) {
              sections.push(
                `Output:\n${typeof result.output === "string"
                  ? result.output
                  : JSON.stringify(result.output, null, 2)}`,
              );
            }
            return {
              content: [{
                type: "text",
                text: sections.length > 0 ? sections.join("\n") : "Marimo execution completed successfully.",
              }],
              details: result,
            };
          }
          case "workers_prune": {
            const result = await pruneWorkers(minutes);
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
              details: result,
            };
          }
          case "upload_file": {
            const resolvedWorkerId = requireField(worker_id, "worker_id");
            const resolvedPath = requireField(path, "path");
            const resolvedContent = requireField(content_b64, "content_b64");
            const result = await uploadFile(resolvedWorkerId, resolvedPath, resolvedContent);
            return {
              content: [
                {
                  type: "text",
                  text: `Uploaded ${result.size} bytes to ${result.path} on ${resolvedWorkerId}`,
                },
              ],
              details: result,
            };
          }
          case "download_file": {
            const resolvedWorkerId = requireField(worker_id, "worker_id");
            const resolvedPath = requireField(path, "path");
            const result = await downloadFile(resolvedWorkerId, resolvedPath, max_bytes);
            return {
              content: [
                {
                  type: "text",
                  text: `Downloaded ${result.size} bytes from ${result.path} on ${resolvedWorkerId}`,
                },
              ],
              details: result,
            };
          }
          case "ask_pm": {
            const sessionId = requireField(process.env.WH_SESSION_ID, "WH_SESSION_ID");
            const result = await askPm(sessionId, requireField(question, "question"));
            return {
              content: [{ type: "text", text: "Question queued for the project manager; this task is now blocked." }],
              details: result,
            };
          }
          case "notify_pm": {
            const sessionId = requireField(process.env.WH_SESSION_ID, "WH_SESSION_ID");
            const result = await notifyPm(sessionId, requireField(note, "note"));
            return {
              content: [{ type: "text", text: "Project manager notified." }],
              details: result,
            };
          }
          case "dispatch_task": {
            const sessionId = requireField(process.env.WH_SESSION_ID, "WH_SESSION_ID");
            const session = await getPiSession(sessionId);
            const projectName = requireField(
              typeof session.meta.project === "string" ? session.meta.project : undefined,
              "meta.project",
            );
            const taskSession = await dispatchTask(
              projectName,
              requireField(branch, "branch"),
              requireField(briefing, "briefing"),
            );
            return {
              content: [{ type: "text", text: `Task launched: ${taskSession.id}` }],
              details: { session: taskSession },
            };
          }
          case "answer": {
            const result = await sendSessionMessage(
              requireField(task_session_id, "task_session_id"),
              requireField(text, "text"),
            );
            return {
              content: [{ type: "text", text: "Answer sent to task agent." }],
              details: result,
            };
          }
          case "submit_pr": {
            const result = await submitPr(
              requireField(task_session_id, "task_session_id"),
              requireField(summary, "summary"),
            );
            return {
              content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
              details: result,
            };
          }
          case "teardown_task": {
            const result = await teardownTask(
              requireField(task_session_id, "task_session_id"),
              force,
            );
            return {
              content: [{ type: "text", text: "Task agent torn down." }],
              details: result,
            };
          }
          case "escalate": {
            const result = await sendOrchestratorMessage(requireField(text, "text"));
            return {
              content: [{ type: "text", text: "Escalation sent to the orchestrator." }],
              details: result,
            };
          }
          case "pm_send": {
            const result = await sendProjectMessage(
              requireField(project, "project"),
              requireField(message, "message"),
            );
            return {
              content: [{ type: "text", text: "Message sent to the project manager." }],
              details: result,
            };
          }
          case "grant_git_access": {
            const resolvedWorkerId = requireField(worker_id, "worker_id");

            // ── 1. Resolve repo ──────────────────────────────────────
            let repoFull = repo?.trim() ?? "";
            if (!repoFull) {
              const gitResult = await runLocal(["git", "remote", "get-url", "origin"]);
              if (gitResult.returncode !== 0) {
                throw new Error(
                  "Could not detect git repo from current directory. " +
                    "Set the `repo` parameter explicitly (e.g. 'owner/repo-name'). " +
                    `git error: ${gitResult.stderr}`,
                );
              }
              repoFull = parseRepoFromRemote(gitResult.stdout.trim()) ?? "";
              if (!repoFull) {
                throw new Error(
                  `Could not parse GitHub owner/repo from remote URL: ${gitResult.stdout.trim()}. ` +
                    "Set the `repo` parameter explicitly.",
                );
              }
            }

            // ── 2. Get worker info ────────────────────────────────────
            const worker = await getWorker(resolvedWorkerId);
            const workerName = (worker as any).name ?? resolvedWorkerId;

            // ── 3. Generate SSH key on worker (idempotent) ────────────
            const keygenCmd =
              'mkdir -p ~/.ssh && chmod 700 ~/.ssh && ' +
              'if [ ! -f ~/.ssh/wh_deploy_key ]; then ' +
              'ssh-keygen -t ed25519 -f ~/.ssh/wh_deploy_key -N "" -C "wh-' + workerName + '" 2>/dev/null; ' +
              'fi && cat ~/.ssh/wh_deploy_key.pub';

            const execResult = await startJob({
              worker_id: resolvedWorkerId,
              command: keygenCmd,
              sync: true,
              sync_timeout: 30,
              no_pty: true,
              name: "ssh-keygen",
            });

            if (execResult.status === "failed") {
              throw new Error(
                `Failed to generate SSH key on worker ${workerName}: ${execResult.stdout || execResult.exit_code}`,
              );
            }

            const pubkey = (execResult.stdout || "").trim();
            if (!pubkey || !pubkey.startsWith("ssh-")) {
              throw new Error(
                `Worker returned unexpected public key output: ${execResult.stdout}`,
              );
            }

            // ── 4. Check if deploy key already exists ────────────────
            const listResult = await runLocal([
              "gh", "api", `repos/${repoFull}/keys`,
            ]);

            if (listResult.returncode === 0) {
              try {
                const keys = JSON.parse(listResult.stdout);
                const pubkeyMaterial = pubkey.split(/\s+/).slice(0, 2).join(" ");
                const existing = keys.find((k: any) => {
                  const existingMaterial = (k.key || "").split(/\s+/).slice(0, 2).join(" ");
                  return existingMaterial === pubkeyMaterial;
                });
                if (existing) {
                  return {
                    content: [
                      {
                        type: "text",
                        text:
                          `Worker ${workerName} already has read access to ${repoFull}.\n` +
                          `Deploy key "${existing.title}" (id: ${existing.id}) is registered.\n` +
                          `Clone on worker: git clone git@github.com:${repoFull}.git`,
                      },
                    ],
                    details: {
                      repo: repoFull,
                      worker_id: resolvedWorkerId,
                      worker_name: workerName,
                      key_id: existing.id,
                      key_title: existing.title,
                      already_registered: true,
                    },
                  };
                }
              } catch {
                // Parse error — proceed to add the key
              }
            }

            // ── 5. Register deploy key ────────────────────────────────
            const title = `wh-${workerName}`;
            const addResult = await runLocal([
              "gh", "api", `repos/${repoFull}/keys`,
              "-f", `title=${title}`,
              "-f", `key=${pubkey}`,
              "-f", "read_only=true",
            ]);

            if (addResult.returncode !== 0) {
              throw new Error(
                `Failed to register deploy key via gh api: ${addResult.stderr}`,
              );
            }

            let keyId: number | undefined;
            try {
              keyId = JSON.parse(addResult.stdout).id;
            } catch {
              // gh api returns the created key object; if parse fails, still success
            }

            events.emit("worker-harness:refresh", undefined);

            return {
              content: [
                {
                  type: "text",
                  text:
                    `Granted worker ${workerName} read-only access to ${repoFull}.\n` +
                    `Deploy key "${title}" registered${keyId ? ` (id: ${keyId})` : ""}.\n` +
                    `Clone on worker:\n` +
                    `  git clone git@github.com:${repoFull}.git`,
                },
              ],
              details: {
                repo: repoFull,
                worker_id: resolvedWorkerId,
                worker_name: workerName,
                key_id: keyId,
                key_title: title,
                already_registered: false,
              },
            };
          }
          default:
            throw new Error(`Unknown action: ${String(action)}`);
        }
      } catch (err) {
        if (err instanceof ApiError && err.status === 409 && (action === "preview_queue_move" || action === "move_queued_job")) {
          throw new Error(`${err.message}. Re-fetch list_queue and re-preview the move before committing; never blind-retry.`);
        }
        throw toToolError(err);
      }
    },
  });
}