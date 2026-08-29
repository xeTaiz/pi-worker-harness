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
 * Specialised operations that do not fit the RO/RW split remain as their own
 * registered tools (see tools/admin.ts: image deploy + worker restart).
 *
 *   wh_read     — RO inspection of workers, jobs, tunnels, and Pi sessions
 *   wh_dispatch — mutations: job exec, tunnels, file transfer, git access,
 *                 Pi child delegation, worker prune, data copy
 *   wh_admin_*  — image deploy / worker restart (not subagent-eligible)
 */
import { Type, type Static } from "typebox";
import { Text } from "@earendil-works/pi-tui";
import type { ExtensionAPI, Theme } from "@earendil-works/pi-coding-agent";
import { StringEnum } from "../utils.ts";
import { events } from "../events.ts";
import { gpuAvailability, workerGpuStatus } from "../gpu-status.ts";
import type { GpuModelStatus } from "../gpu-status.ts";
import type { Worker } from "../types.ts";
import {
  availableGpuWorkers,
  buildDataCatalog,
  formatDataCatalog,
  formatAvailableGpus,
  formatWorkerOverview,
} from "../agent-output.ts";
import type { DataCatalog } from "../agent-output.ts";
import {
  ApiError,
  addTunnel,
  copyData,
  createDelegation,
  downloadFile,
  getDelegation,
  getJobLogs,
  getOrchestratorUrl,
  getWorker,
  getWorkerSummary,
  listDataPaths,
  listJobs,
  listPiSessions,
  listTunnels,
  listWorkers,
  pruneWorkers,
  removeTunnel,
  startJob,
  stopJob,
  uploadFile,
} from "../api.ts";
import { subscribeJobLogs } from "../sse.ts";

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
    case "pi_delegation":
      return args.delegation_id ?? "";
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
    case "stop_job":
      return args.job_id ?? "";
    case "add_tunnel":
      return `${args.worker_id} :${args.remote_port}→localhost:${args.local_port}${args.name ? ` (${args.name})` : ""}`;
    case "remove_tunnel":
      return args.tunnel_id ?? "";
    case "workers_prune":
      return args.minutes !== undefined ? `${args.minutes}m` : "";
    case "upload_file":
      return `${args.worker_id}:${truncate(args.path ?? "", 60)}`;
    case "download_file":
      return `${args.worker_id}:${truncate(args.path ?? "", 60)}`;
    case "delegate": {
      const parts: string[] = [];
      if (args.worker_id) parts.push(`worker=${args.worker_id}`);
      if (args.sync) parts.push("sync");
      parts.push(`task=${truncate(args.task ?? "", 60)}`);
      return parts.filter(Boolean).join(" ");
    }
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
function workerStatusEmoji(status: Worker["status"]): string {
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

export function buildWorkerList(workers: Worker[], theme: Theme): string {
  if (workers.length === 0) return theme.fg("dim", "No workers");

  const rows = workers.map((worker) => ({
    worker,
    dns: worker.dns_name || worker.worker_ip || "—",
    gpus: workerGpuStatus(worker),
  }));
  const workerWidth = Math.max(...rows.map(({ worker }) => worker.name.length));
  const dnsWidth = Math.max(...rows.map(({ dns }) => dns.length));
  const gpuStatuses = rows.flatMap(({ gpus }) => gpus);
  const modelWidth = Math.max(0, ...gpuStatuses.map(({ model }) => model.length));
  const ratioWidth = Math.max(
    0,
    ...gpuStatuses.map(({ busy, total }) => `${busy}/${total}`.length),
  );
  const separator = theme.fg("dim", " │ ");

  return rows.flatMap(({ worker, dns, gpus }) => {
    let firstPrefix = workerStatusEmoji(worker.status);
    firstPrefix += separator + theme.fg("toolOutput", worker.name.padEnd(workerWidth));
    firstPrefix += separator + theme.fg("dim", dns.padEnd(dnsWidth));
    firstPrefix += separator + theme.fg("dim", "GPUs: ");
    if (gpus.length === 0) return [firstPrefix + theme.fg("dim", "—")];

    let continuationPrefix = "  ";
    continuationPrefix += separator + " ".repeat(workerWidth);
    continuationPrefix += separator + " ".repeat(dnsWidth);
    continuationPrefix += separator + " ".repeat("GPUs: ".length);

    return gpus.map((status, index) => {
      const ratio = `${status.busy}/${status.total}`.padStart(ratioWidth);
      const value = `${status.model.padEnd(modelWidth)}: ${ratio}`;
      return (index === 0 ? firstPrefix : continuationPrefix)
        + theme.fg(gpuStatusColor(status), value);
    });
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
    "get_job_logs",
    "list_tunnels",
    "pi_sessions",
    "pi_delegation",
  ]),
  worker_id: Type.Optional(Type.String({ description: "Worker ID or name" })),
  query: Type.Optional(
    Type.String({ description: "Case-insensitive substring filter for list_data paths" }),
  ),
  job_id: Type.Optional(Type.String({ description: "Job ID" })),
  delegation_id: Type.Optional(Type.String({ description: "Delegation ID" })),
  origin_session_id: Type.Optional(
    Type.String({ description: "Delegated Pi child session ID that originated the job" }),
  ),
  status: Type.Optional(StringEnum(["pending", "running", "done", "failed"])),
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
    "add_tunnel",
    "remove_tunnel",
    "workers_prune",
    "upload_file",
    "download_file",
    "delegate",
    "grant_git_access",
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
      "Use /code/work and /code/dev for repos, /data/shared for fleet-shared storage, /data/local for worker-local storage, and ~ for home files.",
  })),
  name: Type.Optional(Type.String({ description: "Job or service label" })),
  no_pty: Type.Optional(Type.Boolean({ description: "Disable PTY" })),
  sync: Type.Optional(Type.Boolean({
    description: "Block until command finishes, return stdout (exec action). Default: false",
  })),
  sync_timeout: Type.Optional(Type.Number({ description: "Timeout in seconds for sync mode (default 120)" })),
  // stop_job / remove_tunnel / get_job_logs / pi_delegation
  job_id: Type.Optional(Type.String({ description: "Job ID" })),
  tunnel_id: Type.Optional(Type.String({ description: "Tunnel ID" })),
  // add_tunnel
  local_port: Type.Optional(Type.Number({ description: "Local port" })),
  remote_port: Type.Optional(Type.Number({ description: "Remote worker port" })),
  // workers_prune
  minutes: Type.Optional(
    Type.Number({ description: "Stale worker threshold in minutes (default 5)" }),
  ),
  // upload_file / download_file
  path: Type.Optional(Type.String({ description: "Remote file path on worker (for file transfer)" })),
  content_b64: Type.Optional(Type.String({ description: "Base64-encoded file content (for upload_file)" })),
  max_bytes: Type.Optional(Type.Number({ description: "Max bytes to download (default 10MB)" })),
  // delegate
  task: Type.Optional(Type.String({ description: "Concrete task for the delegated Pi child" })),
  parent_session_id: Type.Optional(Type.String({ description: "Optional registered parent Pi session ID" })),
  cwd: Type.Optional(Type.String({ description: "Optional existing worker directory; defaults to worker home" })),
  timeout_seconds: Type.Optional(Type.Number({
    description: "Delegation timeout gate; 0 disables (default 0). Unacknowledged expiry becomes termination_unknown.",
  })),
  // grant_git_access
  repo: Type.Optional(Type.String({
    description: 'GitHub repo in "owner/repo" format. If omitted, detected from the current directory\'s git remote.',
  })),
});

// ── registration ───────────────────────────────────────
// OMP consumes loadMode; standard Pi safely ignores the extra runtime property.
// Spreading it avoids requiring standard Pi's ToolDefinition type to declare it.
const essentialToolPresentation = { loadMode: "essential" as const };


export function registerGroupedTools(
  pi: ExtensionAPI,
) {
  pi.registerTool({
    name: "wh_read",
    label: "Worker Harness Read",
    ...essentialToolPresentation,
    description:
      "Token-efficient read-only Worker Harness inspection. Choose the narrowest action:\n" +
      "- `available_gpus`: preferred GPU preflight; returns only online machines with at least one free GPU.\n" +
      "- `list_workers`: compact one-row-per-worker fleet overview; use only when offline/busy workers matter.\n" +
      "- `get_worker(worker_id)`: full detail after selecting one worker.\n" +
      "- `list_data(query?)`: shallow directory-to-worker catalog; `/data/shared/...` is fleet-shared, `/data/local/...` is worker-specific, and `/code/...` contains repos. Pass `query` for known names; omit it only for full inventory.\n" +
      "- `list_jobs(worker_id?, status?, origin_session_id?)`: filter whenever possible; `get_job_logs(job_id, tail|head|follow?)` reads logs.\n" +
      "- `list_tunnels`, `get_worker_summary`, `pi_sessions(worker_id?)`, and `pi_delegation(delegation_id)` inspect their named resources.\n\n" +
      "MUST use instead of the `wh` CLI or Worker Harness APIs. Mutation/admin operations require `wh_dispatch`/`wh_admin_*`; if unavailable, report the limitation—NEVER work around it through Bash, the CLI, or APIs. Before GPU work use `available_gpus`, not `list_workers`. Use returned full worker IDs for dispatch because worker names can be duplicated.",
    promptSnippet:
      "wh_read (RO): use available_gpus for GPU selection; list_workers only for fleet overview; get_worker only for one chosen worker; list_data(query) for shallow /data/shared, /data/local, and /code discovery. Filter list_jobs/logs. Never invoke the wh CLI/API directly.",
    parameters: whReadParams,
    renderCall(args, theme, context) {
      const text = (context.lastComponent as Text | undefined) ?? new Text("", 0, 0);
      text.setText(buildReadHeader(args, theme));
      return text;
    },
    renderResult(result, _options, theme) {
      const details = result.details as {
        workers?: Worker[];
        data?: DataCatalog;
      } | undefined;
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
        delegation_id,
        origin_session_id,
        status,
        query,
        tail,
        head,
        follow,
      } = params;
      try {
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
            const jobs = await listJobs({ worker_id, status, origin_session_id });
            return {
              content: [{ type: "text", text: JSON.stringify(jobs, null, 2) }],
              details: { jobs },
            };
          }
          case "get_job_logs": {
            const id = requireField(job_id, "job_id");
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
                  lines.push(line);
                  onUpdate?.({ content: [{ type: "text", text: line + "\n" }] });
                  if (line.trim()) {
                    events.emit("worker-harness:log-line", { job_id: id, line });
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
          case "pi_sessions": {
            const sessions = await listPiSessions(worker_id);
            return {
              content: [{ type: "text", text: JSON.stringify(sessions, null, 2) }],
              details: { sessions },
            };
          }
          case "pi_delegation": {
            const delegation = await getDelegation(requireField(delegation_id, "delegation_id"));
            return {
              content: [{ type: "text", text: JSON.stringify(delegation, null, 2) }],
              details: delegation,
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
    description:
      "Mutating Worker Harness operations: job execution/stopping, tunnels, file transfer, git access, Pi delegation, pruning, and worker-to-worker data copy. MUST use instead of the `wh` CLI or Worker Harness APIs. Inspection requires `wh_read`; image deployment/restart requires `wh_admin_*`. If a required tool is unavailable, report the limitation—NEVER work around it through Bash, the CLI, or APIs. `tools: [\"wh_dispatch\"]` grants a child the full mutation surface.\n\n" +
      "Preferred over local Bash for long-running jobs, experiments, GPU work, and remote compute. Before GPU work call `wh_read(action=\"available_gpus\")` once and dispatch with the returned full worker ID; do not fetch the full fleet or repeatedly preflight unchanged state. If `wh_read` is unavailable and the assignment does not identify an available worker/GPU, report the missing preflight instead of launching. On workers, create project-local `uv` environments for dependencies as needed.\n\n" +
      "Action-specific fields: `data_copy(src_worker, src_path, dst_worker, dst_path, ttl_seconds?)`; `exec(worker_id, command, name?, no_pty?, sync?, sync_timeout?)`; `stop_job(job_id)`; `add_tunnel(worker_id, local_port, remote_port, name?)`; `remove_tunnel(tunnel_id)`; `workers_prune(minutes?)`; `upload_file(worker_id, path, content_b64)`; `download_file(worker_id, path, max_bytes?)`; `delegate(task, worker_id?, parent_session_id?, cwd?, timeout_seconds?, sync?)`; `grant_git_access(worker_id, repo?)`.\n\n" +
      "`exec` defaults asynchronous and returns a job ID; `sync: true` blocks and returns output. For versioned project files prefer git plus `grant_git_access`; use upload/download for ad-hoc files. Before `data_copy`, use `wh_read(action=\"list_data\", query=\"known-substring\")`; omit `query` only when a complete data inventory is actually needed.",
    promptSnippet:
      "wh_dispatch (RW): before GPU work call wh_read available_gpus once and use its full worker ID; before data_copy call wh_read list_data with a known query. Exec defaults async. Never invoke the wh CLI/API directly.",
    parameters: whDispatchParams,
    renderCall(args, theme, context) {
      const text = (context.lastComponent as Text | undefined) ?? new Text("", 0, 0);
      text.setText(buildDispatchHeader(args, theme));
      return text;
    },
    async execute(_toolCallId, params) {
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
        task,
        parent_session_id,
        cwd,
        timeout_seconds,
        repo,
      } = params;

      try {
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
            const resolvedWorkerId = requireField(worker_id, "worker_id");
            const resolvedCommand = requireField(command, "command");
            const job = await startJob({
              worker_id: resolvedWorkerId,
              command: resolvedCommand,
              name,
              no_pty,
              sync,
              sync_timeout,
            });
            events.emit("worker-harness:job-started", { job });
            events.emit("worker-harness:refresh", undefined);

            // Sync mode: return the command output directly
            if (sync && job.stdout !== undefined) {
              const text =
                job.status === "done"
                  ? job.stdout || "(no output)"
                  : `[${job.status}] ${job.stdout}`.trim();
              return {
                content: [{ type: "text", text }],
                details: { job },
              };
            }

            // Resolve the worker name for a human-readable message. Best-effort:
            // fall back to whatever the caller passed (name or id) so the message
            // is still informative even if the lookup fails.
            let workerLabel = resolvedWorkerId;
            try {
              const worker = await getWorker(resolvedWorkerId);
              workerLabel = worker.name || resolvedWorkerId;
            } catch {
              /* keep fallback */
            }

            return {
              content: [
                {
                  type: "text",
                  text: `Job started: ${job.id} on ${workerLabel}\nSession: ${job.tmux_session}`,
                },
              ],
              details: { job },
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
                    ? `Job stopped: ${resolvedJobId}`
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
                    `Tunnel started: localhost:${tunnel.local_port} → ${tunnel.worker_name ?? tunnel.worker_id}:${tunnel.remote_port}\n` +
                    `Service: ${tunnel.service_name} (PID: ${tunnel.pid})\n` +
                    `Access: ${orchestratorHost}:${tunnel.local_port}`,
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
          case "delegate": {
            const delegation = await createDelegation({
              task: requireField(task, "task"),
              worker_id,
              parent_session_id,
              cwd,
              timeout_seconds,
              sync,
            });
            const syncNote = delegation.settled === undefined
              ? ""
              : delegation.settled
                ? "\nSettled: yes"
                : "\nSettled: no (wait cap elapsed; child still running)";
            return {
              content: [{
                type: "text",
                text: `Delegated to worker: ${delegation.child_session_id}\nDelegation: ${delegation.delegation_id}\nState: ${delegation.state}${syncNote}`,
              }],
              details: delegation,
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
        throw toToolError(err);
      }
    },
  });
}