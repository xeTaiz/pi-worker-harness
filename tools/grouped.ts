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
 *   wh_read     — role-scoped read-only inspection
 *   wh_dispatch — role-scoped mutations, Marimo, and fleet orchestration
 *   wh_admin_*  — image deploy / worker restart for normal operator sessions
 */
import { Type, type Static } from "typebox";
import { Text } from "@earendil-works/pi-tui";
import type { ExtensionAPI, Theme } from "@earendil-works/pi-coding-agent";
import { StringEnum } from "../utils.ts";
import { events } from "../events.ts";
import { gpuAvailability, workerGpuStatus } from "../gpu-status.ts";
import type { GpuModelStatus } from "../gpu-status.ts";
import type { ClusterTarget, ComputeTarget, QueuedJob, Worker } from "../types.ts";
import {
  formatDataCatalog,
  formatAvailableGpus,
  formatQueueOverview,
  formatWorkerOverview,
} from "../agent-output.ts";
import type { DataCatalog } from "../agent-output.ts";
import {
  askPm,
  dispatchTask,
  requireWorkerTarget,
  getPiSession,
  getWorker,
  listPiSessions,
  listProjects,
  notifyPm,
  pruneWorkers,
  sendOrchestratorMessage,
  sendProjectMessage,
  sendSessionMessage,
  startJob,
  submitPr,
  teardownTask,
} from "../api.ts";
import type { SlurmContext } from "../slurm/client.ts";
import {
  COMPUTE_READ_ACTIONS,
  COMPUTE_DISPATCH_ACTIONS,
  computeReadSchema,
  computeDispatchSchema,
  executeComputeRead,
  executeComputeDispatch,
  requireField,
  toToolError,
  type ComputeSlurmClient,
} from "./compute-actions.ts";

// ── shared helpers ─────────────────────────────────────


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
        args.position !== undefined ? `position=${args.position}` : "",
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
  ...computeReadSchema.properties,
  action: StringEnum([...COMPUTE_READ_ACTIONS, "pi_sessions", "fleet_status", "list_projects"]),
});

/**
 * wh_dispatch — mutations.
 *
 * Each `action` only uses a subset of the optional fields. Required-for-action
 * fields are enforced at runtime via `requireField`.
 */
const whDispatchParams = Type.Object({
  ...computeDispatchSchema.properties,
  action: StringEnum([
    ...COMPUTE_DISPATCH_ACTIONS,
    "workers_prune", "grant_git_access", "ask_pm", "notify_pm", "dispatch_task",
    "answer", "submit_pr", "teardown_task", "escalate", "pm_send",
  ]),
  // workers_prune
  minutes: Type.Optional(
    Type.Number({ description: "Stale worker threshold in minutes (default 5)" }),
  ),
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
});

// ── registration ───────────────────────────────────────
// OMP consumes loadMode; standard Pi safely ignores the extra runtime property.
// Spreading it avoids requiring standard Pi's ToolDefinition type to declare it.
const essentialToolPresentation = { loadMode: "essential" as const };

export interface GroupedToolOptions {
  getContext: () => Promise<SlurmContext>;
  slurmClient?: ComputeSlurmClient;
}

export function registerGroupedTools(pi: ExtensionAPI, options: GroupedToolOptions) {
  const role = process.env.WH_SESSION_ROLE?.trim() ?? "";
  let readActions: string[];
  let dispatchActions: string[];
  let readDescription: string;
  let dispatchDescription: string;

  const computeReadActions = COMPUTE_READ_ACTIONS.filter((action) => action !== "list_tunnels");
  const computeDispatchActions = COMPUTE_DISPATCH_ACTIONS.filter((action) =>
    !["add_tunnel", "remove_tunnel", "upload_file", "download_file"].includes(action));

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
    readActions = [...computeReadActions, "list_tunnels", "pi_sessions"];
    dispatchActions = [
      "data_copy",
      "exec",
      "stop_job",
      "enqueue",
      "update_queued_job",
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
    ];
    readDescription =
      "Token-efficient read-only Worker Harness inspection. Inspect list_queue before scheduling GPU work and use the narrowest action.";
    dispatchDescription =
      "Mutating and capability-bearing Worker Harness operations for jobs, tunnels, file transfer, git access, pruning, data copy, and managed Marimo. Use wh_read for inspection and never invoke the Worker Harness CLI or API directly.";
  } else {
    console.warn(`[pi-worker-harness] Unknown WH_SESSION_ROLE ${JSON.stringify(role)}; grouped tools disabled`);
    return;
  }

  const readPromptSnippet = role === "orchestrator"
    ? "wh_read: inspect fleet_status or list_projects before routing work to a project manager."
    : "wh_read (RO): inspect list_queue before scheduling GPU work; available_gpus separates immediately free workers from scheduler-backed cluster capacity. list_workers merges real workers and configured cluster targets without polling every cluster; get_worker refreshes one chosen target's resources/pending reasons. list_data(query) discovers shallow /data/shared, /data/local, and /code paths. Filter list_jobs/logs. Never invoke the wh CLI/API directly.";
  const dispatchPromptSnippet = role === "orchestrator"
    ? "wh_dispatch: use pm_send to route a request to exactly one configured project's manager."
    : "wh_dispatch (RW/capability): inspect wh_read list_queue, then enqueue scheduled work. Cluster targets (cluster:<id>) require slurm.project_path and explicit slurm.walltime_seconds; selected shared environment bin/cwd persist in the queued command. Prepare shared files/environments through ordinary worker tools or CPU cluster enqueue; never exec a cluster target. Preserve submission_key on uncertainty and inspect get_worker evidence before candidate_job_id. stop_job defaults to job scope; allocation scope requests cancellation of the entire verified allocation. exec bypasses the queue only for short setup, diagnosis, or non-GPU commands; before immediate GPU work inspect available_gpus and use its full actual worker ID. Do not alter another agent's job without explicit user direction. Before data_copy inspect list_data with a known query. Use wh_dispatch for managed Marimo; no browser is needed to execute. Never invoke the wh CLI/API directly.";

  const roleReadParams = Type.Object({
    ...whReadParams.properties,
    action: StringEnum(readActions),
  });
  const roleDispatchParams = Type.Object({
    ...whDispatchParams.properties,
    action: StringEnum(dispatchActions),
  });

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
        workers?: Worker[];
        jobs?: QueuedJob[];
        queue?: boolean;
        data?: DataCatalog;
        targets?: ComputeTarget[];
        cluster_targets?: ClusterTarget[];
      } | undefined;
      if (details?.queue && Array.isArray(details.workers) && Array.isArray(details.jobs)) {
        return new Text(
          formatQueueOverview(details.workers, details.jobs, Math.floor(Date.now() / 1000)),
          0,
          0,
        );
      }
      if (details?.targets) {
        return new Text(formatWorkerOverview(details.targets), 0, 0);
      }
      if (details?.cluster_targets) {
        return new Text(formatAvailableGpus(details.workers ?? [], details.cluster_targets), 0, 0);
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
      const { action, worker_id } = params;
      try {
        if (typeof action !== "string" || !readActions.includes(action)) throw new Error(`Action ${action} is not available for role ${role || "operator"}`);
        if (action !== "get_worker" && action !== "get_worker_summary" && action !== "list_jobs" && action !== "list_queue") {
          requireWorkerTarget(worker_id);
        }
        switch (action) {
          case "list_workers":
          case "available_gpus":
          case "get_worker":
          case "get_worker_summary":
          case "list_data":
          case "list_jobs":
          case "list_queue":
          case "get_job_logs":
          case "list_tunnels":
            return await executeComputeRead(params, {
              slurmContext: await options.getContext(),
              slurmClient: options.slurmClient,
              signal,
              callbacks: {
                onUpdate,
                onLogLine: (job_id, line) => events.emit("worker-harness:log-line", { job_id, line }),
              },
            });
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
        action, worker_id, minutes, slurm, repo, question, note, branch, briefing,
        task_session_id, text, summary, force, project, message,
      } = params;

      try {
        if (typeof action !== "string" || !dispatchActions.includes(action)) throw new Error(`Action ${action} is not available for role ${role || "operator"}`);
        if (slurm !== undefined && action !== "enqueue") throw new Error("slurm is valid only for cluster enqueue");
        if (action !== "enqueue") {
          requireWorkerTarget(worker_id);
        }
        switch (action) {
          case "data_copy":
          case "exec":
          case "enqueue":
          case "update_queued_job":
          case "stop_job":
          case "add_tunnel":
          case "remove_tunnel":
          case "list_marimo":
          case "get_marimo":
          case "start_marimo":
          case "stop_marimo":
          case "execute_marimo":
          case "upload_file":
          case "download_file":
            return await executeComputeDispatch(params, {
              slurmContext: await options.getContext(),
              slurmClient: options.slurmClient,
              signal,
              callbacks: {
                onUpdate,
                onRefresh: () => events.emit("worker-harness:refresh", undefined),
                onJobStarted: (job) => events.emit("worker-harness:job-started", { job }),
              },
            });
          case "workers_prune": {
            const result = await pruneWorkers(minutes);
            events.emit("worker-harness:refresh", undefined);
            return {
              content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
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
        throw toToolError(err);
      }
    },
  });
}