/** UI-independent compute tools shared by omp and Claude MCP. */
import { Type, type Static } from "typebox";
import { Value } from "typebox/value";
import { StringEnum } from "../utils.ts";
import type { ComputeTarget, Job } from "../types.ts";
import { availableGpuWorkers, buildDataCatalog, formatAvailableGpus, formatQueueOverview, formatWorkerOverview } from "../agent-output.ts";
import { ApiError, addTunnel, copyData, createMarimo, downloadFile, enqueueJob, getJobLogs, isClusterTargetId, requireWorkerTarget, getMarimo, getOrchestratorUrl, getWorker, getWorkerSummary, listDataPaths, listJobs, listMarimo, listQueue, listTunnels, listWorkers, removeMarimo, removeTunnel, startJob, updateQueuedJob, uploadFile, unwrapDoubleStringified } from "../api.ts";
import { streamJobLogs } from "../sse.ts";
import { ensureKernelSession, executeMarimoCode, hydrateNotebook } from "../marimo.ts";
import { enqueueCluster, getClusterTarget, listClusterTargets, readClusterJobLogs, stopClusterJob, type SlurmClient, type SlurmContext } from "../slurm/client.ts";

export const COMPUTE_READ_ACTIONS = ["list_workers", "available_gpus", "get_worker", "get_worker_summary", "list_data", "list_jobs", "list_queue", "get_job_logs", "list_tunnels"];
export const COMPUTE_DISPATCH_ACTIONS = ["data_copy", "exec", "stop_job", "enqueue", "update_queued_job", "add_tunnel", "remove_tunnel", "upload_file", "download_file", "list_marimo", "get_marimo", "start_marimo", "stop_marimo", "execute_marimo"];

export const computeReadSchema = Type.Object({
  action: StringEnum(COMPUTE_READ_ACTIONS),
  worker_id: Type.Optional(Type.String({ description: "Actual worker ID/name, or cluster:<profile.id> for target detail and aggregated jobs/queue" })),
  query: Type.Optional(
    Type.String({ description: "Case-insensitive substring filter for list_data paths" }),
  ),
  job_id: Type.Optional(Type.String({ description: "Job ID" })),
  origin_session_id: Type.Optional(
    Type.String({ description: "Child Pi session ID that originated the job" }),
  ),
  status: Type.Optional(StringEnum(["pending", "starting", "running", "done", "failed"])),
  tail: Type.Optional(Type.Number({ description: "Last N log lines. Default 10" })),
  head: Type.Optional(
    Type.Number({ description: "First N log lines. Mutually exclusive with tail" }),
  ),
  follow: Type.Optional(Type.Boolean({ description: "Stream logs in real time" })),
}, { additionalProperties: false });

export const computeDispatchSchema = Type.Object({
  action: StringEnum(COMPUTE_DISPATCH_ACTIONS),
  // data_copy
  src_worker: Type.Optional(Type.String({ description: "Source worker ID or name for data_copy" })),
  src_path: Type.Optional(Type.String({ description: "Advertised absolute source path for data_copy" })),
  dst_worker: Type.Optional(Type.String({ description: "Destination worker ID or name for data_copy" })),
  dst_path: Type.Optional(Type.String({ description: "Absolute destination path for data_copy" })),
  ttl_seconds: Type.Optional(Type.Number({ description: "Temporary source export TTL in seconds (default 21600)" })),
  // exec
  worker_id: Type.Optional(Type.String({ description: "Actual worker ID/name; cluster:<profile.id> accepts enqueue only" })),
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
  expected_seconds: Type.Optional(Type.Number({
    minimum: 1,
    description: "Expected duration in seconds; scheduling information only",
  })),
  gpu_count: Type.Optional(Type.Number({
    minimum: 0,
    description: "GPUs requested; GPU targets default 1, CPU cluster targets require/default 0",
  })),
  position: Type.Optional(Type.Number({
    minimum: 1,
    description: "One-based pending queue position",
  })),
  slurm: Type.Optional(Type.Object({
    project_path: Type.String({ description: "Existing project below the target's shared_path; symlinks are resolved" }),
    environment_path: Type.Optional(Type.String({ description: "Shared environment directory containing bin; omit for SIF uv preparation" })),
    cpus: Type.Optional(Type.Number({ minimum: 1 })),
    memory_mb: Type.Optional(Type.Number({ minimum: 1 })),
    walltime_seconds: Type.Number({ minimum: 1, description: "Explicit allocation walltime, bounded by profile/partition" }),
    checkpoint_signal: Type.Optional(StringEnum(["USR1", "TERM"])),
  })),
  submission_key: Type.Optional(Type.String({ description: "Persisted idempotency key; reuse unchanged input to recover uncertain submission" })),
  candidate_job_id: Type.Optional(Type.String({ description: "Verified scheduler candidate for an existing submission_key or allocation stop" })),
  scope: Type.Optional(StringEnum(["job", "allocation"])),
  // stop_job / remove_tunnel
  job_id: Type.Optional(Type.String({ description: "Job ID" })),
  tunnel_id: Type.Optional(Type.String({ description: "Tunnel ID" })),
  // add_tunnel
  local_port: Type.Optional(Type.Number({ description: "Local port" })),
  remote_port: Type.Optional(Type.Number({ description: "Remote worker port" })),
  // upload_file / download_file
  path: Type.Optional(Type.String({ description: "Remote file path on worker (for file transfer)" })),
  content_b64: Type.Optional(Type.String({ description: "Base64-encoded file content (for upload_file)" })),
  max_bytes: Type.Optional(Type.Number({ description: "Max bytes to download (default 10MB)" })),
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
}, { additionalProperties: false });

export interface ComputeResult {
  content: { type: "text"; text: string }[];
  details?: unknown;
  isError?: boolean;
}
export interface ComputeCallbacks {
  onUpdate?: (result: ComputeResult) => void;
  onRefresh?: () => void;
  onJobStarted?: (job: Job) => void;
  onLogLine?: (jobId: string, line: string) => void;
}
export type ComputeSlurmClient = Pick<SlurmClient, "enqueueCluster" | "getClusterTarget" | "listClusterTargets" | "readClusterJobLogs" | "stopClusterJob">;
export interface ComputeActionContext {
  slurmContext: SlurmContext;
  slurmClient?: ComputeSlurmClient;
  signal?: AbortSignal;
  callbacks?: ComputeCallbacks;
}
const defaultSlurmClient: ComputeSlurmClient = { enqueueCluster, getClusterTarget, listClusterTargets, readClusterJobLogs, stopClusterJob };

export function toToolError(err: unknown): Error {
  if (err instanceof ApiError) {
    return new Error(`${err.message}${err.detail ? ` (${JSON.stringify(err.detail)})` : ""}`);
  }
  return err instanceof Error ? err : new Error(String(err));
}

export function requireField<T>(value: T | undefined, name: string): T {
  if (value === undefined || value === null || value === "") {
    throw new Error(`Missing required parameter for action: ${name}`);
  }
  return value;
}

export async function executeComputeRead(params: unknown, context: ComputeActionContext): Promise<ComputeResult> {
  if (!Value.Check(computeReadSchema, params)) throw new Error("Invalid compute read action or parameters");
  const { action, worker_id, job_id, origin_session_id, status, query, tail, head, follow } = params as Static<typeof computeReadSchema>;
  const { signal, callbacks } = context;
  const onUpdate = callbacks?.onUpdate;
  const slurmClient = context.slurmClient ?? defaultSlurmClient;
  try {
    if (action !== "get_worker" && action !== "get_worker_summary" && action !== "list_jobs" && action !== "list_queue") requireWorkerTarget(worker_id);
    switch (action) {
          case "list_workers": {
            const [workers, clusters] = await Promise.all([listWorkers(), slurmClient.listClusterTargets()]);
            const targets: ComputeTarget[] = [
              ...workers.map((worker) => ({ ...worker, kind: "worker" as const })),
              ...clusters,
            ];
            return {
              content: [{ type: "text", text: formatWorkerOverview(targets) }],
              details: { workers, targets },
            };
          }
          case "available_gpus": {
            const [workers, clusters] = await Promise.all([listWorkers(), slurmClient.listClusterTargets()]);
            const cluster_targets = clusters.filter((target) => target.compute_kind === "gpu");
            return {
              content: [{ type: "text", text: formatAvailableGpus(workers, cluster_targets) }],
              details: { workers: availableGpuWorkers(workers), cluster_targets },
            };
          }
          case "get_worker": {
            const id = requireField(worker_id, "worker_id");
            const worker = isClusterTargetId(id)
              ? await slurmClient.getClusterTarget(id, context.slurmContext)
              : { ...await getWorker(id), kind: "worker" as const };
            return {
              content: [{ type: "text", text: JSON.stringify(worker, null, 2) }],
              details: { worker },
            };
          }
          case "get_worker_summary": {
            const summary = worker_id
              ? isClusterTargetId(worker_id)
                ? await slurmClient.getClusterTarget(worker_id, context.slurmContext)
                : { ...await getWorker(worker_id), kind: "worker" as const }
              : { ...await getWorkerSummary(), cluster_targets: await slurmClient.listClusterTargets() };
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
            const targetId = unwrapDoubleStringified(worker_id) as string | undefined;
            const cluster = isClusterTargetId(targetId)
              ? requireField((await slurmClient.listClusterTargets()).find((target) => target.id === targetId), "configured cluster target")
              : null;
            const matchingIds = cluster
              ? new Set((await listWorkers()).filter((worker) => worker.slurm?.cluster_id === cluster.cluster_id).map((worker) => worker.id))
              : null;
            const allJobs = await listJobs({ worker_id: cluster ? undefined : targetId, status, origin_session_id });
            const jobs = matchingIds ? allJobs.filter((job) => job.worker_id && matchingIds.has(job.worker_id)) : allJobs;
            return {
              content: [{ type: "text", text: JSON.stringify(jobs, null, 2) }],
              details: { jobs },
            };
          }
          case "list_queue": {
            const targetId = unwrapDoubleStringified(worker_id) as string | undefined;
            const cluster = isClusterTargetId(targetId)
              ? requireField((await slurmClient.listClusterTargets()).find((target) => target.id === targetId), "configured cluster target")
              : null;
            const [allWorkers, allJobs] = await Promise.all([
              listWorkers(),
              listQueue(cluster ? undefined : targetId),
            ]);
            const workers = cluster ? allWorkers.filter((worker) => worker.slurm?.cluster_id === cluster.cluster_id) : allWorkers;
            const matchingIds = new Set(workers.map((worker) => worker.id));
            const jobs = cluster ? allJobs.filter((job) => job.worker_id && matchingIds.has(job.worker_id)) : allJobs;
            return {
              content: [{
                type: "text",
                text: formatQueueOverview(workers, jobs, Math.floor(Date.now() / 1000)),
              }],
              details: { workers, jobs, queue: true },
            };
          }
          case "get_job_logs": {
            const id = requireField(job_id, "job_id");
            if (head !== undefined && tail !== undefined) throw new Error("head and tail are mutually exclusive");
            if (follow) {
              if (head !== undefined) {
                throw new Error("head is not supported in follow mode");
              }
              const sharedLogs = await slurmClient.readClusterJobLogs(id, { tail: tail ?? 50 },
                context.slurmContext);
              if (sharedLogs) {
                return {
                  content: [{ type: "text", text: sharedLogs.logs + "\n[Bounded shared log snapshot; worker transport unavailable]" }],
                  details: sharedLogs,
                };
              }

              const lines: string[] = [];
              const timeoutController = new AbortController();
              const timeout = setTimeout(() => timeoutController.abort(), 60_000);

              let abortListener: (() => void) | null = null;
              if (signal) {
                if (signal.aborted) timeoutController.abort();
                const onAbort = () => timeoutController.abort();
                signal.addEventListener("abort", onAbort, { once: true });
                abortListener = () => signal.removeEventListener("abort", onAbort);
              }

              const stream = streamJobLogs(id, {
                tail: tail ?? 50,
                signal: timeoutController.signal,
                pollSeconds: 1,
                onLine: (line) => {
                  lines.push(line);
                  onUpdate?.({ content: [{ type: "text", text: line + "\n" }] });
                  if (line.trim()) {
                    callbacks?.onLogLine?.(id, line);
                  }
                },
              });

              try {
                await stream;
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
                timeoutController.abort();
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

            let logs;
            try {
              logs = await getJobLogs(id, { tail, head });
              if (!logs.logs) {
                logs = await slurmClient.readClusterJobLogs(id, { tail, head },
                  context.slurmContext) ?? logs;
              }
            } catch (err) {
              const sharedLogs = await slurmClient.readClusterJobLogs(id, { tail, head },
                context.slurmContext);
              if (!sharedLogs) throw err;
              logs = sharedLogs;
            }
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
      default: throw new Error(`Unknown compute read action: ${String(action)}`);
    }
  } catch (err) { throw toToolError(err); }
}

export async function executeComputeDispatch(params: unknown, context: ComputeActionContext): Promise<ComputeResult> {
  if (!Value.Check(computeDispatchSchema, params)) throw new Error("Invalid compute dispatch action or parameters");
  const { action, worker_id, src_worker, src_path, dst_worker, dst_path, ttl_seconds, command, name, no_pty, job_id, tunnel_id, local_port, remote_port, path, content_b64, max_bytes, sync, sync_timeout, expected_seconds, gpu_count, position, slurm, submission_key, candidate_job_id, scope, marimo_session_id, notebook_path, environment, ready_timeout, code } = params as Static<typeof computeDispatchSchema>;
  const { signal, callbacks } = context;
  const onUpdate = callbacks?.onUpdate;
  const slurmClient = context.slurmClient ?? defaultSlurmClient;
  try {
    if (slurm !== undefined && action !== "enqueue") throw new Error("slurm is valid only for cluster enqueue");
    if (action !== "enqueue") {
      requireWorkerTarget(worker_id);
      requireWorkerTarget(src_worker);
      requireWorkerTarget(dst_worker);
      requireWorkerTarget(marimo_session_id);
      requireWorkerTarget(tunnel_id);
    }
    switch (action) {
          case "data_copy": {
            const result = await copyData({
              src_worker: requireField(src_worker, "src_worker"),
              src_path: requireField(src_path, "src_path"),
              dst_worker: requireField(dst_worker, "dst_worker"),
              dst_path: requireField(dst_path, "dst_path"),
              ttl_seconds,
            });
            callbacks?.onRefresh?.();
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
              notify_recipient_id: context.slurmContext.recipient_id,
            });
            callbacks?.onJobStarted?.(job);
            callbacks?.onRefresh?.();

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
          case "enqueue": {
            const targetId = requireField(worker_id, "worker_id");
            if (!isClusterTargetId(targetId) && (slurm !== undefined || candidate_job_id !== undefined)) {
              throw new Error("slurm and candidate_job_id are valid only for cluster targets");
            }
            const slurmContext = context.slurmContext;
            const input = {
              command: requireField(command, "command"),
              name: requireField(name, "name"),
              expected_seconds: requireField(expected_seconds, "expected_seconds"),
              gpu_count,
              no_pty,
              submission_key,
            };
            if (isClusterTargetId(targetId)) {
              const result = await slurmClient.enqueueCluster(targetId, {
                ...input,
                slurm: requireField(slurm, "slurm"),
                candidate_job_id,
              }, slurmContext);
              callbacks?.onRefresh?.();
              return {
                content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
                details: result,
              };
            }
            const job = await enqueueJob({
              ...input,
              worker_id: targetId,
              notify_recipient_id: slurmContext.recipient_id,
            });
            callbacks?.onRefresh?.();
            return {
              content: [{
                type: "text",
                text: `Job queued: ${job.id} (${job.name}) on ${job.worker_id} [${job.status}]`,
              }],
              details: { job },
            };
          }
          case "update_queued_job": {
            const resolvedJobId = requireField(job_id, "job_id");
            if (
              worker_id === undefined
              && position === undefined
              && name === undefined
              && expected_seconds === undefined
              && gpu_count === undefined
            ) {
              throw new Error("update_queued_job requires at least one queue field");
            }
            const job = await updateQueuedJob(resolvedJobId, {
              worker_id,
              position,
              name,
              expected_seconds,
              gpu_count,
            });
            callbacks?.onRefresh?.();
            return {
              content: [{
                type: "text",
                text: `Queued job updated: ${job.id} (${job.name}) on ${job.worker_id} [${job.status}]`,
              }],
              details: { job },
            };
          }
          case "stop_job": {
            const resolvedJobId = requireField(job_id, "job_id");
            const slurmContext = context.slurmContext;
            const result = await slurmClient.stopClusterJob(resolvedJobId, { scope: scope ?? "job", candidate_job_id }, slurmContext);
            callbacks?.onRefresh?.();
            return {
              content: [
                {
                  type: "text",
                  text: result.requested
                    ? `Stop requested: ${resolvedJobId} (${scope ?? "job"}); awaiting terminal evidence`
                    : result.stopped ? `Job stopped: ${resolvedJobId}` : `Job already terminal: ${resolvedJobId}`,
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
            callbacks?.onRefresh?.();
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
            callbacks?.onRefresh?.();
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
            callbacks?.onRefresh?.();
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
            callbacks?.onRefresh?.();
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
      default: throw new Error(`Unknown compute dispatch action: ${String(action)}`);
    }
  } catch (err) { throw toToolError(err); }
}
