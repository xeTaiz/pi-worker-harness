import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import type {
  AddTunnelRequest,
  ApiErrorBody,
  DataCopyResult,
  DataPaths,
  FileTransferResult,
  EnqueueJobRequest,
  OwnedJob,
  MarimoCreateRequest,
  MarimoSession,
  QueueBoard,
  QueuePreview,
  QueueMoveResult,
  QueueHistory,
  UsageResult,
  PiSession,
  Project,
  JobLogsResult,
  RemoveTunnelResponse,
  RemoveMarimoResponse,
  StartJobRequest,
  StopJobResult,
  UpdateQueuedJobRequest,
  UpdateJobRequest,
  Tunnel,
  PublicWorker,
  WorkersPruneResult,
  WorkersSummary,
} from "./types.ts";

export class ApiError extends Error {
  constructor(
    public readonly status: number,
    public readonly code: string,
    message: string,
    public readonly detail: unknown,
  ) {
    // NOTE: `message` is passed to super() and must NOT be redeclared as a
    // class field (e.g. `public readonly message`), because doing so creates an
    // own property equal to the bare argument and shadows the prefixed message
    // set here — collapsing "[404] HTTP_ERROR: Not Found" back to "Not Found".
    super("[" + status + "] " + code + ": " + message);
    this.name = "ApiError";
  }
}

// The privileged control API is deliberately separate from worker registration.
// This is a personal Tailnet extension: a fresh dotfiles checkout should find
// the durable orchestrator without requiring a machine-local config file first.
let orchestratorUrl = "http://orchestrator.hs.d0me.xyz:12889";

/** Bound a stalled HTTP connection. Server-side lanes prevent worker-level
 * starvation; this protects the agent from a dead/unreachable server itself. */
const DEFAULT_API_TIMEOUT_MS = 30_000;
const inFlightJobLists = new Map<string, Promise<OwnedJob[]>>();

export function getOrchestratorUrl(): string {
  return (process.env.WH_ORCHESTRATOR_URL?.trim() || orchestratorUrl).replace(/\/+$/, "");
}

export function setOrchestratorUrl(url: string): void {
  orchestratorUrl = url.trim().replace(/\/+$/, "");
}

/** Only explicit fleet roles may register Pi bridges and host relays. */
export function isInternalSession(): boolean {
  const role = process.env.WH_SESSION_ROLE?.trim();
  return role === "orchestrator" || role === "pm" || role === "task";
}

export function assertInternalSessionToken(): void {
  if (process.env.WH_SESSION_ROLE?.trim() && !isInternalSession()) {
    throw new Error("WH_SESSION_ROLE must be orchestrator, pm, or task.");
  }
  if (isInternalSession() && !process.env.WH_SESSION_TOKEN?.trim()) {
    throw new Error("WH_SESSION_TOKEN is required for internal Worker Harness sessions; human API keys are never a fallback.");
  }
}

function getCredential(): string {
  assertInternalSessionToken();
  if (isInternalSession()) return process.env.WH_SESSION_TOKEN!.trim();
  const key = process.env.WH_API_KEY?.trim();
  if (key) return key;
  const keyFile = process.env.WH_API_KEY_FILE?.trim();
  const fileKey = keyFile ? readFileSync(keyFile, "utf8").trim() : "";
  if (!fileKey) {
    throw new Error("WH_API_KEY or a nonempty WH_API_KEY_FILE is required for Worker Harness requests.");
  }
  return fileKey;
}

/** Read on every call so rotating a key file also invalidates private caches. */
export function getCredentialIdentity(): string {
  return createHash("sha256")
    .update(process.env.WH_SESSION_ROLE?.trim() || "user")
    .update("\0")
    .update(getCredential())
    .digest("hex");
}

export async function getAuthorizationHeaders(): Promise<Record<string, string>> {
  return { Authorization: `Bearer ${getCredential()}` };
}

async function parseErrorBody(res: Response): Promise<ApiErrorBody | null> {
  try {
    return (await res.json()) as ApiErrorBody;
  } catch (_err) {
    return null;
  }
}

/**
 * Workaround for a double-encoding bug in the @earendil-works/pi-coding-agent
 * fork: tool-call string arguments (e.g. `worker_id`) arrive at the extension
 * as the JSON string literal — i.e. the 38-char value `"<uuid>"` (with literal
 * surrounding double-quotes) instead of the bare 36-char `<uuid>`. This walks
 * the value and unwraps any string that is a JSON-encoded string.
 */
function unwrapDoubleStringified(value: unknown): unknown {
  if (typeof value !== "string") return value;
  if (value.length < 2 || value[0] !== '"' || value[value.length - 1] !== '"') return value;
  try {
    const parsed = JSON.parse(value);
    return typeof parsed === "string" ? parsed : value;
  } catch {
    return value;
  }
}

function normalizeStringArgs<T>(value: T): T {
  if (value == null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(normalizeStringArgs) as unknown as T;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    out[k] = typeof v === "string" ? (unwrapDoubleStringified(v) as string) : normalizeStringArgs(v);
  }
  return out as T;
}

async function apiFetch<T>(path: string, options?: RequestInit): Promise<T> {
  const baseUrl = getOrchestratorUrl();

  // Normalize any double-stringified string values in a JSON body before sending.
  let normalizedBody = options?.body;
  if (typeof normalizedBody === "string") {
    try {
      const parsed = JSON.parse(normalizedBody);
      normalizedBody = JSON.stringify(normalizeStringArgs(parsed));
    } catch {
      /* not JSON (or malformed) — leave as-is */
    }
  }

  const timeoutSignal = AbortSignal.timeout(DEFAULT_API_TIMEOUT_MS);
  const signal = options?.signal
    ? AbortSignal.any([options.signal, timeoutSignal])
    : timeoutSignal;
  const agentName = process.env.PI_AGENT_NAME || "pi";
  const authorization = await getAuthorizationHeaders();

  let res: Response;
  try {
    res = await fetch(baseUrl + path, {
      ...options,
      signal,
      body: normalizedBody,
      // Extension-owned identity headers override caller-provided values.
      headers: {
        "Content-Type": "application/json",
        ...options?.headers,
        "X-Agent-Name": agentName,
        ...authorization,
      },
    });
  } catch (err) {
    if (timeoutSignal.aborted) {
      throw new ApiError(504, "UPSTREAM_TIMEOUT", `Worker-harness did not respond within ${DEFAULT_API_TIMEOUT_MS / 1000}s`, null);
    }
    throw err;
  }

  if (!res.ok) {
    const body = await parseErrorBody(res);
    // FastAPI's HTTPException returns {"detail": "..."} (no nested "error"
    // object), so fall back to body.detail for both message and detail.
    const codedDetail = body?.detail && typeof body.detail === "object"
      ? body.detail as { code?: string; message?: string }
      : undefined;
    const errCode = body?.error?.code ?? codedDetail?.code ?? "HTTP_ERROR";
    const errMsg = body?.error?.message ?? codedDetail?.message ??
      (typeof body?.detail === "string" ? body.detail : res.statusText);
    const errDetail = body?.error?.detail ?? body?.detail ?? null;
    throw new ApiError(res.status, errCode, errMsg, errDetail);
  }

  return res.json() as Promise<T>;
}

export async function listPiSessions(workerId?: string): Promise<PiSession[]> {
  const qs = workerId ? "?worker_id=" + encodeURIComponent(workerId) : "";
  return apiFetch<PiSession[]>("/api/v1/pi/sessions" + qs);
}

export async function getPiSession(id: string): Promise<PiSession> {
  return apiFetch<PiSession>("/api/v1/pi/sessions/" + encodeURIComponent(id));
}

export async function askPm(sessionId: string, question: string): Promise<unknown> {
  return apiFetch(`/api/v1/pi/sessions/${encodeURIComponent(sessionId)}:ask-pm`, {
    method: "POST",
    body: JSON.stringify({ question }),
  });
}

export async function notifyPm(sessionId: string, note: string): Promise<unknown> {
  return apiFetch(`/api/v1/pi/sessions/${encodeURIComponent(sessionId)}:notify-pm`, {
    method: "POST",
    body: JSON.stringify({ note }),
  });
}

export async function dispatchTask(project: string, branch: string, briefing: string): Promise<PiSession> {
  return apiFetch<PiSession>(`/api/v1/pi/projects/${encodeURIComponent(project)}/tasks`, {
    method: "POST",
    body: JSON.stringify({ branch, briefing }),
  });
}

export async function sendSessionMessage(sessionId: string, message: string): Promise<unknown> {
  return apiFetch(`/api/v1/pi/sessions/${encodeURIComponent(sessionId)}:send`, {
    method: "POST",
    body: JSON.stringify({ message }),
  });
}

export async function sendOrchestratorMessage(message: string): Promise<unknown> {
  return apiFetch("/api/v1/pi/orchestrator:send", {
    method: "POST",
    body: JSON.stringify({ message }),
  });
}

export async function submitPr(taskSessionId: string, summary: string): Promise<unknown> {
  return apiFetch(`/api/v1/pi/sessions/${encodeURIComponent(taskSessionId)}:submit-pr`, {
    method: "POST",
    body: JSON.stringify({ summary }),
  });
}

export async function teardownTask(taskSessionId: string, force = false): Promise<unknown> {
  return apiFetch(`/api/v1/pi/sessions/${encodeURIComponent(taskSessionId)}:teardown`, {
    method: "POST",
    body: JSON.stringify({ force }),
  });
}

export async function listProjects(): Promise<Project[]> {
  return apiFetch<Project[]>("/api/v1/pi/projects");
}

export async function sendProjectMessage(project: string, message: string): Promise<unknown> {
  return apiFetch(`/api/v1/pi/projects/${encodeURIComponent(project)}:send`, {
    method: "POST",
    body: JSON.stringify({ message }),
  });
}

export async function createMarimo(params: MarimoCreateRequest): Promise<MarimoSession> {
  return apiFetch<MarimoSession>("/api/v1/marimo", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

export async function listMarimo(workerId?: string): Promise<MarimoSession[]> {
  const qs = workerId ? "?worker_id=" + encodeURIComponent(workerId) : "";
  return apiFetch<MarimoSession[]>("/api/v1/marimo" + qs);
}

export async function getMarimo(id: string): Promise<MarimoSession> {
  const cleanId = unwrapDoubleStringified(id) as string;
  return apiFetch<MarimoSession>("/api/v1/marimo/" + encodeURIComponent(cleanId));
}

export async function removeMarimo(id: string): Promise<RemoveMarimoResponse> {
  const cleanId = unwrapDoubleStringified(id) as string;
  return apiFetch<RemoveMarimoResponse>("/api/v1/marimo/" + encodeURIComponent(cleanId), {
    method: "DELETE",
  });
}

export async function listWorkers(): Promise<PublicWorker[]> {
  return apiFetch<PublicWorker[]>("/api/v1/workers");
}

export async function getWorker(id: string): Promise<PublicWorker> {
  const cleanId = unwrapDoubleStringified(id) as string;
  return apiFetch<PublicWorker>("/api/v1/workers/" + encodeURIComponent(cleanId));
}

export async function pruneWorkers(minutes?: number): Promise<WorkersPruneResult> {
  const qs = minutes !== undefined ? `?minutes=${encodeURIComponent(String(minutes))}` : "";
  return apiFetch<WorkersPruneResult>(`/api/v1/workers/prune${qs}`, { method: "DELETE" });
}

export async function getWorkerSummary(): Promise<WorkersSummary> {
  return apiFetch<WorkersSummary>("/api/v1/workers/summary");
}

export async function listDataPaths(): Promise<DataPaths> {
  return apiFetch<DataPaths>("/api/v1/data");
}

export async function copyData(params: {
  src_worker: string;
  src_path: string;
  dst_worker: string;
  dst_path: string;
  ttl_seconds?: number;
}): Promise<DataCopyResult> {
  return apiFetch<DataCopyResult>("/api/v1/data/copy", {
    method: "POST",
    body: JSON.stringify(params),
  });
}

export async function startJob(params: StartJobRequest): Promise<OwnedJob> {
  const { worker_id, command, name, expected_seconds, priority, priority_note,
    deadline_at, no_pty = false, sync, sync_timeout } = params;
  return apiFetch<OwnedJob>("/api/v1/jobs", {
    method: "POST",
    body: JSON.stringify({ worker_id, command, name, expected_seconds, priority,
      priority_note, deadline_at, no_pty, sync, sync_timeout }),
  });
}

export async function listQueue(workerId?: string): Promise<QueueBoard> {
  const cleanWorkerId = workerId === undefined
    ? undefined
    : unwrapDoubleStringified(workerId) as string;
  const query = cleanWorkerId
    ? "?worker_id=" + encodeURIComponent(cleanWorkerId)
    : "";
  return apiFetch<QueueBoard>("/api/v1/jobs/queue" + query);
}

export async function enqueueJob(params: EnqueueJobRequest): Promise<OwnedJob> {
  const { worker_id, command, name, expected_seconds, gpu_count, priority,
    priority_note, deadline_at, no_pty = false } = params;
  return apiFetch<OwnedJob>("/api/v1/jobs/queue", {
    method: "POST",
    body: JSON.stringify({ worker_id, command, name, expected_seconds, gpu_count,
      priority, priority_note, deadline_at, no_pty }),
  });
}

export async function updateQueuedJob(
  id: string,
  params: UpdateQueuedJobRequest,
): Promise<OwnedJob> {
  const cleanId = unwrapDoubleStringified(id) as string;
  const { worker_id, name, expected_seconds, gpu_count, priority, priority_note, deadline_at } = params;
  return apiFetch<OwnedJob>(
    "/api/v1/jobs/" + encodeURIComponent(cleanId) + "/queue",
    { method: "PATCH", body: JSON.stringify({ worker_id, name, expected_seconds,
      gpu_count, priority, priority_note, deadline_at }) },
  );
}

export async function updateJob(id: string, params: UpdateJobRequest): Promise<OwnedJob> {
  const { expected_seconds, priority, priority_note, deadline_at } = params;
  return apiFetch<OwnedJob>("/api/v1/jobs/" + encodeURIComponent(unwrapDoubleStringified(id) as string), {
    method: "PATCH", body: JSON.stringify({ expected_seconds, priority, priority_note, deadline_at }),
  });
}

export async function getJob(id: string): Promise<OwnedJob> {
  return apiFetch<OwnedJob>("/api/v1/jobs/" + encodeURIComponent(unwrapDoubleStringified(id) as string));
}

export async function previewQueueMove(id: string, params: {
  before_job_id: string | null; expected_revision: number; reason: string;
}): Promise<QueuePreview> {
  return apiFetch<QueuePreview>("/api/v1/jobs/" + encodeURIComponent(unwrapDoubleStringified(id) as string) + "/queue/preview", {
    method: "POST", body: JSON.stringify(params),
  });
}

export async function moveQueueJob(id: string, params: {
  preview_id: string; expected_revision: number;
}): Promise<QueueMoveResult> {
  return apiFetch<QueueMoveResult>("/api/v1/jobs/" + encodeURIComponent(unwrapDoubleStringified(id) as string) + "/queue/move", {
    method: "POST", body: JSON.stringify(params),
  });
}

export async function getQueueHistory(filters: {
  worker_id?: string; job_id?: string; before_event_id?: string; limit?: number;
} = {}): Promise<QueueHistory> {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined) query.set(key, String(value));
  }
  return apiFetch<QueueHistory>("/api/v1/jobs/queue/history" + (query.size ? "?" + query : ""));
}

export async function getUsage(filters: { worker_id?: string; user_id?: string } = {}): Promise<UsageResult> {
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(filters)) {
    if (value !== undefined) query.set(key, value);
  }
  return apiFetch<UsageResult>("/api/v1/usage" + (query.size ? "?" + query : ""));
}

export async function listJobs(filters?: {
  worker_id?: string;
  status?: string;
  origin_session_id?: string;
}): Promise<OwnedJob[]> {
  const params = new URLSearchParams();
  if (filters?.worker_id) params.set("worker_id", filters.worker_id);
  if (filters?.status) params.set("status", filters.status);
  if (filters?.origin_session_id) params.set("origin_session_id", filters.origin_session_id);
  const qs = params.toString() ? "?" + params.toString() : "";
  const path = "/api/v1/jobs" + qs;
  const key = getCredentialIdentity() + "\n" + getOrchestratorUrl() + "\n" + path;
  const existing = inFlightJobLists.get(key);
  if (existing) return existing;

  const request = apiFetch<OwnedJob[]>(path);
  inFlightJobLists.set(key, request);
  try {
    return await request;
  } finally {
    if (inFlightJobLists.get(key) === request) {
      inFlightJobLists.delete(key);
    }
  }
}

export async function getJobLogs(id: string, opts?: { tail?: number; head?: number }): Promise<JobLogsResult> {
  const cleanId = unwrapDoubleStringified(id) as string;
  const params = new URLSearchParams();
  if (opts?.tail !== undefined) params.set("tail", String(opts.tail));
  if (opts?.head !== undefined) params.set("head", String(opts.head));
  const qs = params.toString() ? "?" + params.toString() : "";
  return apiFetch<JobLogsResult>("/api/v1/jobs/" + encodeURIComponent(cleanId) + "/logs" + qs);
}

export async function stopJob(id: string): Promise<StopJobResult> {
  const cleanId = unwrapDoubleStringified(id) as string;
  return apiFetch<StopJobResult>("/api/v1/jobs/" + encodeURIComponent(cleanId), { method: "DELETE" });
}

export async function addTunnel(params: AddTunnelRequest): Promise<Tunnel> {
  return apiFetch<Tunnel>("/api/v1/tunnels", { method: "POST", body: JSON.stringify(params) });
}

export async function listTunnels(): Promise<Tunnel[]> {
  return apiFetch<Tunnel[]>("/api/v1/tunnels");
}

export async function removeTunnel(id: string): Promise<RemoveTunnelResponse> {
  return apiFetch<RemoveTunnelResponse>("/api/v1/tunnels/" + encodeURIComponent(id), { method: "DELETE" });
}

export async function uploadFile(
  workerId: string,
  path: string,
  contentB64: string,
): Promise<FileTransferResult> {
  const cleanId = unwrapDoubleStringified(workerId) as string;
  return apiFetch<FileTransferResult>(
    "/api/v1/workers/" + encodeURIComponent(cleanId) + "/files",
    { method: "POST", body: JSON.stringify({ path, content_b64: contentB64 }) },
  );
}

export async function downloadFile(
  workerId: string,
  path: string,
  maxBytes?: number,
): Promise<FileTransferResult> {
  const cleanId = unwrapDoubleStringified(workerId) as string;
  const params = new URLSearchParams({ path });
  if (maxBytes !== undefined) params.set("max_bytes", String(maxBytes));
  return apiFetch<FileTransferResult>(
    "/api/v1/workers/" + encodeURIComponent(cleanId) + "/files?" + params.toString(),
  );
}
