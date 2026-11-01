import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  addTunnel,
  createMarimo,
  getMarimo,
  listMarimo,
  listTunnels,
  removeMarimo,
  removeTunnel,
  ApiError,
  askPm,
  dispatchTask,
  enqueueJob,
  getOrchestratorUrl,
  getAuthorizationHeaders,
  getCredentialIdentity,
  getJob,
  getQueueHistory,
  getUsage,
  isInternalSession,
  listJobs,
  moveQueueJob,
  previewQueueMove,
  startJob,
  updateJob,
  getPiSession,
  listProjects,
  listQueue,
  notifyPm,
  sendProjectMessage,
  sendSessionMessage,
  sendOrchestratorMessage,
  setOrchestratorUrl,
  stopJob,
  submitPr,
  teardownTask,
  updateQueuedJob,
} from "./api.ts";

const originalFetch = globalThis.fetch;
const originalUrl = getOrchestratorUrl();
const environmentKeys = ["WH_SESSION_TOKEN", "WH_SESSION_ROLE", "WH_API_KEY", "WH_API_KEY_FILE", "WH_ORCHESTRATOR_URL"] as const;
const originalEnvironment = Object.fromEntries(environmentKeys.map((key) => [key, process.env[key]]));
let requests: Array<{ url: string; init?: RequestInit }>;
let responseBody: unknown;
let responseStatus: number;

beforeEach(() => {
  for (const key of environmentKeys) delete process.env[key];
  process.env.WH_API_KEY = "whk_test";
  requests = [];
  responseBody = [];
  responseStatus = 200;
  setOrchestratorUrl("http://queue.test");
  globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
    requests.push({ url: String(input), init });
    return new Response(JSON.stringify(responseBody), {
      status: responseStatus,
      headers: { "Content-Type": "application/json" },
    });
  }) as typeof fetch;
});

afterEach(() => {
  globalThis.fetch = originalFetch;
  setOrchestratorUrl(originalUrl);
  for (const key of environmentKeys) {
    if (originalEnvironment[key] === undefined) delete process.env[key];
    else process.env[key] = originalEnvironment[key];
  }
});

test("listQueue serializes the optional worker query exactly", async () => {
  responseBody = { workers: [{ worker_id: "worker-1", worker_name: "compute",
    revision: 2, jobs: [], cpu_jobs: [] }], as_of: 123 };
  expect(await listQueue("worker/id with spaces")).toEqual(responseBody);
  await listQueue();

  expect(requests.map(({ url }) => url)).toEqual([
    "http://queue.test/api/v1/jobs/queue?worker_id=worker%2Fid%20with%20spaces",
    "http://queue.test/api/v1/jobs/queue",
  ]);
  expect(requests[0].init?.method).toBeUndefined();
});

test("unscoped session tokens are not ordinary user credentials", async () => {
  delete process.env.WH_API_KEY;
  await expect(listQueue()).rejects.toThrow("WH_API_KEY");
  process.env.WH_SESSION_TOKEN = "unscoped-secret";
  await expect(listQueue()).rejects.toThrow("WH_API_KEY");
  expect(requests).toHaveLength(0);
  process.env.WH_SESSION_ROLE = "task";
  await listQueue();
  expect(new Headers(requests[0].init?.headers).get("Authorization")).toBe("Bearer unscoped-secret");
});

test("environment pins the service for API calls even after setting a user default", async () => {
  process.env.WH_ORCHESTRATOR_URL = " http://fleet.test/// ";
  setOrchestratorUrl("http://other.test");
  await listQueue();
  expect(requests[0].url).toBe("http://fleet.test/api/v1/jobs/queue");
  delete process.env.WH_ORCHESTRATOR_URL;
  await listQueue();
  expect(requests[1].url).toBe("http://other.test/api/v1/jobs/queue");
});

test.each(["orchestrator", "pm", "task"])("%s requires its session token and never falls back to a user key", async (role) => {
  process.env.WH_API_KEY = "whk_user";
  await listQueue();
  process.env.WH_SESSION_ROLE = role;
  expect(isInternalSession()).toBeTrue();
  await expect(listQueue()).rejects.toThrow("WH_SESSION_TOKEN is required");
  expect(requests).toHaveLength(1);
  process.env.WH_SESSION_TOKEN = "task-secret";
  await listQueue();
  expect(requests.map(({ init }) => new Headers(init?.headers).get("Authorization"))).toEqual([
    "Bearer whk_user", "Bearer task-secret",
  ]);
});

test("API key environment wins over file and file rotation changes identity", async () => {
  const directory = await mkdtemp(join(tmpdir(), "wh-api-key-"));
  try {
    delete process.env.WH_API_KEY;
    process.env.WH_API_KEY_FILE = join(directory, "key");
    await writeFile(process.env.WH_API_KEY_FILE, "whk_file\n");
    expect(await getAuthorizationHeaders()).toEqual({ Authorization: "Bearer whk_file" });
    const first = getCredentialIdentity();
    await writeFile(process.env.WH_API_KEY_FILE, "whk_rotated\n");
    expect(getCredentialIdentity()).not.toBe(first);
    expect(await getAuthorizationHeaders()).toEqual({ Authorization: "Bearer whk_rotated" });
    process.env.WH_API_KEY = " whk_env ";
    expect(await getAuthorizationHeaders()).toEqual({ Authorization: "Bearer whk_env" });
    expect(getCredentialIdentity()).not.toBe(first);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("unknown internal roles are configuration errors", async () => {
  process.env.WH_SESSION_ROLE = "typo";
  process.env.WH_API_KEY = "whk_user";
  await expect(getAuthorizationHeaders()).rejects.toThrow("WH_SESSION_ROLE");
  expect(requests).toHaveLength(0);
});

test("PM escalation uses the lazy orchestrator mailbox rather than a cached session", async () => {
  await sendOrchestratorMessage("Need an operator decision");
  expect(requests.map(({ url, init }) => [url, init?.method, JSON.parse(String(init?.body))])).toEqual([
    ["http://queue.test/api/v1/pi/orchestrator:send", "POST", { message: "Need an operator decision" }],
  ]);
});

test("fleet helpers use the hierarchical session and project routes", async () => {
  responseBody = { id: "session-1" };
  await getPiSession("session/1");
  await askPm("task/1", "Which database?");
  await notifyPm("task/1", "Ready for review");
  await dispatchTask("my/project", "feature", "Implement it");
  await sendSessionMessage("task/1", "Use SQLite");
  await submitPr("task/1", "six sections");
  await teardownTask("task/1", true);
  await listProjects();
  await sendProjectMessage("my/project", "Please investigate");

  expect(requests.map(({ url, init }) => [
    url,
    init?.method,
    init?.body === undefined ? undefined : JSON.parse(String(init.body)),
  ])).toEqual([
    ["http://queue.test/api/v1/pi/sessions/session%2F1", undefined, undefined],
    ["http://queue.test/api/v1/pi/sessions/task%2F1:ask-pm", "POST", { question: "Which database?" }],
    ["http://queue.test/api/v1/pi/sessions/task%2F1:notify-pm", "POST", { note: "Ready for review" }],
    ["http://queue.test/api/v1/pi/projects/my%2Fproject/tasks", "POST", { branch: "feature", briefing: "Implement it" }],
    ["http://queue.test/api/v1/pi/sessions/task%2F1:send", "POST", { message: "Use SQLite" }],
    ["http://queue.test/api/v1/pi/sessions/task%2F1:submit-pr", "POST", { summary: "six sections" }],
    ["http://queue.test/api/v1/pi/sessions/task%2F1:teardown", "POST", { force: true }],
    ["http://queue.test/api/v1/pi/projects", undefined, undefined],
    ["http://queue.test/api/v1/pi/projects/my%2Fproject:send", "POST", { message: "Please investigate" }],
  ]);
});

test("enqueueJob sends the exact queue POST body with defaults", async () => {
  responseBody = { id: "job-1" };
  await enqueueJob({
    worker_id: "worker-1",
    command: "python train.py",
    name: "train",
    expected_seconds: 900,
    gpu_count: 1,
  });

  expect(requests[0].url).toBe("http://queue.test/api/v1/jobs/queue");
  expect(requests[0].init?.method).toBe("POST");
  expect(JSON.parse(String(requests[0].init?.body))).toEqual({
    worker_id: "worker-1",
    command: "python train.py",
    name: "train",
    expected_seconds: 900,
    gpu_count: 1,
    no_pty: false,
  });
});

test("pending metadata updates never use positional queue movement", async () => {
  responseBody = { id: "job-1" };
  await updateQueuedJob('"job-1"', {
    worker_id: "worker-2",
    name: "renamed",
    expected_seconds: 120,
    gpu_count: 2,
  });

  expect(requests[0].url).toBe("http://queue.test/api/v1/jobs/job-1/queue");
  expect(requests[0].init?.method).toBe("PATCH");
  expect(JSON.parse(String(requests[0].init?.body))).toEqual({
    worker_id: "worker-2",
    name: "renamed",
    expected_seconds: 120,
    gpu_count: 2,
  });
});

test("queue API errors retain structured status, code, and detail", async () => {
  responseStatus = 409;
  responseBody = {
    detail: { code: "QUEUE_CHANGED", message: "job already started" },
  };

  try {
    await updateQueuedJob("job-1", { name: "late rename" });
    throw new Error("expected update to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(ApiError);
    const apiError = error as ApiError;
    expect(apiError.status).toBe(409);
    expect(apiError.code).toBe("QUEUE_CHANGED");
    expect(apiError.message).toBe("[409] QUEUE_CHANGED: job already started");
    expect(apiError.detail).toEqual({ code: "QUEUE_CHANGED", message: "job already started" });
  }
});

test("stopJob reuses DELETE for pending and running queue jobs", async () => {
  responseBody = { job_id: "pending", stopped: true, already_terminal: false, status: "failed" };
  await stopJob("pending");
  responseBody = { job_id: "running", stopped: true, already_terminal: false, status: "failed" };
  await stopJob("running");

  expect(requests.map(({ url, init }) => [url, init?.method])).toEqual([
    ["http://queue.test/api/v1/jobs/pending", "DELETE"],
    ["http://queue.test/api/v1/jobs/running", "DELETE"],
  ]);
});

test("CPU execution includes required metadata and excludes GPU allocation", async () => {
  await startJob({ worker_id: "worker-1", command: "setup", name: "prepare",
    expected_seconds: 60, priority: "blocking", priority_note: "prepare inputs",
    deadline_at: null, sync: true, sync_timeout: 30 });
  expect(requests[0].url).toBe("http://queue.test/api/v1/jobs");
  expect(JSON.parse(String(requests[0].init?.body))).toEqual({
    worker_id: "worker-1", command: "setup", name: "prepare", expected_seconds: 60,
    priority: "blocking", priority_note: "prepare inputs", deadline_at: null,
    no_pty: false, sync: true, sync_timeout: 30,
  });
});

test("active metadata and owned details use private job routes", async () => {
  await updateJob("job/one", { expected_seconds: 300, priority: "normal", deadline_at: null });
  await getJob("job/one");
  expect(requests.map(({ url, init }) => [url, init?.method])).toEqual([
    ["http://queue.test/api/v1/jobs/job%2Fone", "PATCH"],
    ["http://queue.test/api/v1/jobs/job%2Fone", undefined],
  ]);
  expect(JSON.parse(String(requests[0].init?.body))).toEqual({
    expected_seconds: 300, priority: "normal", deadline_at: null,
  });
});

test("queue movement uses preview and explicit revision commit without retries", async () => {
  await previewQueueMove("job/one", { before_job_id: null, expected_revision: 4, reason: "Deadline" });
  await moveQueueJob("job/one", { preview_id: "preview", expected_revision: 4 });
  expect(requests.map(({ url, init }) => [url, init?.method, JSON.parse(String(init?.body))])).toEqual([
    ["http://queue.test/api/v1/jobs/job%2Fone/queue/preview", "POST",
      { before_job_id: null, expected_revision: 4, reason: "Deadline" }],
    ["http://queue.test/api/v1/jobs/job%2Fone/queue/move", "POST",
      { preview_id: "preview", expected_revision: 4 }],
  ]);
  responseStatus = 409;
  responseBody = { detail: { code: "PREVIEW_EXPIRED", message: "Preview expired" } };
  await expect(moveQueueJob("job/one", { preview_id: "preview", expected_revision: 4 }))
    .rejects.toMatchObject({ status: 409, code: "PREVIEW_EXPIRED" });
  expect(requests).toHaveLength(3);
});

test("usage and queue history encode supported filters", async () => {
  await getUsage({ worker_id: "worker/one", user_id: "user two" });
  await getQueueHistory({ worker_id: "worker/one", job_id: "job", before_event_id: "event", limit: 20 });
  expect(requests.map(({ url }) => url)).toEqual([
    "http://queue.test/api/v1/usage?worker_id=worker%2Fone&user_id=user+two",
    "http://queue.test/api/v1/jobs/queue/history?worker_id=worker%2Fone&job_id=job&before_event_id=event&limit=20",
  ]);
});

test("in-flight owned lists do not cross credential identities", async () => {
  const releases: Array<(response: Response) => void> = [];
  globalThis.fetch = ((input: string | URL | Request, init?: RequestInit) => {
    requests.push({ url: String(input), init });
    return new Promise<Response>((resolve) => releases.push(resolve));
  }) as typeof fetch;
  process.env.WH_API_KEY = "whk_alice";
  const alice = listJobs();
  const aliceDuplicate = listJobs();
  process.env.WH_API_KEY = "whk_bob";
  const bob = listJobs();
  // Authentication resolves asynchronously, but request identities are captured at invocation.
  await Promise.resolve();
  expect(requests.map(({ init }) => new Headers(init?.headers).get("Authorization")))
    .toEqual(["Bearer whk_alice", "Bearer whk_bob"]);
  releases[0](Response.json([{ id: "alice-job" }]));
  releases[1](Response.json([{ id: "bob-job" }]));
  expect(await alice).toEqual([{ id: "alice-job" }]);
  expect(await aliceDuplicate).toEqual([{ id: "alice-job" }]);
  expect(await bob).toEqual([{ id: "bob-job" }]);
});

test("owned tunnel and managed Marimo methods use owner-scoped routes", async () => {
  process.env.WH_API_KEY = "whk_owner";
  await addTunnel({ worker_id: "worker", local_port: 12000, remote_port: 8080, name: "service" });
  await listTunnels();
  await removeTunnel("tunnel/one");
  await createMarimo({ worker_id: "worker", notebook_path: "/work/notebook.py",
    environment: "/work/.venv", ready_timeout: 20 });
  await listMarimo("worker/one");
  await getMarimo("session/one");
  await removeMarimo("session/one");
  expect(requests.map(({ url, init }) => [url, init?.method])).toEqual([
    ["http://queue.test/api/v1/tunnels", "POST"],
    ["http://queue.test/api/v1/tunnels", undefined],
    ["http://queue.test/api/v1/tunnels/tunnel%2Fone", "DELETE"],
    ["http://queue.test/api/v1/marimo", "POST"],
    ["http://queue.test/api/v1/marimo?worker_id=worker%2Fone", undefined],
    ["http://queue.test/api/v1/marimo/session%2Fone", undefined],
    ["http://queue.test/api/v1/marimo/session%2Fone", "DELETE"],
  ]);
  expect(JSON.parse(String(requests[0].init?.body))).toEqual({
    worker_id: "worker", local_port: 12000, remote_port: 8080, name: "service",
  });
  expect(JSON.parse(String(requests[3].init?.body))).toEqual({
    worker_id: "worker", notebook_path: "/work/notebook.py", environment: "/work/.venv",
    ready_timeout: 20,
  });
  expect(requests.every(({ init }) => new Headers(init?.headers).get("Authorization") === "Bearer whk_owner")).toBeTrue();
});

test("missing and empty ordinary credentials fail before any HTTP request", async () => {
  const directory = await mkdtemp(join(tmpdir(), "wh-empty-key-"));
  try {
    delete process.env.WH_API_KEY;
    await expect(getAuthorizationHeaders()).rejects.toThrow("WH_API_KEY");
    await expect(listQueue()).rejects.toThrow("WH_API_KEY");
    process.env.WH_API_KEY = " \n ";
    process.env.WH_API_KEY_FILE = join(directory, "key");
    await writeFile(process.env.WH_API_KEY_FILE, " \n ");
    await expect(getAuthorizationHeaders()).rejects.toThrow("nonempty WH_API_KEY_FILE");
    await expect(listQueue()).rejects.toThrow("nonempty WH_API_KEY_FILE");
    expect(requests).toHaveLength(0);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
