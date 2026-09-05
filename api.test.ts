import { afterEach, beforeEach, expect, test } from "bun:test";
import {
  ApiError,
  askPm,
  dispatchTask,
  enqueueJob,
  getOrchestratorUrl,
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
const environmentKeys = ["WH_SESSION_TOKEN", "WH_SESSION_ROLE", "WH_OPERATOR_TOKEN", "WH_OPERATOR_TOKEN_FILE", "WH_ORCHESTRATOR_URL"] as const;
const originalEnvironment = Object.fromEntries(environmentKeys.map((key) => [key, process.env[key]]));
let requests: Array<{ url: string; init?: RequestInit }>;
let responseBody: unknown;
let responseStatus: number;

beforeEach(() => {
  for (const key of environmentKeys) delete process.env[key];
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
  await listQueue("worker/id with spaces");
  await listQueue();

  expect(requests.map(({ url }) => url)).toEqual([
    "http://queue.test/api/v1/jobs/queue?worker_id=worker%2Fid%20with%20spaces",
    "http://queue.test/api/v1/jobs/queue",
  ]);
  expect(requests[0].init?.method).toBeUndefined();
});

test("API requests add the session bearer token only when configured", async () => {
  delete process.env.WH_SESSION_TOKEN;
  await listQueue();
  expect(new Headers(requests[0].init?.headers).has("Authorization")).toBeFalse();

  process.env.WH_SESSION_TOKEN = "session-secret";
  await listQueue();
  expect(new Headers(requests[1].init?.headers).get("Authorization")).toBe(
    "Bearer session-secret",
  );
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

test("ordinary operators authenticate but a fleet role never inherits operator authority", async () => {
  process.env.WH_OPERATOR_TOKEN = "operator-secret";
  await listQueue();
  process.env.WH_SESSION_ROLE = "task";
  await listQueue();
  process.env.WH_SESSION_TOKEN = "task-secret";
  await listQueue();
  expect(requests.map(({ init }) => new Headers(init?.headers).get("Authorization"))).toEqual([
    "Bearer operator-secret", null, "Bearer task-secret",
  ]);
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

test("updateQueuedJob sends only supplied one-based queue fields", async () => {
  responseBody = { id: "job-1", position: 2 };
  await updateQueuedJob('"job-1"', {
    worker_id: "worker-2",
    position: 2,
    name: "renamed",
    expected_seconds: 120,
    gpu_count: 2,
  });

  expect(requests[0].url).toBe("http://queue.test/api/v1/jobs/job-1/queue");
  expect(requests[0].init?.method).toBe("PATCH");
  expect(JSON.parse(String(requests[0].init?.body))).toEqual({
    worker_id: "worker-2",
    position: 2,
    name: "renamed",
    expected_seconds: 120,
    gpu_count: 2,
  });
});

test("queue API errors retain structured status, code, and detail", async () => {
  responseStatus = 409;
  responseBody = {
    error: {
      code: "QUEUE_CONFLICT",
      message: "job already started",
      detail: { status: "running" },
    },
  };

  try {
    await updateQueuedJob("job-1", { name: "late rename" });
    throw new Error("expected update to fail");
  } catch (error) {
    expect(error).toBeInstanceOf(ApiError);
    const apiError = error as ApiError;
    expect(apiError.status).toBe(409);
    expect(apiError.code).toBe("QUEUE_CONFLICT");
    expect(apiError.message).toBe("[409] QUEUE_CONFLICT: job already started");
    expect(apiError.detail).toEqual({ status: "running" });
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
