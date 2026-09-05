import { afterEach, beforeEach, expect, test } from "bun:test";
import {
  ApiError,
  enqueueJob,
  getOrchestratorUrl,
  listQueue,
  setOrchestratorUrl,
  stopJob,
  updateQueuedJob,
} from "./api.ts";

const originalFetch = globalThis.fetch;
const originalUrl = getOrchestratorUrl();
let requests: Array<{ url: string; init?: RequestInit }>;
let responseBody: unknown;
let responseStatus: number;

beforeEach(() => {
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
