import { afterEach, beforeAll, beforeEach, expect, mock, test } from "bun:test";
import { getOrchestratorUrl, setOrchestratorUrl } from "./api.ts";
import type { ClusterTarget, ComputeTarget } from "./types.ts";
import type { registerGroupedTools as RegisterGroupedTools } from "./tools/grouped.ts";

mock.module("@earendil-works/pi-tui", () => ({ Text: class {
  constructor(public text = "") {} setText(text: string) { this.text = text; }
} }));

interface TestDetails {
  targets: ComputeTarget[];
  summary: { cluster_targets: ClusterTarget[] };
  cluster_targets: ClusterTarget[];
  jobs: Array<{ id: string }>;
  workers: Array<{ id: string }>;
}
interface Tool {
  execute: (id: string, params: object, signal?: AbortSignal, update?: unknown, context?: unknown) =>
    Promise<{ content: Array<{ text: string }>; details: TestDetails }>;
}
const gpu: ClusterTarget = {
  id: "cluster:test", name: "test", kind: "cluster", cluster_id: "test",
  compute_kind: "gpu", partition: "gpu", description: "Preemptible account capacity",
  shared_path: "/data/shared/test", resource_classes: { gpu_type: "a100", max_gpus: 8, max_walltime_seconds: 600 },
  availability: { status: "unknown", observed_at: null, detail: "No inventory cached" },
};
const cpu: ClusterTarget = { ...gpu, id: "cluster:cpu", name: "cpu", cluster_id: "cpu", compute_kind: "cpu", partition: "configured-cpu", resource_classes: { max_gpus: 0, max_walltime_seconds: 600 } };
const worker = { id: "real", name: "allocated", status: "offline", gpu_names: [], gpu_count: 0, slurm: { cluster_id: "test", scheduler_state: "PENDING" } };
const originalFetch = globalThis.fetch;
const originalUrl = getOrchestratorUrl();
const envKeys = ["WH_SESSION_ROLE", "WH_ORCHESTRATOR_URL"];
const originalEnv = Object.fromEntries(envKeys.map((key) => [key, process.env[key]]));
let register: typeof RegisterGroupedTools;
let tools: Record<string, Tool>;
let calls: Array<{ operation: string }>;
let requests: Array<{ url: string }>;
const ctx = { sessionManager: { getSessionId: () => "session-test" } };
const options = { project_path: "/data/shared/test/project", environment_path: "/data/shared/test/env", walltime_seconds: 300 };

// Host-only UI modules must be mocked before the grouped module is loaded.
beforeAll(async () => { ({ registerGroupedTools: register } = await import("./tools/grouped.ts")); });
beforeEach(() => {
  for (const key of envKeys) delete process.env[key];
  setOrchestratorUrl("http://grouped.test");
  calls = []; requests = []; tools = {};
  const fake = {
    async listClusterTargets() { calls.push({ operation: "list" }); return [gpu, cpu]; },
    async getClusterTarget(): Promise<ClusterTarget> { calls.push({ operation: "refresh" }); return { ...gpu, availability: { status: "fresh", observed_at: 100, detail: "busy" } }; },
    async enqueueCluster() {
      calls.push({ operation: "enqueue" });
      return { job_id: "initial", worker_id: "real" };
    },
    async readClusterJobLogs() {
      calls.push({ operation: "logs" });
      return { job_id: "initial", tail: 50, head: null, logs: "retained batch log" };
    },
    async stopClusterJob() { calls.push({ operation: "stop" }); return { requested: true }; },
  };
  register({ registerTool(tool: Tool & { name: string }) { tools[tool.name] = tool; } } as never, {
    getContext: async () => ({ client_kind: "omp", session_id: "session-test", recipient_id: "recipient-test" }),
    slurmClient: fake,
  });
  globalThis.fetch = (async (input, init) => {
    const url = String(input); requests.push({ url });
    let body: unknown = [];
    if (url.endsWith("/workers")) body = [worker, { ...worker, id: "unrelated", slurm: { cluster_id: "other" } }];
    else if (url.endsWith("/workers/summary")) body = { total: 2, online: 0, offline: 2, draining: 0 };
    else if (url.endsWith("/workers/real")) body = worker;
    else if (url.includes("/logs")) return new Response("offline", { status: 503 });
    else if (init?.method === "POST") body = { id: "ordinary", worker_id: "real", name: "work", status: "pending" };
    else if (url.includes("/jobs")) body = [{ id: "mine", worker_id: "real", status: "pending", gpu_count: 0, position: 1, gpu_indices: [] }, { id: "other", worker_id: "unrelated", status: "pending" }];
    return Response.json(body);
  }) as typeof fetch;
});
afterEach(() => {
  globalThis.fetch = originalFetch; setOrchestratorUrl(originalUrl);
  for (const key of envKeys) {
    if (originalEnv[key] === undefined) delete process.env[key]; else process.env[key] = originalEnv[key];
  }
});

async function run(tool: string, params: object) { return tools[tool].execute("call", params, undefined, undefined, ctx); }

test("discovery merges truthful cached targets and refreshes only selected target", async () => {
  const overview = await run("wh_read", { action: "list_workers" });
  expect(overview.details.targets.map((target) => target.kind)).toEqual(["worker", "worker", "cluster", "cluster"]);
  expect(overview.details.targets[2]).not.toHaveProperty("status");
  expect(overview.content[0].text).toContain("scheduler-backed");
  expect(calls.some((call) => call.operation === "refresh")).toBeFalse();
  await run("wh_read", { action: "get_worker", worker_id: gpu.id });
  await run("wh_read", { action: "get_worker_summary", worker_id: gpu.id });
  expect(calls.filter((call) => call.operation === "refresh")).toHaveLength(2);
  const summary = await run("wh_read", { action: "get_worker_summary" });
  expect(summary.details.summary.cluster_targets).toHaveLength(2);
  const available = await run("wh_read", { action: "available_gpus" });
  expect(available.details.cluster_targets).toEqual([gpu]);
  expect(available.content[0].text).toContain("not immediately free workers");
  expect(available.content[0].text).not.toContain("cluster:cpu");
});

test("cluster enqueue routes locally rather than calling the worker queue API and requires Slurm options", async () => {
  await run("wh_dispatch", { action: "enqueue", worker_id: gpu.id, command: "python train.py", name: "train", expected_seconds: 50, slurm: options });
  expect(calls.filter((call) => call.operation === "enqueue")).toHaveLength(1);
  expect(requests).toHaveLength(0);
  await expect(run("wh_dispatch", { action: "enqueue", worker_id: gpu.id, command: "id", name: "x", expected_seconds: 1 })).rejects.toThrow("slurm");
});

test("ordinary enqueue uses the worker queue API and rejects cluster-only options", async () => {
  await run("wh_dispatch", { action: "enqueue", worker_id: "real", command: "uv sync", name: "work", expected_seconds: 20, gpu_count: 0 });
  expect(requests.map((request) => request.url)).toEqual(["http://grouped.test/api/v1/jobs/queue"]);
  expect(calls.some((call) => call.operation === "enqueue")).toBeFalse();
  await expect(run("wh_dispatch", { action: "enqueue", worker_id: "real", command: "id", name: "x", expected_seconds: 1, slurm: options })).rejects.toThrow("only for cluster");
  expect(requests).toHaveLength(1);
});

test("cluster IDs never reach exec, file, data, tunnel or Marimo APIs", async () => {
  for (const action of ["exec", "add_tunnel", "list_marimo", "start_marimo", "get_marimo", "execute_marimo", "upload_file", "download_file", "update_queued_job"]) {
    await expect(run("wh_dispatch", { action, worker_id: gpu.id })).rejects.toThrow("CLUSTER_REQUIRES_ENQUEUE");
  }
  await expect(run("wh_dispatch", { action: "data_copy", src_worker: "real", dst_worker: gpu.id })).rejects.toThrow("CLUSTER_REQUIRES_ENQUEUE");
  expect(requests).toHaveLength(0);
});

test.each([gpu.id, JSON.stringify(gpu.id)])("cluster job and queue views select actual allocation work for target %s", async (worker_id) => {
  const jobs = await run("wh_read", { action: "list_jobs", worker_id, status: "pending" });
  const queue = await run("wh_read", { action: "list_queue", worker_id });
  expect(jobs.details.jobs.map((job) => job.id)).toEqual(["mine"]);
  expect(queue.details.jobs.map((job) => job.id)).toEqual(["mine"]);
  expect(queue.details.workers.map((item) => item.id)).toEqual(["real"]);
  expect(requests.every((request) => !request.url.includes("cluster%3A"))).toBeTrue();
  expect(calls.some((call) => call.operation === "refresh")).toBeFalse();
});

test("offline log follow reports a bounded snapshot, while allocation stop remains requested rather than confirmed", async () => {
  await run("wh_read", { action: "get_job_logs", job_id: "initial", head: 5 });
  expect(calls.filter((call) => call.operation === "logs")).toHaveLength(1);
  const follow = await run("wh_read", { action: "get_job_logs", job_id: "initial", follow: true });
  expect(follow.content[0].text).toContain("Bounded shared log snapshot");
  expect(requests).toHaveLength(1);
  const stopped = await run("wh_dispatch", { action: "stop_job", job_id: "initial", scope: "allocation", candidate_job_id: "123" });
  expect(stopped.content[0].text).toContain("awaiting terminal evidence");
});
