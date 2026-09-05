import { expect, test } from "bun:test";
import {
  availableGpuWorkers,
  buildDataCatalog,
  formatDataCatalog,
  formatAvailableGpus,
  formatQueueOverview,
  formatWorkerOverview,
} from "./agent-output.ts";
import type { DataPaths, QueuedJob, Worker } from "./types.ts";

function worker(patch: Partial<Worker>): Worker {
  return {
    id: "aaaaaaaa-0000-0000-0000-000000000001",
    name: "worker-a",
    worker_ip: "100.64.0.1",
    dns_name: "worker-a.hs.example",
    ssh_user: "root",
    harness_dir: "/harness",
    gpu_count: 1,
    gpu_names: ["NVIDIA A100-SXM4-80GB"],
    gpu_vram_gb: [80],
    gpu_used_vram_gb: [0],
    gpu_busy: [false],
    cpu_cores: 8,
    total_ram_gb: 64,
    used_ram_gb: 8,
    total_disk_gb: 100,
    used_disk_gb: 10,
    status: "online",
    last_heartbeat_ts: 0,
    created_at: 0,
    ...patch,
  };
}

function queuedJob(patch: Partial<QueuedJob>): QueuedJob {
  return {
    id: "job-1",
    worker_id: "aaaaaaaa-0000-0000-0000-000000000001",
    worker_name: "worker-a",
    name: "experiment",
    tmux_session: "wh_job-1",
    command: "python train.py",
    status: "pending",
    queue_managed: true,
    exit_code: null,
    pty_enabled: true,
    kind: "ssh",
    origin_session_id: null,
    report_revision: 0,
    expected_seconds: 60,
    gpu_count: 1,
    gpu_indices: [],
    queue_order: 1,
    position: 1,
    started_at: 0,
    finished_at: 0,
    ...patch,
  };
}

test("formats compact fleet and available-GPU rows", () => {
  const workers = [
    worker({}),
    worker({
      id: "bbbbbbbb-0000-0000-0000-000000000002",
      name: "worker-b",
      dns_name: "worker-b.hs.example",
      gpu_count: 2,
      gpu_names: ["NVIDIA RTX A6000", "NVIDIA RTX A6000"],
      gpu_vram_gb: [48, 48],
      gpu_used_vram_gb: [30, 30],
      gpu_busy: [true, true],
    }),
    worker({
      id: "cccccccc-0000-0000-0000-000000000003",
      name: "worker-c",
      dns_name: "worker-c.hs.example",
      status: "offline",
      gpu_busy: [false],
    }),
  ];

  expect(formatWorkerOverview(workers)).toContain(
    "online   | aaaaaaaa-0000-0000-0000-000000000001 | worker-a | worker-a.hs.example | A100 0/1",
  );
  expect(formatWorkerOverview(workers)).toContain("RTX A6000 2/2");
  expect(availableGpuWorkers(workers).map(({ name }) => name)).toEqual(["worker-a"]);
  expect(formatAvailableGpus(workers)).toContain(
    "aaaaaaaa-0000-0000-0000-000000000001 | worker-a | worker-a.hs.example | A100 1/1",
  );
  expect(formatAvailableGpus(workers)).not.toContain("worker-b");
  expect(formatAvailableGpus(workers)).not.toContain("worker-c");
});

test("formats empty and active worker queues", () => {
  const online = worker({});
  expect(formatQueueOverview([], [], 100)).toBe("No online workers or active queued jobs.");
  expect(formatQueueOverview([online], [], 100)).toBe(
    "aaaaaaaa-0000-0000-0000-000000000001 | worker-a | online | GPUs A100 0/1\n"
    + "  (queue empty)",
  );
});

test("formats parallel active rows, strict pending order, and multi-GPU requests", () => {
  const gpuWorker = worker({
    gpu_count: 4,
    gpu_names: ["NVIDIA A100", "NVIDIA A100", "NVIDIA A100", "NVIDIA A100"],
    gpu_vram_gb: [80, 80, 80, 80],
    gpu_used_vram_gb: [1, 1, 1, 1],
    gpu_busy: [true, true, false, false],
  });
  const jobs = [
    queuedJob({ id: "running", name: "run", status: "running", gpu_indices: [0], started_at: 90, position: 0 }),
    queuedJob({ id: "starting", name: "start", status: "starting", gpu_indices: [1], started_at: 110, position: 0 }),
    queuedJob({ id: "head", name: "two-gpu", gpu_count: 2, queue_order: 3, position: 1 }),
    queuedJob({ id: "later", name: "later", queue_order: 8, position: 2 }),
  ];
  const output = formatQueueOverview([gpuWorker], jobs, 100);

  expect(output).toContain("GPUs A100 2/4");
  expect(output).toContain("running | running | run | GPU indices [0] | elapsed 10s / expected 60s");
  expect(output).toContain("starting | starting | start | GPU indices [1] | elapsed 0s / expected 60s");
  expect(output.indexOf("pending #1 | head | two-gpu | GPUs 2")).toBeLessThan(
    output.indexOf("pending #2 | later | later | GPUs 1"),
  );
  expect(output).not.toContain("python train.py");
});

test("includes offline and orphaned queue owners", () => {
  const offline = worker({
    id: "offline-id",
    name: "offline-worker",
    status: "offline",
  });
  const output = formatQueueOverview(
    [offline],
    [
      queuedJob({ id: "offline-job", worker_id: "offline-id", worker_name: "offline-worker" }),
      queuedJob({ id: "orphan-job", worker_id: "missing-id", worker_name: "missing-worker" }),
    ],
    100,
  );
  expect(output).toContain("offline-id | offline-worker | offline");
  expect(output).toContain("missing-id | missing-worker | absent | GPUs unknown");
});

test("deduplicates worker identities and filters paths by query", () => {
  const data: DataPaths = {
    "/code/DRRT": [
      { worker_id: "aaaaaaaa-1111-0000-0000-000000000001", worker_name: "worker-a" },
      { worker_id: "aaaaaaaa-2222-0000-0000-000000000002", worker_name: "worker-b" },
    ],
    "/data/IN1K": [
      { worker_id: "aaaaaaaa-1111-0000-0000-000000000001", worker_name: "worker-a" },
    ],
  };

  const catalog = buildDataCatalog(data);
  expect(Object.keys(catalog.workers)).toEqual(["aaaaaaaa-1", "aaaaaaaa-2"]);
  expect(catalog.paths).toEqual({
    "/code/DRRT": ["aaaaaaaa-1", "aaaaaaaa-2"],
    "/data/IN1K": ["aaaaaaaa-1"],
  });
  expect(formatDataCatalog(catalog)).toContain(
    "@aaaaaaaa-1 = aaaaaaaa-1111-0000-0000-000000000001 | worker-a",
  );
  expect(formatDataCatalog(catalog)).toContain(
    "/code/DRRT | @aaaaaaaa-1 · @aaaaaaaa-2",
  );
  expect(buildDataCatalog(data, "in1k")).toEqual({
    workers: {
      "aaaaaaaa": {
        id: "aaaaaaaa-1111-0000-0000-000000000001",
        name: "worker-a",
      },
    },
    paths: { "/data/IN1K": ["aaaaaaaa"] },
  });
  expect(buildDataCatalog(data, "missing")).toEqual({ workers: {}, paths: {} });
  expect(formatDataCatalog({ workers: {}, paths: {} })).toBe("No data paths matched.");
});
