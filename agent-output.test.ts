import { expect, test } from "bun:test";
import {
  availableGpuWorkers,
  buildDataCatalog,
  formatDataCatalog,
  formatAvailableGpus,
  formatDelayEntry,
  formatOwnedJob,
  formatQueueOverview,
  formatWorkerOverview,
} from "./agent-output.ts";
import type { DataPaths, DelayEntry, OwnedJob, PublicWorker, QueueBoard, QueueJob } from "./types.ts";

function worker(patch: Partial<PublicWorker> = {}): PublicWorker {
  return {
    id: "worker-1", name: "worker-a", status: "online",
    gpu_count: 1, gpu_names: ["NVIDIA A100-SXM4-80GB"],
    gpu_vram_gb: [80], gpu_used_vram_gb: [0],
    gpu_utilization_percent: [0], gpu_available: [true],
    cpu_cores: 8, data_paths: [], ...patch,
  };
}

function queueJob(patch: Partial<QueueJob> = {}): QueueJob {
  return {
    id: "job-1", worker_id: "worker-1", worker_name: "worker-a", name: "experiment",
    owner_user_id: "owner-1", owner_display_name: "Alice",
    status: "pending", execution_state: "active", position: 1,
    submitted_at: 1, submission_sequence: 1, priority: "normal", priority_note: "",
    deadline_at: null, expected_seconds: 60, gpu_count: 1, started_at: null,
    termination_reason: null, stop_requested_at: null,
    estimated_start_at: 120, estimated_finish_at: 180, estimate_status: "known",
    estimate_explanation: "FIFO resource estimate", delay_history: [], ...patch,
  };
}

function delay(patch: Partial<DelayEntry> = {}): DelayEntry {
  return {
    event_id: "event-1", created_at: 100, delay_seconds: 1,
    moved_job_id: "moved", moved_job_name: "urgent training",
    moved_job_owner_display_name: "Bob", actor_user_id: "actor-1",
    actor_display_name: "Carol", reason: "Agreed experiment window", ...patch,
  };
}

function board(jobs: QueueJob[], cpu_jobs: QueueJob[] = []): QueueBoard {
  return {
    as_of: 100,
    workers: [{ worker_id: "worker-1", worker_name: "worker-a", revision: 7, jobs, cpu_jobs }],
  };
}

test("renders only public fleet fields and excludes unavailable/unknown GPU workers", () => {
  const workers = [
    worker(),
    worker({ id: "busy", name: "busy", gpu_available: [false] }),
    worker({ id: "unknown", name: "unknown", gpu_available: [null] }),
    worker({ id: "offline", name: "offline", status: "offline" }),
  ];
  expect(formatWorkerOverview(workers)).toContain("online | worker-1 | worker-a");
  expect(formatWorkerOverview(workers)).not.toContain("tailnet_dns");
  expect(availableGpuWorkers(workers).map(({ id }) => id)).toEqual(["worker-1"]);
  expect(formatAvailableGpus(workers)).toContain("worker-1 | worker-a | A100 1/1");
  expect(formatAvailableGpus(workers)).not.toContain("unknown");
});

test("partially available GPU models remain schedulable", () => {
  const partial = worker({
    gpu_count: 2, gpu_names: ["NVIDIA A100", "NVIDIA A100"],
    gpu_available: [false, true], gpu_utilization_percent: [50, 0],
    gpu_vram_gb: [80, 80], gpu_used_vram_gb: [10, 0],
  });
  expect(availableGpuWorkers([partial])).toEqual([partial]);
  expect(formatAvailableGpus([partial])).toContain("A100 1/2");
});

test("renders QueueBoard revisions, CPU jobs, ownership, metadata and server ETA explanations", () => {
  const output = formatQueueOverview(board([
    queueJob({ id: "active", status: "running", position: 0, started_at: 90, execution_state: "stopping" }),
    queueJob({ id: "head", name: "two-gpu", gpu_count: 2, priority: "time_critical",
      priority_note: "Deadline tonight", deadline_at: 300, delay_history: [delay()] }),
    queueJob({ id: "later", position: 2, expected_seconds: 0, estimate_status: "unknown",
      estimated_start_at: null, estimated_finish_at: null, estimate_explanation: "Active duration unknown" }),
  ], [queueJob({ id: "cpu", status: "running", position: 0, gpu_count: 0 })]));
  expect(output).toContain("worker-1 | worker-a | revision 7");
  expect(output).toContain("execution_state=stopping");
  expect(output).toContain("owner Alice | priority time_critical");
  expect(output).toContain("Priority note: Deadline tonight");
  expect(output).toContain("Deadline: 1970-01-01T00:05:00.000Z");
  expect(output).toContain("ETA start 1970-01-01T00:02:00.000Z");
  expect(output).toContain("known: FIFO resource estimate");
  expect(output).toContain("ETA start unavailable | finish unavailable | unknown: Active duration unknown");
  expect(output).toContain("expected unknown");
  expect(output).not.toContain("expected 0s");
  expect(output).toContain("CPU/setup jobs:");
  expect(output).toContain("cpu | experiment | GPUs 0");
  expect(output.indexOf("pending #1 | head")).toBeLessThan(output.indexOf("pending #2 | later"));
  expect(output).not.toContain("command");
});

test("renders positive delay with ceil minutes, actor, owner, reason and time", () => {
  expect(formatDelayEntry(delay())).toBe(
    "+1 minutes estimated delay because of urgent training by Bob. Moved by Carol: Agreed experiment window (1970-01-01T00:01:40.000Z)",
  );
  expect(formatDelayEntry(delay({ delay_seconds: 61 }))).toContain("+2 minutes");
  expect(formatDelayEntry(delay({ delay_seconds: null }))).toContain("Moved later; delay estimate unavailable");
  expect(formatDelayEntry(delay({ delay_seconds: 0 }))).toBe("");
  expect(formatDelayEntry(delay({ delay_seconds: -60 }))).toBe("");
  expect(formatQueueOverview(board([queueJob({ delay_history: [delay({ delay_seconds: 0 }), delay({ delay_seconds: -60 })] })]))).not.toContain("estimated delay");
});

test("renders empty boards and CPU-only workers", () => {
  expect(formatQueueOverview({ workers: [], as_of: 100 })).toBe("No workers or active queued jobs.");
  expect(formatQueueOverview(board([]))).toContain("(GPU queue empty)");
  expect(formatQueueOverview(board([], [queueJob({ gpu_count: 0 })]))).toContain("CPU/setup jobs:");
});

test("owned job output retains execution state and termination reason", () => {
  const job: OwnedJob = {
    ...queueJob({ expected_seconds: 0, termination_reason: "gpu_idle_timeout", execution_state: "termination_unknown" }),
    command: "private training command", exit_code: null, kind: "ssh",
    submitted_by_actor_id: "actor", gpu_indices: [0], queue_managed: true,
    finished_at: null, origin_session_id: null,
  };
  const output = formatOwnedJob(job);
  expect(output).toContain("execution_state=termination_unknown");
  expect(output).toContain("termination_reason=gpu_idle_timeout");
  expect(output).toContain("expected unknown");
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
