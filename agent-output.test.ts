import { expect, test } from "bun:test";
import {
  availableGpuWorkers,
  buildDataCatalog,
  formatDataCatalog,
  formatAvailableGpus,
  formatWorkerOverview,
} from "./agent-output.ts";
import type { DataPaths, Worker } from "./types.ts";

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
