import { expect, test } from "bun:test";
import { aggregateGpuStatus, gpuAvailability, gpuIsBusy, normalizeGpuModel, workerGpuStatus } from "./gpu-status.ts";
import type { Worker } from "./types.ts";

function worker(patch: Partial<Worker>): Worker {
  return {
    id: "worker",
    name: "worker",
    worker_ip: "100.64.0.1",
    dns_name: "worker.hs.example",
    ssh_user: "root",
    harness_dir: "/harness",
    gpu_count: 0,
    gpu_names: [],
    gpu_vram_gb: [],
    gpu_used_vram_gb: [],
    gpu_busy: [],
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

test("groups reported busy GPUs by model", () => {
  const workers = [
    worker({
      gpu_count: 2,
      gpu_names: ["NVIDIA RTX 6000 Ada Generation", "NVIDIA RTX 6000 Ada Generation"],
      gpu_vram_gb: [48, 48],
      gpu_used_vram_gb: [1, 24],
      gpu_busy: [false, true],
    }),
    worker({
      id: "offline",
      status: "offline",
      gpu_count: 1,
      gpu_names: ["Tesla V100-SXM2-32GB"],
      gpu_vram_gb: [16],
      gpu_used_vram_gb: [0],
      gpu_busy: [false],
    }),
  ];

  expect(workerGpuStatus(workers[0])).toEqual([
    { model: "RTX 6000 Ada", busy: 1, total: 2 },
  ]);
  expect(aggregateGpuStatus(workers)).toEqual([
    { model: "RTX 6000 Ada", busy: 1, total: 2 },
    { model: "V100", busy: 1, total: 1 },
  ]);
  expect(gpuAvailability({ model: "RTX 6000 Ada", busy: 0, total: 2 })).toBe("free");
  expect(gpuAvailability({ model: "RTX 6000 Ada", busy: 1, total: 2 })).toBe("partial");
  expect(normalizeGpuModel("NVIDIA A100-SXM4-80GB")).toBe("A100");
  expect(gpuAvailability({ model: "RTX 6000 Ada", busy: 2, total: 2 })).toBe("full");
});

test("falls back to mostly-free VRAM for workers without busy telemetry", () => {
  const available = worker({
    gpu_count: 1,
    gpu_names: ["V100"],
    gpu_vram_gb: [16],
    gpu_used_vram_gb: [0.5],
    gpu_busy: [null],
  });
  const occupied = worker({
    gpu_count: 1,
    gpu_names: ["V100"],
    gpu_vram_gb: [16],
    gpu_used_vram_gb: [2],
    gpu_busy: [null],
  });

  expect(gpuIsBusy(available, 0)).toBe(false);
  expect(gpuIsBusy(occupied, 0)).toBe(true);
});
