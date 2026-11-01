import { expect, test } from "bun:test";
import { aggregateGpuStatus, gpuAvailability, gpuIsBusy, normalizeGpuModel, workerGpuStatus } from "./gpu-status.ts";
import type { PublicWorker } from "./types.ts";

function worker(patch: Partial<PublicWorker>): PublicWorker {
  return {
    id: "worker",
    name: "worker",
    gpu_count: 0,
    gpu_names: [],
    gpu_vram_gb: [],
    gpu_used_vram_gb: [],
    gpu_utilization_percent: [],
    gpu_available: [],
    cpu_cores: 8,
    data_paths: [],
    status: "online",
    ...patch,
  };
}

test("aggregates public availability by normalized model across online workers", () => {
  const workers = [
    worker({
      gpu_count: 2,
      gpu_names: ["NVIDIA RTX 6000 Ada Generation", "NVIDIA RTX 6000 Ada Generation"],
      gpu_vram_gb: [48, 48],
      gpu_used_vram_gb: [24, 0],
      gpu_available: [true, false],
    }),
    worker({
      id: "second",
      gpu_count: 1,
      gpu_names: ["RTX 6000 Ada"],
      gpu_available: [true],
    }),
    worker({
      id: "offline",
      status: "offline",
      gpu_count: 1,
      gpu_names: ["Tesla V100-SXM2-32GB"],
      gpu_available: [true],
    }),
  ];

  expect(workerGpuStatus(workers[0])).toEqual([
    { model: "RTX 6000 Ada", busy: 1, total: 2 },
  ]);
  expect(aggregateGpuStatus(workers)).toEqual([
    { model: "RTX 6000 Ada", busy: 1, total: 3 },
  ]);
});

test("unknown or missing availability never becomes free from low VRAM or utilization", () => {
  const unknown = worker({
    gpu_count: 2,
    gpu_names: ["V100", "V100"],
    gpu_vram_gb: [16, 16],
    gpu_used_vram_gb: [0, 0],
    gpu_utilization_percent: [0, null],
    gpu_available: [null],
  });
  expect(gpuIsBusy(unknown, 0)).toBe(true);
  expect(gpuIsBusy(unknown, 1)).toBe(true);
  expect(workerGpuStatus(unknown)).toEqual([{ model: "V100", busy: 2, total: 2 }]);
});

test("server availability is authoritative rather than inferred from aggregate memory", () => {
  const reported = worker({
    gpu_count: 2,
    gpu_vram_gb: [48, 48],
    gpu_used_vram_gb: [30, 0],
    gpu_utilization_percent: [50, 0],
    gpu_available: [true, false],
  });
  expect(gpuIsBusy(reported, 0)).toBe(false);
  expect(gpuIsBusy(reported, 1)).toBe(true);
});

test("offline and draining workers cannot advertise allocatable GPUs", () => {
  for (const status of ["offline", "draining"] as const) {
    const unavailable = worker({ status, gpu_count: 1, gpu_available: [true] });
    expect(gpuIsBusy(unavailable, 0)).toBe(true);
    expect(aggregateGpuStatus([unavailable])).toEqual([]);
  }
});

test("classifies model availability and normalizes vendor model suffixes", () => {
  expect(gpuAvailability({ model: "Ada", busy: 0, total: 2 })).toBe("free");
  expect(gpuAvailability({ model: "Ada", busy: 1, total: 2 })).toBe("partial");
  expect(gpuAvailability({ model: "Ada", busy: 2, total: 2 })).toBe("full");
  expect(normalizeGpuModel("NVIDIA A100-SXM4-80GB")).toBe("A100");
  expect(normalizeGpuModel("Tesla V100-SXM2-32GB")).toBe("V100");
  expect(normalizeGpuModel("NVIDIA RTX 5000 Max-Q Workstation Edition")).toBe("RTX 5000");
});
