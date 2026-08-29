import type { Worker } from "./types.ts";

export type GpuAvailability = "free" | "partial" | "full";

export interface GpuModelStatus {
  model: string;
  busy: number;
  total: number;
}

const FALLBACK_FREE_VRAM_FRACTION = 0.90;
const FALLBACK_IGNORED_VRAM_GB = 0.5;
export function normalizeGpuModel(name: string): string {
  return name
    .trim()
    .replace(/^(?:NVIDIA|Tesla)\s+/i, "")
    .replace(/\s+Max-Q Workstation Edition$/i, "")
    .replace(/\s+Workstation Edition$/i, "")
    .replace(/\s+Generation$/i, "")
    .replace(/-(?:SXM\d*|PCIe)-\d+GB$/i, "")
    .replace(/\s+\d+GB$/i, "");
}


export function gpuIsBusy(worker: Worker, index: number): boolean {
  if (worker.status !== "online") return true;

  const reported = worker.gpu_busy?.[index];
  if (typeof reported === "boolean") return reported;

  const total = worker.gpu_vram_gb[index] ?? 0;
  const used = worker.gpu_used_vram_gb[index] ?? 0;
  const mostlyFreeLimit = Math.max(
    FALLBACK_IGNORED_VRAM_GB,
    total * (1 - FALLBACK_FREE_VRAM_FRACTION),
  );
  return used > mostlyFreeLimit;
}

export function workerGpuStatus(worker: Worker): GpuModelStatus[] {
  const byModel = new Map<string, GpuModelStatus>();
  for (let index = 0; index < worker.gpu_count; index += 1) {
    const rawModel = worker.gpu_names[index]?.trim() || `GPU ${index}`;
    const model = normalizeGpuModel(rawModel);
    let status = byModel.get(model);
    if (!status) {
      status = { model, busy: 0, total: 0 };
      byModel.set(model, status);
    }
    status.total += 1;
    if (gpuIsBusy(worker, index)) status.busy += 1;
  }
  return [...byModel.values()].sort((left, right) => left.model.localeCompare(right.model));
}

export function aggregateGpuStatus(workers: Worker[]): GpuModelStatus[] {
  const byModel = new Map<string, GpuModelStatus>();
  for (const worker of workers) {
    for (const workerStatus of workerGpuStatus(worker)) {
      let status = byModel.get(workerStatus.model);
      if (!status) {
        status = { model: workerStatus.model, busy: 0, total: 0 };
        byModel.set(workerStatus.model, status);
      }
      status.busy += workerStatus.busy;
      status.total += workerStatus.total;
    }
  }
  return [...byModel.values()].sort((left, right) => left.model.localeCompare(right.model));
}

export function gpuAvailability(status: GpuModelStatus): GpuAvailability {
  if (status.busy === 0) return "free";
  if (status.busy < status.total) return "partial";
  return "full";
}
