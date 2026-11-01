import type { PublicWorker } from "./types.ts";

export type GpuAvailability = "free" | "partial" | "full";

export interface GpuModelStatus {
  model: string;
  busy: number;
  total: number;
}

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


export function gpuIsBusy(worker: PublicWorker, index: number): boolean {
  if (worker.status !== "online") return true;

  // Missing or invalid telemetry is not evidence that a GPU can be allocated.
  return worker.gpu_available[index] !== true;
}

export function workerGpuStatus(worker: PublicWorker): GpuModelStatus[] {
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

export function aggregateGpuStatus(workers: PublicWorker[]): GpuModelStatus[] {
  const byModel = new Map<string, GpuModelStatus>();
  for (const worker of workers) {
    if (worker.status !== "online") continue;
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
