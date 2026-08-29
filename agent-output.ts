import { workerGpuStatus } from "./gpu-status.ts";
import type { DataPaths, Worker } from "./types.ts";

export interface DataCatalogWorker {
  id: string;
  name: string;
}

export interface DataCatalog {
  workers: Record<string, DataCatalogWorker>;
  paths: Record<string, string[]>;
}

function workerDns(worker: Worker): string {
  return worker.dns_name || worker.worker_ip || "—";
}

function columnWidths(workers: Worker[]): { name: number; dns: number } {
  return {
    name: Math.max(0, ...workers.map((worker) => worker.name.length)),
    dns: Math.max(0, ...workers.map((worker) => workerDns(worker).length)),
  };
}

export function formatWorkerOverview(workers: Worker[]): string {
  if (workers.length === 0) return "No workers registered.";

  const widths = columnWidths(workers);
  const header = "status   | worker_id                            | worker".padEnd(55 + widths.name)
    + " | tailnet_dns".padEnd(15 + widths.dns)
    + " | GPUs (busy/total)";
  const rows = workers.map((worker) => {
    const gpuSummary = workerGpuStatus(worker)
      .map(({ model, busy, total }) => `${model} ${busy}/${total}`)
      .join(" · ") || "—";
    return `${worker.status.padEnd(8)} | ${worker.id} | ${worker.name.padEnd(widths.name)} | ${workerDns(worker).padEnd(widths.dns)} | ${gpuSummary}`;
  });
  return [header, ...rows].join("\n");
}

export function availableGpuWorkers(workers: Worker[]): Worker[] {
  return workers.filter((worker) =>
    worker.status === "online"
    && workerGpuStatus(worker).some(({ busy, total }) => busy < total)
  );
}

export function formatAvailableGpus(workers: Worker[]): string {
  const available = availableGpuWorkers(workers);
  if (available.length === 0) return "No available GPUs.";

  const widths = columnWidths(available);
  const header = "worker_id                            | worker".padEnd(45 + widths.name)
    + " | tailnet_dns".padEnd(15 + widths.dns)
    + " | GPUs (free/total)";
  const rows = available.map((worker) => {
    const gpuSummary = workerGpuStatus(worker)
      .filter(({ busy, total }) => busy < total)
      .map(({ model, busy, total }) => `${model} ${total - busy}/${total}`)
      .join(" · ");
    return `${worker.id} | ${worker.name.padEnd(widths.name)} | ${workerDns(worker).padEnd(widths.dns)} | ${gpuSummary}`;
  });
  return [header, ...rows].join("\n");
}

function uniqueWorkerRefs(ids: string[]): Map<string, string> {
  const uniqueIds = [...new Set(ids)];
  const refs = new Map<string, string>();
  for (const id of uniqueIds) {
    let length = Math.min(8, id.length);
    while (
      length < id.length
      && uniqueIds.some((other) => other !== id && other.startsWith(id.slice(0, length)))
    ) {
      length += 1;
    }
    refs.set(id, id.slice(0, length));
  }
  return refs;
}

export function buildDataCatalog(data: DataPaths, query?: string): DataCatalog {
  const needle = query?.trim().toLowerCase() ?? "";
  const entries = Object.entries(data).filter(([path]) =>
    !needle || path.toLowerCase().includes(needle)
  );
  const locations = entries.flatMap(([, workers]) => workers);
  const refs = uniqueWorkerRefs(locations.map(({ worker_id }) => worker_id));
  const workers = Object.fromEntries(
    [...new Map(locations.map((worker) => [worker.worker_id, worker])).values()]
      .map((worker) => [refs.get(worker.worker_id)!, {
        id: worker.worker_id,
        name: worker.worker_name,
      }]),
  );
  const paths = Object.fromEntries(entries.map(([path, pathWorkers]) => [
    path,
    pathWorkers.map(({ worker_id }) => refs.get(worker_id)!),
  ]));
  return { workers, paths };
}

export function formatDataCatalog(catalog: DataCatalog): string {
  const paths = Object.entries(catalog.paths);
  if (paths.length === 0) return "No data paths matched.";

  const workers = Object.entries(catalog.workers)
    .map(([ref, worker]) => `@${ref} = ${worker.id} | ${worker.name}`);
  const pathRows = paths.map(([path, refs]) =>
    `${path} | ${refs.map((ref) => `@${ref}`).join(" · ")}`
  );
  return ["workers:", ...workers, "", "paths:", ...pathRows].join("\n");
}
