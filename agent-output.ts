import { workerGpuStatus } from "./gpu-status.ts";
import type { DataPaths, DelayEntry, OwnedJob, PublicWorker, QueueBoard, QueueJob } from "./types.ts";

export interface DataCatalogWorker {
  id: string;
  name: string;
}

export interface DataCatalog {
  workers: Record<string, DataCatalogWorker>;
  paths: Record<string, string[]>;
}

export function formatWorkerOverview(workers: PublicWorker[]): string {
  if (workers.length === 0) return "No workers registered.";
  const rows = workers.map((worker) => {
    const gpuSummary = workerGpuStatus(worker)
      .map(({ model, busy, total }) => `${model} ${busy}/${total}`)
      .join(" · ") || "—";
    return `${worker.status} | ${worker.id} | ${worker.name} | GPUs ${gpuSummary}`;
  });
  return ["status | worker_id | worker | GPUs (busy/total)", ...rows].join("\n");
}

export function availableGpuWorkers(workers: PublicWorker[]): PublicWorker[] {
  return workers.filter((worker) =>
    worker.status === "online"
    && workerGpuStatus(worker).some(({ busy, total }) => busy < total)
  );
}

/** Empty means the event did not delay this job and must not be rendered as a delay. */
export function formatDelayEntry(entry: DelayEntry): string {
  if (entry.delay_seconds !== null && entry.delay_seconds <= 0) return "";
  const delay = entry.delay_seconds === null
    ? "Moved later; delay estimate unavailable"
    : `+${Math.ceil(entry.delay_seconds / 60)} minutes estimated delay because of ${entry.moved_job_name} by ${entry.moved_job_owner_display_name}`;
  return `${delay}. Moved by ${entry.actor_display_name}: ${entry.reason} (${new Date(entry.created_at * 1000).toISOString()})`;
}

function formatExpected(seconds: number): string {
  return seconds > 0 ? `${seconds}s` : "unknown";
}

/** Compact owner-only output retains execution supervision fields. */
export function formatOwnedJob(job: OwnedJob): string {
  return `${job.id} (${job.name}) on ${job.worker_name ?? job.worker_id ?? "unassigned"}`
    + ` | ${job.status} | execution_state=${job.execution_state}`
    + ` | termination_reason=${job.termination_reason ?? "none"}`
    + ` | owner ${job.owner_display_name} | priority ${job.priority}`
    + ` | expected ${formatExpected(job.expected_seconds)}`;
}

function formatQueueJob(job: QueueJob, nowSeconds: number): string[] {
  const state = job.status === "pending" ? `pending #${job.position}` : job.status;
  const elapsed = job.started_at === null ? "" : ` | elapsed ${Math.max(0, nowSeconds - job.started_at)}s`;
  const start = job.estimated_start_at === null ? "unavailable" : new Date(job.estimated_start_at * 1000).toISOString();
  const finish = job.estimated_finish_at === null ? "unavailable" : new Date(job.estimated_finish_at * 1000).toISOString();
  const lines = [
    `  ${state} | ${job.id} | ${job.name} | GPUs ${job.gpu_count}`
      + ` | owner ${job.owner_display_name} | priority ${job.priority}`
      + ` | execution_state=${job.execution_state} | termination_reason=${job.termination_reason ?? "none"}`
      + ` | expected ${formatExpected(job.expected_seconds)}${elapsed}`,
    `    ETA start ${start} | finish ${finish} | ${job.estimate_status}: ${job.estimate_explanation}`,
  ];
  if (job.priority_note) lines.push(`    Priority note: ${job.priority_note}`);
  if (job.deadline_at !== null) lines.push(`    Deadline: ${new Date(job.deadline_at * 1000).toISOString()}`);
  for (const entry of job.delay_history) {
    const text = formatDelayEntry(entry);
    if (text) lines.push(`    ${text}`);
  }
  return lines;
}

export function formatQueueOverview(board: QueueBoard): string {
  if (board.workers.length === 0) return "No workers or active queued jobs.";
  const lines: string[] = [];
  for (const worker of board.workers) {
    lines.push(`${worker.worker_id} | ${worker.worker_name} | revision ${worker.revision}`);
    if (worker.jobs.length === 0) lines.push("  (GPU queue empty)");
    for (const job of worker.jobs) lines.push(...formatQueueJob(job, board.as_of));
    if (worker.cpu_jobs.length) {
      lines.push("  CPU/setup jobs:");
      for (const job of worker.cpu_jobs) lines.push(...formatQueueJob(job, board.as_of));
    }
  }
  return lines.join("\n");
}

export function formatAvailableGpus(workers: PublicWorker[]): string {
  const available = availableGpuWorkers(workers);
  if (available.length === 0) return "No available GPUs.";
  const rows = available.map((worker) => {
    const gpuSummary = workerGpuStatus(worker)
      .filter(({ busy, total }) => busy < total)
      .map(({ model, busy, total }) => `${model} ${total - busy}/${total}`)
      .join(" · ");
    return `${worker.id} | ${worker.name} | ${gpuSummary}`;
  });
  return ["worker_id | worker | GPUs (free/total)", ...rows].join("\n");
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
