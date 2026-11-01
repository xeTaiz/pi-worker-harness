import { spawn } from "node:child_process";
import { Key, matchesKey, truncateToWidth } from "@earendil-works/pi-tui";
import { events } from "../events.ts";
import {
  addTunnel,
  createMarimo,
  getCredentialIdentity,
  getMarimo,
  enqueueJob,
  getJobLogs,
  listJobs,
  listQueue,
  listMarimo,
  listTunnels,
  listWorkers,
  removeMarimo,
  removeTunnel,
  startJob,
  stopJob,
} from "../api.ts";
import { subscribeJobLogs, type JobLogSubscription } from "../sse.ts";
import type { OwnedJob, Tunnel, PublicWorker, QueueBoard, QueueJob, MarimoSession } from "../types.ts";
import { formatDelayEntry } from "../agent-output.ts";

type Tab = "workers" | "queue" | "jobs" | "tunnels" | "marimo";

const TAB_LABELS: Record<Tab, string> = {
  workers: "Workers",
  queue: "Shared queue",
  jobs: "My jobs",
  tunnels: "Tunnels",
  marimo: "Marimo",
};

interface PanelState {
  workers: PublicWorker[];
  jobs: OwnedJob[];
  queue: QueueBoard;
  tunnels: Tunnel[];
  marimo: MarimoSession[];
  tab: Tab;
  selectedIndex: number;
  logJob: OwnedJob | null;
  logLines: string[];
  following: boolean;
  logFocusMode: boolean;
  logScrollOffset: number;
  loading: boolean;
  lastError: string | null;
  newJobCommandMode: boolean;
  newJobCommandBuffer: string;
  newJobNameMode: boolean;
  newJobNameBuffer: string;
  newJobPendingCommand: string;
  newJobDetailsMode: "duration" | "mode" | "gpu_count" | null;
  newJobDetailsBuffer: string;
  newJobExpectedSeconds: number;
  newTunnelPortMode: boolean;
  newTunnelPortBuffer: string;
  newTunnelLocalPortMode: boolean;
  newTunnelLocalPortBuffer: string;
  newTunnelNameMode: boolean;
  newTunnelNameBuffer: string;
  newTunnelPendingRemotePort: number | null;
  newTunnelPendingLocalPort: number | null;
}

function stripAnsi(input: string): string {
  return input.replace(/\u001B\[[0-9;]*m/g, "");
}

function statusIcon(status: string): string {
  return status === "online" ? "●" : "○";
}

function statusColor(status: string): string {
  return status === "online" ? "green" : "red";
}

function jobStatusIcon(status: string): string {
  switch (status) {
    case "starting":
    case "running":
      return "●";
    case "done":
      return "✓";
    case "failed":
      return "✗";
    default:
      return "○";
  }
}

function jobStatusColor(status: string): string {
  switch (status) {
    case "starting":
    case "running":
      return "yellow";
    case "done":
      return "green";
    case "failed":
      return "red";
    default:
      return "dim";
  }
}

function finiteNumber(value: unknown, fallback = 0): number {
  return typeof value === "number" && Number.isFinite(value) ? value : fallback;
}

function usageBar(used: unknown, total: unknown, width = 20): string {
  const safeWidth = Math.max(1, Math.floor(finiteNumber(width, 20)));
  const safeTotal = finiteNumber(total, 0);
  const safeUsed = finiteNumber(used, 0);

  if (safeTotal <= 0) return "░".repeat(safeWidth);

  const rawFilled = Math.round((safeWidth * Math.max(0, safeUsed)) / safeTotal);
  const filled = Math.max(0, Math.min(safeWidth, rawFilled));
  return "█".repeat(filled) + "░".repeat(safeWidth - filled);
}

function gpuBar(used: number, total: number, width = 20): string {
  return usageBar(used, total, width);
}


function formatTimestamp(ts: number | null): string {
  if (!ts) return "-";
  return new Date(ts * 1000).toLocaleTimeString(undefined, {
    hour: "2-digit",
    minute: "2-digit",
  });
}

function sortJobs(jobs: OwnedJob[]): OwnedJob[] {
  return [...jobs].sort((a, b) => {
    const order: Record<string, number> = {
      starting: 0,
      running: 1,
      pending: 2,
      done: 3,
      failed: 4,
    };
    return (order[a.status] ?? 4) - (order[b.status] ?? 4);
  });
}

function queueJobs(board: QueueBoard): QueueJob[] {
  return board.workers.flatMap((worker) => [...worker.jobs, ...worker.cpu_jobs]);
}

function queueMetadata(job: QueueJob): string[] {
  return [
    `Owner: ${job.owner_display_name} · Priority: ${job.priority} · ${job.gpu_count > 0 ? `${job.gpu_count} GPU` : "CPU/setup"}`,
    `Runtime: ${job.expected_seconds > 0 ? `${job.expected_seconds}s` : "unknown"}`,
    job.estimate_status === "known"
      ? `Estimated start: ${formatTimestamp(job.estimated_start_at)} · finish: ${formatTimestamp(job.estimated_finish_at)}`
      : `ETA unknown: ${job.estimate_explanation}`,
    ...(job.priority_note ? [`Priority note: ${job.priority_note}`] : []),
    ...(job.deadline_at ? [`Deadline: ${new Date(job.deadline_at * 1000).toLocaleString()}`] : []),
    ...job.delay_history.map(formatDelayEntry).filter(Boolean),
  ];
}

function lastNonEmptyLine(lines: string[]): string | null {
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i];
    if (line?.trim()) return line;
  }
  return null;
}

function shortcutsHint(state: PanelState): string {
  if (state.newJobCommandMode) {
    return "[type command] [Enter]next [Esc]cancel [Backspace]edit";
  }
  if (state.newJobNameMode) {
    return "[required public job name] [Enter]next [Esc]cancel [Backspace]edit";
  }
  if (state.newJobDetailsMode) {
    return "[Enter]continue [Esc]cancel [Backspace]edit";
  }
  if (state.newTunnelPortMode) {
    return "[type remote port] [Enter]next [Esc]cancel [Backspace]edit";
  }
  if (state.newTunnelLocalPortMode) {
    return "[type local port or blank for random] [Enter]next [Esc]cancel [Backspace]edit";
  }
  if (state.newTunnelNameMode) {
    return "[optional tunnel name] [Enter]create tunnel [Esc]cancel [Backspace]edit";
  }
  if (state.logFocusMode) {
    return "[j/k]scroll [f/q/Esc]close logs";
  }

  const base = "[j/k]nav [tab/S-tab h/l]tab [r]refresh [q]close";
  if (state.tab === "workers") return `${base} [n]new job [t]tunnel [m]new Marimo`;
  if (state.tab === "jobs") return `${base} [f]open logs [s]stop [n]new job [t]tunnel`;
  if (state.tab === "marimo") return `${base} [m]create [o]open [s]stop`;
  if (state.tab === "tunnels") return `${base} [d]remove`;
  return base;
}

function buildPanel(
  state: PanelState,
  themeFg: (color: string, text: string) => string,
  themeBold: (text: string) => string,
  width: number,
): string[] {
  const colorFallbacks: Record<string, string> = {
    yellow: "accent",
    red: "accent",
    green: "accent",
    dim: "muted",
  };

  const fg = (color: string, text: string) => {
    const attempts = [color, colorFallbacks[color], "accent", "muted", "dim"];
    for (const candidate of attempts) {
      if (!candidate) continue;
      try {
        return themeFg(candidate, text);
      } catch {
        // try next fallback color
      }
    }
    return text;
  };

  const bold = (text: string) => {
    try {
      return themeBold(text);
    } catch {
      return text;
    }
  };
  const lines: string[] = [];

  const border = fg("border", "─".repeat(Math.max(1, width)));
  lines.push(border);
  const online = state.workers.filter((w) => w.status === "online").length;
  const running = state.jobs.filter((j) => j.status === "starting" || j.status === "running").length;
  lines.push(
    fg("accent", bold(" Worker Harness ")) +
      " " +
      fg("dim", `Workers:${online} Jobs:${running} Tunnels:${state.tunnels.length}`),
  );

  const tabs: Tab[] = ["workers", "queue", "jobs", "tunnels", "marimo"];
  const tabLine = tabs
    .map((t) =>
      t === state.tab
        ? fg("accent", "▸ " + TAB_LABELS[t])
        : fg("muted", "  " + TAB_LABELS[t]),
    )
    .join(fg("border", " │ "));
  lines.push(
    "  " +
      tabLine +
      "    " +
      fg("dim", shortcutsHint(state)),
  );
  if (state.loading) {
    lines.push(fg("yellow", "  loading worker-harness data..."));
  }
  if (state.lastError) {
    lines.push(fg("red", "  error: " + truncateToWidth(state.lastError, Math.max(20, width - 10))));
  }
  if (state.newJobCommandMode) {
    lines.push(fg("accent", "  new job command> ") + truncateToWidth(state.newJobCommandBuffer, Math.max(10, width - 20)));
  }
  if (state.newJobNameMode) {
    lines.push(fg("accent", "  public job name (required)> ") + truncateToWidth(state.newJobNameBuffer, Math.max(10, width - 30)));
  }
  if (state.newJobDetailsMode) {
    const prompts = {
      duration: "expected duration in seconds (positive integer)",
      mode: "mode: cpu (default) or gpu",
      gpu_count: "GPU count (positive integer)",
    };
    lines.push(fg("accent", `  ${prompts[state.newJobDetailsMode]}> `) + state.newJobDetailsBuffer);
  }
  if (state.newTunnelPortMode) {
    lines.push(fg("accent", "  tunnel remote port> ") + truncateToWidth(state.newTunnelPortBuffer, Math.max(5, width - 24)));
  }
  if (state.newTunnelLocalPortMode) {
    lines.push(fg("accent", "  tunnel local port (optional)> ") + truncateToWidth(state.newTunnelLocalPortBuffer, Math.max(5, width - 33)));
  }
  if (state.newTunnelNameMode) {
    lines.push(fg("accent", "  tunnel name (optional)> ") + truncateToWidth(state.newTunnelNameBuffer, Math.max(10, width - 28)));
  }
  lines.push(border);

  lines.push(fg("accent", bold(" " + TAB_LABELS[state.tab].toUpperCase())));

  if (state.tab === "workers") {
    if (state.workers.length === 0) {
      lines.push(fg("muted", state.loading ? "  (loading workers...)" : "  (no workers)"));
    } else {
      for (let i = 0; i < state.workers.length; i++) {
        const w = state.workers[i];
        const prefix = i === state.selectedIndex ? "▸ " : "  ";
        const name = i === state.selectedIndex ? bold(w.name) : w.name;
        const gpu = w.gpu_count > 0 ? ` ${w.gpu_count}GPU` : "";
        lines.push(prefix + fg(statusColor(w.status), statusIcon(w.status)) + " " + name + gpu);
      }
    }
  } else if (state.tab === "queue") {
    const jobs = queueJobs(state.queue);
    if (jobs.length === 0) lines.push(fg("muted", state.loading ? "  (loading shared queue...)" : "  (no shared jobs)"));
    let index = 0;
    for (const worker of state.queue.workers) {
      if (worker.jobs.length + worker.cpu_jobs.length === 0) continue;
      lines.push(fg("accent", `  ${worker.worker_name} · revision ${worker.revision}`));
      for (const [label, jobs] of [["GPU queue (FIFO)", worker.jobs], ["CPU/setup (active)", worker.cpu_jobs]] as const) {
        lines.push(fg("muted", `    ${label}`));
        for (const job of jobs) {
          const prefix = index++ === state.selectedIndex ? "▸ " : "  ";
          lines.push(prefix + `${job.position > 0 ? `#${job.position}` : job.status} ${job.name} · ${job.owner_display_name} · ${job.priority} · ${job.execution_state}`);
        }
      }
    }
  } else if (state.tab === "jobs") {
    const sorted = sortJobs(state.jobs);
    if (sorted.length === 0) {
      lines.push(fg("muted", state.loading ? "  (loading jobs...)" : "  (no jobs)"));
    } else {
      for (let i = 0; i < sorted.length; i++) {
        const j = sorted[i];
        const prefix = i === state.selectedIndex ? "▸ " : "  ";
        const wname = String(j.worker_name || "-");
        const cmd = truncateToWidth(stripAnsi(j.command), 40);
        lines.push(
          prefix +
            fg(jobStatusColor(j.status), jobStatusIcon(j.status)) +
            " " +
            wname.padEnd(15) +
            " " +
            cmd,
        );
      }
    }
  } else if (state.tab === "marimo") {
    if (!state.marimo.length) lines.push(fg("muted", state.loading ? "  (loading notebooks...)" : "  (no owned notebooks; press m to create)"));
    for (let i = 0; i < state.marimo.length; i++) {
      const session = state.marimo[i];
      lines.push(`${i === state.selectedIndex ? "▸ " : "  "}${session.worker_name} · ${session.notebook_path} · ${session.status}`);
      if (i === state.selectedIndex) {
        lines.push(`    Environment: ${session.environment}`);
        lines.push(`    Browser: ${session.url}`);
      }
    }
  } else {
    lines.push(fg("yellow", "  Raw TCP is reachable on the trusted Tailnet; the target service must authenticate."));
    if (state.tunnels.length === 0) {
      lines.push(fg("muted", state.loading ? "  (loading tunnels...)" : "  (no tunnels)"));
    } else {
      for (let i = 0; i < state.tunnels.length; i++) {
        const t = state.tunnels[i];
        const wname = t.worker_name || t.worker_id.slice(0, 8);
        lines.push(
          (i === state.selectedIndex ? "▸ " : "  ") + "→ " + t.bind_host + ":" +
            t.local_port +
            " → " +
            wname +
            ":" +
            t.remote_port +
            " (" +
            (t.service_name || "unnamed") +
            ")",
        );
      }
    }
  }

  lines.push("");
  lines.push(fg("accent", bold(" DETAIL")));

  if (state.tab === "workers") {
    const w = state.workers[state.selectedIndex];
    if (!w) {
      lines.push(fg("muted", "  (no worker selected)"));
    } else {
      lines.push(
        fg("accent", bold("Worker: " + w.name)) +
          " " +
          fg(statusColor(w.status), statusIcon(w.status)),
      );
      lines.push(fg("muted", `  CPU cores: ${w.cpu_cores}`));
      if (w.data_paths.length) lines.push(fg("muted", "  Shared data: " + w.data_paths.join(", ")));

      for (let i = 0; i < w.gpu_count; i++) {
        const name = w.gpu_names[i] || `GPU ${i}`;
        const used = w.gpu_used_vram_gb[i] ?? 0;
        const total = w.gpu_vram_gb[i] ?? 0;
        const bar = gpuBar(used, total, 20);
        lines.push(
          "  " +
            fg("accent", bar) +
            " " +
            used.toFixed(0) +
            "/" +
            total.toFixed(0) +
            " GB  " +
            name,
        );
        const available = w.gpu_available[i];
        const utilization = w.gpu_utilization_percent[i];
        lines.push(fg("muted", `    ${available === true ? "available" : available === false ? "unavailable" : "availability unknown"} · utilization ${utilization === null || utilization === undefined ? "unknown" : `${utilization}%`}`));
      }
    }
  } else if (state.tab === "queue") {
    const job = queueJobs(state.queue)[state.selectedIndex];
    if (job) {
      lines.push(bold(`  ${job.name} · ${job.status} · ${job.execution_state}`));
      for (const line of queueMetadata(job)) lines.push("  " + line);
      if (job.termination_reason) lines.push(`  Stop reason: ${job.termination_reason}`);
      lines.push(fg("muted", "  Private command/log/stop controls are available only in My jobs."));
    } else {
      lines.push(fg("muted", "  (no shared job selected)"));
    }
  } else if (state.tab === "jobs") {
    const sorted = sortJobs(state.jobs);
    const j = sorted[state.selectedIndex];
    if (!j) {
      lines.push(fg("muted", "  (no job selected)"));
    } else {
      lines.push(
        fg(jobStatusColor(j.status), jobStatusIcon(j.status)) +
          " " +
          bold(j.id.slice(0, 8)) +
          " " +
          fg("muted", j.status),
      );
      lines.push("  " + truncateToWidth(stripAnsi(j.command), 80));
      lines.push(`  Public name: ${j.name} · Owner: ${j.owner_display_name} · Priority: ${j.priority}`);
      lines.push(`  Runtime: ${j.expected_seconds > 0 ? `${j.expected_seconds}s` : "unknown"} · State: ${j.execution_state}`);
      if (j.termination_reason) lines.push(`  Stop reason: ${j.termination_reason}`);
      const shared = queueJobs(state.queue).find((job) => job.id === j.id);
      if (shared) for (const line of queueMetadata(shared).slice(2)) lines.push("  " + line);
      if (j.exit_code !== null) {
        lines.push(fg(j.exit_code === 0 ? "green" : "red", "  exit: " + j.exit_code));
      }
      lines.push(fg("muted", "  started: " + formatTimestamp(j.started_at)));

      if (state.logFocusMode) {
        lines.push("");
        lines.push(fg("accent", bold("  LOG VIEW")) + " " + fg("muted", state.following ? "(live)" : "(snapshot)"));

        const maxWindowLines = 200;
        const allLines = state.logLines.filter((line) => line !== undefined);
        const endExclusive = Math.max(0, allLines.length - Math.max(0, state.logScrollOffset));
        const start = Math.max(0, endExclusive - maxWindowLines);
        const windowLines = allLines.slice(start, endExclusive);

        if (windowLines.length === 0) {
          lines.push(fg("muted", "  (no logs yet)"));
        } else {
          for (const line of windowLines) {
            lines.push("  " + truncateToWidth(stripAnsi(line), Math.max(20, width - 4)));
          }
        }

        const shownStart = windowLines.length > 0 ? start + 1 : 0;
        const shownEnd = endExclusive;
        lines.push(
          fg(
            "muted",
            `  showing ${shownStart}-${shownEnd} of ${allLines.length} lines${state.logScrollOffset > 0 ? ` (scroll +${state.logScrollOffset})` : ""}`,
          ),
        );
      } else if (state.logJob?.id === j.id && state.logLines.length > 0) {
        const last = lastNonEmptyLine(state.logLines) ?? state.logLines[state.logLines.length - 1] ?? "";
        lines.push("");
        lines.push(fg("muted", state.following ? "  live last line:" : "  last log line:"));
        if (last.trim()) {
          lines.push("  " + truncateToWidth(stripAnsi(last), Math.max(20, width - 4)));
        } else {
          lines.push(fg("muted", "  (last line is empty)"));
        }
      }
    }
  }

  lines.push(border);
  return lines;
}

export function createSimplePanel() {
  return new SimplePanel();
}

class SimplePanel {
  private tui!: { requestRender: () => void };
  private theme!: { fg: (color: string, text: string) => string; bold: (text: string) => string };
  private done!: (value: void) => void;
  private uiCtx: any;
  private onTrackJob!: (job: OwnedJob) => void;
  private onSetWidget!: (worker: PublicWorker | null, job: OwnedJob | null, jobName: string | null) => void;
  private refreshUnsubscribe: (() => void) | null = null;
  private jobStartedUnsubscribe: (() => void) | null = null;
  private logSub: JobLogSubscription | null = null;
  private startedJobs = new Map<string, { job: OwnedJob; expiresAt: number }>();
  private inFlightFetch: Promise<void> | null = null;
  private previewRequestSeq = 0;
  private credentialIdentity: string | null = null;

  private state: PanelState = {
    workers: [],
    jobs: [],
    queue: { workers: [], as_of: 0 },
    tunnels: [],
    marimo: [],
    tab: "workers",
    selectedIndex: 0,
    logJob: null,
    logLines: [],
    following: false,
    logFocusMode: false,
    logScrollOffset: 0,
    loading: true,
    lastError: null,
    newJobCommandMode: false,
    newJobCommandBuffer: "",
    newJobNameMode: false,
    newJobNameBuffer: "",
    newJobPendingCommand: "",
    newJobDetailsMode: null,
    newJobDetailsBuffer: "",
    newJobExpectedSeconds: 0,
    newTunnelPortMode: false,
    newTunnelPortBuffer: "",
    newTunnelLocalPortMode: false,
    newTunnelLocalPortBuffer: "",
    newTunnelNameMode: false,
    newTunnelNameBuffer: "",
    newTunnelPendingRemotePort: null,
    newTunnelPendingLocalPort: null,
  };

  private syncCredentialIdentity(): string | null {
    let identity: string | null;
    try {
      identity = getCredentialIdentity();
    } catch {
      identity = null;
    }
    if (identity !== null && identity === this.credentialIdentity) return identity;
    this.credentialIdentity = identity;
    this.stopFollow();
    this.previewRequestSeq++;
    this.startedJobs.clear();
    this.state = {
      ...this.state,
      jobs: [], tunnels: [], marimo: [], selectedIndex: 0,
      logJob: null, logLines: [], following: false, logFocusMode: false, logScrollOffset: 0,
      newJobCommandMode: false, newJobCommandBuffer: "", newJobNameMode: false,
      newJobNameBuffer: "", newJobPendingCommand: "", newJobDetailsMode: null,
      newJobDetailsBuffer: "", newJobExpectedSeconds: 0, newTunnelPortMode: false,
      newTunnelPortBuffer: "", newTunnelLocalPortMode: false, newTunnelLocalPortBuffer: "",
      newTunnelNameMode: false, newTunnelNameBuffer: "", newTunnelPendingRemotePort: null,
      newTunnelPendingLocalPort: null, loading: false,
      lastError: identity === null ? "Worker Harness credentials unavailable; configure the current session's credentials." : "Credentials changed; press r to reload your resources.",
    };
    this.onSetWidget?.(null, null, null);
    return identity;
  }

  async show(
    uiCtx: any,
    onTrackJob: (job: OwnedJob) => void,
    onSetWidget: (worker: PublicWorker | null, job: OwnedJob | null, jobName: string | null) => void,
  ): Promise<void> {
    this.uiCtx = uiCtx;
    this.onTrackJob = onTrackJob;
    this.onSetWidget = onSetWidget;

    this.refreshUnsubscribe = events.on<void>("worker-harness:refresh", async () => {
      await this.refreshData({ notifyOnError: true });
    });

    this.jobStartedUnsubscribe = events.on<void>("worker-harness:job-started", () => {
      void this.refreshData();
    });

    const panelPromise = new Promise<void>((resolve, reject) => {
      this.done = resolve;
      try {
        const promise = uiCtx.custom(
          (tui: any, theme: any, _keybindings: any, done: (v: void) => void) => {
            this.tui = tui;
            this.theme = theme;
            this.done = done;
            return this;
          },
          {
            overlay: true,
            overlayOptions: {
              anchor: "bottom-left",
              width: "100%",
              maxHeight: "85%",
              visible: () => true,
              nonCapturing: false,
            },
          },
        );
        if (promise && typeof promise.then === "function") {
          promise.then(resolve).catch(reject);
        }
      } catch (err) {
        reject(err);
      }
    });

    // Open panel immediately, then load data in the background so API issues remain visible.
    void this.refreshData({ notifyOnError: true });

    return panelPromise.finally(() => {
      this.refreshUnsubscribe?.();
      this.refreshUnsubscribe = null;
      this.jobStartedUnsubscribe?.();
      this.jobStartedUnsubscribe = null;
      this.stopFollow();
    });
  }

  private errorMessage(err: unknown): string {
    return err instanceof Error ? err.message : String(err);
  }

  private updateState(fn: (s: PanelState) => PanelState): void {
    this.state = fn(this.state);
  }

  private clampSelection() {
    const count =
      this.state.tab === "workers"
        ? this.state.workers.length
        : this.state.tab === "jobs"
          ? this.state.jobs.length
          : this.state.tab === "queue" ? queueJobs(this.state.queue).length : this.state.tab === "marimo" ? this.state.marimo.length : this.state.tunnels.length;

    if (count === 0) {
      this.state.selectedIndex = 0;
      return;
    }
    this.state.selectedIndex = Math.max(0, Math.min(count - 1, this.state.selectedIndex));
  }

  private insertOptimisticJob(job: OwnedJob, options: { focus?: boolean } = {}) {
    this.startedJobs.set(job.id, { job, expiresAt: Date.now() + 2 * 60 * 1000 });

    this.updateState((s) => {
      const jobs = s.jobs.some((j) => j.id === job.id) ? s.jobs : [job, ...s.jobs];
      let selectedIndex = s.selectedIndex;
      let tab = s.tab;

      if (options.focus) {
        const sorted = sortJobs(jobs);
        const idx = sorted.findIndex((j) => j.id === job.id);
        selectedIndex = idx >= 0 ? idx : 0;
        tab = "jobs";
      }

      return {
        ...s,
        jobs,
        selectedIndex,
        tab,
      };
    });
  }

  private async fetchAll() {
    if (this.inFlightFetch) {
      await this.inFlightFetch;
      return;
    }

    this.inFlightFetch = (async () => {
      const identity = this.syncCredentialIdentity();
      const [workers, jobs, tunnels, queue, marimo] = await Promise.all([
        listWorkers(),
        listJobs(),
        listTunnels(),
        listQueue(),
        listMarimo(),
      ]);
      if (identity === null || this.syncCredentialIdentity() !== identity) return;

      const now = Date.now();
      for (const [id, pending] of this.startedJobs.entries()) {
        if (pending.expiresAt <= now) this.startedJobs.delete(id);
      }

      const mergedJobs = [...jobs];
      const seen = new Set(jobs.map((j) => j.id));

      for (const [id, pending] of this.startedJobs.entries()) {
        if (seen.has(id)) {
          this.startedJobs.delete(id);
          continue;
        }
        mergedJobs.unshift(pending.job);
      }

      this.updateState((s) => ({
        ...s,
        workers,
        jobs: mergedJobs,
        tunnels,
        queue,
        marimo,
      }));
      this.clampSelection();
    })();

    try {
      await this.inFlightFetch;
    } finally {
      this.inFlightFetch = null;
    }
  }

  private async refreshData(options: { notifyOnError?: boolean } = {}) {
    const identity = this.syncCredentialIdentity();
    if (identity === null) {
      this.tui?.requestRender();
      return;
    }
    this.updateState((s) => ({ ...s, loading: true }));
    this.tui?.requestRender();

    try {
      await this.fetchAll();
      if (identity === null || this.syncCredentialIdentity() !== identity) return;
      this.updateState((s) => ({ ...s, loading: false, lastError: null }));
      if (this.state.tab === "jobs" && !this.state.logFocusMode) {
        void this.loadSelectedJobPreview();
      }
    } catch (err) {
      if (identity === null || this.syncCredentialIdentity() !== identity) return;
      const message = this.errorMessage(err);
      this.updateState((s) => ({ ...s, loading: false, lastError: message }));
      if (options.notifyOnError) {
        this.uiCtx.notify(`Worker harness refresh failed: ${message}`, "error");
      }
    }

    this.tui?.requestRender();
  }

  private stopFollow() {
    if (this.logSub) {
      this.logSub.stop();
      this.logSub = null;
    }
  }

  private isAbortError(err: unknown): boolean {
    return (
      (err instanceof Error && err.name === "AbortError") ||
      (!!err && typeof err === "object" && "name" in err && (err as { name?: string }).name === "AbortError")
    );
  }

  private async sleep(ms: number): Promise<void> {
    await new Promise<void>((resolve) => setTimeout(resolve, ms));
  }

  render(width: number): string[] {
    try {
      this.syncCredentialIdentity();
      return buildPanel(
        this.state,
        (c, t) => this.theme.fg(c, t),
        (t) => this.theme.bold(t),
        width,
      );
    } catch (err) {
      const message = this.errorMessage(err);
      this.state = { ...this.state, lastError: `render failed: ${message}`, loading: false };
      return [
        "Worker Harness panel failed to render",
        `error: ${message}`,
        "Press q to close, r to retry refresh.",
      ];
    }
  }

  handleInput(data: string): void {
    try {
      this.syncCredentialIdentity();
      if (this.state.newJobCommandMode) {
        this.handleNewJobCommandInput(data);
        return;
      }
      if (this.state.newJobNameMode) {
        this.handleNewJobNameInput(data);
        return;
      }
      if (this.state.newJobDetailsMode) {
        this.handleNewJobDetailsInput(data);
        return;
      }
      if (this.state.newTunnelPortMode) {
        this.handleNewTunnelPortInput(data);
        return;
      }
      if (this.state.newTunnelLocalPortMode) {
        this.handleNewTunnelLocalPortInput(data);
        return;
      }
      if (this.state.newTunnelNameMode) {
        this.handleNewTunnelNameInput(data);
        return;
      }

      if (this.state.logFocusMode) {
        if (data === "f" || data === "q" || matchesKey(data, Key.escape)) {
          this.closeLogFocus();
          return;
        }
        if (matchesKey(data, Key.down) || data === "j") {
          this.scrollLogWindow(1);
          return;
        }
        if (matchesKey(data, Key.up) || data === "k") {
          this.scrollLogWindow(-1);
          return;
        }
      }

      if (data === "q" || matchesKey(data, Key.escape)) {
        this.done();
        return;
      }

      if (data === "r") {
        void this.refreshData({ notifyOnError: true });
        return;
      }

      if (matchesKey(data, Key.down) || data === "j") {
        this.navigate(1);
      } else if (matchesKey(data, Key.up) || data === "k") {
        this.navigate(-1);
      } else if (
        matchesKey(data, Key.right) ||
        data === "l" ||
        data === "\t" ||
        data === "w"
      ) {
        this.navigateTab(1);
      } else if (
        matchesKey(data, Key.left) ||
        data === "h" ||
        data === "\u001b[Z" ||
        data === "b"
      ) {
        this.navigateTab(-1);
      } else if (data === "f") {
        if (this.state.tab === "jobs") {
          this.openLogFocusForSelectedJob().catch((err) => this.uiCtx.notify(this.errorMessage(err), "error"));
        }
      } else if (data === "n") {
        if (this.state.tab === "workers" || this.state.tab === "jobs") {
          this.beginNewJobCommandMode();
        }
      } else if (data === "t") {
        if (this.state.tab === "workers" || this.state.tab === "jobs") {
          this.beginNewTunnelPortMode();
        }
      } else if (data === "s") {
        if (this.state.tab === "jobs") {
          this.stopSelectedJob().catch((err) => this.uiCtx.notify(this.errorMessage(err), "error"));
        }
        if (this.state.tab === "marimo") {
          this.stopSelectedMarimo().catch((err) => this.uiCtx.notify(this.errorMessage(err), "error"));
        }
      } else if (data === "m" && (this.state.tab === "marimo" || this.state.tab === "workers")) {
        this.createOwnedMarimo().catch((err) => this.uiCtx.notify(this.errorMessage(err), "error"));
      } else if (data === "o" && this.state.tab === "marimo") {
        this.openSelectedMarimo().catch((err) => this.uiCtx.notify(this.errorMessage(err), "error"));
      } else if (data === "d" && this.state.tab === "tunnels") {
        this.removeSelectedTunnel().catch((err) => this.uiCtx.notify(this.errorMessage(err), "error"));
      }
    } catch (err) {
      const message = this.errorMessage(err);
      this.updateState((s) => ({ ...s, lastError: `input failed: ${message}` }));
      this.uiCtx?.notify?.(`Worker harness panel input error: ${message}`, "error");
      this.tui?.requestRender();
    }
  }

  private navigateTab(delta: number) {
    const tabs: Tab[] = ["workers", "queue", "jobs", "tunnels", "marimo"];
    const idx = tabs.indexOf(this.state.tab);
    const next = (idx + delta + tabs.length) % tabs.length;
    this.updateState((s) => ({
      ...s,
      tab: tabs[next],
      selectedIndex: 0,
      logJob: null,
      logLines: [],
      following: false,
      logFocusMode: false,
      logScrollOffset: 0,
      newJobCommandMode: false,
      newJobCommandBuffer: "",
      newJobNameMode: false,
      newJobNameBuffer: "",
      newJobPendingCommand: "",
      newJobDetailsMode: null,
      newJobDetailsBuffer: "",
      newJobExpectedSeconds: 0,
      newTunnelPortMode: false,
      newTunnelPortBuffer: "",
      newTunnelLocalPortMode: false,
      newTunnelLocalPortBuffer: "",
      newTunnelNameMode: false,
      newTunnelNameBuffer: "",
      newTunnelPendingRemotePort: null,
      newTunnelPendingLocalPort: null,
    }));
    this.stopFollow();
    if (tabs[next] === "jobs") {
      void this.loadSelectedJobPreview();
    }
    this.tui?.requestRender();
  }

  private navigate(delta: number) {
    const { tab, workers, jobs, tunnels } = this.state;
    const count =
      tab === "workers" ? workers.length : tab === "jobs" ? jobs.length : tab === "queue" ? queueJobs(this.state.queue).length : tab === "marimo" ? this.state.marimo.length : tunnels.length;
    if (count === 0) return;
    this.updateState((s) => ({
      ...s,
      selectedIndex: Math.max(0, Math.min(count - 1, s.selectedIndex + delta)),
    }));
    if (tab === "jobs" && !this.state.logFocusMode) {
      void this.loadSelectedJobPreview();
    }
    this.tui?.requestRender();
  }

  private selectedJob(): OwnedJob | null {
    const sorted = sortJobs(this.state.jobs);
    return sorted[this.state.selectedIndex] ?? null;
  }

  private async loadSelectedJobPreview() {
    const identity = this.syncCredentialIdentity();
    if (this.state.tab !== "jobs" || this.state.logFocusMode || this.state.following) return;

    const job = this.selectedJob();
    if (!job) {
      this.updateState((s) => ({ ...s, logJob: null, logLines: [] }));
      this.tui?.requestRender();
      return;
    }

    const seq = ++this.previewRequestSeq;
    try {
      const logs = await getJobLogs(job.id, { tail: 1 });
      if (identity === null || this.syncCredentialIdentity() !== identity) return;
      if (seq !== this.previewRequestSeq) return;
      if (this.state.logFocusMode || this.state.following) return;
      const current = this.selectedJob();
      if (!current || current.id !== job.id) return;

      const lines = typeof logs.logs === "string" ? logs.logs.split("\n") : [JSON.stringify(logs)];
      this.updateState((s) => ({
        ...s,
        logJob: current,
        logLines: lines.slice(-1),
      }));
      this.tui?.requestRender();
    } catch {
      // Keep panel responsive; preview should never interrupt main interactions.
    }
  }

  private closeLogFocus() {
    this.stopFollow();
    this.updateState((s) => ({
      ...s,
      logFocusMode: false,
      logScrollOffset: 0,
      following: false,
    }));
    void this.loadSelectedJobPreview();
    this.tui?.requestRender();
  }

  private scrollLogWindow(delta: number) {
    const maxOffset = Math.max(0, this.state.logLines.length - 1);
    this.updateState((s) => ({
      ...s,
      logScrollOffset: Math.max(0, Math.min(maxOffset, s.logScrollOffset + delta)),
    }));
    this.tui?.requestRender();
  }

  private async openLogFocusForSelectedJob() {
    const job = this.selectedJob();
    if (!job) {
      this.uiCtx.notify("No job selected.", "warning");
      return;
    }

    this.updateState((s) => ({
      ...s,
      logFocusMode: true,
      logScrollOffset: 0,
      logJob: job,
      logLines: s.logJob?.id === job.id ? s.logLines : [],
    }));
    this.tui?.requestRender();

    if (job.status === "starting" || job.status === "running" || job.status === "pending") {
      await this.followLogs(job, { tail: 200, keepLogFocus: true });
    } else {
      await this.loadJobLogs(job, { tail: 2000, keepLogFocus: true });
    }
  }

  private async loadJobLogs(job: OwnedJob, options: { tail?: number; keepLogFocus?: boolean } = {}) {
    const identity = this.syncCredentialIdentity();
    if (!this.state.jobs.some((owned) => owned.id === job.id)) return;
    const tail = Number.isFinite(options.tail) ? Math.max(1, Math.floor(options.tail ?? 200)) : 200;

    this.stopFollow();
    this.updateState((s) => ({
      ...s,
      logJob: job,
      logLines: [],
      following: false,
      logFocusMode: options.keepLogFocus ? s.logFocusMode : false,
      logScrollOffset: 0,
    }));

    const worker = this.state.workers.find((w) => w.id === job.worker_id);
    this.onSetWidget(worker ?? null, job, null);
    this.onTrackJob(job);

    const logs = await getJobLogs(job.id, { tail });
    if (identity === null || this.syncCredentialIdentity() !== identity) return;
    const lines = typeof logs.logs === "string" ? logs.logs.split("\n") : [JSON.stringify(logs)];
    this.updateState((s) => ({
      ...s,
      logLines: lines,
      following: false,
      logScrollOffset: 0,
    }));
    this.tui?.requestRender();
  }

  private async followLogs(job: OwnedJob, options: { tail?: number; keepLogFocus?: boolean } = {}) {
    const identity = this.syncCredentialIdentity();
    if (!this.state.jobs.some((owned) => owned.id === job.id)) return;
    const tail = Number.isFinite(options.tail) ? Math.max(1, Math.floor(options.tail ?? 50)) : 50;

    this.stopFollow();
    this.updateState((s) => ({
      ...s,
      logJob: job,
      logLines: [],
      following: true,
      logFocusMode: options.keepLogFocus ? s.logFocusMode : false,
      logScrollOffset: 0,
    }));

    const worker = this.state.workers.find((w) => w.id === job.worker_id);
    this.onSetWidget(worker ?? null, job, null);
    this.onTrackJob(job);

    this.logSub = subscribeJobLogs(job.id, {
      tail,
      pollSeconds: 1,
      onLine: (line) => {
        if (identity === null || this.syncCredentialIdentity() !== identity) return;
        this.updateState((s) => ({
          ...s,
          logLines: [...s.logLines, line].slice(-5000),
          following: true,
        }));
        if (line.trim()) {
          events.emit("worker-harness:log-line", { job_id: job.id, line, credentialIdentity: identity });
        }
        this.tui?.requestRender();
      },
    });

    this.logSub.done.catch((err) => {
      if (identity === null || this.syncCredentialIdentity() !== identity) return;
      if (!this.isAbortError(err)) {
        this.uiCtx.notify(this.errorMessage(err), "error");
      }
      this.updateState((s) => ({ ...s, following: false }));
      this.tui?.requestRender();
    });
  }

  private selectedWorkerId(): string | null {
    if (this.state.tab === "workers") {
      return this.state.workers[this.state.selectedIndex]?.id ?? null;
    }

    if (this.state.tab === "jobs") {
      return this.selectedJob()?.worker_id ?? null;
    }

    return null;
  }

  private beginNewJobCommandMode() {
    const workerId = this.selectedWorkerId();
    if (!workerId) {
      this.uiCtx.notify("Select a worker (or a job tied to a worker) before pressing n.", "warning");
      return;
    }

    this.updateState((s) => ({
      ...s,
      newJobCommandMode: true,
      newJobCommandBuffer: "",
      newJobNameMode: false,
      newJobNameBuffer: "",
      newJobPendingCommand: "",
      newJobDetailsMode: null,
      newJobDetailsBuffer: "",
      newJobExpectedSeconds: 0,
      newTunnelPortMode: false,
      newTunnelPortBuffer: "",
      newTunnelLocalPortMode: false,
      newTunnelLocalPortBuffer: "",
      newTunnelNameMode: false,
      newTunnelNameBuffer: "",
      newTunnelPendingRemotePort: null,
      newTunnelPendingLocalPort: null,
      lastError: null,
    }));
    this.tui?.requestRender();
  }

  private handleNewJobCommandInput(data: string) {
    if (data === "\r" || data === "\n") {
      const command = this.state.newJobCommandBuffer.trim();
      if (!command) {
        this.uiCtx.notify("Command is empty. Type a command or press Esc to cancel.", "warning");
        return;
      }

      this.updateState((s) => ({
        ...s,
        newJobCommandMode: false,
        newJobNameMode: true,
        newJobNameBuffer: "",
        newJobPendingCommand: command,
      }));
      this.tui?.requestRender();
      return;
    }

    if (matchesKey(data, Key.escape)) {
      this.updateState((s) => ({
        ...s,
        newJobCommandMode: false,
        newJobCommandBuffer: "",
        newJobNameMode: false,
        newJobNameBuffer: "",
        newJobPendingCommand: "",
      }));
      this.tui?.requestRender();
      return;
    }

    if (data === "\u007f" || data === "\b") {
      this.updateState((s) => ({
        ...s,
        newJobCommandBuffer: s.newJobCommandBuffer.slice(0, -1),
      }));
      this.tui?.requestRender();
      return;
    }

    if (this.isPrintableInput(data)) {
      this.updateState((s) => ({
        ...s,
        newJobCommandBuffer: s.newJobCommandBuffer + data,
      }));
      this.tui?.requestRender();
    }
  }

  private handleNewJobNameInput(data: string) {
    if (data === "\r" || data === "\n") {
      const command = this.state.newJobPendingCommand.trim();
      if (!command) {
        this.uiCtx.notify("Missing command. Press n to start over.", "warning");
        this.updateState((s) => ({ ...s, newJobNameMode: false, newJobPendingCommand: "" }));
        this.tui?.requestRender();
        return;
      }

      const name = this.state.newJobNameBuffer.trim();
      if (!name) {
        this.uiCtx.notify("A public job name is required; do not include private command or path details.", "warning");
        return;
      }
      this.updateState((s) => ({
        ...s,
        newJobNameMode: false,
        newJobNameBuffer: name,
        newJobDetailsMode: "duration",
        newJobDetailsBuffer: "",
      }));
      this.tui?.requestRender();
      return;
    }

    if (matchesKey(data, Key.escape)) {
      this.updateState((s) => ({
        ...s,
        newJobNameMode: false,
        newJobNameBuffer: "",
        newJobPendingCommand: "",
      }));
      this.tui?.requestRender();
      return;
    }

    if (data === "\u007f" || data === "\b") {
      this.updateState((s) => ({
        ...s,
        newJobNameBuffer: s.newJobNameBuffer.slice(0, -1),
      }));
      this.tui?.requestRender();
      return;
    }

    if (this.isPrintableInput(data)) {
      this.updateState((s) => ({
        ...s,
        newJobNameBuffer: s.newJobNameBuffer + data,
      }));
      this.tui?.requestRender();
    }
  }

  private handleNewJobDetailsInput(data: string) {
    if (matchesKey(data, Key.escape)) {
      this.updateState((s) => ({ ...s, newJobDetailsMode: null, newJobDetailsBuffer: "", newJobPendingCommand: "", newJobNameBuffer: "", newJobExpectedSeconds: 0 }));
    } else if (data === "\u007f" || data === "\b") {
      this.updateState((s) => ({ ...s, newJobDetailsBuffer: s.newJobDetailsBuffer.slice(0, -1) }));
    } else if (data === "\r" || data === "\n") {
      const input = this.state.newJobDetailsBuffer.trim();
      if (this.state.newJobDetailsMode === "duration") {
        const seconds = Number(input);
        if (!Number.isSafeInteger(seconds) || seconds < 1) {
          this.uiCtx.notify("Expected duration must be a positive integer in seconds.", "warning");
          return;
        }
        this.updateState((s) => ({ ...s, newJobExpectedSeconds: seconds, newJobDetailsMode: "mode", newJobDetailsBuffer: "" }));
      } else if (this.state.newJobDetailsMode === "mode") {
        const mode = input.toLowerCase() || "cpu";
        if (mode !== "cpu" && mode !== "gpu") {
          this.uiCtx.notify("Choose cpu (default) or gpu.", "warning");
          return;
        }
        if (mode === "gpu") {
          this.updateState((s) => ({ ...s, newJobDetailsMode: "gpu_count", newJobDetailsBuffer: "" }));
        } else {
          this.submitNewJob(0);
        }
      } else {
        const count = Number(input);
        const worker = this.state.workers.find((worker) => worker.id === this.selectedWorkerId());
        if (!Number.isSafeInteger(count) || count < 1 || !worker || count > worker.gpu_count) {
          this.uiCtx.notify("GPU count must be positive and within the selected worker's capacity.", "warning");
          return;
        }
        this.submitNewJob(count);
      }
    } else if (this.isPrintableInput(data)) {
      this.updateState((s) => ({ ...s, newJobDetailsBuffer: s.newJobDetailsBuffer + data }));
    }
    this.tui?.requestRender();
  }

  private submitNewJob(gpuCount: number) {
    const command = this.state.newJobPendingCommand;
    const name = this.state.newJobNameBuffer;
    const seconds = this.state.newJobExpectedSeconds;
    this.updateState((s) => ({ ...s, newJobDetailsMode: null, newJobDetailsBuffer: "" }));
    this.startNewJob(command, name, seconds, gpuCount).catch((err) => this.uiCtx.notify(this.errorMessage(err), "error"));
  }

  private isPrintableInput(data: string): boolean {
    if (!data || data.length === 0) return false;
    if (data === "\t") return false;
    if (data.startsWith("\u001b")) return false;
    return !/[\x00-\x1F\x7F]/.test(data);
  }

  private beginNewTunnelPortMode() {
    const workerId = this.selectedWorkerId();
    if (!workerId) {
      this.uiCtx.notify("Select a worker (or a job tied to a worker) before pressing t.", "warning");
      return;
    }

    this.updateState((s) => ({
      ...s,
      newTunnelPortMode: true,
      newTunnelPortBuffer: "",
      newTunnelLocalPortMode: false,
      newTunnelLocalPortBuffer: "",
      newTunnelNameMode: false,
      newTunnelNameBuffer: "",
      newTunnelPendingRemotePort: null,
      newTunnelPendingLocalPort: null,
      newJobCommandMode: false,
      newJobCommandBuffer: "",
      newJobNameMode: false,
      newJobNameBuffer: "",
      newJobPendingCommand: "",
      lastError: null,
    }));
    this.tui?.requestRender();
  }

  private handleNewTunnelPortInput(data: string) {
    if (data === "\r" || data === "\n") {
      const remotePort = Number(this.state.newTunnelPortBuffer.trim());
      if (!Number.isInteger(remotePort) || remotePort < 1 || remotePort > 65535) {
        this.uiCtx.notify("Remote port must be an integer between 1 and 65535.", "warning");
        return;
      }

      this.updateState((s) => ({
        ...s,
        newTunnelPortMode: false,
        newTunnelLocalPortMode: true,
        newTunnelPendingRemotePort: remotePort,
        newTunnelLocalPortBuffer: "",
      }));
      this.tui?.requestRender();
      return;
    }

    if (matchesKey(data, Key.escape)) {
      this.updateState((s) => ({
        ...s,
        newTunnelPortMode: false,
        newTunnelPortBuffer: "",
        newTunnelPendingRemotePort: null,
        newTunnelPendingLocalPort: null,
      }));
      this.tui?.requestRender();
      return;
    }

    if (data === "\u007f" || data === "\b") {
      this.updateState((s) => ({
        ...s,
        newTunnelPortBuffer: s.newTunnelPortBuffer.slice(0, -1),
      }));
      this.tui?.requestRender();
      return;
    }

    if (/^[0-9]$/.test(data)) {
      this.updateState((s) => ({
        ...s,
        newTunnelPortBuffer: (s.newTunnelPortBuffer + data).slice(0, 5),
      }));
      this.tui?.requestRender();
    }
  }

  private handleNewTunnelLocalPortInput(data: string) {
    if (data === "\r" || data === "\n") {
      const trimmed = this.state.newTunnelLocalPortBuffer.trim();
      let localPort: number | null = null;
      if (trimmed.length > 0) {
        const parsed = Number(trimmed);
        if (!Number.isInteger(parsed) || parsed < 1024 || parsed > 65535) {
          this.uiCtx.notify("Local port must be blank or an integer between 1024 and 65535.", "warning");
          return;
        }
        localPort = parsed;
      }

      this.updateState((s) => ({
        ...s,
        newTunnelLocalPortMode: false,
        newTunnelNameMode: true,
        newTunnelPendingLocalPort: localPort,
        newTunnelNameBuffer: "",
      }));
      this.tui?.requestRender();
      return;
    }

    if (matchesKey(data, Key.escape)) {
      this.updateState((s) => ({
        ...s,
        newTunnelLocalPortMode: false,
        newTunnelLocalPortBuffer: "",
        newTunnelPendingRemotePort: null,
        newTunnelPendingLocalPort: null,
      }));
      this.tui?.requestRender();
      return;
    }

    if (data === "\u007f" || data === "\b") {
      this.updateState((s) => ({
        ...s,
        newTunnelLocalPortBuffer: s.newTunnelLocalPortBuffer.slice(0, -1),
      }));
      this.tui?.requestRender();
      return;
    }

    if (/^[0-9]$/.test(data)) {
      this.updateState((s) => ({
        ...s,
        newTunnelLocalPortBuffer: (s.newTunnelLocalPortBuffer + data).slice(0, 5),
      }));
      this.tui?.requestRender();
    }
  }

  private handleNewTunnelNameInput(data: string) {
    if (data === "\r" || data === "\n") {
      const remotePort = this.state.newTunnelPendingRemotePort;
      if (!remotePort || remotePort < 1) {
        this.uiCtx.notify("Missing remote port. Press t to start over.", "warning");
        this.updateState((s) => ({ ...s, newTunnelNameMode: false }));
        this.tui?.requestRender();
        return;
      }

      const localPort = this.state.newTunnelPendingLocalPort ?? undefined;
      const name = this.state.newTunnelNameBuffer.trim() || undefined;

      this.updateState((s) => ({
        ...s,
        newTunnelNameMode: false,
        newTunnelNameBuffer: "",
        newTunnelPortBuffer: "",
        newTunnelLocalPortBuffer: "",
        newTunnelPendingRemotePort: null,
        newTunnelPendingLocalPort: null,
      }));
      this.createTunnel(remotePort, localPort, name)
        .catch((err) => this.uiCtx.notify(this.errorMessage(err), "error"));
      this.tui?.requestRender();
      return;
    }

    if (matchesKey(data, Key.escape)) {
      this.updateState((s) => ({
        ...s,
        newTunnelNameMode: false,
        newTunnelNameBuffer: "",
        newTunnelPendingRemotePort: null,
        newTunnelPendingLocalPort: null,
      }));
      this.tui?.requestRender();
      return;
    }

    if (data === "\u007f" || data === "\b") {
      this.updateState((s) => ({
        ...s,
        newTunnelNameBuffer: s.newTunnelNameBuffer.slice(0, -1),
      }));
      this.tui?.requestRender();
      return;
    }

    if (this.isPrintableInput(data)) {
      this.updateState((s) => ({
        ...s,
        newTunnelNameBuffer: s.newTunnelNameBuffer + data,
      }));
      this.tui?.requestRender();
    }
  }

  private randomLocalPort(min = 20000, max = 60000): number {
    return Math.floor(Math.random() * (max - min + 1)) + min;
  }

  private isTunnelPortConflictError(err: unknown): boolean {
    const msg = this.errorMessage(err).toLowerCase();
    return (
      msg.includes("in use") ||
      msg.includes("already") ||
      msg.includes("conflict") ||
      msg.includes("eaddrinuse")
    );
  }

  private async startNewJob(command: string, name: string, expectedSeconds: number, gpuCount: number) {
    const identity = this.syncCredentialIdentity();
    if (identity === null) return;
    const workerId = this.selectedWorkerId();
    if (!workerId) {
      this.uiCtx.notify("Select a worker (or a job tied to a worker) before pressing n.", "warning");
      return;
    }

    const request = {
      worker_id: workerId,
      command,
      name,
      expected_seconds: expectedSeconds,
    };
    const job = gpuCount > 0
      ? await enqueueJob({ ...request, gpu_count: gpuCount })
      : await startJob(request);
    if (identity === null || this.syncCredentialIdentity() !== identity) return;

    this.updateState((s) => ({
      ...s,
      newJobCommandBuffer: "",
      newJobNameBuffer: "",
      newJobPendingCommand: "",
    }));
    this.insertOptimisticJob(job, { focus: true });
    this.tui?.requestRender();

    void this.refreshData();
    void this.sleep(1200).then(() => this.refreshData());

    events.emit("worker-harness:refresh", undefined);
    this.uiCtx.notify(`${gpuCount > 0 ? "Queued" : "Started"} job ${job.id.slice(0, 8)} on ${workerId}.`, "info");
  }

  private async createTunnel(remotePort: number, localPort?: number, name?: string) {
    const identity = this.syncCredentialIdentity();
    const workerId = this.selectedWorkerId();
    if (!workerId) {
      this.uiCtx.notify("Select a worker (or a job tied to a worker) before pressing t.", "warning");
      return;
    }

    let lastErr: unknown = null;
    const useRandomLocalPort = !Number.isInteger(localPort);

    for (let attempt = 0; attempt < 8; attempt++) {
      if (identity === null || this.syncCredentialIdentity() !== identity) return;
      const candidateLocalPort = useRandomLocalPort ? this.randomLocalPort() : Number(localPort);
      try {
        const tunnel = await addTunnel({
          worker_id: workerId,
          local_port: candidateLocalPort,
          remote_port: remotePort,
          name,
        });
        if (this.syncCredentialIdentity() !== identity) return;

        this.updateState((s) => ({
          ...s,
          newTunnelPortBuffer: "",
          newTunnelLocalPortBuffer: "",
          newTunnelNameBuffer: "",
          tunnels: s.tunnels.some((t) => t.id === tunnel.id) ? s.tunnels : [tunnel, ...s.tunnels],
        }));
        this.tui?.requestRender();

        events.emit("worker-harness:refresh", undefined);
        this.uiCtx.notify(
          `Tunnel created localhost:${tunnel.local_port} -> ${workerId}:${tunnel.remote_port}.`,
          "info",
        );
        return;
      } catch (err) {
        if (this.syncCredentialIdentity() !== identity) return;
        lastErr = err;
        if (!useRandomLocalPort || !this.isTunnelPortConflictError(err)) break;
      }
    }

    throw lastErr ?? new Error("Failed to allocate local port for tunnel.");
  }

  private async stopSelectedJob() {
    const job = this.selectedJob();
    if (!job || !["pending", "starting", "running"].includes(job.status)) return;

    await stopJob(job.id);
    events.emit("worker-harness:refresh", undefined);
    await this.refreshData({ notifyOnError: true });
  }

  private async createOwnedMarimo() {
    const identity = this.syncCredentialIdentity();
    const selected = this.state.tab === "workers" ? this.state.workers[this.state.selectedIndex] : null;
    let worker = selected;
    if (!worker) {
      const name = await this.uiCtx.select("Marimo worker", this.state.workers.map((worker) => worker.name));
      worker = this.state.workers.find((worker) => worker.name === name) ?? null;
    }
    if (!worker) return;
    const notebookPath = await this.uiCtx.input("Notebook path", "/workspace/notebook.py");
    if (!notebookPath?.trim()) return;
    const environment = await this.uiCtx.input("Environment", "/workspace/.venv");
    if (!environment?.trim()) return;
    const timeoutInput = await this.uiCtx.input("Ready timeout in seconds (blank = 30)", "30");
    if (timeoutInput === undefined) return;
    const timeout = timeoutInput.trim() ? Number(timeoutInput) : 30;
    if (!Number.isFinite(timeout) || timeout <= 0) throw new Error("Ready timeout must be positive.");
    if (identity === null || this.syncCredentialIdentity() !== identity) return;
    await createMarimo({ worker_id: worker.id, notebook_path: notebookPath.trim(), environment: environment.trim(), ready_timeout: timeout });
    if (this.syncCredentialIdentity() !== identity) return;
    this.state.tab = "marimo";
    this.state.selectedIndex = 0;
    await this.refreshData({ notifyOnError: true });
  }

  private async openSelectedMarimo() {
    const identity = this.syncCredentialIdentity();
    const selected = this.state.marimo[this.state.selectedIndex];
    if (!selected) return;
    const session = await getMarimo(selected.id);
    if (identity === null || this.syncCredentialIdentity() !== identity) return;
    if (session.status !== "ready") throw new Error("This notebook is stopped.");
    const url = new URL(session.url);
    if (url.protocol !== "http:" && url.protocol !== "https:") throw new Error("Notebook returned an invalid browser URL.");
    const command = process.platform === "darwin" ? "open" : process.platform === "win32" ? "explorer.exe" : "xdg-open";
    await new Promise<void>((resolve, reject) => {
      const child = spawn(command, [url.href], { stdio: "ignore" });
      child.once("error", reject);
      child.once("exit", (code) => code === 0 ? resolve() : reject(new Error(`Could not open browser; open ${url.href}`)));
    });
  }

  private async stopSelectedMarimo() {
    this.syncCredentialIdentity();
    const session = this.state.marimo[this.state.selectedIndex];
    if (!session || session.status !== "ready") return;
    await removeMarimo(session.id);
    await this.refreshData({ notifyOnError: true });
  }

  private async removeSelectedTunnel() {
    this.syncCredentialIdentity();
    const tunnel = this.state.tunnels[this.state.selectedIndex];
    if (!tunnel) return;
    await removeTunnel(tunnel.id);
    await this.refreshData({ notifyOnError: true });
  }

  invalidate(): void {
    // No-op: render is stateless
  }
}

export async function openPanel(
  uiCtx: any,
  onTrackJob: (job: OwnedJob) => void,
  onSetWidget: (worker: PublicWorker | null, job: OwnedJob | null, jobName: string | null) => void,
): Promise<void> {
  const panel = new SimplePanel();
  await panel.show(uiCtx, onTrackJob, onSetWidget);
}
