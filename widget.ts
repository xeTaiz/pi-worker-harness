import { Container, Text } from "@earendil-works/pi-tui";
import { events } from "./events.ts";
import type { Worker, Job } from "./types.ts";
import { aggregateGpuStatus } from "./gpu-status.ts";

export interface WidgetState {
  trackedWorker: string | null;
  trackedJob: string | null;
  trackedJobName: string | null;
  lastLogLine: string;
  following: boolean;
  workers: Worker[];
  jobs: Job[];
}

export interface WidgetHandle {
  updateState: (patch: Partial<WidgetState>) => void;
  readonly state: WidgetState;
  teardown: () => void;
}

const MAX_STATUS_WIDTH = 120;

function stripAnsi(input: string): string {
  return input.replace(/\u001B\[[0-9;]*m/g, "");
}

function truncate(input: string, max: number): string {
  if (input.length <= max) return input;
  if (max <= 1) return "…";
  return input.slice(0, max - 1) + "…";
}

export function buildStatusLine(state: WidgetState): string {
  if (!state.trackedWorker) {
    const online = state.workers.filter((w) => w.status === "online").length;
    const running = state.jobs.filter((j) => j.status === "running").length;
    const gpuSummary = aggregateGpuStatus(state.workers)
      .map(({ model, busy, total }) => `${model}: ${busy}/${total}`)
      .join(" · ");
    return `▸ Workers: ${online}/${state.workers.length} online · Jobs: ${running} running · GPUs: ${gpuSummary || "—"}`;
  }

  const worker = state.workers.find((w) => w.name === state.trackedWorker);
  const icon = worker?.status === "online" ? "●" : "○";
  const workerPart = `${icon} ${state.trackedWorker}`;

  const jobPart =
    state.trackedJobName ||
    state.trackedJob?.slice(0, 8) ||
    "-";
  const jobShort = jobPart.length > 30 ? jobPart.slice(0, 29) + "…" : jobPart;

  const avail = MAX_STATUS_WIDTH - workerPart.length - jobShort.length - 8;
  const logPart = state.lastLogLine
    ? truncate(stripAnsi(state.lastLogLine), Math.max(avail, 10))
    : "[no logs]";

  const follow = state.following ? " ◀" : "";
  return `▸ ${workerPart} > ${jobShort} > ${logPart}${follow}`;
}

export function setupWidget(
  ui: any,
  initialState: WidgetState
): WidgetHandle {
  let state: WidgetState = { ...initialState };

  function makeComponent(_theme: any): Container {
    const c = new Container();
    c.addChild(new Text(buildStatusLine(state), 1, 0));
    return c;
  }

  function renderWidget() {
    ui.setWidget("worker-harness", makeComponent, { placement: "aboveEditor" });
  }

  renderWidget();

  const unsubscribeLogLine = events.on<{ job_id: string; line: string }>(
    "worker-harness:log-line",
    (data) => {
      if (data.job_id === state.trackedJob && data.line.trim()) {
        state = { ...state, lastLogLine: data.line, following: true };
        renderWidget();
      }
    }
  );

  return {
    updateState(patch: Partial<WidgetState>) {
      state = { ...state, ...patch };
      renderWidget();
    },
    get state() {
      return state;
    },
    teardown() {
      unsubscribeLogLine();
      ui.setWidget("worker-harness", undefined, { placement: "aboveEditor" });
    },
  };
}

export const initWidget = setupWidget;
