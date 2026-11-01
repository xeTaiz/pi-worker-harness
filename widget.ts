import { Container, Text } from "@earendil-works/pi-tui";
import { events } from "./events.ts";
import type { PublicWorker, OwnedJob } from "./types.ts";
import { aggregateGpuStatus } from "./gpu-status.ts";
import { getCredentialIdentity } from "./api.ts";

export interface WidgetState {
  trackedWorker: string | null;
  trackedJob: string | null;
  trackedJobName: string | null;
  lastLogLine: string;
  following: boolean;
  workers: PublicWorker[];
  jobs: OwnedJob[];
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
    const active = state.jobs.filter((j) => j.status === "starting" || j.status === "running").length;
    const gpuSummary = aggregateGpuStatus(state.workers)
      .map(({ model, busy, total }) => `${model}: ${busy}/${total}`)
      .join(" · ");
    return `▸ Workers: ${online}/${state.workers.length} online · Jobs: ${active} active · GPUs: ${gpuSummary || "—"}`;
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
  let credentialIdentity: string | null;
  try {
    credentialIdentity = getCredentialIdentity();
  } catch {
    credentialIdentity = null;
  }

  function clearChangedCredentials(): boolean {
    let identity: string | null;
    try {
      identity = getCredentialIdentity();
    } catch {
      identity = null;
    }
    if (identity !== null && identity === credentialIdentity) return false;
    credentialIdentity = identity;
    state = { ...state, trackedWorker: null, trackedJob: null, trackedJobName: null, lastLogLine: "", following: false, jobs: [] };
    return true;
  }

  function makeComponent(_theme: any): Container {
    clearChangedCredentials();
    const c = new Container();
    const text = new Text(buildStatusLine(state), 1, 0);
    c.addChild(text);
    const render = c.render.bind(c);
    c.render = (width: number) => {
      clearChangedCredentials();
      text.setText(buildStatusLine(state));
      return render(width);
    };
    return c;
  }

  function renderWidget() {
    ui.setWidget("worker-harness", makeComponent, { placement: "belowEditor" });
  }

  renderWidget();

  const unsubscribeLogLine = events.on<{ job_id: string; line: string; credentialIdentity: string }>(
    "worker-harness:log-line",
    (data) => {
      if (clearChangedCredentials()) {
        renderWidget();
        return;
      }
      if (data.credentialIdentity === credentialIdentity && data.job_id === state.trackedJob && data.line.trim()) {
        state = { ...state, lastLogLine: data.line, following: true };
        renderWidget();
      }
    }
  );

  return {
    updateState(patch: Partial<WidgetState>) {
      if (clearChangedCredentials()) {
        renderWidget();
        return;
      }
      state = { ...state, ...patch };
      renderWidget();
    },
    get state() {
      clearChangedCredentials();
      return state;
    },
    teardown() {
      unsubscribeLogLine();
      ui.setWidget("worker-harness", undefined, { placement: "belowEditor" });
    },
  };
}

export const initWidget = setupWidget;
