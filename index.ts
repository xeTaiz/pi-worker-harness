import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import {
  registerGroupedTools,
  registerAdminTools,
} from "./tools/index.ts";
import { openPanel } from "./panel/index.ts";
import { initWidget } from "./widget.ts";
import {
  getOrchestratorUrl,
  listJobs,
  listWorkers,
  setOrchestratorUrl,
} from "./api.ts";
import { events } from "./events.ts";
import type { Job } from "./types.ts";
import { registerSessionBridge } from "./session-bridge.ts";
import {
  getWorkerHarnessConfigPath,
  loadWorkerHarnessConfig,
  saveWorkerHarnessConfig,
} from "./config.ts";

export default async function (pi: ExtensionAPI) {
  try {
    const config = await loadWorkerHarnessConfig();
    if (typeof config.orchestratorUrl === "string" && config.orchestratorUrl.trim()) {
      setOrchestratorUrl(config.orchestratorUrl.trim());
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.warn(`[pi-worker-harness] Failed to load config ${getWorkerHarnessConfigPath()}: ${message}`);
  }

  // Ordinary non-worker Pi sessions register over the control plane. Worker
  // delegates are excluded inside registerSessionBridge and keep their UDS-only profile.
  registerSessionBridge(pi);

  // Register all tools.
  // wh_read / wh_dispatch are the only worker-harness tools that cover workers,
  // jobs, tunnels, file transfer, git access, and Pi session operations. They
  // are action-based so subagent configs can grant RO/RW with one tool name.
  // wh_admin_* remain as individual tools for image deploy / worker restart,
  // which are not subagent-eligible.
  registerGroupedTools(pi);
  registerAdminTools(pi);

  // Captured from session_start to use ctx.ui for widget updates
  let uiCtx: ExtensionContext | null = null;
  let widgetHandle: ReturnType<typeof initWidget> | null = null;
  let refreshUnsubscribe: (() => void) | null = null;

  async function refreshWidgetState(): Promise<void> {
    if (!widgetHandle) return;

    const [workers, jobs] = await Promise.all([listWorkers(), listJobs()]);
    widgetHandle.updateState({ workers, jobs });

    // Stop following if tracked job finished
    const { trackedJob } = widgetHandle.state;
    if (trackedJob) {
      const job = jobs.find((j) => j.id === trackedJob);
      if (job && !["pending", "starting", "running"].includes(job.status)) {
        widgetHandle.updateState({ following: false });
      }
    }
  }

  pi.on("session_start", async (_event, ctx) => {
    uiCtx = ctx;

    widgetHandle = initWidget(ctx.ui as any, {
      trackedWorker: null,
      trackedJob: null,
      trackedJobName: null,
      lastLogLine: "",
      following: false,
      workers: [],
      jobs: [],
    });

    // Never block session startup on worker-harness network calls.
    void refreshWidgetState().catch((err) => {
      const message = err instanceof Error ? err.message : String(err);
      widgetHandle?.updateState({
        lastLogLine: `worker-harness unavailable: ${message}`,
        workers: [],
        jobs: [],
      });
    });

    // Event-driven refresh lifecycle (replaces interval polling)
    refreshUnsubscribe = events.on<void>("worker-harness:refresh", async () => {
      try {
        await refreshWidgetState();
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        ctx.ui.notify(`Worker harness refresh failed: ${message}`, "error");
      }
    });
  });

  pi.on("session_shutdown", async () => {
    if (refreshUnsubscribe) {
      refreshUnsubscribe();
      refreshUnsubscribe = null;
    }
    widgetHandle?.teardown();
    widgetHandle = null;
    uiCtx = null;
  });

  // Command to open the TUI panel
  pi.registerCommand("worker-harness", {
    description: "Open the Worker Harness panel (workers, jobs, tunnels, logs)",
    handler: async (_args, ctx) => {
      const activeCtx = uiCtx ?? ctx;
      try {
        await openPanel(
          activeCtx.ui as any,
          // onTrackJob: set as tracked job when user selects a job in panel
          (job: Job) => {
            if (widgetHandle) widgetHandle.updateState({ trackedJob: job.id });
          },
          // onSetWidget: update the pinned status bar
          (worker, job, jobName) => {
            if (widgetHandle) {
              widgetHandle.updateState({
                trackedWorker: worker?.name ?? null,
                trackedJob: job?.id ?? null,
                trackedJobName: jobName ?? null,
                lastLogLine: "",
                following: false,
              });
            }
          }
        );
      } catch (err) {
        ctx.ui.notify(`Worker harness error: ${err}`, "error");
      }
    },
  });

  // Keyboard shortcut: Ctrl+Shift+H
  pi.registerShortcut("ctrl+shift+h", {
    description: "Toggle Worker Harness panel",
    handler: async (_args, ctx) => {
      const activeCtx = uiCtx ?? ctx;
      try {
        await openPanel(
          activeCtx.ui as any,
          (job: Job) => {
            if (widgetHandle) widgetHandle.updateState({ trackedJob: job.id });
          },
          (worker, job, jobName) => {
            if (widgetHandle) {
              widgetHandle.updateState({
                trackedWorker: worker?.name ?? null,
                trackedJob: job?.id ?? null,
                trackedJobName: jobName ?? null,
                lastLogLine: "",
                following: false,
              });
            }
          }
        );
      } catch (err) {
        ctx.ui.notify(`Worker harness error: ${err}`, "error");
      }
    },
  });

  pi.registerCommand("worker-harness-url", {
    description: "Get or set worker-harness orchestrator URL",
    handler: async (args, ctx) => {
      const nextUrl =
        Array.isArray(args) ? args.join(" ").trim() : String(args ?? "").trim();

      if (!nextUrl) {
        ctx.ui.notify(
          `Worker harness URL: ${getOrchestratorUrl()} (${getWorkerHarnessConfigPath()})`,
          "info",
        );
        return;
      }

      setOrchestratorUrl(nextUrl);

      try {
        const existing = await loadWorkerHarnessConfig();
        await saveWorkerHarnessConfig({
          ...existing,
          orchestratorUrl: getOrchestratorUrl(),
        });
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        ctx.ui.notify(`Worker harness URL set, but failed to persist config: ${message}`, "warning");
        events.emit("worker-harness:refresh", undefined);
        return;
      }

      events.emit("worker-harness:refresh", undefined);
      ctx.ui.notify(
        `Worker harness URL set to ${getOrchestratorUrl()} and saved to ${getWorkerHarnessConfigPath()}`,
        "success",
      );
    },
  });

  // Command to manually refresh widget state
  pi.registerCommand("workers-refresh", {
    description: "Refresh worker harness data and status bar widget",
    handler: async (_args, ctx) => {
      try {
        await refreshWidgetState();
        ctx.ui.notify("Worker harness data refreshed", "info");
      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        ctx.ui.notify(`Worker harness refresh failed: ${message}`, "error");
      }
    },
  });
}
