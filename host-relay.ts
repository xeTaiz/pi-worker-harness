#!/usr/bin/env bun
import { chmodSync, existsSync, mkdirSync, realpathSync, rmSync } from "node:fs";
import { createConnection, createServer as createUnixServer } from "node:net";
import { dirname } from "node:path";

const PROTOCOL_VERSION = 2;
const RELAY_REVISION = 17;
const LOCAL_HOST = "127.0.0.1";
const LOCAL_PORT = Number.parseInt(process.env.WH_PI_HOST_RELAY_LOCAL_PORT || "27890", 10);
const PUBLIC_PORT = Number.parseInt(process.env.WH_PI_HOST_RELAY_PORT || "27888", 10);
const SOCKET_PATH = process.env.WH_PI_HOST_RELAY_SOCKET || `${process.env.XDG_RUNTIME_DIR || `/tmp/worker-harness-${process.getuid?.() ?? "user"}`}/worker-harness/pi-host-relay.sock`;
const PUBLISH = process.env.WH_PI_HOST_RELAY_PUBLISH !== "0";
const RELAY_DEBUG = process.env.WH_PI_RELAY_DEBUG === "1";
const ROUTE_TTL_MS = 90_000;
const MAX_ATTACHMENTS_PER_SESSION = Math.max(1, Number.parseInt(process.env.WH_PI_MAX_ATTACHMENTS || "8", 10) || 8);
const WS_BACKPRESSURE_LIMIT = Math.min(
  64 * 1024 * 1024,
  Math.max(
    1024 * 1024,
    Number.parseInt(process.env.WH_PI_WS_BACKPRESSURE_LIMIT || `${8 * 1024 * 1024}`, 10) || 8 * 1024 * 1024,
  ),
);
const REAPER_INTERVAL_MS = 30_000;
const KNOWN_ATTACHMENT_CLOSE_REASONS = new Set([
  "terminal detached",
  "terminal output backpressure",
  "replaced by newer attachment",
  "attachment limit reached",
  "session not found",
  "terminal attach failed",
  "Zellij attach failed",
]);

type TmuxRoute = {
  multiplexer: "tmux";
  sessionId: string;
  incarnation: string;
  tmuxSocket: string;
  tmuxSession: string;
  windowIndex: string;
  paneIndex: string;
  tmuxPaneId: string;
  managedRuntime: boolean;
  updatedAt: number;
};
type TmuxLocation = {
  tmuxSession: string;
  windowIndex: string;
  paneIndex: string;
  tmuxPaneId: string;
};
type ZellijRoute = {
  multiplexer: "zellij";
  sessionId: string;
  incarnation: string;
  zellijSessionName: string;
  zellijPaneId: string;
  updatedAt: number;
};
type Route = TmuxRoute | ZellijRoute;

type SocketData = { sessionId: string; attachmentId: string; initialRows: number; initialCols: number; rejected?: boolean };
type Attachment = {
  id: string;
  sessionId: string;
  multiplexer: Route["multiplexer"];
  ws: ServerWebSocket<SocketData>;
  terminal: InstanceType<typeof Bun.Terminal>;
  process: ReturnType<typeof Bun.spawn>;
  relaySession: string;
  tmuxSocket?: string;
  windowKey?: string;
  ready: boolean;
  startupPoll?: ReturnType<typeof setInterval>;
  startupTimeout?: ReturnType<typeof setTimeout>;
  lastActivityAt: number;
  rows: number;
  cols: number;
  closing: boolean;
};
type WindowAttachmentState = {
  route: TmuxRoute;
  sourceWindow: string;
  targetPaneId: string;
  originalActivePaneId: string;
  originalZoomed: boolean;
  attachmentIds: Set<string>;
};

process.umask(0o077);
const routes = new Map<string, Route>();
const attachments = new Map<string, Attachment>();
const sessionAttachmentIds = new Map<string, Set<string>>();
const reservationActivityAt = new Map<string, number>();
const replacedReservations = new Set<string>();
const windowAttachmentStates = new Map<string, WindowAttachmentState>();
let attachmentEvictionsTotal = 0;
let attachmentBackpressureDropsTotal = 0;
const attachmentCloseReasons = new Map<string, number>();
let published = false;
let tailnetHost = process.env.WH_PI_HOST_RELAY_HOST || "";

function run(command: string[], timeout = 10_000, env?: Record<string, string>) {
  return Bun.spawnSync(command, { stdout: "pipe", stderr: "pipe", timeout, ...(env ? { env } : {}) });
}

function output(result: ReturnType<typeof Bun.spawnSync>, stream: "stdout" | "stderr" = "stdout"): string {
  return new TextDecoder().decode(result[stream]).trim();
}

function countAttachmentClose(code: number, reason: string): void {
  const key = KNOWN_ATTACHMENT_CLOSE_REASONS.has(reason) ? reason : `code-${code}`;
  attachmentCloseReasons.set(key, (attachmentCloseReasons.get(key) || 0) + 1);
}

function sendTerminalData(ws: ServerWebSocket<SocketData>, data: Uint8Array): void {
  if (ws.readyState !== 1) return;
  const status = ws.send(data);
  if (status !== 0) return;
  attachmentBackpressureDropsTotal += 1;
  if (ws.readyState === 1) ws.close(1013, "terminal output backpressure");
}

function resizeAttachmentTerminal(attachment: Attachment, cols: number, rows: number): void {
  attachment.terminal.resize(cols, rows);
  // Bun.Terminal.resize updates the PTY winsize but Bun 1.3.14 does not wake
  // an attached tmux client. Signal it explicitly so tmux re-reads TIOCGWINSZ,
  // applies `window-size latest`, and forwards SIGWINCH to the source Pi.
  try { attachment.process.kill("SIGWINCH"); } catch { /* process already exited */ }
}

function tmuxEnvironment(): Record<string, string> {
  const clientContext = new Set([
    "TMUX", "TMUX_PANE", "ZELLIJ", "ZELLIJ_SESSION_NAME", "ZELLIJ_PANE_ID",
  ]);
  return Object.fromEntries(
    Object.entries(process.env).flatMap(([key, value]) => (
      value !== undefined && !clientContext.has(key) ? [[key, value]] : []
    )),
  );
}

function zellijEnvironment(): Record<string, string> {
  const clientContext = new Set(["ZELLIJ", "ZELLIJ_SESSION_NAME", "ZELLIJ_PANE_ID"]);
  const environment = Object.fromEntries(
    Object.entries(process.env).flatMap(([key, value]) => (
      value !== undefined && !clientContext.has(key) ? [[key, value]] : []
    )),
  );
  // Long-lived relays can outlive temporary launch directories. Zellij uses
  // TMPDIR for its CLI log path and panics if that directory has disappeared.
  if (environment.TMPDIR && !existsSync(environment.TMPDIR)) delete environment.TMPDIR;
  return environment;
}

function tmux(route: TmuxRoute, args: string[]) {
  return run(["tmux", "-S", route.tmuxSocket, ...args], 5_000, tmuxEnvironment());
}

function locateTmuxPane(
  tmuxSocket: string,
  tmuxPaneId: string,
  managedRuntime: boolean,
  previous?: TmuxRoute,
): TmuxLocation | null {
  const listed = run([
    "tmux", "-S", tmuxSocket, "list-panes", "-a", "-F",
    "#{session_name}\t#{window_index}\t#{pane_index}\t#{pane_id}",
  ], 3_000, tmuxEnvironment());
  if (listed.exitCode !== 0) return null;
  const candidates = output(listed).split("\n").flatMap((line) => {
    const [tmuxSession, windowIndex, paneIndex, resolvedPaneId] = line.split("\t");
    return tmuxSession && windowIndex && paneIndex && resolvedPaneId === tmuxPaneId
      ? [{ tmuxSession, windowIndex, paneIndex, tmuxPaneId: resolvedPaneId }]
      : [];
  });
  if (candidates.length === 0) return null;
  if (managedRuntime) {
    const owner = candidates.find((candidate) => candidate.tmuxSession === "wh-pi");
    if (owner) return owner;
  }
  if (previous && previous.tmuxSocket === tmuxSocket && previous.tmuxPaneId === tmuxPaneId) {
    const prior = candidates.find((candidate) => (
      candidate.tmuxSession === previous.tmuxSession
      && candidate.windowIndex === previous.windowIndex
      && candidate.paneIndex === previous.paneIndex
    ));
    if (prior) return prior;
  }
  const source = candidates.find((candidate) => !/^wh_(?:attach|diag)_/.test(candidate.tmuxSession));
  return source || candidates.sort((left, right) => (
    `${left.tmuxSession}\t${left.windowIndex}\t${left.paneIndex}`
      .localeCompare(`${right.tmuxSession}\t${right.windowIndex}\t${right.paneIndex}`)
  ))[0];
}

function zellij(route: ZellijRoute, args: string[]) {
  return run(["zellij", "--session", route.zellijSessionName, "action", ...args], 5_000, zellijEnvironment());
}

function zellijPanes(route: ZellijRoute): Array<Record<string, unknown>> {
  const result = zellij(route, ["list-panes", "--json", "--all"]);
  if (result.exitCode !== 0) return [];
  try {
    const panes = JSON.parse(output(result));
    return Array.isArray(panes) ? panes : [];
  } catch {
    return [];
  }
}

function zellijClients(route: ZellijRoute): Map<string, string> {
  const result = zellij(route, ["list-clients"]);
  if (result.exitCode !== 0) return new Map();
  return new Map(output(result).split("\n").slice(1).flatMap((line) => {
    const match = line.trim().match(/^(\S+)\s+(\S+)/);
    return match ? [[match[1], match[2]]] : [];
  }));
}

function routeIsLive(route: Route): boolean {
  if (Date.now() - route.updatedAt > ROUTE_TTL_MS) return false;
  if (route.multiplexer === "zellij") {
    return zellijPanes(route).some((pane) => (
      pane.is_plugin === false && `terminal_${Number(pane.id)}` === route.zellijPaneId
    ));
  }
  const result = tmux(route, ["display-message", "-p", "-t", route.tmuxPaneId, "#{pane_id}"]);
  return result.exitCode === 0 && output(result) === route.tmuxPaneId;
}

function routeIdentity(route: Route): string {
  return route.multiplexer === "tmux"
    ? `tmux\u0000${route.tmuxSocket}\u0000${route.tmuxPaneId}`
    : `zellij\u0000${route.zellijSessionName}\u0000${route.zellijPaneId}`;
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", `'\"'\"'`)}'`;
}

function activePaneId(route: TmuxRoute, windowTarget: string): string {
  const result = tmux(route, ["list-panes", "-t", windowTarget, "-F", "#{pane_active}\t#{pane_id}"]);
  if (result.exitCode !== 0) return "";
  return output(result).split("\n").find((line) => line.startsWith("1\t"))?.split("\t")[1] || "";
}

function windowIsZoomed(route: TmuxRoute, windowTarget: string): boolean {
  const result = tmux(route, ["display-message", "-p", "-t", windowTarget, "#{window_zoomed_flag}"]);
  return result.exitCode === 0 && output(result) === "1";
}

function tmuxStatus(route: TmuxRoute, target?: string, global = false): string {
  const args = ["show-options", "-v"];
  if (global) args.push("-g");
  else if (target) args.push("-t", target);
  args.push("status");
  const result = tmux(route, args);
  return result.exitCode === 0 ? output(result) : `error:${output(result, "stderr")}`;
}

function debugTmuxStatus(route: TmuxRoute, relaySession: string, phase: string): void {
  if (!RELAY_DEBUG) return;
  console.error(`[pi-host-relay] tmux-status phase=${phase} session=${route.sessionId} managed=${route.managedRuntime} source=${route.tmuxSession} relay=${relaySession} global=${tmuxStatus(route, undefined, true)} owner=${tmuxStatus(route, route.tmuxSession)} grouped=${tmuxStatus(route, relaySession)}`);
}

function discoverTailnetHost(): string {
  if (tailnetHost) return tailnetHost;
  const result = run(["tailscale", "ip", "-4"]);
  if (result.exitCode === 0) tailnetHost = output(result).split(/\s+/)[0] || "";
  return tailnetHost;
}

function publishRelay(): boolean {
  if (!PUBLISH) {
    tailnetHost = tailnetHost || LOCAL_HOST;
    return true;
  }
  const expected = `tcp://127.0.0.1:${LOCAL_PORT}`;
  const status = run(["tailscale", "serve", "status"]);
  const statusText = `${output(status)}\n${output(status, "stderr")}`;
  if (status.exitCode === 0 && statusText.includes(expected)) return Boolean(discoverTailnetHost());
  if (statusText.includes(String(PUBLIC_PORT))) {
    console.error(`[pi-host-relay] refusing to replace existing Tailscale Serve rule on ${PUBLIC_PORT}`);
    return false;
  }
  const result = run([
    "tailscale", "serve", "--bg", "--yes", `--tcp=${PUBLIC_PORT}`, expected,
  ]);
  if (result.exitCode !== 0) {
    console.error(`[pi-host-relay] publication failed: ${output(result, "stderr")}`);
    return false;
  }
  return Boolean(discoverTailnetHost());
}

function unpublishRelay(): void {
  if (!PUBLISH || !published) return;
  const expected = `tcp://127.0.0.1:${LOCAL_PORT}`;
  const status = run(["tailscale", "serve", "status"]);
  if (!output(status).includes(expected)) return;
  run(["tailscale", "serve", `--tcp=${PUBLIC_PORT}`, "off"]);
}

function terminalDimension(value: unknown, fallback: number): number {
  return Math.max(1, Math.min(1000, Math.floor(Number(value) || fallback)));
}

function attachUrl(sessionId: string): string {
  const port = PUBLISH ? PUBLIC_PORT : LOCAL_PORT;
  return `ws://${tailnetHost}:${port}/v1/sessions/${encodeURIComponent(sessionId)}/attach`;
}

function reclaimLongestIdleAttachment(sessionId: string): boolean {
  const ids = sessionAttachmentIds.get(sessionId);
  if (!ids?.size) return false;
  const victimId = [...ids].reduce((oldest, candidate) => {
    const oldestAt = attachments.get(oldest)?.lastActivityAt ?? reservationActivityAt.get(oldest) ?? 0;
    const candidateAt = attachments.get(candidate)?.lastActivityAt ?? reservationActivityAt.get(candidate) ?? 0;
    return candidateAt < oldestAt || (candidateAt === oldestAt && candidate < oldest) ? candidate : oldest;
  });
  const victim = attachments.get(victimId);
  if (victim) {
    try {
      if (victim.ws.readyState === 1) {
        victim.ws.send(JSON.stringify({
          type: "status",
          state: "replaced",
          reason: "attachment capacity reclaimed by a newer client",
        }));
        victim.ws.close(4410, "replaced by newer attachment");
      }
    } catch {
      // Cleanup below is the authoritative detach path.
    } finally {
      cleanupAttachment(victimId);
    }
  } else {
    releaseReservation(sessionId, victimId);
    replacedReservations.add(victimId);
  }
  attachmentEvictionsTotal += 1;
  return true;
}

function reserveAttachment(sessionId: string): string | null {
  const ids = sessionAttachmentIds.get(sessionId) || new Set<string>();
  if (ids.size >= MAX_ATTACHMENTS_PER_SESSION && !reclaimLongestIdleAttachment(sessionId)) return null;
  const activeIds = sessionAttachmentIds.get(sessionId) || new Set<string>();
  if (activeIds.size >= MAX_ATTACHMENTS_PER_SESSION) return null;
  const attachmentId = crypto.randomUUID();
  activeIds.add(attachmentId);
  reservationActivityAt.set(attachmentId, Date.now());
  sessionAttachmentIds.set(sessionId, activeIds);
  return attachmentId;
}

function releaseReservation(sessionId: string, attachmentId: string): void {
  reservationActivityAt.delete(attachmentId);
  const ids = sessionAttachmentIds.get(sessionId);
  if (!ids) return;
  ids.delete(attachmentId);
  if (ids.size === 0) sessionAttachmentIds.delete(sessionId);
}

function restoreWindowState(state: WindowAttachmentState, force = false): void {
  const active = activePaneId(state.route, state.sourceWindow);
  const relayZoomed = windowIsZoomed(state.route, state.sourceWindow) && active === state.targetPaneId;
  if (relayZoomed) {
    tmux(state.route, ["resize-pane", "-Z", "-t", state.targetPaneId]);
  }
  // During normal cleanup, preserve manual layout changes. During failed
  // creation, restore the snapshot because no usable attachment was exposed.
  if ((relayZoomed || force) && state.originalActivePaneId) {
    tmux(state.route, ["select-pane", "-t", state.originalActivePaneId]);
    if (state.originalZoomed && !windowIsZoomed(state.route, state.sourceWindow)) {
      tmux(state.route, ["resize-pane", "-Z", "-t", state.originalActivePaneId]);
    }
  }
}

function cleanupAttachment(attachmentId: string): void {
  const attachment = attachments.get(attachmentId);
  if (!attachment) {
    for (const [sessionId, ids] of sessionAttachmentIds) {
      if (ids.has(attachmentId)) releaseReservation(sessionId, attachmentId);
    }
    return;
  }
  attachment.closing = true;
  attachments.delete(attachmentId);
  releaseReservation(attachment.sessionId, attachmentId);
  if (attachment.startupPoll) clearInterval(attachment.startupPoll);
  if (attachment.startupTimeout) clearTimeout(attachment.startupTimeout);
  try { attachment.terminal.close(); } catch { /* already closed */ }
  try { attachment.process.kill(); } catch { /* already exited */ }

  if (attachment.multiplexer === "tmux" && attachment.windowKey && attachment.tmuxSocket) {
    const windowState = windowAttachmentStates.get(attachment.windowKey);
    if (windowState) {
      windowState.attachmentIds.delete(attachmentId);
      if (windowState.attachmentIds.size === 0) {
        windowAttachmentStates.delete(attachment.windowKey);
        restoreWindowState(windowState);
      }
    }
    run(["tmux", "-S", attachment.tmuxSocket, "kill-session", "-t", attachment.relaySession], 3_000);
  } else if (attachment.multiplexer === "zellij") {
    run(["zellij", "kill-session", attachment.relaySession], 3_000, zellijEnvironment());
  }
}

function cleanupSessionAttachments(sessionId: string): void {
  for (const attachmentId of [...(sessionAttachmentIds.get(sessionId) || [])]) {
    cleanupAttachment(attachmentId);
  }
}

function createTmuxAttachment(ws: ServerWebSocket<SocketData>, route: TmuxRoute): Attachment {
  if (!routeIsLive(route)) throw new Error("tmux pane is unavailable");
  const sourceWindow = `${route.tmuxSession}:${route.windowIndex}`;
  const windowKey = `${route.tmuxSocket}\u0000${sourceWindow}`;
  let windowState = windowAttachmentStates.get(windowKey);
  let createdWindowState = false;
  if (windowState && windowState.targetPaneId !== route.tmuxPaneId) {
    throw new Error("another pane in this tmux window is already being relayed");
  }
  if (!windowState) {
    const originalActivePaneId = activePaneId(route, sourceWindow);
    const originalZoomed = windowIsZoomed(route, sourceWindow);
    if (originalZoomed && originalActivePaneId) {
      tmux(route, ["resize-pane", "-Z", "-t", originalActivePaneId]);
    }
    windowState = {
      route,
      sourceWindow,
      targetPaneId: route.tmuxPaneId,
      originalActivePaneId,
      originalZoomed,
      attachmentIds: new Set<string>(),
    };
    windowAttachmentStates.set(windowKey, windowState);
    createdWindowState = true;
  }

  const relaySession = `wh_attach_${route.sessionId.replace(/[^a-zA-Z0-9]/g, "").slice(0, 16)}_${crypto.randomUUID().slice(0, 8)}`;
  try {
    debugTmuxStatus(route, relaySession, "before-create");
    if (route.managedRuntime) {
      const globalStatusOff = tmux(route, ["set-option", "-g", "status", "off"]);
      const globalMouseOn = tmux(route, ["set-option", "-g", "mouse", "on"]);
      if (globalStatusOff.exitCode !== 0 || globalMouseOn.exitCode !== 0) {
        throw new Error("could not configure managed tmux global options");
      }
    }
    const linked = tmux(route, ["new-session", "-d", "-t", route.tmuxSession, "-s", relaySession]);
    if (linked.exitCode !== 0) throw new Error(`could not create linked tmux session: ${output(linked, "stderr")}`);
    const statusOff = tmux(route, ["set-option", "-t", relaySession, "status", "off"]);
    const latestSize = tmux(route, ["set-option", "-t", relaySession, "window-size", "latest"]);
    const mouseOn = route.managedRuntime
      ? tmux(route, ["set-option", "-t", relaySession, "mouse", "on"])
      : undefined;
    if (
      statusOff.exitCode !== 0
      || latestSize.exitCode !== 0
      || (mouseOn !== undefined && mouseOn.exitCode !== 0)
    ) {
      throw new Error("could not configure hidden tmux relay session");
    }
    if (tmuxStatus(route, relaySession) !== "off") {
      throw new Error("hidden tmux relay status could not be disabled");
    }
    debugTmuxStatus(route, relaySession, "configured");
    const selectedWindow = tmux(route, ["select-window", "-t", `${relaySession}:${route.windowIndex}`]);
    const selectedPane = tmux(route, ["select-pane", "-t", route.tmuxPaneId]);
    let zoomExitCode = 0;
    if (createdWindowState || !windowIsZoomed(route, sourceWindow) || activePaneId(route, sourceWindow) !== route.tmuxPaneId) {
      const zoomedPane = tmux(route, ["resize-pane", "-Z", "-t", route.tmuxPaneId]);
      zoomExitCode = zoomedPane.exitCode;
    }
    if (selectedWindow.exitCode !== 0 || selectedPane.exitCode !== 0 || zoomExitCode !== 0) {
      throw new Error("registered tmux pane is unavailable");
    }

    let attachment: Attachment;
    const terminal = new Bun.Terminal({
      cols: ws.data.initialCols,
      rows: ws.data.initialRows,
      name: "xterm-256color",
      data(_terminal, data) {
        sendTerminalData(ws, data);
      },
      exit() {
        if (attachment?.closing !== true && ws.readyState === 1) ws.close(1000, "terminal detached");
      },
    });
    const terminalProcess = Bun.spawn([
      // The most recently connected/resized client controls tmux's shared
      // `window-size latest`; clients emit resize only when dimensions change.
      "tmux", "-S", route.tmuxSocket, "attach-session", "-t", relaySession,
    ], {
      terminal,
      env: { ...tmuxEnvironment(), TERM: "xterm-256color" },
    });
    if (RELAY_DEBUG) {
      setTimeout(() => debugTmuxStatus(route, relaySession, "attached"), 100);
    }
    attachment = {
      id: ws.data.attachmentId,
      sessionId: route.sessionId,
      multiplexer: "tmux",
      ws,
      terminal,
      process: terminalProcess,
      relaySession,
      tmuxSocket: route.tmuxSocket,
      windowKey,
      ready: true,
      lastActivityAt: Date.now(),
      rows: ws.data.initialRows,
      cols: ws.data.initialCols,
      closing: false,
    };
    windowState.attachmentIds.add(attachment.id);
    return attachment;
  } catch (error) {
    run(["tmux", "-S", route.tmuxSocket, "kill-session", "-t", relaySession], 3_000);
    if (createdWindowState && windowState.attachmentIds.size === 0) {
      windowAttachmentStates.delete(windowKey);
      restoreWindowState(windowState, true);
    }
    throw error;
  }
}

function sendConnectedStatus(ws: ServerWebSocket<SocketData>, route: Route): void {
  if (ws.readyState !== 1) return;
  ws.send(JSON.stringify({
    type: "status",
    session_id: ws.data.sessionId,
    state: "connected",
    terminal: "ready",
    protocol_version: PROTOCOL_VERSION,
    relay_revision: RELAY_REVISION,
    rows: ws.data.initialRows,
    cols: ws.data.initialCols,
    transport: "direct-interactive-websocket",
    source_multiplexer: route.multiplexer,
    attachment_id: ws.data.attachmentId,
  }));
}

function createZellijAttachment(ws: ServerWebSocket<SocketData>, route: ZellijRoute): Attachment {
  if (!routeIsLive(route)) throw new Error("Zellij pane is unavailable");
  const existingClientIds = new Set(zellijClients(route).keys());
  const relaySession = `wh_zattach_${route.sessionId.replace(/[^a-zA-Z0-9]/g, "").slice(0, 16)}_${crypto.randomUUID().slice(0, 8)}`;
  let bootstrapWritten = false;
  let attachment: Attachment;
  const terminal = new Bun.Terminal({
    cols: ws.data.initialCols,
    rows: ws.data.initialRows,
    name: "xterm-256color",
    data(_terminal, data) {
      const current = attachments.get(ws.data.attachmentId);
      if (!bootstrapWritten) {
        bootstrapWritten = true;
        setTimeout(() => {
          const active = attachments.get(ws.data.attachmentId);
          if (!active || active.ready) return;
          const command = [
            "zellij action switch-session",
            shellQuote(route.zellijSessionName),
            "--pane-id",
            shellQuote(route.zellijPaneId),
          ].join(" ");
          active.terminal.write(`${command}\r`);
        }, 50).unref();
      }
      // Do not leak bootstrap-shell rendering. The target Zellij client emits
      // a fresh redraw after the client-specific switch is confirmed below.
      if (current?.ready) sendTerminalData(ws, data);
    },
    exit() {
      if (attachment?.closing !== true && ws.readyState === 1) ws.close(1000, "terminal detached");
    },
  });
  const terminalProcess = Bun.spawn(["zellij", "--session", relaySession], {
    terminal,
    env: { ...zellijEnvironment(), TERM: "xterm-256color" },
  });
  attachment = {
    id: ws.data.attachmentId,
    sessionId: route.sessionId,
    multiplexer: "zellij",
    ws,
    terminal,
    process: terminalProcess,
    relaySession,
    ready: false,
    lastActivityAt: Date.now(),
    rows: ws.data.initialRows,
    cols: ws.data.initialCols,
    closing: false,
  };
  attachment.startupPoll = setInterval(() => {
    if (attachments.get(attachment.id) !== attachment) return;
    const clients = zellijClients(route);
    const targetClient = [...clients].some(([clientId, paneId]) => (
      !existingClientIds.has(clientId) && paneId === route.zellijPaneId
    ));
    if (!targetClient) return;
    if (attachment.startupPoll) clearInterval(attachment.startupPoll);
    attachment.startupPoll = undefined;
    if (attachment.startupTimeout) clearTimeout(attachment.startupTimeout);
    attachment.startupTimeout = undefined;
    attachment.ready = true;
    sendConnectedStatus(ws, route);
    // Force a target redraw after bootstrap output was suppressed. This only
    // resizes the disposable client PTY and does not move existing clients.
    const redrawCols = attachment.cols === 1 ? 2 : attachment.cols - 1;
    attachment.terminal.resize(redrawCols, attachment.rows);
    attachment.terminal.resize(attachment.cols, attachment.rows);
  }, 50);
  attachment.startupPoll.unref();
  attachment.startupTimeout = setTimeout(() => {
    if (attachments.get(attachment.id) !== attachment || attachment.ready) return;
    if (ws.readyState === 1) {
      ws.send(JSON.stringify({ type: "error", detail: "could not focus the registered Zellij pane" }));
      ws.close(1011, "Zellij attach failed");
    }
    cleanupAttachment(attachment.id);
  }, 5_000);
  attachment.startupTimeout.unref();
  return attachment;
}

function createAttachment(ws: ServerWebSocket<SocketData>, route: Route): Attachment {
  return route.multiplexer === "tmux"
    ? createTmuxAttachment(ws, route)
    : createZellijAttachment(ws, route);
}

const web = Bun.serve<SocketData>({
  hostname: LOCAL_HOST,
  port: LOCAL_PORT,
  fetch(request, server) {
    const url = new URL(request.url);
    if (url.pathname === "/healthz") {
      return Response.json({
        status: "healthy",
        protocol_version: PROTOCOL_VERSION,
        relay_revision: RELAY_REVISION,
        session_count: routes.size,
        sessions_by_multiplexer: {
          tmux: [...routes.values()].filter((route) => route.multiplexer === "tmux").length,
          zellij: [...routes.values()].filter((route) => route.multiplexer === "zellij").length,
        },
        attachment_count: attachments.size,
        max_attachments_per_session: MAX_ATTACHMENTS_PER_SESSION,
        websocket_backpressure_limit: WS_BACKPRESSURE_LIMIT,
        attachment_backpressure_drops_total: attachmentBackpressureDropsTotal,
        attachment_close_reasons: Object.fromEntries(attachmentCloseReasons),
        attachment_evictions_total: attachmentEvictionsTotal,
      });
    }
    const match = url.pathname.match(/^\/v1\/sessions\/([^/]+)\/attach$/);
    if (!match) return new Response("not found", { status: 404 });
    const sessionId = decodeURIComponent(match[1]);
    const route = routes.get(sessionId);
    if (!route || !routeIsLive(route)) return new Response("session is not attachable", { status: 409 });
    const attachmentId = reserveAttachment(sessionId);
    const initialRows = terminalDimension(url.searchParams.get("rows"), 24);
    const initialCols = terminalDimension(url.searchParams.get("cols"), 80);
    if (!attachmentId) {
      if (!server.upgrade(request, {
        data: { sessionId, attachmentId: crypto.randomUUID(), initialRows, initialCols, rejected: true },
      })) return new Response("upgrade required", { status: 426 });
      return;
    }
    if (!server.upgrade(request, { data: { sessionId, attachmentId, initialRows, initialCols } })) {
      releaseReservation(sessionId, attachmentId);
      return new Response("upgrade required", { status: 426 });
    }
  },
  websocket: {
    idleTimeout: 0,
    backpressureLimit: WS_BACKPRESSURE_LIMIT,
    closeOnBackpressureLimit: false,
    open(ws) {
      if (replacedReservations.delete(ws.data.attachmentId)) {
        ws.send(JSON.stringify({
          type: "status",
          state: "replaced",
          reason: "attachment capacity reclaimed by a newer client",
        }));
        return ws.close(4410, "replaced by newer attachment");
      }
      if (ws.data.rejected) {
        ws.send(JSON.stringify({ type: "error", code: "attachment_limit", limit: MAX_ATTACHMENTS_PER_SESSION }));
        return ws.close(4429, "attachment limit reached");
      }
      const route = routes.get(ws.data.sessionId);
      if (!route) {
        releaseReservation(ws.data.sessionId, ws.data.attachmentId);
        return ws.close(4404, "session not found");
      }
      try {
        const attachment = createAttachment(ws, route);
        attachments.set(ws.data.attachmentId, attachment);
        if (attachment.ready) sendConnectedStatus(ws, route);
      } catch (error) {
        releaseReservation(ws.data.sessionId, ws.data.attachmentId);
        ws.send(JSON.stringify({ type: "error", detail: error instanceof Error ? error.message : String(error) }));
        ws.close(1011, "terminal attach failed");
      }
    },
    message(ws, incoming) {
      const attachment = attachments.get(ws.data.attachmentId);
      if (!attachment || !attachment.ready) return;
      // Bun normally preserves WebSocket text/binary framing, but macOS Bun
      // may surface Python-websockets text frames as Buffer. Parse a valid
      // control envelope from either representation before treating bytes as
      // raw terminal input.
      try {
        const text = typeof incoming === "string" ? incoming : new TextDecoder().decode(incoming);
        const message = JSON.parse(text) as { type?: string; data?: string; rows?: number; cols?: number };
        if (message.type === "input" && typeof message.data === "string") {
          attachment.lastActivityAt = Date.now();
          attachment.terminal.write(message.data);
          return;
        }
        if (message.type === "resize") {
          const rows = terminalDimension(message.rows, 24);
          const cols = terminalDimension(message.cols, 80);
          if (rows !== attachment.rows || cols !== attachment.cols) {
            attachment.lastActivityAt = Date.now();
            attachment.rows = rows;
            attachment.cols = cols;
            resizeAttachmentTerminal(attachment, cols, rows);
          }
          ws.send(JSON.stringify({ type: "resize-applied", rows, cols }));
          return;
        }
      } catch {
        // Not a protocol control frame; forward it unchanged below.
      }
      attachment.lastActivityAt = Date.now();
      attachment.terminal.write(incoming);
    },
    close(ws, code, reason) {
      countAttachmentClose(code, reason);
      cleanupAttachment(ws.data.attachmentId);
    },
  },
});

function sameSocketPath(left: string, right: string): boolean {
  if (left === right) return true;
  try {
    return realpathSync(left) === realpathSync(right);
  } catch {
    return false;
  }
}

async function socketIsLive(path: string): Promise<boolean> {
  return await new Promise((resolve) => {
    const socket = createConnection(path);
    socket.once("connect", () => { socket.destroy(); resolve(true); });
    socket.once("error", () => resolve(false));
    socket.setTimeout(300, () => { socket.destroy(); resolve(false); });
  });
}

async function startControlSocket(): Promise<void> {
  mkdirSync(dirname(SOCKET_PATH), { recursive: true, mode: 0o700 });
  if (existsSync(SOCKET_PATH)) {
    if (await socketIsLive(SOCKET_PATH)) {
      web.stop(true);
      process.exit(0);
    }
    rmSync(SOCKET_PATH, { force: true });
  }
  const server = createUnixServer((socket) => {
    let buffer = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk) => {
      buffer += chunk;
      if (buffer.length > 64 * 1024) return socket.destroy();
      const newline = buffer.indexOf("\n");
      if (newline < 0) return;
      try {
        const request = JSON.parse(buffer.slice(0, newline)) as Record<string, unknown>;
        if (request.action === "locate") {
          const multiplexer = String(request.multiplexer || "");
          if (multiplexer === "tmux") {
            const tmuxSocket = String(request.tmux_socket || "");
            const tmuxPaneId = String(request.tmux_pane_id || "");
            if (!tmuxSocket || !/^%\d+$/.test(tmuxPaneId)) {
              throw new Error("complete tmux locator is required");
            }
            const found = [...routes.values()].find((route) => (
              route.multiplexer === "tmux" &&
              route.tmuxPaneId === tmuxPaneId &&
              sameSocketPath(route.tmuxSocket, tmuxSocket) &&
              routeIsLive(route)
            ));
            if (!found) throw new Error("local Pi session is unavailable");
            socket.end(`${JSON.stringify({ ok: true, session_id: found.sessionId })}\n`);
            return;
          }
          const zellijSessionName = String(request.zellij_session_name || "");
          const rawPaneId = String(request.zellij_pane_id || "");
          const zellijPaneId = /^\d+$/.test(rawPaneId) ? `terminal_${rawPaneId}` : rawPaneId;
          if (multiplexer !== "zellij" || !zellijSessionName || !/^terminal_\d+$/.test(zellijPaneId)) {
            throw new Error("complete Zellij locator is required");
          }
          const current = [...routes.values()].find((route) => (
            route.multiplexer === "zellij" &&
            route.zellijSessionName === zellijSessionName &&
            route.zellijPaneId === zellijPaneId &&
            routeIsLive(route)
          ));
          if (!current) throw new Error("local Pi session is unavailable");
          socket.end(`${JSON.stringify({ ok: true, session_id: current.sessionId })}\n`);
          return;
        }
        const sessionId = String(request.session_id || "");
        const incarnation = String(request.incarnation || "");
        if (!sessionId) throw new Error("session_id is required");
        if (request.action === "register") {
          if (!incarnation) throw new Error("incarnation is required");
          const requestedMultiplexer = String(request.multiplexer || "");
          const multiplexer = requestedMultiplexer || (
            request.zellij_session_name || request.zellij_pane_id ? "zellij" : "tmux"
          );
          const previous = routes.get(sessionId);
          let route: Route;
          if (multiplexer === "zellij") {
            const zellijSessionName = String(request.zellij_session_name || "");
            const rawPaneId = String(request.zellij_pane_id || "");
            const zellijPaneId = /^\d+$/.test(rawPaneId) ? `terminal_${rawPaneId}` : rawPaneId;
            if (!zellijSessionName || !/^terminal_\d+$/.test(zellijPaneId)) {
              throw new Error("complete Zellij locator is required");
            }
            route = {
              multiplexer: "zellij",
              sessionId,
              incarnation,
              zellijSessionName,
              zellijPaneId,
              updatedAt: Date.now(),
            };
            if (!routeIsLive(route)) throw new Error("Zellij locator is not live");
          } else if (multiplexer === "tmux") {
            const tmuxSocket = String(request.tmux_socket || "");
            const tmuxPaneId = String(request.tmux_pane_id || "");
            const managedRuntime = request.managed_runtime === true;
            let location: TmuxLocation | null = null;
            if (tmuxSocket && tmuxPaneId) {
              location = locateTmuxPane(
                tmuxSocket,
                tmuxPaneId,
                managedRuntime,
                previous?.multiplexer === "tmux" ? previous : undefined,
              );
              if (!location) throw new Error("registered tmux pane is unavailable");
            } else {
              const tmuxSession = String(request.tmux_session || "");
              const windowIndex = String(request.window_index || "");
              const paneIndex = String(request.pane_index || "");
              if (tmuxSocket && tmuxSession && windowIndex && paneIndex) {
                const located = run([
                  "tmux", "-S", tmuxSocket, "display-message", "-p", "-t",
                  `${tmuxSession}:${windowIndex}.${paneIndex}`, "#{pane_id}",
                ], 3_000, tmuxEnvironment());
                const resolvedPaneId = output(located);
                if (located.exitCode === 0 && /^%\d+$/.test(resolvedPaneId)) {
                  location = { tmuxSession, windowIndex, paneIndex, tmuxPaneId: resolvedPaneId };
                }
              }
            }
            if (!tmuxSocket || !location) throw new Error("complete tmux locator is required");
            route = {
              multiplexer: "tmux",
              sessionId,
              incarnation,
              tmuxSocket,
              ...location,
              managedRuntime,
              updatedAt: Date.now(),
            };
            if (!routeIsLive(route)) throw new Error("tmux locator is not live");
          } else {
            throw new Error("unsupported terminal multiplexer");
          }
          if (previous && (
            previous.incarnation !== incarnation || routeIdentity(previous) !== routeIdentity(route)
          )) {
            cleanupSessionAttachments(sessionId);
          }
          routes.set(sessionId, route);
          socket.end(`${JSON.stringify({
            ok: true,
            attachable: published,
            host: tailnetHost,
            port: PUBLISH ? PUBLIC_PORT : LOCAL_PORT,
            protocol_version: PROTOCOL_VERSION,
            websocket_url: published ? attachUrl(sessionId) : "",
          })}\n`);
        } else if (request.action === "unregister") {
          if (!incarnation) throw new Error("incarnation is required");
          const current = routes.get(sessionId);
          if (current?.incarnation === incarnation) {
            cleanupSessionAttachments(sessionId);
            routes.delete(sessionId);
          }
          socket.end(`${JSON.stringify({ ok: true })}\n`);
        } else if (request.action === "describe") {
          const current = routes.get(sessionId);
          if (!current || !routeIsLive(current)) throw new Error("local Pi session is unavailable");
          socket.end(`${JSON.stringify(current.multiplexer === "tmux" ? {
            ok: true,
            multiplexer: "tmux",
            tmux_socket: current.tmuxSocket,
            tmux_session: current.tmuxSession,
            window_index: current.windowIndex,
            pane_index: current.paneIndex,
            tmux_pane_id: current.tmuxPaneId,
            managed_runtime: current.managedRuntime,
          } : {
            ok: true,
            multiplexer: "zellij",
            zellij_session_name: current.zellijSessionName,
            zellij_pane_id: current.zellijPaneId,
          })}\n`);
        } else {
          throw new Error("unknown action");
        }
      } catch (error) {
        socket.end(`${JSON.stringify({ ok: false, error: error instanceof Error ? error.message : String(error) })}\n`);
      }
    });
  });
  server.on("error", (error: NodeJS.ErrnoException) => {
    if (error.code === "EADDRINUSE") process.exit(0);
    console.error(`[pi-host-relay] control socket failed: ${error.message}`);
    process.exit(1);
  });
  server.listen(SOCKET_PATH, () => chmodSync(SOCKET_PATH, 0o600));
}

published = publishRelay();
await startControlSocket();
setInterval(() => {
  if (PUBLISH && !published) published = publishRelay();
  for (const [sessionId, route] of routes) {
    if (!routeIsLive(route)) {
      cleanupSessionAttachments(sessionId);
      routes.delete(sessionId);
    }
  }
}, REAPER_INTERVAL_MS).unref();

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.on(signal, () => {
    for (const attachmentId of [...attachments.keys()]) cleanupAttachment(attachmentId);
    rmSync(SOCKET_PATH, { force: true });
    unpublishRelay();
    web.stop(true);
    process.exit(0);
  });
}
