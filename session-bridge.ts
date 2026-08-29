import type { ExtensionAPI, ExtensionContext } from "@earendil-works/pi-coding-agent";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { join } from "node:path";
import { hostname as systemHostname } from "node:os";
import { fileURLToPath } from "node:url";
import { createConnection } from "node:net";
import { getOrchestratorUrl } from "./api.ts";
import { harnessAgent } from "./config.ts";

type BridgeState = "working" | "idle" | "stopped";

type BridgeCommand = {
  id: string;
  kind: "prompt" | "configure" | "interrupt";
  message: string;
  deliver_as: "steer" | "followUp";
  payload?: {
    provider?: string;
    model?: string;
    thinking_level?: "off" | "minimal" | "low" | "medium" | "high" | "xhigh" | "max";
  };
};

type BridgeEvent = {
  id: string;
  event_type: string;
  payload: Record<string, unknown>;
  created_at: number;
};

type TerminalLocator =
  | { multiplexer: "tmux"; tmuxSocket: string; tmuxPaneId: string; managedRuntime: boolean }
  | { multiplexer: "zellij"; zellijSessionName: string; zellijPaneId: string };

type TerminalInfo = {
  attachable: boolean;
  host: string;
  port: number;
  protocolVersion: number;
};

type StreamMessage = {
  role?: string;
  content?: unknown;
  timestamp?: number;
  toolCallId?: string;
  toolName?: string;
  isError?: boolean;
  provider?: string;
  model?: string;
  stopReason?: string;
  errorMessage?: string;
};

const RETRY_MS = 2_000;
const HEARTBEAT_MS = 30_000;
const DELTA_FLUSH_MS = 200;
const MAX_OUTBOX_EVENTS = 2_000;
const adjacentHostRelay = fileURLToPath(new URL("./host-relay.ts", import.meta.url));
const configuredHostRelay = process.env.WH_PI_HOST_RELAY_SCRIPT || "";
const homeHostRelays = [".pi", ".omp"].map((dir) => join(process.env.HOME || "", dir, "agent/extensions/pi-worker-harness/host-relay.ts"));
const HOST_RELAY_SCRIPT = [configuredHostRelay, adjacentHostRelay, ...homeHostRelays].find((path) => path && existsSync(path)) || adjacentHostRelay;

export function registerSessionBridge(pi: ExtensionAPI): void {
  // Delegated children use the worker-local UDS bridge instead. Headless Pi
  // processes (subagents, planners, one-shot automation) are not operator
  // sessions and would otherwise clutter discovery with short-lived copies.
  if (process.env.WH_PI_SESSION_ID || !process.stdin.isTTY || !process.stdout.isTTY) return;

  let controller: AbortController | null = null;
  let heartbeat: ReturnType<typeof setInterval> | null = null;
  let outboxTimer: ReturnType<typeof setInterval> | null = null;
  let deltaTimer: ReturnType<typeof setInterval> | null = null;
  let terminalTimer: ReturnType<typeof setInterval> | null = null;
  let sessionId = "";
  let incarnation = "";
  let bridgeContext: ExtensionContext | null = null;
  let flushing = false;
  let registrationLost = false;
  let terminalLocator: TerminalLocator | null = null;
  let terminalInfo: TerminalInfo = { attachable: false, host: "", port: 0, protocolVersion: 0 };
  let terminalRefresh: Promise<TerminalInfo> | null = null;
  const eventOutbox: BridgeEvent[] = [];
  const pendingDeltas = new Map<string, { messageId: string; contentIndex: number; delta: string }>();

  function endpoint(path: string): string {
    return `${getOrchestratorUrl().replace(/\/+$/, "")}${path}`;
  }

  async function request(
    path: string,
    init: RequestInit = {},
    signal: AbortSignal | undefined = controller?.signal,
  ): Promise<Response> {
    const response = await fetch(endpoint(path), {
      ...init,
      headers: { "content-type": "application/json", ...(init.headers ?? {}) },
      signal,
    });
    if (!response.ok) {
      const error = new Error(`Pi bridge HTTP ${response.status}: ${await response.text()}`);
      (error as Error & { status?: number }).status = response.status;
      throw error;
    }
    return response;
  }

  function relaySocketPath(): string {
    const runtime = process.env.XDG_RUNTIME_DIR || `/tmp/worker-harness-${process.getuid?.() ?? "user"}`;
    return process.env.WH_PI_HOST_RELAY_SOCKET || `${runtime}/worker-harness/pi-host-relay.sock`;
  }

  function discoverTerminalLocator(): TerminalLocator | null {
    const tmuxSocket = process.env.TMUX?.split(",", 1)[0] || "";
    const tmuxPaneId = process.env.TMUX_PANE || "";
    // Prefer the immediate tmux parent when tmux itself runs inside Zellij.
    // The persistent relay resolves tmux's mutable indices from its stable pane.
    if (tmuxSocket && tmuxPaneId) {
      return {
        multiplexer: "tmux",
        tmuxSocket,
        tmuxPaneId,
        managedRuntime: process.env.WH_MANAGED_PI === "1",
      };
    }

    const zellijSessionName = process.env.ZELLIJ_SESSION_NAME || "";
    const rawZellijPaneId = process.env.ZELLIJ_PANE_ID || "";
    const zellijPaneId = /^\d+$/.test(rawZellijPaneId)
      ? `terminal_${rawZellijPaneId}`
      : rawZellijPaneId;
    return zellijSessionName && /^terminal_\d+$/.test(zellijPaneId)
      ? { multiplexer: "zellij", zellijSessionName, zellijPaneId }
      : null;
  }

  async function relayRequest(payload: Record<string, unknown>): Promise<Record<string, unknown>> {
    return await new Promise((resolve, reject) => {
      const socket = createConnection(relaySocketPath());
      let buffer = "";
      const finish = (error?: Error) => {
        socket.destroy();
        if (error) reject(error);
      };
      socket.setEncoding("utf8");
      socket.setTimeout(1_500, () => finish(new Error("host relay timed out")));
      socket.once("error", (error) => finish(error));
      socket.once("connect", () => socket.write(`${JSON.stringify(payload)}\n`));
      socket.on("data", (chunk) => {
        buffer += chunk;
        if (buffer.length > 64 * 1024) return finish(new Error("host relay response is too large"));
        const newline = buffer.indexOf("\n");
        if (newline < 0) return;
        try {
          const response = JSON.parse(buffer.slice(0, newline)) as Record<string, unknown>;
          socket.destroy();
          if (!response.ok) reject(new Error(String(response.error || "host relay rejected registration")));
          else resolve(response);
        } catch (error) {
          finish(error instanceof Error ? error : new Error(String(error)));
        }
      });
    });
  }

  function startHostRelay(): void {
    const executable = process.execPath.toLowerCase().includes("bun") ? process.execPath : "bun";
    const relayEnvironment = { ...process.env };
    // The relay is host-scoped and outlives the Pi pane that bootstraps it.
    // Never let an outer/managed tmux client context trigger tmux's nested
    // attachment guard; every relay operation uses an explicit route socket.
    for (const key of ["TMUX", "TMUX_PANE", "ZELLIJ", "ZELLIJ_SESSION_NAME", "ZELLIJ_PANE_ID"]) {
      delete relayEnvironment[key];
    }
    const child = spawn(executable, [HOST_RELAY_SCRIPT], {
      detached: true,
      stdio: "ignore",
      env: relayEnvironment,
    });
    // A fresh host may have Pi/Node but not Bun yet. Missing optional relay
    // prerequisites must make terminal attach unavailable, never crash Pi.
    child.once("error", (error) => {
      console.warn(`[pi-session-bridge] could not start host terminal relay: ${String(error)}`);
    });
    child.unref();
  }

  async function refreshTerminalRoute(): Promise<TerminalInfo> {
    if (!terminalLocator || !sessionId || !incarnation) {
      return { attachable: false, host: "", port: 0, protocolVersion: 0 };
    }
    const payload = terminalLocator.multiplexer === "tmux" ? {
      action: "register",
      session_id: sessionId,
      incarnation,
      multiplexer: "tmux",
      tmux_socket: terminalLocator.tmuxSocket,
      tmux_pane_id: terminalLocator.tmuxPaneId,
      managed_runtime: terminalLocator.managedRuntime,
    } : {
      action: "register",
      session_id: sessionId,
      incarnation,
      multiplexer: "zellij",
      zellij_session_name: terminalLocator.zellijSessionName,
      zellij_pane_id: terminalLocator.zellijPaneId,
    };
    let response: Record<string, unknown>;
    try {
      response = await relayRequest(payload);
    } catch {
      startHostRelay();
      let lastError: unknown;
      for (let attempt = 0; attempt < 20; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 100));
        try {
          response = await relayRequest(payload);
          return {
            attachable: Boolean(response.attachable),
            host: String(response.host || ""),
            port: Number(response.port) || 0,
            protocolVersion: Number(response.protocol_version) || 0,
          };
        } catch (error) {
          lastError = error;
        }
      }
      console.warn(`[pi-session-bridge] host terminal relay unavailable: ${String(lastError)}`);
      return { attachable: false, host: "", port: 0, protocolVersion: 0 };
    }
    return {
      attachable: Boolean(response.attachable),
      host: String(response.host || ""),
      port: Number(response.port) || 0,
      protocolVersion: Number(response.protocol_version) || 0,
    };
  }

  async function ensureTerminalRoute(): Promise<TerminalInfo> {
    if (!terminalRefresh) terminalRefresh = refreshTerminalRoute().finally(() => { terminalRefresh = null; });
    terminalInfo = await terminalRefresh;
    return terminalInfo;
  }

  async function unregisterTerminalRoute(targetSessionId = sessionId, targetIncarnation = incarnation): Promise<void> {
    if (!targetSessionId || !targetIncarnation || !terminalLocator) return;
    await relayRequest({
      action: "unregister",
      session_id: targetSessionId,
      incarnation: targetIncarnation,
    }).catch(() => undefined);
  }

  function hasPendingMessages(): boolean {
    return bridgeContext?.hasPendingMessages() ?? false;
  }

  async function report(
    state: BridgeState | undefined,
    eventType?: string,
    payload: Record<string, unknown> = {},
  ): Promise<void> {
    if (!sessionId || !incarnation || controller?.signal.aborted) return;
    await request(`/api/v1/pi/bridge/${encodeURIComponent(sessionId)}/events`, {
      method: "POST",
      body: JSON.stringify({
        incarnation,
        ...(state ? { state } : {}),
        has_pending_messages: hasPendingMessages(),
        events: eventType
          ? [{ id: crypto.randomUUID(), event_type: eventType, payload, created_at: Math.floor(Date.now() / 1000) }]
          : [],
      }),
    });
  }

  function queueEvent(eventType: string, payload: Record<string, unknown>, essential = false): void {
    if (!sessionId || !incarnation || controller?.signal.aborted) return;
    if (eventOutbox.length >= MAX_OUTBOX_EVENTS) {
      if (!essential) return;
      const disposable = eventOutbox.findIndex((event) => event.event_type === "message-delta");
      if (disposable >= 0) eventOutbox.splice(disposable, 1);
      else eventOutbox.shift();
    }
    eventOutbox.push({
      id: crypto.randomUUID(),
      event_type: eventType,
      payload,
      created_at: Math.floor(Date.now() / 1000),
    });
    void flushEventOutbox();
  }

  async function flushEventOutbox(): Promise<void> {
    if (flushing || eventOutbox.length === 0 || controller?.signal.aborted) return;
    flushing = true;
    try {
      while (eventOutbox.length > 0 && !controller?.signal.aborted) {
        const batch = eventOutbox.slice(0, 100);
        await request(`/api/v1/pi/bridge/${encodeURIComponent(sessionId)}/events`, {
          method: "POST",
          body: JSON.stringify({
            incarnation,
            has_pending_messages: hasPendingMessages(),
            events: batch,
          }),
        });
        eventOutbox.splice(0, batch.length);
      }
    } catch (error) {
      // A 404 means the orchestrator lost its session registry (commonly after
      // a restart with a fresh database). Keep the outbox intact and let the
      // command loop return to registration before retrying it.
      if ((error as Error & { status?: number }).status === 404) registrationLost = true;
      // Stable event IDs make every other retry safe as well.
    } finally {
      flushing = false;
    }
  }

  function messageId(message: StreamMessage): string {
    const discriminator = message.toolCallId ?? "message";
    return `${message.role ?? "unknown"}:${message.timestamp ?? Date.now()}:${discriminator}`;
  }

  function sanitizeMessage(message: StreamMessage): Record<string, unknown> {
    const blocks = typeof message.content === "string"
      ? [{ type: "text", text: message.content }]
      : Array.isArray(message.content)
        ? message.content.flatMap((raw) => {
            if (!raw || typeof raw !== "object") return [];
            const block = raw as Record<string, unknown>;
            if (block.type === "text") return [{ type: "text", text: String(block.text ?? "") }];
            if (block.type === "image") return [{ type: "image", mimeType: String(block.mimeType ?? "") }];
            if (block.type === "toolCall") return [{
              type: "toolCall",
              id: String(block.id ?? ""),
              name: String(block.name ?? ""),
              arguments: block.arguments ?? {},
            }];
            // Deliberately do not export hidden thinking blocks.
            return [];
          })
        : [];
    return {
      role: message.role ?? "unknown",
      timestamp: message.timestamp ?? Date.now(),
      content: blocks,
      ...(message.toolCallId ? { toolCallId: message.toolCallId } : {}),
      ...(message.toolName ? { toolName: message.toolName } : {}),
      ...(message.isError !== undefined ? { isError: message.isError } : {}),
      ...(message.provider ? { provider: message.provider } : {}),
      ...(message.model ? { model: message.model } : {}),
      ...(message.stopReason ? { stopReason: message.stopReason } : {}),
      ...(message.errorMessage ? { errorMessage: message.errorMessage } : {}),
    };
  }

  function recentExchangeEvents(ctx: ExtensionContext): BridgeEvent[] {
    const branch = ctx.sessionManager.getBranch();
    let assistantIndex = -1;
    for (let index = branch.length - 1; index >= 0; index -= 1) {
      const entry = branch[index];
      if (entry.type === "message" && entry.message.role === "assistant") {
        assistantIndex = index;
        break;
      }
    }
    if (assistantIndex < 0) return [];

    let userIndex = -1;
    for (let index = assistantIndex - 1; index >= 0; index -= 1) {
      const entry = branch[index];
      if (entry.type === "message" && entry.message.role === "user") {
        userIndex = index;
        break;
      }
    }
    if (userIndex < 0) return [];

    return [branch[userIndex], branch[assistantIndex]].flatMap((entry) => {
      if (entry.type !== "message") return [];
      const message = entry.message as StreamMessage;
      const id = messageId(message);
      const rawTimestamp = message.timestamp ?? Date.parse(entry.timestamp);
      const createdAt = Math.floor((Number.isFinite(rawTimestamp) ? rawTimestamp : Date.now()) / 1000);
      const prefix = `history:${sessionId}:${entry.id}`;
      return [
        {
          id: `${prefix}:start`,
          event_type: "message-start",
          payload: { message_id: id, role: message.role ?? "unknown", timestamp: message.timestamp ?? rawTimestamp },
          created_at: createdAt,
        },
        {
          id: `${prefix}:end`,
          event_type: "message-end",
          payload: { message_id: id, message: sanitizeMessage(message) },
          created_at: createdAt,
        },
      ];
    });
  }

  function flushPendingDeltas(targetMessageId?: string): void {
    for (const [key, pending] of pendingDeltas) {
      if (targetMessageId && pending.messageId !== targetMessageId) continue;
      pendingDeltas.delete(key);
      if (pending.delta) queueEvent("message-delta", {
        message_id: pending.messageId,
        content_index: pending.contentIndex,
        delta: pending.delta,
      });
    }
  }

  function reportSettings(ctx: ExtensionContext | null = bridgeContext): void {
    if (!ctx) return;
    const model = ctx.model;
    queueEvent("session-settings", {
      provider: model?.provider ?? "",
      model: model?.id ?? "",
      thinking_level: pi.getThinkingLevel(),
      available_models: ctx.modelRegistry.getAvailable().map((available) => ({
        provider: available.provider,
        id: available.id,
        name: available.name,
      })),
    }, true);
  }

  async function applyConfiguration(command: BridgeCommand): Promise<void> {
    const requested = command.payload ?? {};
    try {
      if (requested.provider && requested.model) {
        if (!bridgeContext) throw new Error("Pi bridge context is not ready");
        const model = bridgeContext.modelRegistry.find(requested.provider, requested.model);
        if (!model) throw new Error(`model not available: ${requested.provider}/${requested.model}`);
        if (!await pi.setModel(model)) throw new Error(`model has no configured authentication: ${requested.provider}/${requested.model}`);
      }
      if (requested.thinking_level) pi.setThinkingLevel(requested.thinking_level);
      reportSettings();
    } catch (error) {
      queueEvent("control-error", {
        command_id: command.id,
        operation: "configure",
        detail: error instanceof Error ? error.message : String(error),
      }, true);
    }
  }

  async function acknowledge(commandId: string, signal: AbortSignal): Promise<void> {
    await request(
      `/api/v1/pi/bridge/${encodeURIComponent(sessionId)}/commands/${encodeURIComponent(commandId)}:ack`,
      { method: "POST", body: JSON.stringify({ incarnation }) },
      signal,
    );
  }

  async function pollCommands(activeController: AbortController): Promise<"stopped" | "reregister"> {
    while (!activeController.signal.aborted) {
      if (registrationLost) return "reregister";
      try {
        const query = new URLSearchParams({ incarnation, wait_seconds: "20" });
        const response = await request(
          `/api/v1/pi/bridge/${encodeURIComponent(sessionId)}/commands?${query.toString()}`,
          {},
          activeController.signal,
        );
        const commands = (await response.json()) as BridgeCommand[];
        for (const command of commands) {
          if (command.kind === "prompt") {
            await pi.sendUserMessage(command.message, { deliverAs: command.deliver_as });
            queueEvent("pending-state", {
              command_id: command.id,
              has_pending_messages: hasPendingMessages(),
            }, true);
          } else if (command.kind === "configure") {
            await applyConfiguration(command);
          } else if (command.kind === "interrupt") {
            const hadPendingMessages = hasPendingMessages();
            if (!bridgeContext) throw new Error("Pi bridge context is not ready");
            bridgeContext.abort();
            queueEvent("interrupt-applied", {
              command_id: command.id,
              had_pending_messages: hadPendingMessages,
            }, true);
          }
          await acknowledge(command.id, activeController.signal);
        }
      } catch (error) {
        if (activeController.signal.aborted) return "stopped";
        const status = (error as Error & { status?: number }).status;
        if (status === 404) return "reregister";
        if (status === 409) return "stopped";
        await new Promise((resolve) => setTimeout(resolve, RETRY_MS));
      }
    }
    return "stopped";
  }

  async function connect(ctx: ExtensionContext, activeController: AbortController): Promise<void> {
    while (!activeController.signal.aborted) {
      try {
        const hostname = process.env.HOSTNAME ?? process.env.COMPUTERNAME ?? systemHostname();
        const terminal = await ensureTerminalRoute();
        const response = await request("/api/v1/pi/bridge/register", {
          method: "POST",
          body: JSON.stringify({
            session_id: sessionId,
            incarnation,
            cwd: ctx.cwd,
            name: ctx.sessionManager.getSessionName() ?? "",
            host: hostname,
            agent: harnessAgent(),
            terminal_attachable: terminal.attachable,
            terminal_host: terminal.host,
            terminal_port: terminal.port,
            terminal_protocol_version: terminal.protocolVersion,
            has_pending_messages: ctx.hasPendingMessages(),
            initial_events: recentExchangeEvents(ctx),
          }),
        }, activeController.signal);
        await response.body?.cancel();
        registrationLost = false;
        reportSettings(ctx);
        if (!heartbeat) {
          heartbeat = setInterval(() => {
            void report(undefined).catch((error) => {
              if ((error as Error & { status?: number }).status === 404) registrationLost = true;
            });
          }, HEARTBEAT_MS);
        }
        if (!outboxTimer) outboxTimer = setInterval(() => void flushEventOutbox(), RETRY_MS);
        if (!deltaTimer) deltaTimer = setInterval(() => flushPendingDeltas(), DELTA_FLUSH_MS);
        if (!terminalTimer && terminalLocator) {
          terminalTimer = setInterval(() => {
            const before = JSON.stringify(terminalInfo);
            void ensureTerminalRoute().then((next) => {
              if (JSON.stringify(next) !== before) registrationLost = true;
            });
          }, HEARTBEAT_MS);
        }
        if (await pollCommands(activeController) === "reregister") continue;
        return;
      } catch (error) {
        if (activeController.signal.aborted) return;
        console.warn(`[pi-session-bridge] registration failed: ${String(error)}`);
        await new Promise((resolve) => setTimeout(resolve, RETRY_MS));
      }
    }
  }

  pi.on("session_start", async (_event, ctx) => {
    await unregisterTerminalRoute();
    controller?.abort();
    if (heartbeat) clearInterval(heartbeat);
    if (outboxTimer) clearInterval(outboxTimer);
    if (deltaTimer) clearInterval(deltaTimer);
    if (terminalTimer) clearInterval(terminalTimer);
    eventOutbox.length = 0;
    pendingDeltas.clear();
    flushing = false;
    registrationLost = false;
    terminalTimer = null;
    terminalLocator = null;
    terminalInfo = { attachable: false, host: "", port: 0, protocolVersion: 0 };
    terminalRefresh = null;
    bridgeContext = null;
    sessionId = "";
    incarnation = "";
    const sessionName = ctx.sessionManager.getSessionName() ?? "";
    if (sessionName.startsWith("subagent-")) return;
    const activeController = new AbortController();
    controller = activeController;
    bridgeContext = ctx;
    sessionId = ctx.sessionManager.getSessionId();
    incarnation = crypto.randomUUID();
    terminalLocator = discoverTerminalLocator();
    if (!sessionId) {
      console.warn("[pi-session-bridge] session identity is unavailable; registration skipped");
      activeController.abort();
      return;
    }
    reportSettings(ctx);
    void connect(ctx, activeController);
  });

  pi.on("message_start", (event) => {
    const message = event.message as StreamMessage;
    queueEvent("message-start", {
      message_id: messageId(message),
      role: message.role ?? "unknown",
      timestamp: message.timestamp ?? Date.now(),
    });
  });

  pi.on("message_update", (event) => {
    const update = event.assistantMessageEvent;
    if (update.type !== "text_delta") return;
    const id = messageId(event.message as StreamMessage);
    const key = `${id}:${update.contentIndex}`;
    const pending = pendingDeltas.get(key) ?? { messageId: id, contentIndex: update.contentIndex, delta: "" };
    pending.delta += update.delta;
    pendingDeltas.set(key, pending);
  });

  pi.on("message_end", (event) => {
    const message = event.message as StreamMessage;
    const id = messageId(message);
    flushPendingDeltas(id);
    queueEvent("message-end", { message_id: id, message: sanitizeMessage(message) }, true);
  });

  pi.on("tool_execution_start", (event) => {
    queueEvent("tool-start", {
      tool_call_id: event.toolCallId,
      tool_name: event.toolName,
      arguments: event.args,
    }, true);
  });

  pi.on("tool_execution_end", (event) => {
    queueEvent("tool-end", {
      tool_call_id: event.toolCallId,
      tool_name: event.toolName,
      is_error: event.isError,
    }, true);
  });

  pi.on("model_select", (_event, ctx) => reportSettings(ctx));
  pi.on("thinking_level_select", (_event, ctx) => reportSettings(ctx));

  pi.on("agent_start", async (_event, ctx) => {
    reportSettings(ctx);
    await report("working", "agent-start").catch(() => undefined);
  });

  pi.on("agent_end", async (event) => {
    if (event.willContinue) return;
    await report("idle", "agent-settled").catch(() => undefined);
  });

  pi.on("agent_settled", async () => {
    await report("idle", "agent-settled").catch(() => undefined);
  });

  pi.on("session_shutdown", async (event) => {
    if (heartbeat) {
      clearInterval(heartbeat);
      heartbeat = null;
    }
    if (outboxTimer) {
      clearInterval(outboxTimer);
      outboxTimer = null;
    }
    if (deltaTimer) {
      clearInterval(deltaTimer);
      deltaTimer = null;
    }
    if (terminalTimer) {
      clearInterval(terminalTimer);
      terminalTimer = null;
    }
    flushPendingDeltas();
    await flushEventOutbox();
    await report("stopped", "bridge-shutdown", { reason: event.reason }).catch(() => undefined);
    await unregisterTerminalRoute();
    controller?.abort();
    controller = null;
    bridgeContext = null;
    terminalLocator = null;
    terminalInfo = { attachable: false, host: "", port: 0, protocolVersion: 0 };
    terminalRefresh = null;
    sessionId = "";
    incarnation = "";
  });
}
