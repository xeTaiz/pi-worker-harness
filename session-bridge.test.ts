import "./test-host.ts";
import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { registerSessionBridge } from "./session-bridge.ts";
import { listQueue } from "./api.ts";
import { streamJobLogs } from "./sse.ts";

test.each(["orchestrator", "pm", "task"])("%s bearer reaches bridge registration, polling, events, acknowledgements, API and logs", async (role) => {
  const keys = ["WH_SESSION_ID", "WH_SESSION_ROLE", "WH_SESSION_TOKEN", "WH_API_KEY", "WH_API_KEY_FILE", "WH_ORCHESTRATOR_URL", "WH_PI_SESSION_ID", "TMUX", "TMUX_PANE", "ZELLIJ_SESSION_NAME", "ZELLIJ_PANE_ID"];
  const saved = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  const stdinTTY = Object.getOwnPropertyDescriptor(process.stdin, "isTTY");
  const stdoutTTY = Object.getOwnPropertyDescriptor(process.stdout, "isTTY");
  const originalFetch = globalThis.fetch;
  const directory = await mkdtemp(join(tmpdir(), "wh-bridge-auth-"));
  const handlers = new Map<string, (event: unknown, ctx?: unknown) => unknown>();
  const requests: Array<{ url: URL; headers: Headers; body: Record<string, unknown> | null }> = [];
  const delivered: unknown[] = [];
  let polls = 0;
  let acknowledge!: () => void;
  const acknowledged = new Promise<void>((resolve) => { acknowledge = resolve; });

  try {
    for (const key of keys) delete process.env[key];
    process.env.WH_ORCHESTRATOR_URL = " http://fleet.test/// ";
    process.env.WH_API_KEY_FILE = join(directory, "key");
    await writeFile(process.env.WH_API_KEY_FILE, "whk_user\n");
    process.env.WH_SESSION_ROLE = role;
    process.env.WH_SESSION_ID = `allocated-${role}`;
    process.env.WH_SESSION_TOKEN = `${role}-secret`;
    Object.defineProperty(process.stdin, "isTTY", { configurable: true, value: true });
    Object.defineProperty(process.stdout, "isTTY", { configurable: true, value: true });
    globalThis.fetch = (async (input: string | URL | Request, init?: RequestInit) => {
      const url = new URL(String(input));
      requests.push({ url, headers: new Headers(init?.headers), body: init?.body ? JSON.parse(String(init.body)) : null });
      if (url.pathname.endsWith("/commands")) {
        if (polls++ === 0) return Response.json([{ id: "command-1", kind: "prompt", message: "Please answer", deliver_as: "followUp" }]);
        return await new Promise<Response>((_resolve, reject) => {
          init?.signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
        });
      }
      if (url.pathname.endsWith(":ack")) acknowledge();
      if (url.pathname.endsWith("/logs/stream")) return new Response("finished\n");
      return Response.json([]);
    }) as typeof fetch;
    registerSessionBridge({
      on: (name: string, handler: (event: unknown, ctx?: unknown) => unknown) => handlers.set(name, handler),
      getThinkingLevel: () => "off",
      sendUserMessage: async (message: string, options: unknown) => { delivered.push([message, options]); },
    } as never);
    const context = {
      cwd: directory,
      sessionManager: {
        getSessionId: () => "ordinary-session",
        getSessionName: () => "",
        getSessionFile: () => join(directory, "conversation.jsonl"),
        getBranch: () => [],
      },
      modelRegistry: { getAvailable: () => [] },
      hasPendingMessages: () => false,
    };
    await handlers.get("session_start")!({}, context);
    await acknowledged;
    await handlers.get("agent_end")!({ willContinue: false });
    await listQueue();
    const lines: string[] = [];
    await streamJobLogs("job", { onLine: (line) => lines.push(line) });
    expect(delivered).toEqual([["Please answer", { deliverAs: "followUp" }]]);
    expect(lines).toEqual(["finished"]);
    const sessionId = `allocated-${role}`;
    const registration = requests.find(({ url }) => url.pathname.endsWith("/register"))?.body;
    expect(registration?.session_id).toBe(sessionId);
    expect(registration?.resume_path).toBe(join(directory, "conversation.jsonl"));
    expect(new Set(requests.map(({ url }) => url.pathname))).toEqual(new Set([
      "/api/v1/pi/bridge/register",
      `/api/v1/pi/bridge/${sessionId}/events`,
      `/api/v1/pi/bridge/${sessionId}/commands`,
      `/api/v1/pi/bridge/${sessionId}/commands/command-1:ack`,
      "/api/v1/jobs/queue",
      "/api/v1/jobs/job/logs/stream",
    ]));
    for (const { url, headers } of requests) {
      expect(url.origin).toBe("http://fleet.test");
      expect(headers.get("Authorization")).toBe(`Bearer ${role}-secret`);
    }
  } finally {
    await handlers.get("session_shutdown")?.({ reason: "test" });
    globalThis.fetch = originalFetch;
    for (const key of keys) {
      if (saved[key] === undefined) delete process.env[key];
      else process.env[key] = saved[key];
    }
    if (stdinTTY) Object.defineProperty(process.stdin, "isTTY", stdinTTY);
    else delete process.stdin.isTTY;
    if (stdoutTTY) Object.defineProperty(process.stdout, "isTTY", stdoutTTY);
    else delete process.stdout.isTTY;
    await rm(directory, { recursive: true, force: true });
  }
});

test("ordinary API keys never register bridge hooks or host relays, even in a TTY", async () => {
  const keys = ["WH_SESSION_ROLE", "WH_SESSION_TOKEN", "WH_API_KEY", "WH_API_KEY_FILE"];
  const saved = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  const stdinTTY = Object.getOwnPropertyDescriptor(process.stdin, "isTTY");
  const stdoutTTY = Object.getOwnPropertyDescriptor(process.stdout, "isTTY");
  const directory = await mkdtemp(join(tmpdir(), "wh-bridge-gate-"));
  const hooks: string[] = [];
  const pi = { on: (name: string) => hooks.push(name) } as never;
  try {
    for (const key of keys) delete process.env[key];
    Object.defineProperty(process.stdin, "isTTY", { configurable: true, value: true });
    Object.defineProperty(process.stdout, "isTTY", { configurable: true, value: true });
    process.env.WH_API_KEY = "whk_ordinary";
    registerSessionBridge(pi);
    expect(hooks).toEqual([]);
    delete process.env.WH_API_KEY;
    process.env.WH_API_KEY_FILE = join(directory, "key");
    await writeFile(process.env.WH_API_KEY_FILE, "whk_file\n");
    registerSessionBridge(pi);
    expect(hooks).toEqual([]);
    // An unscoped token must not turn a human session into an internal one.
    process.env.WH_SESSION_TOKEN = "unscoped-token";
    registerSessionBridge(pi);
    expect(hooks).toEqual([]);
  } finally {
    for (const key of keys) {
      if (saved[key] === undefined) delete process.env[key];
      else process.env[key] = saved[key];
    }
    if (stdinTTY) Object.defineProperty(process.stdin, "isTTY", stdinTTY);
    else delete process.stdin.isTTY;
    if (stdoutTTY) Object.defineProperty(process.stdout, "isTTY", stdoutTTY);
    else delete process.stdout.isTTY;
    await rm(directory, { recursive: true, force: true });
  }
});

test.each(["orchestrator", "pm", "task"])("%s bridge without its token fails before registering hooks", (role) => {
  const keys = ["WH_SESSION_ROLE", "WH_SESSION_TOKEN", "WH_API_KEY"];
  const saved = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  const hooks: string[] = [];
  try {
    process.env.WH_SESSION_ROLE = role;
    process.env.WH_API_KEY = "whk_never-inherited";
    delete process.env.WH_SESSION_TOKEN;
    expect(() => registerSessionBridge({ on: (name: string) => hooks.push(name) } as never))
      .toThrow("WH_SESSION_TOKEN is required");
    expect(hooks).toEqual([]);
  } finally {
    for (const key of keys) {
      if (saved[key] === undefined) delete process.env[key];
      else process.env[key] = saved[key];
    }
  }
});

test("ordinary extension startup exposes grouped tools without bridge or admin registration", async () => {
  // Load the entrypoint only after the shared Pi host mocks are installed.
  const { default: registerExtension } = await import("./index.ts");
  const keys = ["WH_SESSION_ROLE", "WH_SESSION_TOKEN", "WH_API_KEY", "WH_ORCHESTRATOR_URL"];
  const saved = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  const hooks: string[] = [];
  const tools: string[] = [];
  try {
    delete process.env.WH_SESSION_ROLE;
    delete process.env.WH_SESSION_TOKEN;
    process.env.WH_API_KEY = "whk_user";
    process.env.WH_ORCHESTRATOR_URL = "http://registration.test";
    await registerExtension({
      on: (name: string) => hooks.push(name),
      registerTool: (tool: { name: string }) => tools.push(tool.name),
      registerCommand: () => {},
      registerShortcut: () => {},
    } as never);
    expect(tools).toEqual(["wh_read", "wh_dispatch"]);
    // Only the panel/widget lifecycle is installed; bridge-only hooks are absent.
    expect(hooks).toEqual(["session_start", "session_shutdown"]);
  } finally {
    for (const key of keys) {
      if (saved[key] === undefined) delete process.env[key];
      else process.env[key] = saved[key];
    }
  }
});

test("internal extension startup without a token fails before installing anything", async () => {
  // Load the entrypoint only after the shared Pi host mocks are installed.
  const { default: registerExtension } = await import("./index.ts");
  const keys = ["WH_SESSION_ROLE", "WH_SESSION_TOKEN", "WH_API_KEY"];
  const saved = Object.fromEntries(keys.map((key) => [key, process.env[key]]));
  const registrations: string[] = [];
  try {
    process.env.WH_SESSION_ROLE = "task";
    process.env.WH_API_KEY = "whk_never-inherited";
    delete process.env.WH_SESSION_TOKEN;
    await expect(registerExtension({
      on: () => registrations.push("hook"),
      registerTool: () => registrations.push("tool"),
      registerCommand: () => registrations.push("command"),
      registerShortcut: () => registrations.push("shortcut"),
    } as never)).rejects.toThrow("WH_SESSION_TOKEN is required");
    expect(registrations).toEqual([]);
  } finally {
    for (const key of keys) {
      if (saved[key] === undefined) delete process.env[key];
      else process.env[key] = saved[key];
    }
  }
});
