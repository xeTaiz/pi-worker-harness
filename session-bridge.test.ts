import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { registerSessionBridge } from "./session-bridge.ts";
import { listQueue } from "./api.ts";
import { streamJobLogs } from "./sse.ts";

test.each(["task", "operator"])("%s bearer reaches bridge registration, polling, events, acknowledgements, API and logs", async (role) => {
  const keys = ["WH_SESSION_ID", "WH_SESSION_ROLE", "WH_SESSION_TOKEN", "WH_OPERATOR_TOKEN", "WH_OPERATOR_TOKEN_FILE", "WH_ORCHESTRATOR_URL", "WH_PI_SESSION_ID", "TMUX", "TMUX_PANE", "ZELLIJ_SESSION_NAME", "ZELLIJ_PANE_ID"];
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
    process.env.WH_OPERATOR_TOKEN_FILE = join(directory, "token");
    await writeFile(process.env.WH_OPERATOR_TOKEN_FILE, "operator-secret\n");
    if (role === "task") {
      process.env.WH_SESSION_ROLE = role;
      process.env.WH_SESSION_ID = "allocated-task";
      process.env.WH_SESSION_TOKEN = "task-secret";
    }
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
    const sessionId = role === "task" ? "allocated-task" : "ordinary-session";
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
      expect(headers.get("Authorization")).toBe(`Bearer ${role === "task" ? "task" : "operator"}-secret`);
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
