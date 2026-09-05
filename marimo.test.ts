import { afterEach, beforeEach, expect, test } from "bun:test";
import {
  createMarimo,
  getMarimo,
  getOrchestratorUrl,
  listMarimo,
  removeMarimo,
  setOrchestratorUrl,
} from "./api.ts";
import { executeMarimoCode, hydrateNotebook } from "./marimo.ts";
import type { MarimoSession } from "./types.ts";

const originalFetch = globalThis.fetch;
const originalOrchestratorUrl = getOrchestratorUrl();
const environmentKeys = ["WH_ORCHESTRATOR_URL", "WH_SESSION_TOKEN", "WH_SESSION_ROLE", "WH_OPERATOR_TOKEN", "WH_OPERATOR_TOKEN_FILE"];
const originalEnvironment = Object.fromEntries(environmentKeys.map((key) => [key, process.env[key]]));

beforeEach(() => {
  for (const key of environmentKeys) delete process.env[key];
  setOrchestratorUrl("http://orchestrator.test");
});

afterEach(() => {
  globalThis.fetch = originalFetch;
  setOrchestratorUrl(originalOrchestratorUrl);
  for (const key of environmentKeys) {
    if (originalEnvironment[key] === undefined) delete process.env[key];
    else process.env[key] = originalEnvironment[key];
  }
});

function lifecycleSession(patch: Partial<MarimoSession> = {}): MarimoSession {
  return {
    id: "managed-1",
    worker_id: "worker-1",
    worker_name: "worker",
    notebook_path: "/code/notebook.py",
    environment: "/usr/bin/python3",
    job_id: "job-1",
    tunnel_id: "tunnel-1",
    local_port: 18001,
    remote_port: 18002,
    bind_host: "100.64.0.8",
    url: "http://100.64.0.8:18001/",
    status: "ready",
    created_at: 1,
    ...patch,
  };
}

function jsonResponse(value: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(value), {
    headers: { "Content-Type": "application/json" },
    ...init,
  });
}

function streamResponse(chunks: string[]): Response {
  const encoder = new TextEncoder();
  return new Response(new ReadableStream({
    start(controller) {
      for (const chunk of chunks) controller.enqueue(encoder.encode(chunk));
      controller.close();
    },
  }), { headers: { "Content-Type": "text/event-stream" } });
}

function successfulStream(output: unknown = null): Response {
  return streamResponse([
    `event: done\ndata: ${JSON.stringify({ success: true, output: { data: output } })}\n\n`,
  ]);
}

test("lifecycle helpers preserve exact orchestrator methods, encoding, query, and body", async () => {
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const created = lifecycleSession();
  const responses = [jsonResponse(created), jsonResponse([created]), jsonResponse(created), jsonResponse({
    session_id: created.id,
    removed: true,
  })];
  globalThis.fetch = (async (input, init) => {
    calls.push({ url: String(input), init });
    return responses.shift()!;
  }) as typeof fetch;

  await createMarimo({
    worker_id: "worker/one",
    notebook_path: "/code/notebook.py",
    environment: "/usr/bin/python3",
    ready_timeout: 75,
  });
  await listMarimo("worker/one");
  await getMarimo("session/one");
  await removeMarimo("session/one");

  expect(calls.map(({ url, init }) => [url, init?.method ?? "GET"])).toEqual([
    ["http://orchestrator.test/api/v1/marimo", "POST"],
    ["http://orchestrator.test/api/v1/marimo?worker_id=worker%2Fone", "GET"],
    ["http://orchestrator.test/api/v1/marimo/session%2Fone", "GET"],
    ["http://orchestrator.test/api/v1/marimo/session%2Fone", "DELETE"],
  ]);
  expect(JSON.parse(String(calls[0].init?.body))).toEqual({
    worker_id: "worker/one",
    notebook_path: "/code/notebook.py",
    environment: "/usr/bin/python3",
    ready_timeout: 75,
  });
});

test("execution resolves the browser kernel on every call and follows reconnects", async () => {
  const requests: Array<{ url: string; init?: RequestInit }> = [];
  const responses = [
    jsonResponse({ "kernel-old": { path: "/code/notebook.py" } }),
    successfulStream(1),
    jsonResponse({ "kernel-new": { filename: "/code/notebook.py" } }),
    successfulStream(2),
  ];
  globalThis.fetch = (async (input, init) => {
    requests.push({ url: String(input), init });
    return responses.shift()!;
  }) as typeof fetch;

  const first = await executeMarimoCode(lifecycleSession(), "1");
  const second = await executeMarimoCode(lifecycleSession(), "2");

  expect(first.kernel_session_id).toBe("kernel-old");
  expect(second.kernel_session_id).toBe("kernel-new");
  expect(requests.map(({ url }) => url)).toEqual([
    "http://100.64.0.8:18001/api/sessions",
    "http://100.64.0.8:18001/api/kernel/execute",
    "http://100.64.0.8:18001/api/sessions",
    "http://100.64.0.8:18001/api/kernel/execute",
  ]);
  expect(new Headers(requests[1].init?.headers).get("Marimo-Session-Id")).toBe("kernel-old");
  expect(new Headers(requests[3].init?.headers).get("Marimo-Session-Id")).toBe("kernel-new");
  expect(requests[3].init?.body).toBe(JSON.stringify({ code: "2" }));
});

test("fragmented CRLF and LF events keep stdout, stderr, callbacks, and final output separate", async () => {
  globalThis.fetch = (async (input) => String(input).endsWith("/api/sessions")
    ? jsonResponse({ kernel: { path: "/code/notebook.py" } })
    : streamResponse([
      "event: std",
      "out\r\ndata: {\"data\":\"hel",
      "lo\\n\"}\r\n\r\nevent: stderr\n",
      "data: {\"data\":\"warn\\n\"}\n\nevent: done\r\ndata: {\"success\":true,\"output\":{\"data\":{\"answer\":42}}}",
    ])) as typeof fetch;
  const stdoutChunks: string[] = [];
  const stderrChunks: string[] = [];

  const result = await executeMarimoCode(lifecycleSession(), "print('hello')", {
    onStdout: (text) => stdoutChunks.push(text),
    onStderr: (text) => stderrChunks.push(text),
  });

  expect(result).toEqual({
    marimo_session_id: "managed-1",
    kernel_session_id: "kernel",
    kernel_created: false,
    success: true,
    stdout: "hello\n",
    stderr: "warn\n",
    output: { answer: 42 },
  });
  expect(stdoutChunks).toEqual(["hello\n"]);
  expect(stderrChunks).toEqual(["warn\n"]);
});

test.each([
  [null, "expected an object map"],
  [[], "expected an object map"],
  [{ bad: null }, "session bad is not an object"],
  [{ bad: { path: 42 } }, "session bad has a non-string path"],
  [{ bad: { filename: null } }, "session bad has a non-string filename"],
] as const)("rejects malformed session map %#", async (payload, message) => {
  globalThis.fetch = (async () => jsonResponse(payload)) as typeof fetch;
  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(message);
});

test("a notebook nobody opened gets a kernel created headlessly, then executes in it", async () => {
  const calls: string[] = [];
  let executeSessionHeader: string | null = null;
  globalThis.fetch = (async (input, init) => {
    const url = String(input);
    calls.push(url);
    if (url.endsWith("/api/sessions")) return jsonResponse({});
    if (url.includes("/sse?")) {
      return streamResponse([`data: ${JSON.stringify({ op: "kernel-ready", data: {} })}\n\n`]);
    }
    executeSessionHeader = new Headers(init?.headers).get("Marimo-Session-Id");
    return successfulStream("ok");
  }) as typeof fetch;

  const result = await executeMarimoCode(lifecycleSession(), "1");

  const attach = new URL(calls[1]);
  expect(attach.pathname).toBe("/sse");
  expect(attach.searchParams.get("file")).toBe("/code/notebook.py");
  const createdId = attach.searchParams.get("session_id")!;
  expect(createdId).toMatch(/^s_wh[0-9a-f]{12}$/);
  expect(calls[2]).toBe("http://100.64.0.8:18001/api/kernel/execute");
  expect(executeSessionHeader).toBe(createdId);
  expect(result.kernel_session_id).toBe(createdId);
  expect(result.output).toBe("ok");
  expect(result.kernel_created).toBe(true);
});

test("a refused attach reports the server's close reason instead of executing", async () => {
  let executed = false;
  globalThis.fetch = (async (input) => {
    const url = String(input);
    if (url.endsWith("/api/sessions")) return jsonResponse({ other: { path: "/code/other.py" } });
    if (url.includes("/sse?")) return streamResponse(["event: close\ndata: NO_FILE_KEY\n\n"]);
    executed = true;
    return successfulStream();
  }) as typeof fetch;

  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(
    "Marimo refused a kernel session for /code/notebook.py: NO_FILE_KEY",
  );
  expect(executed).toBe(false);
});

test("an attach stream that ends without kernel-ready fails instead of executing", async () => {
  let executed = false;
  globalThis.fetch = (async (input) => {
    const url = String(input);
    if (url.endsWith("/api/sessions")) return jsonResponse({});
    if (url.includes("/sse?")) return streamResponse([": keep-alive\n\n"]);
    executed = true;
    return successfulStream();
  }) as typeof fetch;

  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(
    "Marimo did not start a kernel for /code/notebook.py; the attach stream ended early",
  );
  expect(executed).toBe(false);
});

test("hydration runs the saved cells and reports how many ran", async () => {
  let submitted = "";
  globalThis.fetch = (async (input, init) => {
    const url = String(input);
    if (url.endsWith("/api/sessions")) return jsonResponse({ kernel: { path: "/code/notebook.py" } });
    submitted = JSON.parse(String(init?.body)).code;
    return streamResponse([
      'event: stdout\ndata: {"data": "re-ran cell \'a\'\\n__wh_hydrated__ 3\\n"}\n\n',
      `event: done\ndata: ${JSON.stringify({ success: true })}\n\n`,
    ]);
  }) as typeof fetch;

  expect(await hydrateNotebook(lifecycleSession())).toBe(3);
  expect(submitted).toContain("ctx.run_cell(_wh_id)");
});

test("hydration fails loudly when the kernel reports no cell count", async () => {
  globalThis.fetch = (async (input) => {
    const url = String(input);
    if (url.endsWith("/api/sessions")) return jsonResponse({ kernel: { path: "/code/notebook.py" } });
    return successfulStream();
  }) as typeof fetch;

  await expect(hydrateNotebook(lifecycleSession())).rejects.toThrow(
    "Marimo did not report how many cells it ran",
  );
});

test("duplicate exact notebook matches fail before execution", async () => {
  let calls = 0;
  globalThis.fetch = (async () => {
    calls += 1;
    return jsonResponse({
      one: { path: "/code/notebook.py" },
      two: { filename: "/code/notebook.py" },
    });
  }) as typeof fetch;

  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(
    "Multiple kernel sessions match /code/notebook.py; close duplicate editors and retry",
  );
  expect(calls).toBe(1);
});

test("stopped lifecycle resources fail without network access", async () => {
  let fetched = false;
  globalThis.fetch = (async () => {
    fetched = true;
    return jsonResponse({});
  }) as typeof fetch;

  await expect(executeMarimoCode(lifecycleSession({ status: "stopped" }), "1")).rejects.toThrow(
    "Marimo session managed-1 is stopped",
  );
  expect(fetched).toBe(false);
});

test("session discovery HTTP errors preserve JSON detail", async () => {
  globalThis.fetch = (async () => jsonResponse(
    { detail: "kernel registry unavailable" },
    { status: 502, statusText: "Bad Gateway" },
  )) as typeof fetch;
  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(
    "kernel registry unavailable",
  );
});

test("execution HTTP errors preserve response text", async () => {
  const responses = [
    jsonResponse({ kernel: { path: "/code/notebook.py" } }),
    new Response("execution rejected", { status: 409, statusText: "Conflict" }),
  ];
  globalThis.fetch = (async () => responses.shift()!) as typeof fetch;
  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow("execution rejected");
});

test("malformed SSE JSON fails with captured output", async () => {
  const responses = [
    jsonResponse({ kernel: { path: "/code/notebook.py" } }),
    streamResponse([
      "event: stdout\ndata: {\"data\":\"before\\n\"}\n\n",
      "event: done\ndata: not-json\n\n",
    ]),
  ];
  globalThis.fetch = (async () => responses.shift()!) as typeof fetch;
  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(
    /Malformed Marimo done event JSON[\s\S]*stdout:\nbefore/,
  );
});

test("done success false fails with captured stdout and stderr", async () => {
  const responses = [
    jsonResponse({ kernel: { path: "/code/notebook.py" } }),
    streamResponse([
      "event: stdout\ndata: {\"data\":\"partial out\"}\n\n",
      "event: stderr\ndata: {\"data\":\"traceback\"}\n\n",
      "event: done\ndata: {\"success\":false}\n\n",
    ]),
  ];
  globalThis.fetch = (async () => responses.shift()!) as typeof fetch;
  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(
    /Marimo execution failed[\s\S]*partial out[\s\S]*traceback/,
  );
});

test("network and abort failures name the Tailnet URL", async () => {
  globalThis.fetch = (async () => {
    throw new DOMException("cancelled", "AbortError");
  }) as typeof fetch;
  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(
    "Failed to reach Marimo Tailnet URL http://100.64.0.8:18001: cancelled",
  );
});

test("stream without done uses the incomplete-stream error and preserves output", async () => {
  const responses = [
    jsonResponse({ kernel: { path: "/code/notebook.py" } }),
    streamResponse(["event: stdout\ndata: {\"data\":\"unfinished\"}"]),
  ];
  globalThis.fetch = (async () => responses.shift()!) as typeof fetch;
  await expect(executeMarimoCode(lifecycleSession(), "1")).rejects.toThrow(
    /Marimo execution stream ended without a done event[\s\S]*stdout:\nunfinished/,
  );
});
