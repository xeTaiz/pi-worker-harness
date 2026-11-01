import "./test-host.ts";
import { afterAll, beforeAll, expect, test } from "bun:test";
import { getOrchestratorUrl, setOrchestratorUrl } from "./api.ts";
import { events } from "./events.ts";


interface CapturedSchema {
  anyOf?: Array<{ const: string }>;
  minimum?: number;
  description?: string;
}

interface CapturedTool {
  name: string;
  parameters: {
    properties: {
      action: { anyOf: Array<{ const: string }> };
      [key: string]: CapturedSchema;
    };
    additionalProperties?: boolean;
  };
  execute: (
    toolCallId: string,
    params: Record<string, unknown>,
    signal?: AbortSignal,
    onUpdate?: (update: unknown) => void,
  ) => Promise<{ content: Array<{ type: string; text: string }>; details: unknown }>;
}

const originalFetch = globalThis.fetch;
const tools = new Map<string, CapturedTool>();
const originalOrchestratorUrl = getOrchestratorUrl();
const environmentKeys = ["WH_ORCHESTRATOR_URL", "WH_SESSION_TOKEN", "WH_SESSION_ROLE", "WH_API_KEY", "WH_API_KEY_FILE"];
const originalEnvironment = Object.fromEntries(environmentKeys.map((key) => [key, process.env[key]]));
let registerGroupedTools: (pi: never) => void;

beforeAll(async () => {
  for (const key of environmentKeys) delete process.env[key];
  process.env.WH_API_KEY = "whk_grouped-test";
  // Dynamic import is intentional: unresolved host modules must be mocked first.
  ({ registerGroupedTools } = await import("./tools/grouped.ts"));
  registerGroupedTools({
    registerTool(tool: CapturedTool) {
      tools.set(tool.name, tool);
    },
  } as never);
  setOrchestratorUrl("http://orchestrator.test");
});

afterAll(() => {
  globalThis.fetch = originalFetch;
  setOrchestratorUrl(originalOrchestratorUrl);
  for (const key of environmentKeys) {
    if (originalEnvironment[key] === undefined) delete process.env[key];
    else process.env[key] = originalEnvironment[key];
  }
});

function actionNames(toolName: string): string[] {
  return tools.get(toolName)!.parameters.properties.action.anyOf.map(({ const: value }) => value);
}

function roleTools(role: string): Map<string, CapturedTool> {
  const captured = new Map<string, CapturedTool>();
  const originalRole = process.env.WH_SESSION_ROLE;
  if (role) process.env.WH_SESSION_ROLE = role;
  else delete process.env.WH_SESSION_ROLE;
  try {
    registerGroupedTools({
      registerTool(tool: CapturedTool) {
        captured.set(tool.name, tool);
      },
    } as never);
  } finally {
    if (originalRole === undefined) delete process.env.WH_SESSION_ROLE;
    else process.env.WH_SESSION_ROLE = originalRole;
  }
  return captured;
}

function roleActionNames(toolsForRole: Map<string, CapturedTool>, toolName: string): string[] {
  return toolsForRole.get(toolName)!.parameters.properties.action.anyOf.map(({ const: value }) => value);
}

test("all Marimo actions exist only on wh_dispatch and no standalone tool is registered", () => {
  const marimoActions = [
    "list_marimo",
    "get_marimo",
    "start_marimo",
    "stop_marimo",
    "execute_marimo",
  ];

  expect(actionNames("wh_dispatch")).toEqual(expect.arrayContaining(marimoActions));
  expect(actionNames("wh_read")).not.toEqual(expect.arrayContaining(marimoActions));
  expect([...tools.keys()]).toEqual(["wh_read", "wh_dispatch"]);
});

test("role schemas expose exactly their allowed actions", () => {
  const expected = {
    "": {
      read: [
        "list_workers", "available_gpus", "get_worker", "get_worker_summary",
        "list_data", "list_jobs", "list_queue", "get_job_logs", "get_usage", "get_queue_history", "list_tunnels",
      ],
      dispatch: [
        "exec", "enqueue", "update_queued_job", "preview_queue_move", "move_queued_job",
        "stop_job", "add_tunnel", "remove_tunnel", "list_marimo", "get_marimo",
        "start_marimo", "stop_marimo", "execute_marimo",
      ],
    },
    orchestrator: {
      read: ["fleet_status", "list_projects"],
      dispatch: ["pm_send"],
    },
    pm: {
      read: [
        "list_workers", "available_gpus", "get_worker", "get_worker_summary",
        "list_data", "list_jobs", "list_queue", "get_job_logs", "get_usage", "get_queue_history", "pi_sessions",
      ],
      dispatch: [
        "data_copy", "exec", "stop_job", "enqueue", "update_queued_job",
        "preview_queue_move", "move_queued_job",
        "list_marimo", "get_marimo", "start_marimo", "stop_marimo", "execute_marimo",
        "dispatch_task", "answer", "submit_pr", "teardown_task", "escalate",
      ],
    },
    task: {
      read: [
        "list_workers", "available_gpus", "get_worker", "get_worker_summary",
        "list_data", "list_jobs", "list_queue", "get_job_logs", "get_usage", "get_queue_history",
      ],
      dispatch: [
        "data_copy", "exec", "stop_job", "enqueue", "update_queued_job",
        "preview_queue_move", "move_queued_job",
        "list_marimo", "get_marimo", "start_marimo", "stop_marimo", "execute_marimo",
        "ask_pm", "notify_pm",
      ],
    },
  } as const;

  for (const role of ["", "orchestrator", "pm", "task"] as const) {
    const captured = roleTools(role);
    expect([...captured.keys()]).toEqual(["wh_read", "wh_dispatch"]);
    expect(roleActionNames(captured, "wh_read")).toEqual(expected[role].read);
    expect(roleActionNames(captured, "wh_dispatch")).toEqual(expected[role].dispatch);
  }
});

test("role restrictions also reject direct execution without relying on schema validation", async () => {
  let requested = false;
  const previousFetch = globalThis.fetch;
  globalThis.fetch = (async () => {
    requested = true;
    throw new Error("unauthorized request reached the transport");
  }) as typeof fetch;
  try {
    await expect(roleTools("orchestrator").get("wh_dispatch")!.execute("exec", {
      action: "exec", worker_id: "worker", command: "id",
    })).rejects.toThrow("not available for role orchestrator");
    await expect(roleTools("task").get("wh_dispatch")!.execute("pr", {
      action: "submit_pr", task_session_id: "task", summary: "ready",
    })).rejects.toThrow("not available for role task");
    await expect(roleTools("orchestrator").get("wh_read")!.execute("logs", {
      action: "get_job_logs", job_id: "job",
    })).rejects.toThrow("not available for role orchestrator");
    for (const action of ["data_copy", "workers_prune", "upload_file", "download_file", "grant_git_access", "ask_pm", "notify_pm", "pm_send"]) {
      await expect(tools.get("wh_dispatch")!.execute(action, { action })).rejects.toThrow("not available");
    }
    for (const action of ["pi_sessions", "fleet_status", "list_projects", "list_marimo", "get_marimo"]) {
      await expect(tools.get("wh_read")!.execute(action, { action })).rejects.toThrow("not available");
    }
    expect(requested).toBeFalse();
    expect([...roleTools("unknown").keys()]).toEqual([]);
  } finally {
    globalThis.fetch = previousFetch;
  }
});

test("URL-bearing list and get resources are returned through wh_dispatch", async () => {
  const session = {
    id: "managed-1",
    worker_id: "worker-1",
    worker_name: "worker",
    notebook_path: "/code/notebook.py",
    environment: "/usr/bin/python3",
    job_id: "job-1",
    owner_user_id: "owner-1",
    url: "http://orchestrator.test/api/v1/marimo/managed-1/proxy/",
    status: "ready",
    created_at: 1,
  };
  const requests: string[] = [];
  globalThis.fetch = (async (input) => {
    const url = String(input);
    requests.push(url);
    return new Response(JSON.stringify(url.includes("managed-1") ? session : [session]), {
      headers: { "Content-Type": "application/json" },
    });
  }) as typeof fetch;
  const dispatch = tools.get("wh_dispatch")!;

  const listed = await dispatch.execute("list", {
    action: "list_marimo",
    worker_id: "worker-1",
  });
  const fetched = await dispatch.execute("get", {
    action: "get_marimo",
    marimo_session_id: "managed-1",
  });

  expect(requests).toEqual([
    "http://orchestrator.test/api/v1/marimo?worker_id=worker-1",
    "http://orchestrator.test/api/v1/marimo/managed-1",
  ]);
  expect(listed.details).toEqual({ sessions: [session] });
  expect(fetched.details).toEqual({ session });
  expect(listed.content[0].text).toContain(session.url);
  expect(fetched.content[0].text).toContain(session.url);
});

test("ordinary schemas omit internal fields and describe queue and tunnel requirements", () => {
  const read = tools.get("wh_read")!.parameters;
  const dispatch = tools.get("wh_dispatch")!.parameters;
  expect(read.properties.origin_session_id).toBeUndefined();
  for (const field of ["position", "repo", "content_b64", "src_path", "task_session_id"]) {
    expect(dispatch.properties[field]).toBeUndefined();
  }
  expect(dispatch.additionalProperties).toBeFalse();
  expect(dispatch.properties.gpu_count.minimum).toBe(1);
  expect(dispatch.properties.expected_seconds.minimum).toBe(1);
  expect(dispatch.properties.update_status.anyOf!.map((entry) => entry.const)).toEqual(["pending", "active"]);
  expect(dispatch.properties.local_port.description).toContain("trusted Tailnet");
  expect(dispatch.properties.remote_port.description).toContain("responsibility");
});

test("CPU and GPU submission validates required names, estimates, allocations and priority", async () => {
  const dispatch = tools.get("wh_dispatch")!;
  const cpu = { action: "exec", worker_id: "worker", command: "id", name: "setup", expected_seconds: 10 };
  for (const [params, message] of [
    [{ ...cpu, name: undefined }, "name"],
    [{ ...cpu, name: " " }, "blank"],
    [{ ...cpu, expected_seconds: undefined }, "expected_seconds"],
    [{ ...cpu, expected_seconds: 0 }, "positive integer"],
    [{ ...cpu, expected_seconds: 1.5 }, "positive integer"],
    [{ ...cpu, gpu_count: 0 }, "CPU-only"],
    [{ ...cpu, gpu_count: 1 }, "CPU-only"],
    [{ ...cpu, action: "enqueue" }, "gpu_count"],
    [{ ...cpu, action: "enqueue", gpu_count: 0 }, "positive integer"],
    [{ ...cpu, action: "enqueue", gpu_count: -1 }, "positive integer"],
    [{ ...cpu, action: "enqueue", gpu_count: 1.5 }, "positive integer"],
    [{ ...cpu, priority: "time_critical", priority_note: " " }, "nonblank priority_note"],
    [{ ...cpu, priority_note: "x".repeat(1001) }, "1000"],
  ] as const) {
    await expect(dispatch.execute("invalid", params)).rejects.toThrow(message);
  }
});

test("queue metadata requires explicit pending or active selection and never accepts position", async () => {
  const dispatch = tools.get("wh_dispatch")!;
  const base = { action: "update_queued_job", job_id: "job", expected_seconds: 20 };
  await expect(dispatch.execute("missing-status", base)).rejects.toThrow("update_status");
  await expect(dispatch.execute("position", { ...base, update_status: "pending", position: 1 })).rejects.toThrow("position is not supported");
  await expect(dispatch.execute("active-gpu", { ...base, update_status: "active", gpu_count: 1 })).rejects.toThrow("Active updates allow only");
  await expect(dispatch.execute("empty", { action: "update_queued_job", job_id: "job", update_status: "pending" })).rejects.toThrow("at least one metadata");
});

test("submissions and pending/active updates send priority metadata through the correct endpoints", async () => {
  const requests: Array<{ url: string; method: string; body: unknown }> = [];
  globalThis.fetch = (async (input, init) => {
    requests.push({ url: String(input), method: init?.method ?? "GET", body: JSON.parse(String(init?.body)) });
    return new Response(JSON.stringify({
      id: "job", name: "training", worker_id: "worker", worker_name: "Worker",
      status: "pending", execution_state: "active", termination_reason: null,
      owner_display_name: "Alice", priority: "time_critical", expected_seconds: 20,
    }), { headers: { "Content-Type": "application/json" } });
  }) as typeof fetch;
  const dispatch = tools.get("wh_dispatch")!;
  const metadata = { expected_seconds: 20, priority: "time_critical", priority_note: "Agreed deadline", deadline_at: null };
  const cpu = await dispatch.execute("cpu", { action: "exec", worker_id: "worker", command: "id", name: "setup", ...metadata });
  await dispatch.execute("gpu", { action: "enqueue", worker_id: "worker", command: "train", name: "train", gpu_count: 2, ...metadata });
  await dispatch.execute("pending", { action: "update_queued_job", job_id: "job", update_status: "pending", name: "renamed", gpu_count: 2, ...metadata });
  await dispatch.execute("active", { action: "update_queued_job", job_id: "job", update_status: "active", ...metadata });
  expect(requests.map(({ url, method }) => [url, method])).toEqual([
    ["http://orchestrator.test/api/v1/jobs", "POST"],
    ["http://orchestrator.test/api/v1/jobs/queue", "POST"],
    ["http://orchestrator.test/api/v1/jobs/job/queue", "PATCH"],
    ["http://orchestrator.test/api/v1/jobs/job", "PATCH"],
  ]);
  for (const { body } of requests) expect(body).toMatchObject(metadata);
  expect(requests[0].body).not.toHaveProperty("gpu_count");
  expect(requests[3].body).not.toHaveProperty("update_status");
  expect(cpu.content[0].text).toContain("execution_state=active");
  expect(cpu.content[0].text).toContain("termination_reason=none");
});

test("queue preview and commit require explicit revision, target, reason and preview identity", async () => {
  const dispatch = tools.get("wh_dispatch")!;
  const preview = { action: "preview_queue_move", job_id: "job", before_job_id: null, expected_revision: 0, reason: " agreed " };
  await expect(dispatch.execute("target", { ...preview, before_job_id: undefined })).rejects.toThrow("before_job_id");
  await expect(dispatch.execute("revision", { ...preview, expected_revision: undefined })).rejects.toThrow("expected_revision");
  await expect(dispatch.execute("revision", { ...preview, expected_revision: -1 })).rejects.toThrow("nonnegative integer");
  await expect(dispatch.execute("reason", { ...preview, reason: " " })).rejects.toThrow("blank");
  await expect(dispatch.execute("reason", { ...preview, reason: "x".repeat(1001) })).rejects.toThrow("1000");
  await expect(dispatch.execute("commit", { action: "move_queued_job", job_id: "job", expected_revision: 0 })).rejects.toThrow("preview_id");
  const requests: Array<{ url: string; body: unknown }> = [];
  globalThis.fetch = (async (input, init) => {
    requests.push({ url: String(input), body: JSON.parse(String(init?.body)) });
    return new Response(JSON.stringify({ changed: false }), { headers: { "Content-Type": "application/json" } });
  }) as typeof fetch;
  await dispatch.execute("preview", preview);
  await dispatch.execute("commit", { action: "move_queued_job", job_id: "job", expected_revision: 0, preview_id: "preview-1" });
  expect(requests).toEqual([
    { url: "http://orchestrator.test/api/v1/jobs/job/queue/preview", body: { before_job_id: null, expected_revision: 0, reason: "agreed" } },
    { url: "http://orchestrator.test/api/v1/jobs/job/queue/move", body: { preview_id: "preview-1", expected_revision: 0 } },
  ]);
});

test("queue conflict errors instruct a fresh fetch and preview without retrying", async () => {
  const dispatch = tools.get("wh_dispatch")!;
  for (const action of ["preview_queue_move", "move_queued_job"]) {
    let requests = 0;
    globalThis.fetch = (async () => {
      requests++;
      return new Response(JSON.stringify({ detail: { code: "QUEUE_CHANGED", message: "Queue changed" } }), {
        status: 409, headers: { "Content-Type": "application/json" },
      });
    }) as typeof fetch;
    await expect(dispatch.execute("conflict", {
      action, job_id: "job", before_job_id: null, reason: "agreed",
      expected_revision: 1, preview_id: "preview-1",
    })).rejects.toThrow("Re-fetch list_queue and re-preview");
    expect(requests).toBe(1);
  }
});

test("queue, usage and history reads use shared-board and filter-aware handlers", async () => {
  const board = { as_of: 100, workers: [{ worker_id: "worker", worker_name: "Worker", revision: 3, jobs: [], cpu_jobs: [] }] };
  const requests: string[] = [];
  globalThis.fetch = (async (input) => {
    const url = String(input);
    requests.push(url);
    return new Response(JSON.stringify(url.includes("history") ? { events: [], next_before_event_id: null } : url.includes("usage") ? { rows: [], as_of: 100, provisional: false } : board), {
      headers: { "Content-Type": "application/json" },
    });
  }) as typeof fetch;
  const read = tools.get("wh_read")!;
  const queue = await read.execute("queue", { action: "list_queue", worker_id: "worker" });
  await read.execute("usage", { action: "get_usage", worker_id: "worker", user_id: "owner" });
  await read.execute("history", { action: "get_queue_history", worker_id: "worker", job_id: "job", before_event_id: "event", limit: 20 });
  expect(queue.details).toEqual({ queue: board });
  expect(queue.content[0].text).toContain("revision 3");
  expect(requests[0]).toBe("http://orchestrator.test/api/v1/jobs/queue?worker_id=worker");
  expect(requests[1]).toContain("worker_id=worker");
  expect(requests[1]).toContain("user_id=owner");
  expect(requests[2]).toContain("before_event_id=event");
  expect(requests[2]).toContain("limit=20");
});

test("credential rotation discards old job output and private start events", async () => {
  const originalKey = process.env.WH_API_KEY;
  const emitted: unknown[] = [];
  const stopListening = events.on("worker-harness:job-started", (event) => emitted.push(event));
  globalThis.fetch = (async () => {
    process.env.WH_API_KEY = "whk_new-identity";
    return new Response(JSON.stringify({
      id: "old-private-job", name: "setup", worker_id: "worker", status: "running",
      execution_state: "active", termination_reason: null, owner_display_name: "Old owner",
      priority: "normal", expected_seconds: 10,
    }), { headers: { "Content-Type": "application/json" } });
  }) as typeof fetch;
  try {
    await expect(tools.get("wh_dispatch")!.execute("rotated", {
      action: "exec", worker_id: "worker", command: "id", name: "setup", expected_seconds: 10,
    })).rejects.toThrow("Credentials changed");
    expect(emitted).toEqual([]);
  } finally {
    stopListening();
    if (originalKey === undefined) delete process.env.WH_API_KEY;
    else process.env.WH_API_KEY = originalKey;
  }
});

test("credential rotation discards fetched logs from the previous owner", async () => {
  const originalKey = process.env.WH_API_KEY;
  globalThis.fetch = (async () => {
    process.env.WH_API_KEY = "whk_rotated-log-owner";
    return new Response(JSON.stringify({ job_id: "job", logs: "private old-owner logs", tail: 10, head: null }), {
      headers: { "Content-Type": "application/json" },
    });
  }) as typeof fetch;
  try {
    await expect(tools.get("wh_read")!.execute("rotated-logs", {
      action: "get_job_logs", job_id: "job",
    })).rejects.toThrow("Credentials changed");
  } finally {
    if (originalKey === undefined) delete process.env.WH_API_KEY;
    else process.env.WH_API_KEY = originalKey;
  }
});
