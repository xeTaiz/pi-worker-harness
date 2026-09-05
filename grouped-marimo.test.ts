import { afterAll, beforeAll, expect, mock, test } from "bun:test";
import { getOrchestratorUrl, setOrchestratorUrl } from "./api.ts";

mock.module("typebox", () => ({
  Type: {
    Object: (properties: Record<string, unknown>) => ({ type: "object", properties }),
    Optional: (schema: Record<string, unknown>) => ({ ...schema, optional: true }),
    String: (options: Record<string, unknown> = {}) => ({ type: "string", ...options }),
    Number: (options: Record<string, unknown> = {}) => ({ type: "number", ...options }),
    Boolean: (options: Record<string, unknown> = {}) => ({ type: "boolean", ...options }),
    Literal: (value: string) => ({ const: value }),
    Union: (anyOf: unknown[]) => ({ anyOf }),
  },
}));

mock.module("@earendil-works/pi-tui", () => ({
  Text: class Text {
    constructor(public text = "") {}
    setText(text: string) {
      this.text = text;
    }
  },
}));

interface CapturedTool {
  name: string;
  parameters: {
    properties: {
      action: { anyOf: Array<{ const: string }> };
    };
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
const environmentKeys = ["WH_ORCHESTRATOR_URL", "WH_SESSION_TOKEN", "WH_SESSION_ROLE", "WH_OPERATOR_TOKEN", "WH_OPERATOR_TOKEN_FILE"];
const originalEnvironment = Object.fromEntries(environmentKeys.map((key) => [key, process.env[key]]));
let registerGroupedTools: (pi: never) => void;

beforeAll(async () => {
  for (const key of environmentKeys) delete process.env[key];
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
        "list_data", "list_jobs", "list_queue", "get_job_logs", "list_tunnels", "pi_sessions",
      ],
      dispatch: [
        "data_copy", "exec", "stop_job", "enqueue", "update_queued_job",
        "add_tunnel", "remove_tunnel", "workers_prune", "upload_file", "download_file",
        "grant_git_access", "list_marimo", "get_marimo", "start_marimo", "stop_marimo",
        "execute_marimo",
      ],
    },
    orchestrator: {
      read: ["fleet_status", "list_projects"],
      dispatch: ["pm_send"],
    },
    pm: {
      read: [
        "list_workers", "available_gpus", "get_worker", "get_worker_summary",
        "list_data", "list_jobs", "list_queue", "get_job_logs", "pi_sessions",
      ],
      dispatch: [
        "data_copy", "exec", "stop_job", "enqueue", "update_queued_job",
        "list_marimo", "get_marimo", "start_marimo", "stop_marimo", "execute_marimo",
        "dispatch_task", "answer", "submit_pr", "teardown_task", "escalate",
      ],
    },
    task: {
      read: [
        "list_workers", "available_gpus", "get_worker", "get_worker_summary",
        "list_data", "list_jobs", "list_queue", "get_job_logs",
      ],
      dispatch: [
        "data_copy", "exec", "stop_job", "enqueue", "update_queued_job",
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
    tunnel_id: "tunnel-1",
    local_port: 18001,
    remote_port: 18002,
    bind_host: "100.64.0.8",
    url: "http://100.64.0.8:18001",
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
