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

beforeAll(async () => {
  // Dynamic import is intentional: unresolved host modules must be mocked first.
  const grouped = await import("./tools/grouped.ts");
  grouped.registerGroupedTools({
    registerTool(tool: CapturedTool) {
      tools.set(tool.name, tool);
    },
  } as never);
  setOrchestratorUrl("http://orchestrator.test");
});

afterAll(() => {
  globalThis.fetch = originalFetch;
  setOrchestratorUrl(originalOrchestratorUrl);
});

function actionNames(toolName: string): string[] {
  return tools.get(toolName)!.parameters.properties.action.anyOf.map(({ const: value }) => value);
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
