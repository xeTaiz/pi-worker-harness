import { afterEach, beforeEach, expect, test } from "bun:test";
import { getOrchestratorUrl, setOrchestratorUrl } from "./api.ts";
import {
  COMPUTE_DISPATCH_ACTIONS,
  COMPUTE_READ_ACTIONS,
  executeComputeDispatch,
  executeComputeRead,
} from "./tools/compute-actions.ts";
import type { ComputeActionContext } from "./tools/compute-actions.ts";

const originalFetch = globalThis.fetch;
const originalUrl = getOrchestratorUrl();
const environmentKeys = ["WH_ORCHESTRATOR_URL", "WH_SESSION_ROLE", "WH_SESSION_TOKEN", "WH_OPERATOR_TOKEN", "WH_OPERATOR_TOKEN_FILE"];
const originalEnvironment = Object.fromEntries(environmentKeys.map((key) => [key, process.env[key]]));
const context: ComputeActionContext = {
  slurmContext: { client_kind: "claude", session_id: "test-client", recipient_id: "recipient-shared" },
};

beforeEach(() => {
  for (const key of environmentKeys) delete process.env[key];
  setOrchestratorUrl("http://compute.test");
});
afterEach(() => {
  globalThis.fetch = originalFetch;
  setOrchestratorUrl(originalUrl);
  for (const key of environmentKeys) {
    if (originalEnvironment[key] === undefined) delete process.env[key];
    else process.env[key] = originalEnvironment[key];
  }
});

test("shared schemas and execution reject Pi and administrative authority before transport", async () => {
  let requested = false;
  globalThis.fetch = (async () => { requested = true; throw new Error("unexpected transport"); }) as typeof fetch;
  expect(COMPUTE_READ_ACTIONS).not.toContain("pi_sessions");
  expect(COMPUTE_DISPATCH_ACTIONS).not.toContain("workers_prune");
  expect(COMPUTE_DISPATCH_ACTIONS).not.toContain("grant_git_access");
  for (const action of ["workers_prune", "grant_git_access", "dispatch_task", "ask_pm"]) {
    await expect(executeComputeDispatch({ action }, context)).rejects.toThrow("Invalid compute dispatch");
  }
  await expect(executeComputeRead({ action: "pi_sessions" }, context)).rejects.toThrow("Invalid compute read");
  await expect(executeComputeDispatch({ action: "exec", worker_id: "worker", command: "id", notify_recipient_id: "spoof" }, context)).rejects.toThrow("Invalid compute dispatch");
  await expect(executeComputeDispatch({ action: "enqueue", worker_id: "worker", command: "id", name: "bad", expected_seconds: -1 }, context)).rejects.toThrow("Invalid compute dispatch");
  expect(requested).toBeFalse();
});
