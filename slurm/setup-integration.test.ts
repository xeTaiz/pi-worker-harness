import { afterEach, beforeEach, expect, spyOn, test, type Mock } from "bun:test";
import { controlReadiness, defaultRegistrationHost } from "./setup-integration.ts";

let request: Mock<typeof fetch> | undefined;
let originalRole: string | undefined;
let originalToken: string | undefined;
const capabilityPaths = {
  "/api/v1/workers/reservations": { post: {} },
  "/api/v1/workers/{worker_id}/slurm": { patch: {} },
  "/api/v1/job-events": { get: {} },
};
beforeEach(() => {
  originalRole = process.env.WH_SESSION_ROLE;
  originalToken = process.env.WH_SESSION_TOKEN;
  // Do not inspect the user's operator token file or send any live credential.
  process.env.WH_SESSION_ROLE = "agent";
  process.env.WH_SESSION_TOKEN = "setup-capability-test-fixture";
});
afterEach(() => {
  request?.mockRestore();
  request = undefined;
  if (originalRole === undefined) delete process.env.WH_SESSION_ROLE; else process.env.WH_SESSION_ROLE = originalRole;
  if (originalToken === undefined) delete process.env.WH_SESSION_TOKEN; else process.env.WH_SESSION_TOKEN = originalToken;
});

test("registration uses active control hostname without the privileged API port", () => {
  expect(defaultRegistrationHost("https://control.example.test:12889/control")).toBe("control.example.test");
  expect(defaultRegistrationHost("http://[2001:db8::7]:12889")).toBe("2001:db8::7");
});

test("live capability check verifies authenticated event headers and immediately closes the fresh-recipient stream", async () => {
  let cancelled = false;
  const stream = new ReadableStream({ cancel() { cancelled = true; } });
  request = spyOn(globalThis, "fetch")
    .mockResolvedValueOnce(Response.json({ paths: capabilityPaths }))
    .mockResolvedValueOnce(new Response(stream, { headers: { "Content-Type": "text/event-stream" } }));
  expect(await controlReadiness()).toEqual([]);
  expect(request).toHaveBeenCalledTimes(2);
  const [url, init] = request.mock.calls[0]!;
  expect(String(url)).toEndWith("/openapi.json");
  expect(init?.method).toBeUndefined();
  const [probeUrl, probeInit] = request.mock.calls[1]!;
  expect(String(probeUrl)).toMatch(/\/api\/v1\/job-events\?recipient_id=wh-setup-[0-9a-f-]+&after=0$/);
  expect(probeInit?.method).toBeUndefined();
  expect(probeInit?.headers).toEqual({ Authorization: "Bearer setup-capability-test-fixture" });
  expect(probeInit?.signal?.aborted).toBe(true);
  expect(cancelled).toBe(true);
});

for (const status of [401, 403]) {
  test(`public OpenAPI with rejected control credential (${status}) is blocked`, async () => {
    request = spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(Response.json({ paths: capabilityPaths }))
      .mockResolvedValueOnce(new Response("denied", { status }));
    const blockers = await controlReadiness();
    expect(blockers).toHaveLength(1);
    expect(blockers[0]).toContain("rejected this credential");
    expect(blockers[0]).toContain("WH_OPERATOR_TOKEN_FILE");
    expect(blockers[0]).toContain("resume");
    expect(request).toHaveBeenCalledTimes(2);
    expect(request.mock.calls[1]![1]?.signal?.aborted).toBe(true);
  });
}

for (const mode of ["http", "timeout"] as const) {
  test(`route availability without verified auth (${mode}) remains blocked`, async () => {
    request = spyOn(globalThis, "fetch").mockResolvedValueOnce(Response.json({ paths: capabilityPaths }));
    if (mode === "http") request.mockResolvedValueOnce(new Response("unavailable", { status: 503 }));
    else request.mockRejectedValueOnce(new DOMException("Timed out", "TimeoutError"));
    const blockers = await controlReadiness();
    expect(blockers).toHaveLength(1);
    expect(blockers[0]).toContain("resume");
    expect(request).toHaveBeenCalledTimes(2);
  });
}

test("missing control authorization blocks with file guidance and no token prompt or HTTP call", async () => {
  delete process.env.WH_SESSION_TOKEN;
  request = spyOn(globalThis, "fetch");
  const blockers = await controlReadiness();
  expect(blockers).toHaveLength(1);
  expect(blockers[0]).toContain("WH_OPERATOR_TOKEN_FILE");
  expect(blockers[0]).toContain("Never paste");
  expect(request).not.toHaveBeenCalled();
});

test("old live control deployment is explicitly blocked rather than ready", async () => {
  request = spyOn(globalThis, "fetch").mockResolvedValue(Response.json({ paths: { "/api/v1/jobs": { post: {} } } }));
  const blockers = await controlReadiness();
  expect(blockers).toHaveLength(3);
  expect(blockers.join("\n")).toContain("POST /api/v1/workers/reservations");
  expect(blockers.join("\n")).toContain("GET /api/v1/job-events");
  expect(blockers.every((message) => message.includes("deploy") && message.includes("no target registered"))).toBe(true);
});

for (const mode of ["http", "network"] as const) {
  test(`unavailable capability check (${mode}) stays resumable`, async () => {
    request = spyOn(globalThis, "fetch");
    if (mode === "http") request.mockResolvedValue(new Response("missing", { status: 404 }));
    else request.mockRejectedValue(new Error("offline"));
    const blockers = await controlReadiness();
    expect(blockers).toHaveLength(1);
    expect(blockers[0]).toContain("resume");
  });
}

test("cancellation propagates instead of presenting a stale capability result", async () => {
  const controller = new AbortController();
  controller.abort();
  request = spyOn(globalThis, "fetch").mockRejectedValue(controller.signal.reason);
  await expect(controlReadiness(controller.signal)).rejects.toBeDefined();
});
