import { expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { JSONRPCMessageSchema, InitializeResultSchema, ListToolsResultSchema, CallToolResultSchema } from "@modelcontextprotocol/sdk/types.js";

interface RpcResponse {
	result?: unknown;
	error?: unknown;
}

const entrypoint = join(import.meta.dir, "mcp.ts");

test("MCP requires an explicit stable client key without stdout diagnostics", async () => {
	const child = Bun.spawn([process.execPath, entrypoint], { stdout: "pipe", stderr: "pipe" });
	const [code, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
	expect(code).toBe(1);
	expect(stdout).toBe("");
	expect(stderr).toContain("--client-key NAME");
});

test("real Bun stdio handshake exposes and executes only shared compute tools", async () => {
	const directory = await mkdtemp(join(tmpdir(), "wh-mcp-test-"));
	const calls: string[] = [];
	const backend = Bun.serve({
		hostname: "127.0.0.1", port: 0,
		fetch(request) {
			expect(request.headers.get("authorization")).toBe(`Bearer ${"m".repeat(48)}`);
			const url = new URL(request.url);
			calls.push(url.pathname);
			if (url.pathname === "/api/v1/job-events") return new Response(new ReadableStream({
				start(controller) { controller.enqueue(new TextEncoder().encode(": keepalive\n\n")); },
			}), { headers: { "content-type": "text/event-stream" } });
			if (url.pathname === "/api/v1/workers") return Response.json([]);
			return new Response("unexpected backend action", { status: 404 });
		},
	});
	await writeFile(join(directory, "clusters.json"), JSON.stringify({ version: 1, clusters: [] }));
	const env = { ...process.env, WH_ORCHESTRATOR_URL: `http://127.0.0.1:${backend.port}`, WH_OPERATOR_TOKEN: "m".repeat(48), WH_CLUSTER_CONFIG: join(directory, "clusters.json"), WH_CLUSTER_STATE_DIR: join(directory, "state") };
	delete env.WH_SESSION_ROLE;
	delete env.WH_SESSION_TOKEN;
	delete env.WH_OPERATOR_TOKEN_FILE;
	const child = Bun.spawn([process.execPath, entrypoint, "--client-key", "stdio-test"], { env, stdin: "pipe", stdout: "pipe", stderr: "pipe" });
	const stderr = new Response(child.stderr).text();
	let nextId = 0;
	const pending = new Map<number, { resolve: (message: RpcResponse) => void; reject: (error: Error) => void }>();
	const reader = (async () => {
		let buffer = "";
		const decoder = new TextDecoder();
		for await (const chunk of child.stdout) {
			buffer += decoder.decode(chunk, { stream: true });
			for (;;) {
				const newline = buffer.indexOf("\n");
				if (newline < 0) break;
				const message = JSONRPCMessageSchema.parse(JSON.parse(buffer.slice(0, newline)));
				buffer = buffer.slice(newline + 1);
				if ("id" in message && typeof message.id === "number") {
					pending.get(message.id)?.resolve({
						result: "result" in message ? message.result : undefined,
						error: "error" in message ? message.error : undefined,
					});
					pending.delete(message.id);
				}
			}
		}
		if (buffer.trim()) throw new Error(`Unframed stdout: ${buffer}`);
		for (const waiter of pending.values()) waiter.reject(new Error("MCP stdout closed before responding"));
	})();
	const send = (message: unknown) => { child.stdin.write(`${JSON.stringify(message)}\n`); child.stdin.flush(); };
	const request = (method: string, params: unknown = {}) => new Promise<RpcResponse>((resolve, reject) => {
		const id = ++nextId;
		pending.set(id, { resolve, reject });
		send({ jsonrpc: "2.0", id, method, params });
	});
	try {
		const initialized = InitializeResultSchema.parse((await request("initialize", { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "wh-stdio-test", version: "1" } })).result);
		expect(initialized.serverInfo.name).toBe("worker-harness");
		expect(initialized.capabilities.experimental?.["claude/channel"]).toEqual({});
		expect(initialized.capabilities.tools).toEqual({});
		expect(calls).toEqual([]);
		send({ jsonrpc: "2.0", method: "notifications/initialized" });
		const listed = ListToolsResultSchema.parse((await request("tools/list")).result);
		expect(listed.tools.map(tool => tool.name)).toEqual(["wh_read", "wh_dispatch"]);
		const schemas = JSON.stringify(listed.tools);
		for (const action of ["pi_sessions", "fleet_status", "workers_prune", "grant_git_access", "pm_send", "dispatch_task", "wh_admin_deploy_image"]) expect(schemas).not.toContain(`\"${action}\"`);
		const read = await request("tools/call", { name: "wh_read", arguments: { action: "list_workers" } });
		expect(read.error).toBeUndefined();
		expect(CallToolResultSchema.parse(read.result).isError).not.toBe(true);
		expect(calls).toContain("/api/v1/workers");
		for (const [name, action] of [["wh_read", "pi_sessions"], ["wh_dispatch", "workers_prune"], ["wh_dispatch", "grant_git_access"], ["wh_dispatch", "pm_send"]]) {
			const denied = await request("tools/call", { name, arguments: { action } });
			expect(Boolean(denied.error) || CallToolResultSchema.parse(denied.result).isError === true).toBe(true);
		}
		const extraField = await request("tools/call", { name: "wh_read", arguments: { action: "list_workers", privileged: true } });
		expect(Boolean(extraField.error) || CallToolResultSchema.parse(extraField.result).isError === true).toBe(true);
		const unknown = await request("tools/call", { name: "wh_admin_restart", arguments: {} });
		expect(unknown.error).toBeDefined();
		expect(calls.every(path => ["/api/v1/workers", "/api/v1/job-events"].includes(path))).toBe(true);
	} finally {
		child.stdin.end();
		await child.exited;
		await reader;
		backend.stop(true);
		await stderr;
		await rm(directory, { recursive: true, force: true });
	}
}, 30_000);
