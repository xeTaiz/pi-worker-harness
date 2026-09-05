import { streamTextLines } from "./sse.ts";
import type {
  MarimoExecutionResult,
  MarimoKernelSession,
  MarimoSession,
} from "./types.ts";

export interface MarimoExecutionOptions {
  signal?: AbortSignal;
  onStdout?: (text: string) => void;
  onStderr?: (text: string) => void;
}

type SessionMap = Record<string, MarimoKernelSession>;

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function describeHttpError(response: Response): Promise<string> {
  const text = await response.text();
  if (text) {
    try {
      const body = JSON.parse(text) as { detail?: unknown };
      if (typeof body.detail === "string") return body.detail;
    } catch {
      // Fall through to the response text.
    }
    return text;
  }
  return response.statusText || `HTTP ${response.status}`;
}

function parseSessionMap(value: unknown): SessionMap {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("Malformed Marimo /api/sessions response: expected an object map");
  }

  for (const [id, entry] of Object.entries(value)) {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) {
      throw new Error(`Malformed Marimo /api/sessions response: session ${id} is not an object`);
    }
    const session = entry as Record<string, unknown>;
    if (Object.hasOwn(session, "path") && typeof session.path !== "string") {
      throw new Error(`Malformed Marimo /api/sessions response: session ${id} has a non-string path`);
    }
    if (Object.hasOwn(session, "filename") && typeof session.filename !== "string") {
      throw new Error(`Malformed Marimo /api/sessions response: session ${id} has a non-string filename`);
    }
  }

  return value as SessionMap;
}

function findKernelSession(sessions: SessionMap, lifecycle: MarimoSession): string | null {
  const matches = Object.entries(sessions).filter(([, session]) =>
    session.path === lifecycle.notebook_path || session.filename === lifecycle.notebook_path
  );

  if (matches.length === 0) return null;
  if (matches.length > 1) {
    throw new Error(
      `Multiple kernel sessions match ${lifecycle.notebook_path}; close duplicate editors and retry`,
    );
  }
  return matches[0][0];
}

async function fetchKernelSessions(
  baseUrl: string,
  signal: AbortSignal | undefined,
): Promise<SessionMap> {
  const sessionsUrl = `${baseUrl}/api/sessions`;
  let response: Response;
  try {
    response = await fetch(sessionsUrl, { signal });
  } catch (error) {
    throw new Error(`Failed to reach Marimo Tailnet URL ${baseUrl}: ${errorMessage(error)}`);
  }
  if (!response.ok) {
    const detail = await describeHttpError(response);
    throw new Error(`Marimo request to ${sessionsUrl} failed (${response.status}): ${detail}`);
  }

  let payload: unknown;
  try {
    payload = await response.json();
  } catch (error) {
    throw new Error(`Malformed Marimo /api/sessions response: ${errorMessage(error)}`);
  }
  return parseSessionMap(payload);
}

/**
 * Marimo spawns a kernel when a client attaches, so a notebook that nobody has
 * opened has no kernel to execute in. Attach headlessly over the SSE transport
 * to create one, then detach immediately: an attached consumer would demote a
 * later browser to a non-editing viewer, while a detached session stays alive
 * (edit mode closes sessions on disconnect only under an explicit --session-ttl)
 * and is resumed — with its outputs replayed — by the first browser that opens
 * the notebook.
 */
async function createKernelSession(
  baseUrl: string,
  lifecycle: MarimoSession,
  signal: AbortSignal | undefined,
): Promise<string> {
  const kernelSessionId = `s_wh${crypto.randomUUID().replaceAll("-", "").slice(0, 12)}`;
  const attachUrl = `${baseUrl}/sse?session_id=${encodeURIComponent(kernelSessionId)}` +
    `&file=${encodeURIComponent(lifecycle.notebook_path)}`;

  const controller = new AbortController();
  const propagateAbort = () => controller.abort();
  signal?.addEventListener("abort", propagateAbort, { once: true });

  try {
    let response: Response;
    try {
      response = await fetch(attachUrl, {
        headers: { Accept: "text/event-stream" },
        signal: controller.signal,
      });
    } catch (error) {
      throw new Error(`Failed to reach Marimo Tailnet URL ${baseUrl}: ${errorMessage(error)}`);
    }
    if (!response.ok) {
      const detail = await describeHttpError(response);
      throw new Error(`Marimo request to ${attachUrl} failed (${response.status}): ${detail}`);
    }

    // The server attaches the consumer lazily on the first read, so the stream
    // must be consumed until the kernel announces itself.
    let ready = false;
    let rejection: string | null = null;
    let closeEvent = false;
    try {
      await streamTextLines(response, (line) => {
        if (line.startsWith("event:")) {
          closeEvent = line.slice("event:".length).trim() === "close";
          return;
        }
        if (!line.startsWith("data:")) return;
        const data = line.slice("data:".length).trim();

        if (closeEvent) {
          rejection = data;
          controller.abort();
          return;
        }
        let payload: unknown;
        try {
          payload = JSON.parse(data);
        } catch {
          return;
        }
        if (
          payload !== null && typeof payload === "object" && "op" in payload &&
          payload.op === "kernel-ready"
        ) {
          ready = true;
          controller.abort();
        }
      });
    } catch (error) {
      if (!controller.signal.aborted) {
        throw new Error(`Marimo kernel attach stream failed: ${errorMessage(error)}`);
      }
      if (signal?.aborted) throw error;
    }

    if (rejection !== null) {
      throw new Error(
        `Marimo refused a kernel session for ${lifecycle.notebook_path}: ${rejection}`,
      );
    }
    if (!ready) {
      throw new Error(
        `Marimo did not start a kernel for ${lifecycle.notebook_path}; the attach stream ended early`,
      );
    }
    return kernelSessionId;
  } finally {
    // Detaching is what keeps the notebook editable for the user.
    controller.abort();
    signal?.removeEventListener("abort", propagateAbort);
  }
}

export interface ResolvedKernelSession {
  id: string;
  /** True when this call created the kernel, so its namespace starts empty. */
  created: boolean;
}

/** Resolve the kernel for a managed session, creating one when none exists. */
export async function ensureKernelSession(
  session: MarimoSession,
  options: { signal?: AbortSignal } = {},
): Promise<ResolvedKernelSession> {
  if (session.status !== "ready") {
    throw new Error(`Marimo session ${session.id} is stopped`);
  }
  const baseUrl = session.url.replace(/\/+$/, "");
  const existing = findKernelSession(await fetchKernelSessions(baseUrl, options.signal), session);
  if (existing !== null) return { id: existing, created: false };
  return { id: await createKernelSession(baseUrl, session, options.signal), created: true };
}

function withCapturedOutput(message: string, stdout: string, stderr: string): string {
  const parts = [message];
  if (stdout) parts.push(`stdout:\n${stdout}`);
  if (stderr) parts.push(`stderr:\n${stderr}`);
  return parts.join("\n");
}

export async function executeMarimoCode(
  session: MarimoSession,
  code: string,
  options: MarimoExecutionOptions = {},
): Promise<MarimoExecutionResult> {
  const baseUrl = session.url.replace(/\/+$/, "");
  const kernel = await ensureKernelSession(session, { signal: options.signal });
  const kernelSessionId = kernel.id;

  const executeUrl = `${baseUrl}/api/kernel/execute`;
  let executeResponse: Response;
  try {
    executeResponse = await fetch(executeUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "text/event-stream",
        "Marimo-Session-Id": kernelSessionId,
      },
      body: JSON.stringify({ code }),
      signal: options.signal,
    });
  } catch (error) {
    throw new Error(`Failed to reach Marimo Tailnet URL ${baseUrl}: ${errorMessage(error)}`);
  }
  if (!executeResponse.ok) {
    const detail = await describeHttpError(executeResponse);
    throw new Error(`Marimo request to ${executeUrl} failed (${executeResponse.status}): ${detail}`);
  }

  let eventType: "stdout" | "stderr" | "done" | null = null;
  let stdout = "";
  let stderr = "";
  let done: { success: boolean; output?: { data?: unknown } } | null = null;

  try {
    await streamTextLines(executeResponse, (line) => {
      if (line.startsWith("event:")) {
        const name = line.slice("event:".length).trim();
        eventType = name === "stdout" || name === "stderr" || name === "done" ? name : null;
        return;
      }
      if (!line.startsWith("data:") || eventType === null) return;

      let payload: unknown;
      try {
        payload = JSON.parse(line.slice("data:".length).trim());
      } catch (error) {
        throw new Error(`Malformed Marimo ${eventType} event JSON: ${errorMessage(error)}`);
      }
      if (payload === null || typeof payload !== "object" || Array.isArray(payload)) {
        throw new Error(`Malformed Marimo ${eventType} event JSON: expected an object`);
      }
      const body = payload as Record<string, unknown>;

      if (eventType === "stdout" || eventType === "stderr") {
        if (typeof body.data !== "string") {
          throw new Error(`Malformed Marimo ${eventType} event JSON: data must be a string`);
        }
        if (eventType === "stdout") {
          stdout += body.data;
          options.onStdout?.(body.data);
        } else {
          stderr += body.data;
          options.onStderr?.(body.data);
        }
        return;
      }

      if (typeof body.success !== "boolean") {
        throw new Error("Malformed Marimo done event JSON: success must be a boolean");
      }
      if (body.output !== undefined && (
        body.output === null || typeof body.output !== "object" || Array.isArray(body.output)
      )) {
        throw new Error("Malformed Marimo done event JSON: output must be an object");
      }
      done = body as { success: boolean; output?: { data?: unknown } };
    });
  } catch (error) {
    throw new Error(withCapturedOutput(errorMessage(error), stdout, stderr));
  }

  if (done === null) {
    throw new Error(withCapturedOutput(
      "Marimo execution stream ended without a done event",
      stdout,
      stderr,
    ));
  }
  if (!done.success) {
    throw new Error(withCapturedOutput("Marimo execution failed", stdout, stderr));
  }

  return {
    marimo_session_id: session.id,
    kernel_session_id: kernelSessionId,
    kernel_created: kernel.created,
    success: true,
    stdout,
    stderr,
    output: done.output?.data,
  };
}

/**
 * Run every cell already saved in the notebook.
 *
 * A kernel created for an existing notebook has its cells registered in the
 * graph but never executed: `/api/kernel/execute` instantiates with
 * `auto_run=False`, and marimo's own `/api/kernel/instantiate` is behind skew
 * protection, which exempts only `/api/kernel/execute`. Running the cells
 * through code mode reproduces what opening the notebook in a browser would
 * have done, so the kernel holds the notebook's state and the user sees
 * outputs on arrival.
 */
export async function hydrateNotebook(
  session: MarimoSession,
  options: { signal?: AbortSignal } = {},
): Promise<number> {
  const result = await executeMarimoCode(
    session,
    [
      "import marimo._code_mode as cm",
      "async with cm.get_context() as ctx:",
      "    _wh_ids = [_wh_cell.id for _wh_cell in ctx.cells]",
      "    for _wh_id in _wh_ids:",
      "        ctx.run_cell(_wh_id)",
      'print("__wh_hydrated__", len(_wh_ids))',
    ].join("\n"),
    options,
  );
  const match = /__wh_hydrated__ (\d+)/.exec(result.stdout);
  if (match === null) {
    throw new Error(withCapturedOutput(
      "Marimo did not report how many cells it ran",
      result.stdout,
      result.stderr,
    ));
  }
  return Number(match[1]);
}
