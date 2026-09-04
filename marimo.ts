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

function selectKernelSession(sessions: SessionMap, lifecycle: MarimoSession): string {
  const matches = Object.entries(sessions).filter(([, session]) =>
    session.path === lifecycle.notebook_path || session.filename === lifecycle.notebook_path
  );

  if (matches.length === 0) {
    const available = [...new Set(
      Object.values(sessions).flatMap((session) =>
        [session.path, session.filename].filter((path): path is string => typeof path === "string")
      ),
    )];
    const suffix = available.length > 0
      ? `; available paths: ${available.slice(0, 5).join(", ")}${available.length > 5 ? ", …" : ""}`
      : "";
    throw new Error(
      `No active browser kernel for ${lifecycle.notebook_path}; open ${lifecycle.url} in a browser first${suffix}`,
    );
  }
  if (matches.length > 1) {
    throw new Error(
      `Multiple active browser kernels match ${lifecycle.notebook_path}; close duplicate tabs and retry`,
    );
  }
  return matches[0][0];
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
  if (session.status !== "ready") {
    throw new Error(`Marimo session ${session.id} is stopped`);
  }

  const baseUrl = session.url.replace(/\/+$/, "");
  const sessionsUrl = `${baseUrl}/api/sessions`;
  let sessionsResponse: Response;
  try {
    sessionsResponse = await fetch(sessionsUrl, { signal: options.signal });
  } catch (error) {
    throw new Error(`Failed to reach Marimo Tailnet URL ${baseUrl}: ${errorMessage(error)}`);
  }
  if (!sessionsResponse.ok) {
    const detail = await describeHttpError(sessionsResponse);
    throw new Error(`Marimo request to ${sessionsUrl} failed (${sessionsResponse.status}): ${detail}`);
  }

  let sessionPayload: unknown;
  try {
    sessionPayload = await sessionsResponse.json();
  } catch (error) {
    throw new Error(`Malformed Marimo /api/sessions response: ${errorMessage(error)}`);
  }
  const kernelSessionId = selectKernelSession(parseSessionMap(sessionPayload), session);

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
    success: true,
    stdout,
    stderr,
    output: done.output?.data,
  };
}
