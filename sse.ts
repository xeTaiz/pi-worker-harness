import { ApiError, getAuthorizationHeaders, getOrchestratorUrl } from "./api.ts";

export interface JobLogStreamOptions {
  tail?: number;
  pollSeconds?: number;
  signal?: AbortSignal;
  onLine?: (line: string) => void;
}

export interface JobLogSubscription {
  stop: () => void;
  done: Promise<void>;
}

export async function streamTextLines(
  response: Response,
  onLine: (line: string) => void,
): Promise<void> {
  if (!response.body) {
    throw new Error("Streaming response body is not available");
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });

    while (true) {
      const nl = buffer.indexOf("\n");
      if (nl < 0) break;
      const line = buffer.slice(0, nl).replace(/\r$/, "");
      buffer = buffer.slice(nl + 1);
      onLine(line);
    }
  }

  buffer += decoder.decode();
  const tail = buffer.replace(/\r$/, "");
  if (tail.length > 0) {
    onLine(tail);
  }
}

export async function streamJobLogs(
  jobId: string,
  options: JobLogStreamOptions = {},
): Promise<void> {
  const params = new URLSearchParams();
  params.set("tail", String(options.tail ?? 50));
  params.set("poll_seconds", String(options.pollSeconds ?? 1));

  const url =
    `${getOrchestratorUrl()}/api/v1/jobs/${encodeURIComponent(jobId)}/logs/stream?` +
    params.toString();

  const authorization = await getAuthorizationHeaders();
  const res = await fetch(url, {
    method: "GET",
    headers: {
      Accept: "text/plain",
      ...authorization,
    },
    signal: options.signal,
  });

  if (!res.ok) {
    let detail: unknown = null;
    try {
      detail = await res.json();
    } catch (jsonErr) {
      try {
        detail = await res.text();
      } catch (textErr) {
        detail = {
          jsonParseError: jsonErr instanceof Error ? jsonErr.message : String(jsonErr),
          textReadError: textErr instanceof Error ? textErr.message : String(textErr),
        };
      }
    }

    throw new ApiError(
      res.status,
      "HTTP_ERROR",
      `Failed to stream logs for job ${jobId}`,
      detail,
    );
  }

  await streamTextLines(res, (line) => options.onLine?.(line));
}

export function subscribeJobLogs(
  jobId: string,
  options: Omit<JobLogStreamOptions, "signal"> = {},
): JobLogSubscription {
  const controller = new AbortController();
  const done = streamJobLogs(jobId, {
    ...options,
    signal: controller.signal,
  });

  return {
    stop: () => controller.abort(),
    done,
  };
}
