import { randomUUID } from "node:crypto";
import { getAuthorizationHeaders, getOrchestratorUrl, listDataPaths } from "../api.ts";

export interface IntegrationSettings {
  tailnet_key_file: string;
  tailnet_login_server: string;
  orchestrator_host: string;
  orchestrator_registration_port: number;
}

export function defaultRegistrationHost(controlUrl = getOrchestratorUrl()): string {
  return new URL(controlUrl).hostname.replace(/^\[|\]$/g, "");
}

/** Catalogue entries may be descendants; expose filesystem identities, not projects. */
export async function sharedFilesystemChoices(): Promise<string[]> {
  const catalogue = await listDataPaths();
  return [...new Set(Object.keys(catalogue).flatMap((path) => {
    const match = path.match(/^\/data\/shared\/([A-Za-z0-9][A-Za-z0-9_-]*)(?:\/|$)/);
    return match ? [`/data/shared/${match[1]}`] : [];
  }))].sort();
}

/** Live deployment capability, not a submission or an assertion of enrollment. */
export async function controlReadiness(signal?: AbortSignal): Promise<string[]> {
  const timeout = AbortSignal.timeout(15_000);
  try {
    signal?.throwIfAborted();
    const headers = await getAuthorizationHeaders();
    if (!headers.Authorization?.trim()) return ["Control API authorization is missing. Configure WH_OPERATOR_TOKEN_FILE (or WH_OPERATOR_TOKEN / an authorized session credential) in the local launcher, then resume. Never paste a token into cluster setup."];
    const response = await fetch(`${getOrchestratorUrl()}/openapi.json`, {
      headers,
      signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
    });
    if (!response.ok) return [`Control API capability check failed (HTTP ${response.status}); deploy a compatible control plane and resume.`];
    const schema = await response.json() as { paths?: Record<string, Record<string, unknown>> };
    const required = [
      ["/api/v1/workers/reservations", "post"],
      ["/api/v1/workers/{worker_id}/slurm", "patch"],
      ["/api/v1/job-events", "get"],
    ];
    const missing = required.filter(([path, method]) => !schema.paths?.[path!]?.[method!])
      .map(([path, method]) => `Control deployment lacks ${method!.toUpperCase()} ${path}; deploy the updated control plane and resume (no target registered).`);
    if (missing.length) return missing;
    // OpenAPI is public even with a bad credential. Verify authority against an
    // authenticated read-only surface without reserving a worker or consuming
    // another recipient's events. Close the stream as soon as headers arrive.
    const probe = new AbortController();
    try {
      const authorization = await fetch(`${getOrchestratorUrl()}/api/v1/job-events?recipient_id=wh-setup-${randomUUID()}&after=0`, {
        headers,
        signal: AbortSignal.any([timeout, probe.signal, ...(signal ? [signal] : [])]),
      });
      if (authorization.status === 401 || authorization.status === 403) return ["Control API rejected this credential. Configure an authorized WH_OPERATOR_TOKEN_FILE (or WH_OPERATOR_TOKEN / session credential) in the local launcher, then resume. Never paste a token into cluster setup."];
      if (authorization.status !== 200) return [`Control API authorization could not be verified (HTTP ${authorization.status}); restore authenticated job-event access, then resume.`];
      await authorization.body?.cancel();
      return [];
    } finally { probe.abort(); }
  } catch (error) {
    if (signal?.aborted) throw error;
    return ["Control API capability / authorization verification unavailable; restore authenticated access or deploy a compatible control plane, then resume."];
  }
}
