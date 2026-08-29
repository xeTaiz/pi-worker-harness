import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { events } from "../events.ts";
import { ApiError, getWorker, startJob } from "../api.ts";

function toToolError(err: unknown): Error {
  if (err instanceof ApiError) {
    return new Error(`${err.message}${err.detail ? ` (${JSON.stringify(err.detail)})` : ""}`);
  }
  return err instanceof Error ? err : new Error(String(err));
}

type DeployStatus = "hashing" | "transferring" | "completed" | "failed" | "cancelling" | "cancelled";

type DeployTransfer = {
  id: string;
  workerId: string;
  workerName: string;
  sshTarget: string;
  localPath: string;
  remotePath: string;
  tmpPath: string;
  totalBytes: number;
  timeoutMs: number;
  status: DeployStatus;
  startedAt: number;
  transferStartedAt?: number;
  finishedAt?: number;
  bytesHashed: number;
  bytesTransferred: number;
  sha256?: string;
  stderr: string;
  error?: string;
  returnCode?: number | null;
  cancelRequested: boolean;
  hashStream?: any;
  inputStream?: any;
  child?: any;
  timeoutHandle?: ReturnType<typeof setTimeout>;
};

const deployTransfers = new Map<string, DeployTransfer>();
const GIB = 1024 * 1024 * 1024;
const DEPLOY_TIMEOUT_PER_GIB_MS = 10 * 60 * 1000;
const MIN_DEPLOY_TIMEOUT_MS = 10 * 60 * 1000;

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, `'"'"'`)}'`;
}

function finishTransfer(
  transfer: DeployTransfer,
  status: "completed" | "failed" | "cancelled",
  error?: string,
): void {
  if (["completed", "failed", "cancelled"].includes(transfer.status)) return;
  if (transfer.timeoutHandle) clearTimeout(transfer.timeoutHandle);
  transfer.status = status;
  transfer.finishedAt = Date.now();
  transfer.child = undefined;
  transfer.inputStream?.destroy();
  transfer.hashStream?.destroy();
  transfer.inputStream = undefined;
  transfer.hashStream = undefined;
  if (error) transfer.error = error;
  if (status === "completed") events.emit("worker-harness:refresh", undefined);
}

function deploymentDetails(transfer: DeployTransfer) {
  const now = transfer.finishedAt ?? Date.now();
  const activeSince = transfer.transferStartedAt ?? transfer.startedAt;
  const bytes = transfer.status === "hashing" ? transfer.bytesHashed : transfer.bytesTransferred;
  const elapsedSeconds = Math.max(0.001, (now - activeSince) / 1000);
  return {
    transfer_id: transfer.id,
    worker_id: transfer.workerId,
    worker_name: transfer.workerName,
    status: transfer.status,
    phase: transfer.status === "hashing" ? "hashing local SIF" : "uploading over Tailscale SSH",
    local_path: transfer.localPath,
    remote_path: transfer.remotePath,
    temp_path: transfer.tmpPath,
    total_bytes: transfer.totalBytes,
    processed_bytes: bytes,
    progress_percent: Math.min(100, Number(((bytes / transfer.totalBytes) * 100).toFixed(1))),
    throughput_mib_per_second: Number((bytes / 1024 / 1024 / elapsedSeconds).toFixed(2)),
    elapsed_seconds: Number(elapsedSeconds.toFixed(1)),
    timeout_seconds: Math.round(transfer.timeoutMs / 1000),
    sha256: transfer.sha256,
    stderr: transfer.stderr || undefined,
    error: transfer.error,
    return_code: transfer.returnCode,
  };
}

function appendStderr(transfer: DeployTransfer, chunk: unknown): void {
  // Keep the status payload bounded even when ssh prints a noisy failure.
  transfer.stderr = (transfer.stderr + String(chunk)).slice(-8_000);
}

async function cleanupRemoteTemp(transfer: DeployTransfer): Promise<void> {
  const { spawn } = await import("node:child_process");
  const child = spawn(
    "tailscale",
    ["ssh", transfer.sshTarget, `rm -f ${shellQuote(transfer.tmpPath)}`],
    { stdio: "ignore" },
  );
  child.unref();
}

function abortTransfer(transfer: DeployTransfer, reason: string, cancelled: boolean): void {
  if (["completed", "failed", "cancelled"].includes(transfer.status)) return;
  transfer.cancelRequested = cancelled;
  transfer.error = reason;
  if (cancelled) transfer.status = "cancelling";
  transfer.hashStream?.destroy(new Error(reason));
  transfer.inputStream?.destroy(new Error(reason));
  transfer.child?.kill("SIGTERM");
  const child = transfer.child;
  if (child) {
    const killTimer = setTimeout(() => child.kill("SIGKILL"), 5_000);
    killTimer.unref();
  }
  // The remote command also verifies byte count and SHA-256 before mv.
  // This best-effort cleanup is for an interrupted transfer's unique temp file.
  void cleanupRemoteTemp(transfer).catch(() => undefined);
}

async function hashLocalFile(transfer: DeployTransfer): Promise<string> {
  const { createReadStream } = await import("node:fs");
  const { createHash } = await import("node:crypto");
  return new Promise((resolve, reject) => {
    const hash = createHash("sha256");
    const input = createReadStream(transfer.localPath);
    transfer.hashStream = input;
    input.on("data", (chunk: Buffer) => {
      transfer.bytesHashed += chunk.length;
      hash.update(chunk);
    });
    input.on("error", reject);
    input.on("end", () => resolve(hash.digest("hex")));
  });
}

async function launchTransfer(transfer: DeployTransfer): Promise<void> {
  const { spawn } = await import("node:child_process");
  const { createReadStream } = await import("node:fs");

  if (transfer.cancelRequested) {
    finishTransfer(transfer, "cancelled", transfer.error ?? "cancelled before upload");
    return;
  }

  const remoteCommand = [
    "set -eu",
    `mkdir -p ${shellQuote(transfer.remotePath.slice(0, transfer.remotePath.lastIndexOf("/")))}`,
    `tmp=${shellQuote(transfer.tmpPath)}`,
    "rm -f \"$tmp\"",
    "cat > \"$tmp\"",
    `test \"$(wc -c < \"$tmp\" | tr -d '[:space:]')\" = ${shellQuote(String(transfer.totalBytes))}`,
    `test \"$(sha256sum \"$tmp\" | awk '{print $1}')\" = ${shellQuote(transfer.sha256 ?? "")}`,
    `mv -f \"$tmp\" ${shellQuote(transfer.remotePath)}`,
  ].join(" && ");

  const child = spawn(
    "tailscale",
    ["ssh", transfer.sshTarget, remoteCommand],
    { stdio: ["pipe", "ignore", "pipe"] },
  );
  transfer.status = "transferring";
  transfer.transferStartedAt = Date.now();
  transfer.child = child;
  transfer.timeoutHandle = setTimeout(() => {
    abortTransfer(transfer, `transfer timed out after ${Math.round(transfer.timeoutMs / 60_000)} minutes`, false);
  }, transfer.timeoutMs);

  child.stderr.on("data", (chunk: unknown) => appendStderr(transfer, chunk));
  child.on("error", (err: Error) => {
    finishTransfer(transfer, transfer.cancelRequested ? "cancelled" : "failed", err.message);
  });
  child.on("close", (code: number | null) => {
    transfer.returnCode = code;
    if (transfer.cancelRequested) {
      finishTransfer(transfer, "cancelled", transfer.error ?? "cancelled");
    } else if (code === 0 && transfer.bytesTransferred === transfer.totalBytes) {
      finishTransfer(transfer, "completed");
    } else {
      finishTransfer(
        transfer,
        "failed",
        transfer.error ?? `tailscale ssh exited with ${code ?? "unknown status"}`,
      );
      void cleanupRemoteTemp(transfer).catch(() => undefined);
    }
  });

  const input = createReadStream(transfer.localPath);
  transfer.inputStream = input;
  input.on("data", (chunk: Buffer) => {
    transfer.bytesTransferred += chunk.length;
  });
  input.on("error", (err: Error) => {
    if (!transfer.cancelRequested) abortTransfer(transfer, `local file read failed: ${err.message}`, false);
  });
  child.stdin.on("error", (err: Error) => {
    if (!transfer.cancelRequested) abortTransfer(transfer, `SSH stdin failed: ${err.message}`, false);
  });
  input.pipe(child.stdin);
}

async function startDeployTransfer(params: {
  workerId: string;
  workerName: string;
  sshTarget: string;
  localPath: string;
  totalBytes: number;
}): Promise<DeployTransfer> {
  const { randomUUID } = await import("node:crypto");
  const id = randomUUID();
  const remotePath = "/var/lib/worker-harness/harness/new-image.sif";
  const transfer: DeployTransfer = {
    id,
    workerId: params.workerId,
    workerName: params.workerName,
    sshTarget: params.sshTarget,
    localPath: params.localPath,
    remotePath,
    tmpPath: `${remotePath}.${id}.tmp`,
    totalBytes: params.totalBytes,
    // Ten minutes per GiB, with a ten-minute minimum. The previous code
    // accidentally applied this multiplier per MiB, yielding ~32 days for
    // a 4.6 GiB SIF.
    timeoutMs: Math.max(MIN_DEPLOY_TIMEOUT_MS, Math.ceil(params.totalBytes / GIB) * DEPLOY_TIMEOUT_PER_GIB_MS),
    status: "hashing",
    startedAt: Date.now(),
    bytesHashed: 0,
    bytesTransferred: 0,
    stderr: "",
    cancelRequested: false,
  };
  deployTransfers.set(id, transfer);

  void (async () => {
    try {
      transfer.sha256 = await hashLocalFile(transfer);
      if (transfer.cancelRequested) {
        finishTransfer(transfer, "cancelled", transfer.error ?? "cancelled while hashing");
        return;
      }
      await launchTransfer(transfer);
    } catch (err) {
      finishTransfer(
        transfer,
        transfer.cancelRequested ? "cancelled" : "failed",
        err instanceof Error ? err.message : String(err),
      );
    }
  })();

  return transfer;
}

export function registerAdminTools(
  pi: ExtensionAPI,
) {
  pi.registerTool({
    name: "wh_admin_deploy_image",
    label: "Deploy Worker Image",
    description:
      "Deploy a local `.sif` image to a worker through an asynchronous checksummed transfer and atomic swap/restart. Returns `transfer_id`; inspect with `wh_admin_deploy_status` or cancel with `wh_admin_deploy_cancel` when those tools are available. The live image remains unchanged unless byte count and SHA-256 match.",
    parameters: Type.Object({
      worker_id: Type.String({
        description: "Worker ID (UUID) or name to deploy to",
      }),
      image_path: Type.String({
        description: "Local path to the .sif image file to deploy",
      }),
    }),
    async execute(_toolCallId, { worker_id, image_path }) {
      try {
        const { existsSync, statSync } = await import("node:fs");
        if (!existsSync(image_path)) {
          throw new Error(`Image file not found: ${image_path}`);
        }

        const stats = statSync(image_path);
        const sizeMB = Math.round(stats.size / (1024 * 1024));

        const worker = await getWorker(worker_id);
        const sshUser = (worker as any).ssh_user ?? "root";
        const sshHost = (worker as any).ssh_host ?? (worker as any).worker_ip ?? worker_id;
        const workerName = (worker as any).name ?? worker_id;
        const transfer = await startDeployTransfer({
          workerId: worker_id,
          workerName,
          sshTarget: `${sshUser}@${sshHost}`,
          localPath: image_path,
          totalBytes: stats.size,
        });

        return {
          content: [
            {
              type: "text",
              text:
                `Started ${sizeMB}MB SIF deployment to ${workerName} (transfer_id: ${transfer.id}).\n` +
                `It is hashing locally, then will upload over Tailscale SSH. The remote path unit is triggered only after remote byte-count and SHA-256 verification succeeds.\n` +
                `Monitor with wh_admin_deploy_status({ transfer_id: "${transfer.id}" }).`,
            },
          ],
          details: deploymentDetails(transfer),
        };
      } catch (err) {
        throw toToolError(err);
      }
    },
  });

  pi.registerTool({
    name: "wh_admin_deploy_status",
    label: "Worker Image Deployment Status",
    description:
      "Show progress or final status for asynchronous SIF deployments started by wh_admin_deploy_image. Omit transfer_id to list recent deployments in this extension process.",
    parameters: Type.Object({
      transfer_id: Type.Optional(Type.String({
        description: "Deployment transfer ID returned by wh_admin_deploy_image",
      })),
    }),
    async execute(_toolCallId, { transfer_id }) {
      try {
        if (transfer_id) {
          const transfer = deployTransfers.get(transfer_id);
          if (!transfer) throw new Error(`Unknown deployment transfer: ${transfer_id}`);
          const details = deploymentDetails(transfer);
          return {
            content: [{
              type: "text",
              text:
                `${details.worker_name}: ${details.status} (${details.phase})\n` +
                `${details.progress_percent}% — ${details.processed_bytes}/${details.total_bytes} bytes ` +
                `at ${details.throughput_mib_per_second} MiB/s\n` +
                (details.error ? `Error: ${details.error}\n` : "") +
                (details.stderr ? `SSH stderr: ${details.stderr}` : ""),
            }],
            details,
          };
        }

        const details = [...deployTransfers.values()]
          .sort((a, b) => b.startedAt - a.startedAt)
          .slice(0, 20)
          .map(deploymentDetails);
        return {
          content: [{
            type: "text",
            text: details.length
              ? details.map((item) => `${item.transfer_id} ${item.worker_name}: ${item.status} ${item.progress_percent}%`).join("\n")
              : "No deployment transfers are tracked in this extension process.",
          }],
          details: { transfers: details },
        };
      } catch (err) {
        throw toToolError(err);
      }
    },
  });

  pi.registerTool({
    name: "wh_admin_deploy_cancel",
    label: "Cancel Worker Image Deployment",
    description:
      "Cancel an in-progress SIF deployment started by wh_admin_deploy_image. The unique remote temporary file is cleaned up on a best-effort basis; the live image path is never modified by cancellation.",
    parameters: Type.Object({
      transfer_id: Type.String({
        description: "Deployment transfer ID returned by wh_admin_deploy_image",
      }),
    }),
    async execute(_toolCallId, { transfer_id }) {
      try {
        const transfer = deployTransfers.get(transfer_id);
        if (!transfer) throw new Error(`Unknown deployment transfer: ${transfer_id}`);
        if (["completed", "failed", "cancelled"].includes(transfer.status)) {
          return {
            content: [{ type: "text", text: `Transfer ${transfer_id} is already ${transfer.status}.` }],
            details: deploymentDetails(transfer),
          };
        }
        abortTransfer(transfer, "cancelled by operator", true);
        return {
          content: [{
            type: "text",
            text: `Cancellation requested for ${transfer.workerName} (${transfer_id}). Use wh_admin_deploy_status to confirm cleanup.`,
          }],
          details: deploymentDetails(transfer),
        };
      } catch (err) {
        throw toToolError(err);
      }
    },
  });

  pi.registerTool({
    name: "wh_admin_restart",
    label: "Restart Worker",
    description:
      "Restart a worker container through the harness service. Use after configuration changes or to recover a stuck worker; the worker is briefly unavailable. Do not call after `wh_admin_deploy_image`, which already swaps and restarts the worker.",
    parameters: Type.Object({
      worker_id: Type.String({
        description: "Worker ID (UUID) or name to restart",
      }),
    }),
    async execute(_toolCallId, { worker_id }) {
      try {
        const worker = await getWorker(worker_id);
        const workerName = (worker as any).name ?? worker_id;

        // Write a trigger file to the bind-mounted harness dir.
        // The systemd path unit on the host watches for this file
        // and restarts worker-harness.service.
        const triggerPath = "/var/lib/worker-harness/harness/restart-trigger";

        // Use async job (not sync) because the container will be killed
        // during restart — a sync job would hang.
        await startJob({
          worker_id,
          command: `touch '${triggerPath}'`,
          name: "restart-trigger",
          no_pty: true,
        });

        events.emit("worker-harness:refresh", undefined);

        return {
          content: [
            {
              type: "text",
              text:
                `Restart triggered for ${workerName}.\n` +
                `The worker will restart momentarily. Monitor with: wh_read({ action: "list_workers" }) to confirm it comes back online.`,
            },
          ],
          details: {
            worker_id,
            worker_name: workerName,
            triggered: true,
          },
        };
      } catch (err) {
        throw toToolError(err);
      }
    },
  });
}
