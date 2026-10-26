import { readFile } from "node:fs/promises";
import { isIP } from "node:net";
import { join, posix, resolve } from "node:path";

interface ClusterProfileBase {
  id: string;
  description: string;
  ssh_host: string;
  ssh_config_file?: string;
  storage_id: string;
  shared_root: string;
  sif_path: string;
  runtime: "apptainer" | "singularity";
  /** Trusted, operator-authored shell prelude; never populated from job input. */
  runtime_setup: string;
  account: string;
  partition: string;
  qos?: string;
  constraint?: string;
  max_walltime_seconds: number;
  max_cpus?: number;
  max_memory_mb?: number;
  failure_grace_seconds: number;
  warning_seconds: number;
  termination_grace_seconds: number;
  tailnet_key_file: string;
  tailnet_login_server: string;
  orchestrator_host: string;
  orchestrator_registration_port: number;
  poll_seconds: number;
}

export type ClusterProfile = ClusterProfileBase & (
  | { compute_kind: "gpu"; gpu_type: string; max_gpus: number }
  | { compute_kind: "cpu"; gpu_type?: never; max_gpus: 0 }
);

export type ClusterConfigErrorCode = "CLUSTER_CONFIG_INVALID" | "CLUSTER_CONFIG_READ_FAILED";

export class ClusterConfigError extends Error {
  readonly name = "ClusterConfigError";

  constructor(
    readonly code: ClusterConfigErrorCode,
    readonly configPath: string | undefined,
    readonly field: string | undefined,
    detail: string,
    options?: ErrorOptions,
  ) {
    super(`Cluster configuration${configPath ? ` ${configPath}` : ""}${field ? ` (${field})` : ""}: ${detail}`, options);
  }
}

export type Environment = Readonly<Record<string, string | undefined>>;
type JsonObject = Record<string, unknown>;
const identifier = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;
const slurmToken = /^[A-Za-z0-9][A-Za-z0-9_.-]*$/;
const controlCharacter = /[\u0000-\u001f\u007f]/;
const profileFields: Readonly<Record<string, true>> = {
  id: true, description: true, ssh_host: true, ssh_config_file: true, storage_id: true, shared_root: true, sif_path: true,
  compute_kind: true, gpu_type: true, runtime: true, runtime_setup: true, account: true, partition: true,
  qos: true, constraint: true, max_gpus: true, max_walltime_seconds: true, max_cpus: true, max_memory_mb: true,
  failure_grace_seconds: true, warning_seconds: true, termination_grace_seconds: true, tailnet_key_file: true,
  tailnet_login_server: true, orchestrator_host: true, orchestrator_registration_port: true, poll_seconds: true,
};

/** Container path shared by every partition profile on the same filesystem. */
export function sharedPath(profile: ClusterProfile): string {
  return `/data/shared/${profile.storage_id}`;
}

/** Resolve the operator-selected configuration file without reading it. */
export function resolveClusterConfigPath(env: Environment = process.env): string {
  if (env.WH_CLUSTER_CONFIG !== undefined) {
    if (!env.WH_CLUSTER_CONFIG.trim() || controlCharacter.test(env.WH_CLUSTER_CONFIG)) {
      throw new ClusterConfigError("CLUSTER_CONFIG_INVALID", undefined, "WH_CLUSTER_CONFIG", "expected a nonempty file path");
    }
    return resolve(env.WH_CLUSTER_CONFIG);
  } else {
    const base = env.XDG_CONFIG_HOME || (env.HOME ? join(env.HOME, ".config") : undefined);
    if (!base || !posix.isAbsolute(base) || controlCharacter.test(base)) {
      throw new ClusterConfigError("CLUSTER_CONFIG_INVALID", undefined, "XDG_CONFIG_HOME/HOME", "an absolute configuration directory is required");
    }
    return join(base, "worker-harness", "clusters.json");
  }
}

/** Missing files mean no targets; malformed or unreadable files are never ignored. */
export async function loadClusterProfiles(env: Environment = process.env): Promise<ClusterProfile[]> {
  const configPath = resolveClusterConfigPath(env);
  let raw: string;
  try {
    raw = await readFile(configPath, "utf8");
  } catch (error) {
    if (error && typeof error === "object" && "code" in error && error.code === "ENOENT") return [];
    throw new ClusterConfigError("CLUSTER_CONFIG_READ_FAILED", configPath, undefined, "could not read configuration", { cause: error });
  }
  let document: unknown;
  try {
    document = JSON.parse(raw);
  } catch (error) {
    throw new ClusterConfigError("CLUSTER_CONFIG_INVALID", configPath, undefined, "expected valid JSON", { cause: error });
  }
  return parseClusterProfiles(document, configPath);
}

/** Validate a complete versioned document using the same policy as the file loader. */
export function parseClusterProfiles(document: unknown, configPath?: string): ClusterProfile[] {
  const invalid = (field: string, detail: string): never => {
    throw new ClusterConfigError("CLUSTER_CONFIG_INVALID", configPath, field, detail);
  };

  function object(value: unknown, field: string): JsonObject {
    if (!value || typeof value !== "object" || Array.isArray(value)) invalid(field, "expected an object");
    return value as JsonObject;
  }
  function text(value: unknown, field: string): string {
    if (typeof value !== "string" || !value.trim() || controlCharacter.test(value)) {
      invalid(field, "expected a nonempty string without control characters");
    }
    return value as string;
  }
  function token(value: unknown, field: string, pattern = slurmToken): string {
    const result = text(value, field);
    if (!pattern.test(result)) invalid(field, "contains unsupported characters");
    return result;
  }
  function integer(value: unknown, field: string, minimum: number, maximum = Number.MAX_SAFE_INTEGER): number {
    if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
      invalid(field, `expected an integer from ${minimum} to ${maximum}`);
    }
    return value as number;
  }
  function path(value: unknown, field: string, bind = false): string {
    const result = text(value, field);
    if (!posix.isAbsolute(result) || result.split("/").some((part) => part === "." || part === "..")) {
      invalid(field, "expected an absolute POSIX path without traversal components");
    }
    if (bind && /[:,;]/.test(result)) invalid(field, "bind paths cannot contain colon, comma or semicolon");
    const normalized = posix.normalize(result).replace(/\/$/, "");
    if (!normalized) invalid(field, "the filesystem root is not a valid profile path");
    return normalized;
  }

  const root = object(document, "config");
  for (const key of Object.keys(root)) {
    if (key !== "version" && key !== "clusters") invalid(key, "unknown configuration field");
  }
  if (root.version !== 1) invalid("version", "expected version 1");
  if (!Array.isArray(root.clusters)) invalid("clusters", "expected an array");
  const ids = new Set<string>();
  const storageRoots = new Map<string, string>();
  return (root.clusters as unknown[]).map((entry, index): ClusterProfile => {
    const prefix = `clusters[${index}]`;
    const source = object(entry, prefix);
    for (const key of Object.keys(source)) {
      if (!Object.hasOwn(profileFields, key)) invalid(`${prefix}.${key}`, "unknown profile field");
    }
    const field = (key: string) => `${prefix}.${key}`;
    const id = token(source.id, field("id"), identifier);
    if (ids.has(id)) invalid(field("id"), `duplicate cluster ID ${id}`);
    ids.add(id);
    const storage_id = token(source.storage_id === undefined ? id : source.storage_id, field("storage_id"), identifier);
    const shared_root = path(source.shared_root, field("shared_root"), true);
    const previousRoot = storageRoots.get(storage_id);
    if (previousRoot !== undefined && previousRoot !== shared_root) {
      invalid(field("shared_root"), `storage_id ${storage_id} must use the same shared_root in every profile`);
    }
    storageRoots.set(storage_id, shared_root);
    const ssh_host = token(source.ssh_host, field("ssh_host"));
    if (source.runtime !== "apptainer" && source.runtime !== "singularity") invalid(field("runtime"), "expected apptainer or singularity");
    if (source.compute_kind !== "gpu" && source.compute_kind !== "cpu") invalid(field("compute_kind"), "expected gpu or cpu");
    const max_gpus = integer(source.max_gpus, field("max_gpus"), source.compute_kind === "cpu" ? 0 : 1);
    if (source.compute_kind === "cpu" && max_gpus !== 0) invalid(field("max_gpus"), "CPU profiles require exactly 0 GPUs");
    if (source.compute_kind === "cpu" && Object.hasOwn(source, "gpu_type")) invalid(field("gpu_type"), "CPU profiles must omit gpu_type");
    const runtime_setup = source.runtime_setup === undefined ? "" : source.runtime_setup;
    if (typeof runtime_setup !== "string" || runtime_setup.includes("\0")) invalid(field("runtime_setup"), "expected a trusted shell prelude without NUL bytes");
    const tailnet_login_server = text(source.tailnet_login_server, field("tailnet_login_server"));
    try {
      const url = new URL(tailnet_login_server);
      if (!["https:", "http:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error("invalid login URL");
    } catch {
      invalid(field("tailnet_login_server"), "expected an HTTP(S) URL without credentials, query or fragment");
    }
    const orchestrator_host = text(source.orchestrator_host, field("orchestrator_host"));
    if (!isIP(orchestrator_host) && !orchestrator_host.split(".").every((label) => /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/.test(label))) {
      invalid(field("orchestrator_host"), "expected a hostname or IP address, without a scheme or port");
    }
    const profile: ClusterProfileBase = {
      id,
      description: text(source.description, field("description")),
      ssh_host,
      storage_id,
      shared_root,
      sif_path: path(source.sif_path, field("sif_path")),
      runtime: source.runtime as ClusterProfileBase["runtime"],
      runtime_setup: runtime_setup as string,
      account: token(source.account, field("account")),
      partition: token(source.partition, field("partition")),
      max_walltime_seconds: integer(source.max_walltime_seconds, field("max_walltime_seconds"), 1),
      failure_grace_seconds: integer(source.failure_grace_seconds, field("failure_grace_seconds"), 0),
      warning_seconds: integer(source.warning_seconds === undefined ? 120 : source.warning_seconds, field("warning_seconds"), 0),
      termination_grace_seconds: integer(source.termination_grace_seconds, field("termination_grace_seconds"), 0),
      tailnet_key_file: path(source.tailnet_key_file, field("tailnet_key_file")),
      tailnet_login_server,
      orchestrator_host,
      orchestrator_registration_port: integer(source.orchestrator_registration_port === undefined ? 12888 : source.orchestrator_registration_port, field("orchestrator_registration_port"), 1, 65535),
      poll_seconds: integer(source.poll_seconds === undefined ? 15 : source.poll_seconds, field("poll_seconds"), 1),
    };
    if (source.ssh_config_file !== undefined) profile.ssh_config_file = path(source.ssh_config_file, field("ssh_config_file"));
    if (source.qos !== undefined) profile.qos = token(source.qos, field("qos"));
    // Eligibility parsing belongs to resource discovery; retain the configured expression here.
    if (source.constraint !== undefined) profile.constraint = text(source.constraint, field("constraint"));
    if (source.max_cpus !== undefined) profile.max_cpus = integer(source.max_cpus, field("max_cpus"), 1);
    if (source.max_memory_mb !== undefined) profile.max_memory_mb = integer(source.max_memory_mb, field("max_memory_mb"), 1);
    return source.compute_kind === "cpu"
      ? { ...profile, compute_kind: "cpu", max_gpus: 0 }
      : { ...profile, compute_kind: "gpu", max_gpus, gpu_type: token(source.gpu_type, field("gpu_type")) };
  });
}
