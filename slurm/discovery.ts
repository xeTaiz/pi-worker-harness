import { posix } from "node:path";
import { shellQuote, sshTransport, type SshConnection } from "./client.ts";
import { DEFAULT_ALLOCATION_WALLTIME_SECONDS, type SharedRoot } from "./profiles.ts";
import { parseInventory, sizeResources, type ResourceLimits, type ResourceProfile } from "./resources.ts";

export const SLURM_RUNTIME_LABEL = "org.worker-harness.slurm-runtime";
const token = /^[A-Za-z0-9][A-Za-z0-9_.-]*$/;
const sourceLimit = 512 * 1024;
const evidenceLimit = 16 * 1024;
export type DiscoveryEvidenceKind = "config" | "partitions" | "nodes" | "membership" | "default_account" | "associations" | "qos" | "runtime" | "modules" | "image";
export interface DiscoveryEvidence {
  kind: DiscoveryEvidenceKind;
  status: "ok" | "unavailable" | "invalid";
  output: string;
}
export interface DiscoveryIssue {
  kind: "scheduler" | "billing" | "runtime" | "image" | "inventory" | "partition";
  message: string;
  blocking: boolean;
  partition?: string;
}
export interface DiscoveryTarget {
  partition: string;
  compute_kind: "cpu" | "gpu";
  gpu_type?: string;
  max_gpus?: number;
  max_walltime_seconds: number;
  limits: ResourceLimits;
  defaults: { cpus: number; memory_mb: number };
  review_required: boolean;
  permission: "unverified_until_submission";
}
export interface DiscoveryResult {
  observed_at: string;
  cluster_name?: string;
  account?: string;
  account_source: "explicit" | "default" | "scheduler";
  accounts: string[];
  qos: string[];
  runtime?: { executable: "apptainer" | "singularity"; version: string; prelude: string };
  sif_path?: string;
  targets: DiscoveryTarget[];
  unresolved: DiscoveryIssue[];
  evidence: DiscoveryEvidence[];
}
export interface DiscoveryInput {
  mappings: SharedRoot[];
  sif_path?: string;
  runtime_setup?: string;
  account?: string;
}
export interface DiscoveryOptions {
  transport?: typeof sshTransport;
  signal?: AbortSignal;
  now?: () => Date;
}
/** Complete bounded command observations; useful for deterministic, offline parsing. */
export interface DiscoverySource {
  commands: DiscoveryEvidence[];
}

function message(error: unknown): string {
  return (error instanceof Error ? error.message : String(error)).slice(0, evidenceLimit);
}
function safePath(path: string): boolean {
  return posix.isAbsolute(path) && path !== "/" && posix.normalize(path) === path
    && !/[\u0000-\u001f\u007f:,]/.test(path) && !path.split("/").some((part) => part === "." || part === "..");
}
/** Only explicit module loads, never arbitrary shell text or module-list output. */
export function normalizeRuntimeSetup(value = ""): string {
  const text = value.trim();
  if (!text) return "";
  const prefix = "export PATH=", suffix = ':"$PATH"';
  if (text.startsWith(prefix) && text.endsWith(suffix)) {
    const quoted = text.slice(prefix.length, -suffix.length);
    const path = quoted.slice(1, -1).replaceAll("'\\''", "'");
    if (safePath(path) && shellQuote(path) === quoted) return text;
    throw new Error("Runtime PATH setup must contain one safely quoted absolute directory");
  }
  if (!/^module[ \t]+load(?:[ \t]+[A-Za-z0-9][A-Za-z0-9_./+-]*)+$/.test(text)) {
    throw new Error("Runtime setup must be a literal module load or a safely quoted runtime PATH export");
  }
  return "module load " + text.split(/[ \t]+/).slice(2).join(" ");
}
function validateInput(input: DiscoveryInput): void {
  if (!input.mappings.length) throw new Error("Discovery requires a primary filesystem mapping");
  const ids = new Set<string>();
  for (const mapping of input.mappings) {
    if (!token.test(mapping.storage_id) || ids.has(mapping.storage_id) || !safePath(mapping.shared_root)) {
      throw new Error("Discovery requires unique storage IDs and normalized safe absolute roots");
    }
    ids.add(mapping.storage_id);
  }
  if (input.sif_path !== undefined && !safePath(input.sif_path)) throw new Error("Image path must be a normalized safe absolute path");
  if (input.account !== undefined && !token.test(input.account)) throw new Error("Account must be a literal Slurm account name");
  normalizeRuntimeSetup(input.runtime_setup);
}
function namedFields(line: string): Record<string, string> {
  const matches = [...line.matchAll(/(?:^|\s)([A-Za-z][A-Za-z0-9_]*)=/g)];
  if (!matches.length || line.slice(0, matches[0]!.index).trim()) throw new Error("Expected named Slurm partition fields");
  const fields: Record<string, string> = {};
  for (let index = 0; index < matches.length; index++) {
    const match = matches[index]!;
    if (Object.hasOwn(fields, match[1]!)) throw new Error("Duplicate Slurm partition field");
    fields[match[1]!] = line.slice(match.index! + match[0].length, matches[index + 1]?.index ?? line.length).trim();
  }
  return fields;
}
function lines(output: string): string[] {
  const result = output.split(/\r?\n/).map((line) => line.trim()).filter(Boolean);
  if (result.length > 8192) throw new Error("Discovery record limit exceeded");
  return result;
}
function runtimeFrom(output: string): DiscoveryResult["runtime"] {
  const rows = output.trim().split(/\r?\n/);
  if (rows.length !== 4 || !/^(apptainer|singularity)$/.test(rows[0]!) || rows[3] !== "WH_BATCH_RUNTIME_OK") return undefined;
  const executable = rows[0] as "apptainer" | "singularity";
  const version = rows[1]!, path = rows[2]!;
  if (!safePath(path) || version.length > 256
    || !new RegExp(`^${executable}(?:-ce)?\\s+(?:version\\s+)?[0-9]+\\.[0-9]+`, "i").test(version)) return undefined;
  const directory = posix.dirname(path);
  const prelude = ["/usr/local/bin", "/usr/bin", "/bin"].includes(directory)
    ? "" : `export PATH=${shellQuote(directory)}:"$PATH"`;
  return { executable, version, prelude };
}
export function parseModuleCandidates(output: string): string[] {
  if (output.length > sourceLimit || output.split(/\r?\n/).length > 8192) return [];
  const result = new Set<string>();
  for (const line of lines(output)) {
    for (const item of line.replace(/\u001b\[[0-9;]*m/g, "").split(/\s+/)) {
      const name = item.replace(/\((?:D|L|default)\)$/i, "");
      if (/^(?:apptainer|singularity)(?:\/[A-Za-z0-9][A-Za-z0-9_.+-]*)?$/.test(name)) result.add(name);
    }
  }
  return [...result].sort().slice(0, 4);
}
export function hasCompatibleImageLabel(output: string): boolean {
  try {
    const parsed = JSON.parse(output);
    // Apptainer/Singularity inspect --json nests labels under data.attributes.
    return parsed?.data?.attributes?.labels?.[SLURM_RUNTIME_LABEL] === "1";
  } catch { return false; }
}

/** Parse scheduler observations, not a claim of permission or current availability. */
export function parseDiscovery(source: DiscoverySource, input: DiscoveryInput, observedAt: string): DiscoveryResult {
  validateInput(input);
  if (!Number.isFinite(Date.parse(observedAt))) throw new Error("Invalid discovery timestamp");
  const commands = new Map<DiscoveryEvidenceKind, DiscoveryEvidence>();
  for (const observation of source.commands) {
    if (commands.has(observation.kind)) throw new Error(`Duplicate discovery observation ${observation.kind}`);
    commands.set(observation.kind, observation.output.length > sourceLimit || observation.output.split(/\r?\n/).length > 8192
      ? { kind: observation.kind, status: "invalid", output: "Discovery command exceeded output or record bound" } : observation);
  }
  const result: DiscoveryResult = {
    observed_at: observedAt, account_source: input.account === undefined ? "scheduler" : "explicit",
    ...(input.account === undefined ? {} : { account: input.account }),
    accounts: [], qos: [], targets: [], unresolved: [],
    evidence: [...commands.values()].map((entry) => ({ ...entry, output: entry.output.slice(0, evidenceLimit) })),
  };
  const get = (kind: DiscoveryEvidenceKind): string | undefined => {
    const entry = commands.get(kind);
    return entry?.status === "ok" ? entry.output : undefined;
  };
  const issue = (kind: DiscoveryIssue["kind"], text: string, blocking: boolean, partition?: string) => {
    result.unresolved.push({ kind, message: text, blocking, ...(partition === undefined ? {} : { partition }) });
  };
  const cluster = get("config")?.match(/^\s*ClusterName\s*=\s*([A-Za-z0-9][A-Za-z0-9_.-]*)\s*$/m)?.[1];
  if (cluster) result.cluster_name = cluster;
  const associations = get("associations");
  const accountNames = new Set<string>();
  const qosNames = new Set<string>();
  if (associations !== undefined) {
    for (const row of lines(associations)) {
      const [associationCluster, account, , qos] = row.split("|");
      if (cluster && associationCluster !== cluster) continue;
      // Without cluster identity an association from another cluster is not usable evidence.
      if (!cluster || !account || !token.test(account)) continue;
      accountNames.add(account);
      for (const name of qos?.split(",") ?? []) if (token.test(name)) qosNames.add(name);
    }
  }
  result.accounts = [...accountNames].sort();
  for (const row of lines(get("qos") ?? "")) {
    const name = row.split("|")[0]!;
    if (token.test(name)) qosNames.add(name);
  }
  result.qos = [...qosNames].sort();
  if (input.account === undefined) {
    const defaults = new Set(lines(get("default_account") ?? "").map((row) => row.split("|")[1]).filter((name): name is string => !!name && token.test(name)));
    if (defaults.size === 1) {
      result.account = [...defaults][0]!;
      result.account_source = "default";
    } else if (defaults.size > 1 || result.accounts.length > 1) {
      issue("billing", "No unique default billing account was established. Choose explicitly if your site requires one, or delegate account selection to Slurm; visible associations do not establish permission.", false);
    } else {
      issue("billing", "Default account is unavailable; account will be omitted so Slurm selects its default. Account and QOS permissions remain unverified until submission.", false);
    }
  }
  const rawPartitions = get("partitions"), nodes = get("nodes"), membership = get("membership");
  if (rawPartitions === undefined || nodes === undefined || membership === undefined) {
    issue("scheduler", "Readonly Slurm partition/node inventory is unavailable; inspect the bounded command evidence.", true);
  } else {
    try {
      const seen = new Set<string>();
      for (const row of lines(rawPartitions)) {
        const fields = namedFields(row), partition = fields.PartitionName;
        if (!partition || !token.test(partition) || seen.has(partition)) throw new Error("Invalid or duplicate partition name");
        seen.add(partition);
        if (fields.State !== "UP") {
          issue("partition", "Partition is not UP; no target generated.", false, partition);
          continue;
        }
        const review = /^(?:YES|1)$/i.test(fields.Hidden ?? "") || /^(?:YES|1)$/i.test(fields.RootOnly ?? "")
          || /^(?:YES|1)$/i.test(fields.ReqResv ?? "") || /(?:premium|debug|interactive|reservation|special)/i.test(partition)
          || ["AllowAccounts", "AllowGroups", "AllowQos"].some((key) => fields[key] !== undefined && !/^(?:ALL|\(null\))$/i.test(fields[key]!))
          || ["DenyAccounts", "DenyQos"].some((key) => fields[key] !== undefined && !/^(?:NONE|\(null\))$/i.test(fields[key]!))
          || (fields.QoS !== undefined && !/^(?:N\/A|NONE|\(null\))$/i.test(fields.QoS));
        try {
          const cpuProfile: ResourceProfile = { id: partition, partition, compute_kind: "cpu" };
          const inventorySource = { sinfo: membership, nodes, partition: row, observed_at: observedAt };
          const inventory = parseInventory(cpuProfile, inventorySource);
          const ceiling = Math.min(DEFAULT_ALLOCATION_WALLTIME_SECONDS, inventory.limits.max_walltime_seconds ?? DEFAULT_ALLOCATION_WALLTIME_SECONDS);
          if (ceiling < 1) throw new Error("Partition has no positive allocation duration");
          const appendTarget = (profile: ResourceProfile) => {
            const sized = sizeResources({ ...profile, max_walltime_seconds: ceiling }, inventory, { walltime_seconds: ceiling });
            result.targets.push({
              partition, compute_kind: profile.compute_kind,
              ...(profile.compute_kind === "gpu" ? { gpu_type: profile.gpu_type, max_gpus: profile.max_gpus } : {}),
              max_walltime_seconds: ceiling, limits: inventory.limits,
              defaults: { cpus: sized.cpus, memory_mb: sized.memory_mb },
              review_required: review, permission: "unverified_until_submission",
            });
          };
          appendTarget(cpuProfile);
          const types = [...new Set(inventory.nodes.flatMap((node) => Object.keys(node.gpus)))].sort();
          for (const gpuType of types) {
            const maxGpus = Math.max(...inventory.nodes.map((node) => node.gpus[gpuType] ?? 0));
            appendTarget({ id: `${partition}-${gpuType}`, partition, compute_kind: "gpu", gpu_type: gpuType, max_gpus: maxGpus });
          }
          if (review) issue("partition", "Visible restricted or special partition requires explicit review; account/QOS access and scheduling are not guaranteed.", false, partition);
        } catch (error) { issue("inventory", message(error), false, partition); }
      }
    } catch (error) { issue("scheduler", message(error), true); }
    if (!result.targets.length) issue("scheduler", "No supported CPU or exact physical-GPU target could be established.", true);
  }
  const runtimeOutput = get("runtime");
  if (runtimeOutput !== undefined) result.runtime = runtimeFrom(runtimeOutput);
  if (!result.runtime) issue("runtime", "No Apptainer/Singularity version working in the allocation's clean environment with python3/flock reachable was established. Supply a confirmed module load or supported runtime. Module initialization/library dependencies cannot be guessed; no containers were executed.", true);
  const imageOutput = get("image");
  if (result.runtime && imageOutput !== undefined && hasCompatibleImageLabel(imageOutput)) {
    result.sif_path = input.sif_path ?? posix.join(input.mappings[0]!.shared_root, "worker-harness-worker.sif");
  } else {
    issue("image", `A readable compatible SIF with static label ${SLURM_RUNTIME_LABEL}=1 was not established. Supply a compatible image path; an old filename alone is not proof.`, true);
  }
  return result;
}

const runtimeCommand = `for wh_runtime in apptainer singularity; do
  wh_path=$(command -v "$wh_runtime") || continue
  case "$wh_path" in /*) ;; *) continue ;; esac
  wh_version=$("$wh_path" --version) || continue
  wh_bindir=\${wh_path%/*}
  /usr/bin/env -i HOME="\${HOME:-/}" PATH="$wh_bindir:/usr/local/bin:/usr/bin:/bin" /bin/bash -c \\
    'command -v python3 >/dev/null && command -v flock >/dev/null && "$1" --version >/dev/null' _ "$wh_path" || continue
  printf '%s\\n%s\\n%s\\nWH_BATCH_RUNTIME_OK\\n' "$wh_runtime" "$wh_version" "$wh_path"
  exit 0
done
exit 1`;

function withRuntimeSetup(prelude: string, command: string): string {
  if (!prelude) return command;
  if (prelude.startsWith("export PATH=")) return prelude + "\n" + command;
  return "module load " + prelude.split(" ").slice(2).map(shellQuote).join(" ") + " >/dev/null &&\n" + command;
}

/** Strict SSH transport only; no allocations, container execution, filesystem scans or services. */
export async function discoverCluster(connection: SshConnection, input: DiscoveryInput, options: DiscoveryOptions = {}): Promise<DiscoveryResult> {
  validateInput(input);
  const observedAt = (options.now?.() ?? new Date()).toISOString();
  const transport = options.transport ?? sshTransport;
  const commands: DiscoveryEvidence[] = [];
  const collect = async (kind: DiscoveryEvidenceKind, script: string): Promise<DiscoveryEvidence> => {
    options.signal?.throwIfAborted();
    let entry: DiscoveryEvidence;
    try {
      const output = await transport(connection, script, "", { timeout_ms: 20_000, max_bytes: sourceLimit, signal: options.signal });
      entry = output.length > sourceLimit ? { kind, status: "invalid", output: "Discovery command exceeded output bound" } : { kind, status: "ok", output };
    } catch (error) {
      options.signal?.throwIfAborted();
      entry = { kind, status: "unavailable", output: message(error) };
    }
    commands.push(entry);
    return entry;
  };
  await collect("config", "LC_ALL=C scontrol show config");
  await collect("partitions", "LC_ALL=C scontrol --oneliner show partitions");
  await collect("membership", "LC_ALL=C sinfo --Node --noheader --format='NodeName=%N Partition=%P State=%T'");
  await collect("nodes", "LC_ALL=C scontrol --oneliner show nodes");
  await collect("default_account", 'LC_ALL=C sacctmgr --noheader --parsable2 show user where name="$(id -un)" format=User,DefaultAccount');
  await collect("associations", 'LC_ALL=C sacctmgr --noheader --parsable2 show association where user="$(id -un)" format=Cluster,Account,Partition,QOS,DefaultQOS');
  await collect("qos", "LC_ALL=C sacctmgr --noheader --parsable2 show qos format=Name");
  let prelude = normalizeRuntimeSetup(input.runtime_setup);
  let runtime = await collect("runtime", withRuntimeSetup(prelude, runtimeCommand));
  if (runtime.status !== "ok" || !runtimeFrom(runtime.output)) {
    const modules = await collect("modules", "module -t avail apptainer singularity 2>&1");
    if (!prelude && modules.status === "ok") {
      for (const candidate of parseModuleCandidates(modules.output)) {
        // Replace failed runtime evidence; each attempt has the same strict bounds.
        commands.splice(commands.findIndex((entry) => entry.kind === "runtime"), 1);
        prelude = normalizeRuntimeSetup(`module load ${candidate}`);
        runtime = await collect("runtime", withRuntimeSetup(prelude, runtimeCommand));
        if (runtime.status === "ok" && runtimeFrom(runtime.output)) break;
      }
    }
  }
  const found = runtime.status === "ok" ? runtimeFrom(runtime.output) : undefined;
  if (found) {
    const path = input.sif_path ?? posix.join(input.mappings[0]!.shared_root, "worker-harness-worker.sif");
    const enginePath = runtime.output.trim().split(/\r?\n/)[2]!;
    await collect("image", withRuntimeSetup(found.prelude,
      `test -f ${shellQuote(path)} && test -r ${shellQuote(path)} && ${shellQuote(enginePath)} inspect --json ${shellQuote(path)}`));
  }
  return parseDiscovery({ commands }, input, observedAt);
}
