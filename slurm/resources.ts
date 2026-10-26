import type { ClusterProfile } from "./profiles.ts";

export class SlurmResourceError extends Error {
  readonly name = "SlurmResourceError";
  readonly code = "SLURM_RESOURCES_INVALID";
}

export interface InventorySource {
  /** sinfo --Node --noheader --partition=... --format='NodeName=%N Partition=%P State=%T' */
  sinfo: string;
  /** scontrol --oneliner show nodes */
  nodes: string;
  /** scontrol --oneliner show partition <partition> */
  partition: string;
  /** Optional, supplemental ginfo output; never used for sizing. */
  ginfo?: string;
  observed_at?: string;
}

export interface ResourceLimits {
  max_cpus?: number;
  max_memory_mb?: number;
  max_memory_per_cpu_mb?: number;
  max_walltime_seconds?: number;
}

export interface InventoryNode {
  name: string;
  partitions: string[];
  state: string;
  features: string[];
  cpus: number;
  memory_mb: number;
  reserved_memory_mb: number;
  /** All physical GPU types, not just the type requested by the profile. */
  gpus: Record<string, number>;
  total_gpus: number;
  allocated_cpus?: number;
  allocated_memory_mb?: number;
  allocated_gpus?: number;
}

export interface Inventory {
  observed_at: string;
  partition: string;
  /** Exact partition, feature and GPU-type matches, including busy nodes. */
  nodes: InventoryNode[];
  limits: ResourceLimits;
  availability: {
    total_nodes: number;
    idle_nodes: number;
    busy_nodes: number;
    unavailable_nodes: number;
    total_gpus: number;
    free_gpus?: number;
    free_cpus?: number;
    free_memory_mb?: number;
    note: string;
  };
  ginfo?: { output: string; truncated: boolean };
}

export interface ResourceRequest {
  gpu_count?: number;
  cpus?: number;
  memory_mb?: number;
  walltime_seconds: number;
}

export interface RequestedResources {
  gpu_count: number;
  cpus: number;
  memory_mb: number;
  walltime_seconds: number;
  sbatch_flags: string[];
  evidence: {
    observed_at: string;
    matching_nodes: string[];
    fitting_nodes: string[];
    defaults: { cpus: number; memory_mb: number };
    overrides: { cpus: boolean; memory_mb: boolean };
    limits: ResourceLimits;
    cpu_per_gpu?: number;
    memory_mb_per_gpu?: number;
    account_qos_limits: "unverified_until_submission";
  };
}

type Fields = Record<string, string>;
const absent = /^(?:\(null\)|N\/A|NONE|NOT_SET|UNDEF|UNLIMITED|INFINITE)$/i;
const token = /^[A-Za-z0-9][A-Za-z0-9_.-]*$/;
const GINFO_MAX_CHARACTERS = 16_384;
const GINFO_MAX_LINES = 80;

function fail(detail: string): never {
  throw new SlurmResourceError(detail);
}

/** Parse named fields, preserving values such as Reason=... containing spaces. */
function fields(line: string): Fields {
  const matches = [...line.matchAll(/(?:^|\s)([A-Za-z][A-Za-z0-9_]*)=/g)];
  if (!matches.length || line.slice(0, matches[0]!.index).trim()) fail("Expected named Slurm fields, not terminal columns");
  const result: Fields = {};
  for (let index = 0; index < matches.length; index++) {
    const match = matches[index]!;
    const key = match[1]!;
    if (Object.hasOwn(result, key)) fail(`Duplicate Slurm field ${key}`);
    const start = match.index! + match[0].length;
    result[key] = line.slice(start, matches[index + 1]?.index ?? line.length).trim();
  }
  return result;
}

function records(text: string): Fields[] {
  return text.split(/\r?\n/).filter((line) => line.trim()).map(fields);
}

function integer(value: string | undefined, field: string, minimum = 0): number {
  if (value === undefined || !/^\d+$/.test(value)) fail(`Missing or invalid ${field}`);
  const result = Number(value);
  if (!Number.isSafeInteger(result) || result < minimum) fail(`Nonpositive, unsafe or invalid ${field}`);
  return result;
}

/** Slurm memory suffixes use powers of 1024; unsuffixed node/partition memory is MiB. */
function memory(value: string | undefined, field: string): number {
  const match = value?.match(/^(\d+(?:\.\d+)?)([KMGTPE]?)(?:i?B)?$/i);
  if (!match) fail(`Missing or invalid ${field}`);
  const exponent = "KMGTPE".indexOf(match[2]!.toUpperCase());
  const multiplier = match[2] ? 1024 ** (exponent - 1) : 1;
  const result = Number(match[1]) * multiplier;
  if (!Number.isSafeInteger(result) || result < 0) fail(`Unsafe or fractional MiB capacity in ${field}`);
  return result;
}

function optionalLimit(value: string | undefined, field: string, parser = integer): number | undefined {
  if (value === undefined || absent.test(value) || value === "0") return undefined;
  return parser(value, field);
}

function walltime(value: string | undefined, field: string): number {
  if (!value) fail(`Missing ${field}`);
  let seconds: number;
  if (/^\d+$/.test(value)) {
    seconds = Number(value) * 60;
  } else {
    const match = value.match(/^(?:(\d+)-)?(\d+)(?::(\d{2}))?(?::(\d{2}))?$/);
    if (!match || (match[1] === undefined && match[3] === undefined)) fail(`Unsupported ${field}: ${value}`);
    const days = Number(match[1] ?? 0);
    const first = Number(match[2]);
    const second = Number(match[3] ?? 0);
    const third = match[4] === undefined ? undefined : Number(match[4]);
    if (second > 59 || (third !== undefined && third > 59) || (match[1] !== undefined && first > 23)) {
      fail(`Invalid ${field}: ${value}`);
    }
    if (match[1] !== undefined && third === undefined) {
      // Slurm's days-hours:minutes form.
      seconds = days * 86400 + first * 3600 + second * 60;
    } else {
      seconds = days * 86400 + (third === undefined ? first * 60 + second : first * 3600 + second * 60 + third);
    }
  }
  if (!Number.isSafeInteger(seconds) || seconds <= 0) fail(`Nonpositive or unsafe ${field}`);
  return seconds;
}

function constraintFeatures(constraint: string | undefined): string[] {
  if (constraint === undefined) return [];
  const result = constraint.split("&");
  if (!result.length || result.some((feature) => !token.test(feature))) {
    fail("Only a feature or '&' conjunction is supported for Slurm constraints; expressions are not approximated");
  }
  return result;
}

function tres(value: string | undefined): Fields {
  if (!value || absent.test(value)) return {};
  const result: Fields = {};
  for (const entry of value.split(",")) {
    const at = entry.indexOf("=");
    if (at < 1 || Object.hasOwn(result, entry.slice(0, at))) fail(`Invalid or duplicate TRES entry: ${entry}`);
    result[entry.slice(0, at)] = entry.slice(at + 1);
  }
  return result;
}

/** Split GRES commas without splitting the socket/index lists in parentheses. */
function gresEntries(value: string): string[] {
  const result: string[] = [];
  let depth = 0;
  let start = 0;
  for (let index = 0; index < value.length; index++) {
    if (value[index] === "(") depth++;
    else if (value[index] === ")") depth--;
    else if (value[index] === "," && depth === 0) {
      result.push(value.slice(start, index));
      start = index + 1;
    }
    if (depth < 0 || depth > 1) fail("Malformed GPU GRES topology");
  }
  if (depth !== 0) fail("Malformed GPU GRES topology");
  result.push(value.slice(start));
  return result;
}

function physicalGpus(value: string | undefined, configured: Fields, node: string): Record<string, number> {
  const result: Record<string, number> = {};
  if (value && !absent.test(value)) {
    for (const entry of gresEntries(value)) {
      if (!entry.startsWith("gpu:") && entry !== "gpu") continue;
      const match = entry.match(/^gpu:([A-Za-z0-9][A-Za-z0-9_.-]*):(\d+)(?:\([^()]*\))?$/);
      if (!match || /(?:mig|\d+g[._-]?\d+gb)/i.test(match[1]!)) {
        fail(`Ambiguous, untyped or unsupported MIG GPU GRES on ${node}: ${entry}`);
      }
      const count = integer(match[2], `${node} GPU count`, 1);
      if (Object.hasOwn(result, match[1]!)) fail(`Ambiguous repeated GPU type on ${node}`);
      result[match[1]!] = count;
    }
  }
  const total = Object.values(result).reduce((sum, count) => sum + count, 0);
  if (!Number.isSafeInteger(total)) fail(`Unsafe GPU capacity on ${node}`);
  if (configured["gres/gpu"] !== undefined && integer(configured["gres/gpu"], `${node} CfgTRES GPU count`) !== total) {
    fail(`GPU GRES and configured physical GPU count disagree on ${node}`);
  }
  for (const [key, value] of Object.entries(configured)) {
    if (key.startsWith("gres/gpu:")) {
      const type = key.slice("gres/gpu:".length);
      if (integer(value, `${node} ${key}`) !== (result[type] ?? 0)) fail(`Typed GPU GRES and CfgTRES disagree on ${node}`);
    }
  }
  return result;
}

function allocatedGpus(allocated: Fields, gpus: Record<string, number>, profile: ClusterProfile): number | undefined {
  if (profile.compute_kind === "cpu") return 0;
  const typed = allocated[`gres/gpu:${profile.gpu_type}`];
  if (typed !== undefined) return integer(typed, "allocated typed GPUs");
  const total = allocated["gres/gpu"];
  if (total === "0") return 0;
  if (total !== undefined && Object.keys(gpus).length === 1) return integer(total, "allocated GPUs");
  return undefined;
}

function unavailable(state: string): boolean {
  return /(?:DOWN|DRAIN|FAIL|MAINT|UNKNOWN|INVAL|FUTURE|POWER_DOWN|POWERED_DOWN|NOT_RESPOND)/i.test(state) || /[*~#!]/.test(state);
}

/** No SSH, scheduling, or ginfo dependency: configured capacities determine all sizing. */
export function parseInventory(profile: ClusterProfile, source: InventorySource): Inventory {
  const requiredFeatures = constraintFeatures(profile.constraint);
  const partitionRecords = records(source.partition).filter((record) => record.PartitionName === profile.partition);
  if (partitionRecords.length !== 1) fail(`Expected exactly one configured partition ${profile.partition}`);
  const partition = partitionRecords[0]!;
  const limits: ResourceLimits = {
    max_cpus: optionalLimit(partition.MaxCPUsPerNode, "MaxCPUsPerNode"),
    max_memory_mb: optionalLimit(partition.MaxMemPerNode, "MaxMemPerNode", memory),
    max_memory_per_cpu_mb: optionalLimit(partition.MaxMemPerCPU, "MaxMemPerCPU", memory),
    max_walltime_seconds: optionalLimit(partition.MaxTime, "MaxTime", walltime),
  };
  const membership = new Map<string, string>();
  for (const row of records(source.sinfo)) {
    if (row.Partition?.replace(/\*$/, "") !== profile.partition) continue;
    if (!row.NodeName || !row.State) fail("sinfo requires named NodeName, Partition and State fields");
    // --Node must return individual node names, never silently approximate a hostlist.
    if (!token.test(row.NodeName)) fail(`sinfo --Node returned an unsupported node name or hostlist: ${row.NodeName}`);
    const old = membership.get(row.NodeName);
    if (old !== undefined && old !== row.State) fail(`Conflicting sinfo observations for ${row.NodeName}`);
    membership.set(row.NodeName, row.State);
  }
  if (!membership.size) fail(`No nodes observed in exact partition ${profile.partition}`);
  const nodes: InventoryNode[] = [];
  const seen = new Set<string>();
  for (const record of records(source.nodes)) {
    const name = record.NodeName;
    if (!name || !membership.has(name)) continue;
    if (seen.has(name)) fail(`Duplicate node record ${name}`);
    seen.add(name);
    const partitions = record.Partitions?.split(",") ?? [];
    if (!partitions.includes(profile.partition)) fail(`Node ${name} has inconsistent exact partition membership`);
    const availableFeatures = !record.AvailableFeatures || absent.test(record.AvailableFeatures) ? [] : record.AvailableFeatures.split(",");
    if (!requiredFeatures.every((feature) => availableFeatures.includes(feature))) continue;
    const configured = tres(record.CfgTRES);
    const gpus = physicalGpus(record.Gres, configured, name);
    if (profile.compute_kind === "gpu" && (gpus[profile.gpu_type] ?? 0) <= 0) continue;
    const cpus = integer(record.CPUEfctv ?? configured.cpu, `${name} configured schedulable CPUs`, 1);
    const realMemory = memory(record.RealMemory, `${name} RealMemory`);
    const reservedMemory = record.MemSpecLimit === undefined ? 0 : memory(record.MemSpecLimit, `${name} MemSpecLimit`);
    if (realMemory <= reservedMemory) fail(`Missing or nonpositive usable memory on ${name}`);
    const allocated = tres(record.AllocTRES);
    const allocatedCpus = record.CPUAlloc === undefined ? (allocated.cpu === undefined ? undefined : integer(allocated.cpu, `${name} allocated CPUs`)) : integer(record.CPUAlloc, `${name} CPUAlloc`);
    const allocatedMemory = record.AllocMem === undefined ? (allocated.mem === undefined ? undefined : memory(allocated.mem, `${name} allocated memory`)) : memory(record.AllocMem, `${name} AllocMem`);
    const allocatedGpuCount = allocatedGpus(allocated, gpus, profile);
    const totalGpus = Object.values(gpus).reduce((sum, count) => sum + count, 0);
    nodes.push({
      name, partitions, state: record.State ?? membership.get(name)!, features: availableFeatures,
      cpus, memory_mb: realMemory - reservedMemory, reserved_memory_mb: reservedMemory,
      gpus, total_gpus: totalGpus, allocated_cpus: allocatedCpus,
      allocated_memory_mb: allocatedMemory, allocated_gpus: allocatedGpuCount,
    });
  }
  for (const name of membership.keys()) {
    if (!seen.has(name)) fail(`Missing scontrol capacity record for partition node ${name}`);
  }
  if (!nodes.length) fail(`No nodes match exact partition, GPU type and constraint for ${profile.id}`);
  const observedAt = source.observed_at ?? new Date().toISOString();
  if (!Number.isFinite(Date.parse(observedAt))) fail("Invalid inventory observation timestamp");
  const sumFree = (configured: (node: InventoryNode) => number, allocated: (node: InventoryNode) => number | undefined): number | undefined => {
    let total = 0;
    for (const node of nodes) {
      if (unavailable(node.state)) continue;
      const used = allocated(node);
      if (used === undefined || used > configured(node)) return undefined;
      total += configured(node) - used;
    }
    return total;
  };
  let supplemental: Inventory["ginfo"];
  if (source.ginfo !== undefined) {
    const lines = source.ginfo.split(/\r?\n/);
    const output = lines.slice(0, GINFO_MAX_LINES).join("\n").slice(0, GINFO_MAX_CHARACTERS);
    supplemental = { output, truncated: lines.length > GINFO_MAX_LINES || output.length < source.ginfo.length };
  }
  return {
    observed_at: observedAt, partition: profile.partition, nodes, limits,
    availability: {
      total_nodes: nodes.length,
      idle_nodes: nodes.filter((node) => !unavailable(node.state) && /\bIDLE\b/i.test(node.state)).length,
      busy_nodes: nodes.filter((node) => !unavailable(node.state) && !/\bIDLE\b/i.test(node.state)).length,
      unavailable_nodes: nodes.filter((node) => unavailable(node.state)).length,
      total_gpus: nodes.reduce((sum, node) => sum + (profile.compute_kind === "gpu" ? node.gpus[profile.gpu_type]! : 0), 0),
      free_gpus: sumFree((node) => profile.compute_kind === "gpu" ? node.gpus[profile.gpu_type]! : 0, (node) => node.allocated_gpus),
      free_cpus: sumFree((node) => node.cpus, (node) => node.allocated_cpus),
      free_memory_mb: sumFree((node) => node.memory_mb, (node) => node.allocated_memory_mb),
      note: "Scheduler observation only; permissions, account/QoS limits, fair-share and prompt scheduling are not guaranteed.",
    },
    ...(supplemental ? { ginfo: supplemental } : {}),
  };
}

function requestInteger(value: number, field: string, minimum = 1): number {
  if (!Number.isSafeInteger(value) || value < minimum) fail(`${field} must be a safe integer >= ${minimum}`);
  return value;
}

function minimum(...values: (number | undefined)[]): number | undefined {
  const present = values.filter((value): value is number => value !== undefined);
  return present.length ? Math.min(...present) : undefined;
}

/** Defaults may be capped; explicit needs are never silently reduced. */
export function sizeResources(profile: ClusterProfile, inventory: Inventory, request: ResourceRequest): RequestedResources {
  if (inventory.partition !== profile.partition) fail("Inventory belongs to another partition");
  const gpuCount = requestInteger(request.gpu_count ?? (profile.compute_kind === "gpu" ? 1 : 0), "gpu_count", 0);
  if (profile.compute_kind === "cpu" ? gpuCount !== 0 : gpuCount < 1 || gpuCount > profile.max_gpus) {
    fail(profile.compute_kind === "cpu" ? "CPU targets require gpu_count=0" : `gpu_count must be from 1 to ${profile.max_gpus}`);
  }
  const requiredFeatures = constraintFeatures(profile.constraint);
  const nodes = inventory.nodes.filter((node) => node.partitions.includes(profile.partition)
    && requiredFeatures.every((feature) => node.features.includes(feature))
    && (profile.compute_kind === "cpu" || (node.gpus[profile.gpu_type] ?? 0) >= gpuCount));
  if (!nodes.length) fail("No matching node has the requested physical GPU count");
  for (const node of nodes) {
    requestInteger(node.cpus, `${node.name} configured CPUs`);
    requestInteger(node.memory_mb, `${node.name} usable memory`);
    if (profile.compute_kind === "gpu") requestInteger(node.total_gpus, `${node.name} total physical GPUs`);
  }
  const limits: ResourceLimits = {
    max_cpus: minimum(profile.max_cpus, inventory.limits.max_cpus),
    max_memory_mb: minimum(profile.max_memory_mb, inventory.limits.max_memory_mb),
    max_memory_per_cpu_mb: inventory.limits.max_memory_per_cpu_mb,
    max_walltime_seconds: minimum(profile.max_walltime_seconds, inventory.limits.max_walltime_seconds),
  };
  const cpuRatio = profile.compute_kind === "gpu" ? Math.min(...nodes.map((node) => node.cpus / node.total_gpus)) : undefined;
  const memoryRatio = profile.compute_kind === "gpu" ? Math.min(...nodes.map((node) => node.memory_mb / node.total_gpus)) : undefined;
  const defaults = {
    cpus: cpuRatio === undefined ? 1 : Math.max(1, Math.floor(gpuCount * cpuRatio)),
    memory_mb: memoryRatio === undefined ? 4096 : 256 * Math.floor(gpuCount * memoryRatio / 256),
  };
  const explicitCpus = request.cpus !== undefined;
  const explicitMemory = request.memory_mb !== undefined;
  let cpus = requestInteger(request.cpus ?? defaults.cpus, "cpus");
  if (limits.max_cpus !== undefined && cpus > limits.max_cpus) {
    if (explicitCpus) fail(`Explicit cpus=${cpus} exceeds CPU limit ${limits.max_cpus}`);
    cpus = limits.max_cpus;
  }
  requestInteger(cpus, "capped cpus");
  const perCpuMemoryLimit = limits.max_memory_per_cpu_mb === undefined ? undefined : cpus * limits.max_memory_per_cpu_mb;
  if (perCpuMemoryLimit !== undefined && !Number.isSafeInteger(perCpuMemoryLimit)) fail("Unsafe per-CPU memory limit");
  const memoryLimit = minimum(limits.max_memory_mb, perCpuMemoryLimit);
  let memoryMb = requestInteger(request.memory_mb ?? defaults.memory_mb, "memory_mb");
  if (memoryLimit !== undefined && memoryMb > memoryLimit) {
    if (explicitMemory) fail(`Explicit memory_mb=${memoryMb} exceeds memory limit ${memoryLimit} for final cpus=${cpus}`);
    memoryMb = memoryLimit;
  }
  if (!explicitMemory) memoryMb = 256 * Math.floor(memoryMb / 256);
  requestInteger(memoryMb, "capped memory_mb");
  const seconds = requestInteger(request.walltime_seconds, "walltime_seconds");
  if (limits.max_walltime_seconds !== undefined && seconds > limits.max_walltime_seconds) {
    fail(`Explicit walltime_seconds=${seconds} exceeds maximum ${limits.max_walltime_seconds}`);
  }
  const fitting = nodes.filter((node) => cpus <= node.cpus && memoryMb <= node.memory_mb);
  if (!fitting.length) fail(`No matching node can fit cpus=${cpus}, memory_mb=${memoryMb}, gpu_count=${gpuCount}`);
  const sbatchFlags = [
    "--nodes=1", "--ntasks=1", `--cpus-per-task=${cpus}`, `--mem=${memoryMb}M`,
    ...(profile.compute_kind === "gpu" ? [`--gpus-per-node=${profile.gpu_type}:${gpuCount}`] : []),
    `--time=${Math.floor(seconds / 86400)}-${String(Math.floor(seconds / 3600) % 24).padStart(2, "0")}:${String(Math.floor(seconds / 60) % 60).padStart(2, "0")}:${String(seconds % 60).padStart(2, "0")}`,
  ];
  return {
    gpu_count: gpuCount, cpus, memory_mb: memoryMb, walltime_seconds: seconds, sbatch_flags: sbatchFlags,
    evidence: {
      observed_at: inventory.observed_at, matching_nodes: nodes.map((node) => node.name), fitting_nodes: fitting.map((node) => node.name),
      defaults, overrides: { cpus: explicitCpus, memory_mb: explicitMemory }, limits,
      ...(cpuRatio === undefined ? {} : { cpu_per_gpu: cpuRatio, memory_mb_per_gpu: memoryRatio }),
      account_qos_limits: "unverified_until_submission",
    },
  };
}
