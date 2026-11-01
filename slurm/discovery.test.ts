import { expect, test } from "bun:test";
import { spawn } from "node:child_process";
import { access, chmod, mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { shellQuote, type sshTransport } from "./client.ts";
import {
  discoverCluster, hasCompatibleImageLabel, normalizeRuntimeSetup, parseDiscovery, parseModuleCandidates,
  type DiscoveryEvidence, type DiscoveryInput, type DiscoverySource,
} from "./discovery.ts";

const observedAt = "2026-11-01T12:00:00.000Z";
const input: DiscoveryInput = { mappings: [{ storage_id: "work", shared_root: "/shared/work" }] };
const image = JSON.stringify({ data: { attributes: { labels: { "org.worker-harness.slurm-runtime": "1" } } } });
const partitions = [
  "PartitionName=cpu State=UP Hidden=NO AllowAccounts=ALL AllowGroups=ALL AllowQos=ALL QoS=N/A MaxTime=UNLIMITED MaxCPUsPerNode=UNLIMITED MaxMemPerNode=UNLIMITED",
  "PartitionName=gpu State=UP Hidden=NO AllowAccounts=ALL AllowGroups=ALL AllowQos=ALL QoS=N/A MaxTime=02:00:00 MaxCPUsPerNode=UNLIMITED MaxMemPerNode=UNLIMITED",
].join("\n");
const membership = "NodeName=c1 Partition=cpu* State=IDLE\nNodeName=a1 Partition=gpu State=ALLOCATED\nNodeName=h1 Partition=gpu State=IDLE\nNodeName=a2 Partition=gpu State=IDLE";
const nodes = [
  "NodeName=c1 Partitions=cpu State=IDLE CPUEfctv=16 RealMemory=65536 Gres=(null) CfgTRES=cpu=16,mem=65536M",
  "NodeName=a1 Partitions=gpu State=ALLOCATED CPUEfctv=64 RealMemory=524288 Gres=gpu:a100:4(S:0-3) CfgTRES=cpu=64,mem=524288M,gres/gpu=4 CPUAlloc=64 AllocMem=524288 AllocTRES=cpu=64,mem=524288M,gres/gpu=4",
  "NodeName=h1 Partitions=gpu State=IDLE CPUEfctv=96 RealMemory=786432 Gres=gpu:h100:8(S:0-7) CfgTRES=cpu=96,mem=786432M,gres/gpu=8",
  "NodeName=a2 Partitions=gpu State=IDLE CPUEfctv=32 RealMemory=131072 Gres=gpu:a100:2 CfgTRES=cpu=32,mem=131072M,gres/gpu=2",
].join("\n");
function source(overrides: Partial<Record<DiscoveryEvidence["kind"], string | null>> = {}): DiscoverySource {
  const outputs: Record<DiscoveryEvidence["kind"], string | null> = {
    config: "ClusterName = fixture", partitions, membership, nodes,
    default_account: "test|standard|", associations: "fixture|standard||normal|normal|\nother-cluster|premium||paid|paid|",
    qos: "normal|\npaid|", runtime: "apptainer\napptainer version 1.3.0\n/usr/bin/apptainer\nWH_BATCH_RUNTIME_OK\n", modules: "", image,
    ...overrides,
  };
  return { commands: Object.entries(outputs).map(([kind, output]) => ({
    kind: kind as DiscoveryEvidence["kind"], status: output === null ? "unavailable" : "ok", output: output ?? "access denied",
  })) };
}

test("discovery produces CPU and exact physical GPU targets, not current free capacity", () => {
  const result = parseDiscovery(source(), input, observedAt);
  expect(result.observed_at).toBe(observedAt);
  expect(result.cluster_name).toBe("fixture");
  expect(result.account).toBe("standard");
  expect(result.account_source).toBe("default");
  expect(result.accounts).toEqual(["standard"]);
  expect(result.qos).toEqual(["normal", "paid"]);
  expect(result.targets).toEqual([
    expect.objectContaining({ partition: "cpu", compute_kind: "cpu", max_walltime_seconds: 604800, defaults: { cpus: 1, memory_mb: 4096 }, review_required: false }),
    expect.objectContaining({ partition: "gpu", compute_kind: "cpu", max_walltime_seconds: 7200 }),
    expect.objectContaining({ partition: "gpu", compute_kind: "gpu", gpu_type: "a100", max_gpus: 4, defaults: { cpus: 16, memory_mb: 65536 }, max_walltime_seconds: 7200 }),
    expect.objectContaining({ partition: "gpu", compute_kind: "gpu", gpu_type: "h100", max_gpus: 8, defaults: { cpus: 12, memory_mb: 98304 }, max_walltime_seconds: 7200 }),
  ]);
  expect(result.targets.every((target) => target.permission === "unverified_until_submission")).toBe(true);
  expect(result.runtime).toEqual({ executable: "apptainer", version: "apptainer version 1.3.0", prelude: "" });
  expect(result.sif_path).toBe("/shared/work/worker-harness-worker.sif");
  expect(result.unresolved).toEqual([]);
});

test("unknown account is delegated, while a known default survives hidden associations", () => {
  const hidden = parseDiscovery(source({ associations: null, qos: null }), input, observedAt);
  expect(hidden.account).toBe("standard");
  expect(hidden.account_source).toBe("default");
  const unknown = parseDiscovery(source({ default_account: null, associations: null, qos: null }), input, observedAt);
  expect(unknown.account).toBeUndefined();
  expect(unknown.account_source).toBe("scheduler");
  expect(unknown.unresolved).toContainEqual(expect.objectContaining({ kind: "billing", blocking: false }));
  expect(unknown.targets).toHaveLength(4);
});

test("ambiguous billing is never resolved by choosing the first visible association", () => {
  const observations = source({ default_account: "test||", associations: "fixture|b||normal|normal|\nfixture|a||paid|paid|\nother-cluster|hidden||paid|paid|" });
  const result = parseDiscovery(observations, input, observedAt);
  expect(result.accounts).toEqual(["a", "b"]);
  expect(result.account).toBeUndefined();
  expect(result.unresolved).toContainEqual(expect.objectContaining({ kind: "billing", blocking: false }));
  const explicit = parseDiscovery(observations, { ...input, account: "b" }, observedAt);
  expect(explicit.account).toBe("b");
  expect(explicit.account_source).toBe("explicit");
});

test("finite partition limits clamp only the allocation ceiling; infinite remains seven days", () => {
  for (const [limit, expected] of [["INFINITE", 604800], ["10-00:00:00", 604800], ["1-02:03:04", 93784], ["00:30:00", 1800]] as const) {
    const result = parseDiscovery(source({ partitions: partitions.replace("MaxTime=UNLIMITED", `MaxTime=${limit}`) }), input, observedAt);
    expect(result.targets.find((target) => target.partition === "cpu")!.max_walltime_seconds).toBe(expected);
  }
});

test("MIG and untyped GPU configurations are reported, never converted to physical targets", () => {
  for (const gres of ["gpu:a100_1g.10gb:4", "gpu:4"]) {
    const result = parseDiscovery(source({ nodes: nodes.replace("gpu:a100:4(S:0-3)", gres) }), input, observedAt);
    expect(result.targets.filter((target) => target.partition === "gpu")).toEqual([]);
    expect(result.targets.find((target) => target.partition === "cpu")).toBeDefined();
    expect(result.unresolved).toContainEqual(expect.objectContaining({ kind: "inventory", partition: "gpu" }));
  }
});

test("restricted and special partitions require review; no QOS is selected", () => {
  const result = parseDiscovery(source({ partitions: partitions.replace("PartitionName=gpu", "PartitionName=gpu").replace("Hidden=NO", "Hidden=YES") }), input, observedAt);
  expect(result.targets.find((target) => target.partition === "cpu")!.review_required).toBe(true);
  expect(result.unresolved).toContainEqual(expect.objectContaining({ kind: "partition", partition: "cpu", blocking: false }));
  expect(result.targets.every((target) => !("qos" in target))).toBe(true);
});

test("runtime and old SIF markers stay unresolved, not fabricated ready values", () => {
  const absent = parseDiscovery(source({ runtime: null, image: null, modules: "apptainer/1.3.0" }), input, observedAt);
  expect(absent.runtime).toBeUndefined();
  expect(absent.sif_path).toBeUndefined();
  expect(absent.unresolved).toContainEqual(expect.objectContaining({ kind: "runtime", blocking: true }));
  expect(absent.unresolved).toContainEqual(expect.objectContaining({ kind: "image", blocking: true }));
  for (const badImage of ["{}", "{not-json", '{"data":{"attributes":{"labels":{"org.worker-harness.slurm-runtime":"0"}}}}']) {
    expect(hasCompatibleImageLabel(badImage)).toBe(false);
    expect(parseDiscovery(source({ image: badImage }), input, observedAt).sif_path).toBeUndefined();
  }
  expect(hasCompatibleImageLabel(image)).toBe(true);
});

test("bounded evidence, failed scheduler queries and invalid metadata have typed reasons", () => {
  const failed = parseDiscovery(source({ nodes: null }), input, observedAt);
  expect(failed.targets).toEqual([]);
  expect(failed.unresolved).toContainEqual(expect.objectContaining({ kind: "scheduler", blocking: true }));
  const oversized = parseDiscovery(source({ nodes: "x".repeat(600_000) }), input, observedAt);
  expect(oversized.evidence.find((entry) => entry.kind === "nodes")!.status).toBe("invalid");
  expect(oversized.evidence.every((entry) => entry.output.length <= 16_384)).toBe(true);
  expect(oversized.targets).toEqual([]);
  const excessiveRecords = parseDiscovery(source({ default_account: "test|standard|\n".repeat(9000) }), input, observedAt);
  expect(excessiveRecords.evidence.find((entry) => entry.kind === "default_account")!.status).toBe("invalid");
  expect(excessiveRecords.account).toBeUndefined();
  expect(excessiveRecords.unresolved).toContainEqual(expect.objectContaining({ kind: "billing", blocking: false }));
  expect(() => parseDiscovery(source(), input, "not-a-date")).toThrow();
  expect(() => parseDiscovery(source(), { ...input, account: "acct; touch /tmp/no" }, observedAt)).toThrow();
});

test("module candidates are literal names and setup never accepts shell commands", () => {
  expect(parseModuleCandidates("/modules:\napptainer/1.3.0(D) singularity/3.11\napptainer/1.3.0\n$(touch /tmp/no)\napptainer/1.2;evil\nother/1.0"))
    .toEqual(["apptainer/1.3.0", "singularity/3.11"]);
  expect(normalizeRuntimeSetup(" module   load apptainer/1.3.0 ")).toBe("module load apptainer/1.3.0");
  const runtimePath = "/opt/runtime's/bin";
  const generated = `export PATH=${shellQuote(runtimePath)}:"$PATH"`;
  expect(normalizeRuntimeSetup(generated)).toBe(generated);
  const custom = parseDiscovery(source({ runtime: `apptainer\napptainer version 1.3.0\n${runtimePath}/apptainer\nWH_BATCH_RUNTIME_OK\n` }), input, observedAt);
  expect(custom.runtime?.prelude).toBe(generated);
  expect(parseDiscovery(source({ runtime: "apptainer\napptainer version 1.3.0\n" }), input, observedAt).runtime).toBeUndefined();
  for (const bad of ["module load apptainer; touch /tmp/no", "$(touch /tmp/no)", "module load apptainer\ntrue", "module load `id`", "source /etc/profile"]) {
    expect(() => normalizeRuntimeSetup(bad)).toThrow();
  }
});

/** Real command fixtures execute the production collector scripts through bash. */
async function commandFixture(options: { modules?: boolean; hiddenAccounts?: boolean; missingRuntime?: boolean; oldImage?: boolean; runtimeNeedsEnv?: boolean } = {}) {
  const root = await mkdtemp(join(tmpdir(), "wh-discovery-"));
  const bin = join(root, "bin"), runtimeBin = join(root, "runtime"), shared = join(root, "shared's;literal");
  await Promise.all([mkdir(bin), mkdir(runtimeBin), mkdir(shared)]);
  const writeCommand = async (directory: string, name: string, body: string) => {
    const path = join(directory, name);
    await writeFile(path, "#!/bin/bash\nset -eu\n" + body + "\n");
    await chmod(path, 0o700);
  };
  const emit = (output: string) => `printf '%s\\n' ${shellQuote(output)}`;
  await writeCommand(bin, "scontrol", `case "$*" in
    'show config') ${emit("ClusterName = fixture")} ;;
    '--oneliner show partitions') ${emit(partitions)} ;;
    '--oneliner show nodes') ${emit(nodes)} ;;
    *) exit 40 ;;
  esac`);
  await writeCommand(bin, "sinfo", emit(membership));
  await writeCommand(bin, "sacctmgr", options.hiddenAccounts ? "printf 'access denied\\n' >&2; exit 1" : `case "$*" in
    *'show user'*) ${emit("test|standard|")} ;;
    *'show association'*) ${emit("fixture|standard||normal|normal|")} ;;
    *'show qos'*) ${emit("normal|")} ;;
    *) exit 41 ;;
  esac`);
  const runtimeBody = (options.runtimeNeedsEnv ? 'test "${WH_RUNTIME_LIBRARY:-}" = 1 || exit 1\n' : "") + `case "$1" in
    --version) printf 'apptainer version 1.3.0\\n' ;;
    inspect) test "$2" = --json; cat -- "$3" ;;
    *) exit 42 ;;
  esac`;
  if (options.modules || options.missingRuntime) await writeCommand(bin, "apptainer", "exit 1");
  else await writeCommand(bin, "apptainer", runtimeBody);
  await writeCommand(bin, "singularity", "exit 1");
  await writeCommand(runtimeBin, "apptainer", runtimeBody);
  await writeCommand(bin, "touch-bad", `touch -- ${shellQuote(join(root, "injected"))}`);
  const setup = options.modules ? `module() {
    case "$*" in
      '-t avail apptainer singularity') printf 'apptainer/1.3.0(D)\\napptainer/1.3.0;touch-bad\\n' ;;
      'load apptainer/1.3.0') export PATH=${shellQuote(runtimeBin)}:"$PATH" ;;
      *) return 1 ;;
    esac
  }` : "module() { return 1; }";
  const sif = join(shared, "worker-harness-worker.sif");
  await writeFile(sif, options.oldImage ? "{}" : image);
  const transport: typeof sshTransport = async (_connection, script, _stdin, transportOptions) => {
    return await new Promise<string>((resolve, reject) => {
      const child = spawn("/bin/bash", ["-c", setup + "\n" + script], {
        env: { PATH: bin + ":/usr/bin:/bin", HOME: root, WH_RUNTIME_LIBRARY: "1" }, signal: transportOptions?.signal,
      });
      let stdout = "", stderr = "";
      child.stdout.on("data", (chunk) => { stdout += chunk; });
      child.stderr.on("data", (chunk) => { stderr += chunk; });
      child.on("error", reject);
      child.on("close", (code) => code === 0 ? resolve(stdout) : reject(new Error(stderr || `exit ${code}`)));
    });
  };
  return { root, shared, transport };
}

test("readonly discovery collectors execute real fixture binaries and safely quote image paths", async () => {
  const fixture = await commandFixture();
  try {
    const result = await discoverCluster({ ssh_host: "fixture" }, { mappings: [{ storage_id: "work", shared_root: fixture.shared }] }, {
      transport: fixture.transport, now: () => new Date(observedAt),
    });
    expect(result.observed_at).toBe(observedAt);
    expect(result.targets).toHaveLength(4);
    expect(result.account).toBe("standard");
    expect(result.runtime?.executable).toBe("apptainer");
    expect(result.sif_path).toBe(join(fixture.shared, "worker-harness-worker.sif"));
    expect(result.unresolved).toEqual([]);
  } finally { await rm(fixture.root, { recursive: true, force: true }); }
});

test("confirmed module candidates make a real runtime discoverable despite hidden accounting", async () => {
  const fixture = await commandFixture({ modules: true, hiddenAccounts: true });
  try {
    const result = await discoverCluster({ ssh_host: "fixture" }, { mappings: [{ storage_id: "work", shared_root: fixture.shared }] }, { transport: fixture.transport });
    expect(result.runtime).toEqual({ executable: "apptainer", version: "apptainer version 1.3.0", prelude: `export PATH=${shellQuote(join(fixture.root, "runtime"))}:"$PATH"` });
    expect(result.account).toBeUndefined();
    expect(result.account_source).toBe("scheduler");
    expect(result.sif_path).toBeDefined();
    expect(result.unresolved.filter((entry) => entry.blocking)).toEqual([]);
    await expect(access(join(fixture.root, "injected"))).rejects.toThrow();
  } finally { await rm(fixture.root, { recursive: true, force: true }); }
});

test("real failed runtime and legacy image observations remain blocking", async () => {
  for (const mode of [{ missingRuntime: true }, { oldImage: true }, { runtimeNeedsEnv: true }]) {
    const fixture = await commandFixture(mode);
    try {
      const result = await discoverCluster({ ssh_host: "fixture" }, { mappings: [{ storage_id: "work", shared_root: fixture.shared }] }, { transport: fixture.transport });
      expect(result.sif_path).toBeUndefined();
      expect(result.unresolved).toContainEqual(expect.objectContaining({ kind: "image", blocking: true }));
      if ("missingRuntime" in mode || "runtimeNeedsEnv" in mode) expect(result.runtime).toBeUndefined();
    } finally { await rm(fixture.root, { recursive: true, force: true }); }
  }
});

test("already-aborted discovery does not execute a collector", async () => {
  const controller = new AbortController();
  controller.abort(new Error("cancelled setup"));
  let calls = 0;
  const transport: typeof sshTransport = async () => { calls++; throw new Error("must not execute"); };
  await expect(discoverCluster({ ssh_host: "fixture" }, input, { transport, signal: controller.signal })).rejects.toThrow("cancelled setup");
  expect(calls).toBe(0);
});
