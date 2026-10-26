import { expect, test } from "bun:test";
import type { ClusterProfile } from "./profiles.ts";
import { parseInventory, sizeResources, SlurmResourceError, type InventorySource } from "./resources.ts";

const observedAt = "2026-10-25T12:00:00.000Z";

function gpu(overrides: Partial<ClusterProfile> = {}): ClusterProfile {
  return {
    id: "test-gpu", description: "Test partition; account and QoS limits unverified", ssh_host: "test-login",
    storage_id: "test", shared_root: "/shared/test", sif_path: "/shared/test/wh.sif", runtime: "apptainer",
    runtime_setup: "", account: "test", partition: "gpu", compute_kind: "gpu", gpu_type: "a100", max_gpus: 8,
    max_walltime_seconds: 3600, failure_grace_seconds: 0, warning_seconds: 120, termination_grace_seconds: 30,
    tailnet_key_file: "/secret/tailnet.key", tailnet_login_server: "https://tailnet.test",
    orchestrator_host: "wh.test", orchestrator_registration_port: 12888, poll_seconds: 15,
    ...overrides,
  } as ClusterProfile;
}

function cpu(overrides: Partial<ClusterProfile> = {}): ClusterProfile {
  const { gpu_type: _gpuType, ...base } = gpu();
  return { ...base, id: "test-cpu", partition: "cpu", compute_kind: "cpu", max_gpus: 0, ...overrides } as ClusterProfile;
}

function node(name = "n1", overrides: Record<string, string | undefined> = {}): string {
  const values: Record<string, string | undefined> = {
    NodeName: name, Partitions: "gpu", State: "IDLE", AvailableFeatures: "avx512,fast", ActiveFeatures: "avx512",
    CPUEfctv: "64", CPUTot: "128", RealMemory: "524288", MemSpecLimit: "0",
    Gres: "gpu:a100:8(S:0-3)", CfgTRES: "cpu=64,mem=524288M,gres/gpu=8,gres/gpu:a100=8",
    CPUAlloc: "0", AllocMem: "0", AllocTRES: "cpu=0,mem=0M,gres/gpu=0",
    ...overrides,
  };
  return Object.entries(values).filter(([, value]) => value !== undefined).map(([key, value]) => `${key}=${value}`).join(" ");
}

function source(overrides: Partial<InventorySource> = {}): InventorySource {
  return {
    sinfo: "NodeName=n1 Partition=gpu* State=idle",
    nodes: node(), partition: "PartitionName=gpu MaxCPUsPerNode=UNLIMITED MaxMemPerNode=UNLIMITED MaxTime=01:00:00",
    observed_at: observedAt, ...overrides,
  };
}

test("8-GPU/64-CPU/512GiB configuration gives proportional one/two GPU defaults and exact flags", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source());
  const one = sizeResources(profile, inventory, { walltime_seconds: 600 });
  const two = sizeResources(profile, inventory, { gpu_count: 2, walltime_seconds: 600 });
  expect([one.gpu_count, one.cpus, one.memory_mb]).toEqual([1, 8, 65536]);
  expect([two.gpu_count, two.cpus, two.memory_mb]).toEqual([2, 16, 131072]);
  expect(one.sbatch_flags).toEqual([
    "--nodes=1", "--ntasks=1", "--cpus-per-task=8", "--mem=65536M", "--gpus-per-node=a100:1", "--time=0-00:10:00",
  ]);
  expect(one.evidence).toMatchObject({
    observed_at: observedAt, matching_nodes: ["n1"], fitting_nodes: ["n1"],
    defaults: { cpus: 8, memory_mb: 65536 }, overrides: { cpus: false, memory_mb: false },
    cpu_per_gpu: 8, memory_mb_per_gpu: 65536, account_qos_limits: "unverified_until_submission",
  });
});

test("busy/free remainder changes availability but never configured sizing", () => {
  const profile = gpu();
  const idle = parseInventory(profile, source());
  const busy = parseInventory(profile, source({ nodes: node("n1", {
    State: "ALLOCATED", CPUAlloc: "63", AllocMem: "500000", AllocTRES: "cpu=63,mem=500000M,gres/gpu=7,gres/gpu:a100=7",
  }), sinfo: "NodeName=n1 Partition=gpu State=allocated" }));
  expect(sizeResources(profile, busy, { gpu_count: 2, walltime_seconds: 300 }))
    .toEqual(sizeResources(profile, idle, { gpu_count: 2, walltime_seconds: 300 }));
  expect(idle.availability).toMatchObject({ idle_nodes: 1, busy_nodes: 0, free_cpus: 64, free_memory_mb: 524288, free_gpus: 8 });
  expect(busy.availability).toMatchObject({ idle_nodes: 0, busy_nodes: 1, free_cpus: 1, free_memory_mb: 24288, free_gpus: 1 });
});

test("heterogeneous nodes use independent conservative per-GPU CPU/RAM minima", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source({
    sinfo: "NodeName=n1 Partition=gpu State=idle\nNodeName=n2 Partition=gpu State=allocated",
    nodes: [node("n1", { CPUEfctv: "32" }), node("n2", { RealMemory: "262144", State: "ALLOCATED" })].join("\n"),
  }));
  const result = sizeResources(profile, inventory, { gpu_count: 2, walltime_seconds: 300 });
  expect([result.cpus, result.memory_mb]).toEqual([8, 65536]);
  expect(result.evidence.cpu_per_gpu).toBe(4);
  expect(result.evidence.memory_mb_per_gpu).toBe(32768);
  expect(result.evidence.matching_nodes).toEqual(["n1", "n2"]);
});

test("GPU count restricts the sizing set before computing conservative ratios", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source({
    sinfo: "NodeName=n1 Partition=gpu State=idle\nNodeName=small Partition=gpu State=idle",
    nodes: [node(), node("small", { Gres: "gpu:a100:1", CfgTRES: "cpu=1,gres/gpu=1", CPUEfctv: "1", RealMemory: "4096" })].join("\n"),
  }));
  const result = sizeResources(profile, inventory, { gpu_count: 2, walltime_seconds: 300 });
  expect([result.cpus, result.memory_mb]).toEqual([16, 131072]);
  expect(result.evidence.matching_nodes).toEqual(["n1"]);
});

test("denominator counts all physical GPUs, not only GPUs of the requested type", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source({ nodes: node("n1", {
    Gres: "gpu:a100:4(S:0,1),gpu:v100:4(S:2,3),mps:400",
    CfgTRES: "cpu=64,gres/gpu=8,gres/gpu:a100=4,gres/gpu:v100=4",
  }) }));
  const result = sizeResources(profile, inventory, { gpu_count: 1, walltime_seconds: 300 });
  expect(inventory.nodes[0]!.total_gpus).toBe(8);
  expect(inventory.availability.total_gpus).toBe(4);
  expect([result.cpus, result.memory_mb]).toEqual([8, 65536]);
});

test("partition and GPU type matching are exact, not substrings", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source({
    sinfo: "NodeName=n1 Partition=gpu State=idle\nNodeName=wrongtype Partition=gpu State=idle\nNodeName=wrongpart Partition=gpu-debug State=idle",
    nodes: [node(), node("wrongtype", {
      Gres: "gpu:a100_80gb:8", CfgTRES: "cpu=64,gres/gpu=8,gres/gpu:a100_80gb=8", CPUEfctv: "1", RealMemory: "1",
    }), node("wrongpart", { Partitions: "gpu-debug", CPUEfctv: "1", RealMemory: "1" })].join("\n"),
    partition: "PartitionName=gpu-debug MaxTime=00:01:00\nPartitionName=gpu MaxTime=01:00:00",
  }));
  expect(inventory.nodes.map((entry) => entry.name)).toEqual(["n1"]);
  expect(sizeResources(profile, inventory, { walltime_seconds: 300 }).cpus).toBe(8);
  expect(() => parseInventory(gpu({ gpu_type: "a10" }), source())).toThrow("No nodes match");
});

test("constraints use AvailableFeatures conjunction, not ActiveFeatures or approximate expressions", () => {
  const profile = gpu({ constraint: "fast&avx512" });
  const inventory = parseInventory(profile, source({
    sinfo: "NodeName=n1 Partition=gpu State=idle\nNodeName=slow Partition=gpu State=idle",
    nodes: [node(), node("slow", { AvailableFeatures: "avx512", ActiveFeatures: "fast,avx512", CPUEfctv: "1" })].join("\n"),
  }));
  expect(inventory.nodes.map((entry) => entry.name)).toEqual(["n1"]);
  for (const constraint of ["fast|avx512", "[fast&avx512]", "fast*2", "fast&&avx512", "fast &avx512", "", "(fast)"]) {
    expect(() => parseInventory(gpu({ constraint }), source())).toThrow("Only a feature");
  }
  expect(() => parseInventory(gpu({ constraint: "missing" }), source())).toThrow("No nodes match");
});

test("configured CPUs prefer CPUEfctv, fall back only to CfgTRES cpu and never CPUTot/free CPUs", () => {
  const profile = gpu();
  const effective = parseInventory(profile, source({ nodes: node("n1", { CPUEfctv: "48", CPUAlloc: "47" }) }));
  expect(sizeResources(profile, effective, { walltime_seconds: 300 }).cpus).toBe(6);
  const fallback = parseInventory(profile, source({ nodes: node("n1", { CPUEfctv: undefined, CPUTot: "999" }) }));
  expect(sizeResources(profile, fallback, { walltime_seconds: 300 }).cpus).toBe(8);
  expect(() => parseInventory(profile, source({ nodes: node("n1", { CPUEfctv: undefined, CfgTRES: "gres/gpu=8", CPUTot: "64" }) })))
    .toThrow("configured schedulable CPUs");
});

test("binary memory normalization and MemSpecLimit reserve are applied before ratios", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source({ nodes: node("n1", { RealMemory: "512G", MemSpecLimit: "8GiB" }) }));
  expect(inventory.nodes[0]!.memory_mb).toBe(516096);
  expect(inventory.nodes[0]!.reserved_memory_mb).toBe(8192);
  expect(sizeResources(profile, inventory, { walltime_seconds: 300 }).memory_mb).toBe(64512);
  const noReserve = parseInventory(profile, source({ nodes: node("n1", { MemSpecLimit: undefined }) }));
  expect(sizeResources(profile, noReserve, { walltime_seconds: 300 }).memory_mb).toBe(65536);
});

test("CPU targets default to 1 CPU/4096MiB and emit no GPU flag", () => {
  const profile = cpu();
  const inventory = parseInventory(profile, source({
    sinfo: "NodeName=n1 Partition=cpu State=idle",
    nodes: node("n1", { Partitions: "cpu", Gres: "(null)", CfgTRES: "cpu=64,mem=524288M", AllocTRES: "cpu=0,mem=0M" }),
    partition: "PartitionName=cpu MaxTime=01:00:00",
  }));
  const result = sizeResources(profile, inventory, { walltime_seconds: 300 });
  expect([result.gpu_count, result.cpus, result.memory_mb]).toEqual([0, 1, 4096]);
  expect(result.sbatch_flags.some((flag) => flag.includes("gpu"))).toBe(false);
  expect(sizeResources(profile, inventory, { gpu_count: 0, cpus: 3, memory_mb: 6145, walltime_seconds: 300 }))
    .toMatchObject({ gpu_count: 0, cpus: 3, memory_mb: 6145 });
  expect(() => sizeResources(profile, inventory, { gpu_count: 1, walltime_seconds: 300 })).toThrow("CPU targets require");
  expect(inventory.availability.total_gpus).toBe(0);
});

test("profile/partition caps CPU first, then RAM and final-CPU per-CPU RAM, rounding only computed memory", () => {
  const profile = gpu({ max_cpus: 6, max_memory_mb: 50000 });
  const inventory = parseInventory(profile, source({ partition: "PartitionName=gpu MaxCPUsPerNode=4 MaxMemPerNode=32G MaxMemPerCPU=6001 MaxTime=30:00" }));
  const result = sizeResources(profile, inventory, { walltime_seconds: 600 });
  expect([result.cpus, result.memory_mb]).toEqual([4, 23808]);
  expect(result.evidence.defaults).toEqual({ cpus: 8, memory_mb: 65536 });
  expect(result.evidence.limits).toEqual({ max_cpus: 4, max_memory_mb: 32768, max_memory_per_cpu_mb: 6001, max_walltime_seconds: 1800 });
  expect(sizeResources(profile, inventory, { cpus: 2, walltime_seconds: 600 }).memory_mb).toBe(11776);
  expect(sizeResources(profile, inventory, { memory_mb: 24004, walltime_seconds: 600 }).memory_mb).toBe(24004);
  expect(() => sizeResources(profile, inventory, { cpus: 5, walltime_seconds: 600 })).toThrow("Explicit cpus");
  expect(() => sizeResources(profile, inventory, { memory_mb: 24005, walltime_seconds: 600 })).toThrow("Explicit memory_mb");
  expect(() => sizeResources(profile, inventory, { cpus: 2, memory_mb: 20000, walltime_seconds: 600 })).toThrow("final cpus=2");
});

test("profile-only limits cap defaults, but reject explicit requirements above those limits", () => {
  const profile = gpu({ max_cpus: 3, max_memory_mb: 17000, max_walltime_seconds: 700 });
  const inventory = parseInventory(profile, source());
  expect(sizeResources(profile, inventory, { walltime_seconds: 700 })).toMatchObject({ cpus: 3, memory_mb: 16896 });
  expect(() => sizeResources(profile, inventory, { cpus: 4, walltime_seconds: 600 })).toThrow("Explicit cpus");
  expect(() => sizeResources(profile, inventory, { memory_mb: 17001, walltime_seconds: 600 })).toThrow("Explicit memory_mb");
  expect(() => sizeResources(profile, inventory, { walltime_seconds: 701 })).toThrow("walltime_seconds");
});

test("documented unlimited/unset partition fields impose no cap", () => {
  const profile = gpu();
  for (const value of ["UNLIMITED", "UNDEF", "(null)", "0"]) {
    const inventory = parseInventory(profile, source({ partition: `PartitionName=gpu MaxCPUsPerNode=${value} MaxMemPerNode=${value} MaxMemPerCPU=${value} MaxTime=${value}` }));
    expect(sizeResources(profile, inventory, { walltime_seconds: 600 })).toMatchObject({ cpus: 8, memory_mb: 65536 });
  }
});

test("tiny memory caps or ratios cannot produce --mem=0", () => {
  const profile = gpu();
  const tinyCap = parseInventory(profile, source({ partition: "PartitionName=gpu MaxMemPerNode=255 MaxTime=01:00:00" }));
  expect(() => sizeResources(profile, tinyCap, { walltime_seconds: 300 })).toThrow("capped memory_mb");
  expect(sizeResources(profile, tinyCap, { memory_mb: 255, walltime_seconds: 300 }).memory_mb).toBe(255);
  const tinyNode = parseInventory(profile, source({ nodes: node("n1", { RealMemory: "1024" }) }));
  expect(() => sizeResources(profile, tinyNode, { walltime_seconds: 300 })).toThrow("memory_mb");
});

test("explicit requests must fit one matching node jointly, not disjoint CPU and RAM maxima", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source({
    sinfo: "NodeName=n1 Partition=gpu State=idle\nNodeName=n2 Partition=gpu State=idle",
    nodes: [node("n1", { CPUEfctv: "64", RealMemory: "65536" }), node("n2", { CPUEfctv: "16", RealMemory: "524288" })].join("\n"),
  }));
  expect(() => sizeResources(profile, inventory, { cpus: 32, memory_mb: 131072, walltime_seconds: 300 })).toThrow("No matching node can fit");
  expect(sizeResources(profile, inventory, { cpus: 32, memory_mb: 65536, walltime_seconds: 300 }).evidence.fitting_nodes).toEqual(["n1"]);
  expect(() => sizeResources(profile, inventory, { cpus: 65, walltime_seconds: 300 })).toThrow("No matching node can fit");
  expect(() => sizeResources(profile, inventory, { memory_mb: 524289, walltime_seconds: 300 })).toThrow("No matching node can fit");
});

test("walltime is explicit, bounded by profile/partition, and emitted without rounding to partition maximum", () => {
  const profile = gpu({ max_walltime_seconds: 200000 });
  const inventory = parseInventory(profile, source({ partition: "PartitionName=gpu MaxTime=1-02:03:04" }));
  expect(inventory.limits.max_walltime_seconds).toBe(93784);
  const result = sizeResources(profile, inventory, { walltime_seconds: 93784 });
  expect(result.sbatch_flags.at(-1)).toBe("--time=1-02:03:04");
  expect(() => sizeResources(profile, inventory, { walltime_seconds: 93785 })).toThrow("exceeds maximum");
  for (const value of [0, -1, 0.5, NaN, Infinity]) {
    expect(() => sizeResources(profile, inventory, { walltime_seconds: value })).toThrow("walltime_seconds");
  }
  expect(() => sizeResources(profile, inventory, {} as { walltime_seconds: number })).toThrow("walltime_seconds");
  for (const MaxTime of ["00:60:00", "1-24:00:00", "not-a-time", "-5"]) {
    expect(() => parseInventory(profile, source({ partition: `PartitionName=gpu MaxTime=${MaxTime}` }))).toThrow(SlurmResourceError);
  }
});

test("GPU bounds and integer safety are enforced before flags are constructed", () => {
  const profile = gpu({ max_gpus: 4 });
  const inventory = parseInventory(profile, source());
  for (const value of [0, -1, 5, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
    expect(() => sizeResources(profile, inventory, { gpu_count: value, walltime_seconds: 300 })).toThrow(SlurmResourceError);
  }
  for (const key of ["cpus", "memory_mb"] as const) {
    for (const value of [0, -1, 0.5, NaN, Infinity, Number.MAX_SAFE_INTEGER + 1]) {
      expect(() => sizeResources(profile, inventory, { [key]: value, walltime_seconds: 300 })).toThrow(SlurmResourceError);
    }
  }
  const tooFew = parseInventory(profile, source({ nodes: node("n1", { Gres: "gpu:a100:1", CfgTRES: "cpu=64,gres/gpu=1" }) }));
  expect(() => sizeResources(profile, tooFew, { gpu_count: 2, walltime_seconds: 300 })).toThrow("No matching node");
});

test("missing, zero, malformed and unsafe configured capacity fails instead of guessing", () => {
  for (const overrides of [
    { CPUEfctv: "0" }, { CPUEfctv: "unknown" }, { CPUEfctv: "9007199254740992" },
    { RealMemory: undefined }, { RealMemory: "0" }, { RealMemory: "unknown" }, { RealMemory: "1.1M" },
    { RealMemory: "9007199254740992" }, { MemSpecLimit: "524288" }, { MemSpecLimit: "524289" }, { MemSpecLimit: "unknown" },
  ]) {
    expect(() => parseInventory(gpu(), source({ nodes: node("n1", overrides) }))).toThrow(SlurmResourceError);
  }
  expect(() => parseInventory(gpu(), source({ nodes: node("n1", { Gres: undefined }) }))).toThrow("GPU GRES");
});

test("ambiguous, conflicting, untyped and MIG GPU descriptions fail closed", () => {
  for (const Gres of [
    "gpu:8", "gpu:a100:0", "gpu:a100:4,gpu:a100:4", "gpu:a100:8(S:0-3", "gpu:a100:8(S:0-3))",
    "gpu:a100_mig:8", "gpu:a100_1g.10gb:8", "gpu:a100:8,gpu:1g.5gb:8", "gpu:a100:8,gpu:4",
  ]) {
    expect(() => parseInventory(gpu(), source({ nodes: node("n1", { Gres }) }))).toThrow(SlurmResourceError);
  }
  expect(() => parseInventory(gpu(), source({ nodes: node("n1", { CfgTRES: "cpu=64,gres/gpu=7" }) }))).toThrow("disagree");
  expect(() => parseInventory(gpu(), source({ nodes: node("n1", { CfgTRES: "cpu=64,gres/gpu:a100=7" }) }))).toThrow("disagree");
  expect(() => parseInventory(gpu(), source({ nodes: node("n1", { CfgTRES: "cpu=64,cpu=64" }) }))).toThrow("duplicate TRES");
});

test("missing inventory, inconsistent membership and terminal columns are not accepted as evidence", () => {
  for (const changes of [
    { sinfo: "" }, { nodes: "" }, { partition: "" }, { sinfo: "n1 gpu idle" },
    { sinfo: "NodeName=n[1-2] Partition=gpu State=idle" },
    { sinfo: "NodeName=n1 Partition=gpu-debug State=idle" },
    { nodes: node("n1", { Partitions: "gpu-debug" }) },
    { nodes: `${node()}\n${node()}` },
    { partition: "PartitionName=gpu\nPartitionName=gpu" },
    { observed_at: "yesterday" },
  ]) {
    expect(() => parseInventory(gpu(), source(changes))).toThrow(SlurmResourceError);
  }
  const inventory = parseInventory(gpu(), source());
  expect(() => sizeResources(gpu({ partition: "gpu-debug" }), inventory, { walltime_seconds: 300 })).toThrow("another partition");
});

test("unavailable nodes remain in sizing but contribute no currently free capacity", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source({ nodes: node("n1", { State: "IDLE+DRAIN" }) }));
  expect(sizeResources(profile, inventory, { walltime_seconds: 300 })).toMatchObject({ cpus: 8, memory_mb: 65536 });
  expect(inventory.availability).toMatchObject({ idle_nodes: 0, unavailable_nodes: 1, free_gpus: 0, free_cpus: 0, free_memory_mb: 0 });
});

test("missing allocation observations stay unknown rather than being advertised immediately free", () => {
  const inventory = parseInventory(gpu(), source({ nodes: node("n1", { CPUAlloc: undefined, AllocMem: undefined, AllocTRES: undefined }) }));
  expect(inventory.availability.free_gpus).toBeUndefined();
  expect(inventory.availability.free_cpus).toBeUndefined();
  expect(inventory.availability.free_memory_mb).toBeUndefined();
  expect(sizeResources(gpu(), inventory, { walltime_seconds: 300 })).toMatchObject({ cpus: 8, memory_mb: 65536 });
});

test("typed allocation accounting does not infer per-type free GPUs from mixed-type aggregate allocation", () => {
  const inventory = parseInventory(gpu(), source({ nodes: node("n1", {
    Gres: "gpu:a100:4,gpu:v100:4", CfgTRES: "cpu=64,gres/gpu=8", AllocTRES: "cpu=8,mem=4096M,gres/gpu=4",
  }) }));
  expect(inventory.availability.free_gpus).toBeUndefined();
});

test("ginfo is optional bounded supplemental text and can never change resource sizing", () => {
  const profile = gpu();
  const normal = parseInventory(profile, source());
  const optional = parseInventory(profile, source({ ginfo: "unrelated human columns: 1 free GPU, 3 CPUs, 2G RAM" }));
  expect(sizeResources(profile, optional, { walltime_seconds: 300 })).toEqual(sizeResources(profile, normal, { walltime_seconds: 300 }));
  expect(normal.ginfo).toBeUndefined();
  expect(optional.ginfo).toEqual({ output: "unrelated human columns: 1 free GPU, 3 CPUs, 2G RAM", truncated: false });
  const long = parseInventory(profile, source({ ginfo: Array.from({ length: 100 }, () => "x".repeat(300)).join("\n") }));
  expect(long.ginfo!.truncated).toBe(true);
  expect(long.ginfo!.output.length).toBeLessThanOrEqual(16384);
  expect(long.ginfo!.output.split("\n").length).toBeLessThanOrEqual(80);
});

test("CPU preparation limits use final CPU count and reject requests that fit no node", () => {
  const profile = cpu({ max_cpus: 4, max_memory_mb: 3000 });
  const inventory = parseInventory(profile, source({
    sinfo: "NodeName=n1 Partition=cpu State=idle",
    nodes: node("n1", { Partitions: "cpu", CPUEfctv: "2", RealMemory: "8192", Gres: "(null)", CfgTRES: "cpu=2", AllocTRES: "cpu=0,mem=0M" }),
    partition: "PartitionName=cpu MaxCPUsPerNode=2 MaxMemPerCPU=2500 MaxTime=01:00:00",
  }));
  expect(sizeResources(profile, inventory, { walltime_seconds: 300 })).toMatchObject({ cpus: 1, memory_mb: 2304, gpu_count: 0 });
  expect(() => sizeResources(profile, inventory, { memory_mb: 2501, walltime_seconds: 300 })).toThrow("Explicit memory_mb");
  expect(sizeResources(profile, inventory, { cpus: 2, memory_mb: 3000, walltime_seconds: 300 })).toMatchObject({ cpus: 2, memory_mb: 3000 });
  expect(() => sizeResources(profile, inventory, { cpus: 3, walltime_seconds: 300 })).toThrow("Explicit cpus");
});

test("snapshot allocation inconsistencies never become configured sizing inputs", () => {
  const profile = gpu();
  const inventory = parseInventory(profile, source({ nodes: node("n1", {
    CPUAlloc: "65", AllocMem: "600000", AllocTRES: "cpu=65,mem=600000M,gres/gpu=9",
  }) }));
  expect(sizeResources(profile, inventory, { walltime_seconds: 300 })).toMatchObject({ cpus: 8, memory_mb: 65536 });
  expect(inventory.availability.free_cpus).toBeUndefined();
  expect(inventory.availability.free_memory_mb).toBeUndefined();
  expect(inventory.availability.free_gpus).toBeUndefined();
});

test("supported Slurm partition durations and binary memory units normalize without guessing", () => {
  const profile = gpu({ max_walltime_seconds: 1_000_000 });
  for (const [value, seconds] of [["60", 3600], ["60:01", 3601], ["2-03", 183600], ["2-03:04", 183840], ["02:03:04", 7384]] as const) {
    const inventory = parseInventory(profile, source({ partition: `PartitionName=gpu MaxTime=${value}` }));
    expect(inventory.limits.max_walltime_seconds).toBe(seconds);
  }
  const inventory = parseInventory(profile, source({
    nodes: node("n1", { RealMemory: "536870912K", MemSpecLimit: "0" }),
    partition: "PartitionName=gpu MaxMemPerNode=0.25T MaxTime=UNLIMITED",
  }));
  expect(inventory.nodes[0]!.memory_mb).toBe(524288);
  expect(inventory.limits.max_memory_mb).toBe(262144);
});
