import { afterEach, beforeEach, expect, test } from "bun:test";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { ClusterConfigError, loadClusterProfiles, parseClusterProfiles, resolveClusterConfigPath, sharedPath, type Environment } from "./profiles.ts";

let directory: string;
let configPath: string;

beforeEach(async () => {
  directory = await mkdtemp(join(tmpdir(), "wh-cluster-profiles-"));
  configPath = join(directory, "clusters.json");
});

afterEach(async () => {
  await rm(directory, { recursive: true, force: true });
});

function gpu(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: "ibex-gpu",
    description: "Preemptible research GPUs; account and utilization limits apply.",
    ssh_host: "ibex-login",
    shared_root: "/cluster/shared/research group",
    sif_path: "/cluster/shared/research group/wh.sif",
    compute_kind: "gpu",
    gpu_type: "a100_80gb",
    runtime: "apptainer",
    account: "research",
    partition: "configured-gpu-partition",
    max_gpus: 8,
    max_walltime_seconds: 3600,
    failure_grace_seconds: 300,
    termination_grace_seconds: 30,
    tailnet_key_file: "/home/user/.config/worker-harness/cluster key",
    tailnet_login_server: "https://control.example.test",
    orchestrator_host: "wh-orchestrator.tailnet.test",
    ...overrides,
  };
}

function cpu(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  const profile = gpu({
    id: "ibex-cpu",
    compute_kind: "cpu",
    partition: "configured-cpu-partition",
    max_gpus: 0,
    failure_grace_seconds: 0,
    ...overrides,
  });
  delete profile.gpu_type;
  return profile;
}

async function save(clusters: unknown = [gpu()], extra: Record<string, unknown> = {}): Promise<void> {
  await writeFile(configPath, JSON.stringify({ version: 1, clusters, ...extra }));
}

async function invalid(field?: string): Promise<ClusterConfigError> {
  const result = await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath }).then(
    () => undefined,
    (error: unknown) => error,
  );
  expect(result).toBeInstanceOf(ClusterConfigError);
  const error = result as ClusterConfigError;
  expect(error.code).toBe("CLUSTER_CONFIG_INVALID");
  expect(error.configPath).toBe(configPath);
  if (field !== undefined) expect(error.field).toBe(field);
  expect(error.message).toContain(configPath);
  return error;
}

test("public config resolver preserves explicit override, XDG and HOME precedence", () => {
  const env: Environment = { HOME: directory, XDG_CONFIG_HOME: join(directory, "xdg"), WH_CLUSTER_CONFIG: "relative/clusters.json" };
  expect(resolveClusterConfigPath(env)).toBe(resolve("relative/clusters.json"));
  expect(resolveClusterConfigPath({ WH_CLUSTER_CONFIG: configPath })).toBe(configPath);
  expect(resolveClusterConfigPath({ HOME: directory, XDG_CONFIG_HOME: join(directory, "xdg") })).toBe(join(directory, "xdg", "worker-harness", "clusters.json"));
  expect(resolveClusterConfigPath({ HOME: directory, XDG_CONFIG_HOME: "" })).toBe(join(directory, ".config", "worker-harness", "clusters.json"));
  expect(resolveClusterConfigPath({ HOME: directory })).toBe(join(directory, ".config", "worker-harness", "clusters.json"));
});

test("public parser and loader produce the same normalized defaults and optional SSH configuration", async () => {
  const document = { version: 1, clusters: [
    gpu({ storage_id: "ibex", shared_root: "/shared//ibex/", ssh_config_file: "/home/user//ssh configs/operator's config/" }),
    cpu({ storage_id: "ibex", shared_root: "/shared/ibex" }),
  ] };
  const parsed = parseClusterProfiles(document);
  expect(parsed[0]).toEqual({
    ...gpu({ storage_id: "ibex", shared_root: "/shared/ibex", ssh_config_file: "/home/user/ssh configs/operator's config" }),
    runtime_setup: "", warning_seconds: 120, orchestrator_registration_port: 12888, poll_seconds: 15,
  });
  expect(parsed[1]).toMatchObject({ compute_kind: "cpu", max_gpus: 0, failure_grace_seconds: 0 });
  expect(parsed[1]).not.toHaveProperty("gpu_type");
  expect(parsed[1]).not.toHaveProperty("ssh_config_file");
  expect(parseClusterProfiles({ version: 1, clusters: [] })).toEqual([]);
  await writeFile(configPath, JSON.stringify(document));
  expect(await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath })).toEqual(parsed);
  expect(document.clusters[0].ssh_config_file).toBe("/home/user//ssh configs/operator's config/");
});

for (const [document, field] of [
  [null, "config"],
  [{ version: 2, clusters: [] }, "version"],
  [{ version: 1, clusters: [], unexpected: true }, "unexpected"],
  [{ version: 1, clusters: {} }, "clusters"],
  [{ version: 1, clusters: [null] }, "clusters[0]"],
  [{ version: 1, clusters: [gpu({ ssh_options: ["-o", "ProxyCommand=unsafe"] })] }, "clusters[0].ssh_options"],
  [{ version: 1, clusters: [gpu(), gpu()] }, "clusters[1].id"],
  [{ version: 1, clusters: [gpu({ storage_id: "ibex" }), cpu({ storage_id: "ibex", shared_root: "/other" })] }, "clusters[1].shared_root"],
  [{ version: 1, clusters: [{ ...cpu(), gpu_type: "a100" }] }, "clusters[0].gpu_type"],
  [{ version: 1, clusters: [gpu({ ssh_config_file: "relative/config" })] }, "clusters[0].ssh_config_file"],
] as const) {
  test(`public parser keeps strict schema diagnostics for ${field}: ${JSON.stringify(document)}`, () => {
    for (const path of [undefined, configPath]) {
      let failure: unknown;
      try { parseClusterProfiles(document, path); } catch (error) { failure = error; }
      expect(failure).toBeInstanceOf(ClusterConfigError);
      expect(failure).toMatchObject({ code: "CLUSTER_CONFIG_INVALID", field });
      expect((failure as ClusterConfigError).configPath).toBe(path);
      expect((failure as ClusterConfigError).cause).toBeUndefined();
    }
  });
}

test("absent default and explicitly selected files mean no configured targets", async () => {
  expect(await loadClusterProfiles({ HOME: directory })).toEqual([]);
  expect(await loadClusterProfiles({ XDG_CONFIG_HOME: directory })).toEqual([]);
  expect(await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath })).toEqual([]);
});

test("HOME fallback and XDG configuration use the worker-harness namespace", async () => {
  const homePath = join(directory, ".config", "worker-harness", "clusters.json");
  await mkdir(dirname(homePath), { recursive: true });
  await writeFile(homePath, JSON.stringify({ version: 1, clusters: [gpu({ id: "home" })] }));
  const xdg = join(directory, "xdg");
  const xdgPath = join(xdg, "worker-harness", "clusters.json");
  await mkdir(dirname(xdgPath), { recursive: true });
  await writeFile(xdgPath, JSON.stringify({ version: 1, clusters: [gpu({ id: "xdg" })] }));
  expect((await loadClusterProfiles({ HOME: directory }))[0].id).toBe("home");
  expect((await loadClusterProfiles({ HOME: directory, XDG_CONFIG_HOME: "" }))[0].id).toBe("home");
  expect((await loadClusterProfiles({ HOME: directory, XDG_CONFIG_HOME: xdg }))[0].id).toBe("xdg");
  await save([gpu({ id: "override" })]);
  expect((await loadClusterProfiles({ HOME: directory, XDG_CONFIG_HOME: xdg, WH_CLUSTER_CONFIG: configPath }))[0].id).toBe("override");
});

test("explicit missing config never falls back to a different target list", async () => {
  const fallback = join(directory, "worker-harness", "clusters.json");
  await mkdir(dirname(fallback), { recursive: true });
  await writeFile(fallback, JSON.stringify({ version: 1, clusters: [gpu()] }));
  expect(await loadClusterProfiles({ XDG_CONFIG_HOME: directory, WH_CLUSTER_CONFIG: configPath })).toEqual([]);
});

test("loader resolves every default and preserves explicit optional policy", async () => {
  await save();
  const [profile] = await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath });
  expect(profile).toEqual({
    ...gpu(), storage_id: "ibex-gpu", runtime_setup: "", warning_seconds: 120,
    orchestrator_registration_port: 12888, poll_seconds: 15,
  });
  expect(sharedPath(profile)).toBe("/data/shared/ibex-gpu");
  await save([gpu({
    storage_id: "IBEX_1", runtime: "singularity", runtime_setup: "module purge\nmodule load singularity",
    qos: "research-qos", constraint: "a100&nvlink", max_cpus: 64, max_memory_mb: 524288,
    warning_seconds: 0, termination_grace_seconds: 0, orchestrator_registration_port: 12345, poll_seconds: 2,
  })]);
  const [explicit] = await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath });
  expect(explicit).toMatchObject({
    storage_id: "IBEX_1", runtime: "singularity", runtime_setup: "module purge\nmodule load singularity",
    qos: "research-qos", constraint: "a100&nvlink", max_cpus: 64, max_memory_mb: 524288,
    warning_seconds: 0, termination_grace_seconds: 0, orchestrator_registration_port: 12345, poll_seconds: 2,
  });
  expect(sharedPath(explicit)).toBe("/data/shared/IBEX_1");
});

test("CPU and GPU partition profiles share one canonical container path", async () => {
  await save([gpu({ storage_id: "ibex", shared_root: "/shared//ibex/" }), cpu({ storage_id: "ibex", shared_root: "/shared/ibex" })]);
  const profiles = await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath });
  expect(profiles.map(sharedPath)).toEqual(["/data/shared/ibex", "/data/shared/ibex"]);
  expect(profiles.map((profile) => profile.shared_root)).toEqual(["/shared/ibex", "/shared/ibex"]);
  expect(profiles[1]).toMatchObject({ compute_kind: "cpu", max_gpus: 0, failure_grace_seconds: 0 });
  expect(Object.hasOwn(profiles[1], "gpu_type")).toBeFalse();
});

test("same storage identity cannot point at conflicting cluster roots", async () => {
  await save([gpu({ storage_id: "ibex" }), cpu({ storage_id: "ibex", shared_root: "/other/filesystem" })]);
  expect((await invalid("clusters[1].shared_root")).message).toContain("storage_id ibex");
});

test("duplicate profile IDs fail even when their content matches", async () => {
  await save([gpu(), gpu()]);
  expect((await invalid("clusters[1].id")).message).toContain("duplicate cluster ID");
});

test("empty configured target lists are valid", async () => {
  await save([]);
  expect(await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath })).toEqual([]);
});

for (const raw of ["", "{", "null", "[]", "true", '"text"', '{"version":1}', '{"version":1,"clusters":{}}', '{"version":2,"clusters":[]}', '{"version":"1","clusters":[]}']) {
  test(`malformed or unsupported configuration fails explicitly: ${raw}`, async () => {
    await writeFile(configPath, raw);
    await invalid();
  });
}

test("unknown root and profile fields fail instead of hiding misspelled policy", async () => {
  await save([], { cluster: [] });
  await invalid("cluster");
  await save([gpu({ max_gpu: 4 })]);
  await invalid("clusters[0].max_gpu");
  await save([gpu({ constructor: "not a profile field" })]);
  await invalid("clusters[0].constructor");
});

for (const entry of [null, [], false, "cluster"]) {
  test(`non-object cluster entries fail: ${JSON.stringify(entry)}`, async () => {
    await save([entry]);
    await invalid("clusters[0]");
  });
}

for (const field of ["id", "description", "ssh_host", "shared_root", "sif_path", "compute_kind", "gpu_type", "runtime", "account", "partition", "max_gpus", "max_walltime_seconds", "failure_grace_seconds", "termination_grace_seconds", "tailnet_key_file", "tailnet_login_server", "orchestrator_host"]) {
  test(`required profile field is not silently defaulted: ${field}`, async () => {
    const profile = gpu();
    delete profile[field];
    await save([profile]);
    await invalid(`clusters[0].${field}`);
  });
}

for (const [field, value] of [
  ["compute_kind", "accelerator"], ["runtime", "docker"], ["gpu_type", ""], ["gpu_type", "a100:2"],
  ["id", "cluster:ibex"], ["id", "../ibex"], ["storage_id", ".."], ["storage_id", "a/b"],
  ["description", ""], ["runtime_setup", "echo before\0echo after"], ["runtime_setup", 42],
  ["account", "account\n#SBATCH --exclusive"], ["partition", "gpu --exclusive"], ["qos", ""], ["constraint", "x\ny"],
  ["ssh_config_file", null], ["ssh_config_file", 42], ["ssh_config_file", true], ["ssh_config_file", []], ["ssh_config_file", {}],
] as const) {
  test(`invalid profile value fails: ${field}=${JSON.stringify(value)}`, async () => {
    await save([gpu({ [field]: value })]);
    await invalid(`clusters[0].${field}`);
  });
}

for (const [field, minimum] of [
  ["max_gpus", 1], ["max_walltime_seconds", 1], ["max_cpus", 1], ["max_memory_mb", 1],
  ["failure_grace_seconds", 0], ["warning_seconds", 0], ["termination_grace_seconds", 0],
  ["orchestrator_registration_port", 1], ["poll_seconds", 1],
] as const) {
  for (const value of [minimum - 1, 0.5, "15", null, Number.MAX_SAFE_INTEGER + 1]) {
    test(`integer policy rejects ${field}=${JSON.stringify(value)}`, async () => {
      await save([gpu({ [field]: value })]);
      await invalid(`clusters[0].${field}`);
    });
  }
}

test("registration port cannot exceed the TCP port range", async () => {
  await save([gpu({ orchestrator_registration_port: 65536 })]);
  await invalid("clusters[0].orchestrator_registration_port");
});

for (const extra of [{ max_gpus: 1 }, { max_gpus: -1 }, { gpu_type: "a100" }, { gpu_type: "" }, { gpu_type: null }]) {
  test(`CPU profiles reject incompatible GPU fields: ${JSON.stringify(extra)}`, async () => {
    await save([{ ...cpu(), ...extra }]);
    await invalid("clusters[0]." + ("gpu_type" in extra ? "gpu_type" : "max_gpus"));
  });
}

for (const ssh_host of ["-oProxyCommand=bad", "user@ibex", "ssh://ibex", "ibex:22", "ibex other", "ibex;touch bad", "$(hostname)", "ibex\nother", ""]) {
  test(`SSH aliases cannot carry options, shell syntax or destinations: ${JSON.stringify(ssh_host)}`, async () => {
    await save([gpu({ ssh_host })]);
    await invalid("clusters[0].ssh_host");
  });
}

for (const field of ["shared_root", "sif_path", "tailnet_key_file", "ssh_config_file"]) {
  for (const value of ["", "relative/path", "~/shared", "/", "///", "/shared/../other", "/shared/./file", "/shared\nfile", "/shared\0file", "/shared\u007ffile"]) {
    test(`unsafe profile path fails: ${field}=${JSON.stringify(value)}`, async () => {
      await save([gpu({ [field]: value })]);
      await invalid(`clusters[0].${field}`);
    });
  }
}

for (const value of ["/shared:other", "/shared,other", "/shared;other"]) {
  test(`shared-root bind delimiters fail: ${value}`, async () => {
    await save([gpu({ shared_root: value })]);
    await invalid("clusters[0].shared_root");
  });
}

test("ordinary spaces and quoted filenames are data, not forbidden path characters", async () => {
  await save([gpu({ shared_root: "/shared/research group's files", sif_path: "/shared/research group's files/worker image.sif" })]);
  const [profile] = await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath });
  expect(profile.shared_root).toBe("/shared/research group's files");
  expect(profile.sif_path).toBe("/shared/research group's files/worker image.sif");
});

for (const [field, value] of [
  ["tailnet_login_server", "not-a-url"], ["tailnet_login_server", "file:///etc/passwd"],
  ["tailnet_login_server", "https://user:secret@example.test"], ["tailnet_login_server", "https://example.test?secret=key"],
  ["tailnet_login_server", "https://example.test#fragment"], ["orchestrator_host", "http://wh.test"],
  ["orchestrator_host", "wh.test:12888"], ["orchestrator_host", "wh.test/route"], ["orchestrator_host", "-bad"],
] as const) {
  test(`invalid endpoint fails: ${field}=${value}`, async () => {
    await save([gpu({ [field]: value })]);
    await invalid(`clusters[0].${field}`);
  });
}

for (const orchestrator_host of ["100.64.0.1", "fd7a:115c:a1e0::1", "wh-orchestrator"]) {
  test(`orchestrator accepts hostname or IP literal: ${orchestrator_host}`, async () => {
    await save([gpu({ orchestrator_host })]);
    expect((await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath }))[0].orchestrator_host).toBe(orchestrator_host);
  });
}

test("non-file configuration errors are typed and preserve their cause", async () => {
  const error = await loadClusterProfiles({ WH_CLUSTER_CONFIG: directory }).catch((cause: unknown) => cause);
  expect(error).toBeInstanceOf(ClusterConfigError);
  expect((error as ClusterConfigError).code).toBe("CLUSTER_CONFIG_READ_FAILED");
  expect((error as ClusterConfigError).cause).toBeDefined();
});

test("malformed JSON preserves diagnostic cause without echoing credential contents", async () => {
  await writeFile(configPath, '{"credential":"do-not-echo"');
  const error = await invalid();
  expect(error.cause).toBeInstanceOf(SyntaxError);
  expect(error.message).not.toContain("do-not-echo");
});

for (const env of [{ WH_CLUSTER_CONFIG: "" }, { WH_CLUSTER_CONFIG: "bad\npath" }, { XDG_CONFIG_HOME: "relative" }, {}]) {
  test(`invalid config-location environment fails explicitly: ${JSON.stringify(env)}`, async () => {
    let resolutionError: unknown;
    try { resolveClusterConfigPath(env); } catch (error) { resolutionError = error; }
    expect(resolutionError).toBeInstanceOf(ClusterConfigError);
    const error = await loadClusterProfiles(env).catch((cause: unknown) => cause);
    expect(error).toBeInstanceOf(ClusterConfigError);
    expect((error as ClusterConfigError).code).toBe("CLUSTER_CONFIG_INVALID");
    expect((error as ClusterConfigError).configPath).toBeUndefined();
    expect((error as ClusterConfigError).field).toBe((resolutionError as ClusterConfigError).field);
    expect((error as ClusterConfigError).message).toBe((resolutionError as ClusterConfigError).message);
  });
}

test("finite integer limits accept their documented boundary values", async () => {
  await save([gpu({
    max_gpus: 1, max_cpus: 1, max_memory_mb: 1, max_walltime_seconds: 1,
    failure_grace_seconds: 0, warning_seconds: 0, termination_grace_seconds: 0,
    poll_seconds: 1, orchestrator_registration_port: 65535,
  })]);
  const [profile] = await loadClusterProfiles({ WH_CLUSTER_CONFIG: configPath });
  expect(profile).toMatchObject({
    max_gpus: 1, max_cpus: 1, max_memory_mb: 1, max_walltime_seconds: 1,
    failure_grace_seconds: 0, warning_seconds: 0, termination_grace_seconds: 0,
    poll_seconds: 1, orchestrator_registration_port: 65535,
  });
});

for (const field of ["max_gpus", "max_walltime_seconds", "failure_grace_seconds", "termination_grace_seconds"]) {
  test(`JSON numbers overflowing to infinity fail: ${field}`, async () => {
    const serialized = JSON.stringify({ version: 1, clusters: [gpu({ [field]: "overflow-marker" })] });
    await writeFile(configPath, serialized.replace('"overflow-marker"', "1e400"));
    await invalid(`clusters[0].${field}`);
  });
}
