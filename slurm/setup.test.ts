import { afterEach, beforeEach, expect, test } from "bun:test";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, readlink, realpath, rename, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { loadClusterProfiles, sharedPath, type ClusterProfile, type Environment } from "./profiles.ts";
import { inspectSystemSshConfig, runClusterAdd, runClusterSshMigrate, type SetupUI } from "./setup.ts";

let root: string;
let home: string;
let configPath: string;
let env: Environment;
const enrollmentSecret = "test-enrollment-secret-never-publish";

beforeEach(async () => {
  // Exercise shell quoting, OpenSSH Include quoting and literal IdentityFile % handling.
  root = await mkdtemp(join(tmpdir(), "wh-setup space %' "));
  home = join(root, "operator home %'");
  configPath = join(home, ".config", "worker-harness", "clusters.json");
  env = { HOME: home, WH_CLUSTER_CONFIG: configPath };
  await mkdir(join(home, ".ssh"), { recursive: true, mode: 0o700 });
  await writeFile(join(home, ".ssh", "config"), [
    "Host site-login",
    "  HostName 192.0.2.42",
    "  User site-user",
    "  Port 2202",
    "  ProxyJump gateway.example.test",
    "  ControlMaster auto",
    "  ControlPersist 5m",
    "",
  ].join("\n"), { mode: 0o600 });
  await writeFile(join(home, "tailnet enrollment key"), enrollmentSecret, { mode: 0o600 });
});

afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

type Reply = string | undefined | (string | undefined)[];
type Prompt = { kind: "input" | "select" | "editor" | "confirm"; title: string; current?: string; options?: string[] };
type ScriptOptions = {
  replies?: Record<string, Reply>;
  confirm?: boolean;
  beforePrompt?: (prompt: Prompt) => void | Promise<void>;
};

/** A consumer UI, not a mock of parsing, persistence, subprocesses or transport. */
class WizardUI implements SetupUI {
  readonly prompts: Prompt[] = [];
  readonly notices: { message: string; type?: string }[] = [];
  readonly published: string[] = [];
  readonly scriptErrors: string[] = [];
  private readonly counts = new Map<string, number>();
  private readonly replies: Record<string, Reply>;

  constructor(readonly id: string, private readonly options: ScriptOptions = {}) {
    this.replies = {
      "Cluster ID": id,
      "Reuse an existing": "New connection and storage",
      "Name / description": "Research CPU partition; site utilization limits apply",
      "Compute kind": "cpu",
      "SSH connection": "Direct hostname",
      "SSH hostname / IP": "192.0.2.17",
      "SSH username": "researcher",
      "SSH port": "2222",
      "ProxyJump": "jump-user@gateway.example.test:2200",
      "Existing SSH alias": "site-login",
      "Shared storage ID": "research-storage",
      "Shared root": "/cluster//research group/",
      "Worker SIF": "/cluster//research group/worker.sif",
      "Container runtime": "apptainer",
      "Trusted runtime shell prelude": "module purge\nmodule load apptainer",
      "Slurm account": "research",
      "Slurm partition": "cpu-partition",
      "Slurm GPU type": "a100_80gb",
      "Maximum GPUs": "4",
      "Maximum walltime": "3600",
      "Failure diagnosis grace": "0",
      "Termination grace": "30",
      "Local Tailnet enrollment KEY FILE": join(home, "tailnet enrollment key"),
      "Tailnet login server": "https://control.example.test",
      "Orchestrator hostname": "orchestrator.example.test",
      "Advanced settings": "Use defaults (warning 120s, port 12888, poll 15s; no extra caps)",
      "QoS": "research-qos",
      "Feature constraint": "a100&nvlink",
      "Maximum CPUs": "32",
      "Maximum memory": "65536",
      "Signal warning": "0",
      "Orchestrator registration port": "12889",
      "Scheduler poll interval": "3",
      "Review cluster": "Save profile",
      ...options.replies,
    };
  }

  private async answer(prompt: Prompt): Promise<string | undefined> {
    this.prompts.push(prompt);
    await this.options.beforePrompt?.(prompt);
    const prefix = Object.keys(this.replies).find((key) => prompt.title.startsWith(key));
    if (prefix === undefined) return this.scriptFailure(`Unexpected ${prompt.kind} prompt: ${prompt.title}`);
    const reply = this.replies[prefix];
    const count = this.counts.get(prefix) ?? 0;
    this.counts.set(prefix, count + 1);
    const value = Array.isArray(reply) ? reply[count] : reply;
    if (Array.isArray(reply) && count >= reply.length) return this.scriptFailure(`Exhausted replies for ${prefix}`);
    if (value !== undefined && prompt.options && !prompt.options.includes(value)) return this.scriptFailure(`Unavailable answer ${value} for ${prompt.title}`);
    return value;
  }

  private scriptFailure(message: string): never {
    this.scriptErrors.push(message);
    throw new Error(message);
  }

  input(title: string, current?: string): Promise<string | undefined> { return this.answer({ kind: "input", title, current }); }
  select(title: string, options: string[]): Promise<string | undefined> { return this.answer({ kind: "select", title, options }); }
  editor(title: string, current?: string): Promise<string | undefined> { return this.answer({ kind: "editor", title, current }); }
  async confirm(title: string, message: string): Promise<boolean> {
    const prompt: Prompt = { kind: "confirm", title, current: message };
    this.prompts.push(prompt);
    await this.options.beforePrompt?.(prompt);
    return this.options.confirm ?? true;
  }
  notify(message: string, type?: string): void { this.notices.push({ message, type }); }
  publish = (content: string): void => { this.published.push(content); };

  run(args = this.id): Promise<ClusterProfile | undefined> { return runClusterAdd(this, this.publish, args, env); }
  matching(prefix: string): Prompt[] { return this.prompts.filter((prompt) => prompt.title.startsWith(prefix)); }
  expectScriptUsed(): void { expect(this.scriptErrors).toEqual([]); }
}

type TreeEntry = { path: string; mode: number; contents?: string; link?: string };
async function tree(directory = root): Promise<TreeEntry[]> {
  const entries: TreeEntry[] = [];
  async function visit(path: string): Promise<void> {
    const info = await lstat(path);
    const entry: TreeEntry = { path: relative(directory, path), mode: info.mode & 0o777 };
    if (info.isSymbolicLink()) entry.link = await readlink(path);
    else if (info.isFile()) entry.contents = (await readFile(path)).toString("base64");
    entries.push(entry);
    if (info.isDirectory()) for (const name of (await readdir(path)).sort()) await visit(join(path, name));
  }
  await visit(directory);
  return entries;
}

function baseline(id = "existing"): Record<string, unknown> {
  // Deliberately omit defaults and retain a noncanonical path: adding a profile must
  // not silently rewrite an operator's already-valid JSON entry.
  return {
    id, description: "Existing profile; preserve this entry", ssh_host: "existing-login",
    compute_kind: "cpu", shared_root: "/existing//shared/", sif_path: "/existing/worker.sif",
    runtime: "singularity", account: "old-account", partition: "old-cpu", max_gpus: 0,
    max_walltime_seconds: 1800, failure_grace_seconds: 0, termination_grace_seconds: 10,
    tailnet_key_file: join(home, "tailnet enrollment key"), tailnet_login_server: "https://old-control.example.test",
    orchestrator_host: "old-orchestrator.example.test",
  };
}

async function saveBaseline(clusters: unknown[] = [baseline()]): Promise<string> {
  await mkdir(dirname(configPath), { recursive: true, mode: 0o700 });
  const content = JSON.stringify({ version: 1, clusters }, null, 4) + "\n";
  await writeFile(configPath, content, { mode: 0o600 });
  return content;
}

async function document(): Promise<{ version: number; clusters: ClusterProfile[] }> {
  return JSON.parse(await readFile(configPath, "utf8"));
}

async function local(argv: string[]): Promise<string> {
  const child = Bun.spawn(argv, { stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  const [stdout, stderr, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
  expect({ argv, code, stderr: code === 0 ? "" : stderr }).toEqual({ argv, code: 0, stderr: "" });
  return stdout;
}

async function effectiveSSH(profile: ClusterProfile, file = profile.ssh_config_file!): Promise<Map<string, string>> {
  // -G only evaluates configuration locally; never connect, resolve an alias or submit work.
  const output = await local(["ssh", "-G", "-F", file, "-o", "CanonicalizeHostname=no", profile.ssh_host]);
  return new Map(output.trim().split("\n").map((line) => {
    const space = line.indexOf(" ");
    return [line.slice(0, space), line.slice(space + 1)];
  }));
}

function shellQuote(value: string): string { return "'" + value.replace(/'/g, "'\\''") + "'"; }

async function expectSecureFiles(profile: ClusterProfile): Promise<void> {
  const directory = dirname(profile.ssh_config_file!);
  for (const path of [dirname(configPath), dirname(directory), directory]) expect((await stat(path)).mode & 0o777).toBe(0o700);
  for (const path of [configPath, profile.ssh_config_file!, join(directory, "bootstrap_config"), join(dirname(configPath), "id_ed25519"), join(dirname(configPath), "id_ed25519.pub"), join(dirname(configPath), "known_hosts")]) {
    const info = await lstat(path);
    expect(info.isFile()).toBe(true);
    expect(info.isSymbolicLink()).toBe(false);
    expect(info.mode & 0o777).toBe(0o600);
  }
}

async function expectBootstrapAuthentication(profile: ClusterProfile, legacyKey?: string): Promise<void> {
  const dedicated = await effectiveSSH(profile);
  const bootstrap = await effectiveSSH(profile, join(dirname(profile.ssh_config_file!), "bootstrap_config"));
  // OpenSSH's effective settings, not config source wording, establish that the
  // install path still offers default identities and unnamed ssh-agent keys.
  expect(dedicated.get("identitiesonly")).toBe("yes");
  expect(bootstrap.get("identitiesonly")).toBe("no");
  if (legacyKey) expect(bootstrap.get("identityfile")?.replace(/%%/g, "%")).toBe(legacyKey);
  else {
    expect(bootstrap.get("identityfile")).toMatch(/(?:~|\/).*\.ssh\/id_/);
    expect(bootstrap.get("identityfile")).not.toContain(dirname(profile.ssh_config_file!));
  }
  for (const values of [dedicated, bootstrap]) {
    expect(values.get("userknownhostsfile")?.replace(/%%/g, "%")).toContain(join(dirname(configPath), "known_hosts"));
  }
  expect(dedicated.get("identityfile")?.replace(/%%/g, "%")).toContain(join(dirname(configPath), "id_ed25519"));
  for (const field of ["hostname", "user", "port", "proxyjump", "controlmaster", "controlpersist"]) {
    expect(bootstrap.get(field)).toBe(dedicated.get(field));
  }
}

async function expectSafePublication(ui: WizardUI, profile: ClusterProfile): Promise<void> {
  await expectBootstrapAuthentication(profile);
  expect(ui.published).toHaveLength(1);
  const output = ui.published[0]!;
  const key = join(dirname(configPath), "id_ed25519");
  const publicKey = (await readFile(key + ".pub", "utf8")).trim();
  const privateKey = await readFile(key, "utf8");
  expect(output.includes(privateKey)).toBe(false);
  expect(output).not.toContain("BEGIN OPENSSH PRIVATE KEY");
  expect(output).not.toContain(enrollmentSecret);
  expect(output).toContain(publicKey);
  expect(output).toContain(`cluster:${profile.id}`);
  const bootstrap = join(dirname(profile.ssh_config_file!), "bootstrap_config");
  expect(output).toContain(`ssh-copy-id -F ${shellQuote(bootstrap)} -i ${shellQuote(key + ".pub")} ${shellQuote(profile.ssh_host)}`);
  expect(output).toContain(`ssh -F ${shellQuote(bootstrap)} ${shellQuote(profile.ssh_host)}`);
  expect(output).toContain(`ssh -F ${shellQuote(profile.ssh_config_file!)} -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 ${shellQuote(profile.ssh_host)} 'true'`);
  expect(output).not.toContain("StrictHostKeyChecking=no");
  expect(output).toContain("authorized_keys");
  expect(output).toContain("fingerprint");
  expect(output).toContain("No allocation submitted");
  expect(output).toContain("not proof of connectivity");
  expect(output).toContain(`ssh-keygen -p -f ${shellQuote(key)}`);
  expect(output).toContain(`ssh-add ${shellQuote(key)}`);
  // Check real key correspondence, not merely plausible public-key wording.
  const derived = await local(["ssh-keygen", "-y", "-f", key]);
  expect(publicKey.split(" ").slice(0, 2).join(" ")).toBe(derived.trim().split(" ").slice(0, 2).join(" "));
  for (const notice of ui.notices) {
    expect(notice.message.includes(privateKey)).toBe(false);
    expect(notice.message).not.toContain(enrollmentSecret);
  }
}

for (const withBaseline of [false, true]) {
  for (const stage of ["input", "editor", "review-escape", "review-cancel", "final-confirm"] as const) {
    test(`cancellation at ${stage} leaves ${withBaseline ? "existing config and key" : "fresh home"} byte-for-byte unchanged`, async () => {
      if (withBaseline) {
        await saveBaseline();
        const oldKeyDirectory = join(dirname(configPath), "ssh", "existing");
        await mkdir(oldKeyDirectory, { recursive: true, mode: 0o700 });
        await writeFile(join(oldKeyDirectory, "id_ed25519"), "existing-private-secret", { mode: 0o600 });
      }
      const before = await tree();
      const replies: Record<string, Reply> = {};
      if (stage === "input") replies["Name / description"] = undefined;
      if (stage === "editor") replies["Trusted runtime shell prelude"] = undefined;
      if (stage === "review-escape") replies["Review cluster"] = undefined;
      if (stage === "review-cancel") replies["Review cluster"] = "Cancel";
      const ui = new WizardUI("cancelled", {
        replies, confirm: stage !== "final-confirm",
        beforePrompt: async () => { expect(await tree()).toEqual(before); },
      });
      expect(await ui.run()).toBeUndefined();
      ui.expectScriptUsed();
      expect(await tree()).toEqual(before);
      expect(ui.published).toEqual([]);
      expect(ui.notices.some((notice) => notice.type === "info" && /cancelled/i.test(notice.message))).toBe(true);
      expect(ui.matching("Confirm cluster")).toHaveLength(stage === "final-confirm" ? 1 : 0);
    });
  }
}

test("CPU/direct registration persists a canonical profile, private files and actionable public-only instructions", async () => {
  const old = baseline();
  await saveBaseline([old]);
  const before = await tree();
  const ui = new WizardUI("cpu-direct", { beforePrompt: async () => { expect(await tree()).toEqual(before); } });
  const result = await ui.run();
  ui.expectScriptUsed();
  expect(result).toEqual({
    id: "cpu-direct", description: "Research CPU partition; site utilization limits apply",
    ssh_host: "wh-cpu-direct", ssh_config_file: join(dirname(configPath), "ssh", "cpu-direct", "config"),
    storage_id: "research-storage", shared_root: "/cluster/research group", sif_path: "/cluster/research group/worker.sif",
    compute_kind: "cpu", max_gpus: 0, runtime: "apptainer", runtime_setup: "module purge\nmodule load apptainer",
    account: "research", partition: "cpu-partition", max_walltime_seconds: 3600, failure_grace_seconds: 0,
    termination_grace_seconds: 30, warning_seconds: 120, orchestrator_registration_port: 12888, poll_seconds: 15,
    tailnet_key_file: join(home, "tailnet enrollment key"), tailnet_login_server: "https://control.example.test",
    orchestrator_host: "orchestrator.example.test",
  });
  const saved = await document();
  expect(saved.version).toBe(1);
  expect(saved.clusters).toEqual([old, result]);
  expect((await loadClusterProfiles(env)).at(-1)).toEqual(result);
  expect(result).not.toHaveProperty("gpu_type");
  expect(ui.matching("Confirm cluster")[0]?.current).toMatch(/UNENCRYPTED/);
  await expectSecureFiles(result!);
  const ssh = await effectiveSSH(result!);
  expect(ssh.get("hostname")).toBe("192.0.2.17");
  expect(ssh.get("user")).toBe("researcher");
  expect(ssh.get("port")).toBe("2222");
  expect(ssh.get("proxyjump")).toBe("jump-user@gateway.example.test:2200");
  expect(ssh.get("identitiesonly")).toBe("yes");
  await expectSafePublication(ui, result!);
  expect((await tree()).some((entry) => /\.lock$|\.tmp$/.test(entry.path))).toBe(false);
});

test("GPU/existing-alias registration retains effective HostName, jump and multiplexing offline", async () => {
  const originalSSH = await readFile(join(home, ".ssh", "config"), "utf8");
  const ui = new WizardUI("gpu-alias", { replies: {
    "Compute kind": "gpu", "SSH connection": "Existing SSH alias", "Slurm partition": "gpu-partition",
    "Container runtime": "singularity", "Advanced settings": "Configure advanced settings",
  } });
  const result = await ui.run();
  ui.expectScriptUsed();
  expect(result).toMatchObject({
    id: "gpu-alias", compute_kind: "gpu", gpu_type: "a100_80gb", max_gpus: 4,
    ssh_host: "site-login", runtime: "singularity", partition: "gpu-partition",
    qos: "research-qos", constraint: "a100&nvlink", max_cpus: 32, max_memory_mb: 65536,
    warning_seconds: 0, orchestrator_registration_port: 12889, poll_seconds: 3,
    shared_root: "/cluster/research group", sif_path: "/cluster/research group/worker.sif",
  });
  expect((await document()).clusters).toEqual([result]);
  expect(await loadClusterProfiles(env)).toEqual([result!]);
  expect(await readFile(join(home, ".ssh", "config"), "utf8")).toBe(originalSSH);
  const ssh = await effectiveSSH(result!);
  expect(ssh.get("hostname")).toBe("192.0.2.42");
  expect(ssh.get("user")).toBe("site-user");
  expect(ssh.get("port")).toBe("2202");
  expect(ssh.get("proxyjump")).toBe("gateway.example.test");
  expect(ssh.get("controlmaster")).toBe("auto");
  expect(ssh.get("controlpersist")).toBe("300");
  expect(ssh.get("identitiesonly")).toBe("yes");
  const identity = ssh.get("identityfile")!;
  expect(identity).toContain("id_ed25519");
  expect(identity).toContain("space");
  await expectSecureFiles(result!);
  await expectSafePublication(ui, result!);
});

test("invalid numeric, path, URL and port corrections retain earlier answers with no writes before confirmation", async () => {
  const before = await tree();
  const ui = new WizardUI("corrected", {
    replies: {
      "Maximum walltime": ["not-a-number", "1.5", "9007199254740992", "7200"],
      "Shared root": ["relative/shared", "/cluster/../escape", "/cluster//corrected shared/"],
      "Local Tailnet enrollment KEY FILE": ["not-an-absolute-file", join(home, "tailnet enrollment key")],
      "Tailnet login server": ["https://user:secret@control.example.test", "https://control.example.test"],
      "Advanced settings": "Configure advanced settings",
      "Orchestrator registration port": ["65536", "12999"],
      "Review cluster": ["Edit a field", "Save profile"],
      "Field to correct": "Name / description (include site limits)",
      "Name / description": ["Retained initial description", "Corrected final description"],
    },
    beforePrompt: async () => { expect(await tree()).toEqual(before); },
  });
  const result = await ui.run();
  ui.expectScriptUsed();
  expect(result).toMatchObject({ description: "Corrected final description", shared_root: "/cluster/corrected shared", max_walltime_seconds: 7200, orchestrator_registration_port: 12999 });
  expect(ui.matching("Maximum walltime")).toHaveLength(4);
  expect(ui.matching("Shared root")).toHaveLength(3);
  expect(ui.matching("Shared root")[1]?.current).toBe("relative/shared");
  expect(ui.matching("Shared root")[2]?.current).toBe("/cluster/../escape");
  expect(ui.matching("Name / description")[1]?.current).toBe("Retained initial description");
  expect(ui.matching("SSH username")).toHaveLength(1);
  expect(ui.matching("Slurm account")).toHaveLength(1);
  expect(ui.matching("Review cluster")[0]?.title).toContain("Retained initial description");
  expect(ui.matching("Field to correct")[0]?.options).not.toContain("Maximum GPUs per allocation");
  expect(ui.notices.filter((notice) => notice.type === "error").length).toBeGreaterThanOrEqual(7);
  expect((await document()).clusters).toEqual([result]);
  await expectSecureFiles(result!);
});

test("correcting a populated optional field with '-' removes it without disturbing other answers", async () => {
  const ui = new WizardUI("clear-optional", { replies: {
    "Advanced settings": "Configure advanced settings",
    "Review cluster": ["Edit a field", "Save profile"],
    "Field to correct": "QoS (optional)",
    "QoS": ["research-qos", "-"],
  } });
  const result = await ui.run();
  ui.expectScriptUsed();
  expect(result).toBeDefined();
  expect(result).not.toHaveProperty("qos");
  expect(result).toMatchObject({ max_cpus: 32, max_memory_mb: 65536, partition: "cpu-partition" });
  expect(ui.matching("QoS")[1]?.current).toBe("research-qos");
  expect((await document()).clusters).toEqual([result]);
});

test("an existing ID is rejected without replacing its profile or managed secret", async () => {
  await saveBaseline();
  const keyDirectory = join(dirname(configPath), "ssh", "existing");
  await mkdir(keyDirectory, { recursive: true, mode: 0o700 });
  await writeFile(join(keyDirectory, "id_ed25519"), "existing-private-secret", { mode: 0o600 });
  const before = await tree();
  const ui = new WizardUI("existing", { replies: { "Cluster ID": ["existing", undefined] } });
  expect(await ui.run()).toBeUndefined();
  ui.expectScriptUsed();
  expect(ui.matching("Cluster ID")).toHaveLength(2);
  expect(ui.notices.some((notice) => notice.type === "error")).toBe(true);
  expect(ui.published).toEqual([]);
  expect(await tree()).toEqual(before);
});

for (const collision of ["existing-directory", "key-directory-symlink", "config-symlink", "parent-symlink", "lock-symlink"] as const) {
  test(`${collision} refuses registration and preserves all preexisting secret bytes`, async () => {
    await saveBaseline();
    const sshDirectory = join(dirname(configPath), "ssh");
    await mkdir(sshDirectory, { mode: 0o700 });
    const victim = join(root, "untouched-secret-directory");
    await mkdir(victim, { mode: 0o700 });
    await writeFile(join(victim, "id_ed25519"), "DO-NOT-REPLACE-existing-secret", { mode: 0o600 });
    const target = join(sshDirectory, "collision");
    if (collision === "existing-directory") {
      await mkdir(target, { mode: 0o700 });
      await writeFile(join(target, "id_ed25519"), "DO-NOT-REPLACE-target-secret", { mode: 0o600 });
    } else if (collision === "key-directory-symlink") await symlink(victim, target);
    else if (collision === "config-symlink") {
      const realConfig = join(victim, "clusters.json");
      await writeFile(realConfig, await readFile(configPath), { mode: 0o600 });
      await rm(configPath);
      await symlink(realConfig, configPath);
    } else if (collision === "parent-symlink") {
      await symlink(dirname(configPath), join(root, "linked-config-directory"));
      env = { ...env, WH_CLUSTER_CONFIG: join(root, "linked-config-directory", "clusters.json") };
    } else await symlink(join(victim, "id_ed25519"), configPath + ".lock");
    const before = await tree();
    const ui = new WizardUI("collision");
    expect(await ui.run()).toBeUndefined();
    ui.expectScriptUsed();
    expect(ui.matching("Confirm cluster")).toHaveLength(1);
    expect(ui.notices.some((notice) => notice.type === "error")).toBe(true);
    expect(ui.published).toEqual([]);
    expect(await tree()).toEqual(before);
  });
}

for (const invalid of ["{not valid JSON", JSON.stringify({ version: 2, clusters: [] }), JSON.stringify({ version: 1, clusters: [{ id: "broken" }] })]) {
  test(`invalid preexisting configuration fails before prompting or writing: ${invalid}`, async () => {
    await saveBaseline();
    await writeFile(configPath, invalid);
    const before = await tree();
    const ui = new WizardUI("new-profile");
    expect(await ui.run()).toBeUndefined();
    ui.expectScriptUsed();
    expect(ui.prompts).toEqual([]);
    expect(ui.published).toEqual([]);
    expect(ui.notices.some((notice) => notice.type === "error" && notice.message.includes(configPath))).toBe(true);
    expect(await tree()).toEqual(before);
  });
}

test("copying a managed connection creates per-profile configs using the same shared key and storage", async () => {
  const sourceUI = new WizardUI("source-gpu", { replies: { "Compute kind": "gpu", "Slurm partition": "gpu-partition" } });
  const source = await sourceUI.run();
  sourceUI.expectScriptUsed();
  expect(source).toBeDefined();
  const sourceDirectory = dirname(source!.ssh_config_file!);
  const oldSSH = await tree(sourceDirectory);
  const before = await tree();
  const copied = new WizardUI("copied-cpu", {
    replies: {
      "Reuse an existing": "Copy source-gpu: connection / storage / runtime",
      "Shared storage ID": "", "Shared root": "", "Worker SIF": "",
      "Local Tailnet enrollment KEY FILE": "", "Tailnet login server": "", "Orchestrator hostname": "",
      "Trusted runtime shell prelude": "module purge\nmodule load apptainer",
    },
    beforePrompt: async () => { expect(await tree()).toEqual(before); },
  });
  const result = await copied.run();
  copied.expectScriptUsed();
  expect(result).toMatchObject({
    id: "copied-cpu", compute_kind: "cpu", max_gpus: 0, storage_id: source!.storage_id,
    shared_root: source!.shared_root, sif_path: source!.sif_path, ssh_host: source!.ssh_host,
    ssh_config_file: join(dirname(configPath), "ssh", "copied-cpu", "config"), runtime: source!.runtime, runtime_setup: source!.runtime_setup,
    tailnet_key_file: source!.tailnet_key_file, tailnet_login_server: source!.tailnet_login_server,
    orchestrator_host: source!.orchestrator_host,
  });
  expect(result).not.toHaveProperty("gpu_type");
  expect(sharedPath(result!)).toBe(sharedPath(source!));
  expect(copied.matching("SSH connection")).toEqual([]);
  expect(copied.matching("SSH hostname")).toEqual([]);
  expect(copied.matching("Confirm cluster")[0]?.current).toMatch(/reuse/i);
  expect(await tree(sourceDirectory)).toEqual(oldSSH);
  expect((await readdir(dirname(sourceDirectory))).sort()).toEqual(["copied-cpu", "source-gpu"]);
  expect((await document()).clusters).toEqual([source, result]);
  await expectSafePublication(copied, result!);
});

test("copying refuses a symlinked private key without changing the source or registering a new profile", async () => {
  const sourceUI = new WizardUI("managed-source");
  const source = await sourceUI.run();
  sourceUI.expectScriptUsed();
  expect(source).toBeDefined();
  const key = join(dirname(configPath), "id_ed25519");
  const outsideKey = join(root, "outside-private-key");
  await writeFile(outsideKey, await readFile(key), { mode: 0o600 });
  await rm(key);
  await symlink(outsideKey, key);
  const before = await tree();
  const copied = new WizardUI("rejected-copy", { replies: {
    "Reuse an existing": "Copy managed-source: connection / storage / runtime",
    "Shared storage ID": "", "Shared root": "", "Worker SIF": "",
  } });
  expect(await copied.run()).toBeUndefined();
  copied.expectScriptUsed();
  expect(copied.matching("Confirm cluster")).toHaveLength(1);
  expect(copied.notices.some((notice) => notice.type === "error")).toBe(true);
  expect(copied.published).toEqual([]);
  expect(await tree()).toEqual(before);
});

for (const invalidPublicFile of ["private-key", "invalid-ed25519-blob"] as const) {
  test(`copying rejects ${invalidPublicFile} in the public-key file without publishing or committing it`, async () => {
    const sourceUI = new WizardUI("public-key-source");
    const source = await sourceUI.run();
    sourceUI.expectScriptUsed();
    expect(source).toBeDefined();
    const key = join(dirname(configPath), "id_ed25519");
    const privateKey = await readFile(key, "utf8");
    await writeFile(key + ".pub", invalidPublicFile === "private-key" ? privateKey : "ssh-ed25519 c2VjcmV0 invalid-key-blob");
    const before = await tree();
    const copied = new WizardUI("unsafe-public-copy", { replies: {
      "Reuse an existing": "Copy public-key-source: connection / storage / runtime",
      "Shared storage ID": "", "Shared root": "", "Worker SIF": "",
    } });
    expect(await copied.run()).toBeUndefined();
    copied.expectScriptUsed();
    expect(copied.matching("Confirm cluster")).toHaveLength(1);
    expect(copied.published).toEqual([]);
    expect(copied.notices.some((notice) => notice.type === "error" && /public key/i.test(notice.message))).toBe(true);
    for (const notice of copied.notices) {
      expect(notice.message.includes(privateKey)).toBe(false);
      expect(notice.message).not.toContain("BEGIN OPENSSH PRIVATE KEY");
      expect(notice.message).not.toContain(privateKey.trim().split("\n")[1]!);
    }
    expect(await tree()).toEqual(before);
    expect((await document()).clusters).toEqual([source]);
  });
}

function simultaneousConfirmation(): (prompt: Prompt) => Promise<void> {
  const ready = Promise.withResolvers<void>();
  let count = 0;
  return async (prompt) => {
    if (prompt.kind !== "confirm") return;
    if (++count === 2) ready.resolve();
    await ready.promise;
  };
}

test("two simultaneous real registrations never lose a profile; a lock loser can retry safely", async () => {
  const old = baseline();
  await saveBaseline([old]);
  const barrier = simultaneousConfirmation();
  const first = new WizardUI("parallel-a", { beforePrompt: barrier });
  const second = new WizardUI("parallel-b", { beforePrompt: barrier });
  const results = await Promise.all([first.run(), second.run()]);
  first.expectScriptUsed();
  second.expectScriptUsed();
  expect(results.filter(Boolean).length).toBeGreaterThanOrEqual(1);
  const firstCommit = await document();
  expect(firstCommit.clusters[0]).toEqual(old);
  expect(firstCommit.clusters.slice(1).map((profile) => profile.id).sort()).toEqual(results.filter((profile): profile is ClusterProfile => profile !== undefined).map((profile) => profile.id).sort());
  const committedKeys = new Map<string, string>();
  for (const profile of firstCommit.clusters.slice(1)) committedKeys.set(profile.id, await readFile(join(dirname(configPath), "id_ed25519"), "utf8"));
  for (let index = 0; index < results.length; index++) {
    if (results[index] !== undefined) continue;
    const failed = index === 0 ? first : second;
    expect(failed.published).toEqual([]);
    expect(failed.notices.some((notice) => notice.type === "error" && /lock|concurren/i.test(notice.message))).toBe(true);
    const retry = new WizardUI(failed.id);
    results[index] = await retry.run();
    retry.expectScriptUsed();
    expect(results[index]).toBeDefined();
  }
  const saved = await document();
  expect(saved.clusters[0]).toEqual(old);
  expect(saved.clusters.slice(1).map((profile) => profile.id).sort()).toEqual(["parallel-a", "parallel-b"]);
  for (const profile of saved.clusters.slice(1)) {
    await expectSecureFiles(profile);
    if (committedKeys.has(profile.id)) expect((await readFile(join(dirname(configPath), "id_ed25519"), "utf8")) === committedKeys.get(profile.id)).toBe(true);
  }
  expect((await tree()).some((entry) => /\.lock$|\.tmp$/.test(entry.path))).toBe(false);
});

test("concurrent registrations of the same ID commit exactly one key/profile without replacement", async () => {
  await saveBaseline();
  const barrier = simultaneousConfirmation();
  const first = new WizardUI("same-id", { replies: { "Name / description": "First concurrent draft" }, beforePrompt: barrier });
  const second = new WizardUI("same-id", { replies: { "Name / description": "Second concurrent draft" }, beforePrompt: barrier });
  const results = await Promise.all([first.run(), second.run()]);
  first.expectScriptUsed();
  second.expectScriptUsed();
  const winners = results.filter((profile): profile is ClusterProfile => profile !== undefined);
  expect(winners).toHaveLength(1);
  expect((await document()).clusters).toEqual([baseline(), winners[0]]);
  expect(first.published.length + second.published.length).toBe(1);
  expect((await readdir(join(dirname(configPath), "ssh"))).sort()).toEqual(["same-id"]);
  await expectSecureFiles(winners[0]!);
  expect((await tree()).some((entry) => /\.lock$|\.tmp$/.test(entry.path))).toBe(false);
});

test("system SSH inclusion requires a regular resolved file with safe ownership and write permissions", async () => {
  const path = join(root, "system-config-fixture");
  await writeFile(path, "Host *\n  ServerAliveInterval 29\n", { mode: 0o644 });
  const resolved = await realpath(path);
  expect(await inspectSystemSshConfig(path)).toEqual({ includePath: resolved });
  const currentUid = process.getuid!();
  // Ownership seams model root/foreign-owned files without chown privileges.
  for (const uid of [0, currentUid]) {
    expect(await inspectSystemSshConfig(path, async () => ({ uid, mode: 0o100644, isFile: () => true }))).toEqual({ includePath: resolved });
  }
  const foreignUid = currentUid + 1;
  const foreign = await inspectSystemSshConfig(path, async () => ({ uid: foreignUid, mode: 0o100644, isFile: () => true }));
  expect(foreign.includePath).toBeUndefined();
  expect(foreign.warning).toContain("system SSH defaults");
  expect(foreign.warning).toContain("required site settings");
  expect(foreign.warning).toContain("~/.ssh/config");
  for (const mode of [0o664, 0o646, 0o666]) {
    await chmod(path, mode);
    const unsafe = await inspectSystemSshConfig(path);
    expect(unsafe.includePath).toBeUndefined();
    expect(unsafe.warning).toContain("have not been weakened");
  }
  await chmod(path, 0o644);
  const link = join(root, "system-config-link");
  await symlink(path, link);
  expect(await inspectSystemSshConfig(link)).toEqual({ includePath: resolved });
  expect((await inspectSystemSshConfig(root)).includePath).toBeUndefined();
  expect((await inspectSystemSshConfig(join(root, "missing-system-config"))).warning).toContain("not inherited");
});

test("native system SSH config policy permits safe user alias resolution without weakening user config checks", async () => {
  const system = await inspectSystemSshConfig();
  try {
    const info = await stat(await realpath("/etc/ssh/ssh_config"));
    const safe = info.isFile() && (info.uid === 0 || info.uid === process.getuid!()) && (info.mode & 0o022) === 0;
    expect(Boolean(system.includePath)).toBe(safe);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    expect(system.includePath).toBeUndefined();
  }
  const ui = new WizardUI("native-system-policy", { replies: { "SSH connection": "Existing SSH alias" } });
  const profile = await ui.run();
  ui.expectScriptUsed();
  expect(profile).toBeDefined();
  const effective = await effectiveSSH(profile!);
  const bootstrap = await effectiveSSH(profile!, join(dirname(profile!.ssh_config_file!), "bootstrap_config"));
  for (const values of [effective, bootstrap]) {
    expect(values.get("hostname")).toBe("192.0.2.42");
    expect(values.get("user")).toBe("site-user");
    expect(values.get("proxyjump")).toBe("gateway.example.test");
    expect(values.get("controlmaster")).toBe("auto");
  }
  const message = ui.published.join("\n");
  if (system.warning) {
    expect(message).toContain(system.warning);
    expect(message).toContain("required site settings in your normal ~/.ssh/config");
  } else expect(message).not.toContain("system SSH defaults from /etc/ssh/ssh_config were not inherited");
  // A bad user config must still fail normally; it is never silently omitted.
  await chmod(join(home, ".ssh", "config"), 0o666);
  const child = Bun.spawn(["ssh", "-G", "-F", profile!.ssh_config_file!, profile!.ssh_host], { stdin: "ignore", stdout: "pipe", stderr: "pipe" });
  const [, stderr, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
  expect(code).not.toBe(0);
  expect(stderr).toContain("Bad owner or permissions");
});

async function legacyProfiles(encrypted = false, ids = ["legacy-cpu", "legacy-second"]): Promise<{ profiles: ClusterProfile[]; keys: string[] }> {
  const profiles: ClusterProfile[] = [];
  const keys: string[] = [];
  const pathToken = (text: string) => '"' + text.replace(/%/g, "%%").replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
  for (const [index, id] of ids.entries()) {
    const directory = join(dirname(configPath), "ssh", id);
    await mkdir(directory, { recursive: true, mode: 0o700 });
    const key = join(directory, "id_ed25519");
    await local(["ssh-keygen", "-q", "-t", "ed25519", "-N", encrypted && index === 0 ? "fixture-passphrase" : "", "-f", key]);
    await chmod(key + ".pub", 0o600);
    const profile = { ...baseline(id), ssh_host: index === 0 ? `wh-${id}` : "site-login", ssh_config_file: join(directory, "config") } as ClusterProfile;
    // Reproduce the prior wizard format independently of the new renderer.
    for (const bootstrap of [false, true]) {
      const lines = [`Host ${profile.ssh_host}`];
      if (!bootstrap) lines.push(`  IdentityFile ${pathToken(key)}`, "  IdentitiesOnly yes");
      if (index === 0) lines.push("  HostName 192.0.2.17", "  User researcher", "  Port 2222", "  ProxyJump jump-user@gateway.example.test:2200");
      lines.push("Host *", `  Include ${pathToken(join(home, ".ssh", "config"))}`, "");
      await writeFile(join(directory, bootstrap ? "bootstrap_config" : "config"), lines.join("\n"), { mode: 0o600 });
    }
    profiles.push(profile);
    keys.push(key);
  }
  await saveBaseline(profiles);
  return { profiles, keys };
}

test("independent profiles share exactly one key and preserve every verified known_hosts byte", async () => {
  const first = await new WizardUI("first").run();
  expect(first).toBeDefined();
  const ssh = dirname(configPath);
  const originalKey = await readFile(join(ssh, "id_ed25519"));
  const trust = "|1|verified-hash|verified-host ssh-ed25519 site-public-key\n# retain exactly\n";
  await writeFile(join(ssh, "known_hosts"), trust, { mode: 0o600 });
  const secondUI = new WizardUI("second", { replies: { "SSH hostname / IP": "192.0.2.18" } });
  const second = await secondUI.run();
  expect(second).toBeDefined();
  expect(await readFile(join(ssh, "id_ed25519"))).toEqual(originalKey);
  expect(await readFile(join(ssh, "known_hosts"), "utf8")).toBe(trust);
  expect(secondUI.matching("Confirm cluster")[0]?.current).toContain("Reuse the existing shared SSH key");
  await expectBootstrapAuthentication(first!);
  await expectBootstrapAuthentication(second!);
  expect(await readdir(dirname(second!.ssh_config_file!))).toEqual(["bootstrap_config", "config"]);
});

for (const encrypted of [false, true]) {
  test(`explicit legacy migration preserves ${encrypted ? "encrypted" : "unencrypted"} chosen key bytes, connection settings and all old keypairs`, async () => {
    const { profiles, keys } = await legacyProfiles(encrypted);
    const original = await readFile(configPath, "utf8");
    const originals = await Promise.all(keys.map(async (key) => [await readFile(key), await readFile(key + ".pub")]));
    const previous = await Promise.all(profiles.map((profile) => effectiveSSH(profile)));
    const externalFile = join(home, "external.config");
    await writeFile(externalFile, "Host *\n  HostName external.example.test\n", { mode: 0o600 });
    await saveBaseline([...profiles, { ...baseline("external-cpu"), ssh_config_file: externalFile }]);
    const configBefore = await readFile(configPath, "utf8");
    const ui = new WizardUI("migration", { replies: { "Select existing managed key": "legacy-cpu" } });
    expect(await runClusterSshMigrate(ui, ui.publish, env)).toEqual(["legacy-cpu", "legacy-second"]);
    ui.expectScriptUsed();
    expect(await readFile(configPath, "utf8")).toBe(configBefore);
    expect(original).not.toBe(configBefore);
    const shared = join(dirname(configPath), "id_ed25519");
    expect(await readFile(shared)).toEqual(originals[0]![0]);
    expect(await readFile(shared + ".pub")).toEqual(originals[0]![1]);
    for (const [index, key] of keys.entries()) {
      expect(await readFile(key)).toEqual(originals[index]![0]);
      expect(await readFile(key + ".pub")).toEqual(originals[index]![1]);
      const effective = await effectiveSSH(profiles[index]!);
      for (const field of ["hostname", "user", "port", "proxyjump", "controlmaster", "controlpersist"]) expect(effective.get(field)).toBe(previous[index]!.get(field));
      await expectBootstrapAuthentication(profiles[index]!, key);
      await expectSecureFiles(profiles[index]!);
    }
    expect(await readFile(externalFile, "utf8")).toBe("Host *\n  HostName external.example.test\n");
    expect(ui.published[0]).toContain("external-cpu");
    expect(ui.published[0]).toContain("NOT migrated");
    expect(ui.published[0]).toContain("EACH distinct login host");
    expect(ui.published[0]).toContain("No host trust was imported automatically");
    expect(ui.published[0]).toContain("SSH_AUTH_SOCK");
    if (encrypted) expect(ui.published[0]).toContain("encrypted; original bytes preserved");
    for (const profile of profiles) expect(ui.published[0]).toContain(`ssh-copy-id -F ${shellQuote(join(dirname(profile.ssh_config_file!), "bootstrap_config"))}`);
    for (const pair of originals) expect(ui.published[0]).not.toContain(pair[0]!.toString("utf8"));
  });
}

test("migration cancellation is completely read-only and add refuses to silently replace legacy authorization", async () => {
  await legacyProfiles();
  const before = await tree();
  const ui = new WizardUI("migration", { confirm: false, replies: { "Select existing managed key": "legacy-cpu" }, beforePrompt: async () => { expect(await tree()).toEqual(before); } });
  expect(await runClusterSshMigrate(ui, ui.publish, env)).toBeUndefined();
  expect(await tree()).toEqual(before);
  const add = new WizardUI("new-profile");
  expect(await add.run()).toBeUndefined();
  expect(add.notices.some(({ message }) => message.includes("/cluster-ssh-migrate"))).toBe(true);
  expect(await tree()).toEqual(before);
});

test("existing shared key always wins migration and verified shared trust is never replaced", async () => {
  const { profiles, keys } = await legacyProfiles();
  const shared = join(dirname(configPath), "id_ed25519");
  await local(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", shared]);
  await chmod(shared + ".pub", 0o600);
  const bytes = await readFile(shared);
  const trust = "# trusted existing site entries must survive migration\n";
  await writeFile(join(dirname(shared), "known_hosts"), trust, { mode: 0o600 });
  const ui = new WizardUI("migration");
  expect(await runClusterSshMigrate(ui, ui.publish, env)).toEqual(profiles.map(({ id }) => id));
  expect(ui.matching("Select existing")).toEqual([]);
  expect(await readFile(shared)).toEqual(bytes);
  expect(await readFile(join(dirname(shared), "known_hosts"), "utf8")).toBe(trust);
  expect(await readFile(shared)).not.toEqual(await readFile(keys[0]!));
  const again = new WizardUI("repeat-migration");
  expect(await runClusterSshMigrate(again, again.publish, env)).toEqual(profiles.map(({ id }) => id));
  expect(await readFile(shared)).toEqual(bytes);
  for (const [index, profile] of profiles.entries()) {
    await expectBootstrapAuthentication(profile, keys[index]);
    const bootstrap = await readFile(join(dirname(profile.ssh_config_file!), "bootstrap_config"), "utf8");
    expect(bootstrap).not.toContain("IdentitiesOnly");
  }
});

test("manually modified wizard-shaped config is explicitly left outside migration scope", async () => {
  const { profiles } = await legacyProfiles();
  const changed = profiles[1]!.ssh_config_file!;
  const contents = await readFile(changed, "utf8") + "# operator customization\n";
  await writeFile(changed, contents);
  const ui = new WizardUI("migration", { replies: { "Select existing managed key": "legacy-cpu" } });
  expect(await runClusterSshMigrate(ui, ui.publish, env)).toEqual(["legacy-cpu"]);
  expect(await readFile(changed, "utf8")).toBe(contents);
  expect(ui.published[0]).toContain("NOT migrated: legacy-second");
});

for (const unsafe of ["shared-key-symlink", "trust-symlink", "trust-world-readable", "mismatched-public", "fake-private-with-valid-public"] as const) {
  test(`shared SSH rejects ${unsafe} without replacing user files`, async () => {
    const profile = await new WizardUI("source").run();
    expect(profile).toBeDefined();
    const ssh = dirname(configPath);
    const key = join(ssh, "id_ed25519");
    const trust = join(ssh, "known_hosts");
    if (unsafe.endsWith("symlink")) {
      const destination = unsafe === "shared-key-symlink" ? key : trust;
      const victim = join(root, "untouched-victim");
      await writeFile(victim, await readFile(destination), { mode: 0o600 });
      await rm(destination);
      await symlink(victim, destination);
    } else if (unsafe === "trust-world-readable") await chmod(trust, 0o644);
    else if (unsafe === "fake-private-with-valid-public") await writeFile(key, "not a private key\n");
    else {
      const other = join(root, "other-key");
      await local(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", other]);
      await writeFile(key + ".pub", await readFile(other + ".pub"));
    }
    const before = await tree();
    const ui = new WizardUI("rejected");
    expect(await ui.run()).toBeUndefined();
    expect(await tree()).toEqual(before);
    expect(ui.published).toEqual([]);
  });
}

test("migration detects configs changed during confirmation before shared-key or trust creation", async () => {
  const { profiles } = await legacyProfiles();
  const changed = profiles[0]!.ssh_config_file!;
  const ui = new WizardUI("migration", {
    replies: { "Select existing managed key": "legacy-cpu" },
    beforePrompt: async ({ kind }) => { if (kind === "confirm") await writeFile(changed, (await readFile(changed, "utf8")) + "# concurrent edit\n"); },
  });
  expect(await runClusterSshMigrate(ui, ui.publish, env)).toBeUndefined();
  expect(ui.notices.some(({ message }) => message.includes("changed concurrently"))).toBe(true);
  expect((await readdir(join(dirname(configPath), "ssh"))).sort()).toEqual(["legacy-cpu", "legacy-second"]);
});

test("legacy profile IDs matching shared filenames migrate without namespace collisions", async () => {
  const { profiles, keys } = await legacyProfiles(false, ["id_ed25519", "known_hosts"]);
  const originalKey = await readFile(keys[0]!);
  const ui = new WizardUI("migration", { replies: { "Select existing managed key": "id_ed25519" } });
  expect(await runClusterSshMigrate(ui, ui.publish, env)).toEqual(["id_ed25519", "known_hosts"]);
  expect(await readFile(join(dirname(configPath), "id_ed25519"))).toEqual(originalKey);
  expect((await lstat(join(dirname(configPath), "known_hosts"))).isFile()).toBe(true);
  for (const [index, profile] of profiles.entries()) {
    expect((await lstat(dirname(profile.ssh_config_file!))).isDirectory()).toBe(true);
    await expectBootstrapAuthentication(profile, keys[index]);
  }
  const bootstrapBefore = await Promise.all(profiles.map((profile) => readFile(join(dirname(profile.ssh_config_file!), "bootstrap_config"), "utf8")));
  const again = new WizardUI("repeat-migration");
  expect(await runClusterSshMigrate(again, again.publish, env)).toEqual(["id_ed25519", "known_hosts"]);
  expect(await Promise.all(profiles.map((profile) => readFile(join(dirname(profile.ssh_config_file!), "bootstrap_config"), "utf8")))).toEqual(bootstrapBefore);
  const added = new WizardUI("new-after-migration");
  expect(await added.run()).toBeDefined();
  expect(await readFile(join(dirname(configPath), "id_ed25519"))).toEqual(originalKey);
});

for (const failAt of [2, 3, 4]) {
  test(`migration rolls back original config pairs after commit rename ${failAt} fails`, async () => {
    const { profiles } = await legacyProfiles();
    const paths = profiles.flatMap((profile) => [profile.ssh_config_file!, join(dirname(profile.ssh_config_file!), "bootstrap_config")]);
    const originals = await Promise.all(paths.map((path) => readFile(path)));
    const ui = new WizardUI("migration", { replies: { "Select existing managed key": "legacy-cpu" } });
    let commits = 0;
    expect(await runClusterSshMigrate(ui, ui.publish, env, async (source, destination) => {
      if (++commits === failAt) {
        // Real ENOENT from the filesystem at a chosen commit boundary, after
        // prior renames have actually replaced their destination files.
        await rename(join(root, "missing-staged-file"), destination);
      }
      await rename(source, destination);
    })).toBeUndefined();
    expect(await Promise.all(paths.map((path) => readFile(path)))).toEqual(originals);
    expect(ui.published).toEqual([]);
    expect((await tree()).some((entry) => /\.lock$|\.tmp$/.test(entry.path))).toBe(false);
    // The shared copy is retained safely; rollback leaves original wizard
    // renderings recognizable, so retry needs no new generation or promotion.
    const again = new WizardUI("retry-migration");
    expect(await runClusterSshMigrate(again, again.publish, env)).toEqual(["legacy-cpu", "legacy-second"]);
    expect(again.matching("Select existing")).toEqual([]);
  });
}
