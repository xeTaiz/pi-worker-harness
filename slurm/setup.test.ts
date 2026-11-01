import { afterEach, beforeEach, expect, test } from "bun:test";
import { chmod, lstat, mkdir, mkdtemp, readFile, readdir, readlink, realpath, rename, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative } from "node:path";
import { loadClusterProfiles, type ClusterProfile, type Environment } from "./profiles.ts";
import { buildDiscoveredProfiles, commitSetupProfiles, discoveredTargetId, inspectSystemSshConfig, runClusterAdd, runClusterSshMigrate, type SetupDraft, type SetupOptions, type SetupUI } from "./setup.ts";
import type { DiscoveryResult, DiscoveryTarget } from "./discovery.ts";
import { defaultRegistrationHost } from "./setup-integration.ts";

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
  confirms?: Record<string, boolean>;
  setup?: SetupOptions;
  beforePrompt?: (prompt: Prompt) => void | Promise<void>;
};

/** A consumer UI, not a mock of parsing, persistence, subprocesses or transport. */
class WizardUI implements SetupUI {
  readonly prompts: Prompt[] = [];
  readonly notices: { message: string; type?: string }[] = [];
  readonly published: string[] = [];
  readonly editorTexts: string[] = [];
  readonly scriptErrors: string[] = [];
  private readonly counts = new Map<string, number>();
  private readonly replies: Record<string, Reply>;

  constructor(readonly id: string, private readonly options: ScriptOptions = {}) {
    this.replies = {
      "SSH destination": "researcher@192.0.2.17",
      "Cluster name": id,
      "Description (optional)": "Research cluster; site utilization limits apply",
      "WH shared filesystem": "/data/shared/research-storage",
      "WH shared path": "/data/shared/research-storage",
      "Same filesystem root": "/cluster//research group/",
      "Add another filesystem mapping?": "No",
      "Discovery prerequisites unresolved": "Save and exit",
      "Trusted runtime shell prelude": "module load apptainer",
      "Compatible Worker SIF": "/cluster/research group/worker.sif",
      "Billing default unresolved": "Use scheduler default",
      "Local Tailnet enrollment KEY FILE": join(home, "tailnet enrollment key"),
      "Tailnet login server": "https://control.example.test",
      "Review cluster": "Register targets",
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
    return this.options.confirms?.[title] ?? this.options.confirm ?? true;
  }
  notify(message: string, type?: string): void { this.notices.push({ message, type }); }
  publish = (content: string): void => { this.published.push(content); };
  setEditorText: ((text: string) => void) | undefined = (text) => { this.editorTexts.push(text); };

  run(args = this.id): Promise<ClusterProfile | undefined> { return runClusterAdd(this, this.publish, args, env, { ...offlineSetup(), ...this.options.setup }); }
  async pauseAndResume(): Promise<ClusterProfile | undefined> {
    expect(await this.run()).toBeUndefined();
    expect((await drafts()).find(({ id }) => id === this.id)?.phase).toBe("ssh");
    const resumed = new WizardUI(this.id, this.options);
    const result = await resumed.run();
    this.prompts.push(...resumed.prompts);
    this.notices.push(...resumed.notices);
    this.published.push(...resumed.published);
    this.editorTexts.push(...resumed.editorTexts);
    this.scriptErrors.push(...resumed.scriptErrors);
    return result;
  }
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

function bootstrapCommands(output: string, count: number): string[] {
  const blocks = [...output.matchAll(/```sh\n([\s\S]*?)\n```/g)];
  expect(blocks).toHaveLength(count);
  expect(output.replace(/```sh\n[\s\S]*?\n```/g, "").trim()).toBe("");
  return blocks.map(([, body]) => {
    const lines = body!.split("\n");
    expect(lines).toHaveLength(1);
    return lines[0]!;
  });
}

function expectedBootstrapCommands(profile: Pick<ClusterProfile, "ssh_host" | "ssh_config_file">): string[] {
  const bootstrap = join(dirname(profile.ssh_config_file!), "bootstrap_config");
  const key = join(dirname(configPath), "id_ed25519");
  return [
    `ssh-copy-id -f -F ${shellQuote(bootstrap)} -i ${shellQuote(key + ".pub")} -o StrictHostKeyChecking=ask ${shellQuote(profile.ssh_host)}`,
    `ssh -F ${shellQuote(bootstrap)} -o StrictHostKeyChecking=ask ${shellQuote(profile.ssh_host)} true`,
  ];
}

async function expectSafePublication(ui: WizardUI, profile: ClusterProfile): Promise<void> {
  await expectBootstrapAuthentication(profile);
  expect(bootstrapCommands(ui.published[0]!, 2)).toEqual(expectedBootstrapCommands(profile));
  const output = ui.published.join("\n");
  const key = join(dirname(configPath), "id_ed25519");
  const publicKey = (await readFile(key + ".pub", "utf8")).trim();
  const privateKey = await readFile(key, "utf8");
  expect(output.includes(privateKey)).toBe(false);
  expect(output).not.toContain("BEGIN OPENSSH PRIVATE KEY");
  expect(output).not.toContain(enrollmentSecret);
  expect(output).not.toContain(publicKey);
  expect(output).not.toContain("StrictHostKeyChecking=no");
  for (const obsolete of ["authorized_keys", "ssh-keygen -p", "ssh-add ", "SSH_AUTH_SOCK", "manual fallback"]) expect(output).not.toContain(obsolete);
  // Check real key correspondence, not merely plausible public-key wording.
  const derived = await local(["ssh-keygen", "-y", "-f", key]);
  expect(publicKey.split(" ").slice(0, 2).join(" ")).toBe(derived.trim().split(" ").slice(0, 2).join(" "));
  for (const notice of ui.notices) {
    expect(notice.message.includes(privateKey)).toBe(false);
    expect(notice.message).not.toContain(enrollmentSecret);
  }
}

function target(partition = "cpu-partition", gpu_type?: string, ceiling = 1_209_600): DiscoveryTarget {
  return {
    partition, compute_kind: gpu_type ? "gpu" : "cpu", ...(gpu_type ? { gpu_type, max_gpus: 4 } : {}),
    max_walltime_seconds: ceiling, limits: {}, defaults: { cpus: 4, memory_mb: 8192 },
    review_required: false, permission: "unverified_until_submission",
  };
}

function discovery(overrides: Partial<DiscoveryResult> = {}): DiscoveryResult {
  return {
    observed_at: "2026-10-25T12:00:00.000Z", cluster_name: "research",
    account_source: "scheduler", accounts: [], qos: [],
    runtime: { executable: "apptainer", version: "1.3.0", prelude: "" },
    sif_path: "/cluster/research group/worker.sif",
    targets: [target()], unresolved: [], evidence: [], ...overrides,
  };
}

/** Keep real private filesystem/keygen and managed ssh -G; isolate resolver input from operator config. */
function offlineSetup(): SetupOptions {
  return {
    transport: async () => "", verifyMappings: async () => {},
    discover: async () => discovery(), catalogue: async () => [], readiness: async () => [],
    resolveSsh: async (destination) => {
      if (!destination.includes("@")) return { connection: { kind: "alias", alias: destination }, alias: destination };
      const [user, hostname] = destination.split("@") as [string, string];
      return hostname === "site-login"
        ? { connection: { kind: "direct", hostname: "192.0.2.42", user, port: 2202, jump: "gateway.example.test" }, alias: hostname }
        : { connection: { kind: "direct", hostname, user, port: 22 }, alias: hostname };
    },
  };
}

async function drafts(): Promise<SetupDraft[]> {
  const path = join(dirname(configPath), "setup-drafts.json");
  const value = JSON.parse(await readFile(path, "utf8"));
  expect(value.version).toBe(1);
  expect((await stat(path)).mode & 0o777).toBe(0o600);
  return value.drafts;
}

for (const withBaseline of [false, true]) {
  for (const stage of ["destination", "name", "description", "consent"] as const) {
    test(`preconsent cancellation at ${stage} preserves ${withBaseline ? "existing config" : "fresh home"} byte-exactly`, async () => {
      if (withBaseline) await saveBaseline();
      const before = await tree();
      const replies: Record<string, Reply> = {};
      if (stage === "destination") replies["SSH destination"] = undefined;
      if (stage === "name") replies["Cluster name"] = undefined;
      if (stage === "description") replies["Description (optional)"] = undefined;
      const ui = new WizardUI("cancelled", {
        replies, confirm: stage !== "consent",
        beforePrompt: async () => { expect(await tree()).toEqual(before); },
      });
      expect(await ui.run()).toBeUndefined();
      ui.expectScriptUsed();
      expect(await tree()).toEqual(before);
      expect(ui.published).toEqual([]);
      expect(ui.notices.some(({ message }) => /before consent|no credentials/i.test(message))).toBe(true);
    });
  }
}

test("consent pauses with only two shell commands, no SSH or overlay, and a fresh invocation resumes", async () => {
  const calls: string[] = [];
  let consented = false;
  const paused = new WizardUI("resume-me", {
    setup: {
      transport: async () => { calls.push("ssh"); return ""; },
      verifyMappings: async () => { calls.push("verify"); },
      discover: async () => { calls.push("discover"); return discovery(); },
    },
    beforePrompt: ({ kind }) => {
      expect(consented).toBe(false);
      if (kind === "confirm") consented = true;
    },
  });
  expect(await paused.run()).toBeUndefined();
  paused.expectScriptUsed();
  expect(consented).toBe(true);
  expect(calls).toEqual([]);
  expect(paused.prompts.map(({ kind }) => kind)).toEqual(["input", "input", "input", "confirm"]);
  expect(paused.editorTexts).toEqual(["/cluster-add resume-me"]);
  expect(await loadClusterProfiles(env)).toEqual([]);
  const draft = (await drafts())[0]!;
  expect(draft).toMatchObject({ id: "resume-me", phase: "ssh", mappings: [] });
  expect(paused.published).toHaveLength(1);
  expect(bootstrapCommands(paused.published[0]!, 2)).toEqual(expectedBootstrapCommands(draft.ssh));
  const key = join(dirname(configPath), "id_ed25519");
  const bytes = await readFile(key);
  expect((await stat(key)).mode & 0o777).toBe(0o600);
  expect(await readFile(join(dirname(configPath), "known_hosts"), "utf8")).toBe("");
  const resumed = new WizardUI("resume-me");
  const profile = await resumed.run();
  resumed.expectScriptUsed();
  expect(profile).toBeDefined();
  for (const prompt of ["SSH destination", "Cluster name", "Create shared SSH connection", "Authorize the public key"]) expect(resumed.matching(prompt)).toEqual([]);
  expect(resumed.published.join("\n")).not.toContain("```sh");
  expect(resumed.editorTexts).toEqual([]);
  expect(await readFile(key)).toEqual(bytes);
  expect(await drafts()).toEqual([]);
  expect(await loadClusterProfiles(env)).toEqual([profile!]);
  expect(await readdir(join(dirname(configPath), "ssh"))).toEqual(["resume-me"]);
  await expectSafePublication(paused, profile!);
});

test("direct registration preserves old entries and uses minimal questions and safe defaults", async () => {
  const old = baseline();
  await saveBaseline([old]);
  const ui = new WizardUI("cpu-direct");
  const result = await ui.pauseAndResume();
  ui.expectScriptUsed();
  expect(result).toMatchObject({
    id: discoveredTargetId("cpu-direct", target()), description: "Research cluster; site utilization limits apply",
    ssh_host: "192.0.2.17", ssh_config_file: join(dirname(configPath), "ssh", "cpu-direct", "config"),
    storage_id: "research-storage", shared_root: "/cluster/research group", compute_kind: "cpu", max_gpus: 0,
    runtime: "apptainer", max_walltime_seconds: 604800, failure_grace_seconds: 0,
    termination_grace_seconds: 20, warning_seconds: 120, poll_seconds: 15,
  });
  expect(result).not.toHaveProperty("account");
  expect(result).not.toHaveProperty("gpu_type");
  expect((await document()).clusters).toEqual([old, result]);
  expect((await loadClusterProfiles(env)).at(-1)).toEqual(result);
  for (const obsolete of ["Compute kind", "SSH username", "SSH port", "Slurm partition", "Slurm GPU type", "Maximum GPUs", "Maximum walltime", "Container runtime", "Trusted runtime", "Slurm account"]) expect(ui.matching(obsolete)).toEqual([]);
  expect(ui.matching("Local Tailnet enrollment KEY FILE")).toEqual([]);
  expect(ui.matching("Tailnet login server")).toEqual([]);
  expect(ui.matching("Review cluster")[0]?.options).toEqual(["Register targets", "Edit review", "Advanced", "Save and exit"]);
  await expectSecureFiles(result!);
  const ssh = await effectiveSSH(result!);
  expect(ssh.get("hostname")).toBe("192.0.2.17");
  expect(ssh.get("user")).toBe("researcher");
  expect(ssh.get("port")).toBe("22");
  await expectSafePublication(ui, result!);
});

test("existing aliases retain effective host, jump, port and multiplexing without editing user SSH config", async () => {
  const original = await readFile(join(home, ".ssh", "config"), "utf8");
  const ui = new WizardUI("gpu-alias", {
    replies: { "SSH destination": "site-login" },
    setup: { discover: async () => discovery({ targets: [target("gpu-partition", "a100_80gb")] }) },
  });
  const profile = await ui.pauseAndResume();
  ui.expectScriptUsed();
  expect(profile).toMatchObject({ compute_kind: "gpu", gpu_type: "a100_80gb", max_gpus: 4, ssh_host: "site-login" });
  expect(await readFile(join(home, ".ssh", "config"), "utf8")).toBe(original);
  const ssh = await effectiveSSH(profile!);
  for (const [field, value] of Object.entries({ hostname: "192.0.2.42", user: "site-user", port: "2202", proxyjump: "gateway.example.test", controlmaster: "auto", controlpersist: "300", identitiesonly: "yes" })) expect(ssh.get(field)).toBe(value);
  await expectSecureFiles(profile!);
  await expectSafePublication(ui, profile!);
});

test("a fresh invocation makes one strict true probe before mapping verification and discovery", async () => {
  const order: string[] = [];
  const controller = new AbortController();
  const setup: SetupOptions = {
    signal: controller.signal,
    transport: async (connection, command, stdin, options) => {
      order.push("ssh");
      expect(connection).toEqual((await drafts())[0]!.ssh);
      expect(command).toBe("true");
      expect(stdin).toBeUndefined();
      expect(options).toMatchObject({ timeout_ms: 15_000, signal: controller.signal });
      return "";
    },
    verifyMappings: async () => { order.push("verify"); },
    discover: async () => { order.push("discover"); return discovery(); },
  };
  const paused = new WizardUI("ordered", { setup });
  expect(await paused.run()).toBeUndefined();
  expect(order).toEqual([]);
  const resumed = new WizardUI("ordered", { setup });
  expect(await resumed.run()).toBeDefined();
  paused.expectScriptUsed();
  resumed.expectScriptUsed();
  expect(order).toEqual(["ssh", "verify", "discover"]);
  expect(resumed.matching("WH shared path")).toHaveLength(1);
  expect(resumed.matching("Authorize the public key")).toEqual([]);
  expect(resumed.published.join("\n")).not.toContain("```sh");
});

test("a saved connection phase prepares files and pauses without probing or opening an overlay", async () => {
  const initial = new WizardUI("connection-resume");
  expect(await initial.run()).toBeUndefined();
  const saved = await drafts();
  saved[0]!.phase = "connection";
  await writeFile(join(dirname(configPath), "setup-drafts.json"), JSON.stringify({ version: 1, drafts: saved }), { mode: 0o600 });
  const key = await readFile(join(dirname(configPath), "id_ed25519"));
  let calls = 0;
  const resumed = new WizardUI("connection-resume", { setup: { transport: async () => { calls++; return ""; } } });
  expect(await resumed.run()).toBeUndefined();
  initial.expectScriptUsed();
  resumed.expectScriptUsed();
  expect(calls).toBe(0);
  expect(resumed.prompts).toEqual([]);
  expect(resumed.editorTexts).toEqual(["/cluster-add connection-resume"]);
  expect((await drafts())[0]).toMatchObject({ phase: "ssh", mappings: [] });
  expect(await readFile(join(dirname(configPath), "id_ed25519"))).toEqual(key);
  expect(resumed.published).toHaveLength(1);
  expect(bootstrapCommands(resumed.published[0]!, 2)).toEqual(expectedBootstrapCommands(saved[0]!.ssh));
  expect(await loadClusterProfiles(env)).toEqual([]);
});

test("editor prefill is optional for both the bootstrap pause and a later successful resume", async () => {
  const paused = new WizardUI("no-editor-prefill");
  paused.setEditorText = undefined;
  expect(await paused.run()).toBeUndefined();
  paused.expectScriptUsed();
  expect(paused.editorTexts).toEqual([]);
  expect((await drafts())[0]!.phase).toBe("ssh");
  expect(bootstrapCommands(paused.published[0]!, 2)).toEqual(expectedBootstrapCommands((await drafts())[0]!.ssh));
  const resumed = new WizardUI("no-editor-prefill");
  resumed.setEditorText = undefined;
  expect(await resumed.run()).toBeDefined();
  resumed.expectScriptUsed();
  expect(resumed.editorTexts).toEqual([]);
  expect(resumed.published.join("\n")).not.toContain("```sh");
});

for (const phase of ["ssh", "mappings", "discovery", "review"] as const) {
  test(`one failed strict probe retains an existing ${phase} draft and publishes commands without an overlay`, async () => {
    const paused = new WizardUI(`failed-${phase}`);
    expect(await paused.run()).toBeUndefined();
    const saved = await drafts();
    saved[0]!.phase = phase;
    if (phase !== "ssh") saved[0]!.mappings = [{ storage_id: "research-storage", shared_root: "/cluster/research group" }];
    await writeFile(join(dirname(configPath), "setup-drafts.json"), JSON.stringify({ version: 1, drafts: saved }), { mode: 0o600 });
    const before = await tree();
    const calls: string[] = [];
    const resumed = new WizardUI(`failed-${phase}`, { setup: {
      transport: async (_connection, command) => { calls.push(command); throw new Error("Permission denied (publickey)"); },
      verifyMappings: async () => { calls.push("verify"); },
      discover: async () => { calls.push("discover"); return discovery(); },
      readiness: async () => { calls.push("readiness"); return []; },
    } });
    expect(await resumed.run()).toBeUndefined();
    paused.expectScriptUsed();
    resumed.expectScriptUsed();
    expect(calls).toEqual(["true"]);
    expect(resumed.prompts).toEqual([]);
    expect(resumed.editorTexts).toEqual([`/cluster-add failed-${phase}`]);
    expect(resumed.published).toHaveLength(1);
    expect(bootstrapCommands(resumed.published[0]!, 2)).toEqual(expectedBootstrapCommands(saved[0]!.ssh));
    expect(resumed.notices.some(({ type, message }) => type === "warning" && /^Strict SSH failed:/.test(message) && /draft retained/.test(message) && message.includes(`/cluster-add failed-${phase}`))).toBe(true);
    expect(await drafts()).toEqual(saved);
    expect(await tree()).toEqual(before);
    expect(await loadClusterProfiles(env)).toEqual([]);
  });
}

async function seedSharedTrust(lookup: string, hashed = false): Promise<string> {
  const directory = dirname(configPath);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const key = join(directory, "id_ed25519");
  await local(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", key]);
  await chmod(key + ".pub", 0o600);
  const hostKey = join(root, "fixture-host-key");
  await local(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", hostKey]);
  const trust = join(directory, "known_hosts");
  await writeFile(trust, `${lookup} ${(await readFile(hostKey + ".pub", "utf8")).trim()}\n`, { mode: 0o600 });
  if (hashed) await local(["ssh-keygen", "-H", "-f", trust]);
  return readFile(trust, "utf8");
}

for (const fixture of [
  { label: "plain default-port hostname", destination: "researcher@192.0.2.17", lookup: "192.0.2.17" },
  { label: "hashed nondefault-port hostname", destination: "researcher@site-login", lookup: "[192.0.2.42]:2202", hashed: true },
  { label: "hashed alias effective hostname and port", destination: "site-login", lookup: "[192.0.2.42]:2202", hashed: true },
  { label: "HostKeyAlias verbatim on a nondefault port", destination: "site-login", lookup: "site-trust-name", hostKeyAlias: "site-trust-name", hashed: true },
]) {
  test(`verified ${fixture.label} omits the separate trust command and preserves trust bytes`, async () => {
    if (fixture.hostKeyAlias) await writeFile(join(home, ".ssh", "config"), (await readFile(join(home, ".ssh", "config"), "utf8")) + `Host site-login\n  HostKeyAlias ${fixture.hostKeyAlias}\n`);
    const trust = await seedSharedTrust(fixture.lookup, fixture.hashed);
    let probes = 0;
    const ui = new WizardUI("known-host", {
      replies: { "SSH destination": fixture.destination },
      setup: { transport: async () => { probes++; return ""; } },
    });
    expect(await ui.run()).toBeUndefined();
    ui.expectScriptUsed();
    expect(probes).toBe(0);
    expect(ui.prompts.every(({ kind }) => kind === "input" || kind === "confirm")).toBe(true);
    const draft = (await drafts())[0]!;
    expect(bootstrapCommands(ui.published[0]!, 1)).toEqual(expectedBootstrapCommands(draft.ssh).slice(0, 1));
    expect(await readFile(join(dirname(configPath), "known_hosts"), "utf8")).toBe(trust);
  });
}

for (const lookup of ["192.0.2.42", "site-login", "[site-trust-name]:2202"]) {
  test(`a known_hosts entry for the wrong effective lookup ${lookup} does not suppress the trust command`, async () => {
    if (lookup.includes("site-trust-name")) await writeFile(join(home, ".ssh", "config"), (await readFile(join(home, ".ssh", "config"), "utf8")) + "Host site-login\n  HostKeyAlias site-trust-name\n");
    const trust = await seedSharedTrust(lookup, true);
    const ui = new WizardUI("wrong-known-host", { replies: { "SSH destination": "site-login" } });
    expect(await ui.run()).toBeUndefined();
    ui.expectScriptUsed();
    expect(bootstrapCommands(ui.published[0]!, 2)).toEqual(expectedBootstrapCommands((await drafts())[0]!.ssh));
    expect(await readFile(join(dirname(configPath), "known_hosts"), "utf8")).toBe(trust);
  });
}

for (const hostKeyFailure of [false, true]) {
  test(`an old saved ssh phase ${hostKeyFailure ? "host-key failure restores" : "authentication failure omits"} the trust command for a known host`, async () => {
    const trust = await seedSharedTrust("192.0.2.17", true);
    const paused = new WizardUI("old-ssh");
    expect(await paused.run()).toBeUndefined();
    expect(bootstrapCommands(paused.published[0]!, 1)).toEqual(expectedBootstrapCommands((await drafts())[0]!.ssh).slice(0, 1));
    const before = await tree();
    let probes = 0;
    const resumed = new WizardUI("old-ssh", { setup: { transport: async () => {
      probes++;
      throw new Error(hostKeyFailure ? "Host key verification failed." : "Permission denied (publickey)");
    } } });
    if (!hostKeyFailure) resumed.setEditorText = undefined;
    expect(await resumed.run()).toBeUndefined();
    resumed.expectScriptUsed();
    expect(probes).toBe(1);
    expect(resumed.prompts).toEqual([]);
    expect(bootstrapCommands(resumed.published[0]!, hostKeyFailure ? 2 : 1)).toEqual(expectedBootstrapCommands((await drafts())[0]!.ssh).slice(0, hostKeyFailure ? 2 : 1));
    expect(resumed.editorTexts).toEqual(hostKeyFailure ? ["/cluster-add old-ssh"] : []);
    expect((await drafts())[0]!.phase).toBe("ssh");
    expect(await readFile(join(dirname(configPath), "known_hosts"), "utf8")).toBe(trust);
    expect(await tree()).toEqual(before);
  });
}

test("unprovable effective SSH settings retain the explicit trust command", async () => {
  const trust = await seedSharedTrust("192.0.2.17");
  await writeFile(join(home, ".ssh", "config"), "UnsupportedFixtureOption yes\n", { mode: 0o600 });
  const ui = new WizardUI("unprovable-trust");
  expect(await ui.run()).toBeUndefined();
  ui.expectScriptUsed();
  expect((await drafts())[0]!.phase).toBe("ssh");
  expect(bootstrapCommands(ui.published[0]!, 2)).toEqual(expectedBootstrapCommands((await drafts())[0]!.ssh));
  expect(await readFile(join(dirname(configPath), "known_hosts"), "utf8")).toBe(trust);
});

test("the exact two-mapping catalogue loop keeps primary identity and forwards both mappings", async () => {
  const mappings = [{ storage_id: "research-storage", shared_root: "/cluster/research group" }, { storage_id: "datasets", shared_root: "/cluster/datasets" }];
  const ui = new WizardUI("two-filesystems", {
    replies: {
      "WH shared filesystem": ["/data/shared/research-storage", "/data/shared/datasets"],
      "Same filesystem root": ["/cluster//research group/", "/cluster/datasets"],
      "Add another filesystem mapping?": ["Yes", "No"],
    },
    setup: {
      catalogue: async () => ["/data/shared/research-storage", "/data/shared/datasets", "/data/local/not-shared"],
      verifyMappings: async (_connection, value) => { expect(value).toEqual(mappings); },
      discover: async (_connection, input) => { expect(input.mappings).toEqual(mappings); return discovery(); },
    },
  });
  expect(await ui.pauseAndResume()).toMatchObject({ ...mappings[0], additional_shared_roots: [mappings[1]] });
  ui.expectScriptUsed();
  expect(ui.matching("WH shared filesystem").map(({ options }) => options)).toEqual([
    ["/data/shared/research-storage", "/data/shared/datasets", "Enter /data/shared/<id>"],
    ["/data/shared/datasets", "Enter /data/shared/<id>"],
  ]);
  expect(ui.matching("Add another filesystem mapping?").map(({ title, options }) => ({ title, options }))).toEqual([
    { title: "Add another filesystem mapping?", options: ["No", "Yes"] },
    { title: "Add another filesystem mapping?", options: ["No", "Yes"] },
  ]);
  expect(ui.matching("WH shared path")).toEqual([]);
});

test("manual path and URL corrections retain earlier answers", async () => {
  const ui = new WizardUI("corrected", { replies: {
    "WH shared path": ["/data/local/wrong", "/data/shared/research-storage"],
    "Same filesystem root": ["relative/shared", "/cluster/../escape", "/cluster//corrected shared/"],
    "Local Tailnet enrollment KEY FILE": ["relative/key", join(home, "tailnet enrollment key")],
    "Tailnet login server": ["https://user:secret@control.example.test", "https://control.example.test"],
  } });
  expect(await ui.pauseAndResume()).toMatchObject({ shared_root: "/cluster/corrected shared" });
  ui.expectScriptUsed();
  expect(ui.matching("SSH destination")).toHaveLength(1);
  expect(ui.notices.filter(({ type }) => type === "error").length).toBeGreaterThanOrEqual(5);
});

test("automatic CPU/GPU IDs are collision-safe; ceilings are seven days or lower partition caps", async () => {
  const targets = [target("cpu.part"), target("cpu-part"), target("short", undefined, 3600), target("gpu", "a100.80gb"), target("gpu", "a100-80gb")];
  const ui = new WizardUI("all-targets", { setup: { discover: async () => discovery({ targets }) } });
  expect(await ui.pauseAndResume()).toBeDefined();
  ui.expectScriptUsed();
  const profiles = (await document()).clusters;
  expect(profiles).toHaveLength(5);
  expect(new Set(profiles.map(({ id }) => id)).size).toBe(5);
  expect(profiles.map(({ id }) => id)).toEqual(targets.map((candidate) => discoveredTargetId("all-targets", candidate)));
  expect(profiles.map(({ max_walltime_seconds }) => max_walltime_seconds)).toEqual([604800, 604800, 3600, 604800, 604800]);
  expect(profiles.filter(({ compute_kind }) => compute_kind === "gpu").map(({ gpu_type }) => gpu_type)).toEqual(["a100.80gb", "a100-80gb"]);
  expect(new Set(profiles.map(({ ssh_config_file }) => ssh_config_file)).size).toBe(1);
});

test("an existing ID is rejected without replacing its profile or managed secret", async () => {
  await saveBaseline();
  const directory = join(dirname(configPath), "ssh", "existing");
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await writeFile(join(directory, "id_ed25519"), "existing-private-secret", { mode: 0o600 });
  const before = await tree();
  const ui = new WizardUI("existing", { replies: { "Cluster name": ["existing", undefined] } });
  expect(await ui.run()).toBeUndefined();
  ui.expectScriptUsed();
  expect(ui.matching("Cluster name")).toHaveLength(2);
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

test("global integration prompts once and is reused by later clusters without publishing secrets", async () => {
  const firstUI = new WizardUI("first-integration");
  const first = await firstUI.pauseAndResume();
  firstUI.expectScriptUsed();
  expect(first).toMatchObject({ tailnet_key_file: join(home, "tailnet enrollment key"), tailnet_login_server: "https://control.example.test", orchestrator_host: defaultRegistrationHost(), orchestrator_registration_port: 12888 });
  expect(firstUI.matching("Local Tailnet enrollment KEY FILE")).toHaveLength(1);
  expect(firstUI.matching("Tailnet login server")).toHaveLength(1);
  expect(firstUI.matching("Orchestrator")).toEqual([]);
  const settingsPath = join(dirname(configPath), "cluster-integration.json");
  const settings = await readFile(settingsPath, "utf8");
  expect(JSON.parse(settings)).toEqual({ version: 1, integration: {
    tailnet_key_file: first!.tailnet_key_file, tailnet_login_server: first!.tailnet_login_server,
    orchestrator_host: first!.orchestrator_host, orchestrator_registration_port: 12888,
  } });
  expect(settings).not.toContain(enrollmentSecret);
  expect((await stat(settingsPath)).mode & 0o777).toBe(0o600);
  const secondUI = new WizardUI("second-integration");
  expect(await secondUI.pauseAndResume()).toBeDefined();
  secondUI.expectScriptUsed();
  expect(secondUI.matching("Local Tailnet enrollment KEY FILE")).toEqual([]);
  expect(secondUI.matching("Tailnet login server")).toEqual([]);
  expect(await readFile(settingsPath, "utf8")).toBe(settings);
  expect([...firstUI.published, ...secondUI.published].join("\n")).not.toContain(enrollmentSecret);
});

test("unambiguous aliases automatically reuse stable mappings without copying credentials", async () => {
  const source = await new WizardUI("source-alias", { replies: { "SSH destination": "site-login" } }).pauseAndResume();
  expect(source).toBeDefined();
  const sourceFiles = await tree(dirname(source!.ssh_config_file!));
  const key = await readFile(join(dirname(configPath), "id_ed25519"));
  const ui = new WizardUI("reused-alias", { replies: { "SSH destination": "site-login" } });
  expect(await ui.pauseAndResume()).toMatchObject({ storage_id: source!.storage_id, shared_root: source!.shared_root, ssh_host: "site-login" });
  ui.expectScriptUsed();
  expect(ui.matching("WH shared path")).toEqual([]);
  expect(ui.matching("Same filesystem root")).toEqual([]);
  expect(await tree(dirname(source!.ssh_config_file!))).toEqual(sourceFiles);
  expect(await readFile(join(dirname(configPath), "id_ed25519"))).toEqual(key);
});

for (const invalidPublicFile of ["private-key", "invalid-ed25519-blob"] as const) {
  test(`shared identity rejects ${invalidPublicFile} in the public-key file without publishing or committing it`, async () => {
    const sourceUI = new WizardUI("public-key-source");
    const source = await sourceUI.pauseAndResume();
    sourceUI.expectScriptUsed();
    expect(source).toBeDefined();
    const key = join(dirname(configPath), "id_ed25519");
    const privateKey = await readFile(key, "utf8");
    await writeFile(key + ".pub", invalidPublicFile === "private-key" ? privateKey : "ssh-ed25519 c2VjcmV0 invalid-key-blob");
    const before = await tree();
    const copied = new WizardUI("unsafe-public-copy");
    expect(await copied.run()).toBeUndefined();
    copied.expectScriptUsed();
    expect(copied.matching("Create shared SSH connection")).toEqual([]);
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

for (const missing of ["runtime", "image"] as const) {
  test(`only missing ${missing} asks a conditional corrective field before rediscovery`, async () => {
    let calls = 0;
    const ui = new WizardUI(`missing-${missing}`, {
      replies: { "Discovery prerequisites unresolved": "Provide missing runtime / image" },
      setup: { discover: async (_connection, input) => {
        if (++calls === 1) return discovery({
          ...(missing === "runtime" ? { runtime: undefined } : { sif_path: undefined }),
          unresolved: [{ kind: missing, blocking: true, message: `Missing ${missing}` }],
        });
        if (missing === "runtime") expect(input.runtime_setup).toBe("module load apptainer");
        else expect(input.sif_path).toBe("/cluster/research group/worker.sif");
        return discovery();
      } },
    });
    expect(await ui.pauseAndResume()).toBeDefined();
    ui.expectScriptUsed();
    expect(calls).toBe(2);
    expect(ui.matching("Trusted runtime shell prelude")).toHaveLength(missing === "runtime" ? 1 : 0);
    expect(ui.matching("Compatible Worker SIF")).toHaveLength(missing === "image" ? 1 : 0);
    expect(ui.matching("Billing default unresolved")).toEqual([]);
  });
}

test("billing is conditional and an explicitly chosen account is rediscovered", async () => {
  let calls = 0;
  const ui = new WizardUI("billing", {
    replies: { "Billing default unresolved": "Account project-b" },
    setup: { discover: async (_connection, input) => {
      if (++calls === 1) return discovery({ accounts: ["project-a", "project-b"], unresolved: [{ kind: "billing", blocking: false, message: "No site default" }] });
      expect(input.account).toBe("project-b");
      return discovery({ account: "project-b", account_source: "explicit" });
    } },
  });
  expect(await ui.pauseAndResume()).toMatchObject({ account: "project-b" });
  ui.expectScriptUsed();
  expect(calls).toBe(2);
  expect(ui.matching("Billing default unresolved")[0]?.options).toEqual(["Use scheduler default", "Account project-a", "Account project-b"]);
});

for (const missing of ["image", "key-file", "private-key-file", "key-file-symlink", "live-openapi"] as const) {
  test(`${missing} saves blocked setup without registering any ready target`, async () => {
    const key = join(home, "tailnet enrollment key");
    if (missing === "key-file") await rm(key);
    if (missing === "private-key-file") await chmod(key, 0o644);
    if (missing === "key-file-symlink") { await rename(key, key + ".real"); await symlink(key + ".real", key); }
    const ui = new WizardUI(`blocked-${missing}`, { setup: {
      ...(missing === "image" ? { discover: async () => discovery({ sif_path: undefined, unresolved: [{ kind: "image", blocking: true, message: "Compatible image missing" }] }) } : {}),
      readiness: async () => missing === "live-openapi" ? ["Control deployment lacks POST /api/v1/workers/reservations and GET /api/v1/job-events"] : [],
    } });
    expect(await ui.pauseAndResume()).toBeUndefined();
    ui.expectScriptUsed();
    expect(await loadClusterProfiles(env)).toEqual([]);
    expect((await drafts())[0]).toMatchObject({ phase: "blocked" });
    expect(ui.matching("Review cluster")).toEqual([]);
    expect(ui.published.join("\n")).toMatch(/blocked/i);
    expect(ui.published.join("\n")).not.toContain(enrollmentSecret);
  });
}

test("failed mapping verification blocks before discovery/readiness with mappings retained", async () => {
  const calls: string[] = [];
  const ui = new WizardUI("mapping-mismatch", { setup: {
    verifyMappings: async () => { throw new Error("Shared filesystem identity could not be verified"); },
    discover: async () => { calls.push("discovery"); return discovery(); },
    readiness: async () => { calls.push("readiness"); return []; },
  } });
  expect(await ui.pauseAndResume()).toBeUndefined();
  ui.expectScriptUsed();
  expect(calls).toEqual([]);
  expect(await loadClusterProfiles(env)).toEqual([]);
  expect((await drafts())[0]).toMatchObject({ phase: "blocked", mappings: [{ storage_id: "research-storage", shared_root: "/cluster/research group" }] });
});

for (const stage of ["mapping", "mapping-loop", "runtime-editor", "image-input", "review-escape", "review-save", "advanced", "edit-review", "final-confirm"] as const) {
  test(`cancellation at ${stage} retains honest resumable state with no targets`, async () => {
    const replies: Record<string, Reply> = {};
    if (stage === "mapping") replies["Same filesystem root"] = undefined;
    if (stage === "mapping-loop") replies["Add another filesystem mapping?"] = undefined;
    if (stage === "runtime-editor" || stage === "image-input") {
      replies["Discovery prerequisites unresolved"] = "Provide missing runtime / image";
      replies[stage === "runtime-editor" ? "Trusted runtime shell prelude" : "Compatible Worker SIF"] = undefined;
    }
    if (stage === "review-escape") replies["Review cluster"] = undefined;
    if (stage === "review-save") replies["Review cluster"] = "Save and exit";
    if (stage === "advanced") { replies["Review cluster"] = "Advanced"; replies["Advanced allocation settings"] = undefined; }
    if (stage === "edit-review") { replies["Review cluster"] = "Edit review"; replies["Edit cluster review"] = undefined; }
    const ui = new WizardUI(`cancel-${stage}`, {
      replies, confirms: { "Register discovered cluster targets?": stage !== "final-confirm" },
      setup: stage === "runtime-editor" || stage === "image-input" ? {
        discover: async () => discovery({ unresolved: [{ kind: stage === "runtime-editor" ? "runtime" : "image", blocking: true, message: "Missing prerequisite" }] }),
      } : {},
    });
    expect(await ui.pauseAndResume()).toBeUndefined();
    ui.expectScriptUsed();
    expect(await loadClusterProfiles(env)).toEqual([]);
    expect(await drafts()).toHaveLength(1);
    expect((await stat(join(dirname(configPath), "id_ed25519"))).isFile()).toBe(true);
    expect(ui.notices.some(({ message }) => /draft retained/.test(message))).toBe(true);
    expect(ui.notices.some(({ message }) => /nothing saved|before consent/.test(message))).toBe(false);
  });
}

test("resuming a partial mapping repeats the extra-filesystem question without duplicating primary", async () => {
  const paused = new WizardUI("partial-mapping", { replies: { "Add another filesystem mapping?": undefined } });
  expect(await paused.pauseAndResume()).toBeUndefined();
  paused.expectScriptUsed();
  expect((await drafts())[0]!.mappings).toHaveLength(1);
  const resumed = new WizardUI("partial-mapping");
  expect(await resumed.run()).toBeDefined();
  resumed.expectScriptUsed();
  expect(resumed.matching("WH shared path")).toEqual([]);
  expect(resumed.matching("Same filesystem root")).toEqual([]);
  expect(resumed.matching("Add another filesystem mapping?")).toHaveLength(1);
});

test("aborting discovery passes the signal and preserves the saved discovery-stage draft", async () => {
  const controller = new AbortController();
  const ui = new WizardUI("abort-discovery", { setup: {
    signal: controller.signal,
    discover: async (_connection, _input, options) => {
      expect(options?.signal).toBe(controller.signal);
      controller.abort();
      controller.signal.throwIfAborted();
      return discovery();
    },
  } });
  expect(await ui.pauseAndResume()).toBeUndefined();
  ui.expectScriptUsed();
  expect((await drafts())[0]!.phase).toBe("discovery");
  expect(await loadClusterProfiles(env)).toEqual([]);
  expect(ui.notices.some(({ message }) => /draft retained/.test(message))).toBe(true);
});

test("advanced failure grace requires idle-policy opt-in while the partition remains a hard ceiling", async () => {
  const ui = new WizardUI("advanced-opt-in", {
    replies: {
      "Review cluster": ["Advanced", "Register targets"],
      "Advanced allocation settings": JSON.stringify({ failure_grace_seconds: 300, max_walltime_seconds: 604800 }),
    },
    setup: { discover: async () => discovery({ targets: [target("short", undefined, 7200)] }) },
  });
  expect(await ui.pauseAndResume()).toMatchObject({ failure_grace_seconds: 300, max_walltime_seconds: 7200 });
  ui.expectScriptUsed();
  expect(ui.matching("Opt in to failure diagnosis idle holding?")[0]?.current).toMatch(/idle allocations.*site.*utilization policy/i);
});

test("concurrent config edits at final confirmation survive atomic registration of the entire target set", async () => {
  const initial = baseline();
  await saveBaseline([initial]);
  const concurrent = { ...baseline("operator-added"), description: "Concurrent operator edit" };
  const ui = new WizardUI("atomic-set", {
    setup: { discover: async () => discovery({ targets: [target("cpu"), target("gpu", "a100")] }) },
    beforePrompt: async ({ title }) => { if (title === "Register discovered cluster targets?") await saveBaseline([initial, concurrent]); },
  });
  expect(await ui.pauseAndResume()).toBeDefined();
  ui.expectScriptUsed();
  const saved = (await document()).clusters;
  expect(saved.slice(0, 2)).toEqual([initial, concurrent]);
  expect(saved.slice(2).map(({ compute_kind }) => compute_kind)).toEqual(["cpu", "gpu"]);
});

test("concurrent draft revision blocks all targets without changing existing config", async () => {
  const before = await saveBaseline();
  const ui = new WizardUI("concurrent-draft", {
    setup: { discover: async () => discovery({ targets: [target("cpu"), target("gpu", "a100")] }) },
    beforePrompt: async ({ title }) => {
      if (title !== "Register discovered cluster targets?") return;
      const saved = await drafts();
      saved[0]!.revision++;
      await writeFile(join(dirname(configPath), "setup-drafts.json"), JSON.stringify({ version: 1, drafts: saved }), { mode: 0o600 });
    },
  });
  expect(await ui.pauseAndResume()).toBeUndefined();
  ui.expectScriptUsed();
  expect(await readFile(configPath, "utf8")).toBe(before);
  expect(ui.notices.some(({ message }) => /changed concurrently/.test(message))).toBe(true);
});

test("commit rename failure leaves existing config byte-exact and the full target set resumable", async () => {
  const before = await saveBaseline();
  let replacements = 0;
  const targets = [target("cpu"), target("gpu", "a100")];
  const ui = new WizardUI("rename-failure", { setup: {
    discover: async () => discovery({ targets }),
    replaceConfig: async (_source, destination) => { replacements++; await rename(join(root, "missing-staged-file"), destination); },
  } });
  expect(await ui.pauseAndResume()).toBeUndefined();
  ui.expectScriptUsed();
  expect(replacements).toBe(1);
  expect(await readFile(configPath, "utf8")).toBe(before);
  expect(await drafts()).toHaveLength(1);
  expect((await tree()).some(({ path }) => /\.lock$|\.tmp$/.test(path))).toBe(false);
  const retry = new WizardUI("rename-failure", { setup: { discover: async () => discovery({ targets }) } });
  expect(await retry.run()).toBeDefined();
  retry.expectScriptUsed();
  expect((await document()).clusters).toHaveLength(3);
});

test("successful atomic commit reports retained draft if only draft cleanup fails", async () => {
  const paused = new WizardUI("cleanup-failure", { replies: { "Review cluster": "Save and exit" } });
  expect(await paused.pauseAndResume()).toBeUndefined();
  paused.expectScriptUsed();
  const draft = (await drafts())[0]!;
  const profiles = buildDiscoveredProfiles(draft, draft.discovery!);
  const draftPath = join(dirname(configPath), "setup-drafts.json");
  const result = await commitSetupProfiles(draft, profiles, env, async (source, destination) => {
    await rename(source, destination);
    // Prevent only cleanup's rename, after the real completed-profile commit.
    await rename(draftPath, draftPath + ".retained");
    await mkdir(draftPath, { mode: 0o700 });
  });
  expect(result).toEqual({ draftRetained: true });
  expect((await document()).clusters).toEqual(profiles);
  expect(JSON.parse(await readFile(draftPath + ".retained", "utf8")).drafts).toEqual([draft]);
});

test("resuming after discovered partitions and GPU types change replaces stale selections with a fresh target", async () => {
  const oldTargets = [target("old-cpu"), target("old-gpu", "a100")];
  const paused = new WizardUI("changed-discovery", {
    replies: { "Review cluster": "Save and exit" },
    setup: { discover: async () => discovery({ targets: oldTargets }) },
  });
  expect(await paused.pauseAndResume()).toBeUndefined();
  paused.expectScriptUsed();
  expect((await drafts())[0]!.selected_targets).toEqual(oldTargets.map((candidate) => discoveredTargetId("changed-discovery", candidate)));
  expect(await loadClusterProfiles(env)).toEqual([]);
  const freshTargets = [target("new-cpu"), target("new-gpu", "h100")];
  const resumed = new WizardUI("changed-discovery", {
    replies: { "Saved targets are no longer available": "new-gpu gpu h100" },
    setup: { discover: async () => discovery({ targets: freshTargets }) },
  });
  const profile = await resumed.run();
  resumed.expectScriptUsed();
  expect(profile).toMatchObject({
    id: discoveredTargetId("changed-discovery", freshTargets[1]!),
    partition: "new-gpu", compute_kind: "gpu", gpu_type: "h100",
  });
  expect(resumed.matching("Saved targets are no longer available")[0]?.options).toEqual(["new-cpu cpu", "new-gpu gpu h100", "Save and exit"]);
  expect(resumed.matching("Review cluster")).toHaveLength(1);
  expect(await loadClusterProfiles(env)).toEqual([profile!]);
  expect(await drafts()).toEqual([]);
});

test("an explicit user on a fixture alias preserves its resolved port, jump and original Host context", async () => {
  const ui = new WizardUI("explicit-alias-user", { replies: { "SSH destination": "researcher@site-login" } });
  const profile = await ui.pauseAndResume();
  ui.expectScriptUsed();
  expect(profile).toMatchObject({ ssh_host: "site-login" });
  const ssh = await effectiveSSH(profile!);
  for (const [field, value] of Object.entries({
    hostname: "192.0.2.42", user: "researcher", port: "2202", proxyjump: "gateway.example.test",
    controlmaster: "auto", controlpersist: "300",
  })) expect(ssh.get(field)).toBe(value);
  await expectBootstrapAuthentication(profile!);
});

test("cluster name defaults to the requested argument or short SSH hostname without extra connection questions", async () => {
  const requested = new WizardUI("unused", {
    replies: { "Cluster name": "" },
  });
  expect(await requested.run("requested-name")).toBeUndefined();
  requested.expectScriptUsed();
  expect(requested.matching("Cluster name")[0]?.current).toBe("requested-name");
  const hostname = new WizardUI("unused", { replies: {
    "SSH destination": "researcher@login.example.test", "Cluster name": "",
    "Resume cluster setup?": "New cluster",
  } });
  expect(await hostname.run("")).toBeUndefined();
  hostname.expectScriptUsed();
  expect(hostname.matching("Cluster name")[0]?.current).toBe("login");
  expect((await drafts()).map(({ id }) => id).sort()).toEqual(["login", "requested-name"]);
  expect(await loadClusterProfiles(env)).toEqual([]);
});

test("review editing registers only the selected discovered target and corrected description", async () => {
  const targets = [target("cpu"), target("gpu", "a100")];
  const selected = discoveredTargetId("edited-review", targets[1]!);
  const ui = new WizardUI("edited-review", {
    replies: {
      "Review cluster": ["Edit review", "Register targets"],
      "Edit cluster review": JSON.stringify({ description: "Selected GPU research target", selected_targets: [selected] }),
    },
    setup: { discover: async () => discovery({ targets }) },
  });
  expect(await ui.pauseAndResume()).toMatchObject({ id: selected, description: "Selected GPU research target", compute_kind: "gpu" });
  ui.expectScriptUsed();
  expect((await document()).clusters).toHaveLength(1);
  expect(ui.matching("Review cluster")[1]?.title).toContain("Selected GPU research target");
});

test("simultaneous commits of one reviewed draft register exactly one complete target set", async () => {
  const paused = new WizardUI("simultaneous-commit", {
    replies: { "Review cluster": "Save and exit" },
    setup: { discover: async () => discovery({ targets: [target("cpu"), target("gpu", "a100")] }) },
  });
  expect(await paused.pauseAndResume()).toBeUndefined();
  paused.expectScriptUsed();
  const draft = (await drafts())[0]!;
  const profiles = buildDiscoveredProfiles(draft, draft.discovery!);
  const key = await readFile(join(dirname(configPath), "id_ed25519"));
  const results = await Promise.allSettled([
    commitSetupProfiles(draft, profiles, env),
    commitSetupProfiles(draft, profiles, env),
  ]);
  expect(results.filter(({ status }) => status === "fulfilled")).toHaveLength(1);
  expect(results.filter(({ status }) => status === "rejected")).toHaveLength(1);
  expect((await document()).clusters).toEqual(profiles);
  expect(await readFile(join(dirname(configPath), "id_ed25519"))).toEqual(key);
  expect(await drafts()).toEqual([]);
});

test("UI reports registration success and retained-draft guidance when only cleanup fails", async () => {
  const draftPath = join(dirname(configPath), "setup-drafts.json");
  const ui = new WizardUI("cleanup-ui", { setup: {
    replaceConfig: async (source, destination) => {
      await rename(source, destination);
      await rename(draftPath, draftPath + ".retained");
      await mkdir(draftPath, { mode: 0o700 });
    },
  } });
  const profile = await ui.pauseAndResume();
  ui.expectScriptUsed();
  expect(profile).toBeDefined();
  expect(await loadClusterProfiles(env)).toEqual([profile!]);
  expect(ui.published.join("\n")).toMatch(/registered.*cluster:/s);
  expect(ui.published.join("\n")).toContain("Completed draft cleanup failed");
  expect(ui.published.join("\n")).toContain("not another target");
  expect(ui.notices.some(({ type }) => type === "error")).toBe(false);
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
  const ui = new WizardUI("native-system-policy", { replies: { "SSH destination": "site-login" } });
  const profile = await ui.pauseAndResume();
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
  if (system.warning) expect(ui.notices).toContainEqual({ message: system.warning, type: "warning" });
  expect(bootstrapCommands(ui.published[0]!, 2)).toEqual(expectedBootstrapCommands(profile!));
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
  const first = await new WizardUI("first").pauseAndResume();
  expect(first).toBeDefined();
  const ssh = dirname(configPath);
  const originalKey = await readFile(join(ssh, "id_ed25519"));
  const trust = "|1|verified-hash|verified-host ssh-ed25519 site-public-key\n# retain exactly\n";
  await writeFile(join(ssh, "known_hosts"), trust, { mode: 0o600 });
  const secondUI = new WizardUI("second", { replies: { "SSH destination": "researcher@192.0.2.18" } });
  const second = await secondUI.pauseAndResume();
  expect(second).toBeDefined();
  expect(await readFile(join(ssh, "id_ed25519"))).toEqual(originalKey);
  expect(await readFile(join(ssh, "known_hosts"), "utf8")).toBe(trust);
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
    expect(ui.published[0]).toContain("No SSH connection, key installation or allocation was performed");
    const blocks = ui.published[0]!.match(/```sh\n[\s\S]*?\n```/g) ?? [];
    expect(bootstrapCommands(blocks.join("\n\n"), profiles.length * 2)).toEqual(profiles.flatMap(expectedBootstrapCommands));
    for (const obsolete of ["SSH_AUTH_SOCK", "ssh-keygen -p", "ssh-add ", "authorized_keys"]) expect(ui.published[0]).not.toContain(obsolete);
    for (const pair of originals) expect(ui.published[0]).not.toContain(pair[1]!.toString("utf8").trim());
    for (const pair of originals) expect(ui.published[0]).not.toContain(pair[0]!.toString("utf8"));
    expect(await readFile(join(dirname(configPath), "known_hosts"), "utf8")).toBe("");
  });
}

test("migration uses the same commands and omits only the already verified hashed host trust step", async () => {
  const { profiles, keys } = await legacyProfiles();
  const trust = await seedSharedTrust("[192.0.2.17]:2222", true);
  const userConfig = await readFile(join(home, ".ssh", "config"));
  const sharedKey = await readFile(join(dirname(configPath), "id_ed25519"));
  const originalKeys = await Promise.all(keys.map((key) => readFile(key)));
  const ui = new WizardUI("migration-known-host");
  expect(await runClusterSshMigrate(ui, ui.publish, env)).toEqual(profiles.map(({ id }) => id));
  ui.expectScriptUsed();
  expect(ui.matching("Select existing")).toEqual([]);
  const blocks = ui.published.join("\n").match(/```sh\n[\s\S]*?\n```/g) ?? [];
  expect(bootstrapCommands(blocks.join("\n\n"), 3)).toEqual([
    expectedBootstrapCommands(profiles[0]!)[0]!,
    ...expectedBootstrapCommands(profiles[1]!),
  ]);
  expect(await readFile(join(dirname(configPath), "known_hosts"), "utf8")).toBe(trust);
  expect(await readFile(join(dirname(configPath), "id_ed25519"))).toEqual(sharedKey);
  expect(await readFile(join(home, ".ssh", "config"))).toEqual(userConfig);
  expect(await Promise.all(keys.map((key) => readFile(key)))).toEqual(originalKeys);
  for (const [index, profile] of profiles.entries()) await expectBootstrapAuthentication(profile, keys[index]);
});

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

for (const unsafe of ["shared-key-symlink", "public-key-symlink", "trust-symlink", "trust-world-readable", "mismatched-public", "fake-private-with-valid-public"] as const) {
  test(`shared SSH rejects ${unsafe} without replacing user files`, async () => {
    const profile = await new WizardUI("source").pauseAndResume();
    expect(profile).toBeDefined();
    const ssh = dirname(configPath);
    const key = join(ssh, "id_ed25519");
    const trust = join(ssh, "known_hosts");
    if (unsafe.endsWith("symlink")) {
      const destination = unsafe === "shared-key-symlink" ? key : unsafe === "public-key-symlink" ? key + ".pub" : trust;
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
  expect(await added.pauseAndResume()).toBeDefined();
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
