import { spawn } from "node:child_process";
import { constants, type Stats } from "node:fs";
import { chmod, lstat, mkdir, open, realpath, rename, rm, stat } from "node:fs/promises";
import { createHash, randomUUID } from "node:crypto";
import { dirname, join, posix } from "node:path";
import { isIP } from "node:net";
import { events } from "../events.ts";
import { DEFAULT_ALLOCATION_WALLTIME_SECONDS, loadClusterProfiles, parseClusterProfiles, resolveClusterConfigPath, sharedRoots, type SharedRoot, type ClusterProfile, type Environment } from "./profiles.ts";
import { sshTransport, verifySharedMappings, type SshConnection } from "./client.ts";
import { discoverCluster, normalizeRuntimeSetup, type DiscoveryResult, type DiscoveryTarget } from "./discovery.ts";
import { controlReadiness, defaultRegistrationHost, sharedFilesystemChoices, type IntegrationSettings } from "./setup-integration.ts";

/** Deliberately limited to real extension UI methods; never request secrets in input(). */
export interface SetupUI {
  input(title: string, placeholder?: string): Promise<string | undefined>;
  select(title: string, options: string[]): Promise<string | undefined>;
  editor(title: string, prefill?: string): Promise<string | undefined>;
  confirm(title: string, message: string): Promise<boolean>;
  notify(message: string, type?: "info" | "warning" | "error" | "success"): void;
}
type Connection = { kind: "alias"; alias: string } | { kind: "direct"; hostname: string; user: string; port: number; jump?: string };
class Cancelled extends Error {}
const controls = /[\u0000-\u001f\u007f]/;
const quote = (text: string) => "'" + text.replace(/'/g, "'\\''") + "'";
// OpenSSH expands % tokens in IdentityFile; %% means a literal percent.
const sshPath = (text: string) => '"' + text.replace(/%/g, "%%").replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
const includePath = sshPath;
const identifier = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;
function validPublicKey(text: string | undefined): text is string {
  if (!text || controls.test(text) || !/^ssh-ed25519 [A-Za-z0-9+/]+={0,2}(?: .*)?$/.test(text)) return false;
  const blob = Buffer.from(text.split(" ")[1]!, "base64");
  return blob.length === 51 && blob.readUInt32BE(0) === 11 && blob.subarray(4, 15).toString() === "ssh-ed25519" && blob.readUInt32BE(15) === 32;
}

async function exists(path: string): Promise<boolean> {
  try { await lstat(path); return true; } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return false; throw error; }
}
/** Refuse symlink destinations, including parent directories, before writing secrets. */
async function safeDirectory(path: string, create = true): Promise<void> {
  const parent = dirname(path);
  if (parent !== path) await safeDirectory(parent, create);
  try {
    const info = await lstat(path);
    if (!info.isDirectory() || info.isSymbolicLink()) throw new Error(`Unsafe configuration directory: ${path}`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT" || !create) throw error;
    try { await mkdir(path, { mode: 0o700 }); }
    catch (race) { if ((race as NodeJS.ErrnoException).code !== "EEXIST") throw race; await safeDirectory(path, false); }
  }
}
async function safeRead(path: string): Promise<string | undefined> {
  let file;
  try { file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return undefined; throw error; }
  try {
    if (!(await file.stat()).isFile()) throw new Error(`Not a regular configuration file: ${path}`);
    return await file.readFile("utf8");
  } finally { await file.close(); }
}
async function privateFile(path: string, contents: string | Buffer): Promise<void> {
  const file = await open(path, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try { await file.writeFile(contents); await file.sync(); } finally { await file.close(); }
}

async function privateRead(path: string): Promise<Buffer> {
  await safeDirectory(dirname(path), false);
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const info = await file.stat();
    if (!info.isFile() || info.uid !== process.getuid?.() || (info.mode & 0o077) !== 0) throw new Error(`Expected a user-owned private regular file (0600): ${path}`);
    return await file.readFile();
  } finally { await file.close(); }
}

/** No terminal or agent prompts, including for encrypted private keys. */
function keyOutput(args: string[]): Promise<string | undefined> {
  const { promise, resolve, reject } = Promise.withResolvers<string | undefined>();
  const child = spawn("ssh-keygen", args, { stdio: ["ignore", "pipe", "ignore"] });
  let output = "";
  const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error("SSH key validation timed out")); }, 10_000);
  child.stdout.on("data", (part) => {
    output += part.toString();
    if (output.length > 16_384) child.kill("SIGKILL");
  });
  child.once("error", () => { clearTimeout(timer); reject(new Error("Could not start ssh-keygen; install OpenSSH locally")); });
  child.once("close", (code) => { clearTimeout(timer); resolve(code === 0 && output.length <= 16_384 ? output.trim() : undefined); });
  return promise;
}

async function validateKey(key: string): Promise<{ publicKey: string; encrypted: boolean }> {
  const privateKey = (await privateRead(key)).toString("utf8");
  const publicKey = (await privateRead(key + ".pub")).toString("utf8").trim();
  if (!validPublicKey(publicKey)) throw new Error(`Invalid public key at ${key}.pub; no key contents were published`);
  const derived = await keyOutput(["-y", "-P", "", "-f", key]);
  if (derived !== undefined) {
    if (!validPublicKey(derived) || derived.split(" ").slice(0, 2).join(" ") !== publicKey.split(" ").slice(0, 2).join(" ")) throw new Error(`Private and public key do not match: ${key}`);
    return { publicKey, encrypted: false };
  }
  // Validate the public blob embedded in the encrypted OpenSSH envelope, not
  // `ssh-keygen -l -f private`: that command can fall back to the .pub sidecar.
  // Full encrypted payload integrity is only provable when the user unlocks it.
  const envelope = privateKey.match(/^-----BEGIN OPENSSH PRIVATE KEY-----\r?\n([A-Za-z0-9+/=\r\n]+)-----END OPENSSH PRIVATE KEY-----\r?\n?$/);
  if (!envelope) throw new Error(`Cannot validate encrypted SSH key: ${key}`);
  const bytes = Buffer.from(envelope[1]!.replace(/\s/g, ""), "base64");
  let offset = 15;
  const field = (): Buffer => {
    if (offset + 4 > bytes.length) throw new Error(`Invalid encrypted SSH key envelope: ${key}`);
    const size = bytes.readUInt32BE(offset);
    offset += 4;
    if (size > bytes.length - offset) throw new Error(`Invalid encrypted SSH key envelope: ${key}`);
    const value = bytes.subarray(offset, offset + size);
    offset += size;
    return value;
  };
  if (bytes.subarray(0, 15).toString() !== "openssh-key-v1\0") throw new Error(`Invalid encrypted SSH key envelope: ${key}`);
  const cipher = field().toString();
  const kdf = field().toString();
  const options = field();
  if (cipher === "none" || !cipher || kdf !== "bcrypt" || options.length < 8 || offset + 4 > bytes.length || bytes.readUInt32BE(offset) !== 1) throw new Error(`Cannot validate encrypted SSH key: ${key}`);
  offset += 4;
  const embedded = field();
  const encrypted = field();
  if (offset !== bytes.length || encrypted.length < 16 || !embedded.equals(Buffer.from(publicKey.split(" ")[1]!, "base64"))) throw new Error(`Encrypted private and public key do not match: ${key}`);
  return { publicKey, encrypted: true };
}

function sshLayout(env: Environment): { configPath: string; directory: string; ssh: string; key: string; trust: string; home: string } {
  const configPath = resolveClusterConfigPath(env);
  const home = env.HOME;
  if (configPath.includes("${") || !home || !home.startsWith("/") || controls.test(home) || /[\[\]*?]/.test(home) || home.includes("${")) throw new Error("Managed SSH requires an absolute HOME without glob/environment expansion, and a config path without ${...} references");
  const directory = dirname(configPath);
  const ssh = join(directory, "ssh");
  return { configPath, directory, ssh, key: join(directory, "id_ed25519"), trust: join(directory, "known_hosts"), home };
}

async function withConfigLock<T>(env: Environment, work: () => Promise<T>): Promise<T> {
  const { configPath, directory } = sshLayout(env);
  await safeDirectory(directory);
  const lockPath = configPath + ".lock";
  let lock;
  try { lock = await open(lockPath, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600); }
  catch { throw new Error(`Configuration is locked or lock path is unsafe: ${lockPath}. Retry after the other writer finishes; do not remove an active lock.`); }
  // Different WH_CLUSTER_CONFIG filenames can intentionally share a dirname
  // and therefore one identity. Serialize that shared key/trust too.
  const sharedLockPath = join(directory, ".ssh-setup.lock");
  let sharedLock;
  try {
    try { sharedLock = await open(sharedLockPath, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600); }
    catch { throw new Error(`Shared SSH configuration is locked or unsafe: ${sharedLockPath}. Retry after the other writer finishes.`); }
    return await work();
  } finally {
    if (sharedLock) { await sharedLock.close(); await rm(sharedLockPath); }
    await lock.close();
    await rm(lockPath);
  }
}

async function initializeTrust(env: Environment): Promise<void> {
  const { directory, ssh, trust } = sshLayout(env);
  await safeDirectory(ssh);
  await chmod(directory, 0o700);
  await chmod(ssh, 0o700);
  if (!await exists(trust)) await privateFile(trust, "");
  // Never truncate, replace or import trust. OpenSSH appends verified hosts.
  await privateRead(trust);
}

async function ensureSharedKey(env: Environment, promote?: string): Promise<{ key: string; publicKey: string; encrypted: boolean }> {
  const { directory, key } = sshLayout(env);
  await safeDirectory(directory);
  await chmod(directory, 0o700);
  if (await exists(key)) return { key, ...await validateKey(key) };
  if (await exists(key + ".pub")) throw new Error(`Shared public key exists without its private key: ${key}. Nothing was overwritten.`);
  if (promote) {
    await validateKey(promote);
    // Exclusive copies preserve encrypted bytes and every original key file.
    await privateFile(key, await privateRead(promote));
    await privateFile(key + ".pub", await privateRead(promote + ".pub"));
  } else {
    // Generate off-destination, then publish with O_EXCL: ssh-keygen itself
    // must never be allowed to overwrite an unexpected destination.
    const temporary = join(directory, `.key-${randomUUID()}`);
    try {
      await keygen(temporary);
      await chmod(temporary, 0o600);
      await chmod(temporary + ".pub", 0o600);
      await validateKey(temporary);
      await privateFile(key, await privateRead(temporary));
      await privateFile(key + ".pub", await privateRead(temporary + ".pub"));
    } finally { await rm(temporary, { force: true }); await rm(temporary + ".pub", { force: true }); }
  }
  return { key, ...await validateKey(key) };
}
function keygen(path: string): Promise<void> {
  const { promise, resolve, reject } = Promise.withResolvers<void>();
  // Never put a passphrase, token or private key in logs/model context.
  const child = spawn("ssh-keygen", ["-q", "-t", "ed25519", "-N", "", "-C", "worker-harness dedicated cluster key", "-f", path], { stdio: "ignore" });
  const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error("ssh-keygen timed out")); }, 30_000);
  child.once("error", () => { clearTimeout(timer); reject(new Error("Could not start ssh-keygen; install OpenSSH locally")); });
  child.once("close", (code) => { clearTimeout(timer); code === 0 ? resolve() : reject(new Error("ssh-keygen failed; no profile was saved")); });
  return promise;
}
/** An Include is checked as user configuration, unlike OpenSSH's implicit system defaults. */
export async function inspectSystemSshConfig(
  path = "/etc/ssh/ssh_config",
  inspect: (path: string) => Promise<Pick<Stats, "uid" | "mode" | "isFile">> = stat,
): Promise<{ includePath?: string; warning?: string }> {
  try {
    const resolved = await realpath(path);
    const info = await inspect(resolved);
    if (info.isFile() && (info.uid === 0 || info.uid === process.getuid?.()) && (info.mode & 0o022) === 0) {
      return { includePath: resolved };
    }
  } catch {
    // Missing/unreadable system config is also not a safe explicit Include.
  }
  return {
    warning: `Warning: system SSH defaults from ${path} were not inherited: the resolved file is missing, inaccessible, non-regular, owned by neither root nor the current user, or group/world-writable. Put required site settings in your normal ~/.ssh/config. SSH ownership and permission checks have not been weakened.`,
  };
}

function managedConfig(connection: Connection, alias: string, home: string, systemConfig: string | undefined, key?: string, trust?: string, restrictIdentity = true): string {
  const lines = [`Host ${alias}`, ...(key ? [`  IdentityFile ${sshPath(key)}`, ...(restrictIdentity ? ["  IdentitiesOnly yes"] : [])] : []), ...(trust ? [`  UserKnownHostsFile ${sshPath(trust)}`] : [])];
  if (connection.kind === "direct") {
    lines.push(`  HostName ${connection.hostname}`, `  User ${connection.user}`, `  Port ${connection.port}`);
    if (connection.jump) lines.push(`  ProxyJump ${connection.jump}`);
  }
  // Reset Host context before Include: aliases inside the user's file still match.
  lines.push("Host *", `  Include ${includePath(join(home, ".ssh", "config"))}`);
  if (systemConfig) lines.push("Host *", `  Include ${includePath(systemConfig)}`);
  lines.push("");
  return lines.join("\n");
}

type ManagedConnection = { connection: Connection; systemConfig?: string; key: string; bootstrapKey?: string; config: string; before: string; bootstrap: string };

/** Recognize only exact wizard renderings, not arbitrary operator SSH programs. */
async function readManaged(profile: ClusterProfile, env: Environment): Promise<ManagedConnection | undefined> {
  const { ssh, home, key, trust } = sshLayout(env);
  const config = profile.ssh_config_file;
  if (!config || dirname(dirname(config)) !== ssh || !identifier.test(dirname(config).slice(ssh.length + 1)) || config !== join(dirname(config), "config")) return undefined;
  await safeDirectory(dirname(config), false);
  const before = await safeRead(config);
  const bootstrap = await safeRead(join(dirname(config), "bootstrap_config"));
  if (!before || !bootstrap) return undefined;
  const decode = (token: string): string | undefined => {
    if (!/^"(?:[^"\\]|\\[\\"])*"$/.test(token)) return undefined;
    const value = token.slice(1, -1).replace(/\\([\\"])/g, "$1").replace(/%%/g, "%");
    return sshPath(value) === token ? value : undefined;
  };
  const lines = before.trimEnd().split("\n");
  if (lines.shift() !== `Host ${profile.ssh_host}`) return undefined;
  const identity = lines.shift();
  if (!identity?.startsWith("  IdentityFile ")) return undefined;
  const oldKey = decode(identity.slice("  IdentityFile ".length));
  if (!oldKey || ![key, join(dirname(config), "id_ed25519")].includes(oldKey) || lines.shift() !== "  IdentitiesOnly yes") return undefined;
  const hasTrust = lines[0]?.startsWith("  UserKnownHostsFile ");
  if (hasTrust && lines.shift() !== `  UserKnownHostsFile ${sshPath(trust)}`) return undefined;
  let connection: ManagedConnection["connection"] = { kind: "alias", alias: profile.ssh_host };
  if (lines[0]?.startsWith("  HostName ")) {
    const hostname = lines.shift()!.slice(11);
    const user = lines.shift()?.match(/^  User ([A-Za-z0-9_][A-Za-z0-9_.-]*)$/)?.[1];
    const port = Number(lines.shift()?.match(/^  Port (\d+)$/)?.[1]);
    if (!user || !Number.isInteger(port) || port < 1 || port > 65535 || !(isIP(hostname) || hostname.split(".").every((label) => /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/.test(label)))) return undefined;
    const jump = lines[0]?.startsWith("  ProxyJump ") ? lines.shift()!.slice(12) : undefined;
    if (jump && !/^[A-Za-z0-9_][A-Za-z0-9_.@:[\],-]*$/.test(jump)) return undefined;
    connection = { kind: "direct", hostname, user, port, jump };
  }
  if (lines.shift() !== "Host *" || lines.shift() !== `  Include ${includePath(join(home, ".ssh", "config"))}`) return undefined;
  let systemConfig: string | undefined;
  if (lines.length) {
    if (lines.shift() !== "Host *") return undefined;
    const include = lines.shift();
    systemConfig = include?.startsWith("  Include ") ? decode(include.slice(10)) : undefined;
    if (!systemConfig?.startsWith("/")) return undefined;
  }
  const bootstrapIdentity = bootstrap.split("\n")[1];
  const bootstrapKey = bootstrapIdentity?.startsWith("  IdentityFile ") ? decode(bootstrapIdentity.slice("  IdentityFile ".length)) : undefined;
  const legacyKey = join(dirname(config), "id_ed25519");
  if (bootstrapKey && bootstrapKey !== legacyKey) return undefined;
  if (lines.length || managedConfig(connection, profile.ssh_host, home, systemConfig, oldKey, hasTrust ? trust : undefined) !== before || managedConfig(connection, profile.ssh_host, home, systemConfig, bootstrapKey, hasTrust ? trust : undefined, false) !== bootstrap) return undefined;
  return { connection, systemConfig, key: oldKey, bootstrapKey: bootstrapKey ?? (oldKey === legacyKey ? legacyKey : undefined), config, before, bootstrap };
}

export type SetupPhase = "connection" | "ssh" | "mappings" | "discovery" | "blocked" | "review";
export interface SetupDraft {
  id: string;
  revision: number;
  phase: SetupPhase;
  description: string;
  connection: Connection;
  ssh: SshConnection;
  mappings: SharedRoot[];
  mappings_complete?: boolean;
  runtime_setup?: string;
  sif_path?: string;
  account?: string;
  integration?: IntegrationSettings;
  discovery?: DiscoveryResult;
  selected_targets?: string[];
  advanced?: Partial<Pick<ClusterProfile, "max_walltime_seconds" | "failure_grace_seconds" | "termination_grace_seconds" | "warning_seconds" | "poll_seconds" | "max_cpus" | "max_memory_mb" | "qos" | "constraint" | "orchestrator_host" | "orchestrator_registration_port">>;
  blockers?: string[];
}
export interface SetupOptions {
  transport?: typeof sshTransport;
  discover?: typeof discoverCluster;
  verifyMappings?: typeof verifySharedMappings;
  catalogue?: () => Promise<string[]>;
  readiness?: (signal?: AbortSignal) => Promise<string[]>;
  resolveSsh?: (destination: string, env: Environment) => Promise<{ connection: SetupDraft["connection"]; alias: string }>;
  signal?: AbortSignal;
  replaceConfig?: typeof rename;
}

/** Local config evaluation only: no connection, trust import or authentication. */
export async function resolveSetupSsh(destination: string, _env: Environment): Promise<{ connection: SetupDraft["connection"]; alias: string }> {
  if (!/^(?:[A-Za-z0-9_][A-Za-z0-9_.-]*@)?[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(destination)) throw new Error("Use an SSH alias or user@hostname; put ports and jump hosts in normal SSH configuration.");
  // Match the operator's ordinary terminal: -F would suppress system defaults,
  // and HOME does not redirect OpenSSH's passwd-based user-config lookup.
  const args = ["-G", destination];
  const output = await new Promise<string>((resolve, reject) => {
    const child = spawn("ssh", args, { stdio: ["ignore", "pipe", "ignore"] });
    let text = "";
    const timer = setTimeout(() => { child.kill("SIGKILL"); reject(new Error("Local ssh -G evaluation timed out")); }, 10_000);
    child.stdout.on("data", (part) => { text += part.toString(); if (text.length > 262_144) child.kill("SIGKILL"); });
    child.once("error", () => { clearTimeout(timer); reject(new Error("Could not evaluate local SSH settings; install OpenSSH")); });
    child.once("close", (code) => { clearTimeout(timer); code === 0 && text.length <= 262_144 ? resolve(text) : reject(new Error("Local ssh -G failed; correct your normal SSH configuration")); });
  });
  const settings: Record<string, string> = {};
  for (const line of output.split("\n")) {
    const space = line.indexOf(" ");
    if (space > 0 && settings[line.slice(0, space)] === undefined) settings[line.slice(0, space)] = line.slice(space + 1);
  }
  if (!settings.hostname || !settings.user || !/^\d+$/.test(settings.port ?? "")) throw new Error("ssh -G did not resolve a hostname, user and port");
  if (!destination.includes("@")) return { connection: { kind: "alias", alias: destination }, alias: destination };
  const hostname = settings.hostname;
  const user = settings.user;
  const port = Number(settings.port);
  const jump = settings.proxyjump === "none" ? undefined : settings.proxyjump;
  if ((!isIP(hostname) && !/^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(hostname)) || !/^[A-Za-z0-9_][A-Za-z0-9_.-]*$/.test(user) || port < 1 || port > 65535 || (jump && !/^[A-Za-z0-9_][A-Za-z0-9_.@:[\],-]*$/.test(jump))) throw new Error("Resolved SSH destination is not safe for a managed config; use a normal SSH alias instead");
  // Keep the original Host match for normal alias/hostname options even when
  // the operator supplied an explicit user; only resolved connection fields
  // precede the normal config Include.
  return { connection: { kind: "direct", hostname, user, port, ...(jump ? { jump } : {}) }, alias: destination.split("@").at(-1)! };
}

async function readDrafts(env: Environment): Promise<SetupDraft[]> {
  const path = join(sshLayout(env).directory, "setup-drafts.json");
  const raw = await safeRead(path);
  if (raw === undefined) return [];
  const document = JSON.parse(raw);
  if (document.version !== 1 || !Array.isArray(document.drafts) || document.drafts.some((draft: SetupDraft) => !identifier.test(draft.id) || !Number.isSafeInteger(draft.revision) || !draft.ssh || !draft.connection || !Array.isArray(draft.mappings))) throw new Error(`Invalid private setup drafts: ${path}`);
  await privateRead(path);
  return document.drafts;
}

async function saveDraft(draft: SetupDraft, env: Environment): Promise<void> {
  await withConfigLock(env, async () => {
    const drafts = await readDrafts(env);
    const index = drafts.findIndex((entry) => entry.id === draft.id);
    if ((index < 0 ? 0 : drafts[index]!.revision) !== draft.revision) throw new Error("This setup draft changed in another invocation; resume the newest saved version");
    const next = { ...draft, revision: draft.revision + 1 };
    if (index < 0) drafts.push(next); else drafts[index] = next;
    await replacePrivateJson(join(sshLayout(env).directory, "setup-drafts.json"), { version: 1, drafts });
    draft.revision = next.revision;
  });
}

async function replacePrivateJson(path: string, value: unknown): Promise<void> {
  const temporary = join(dirname(path), `.setup-${randomUUID()}.tmp`);
  try {
    await privateFile(temporary, JSON.stringify(value, null, 2) + "\n");
    await rename(temporary, path);
  } finally { await rm(temporary, { force: true }); }
}

async function prepareConnection(draft: SetupDraft, env: Environment): Promise<{ key: string; publicKey: string; encrypted: boolean; warning?: string }> {
  return withConfigLock(env, async () => {
    const { ssh, key, trust, home } = sshLayout(env);
    const profiles = await loadClusterProfiles(env);
    if (!await exists(key)) {
      for (const profile of profiles) if (await readManaged(profile, env)) throw new Error("Existing managed keys require /cluster-ssh-migrate first; no new key was generated");
    }
    await initializeTrust(env);
    const result = await ensureSharedKey(env);
    const target = join(ssh, draft.id);
    if (await exists(target)) await safeDirectory(target, false);
    const system = await inspectSystemSshConfig();
    const config = managedConfig(draft.connection, draft.ssh.ssh_host, home, system.includePath, key, trust);
    const bootstrap = managedConfig(draft.connection, draft.ssh.ssh_host, home, system.includePath, undefined, trust);
    const existingConfig = await safeRead(join(target, "config"));
    const existingBootstrap = await safeRead(join(target, "bootstrap_config"));
    if (existingConfig !== undefined || existingBootstrap !== undefined) {
      if (existingConfig !== config || existingBootstrap !== bootstrap) throw new Error("Saved managed SSH configuration changed; no file was overwritten");
    } else {
      if (await exists(target)) throw new Error(`SSH destination already exists: ${target}; no file was overwritten`);
      await mkdir(target, { mode: 0o700 });
      try {
        await privateFile(join(target, "config"), config);
        await privateFile(join(target, "bootstrap_config"), bootstrap);
      } catch (error) { await rm(target, { recursive: true, force: true }); throw error; }
    }
    return { ...result, warning: system.warning };
  });
}

/** Deterministic target identity includes the exact scheduler tokens, not a lossy slug. */
export function discoveredTargetId(cluster: string, target: DiscoveryTarget): string {
  const identity = `${target.partition}\0${target.compute_kind}\0${target.gpu_type ?? ""}`;
  const suffix = createHash("sha256").update(identity).digest("hex").slice(0, 12);
  const label = `${target.partition}-${target.compute_kind}${target.gpu_type ? `-${target.gpu_type}` : ""}`.replace(/[^A-Za-z0-9_-]/g, "-");
  return `${cluster}-${label}-${suffix}`;
}

export function buildDiscoveredProfiles(draft: SetupDraft, result: DiscoveryResult): ClusterProfile[] {
  if (!result.runtime || !result.sif_path || !draft.integration || !draft.mappings.length || result.unresolved.some((issue) => issue.blocking)) throw new Error("Setup prerequisites remain unresolved; no profiles can be registered");
  const primary = draft.mappings[0]!;
  const profiles = result.targets.filter((target) => !draft.selected_targets || draft.selected_targets.includes(discoveredTargetId(draft.id, target))).map((target) => ({
    id: discoveredTargetId(draft.id, target),
    description: draft.description || draft.id,
    ...draft.ssh,
    ...primary,
    ...(draft.mappings.length > 1 ? { additional_shared_roots: draft.mappings.slice(1) } : {}),
    runtime: result.runtime!.executable,
    runtime_setup: result.runtime!.prelude,
    sif_path: result.sif_path,
    ...(result.account ? { account: result.account } : {}),
    ...draft.integration,
    failure_grace_seconds: 0,
    termination_grace_seconds: 20,
    warning_seconds: 120,
    poll_seconds: 15,
    ...draft.advanced,
    partition: target.partition,
    compute_kind: target.compute_kind,
    ...(target.compute_kind === "gpu" ? { gpu_type: target.gpu_type, max_gpus: target.max_gpus } : { max_gpus: 0 }),
    max_walltime_seconds: Math.min(draft.advanced?.max_walltime_seconds ?? DEFAULT_ALLOCATION_WALLTIME_SECONDS, target.max_walltime_seconds),
  }));
  if (!profiles.length) throw new Error("Select at least one discovered target");
  return parseClusterProfiles({ version: 1, clusters: profiles });
}

/** The only completed-profile write: validate and commit the entire set or none. */
export async function commitSetupProfiles(draft: SetupDraft, profiles: ClusterProfile[], env: Environment, replaceConfig: typeof rename = rename): Promise<{ draftRetained: boolean }> {
  return withConfigLock(env, async () => {
    const { configPath, directory } = sshLayout(env);
    const before = await safeRead(configPath);
    const document = before === undefined ? { version: 1, clusters: [] } : JSON.parse(before);
    parseClusterProfiles(document, configPath);
    parseClusterProfiles({ version: 1, clusters: [...document.clusters, ...profiles] }, configPath);
    const drafts = await readDrafts(env);
    if (drafts.find((entry) => entry.id === draft.id)?.revision !== draft.revision) throw new Error("Setup draft changed concurrently; resume before registering");
    const temporary = join(directory, `.clusters-${randomUUID()}.tmp`);
    try {
      await privateFile(temporary, JSON.stringify({ version: 1, clusters: [...document.clusters, ...profiles] }, null, 2) + "\n");
      if (await safeRead(configPath) !== before) throw new Error("Cluster configuration changed concurrently; no targets registered");
      await replaceConfig(temporary, configPath);
    } finally { await rm(temporary, { force: true }); }
    // A completed draft left by an interrupted cleanup is harmless: it is never
    // a target, and a repeated commit rejects the existing target IDs.
    try {
      await replacePrivateJson(join(directory, "setup-drafts.json"), { version: 1, drafts: drafts.filter((entry) => entry.id !== draft.id) });
      return { draftRetained: false };
    } catch { return { draftRetained: true }; }
  });
}

/** SSH-first setup. Explicit consent is the boundary for retained local state. */
export async function runClusterAdd(ui: SetupUI, publish: (content: string) => void | Promise<void>, args = "", env: Environment = process.env, options: SetupOptions = {}): Promise<ClusterProfile | undefined> {
  let draft: SetupDraft | undefined;
  let consented = false;
  const transport = options.transport ?? sshTransport;
  const signal = options.signal;
  const choose = async (title: string, choices: string[]): Promise<string> => {
    signal?.throwIfAborted();
    const value = await ui.select(title, choices);
    if (value === undefined) throw new Cancelled();
    return value;
  };
  const input = async (title: string, current = "", validate?: (value: string) => boolean, allowEmpty = false): Promise<string> => {
    for (;;) {
      signal?.throwIfAborted();
      const value = await ui.input(title, current || undefined);
      if (value === undefined) throw new Cancelled();
      const answer = value === "" ? current : value.trim();
      if ((!answer && !allowEmpty) || controls.test(answer) || (validate && !validate(answer))) { ui.notify("Invalid value; correct this field or Escape to pause.", "error"); continue; }
      return answer;
    }
  };
  try {
    const { configPath, key, directory } = sshLayout(env);
    // Reject a symlink/non-regular config before any prompts or credential
    // creation; the profile loader intentionally has broader read semantics.
    await safeRead(configPath);
    const existing = await loadClusterProfiles(env);
    const drafts = await readDrafts(env);
    const requested = args.trim();
    draft = requested ? drafts.find((entry) => entry.id === requested) : undefined;
    if (!requested && drafts.length) {
      const choice = await choose("Resume cluster setup?", [...drafts.map((entry) => `Resume ${entry.id} (${entry.phase})`), "New cluster"]);
      draft = drafts.find((entry) => choice === `Resume ${entry.id} (${entry.phase})`);
    }
    if (!draft) {
      const destination = await input("SSH destination (alias or user@host)");
      const resolved = await (options.resolveSsh ?? resolveSetupSsh)(destination, env);
      const suggestion = requested || destination.split("@").at(-1)!.split(".")[0]!.replace(/[^A-Za-z0-9_-]/g, "-");
      const id = await input("Cluster name", suggestion, (value) => identifier.test(value) && !drafts.some((entry) => entry.id === value) && !existing.some((entry) => entry.id === value || entry.id.startsWith(`${value}-`)));
      const description = await input("Description (optional)", "", undefined, true);
      const alias = resolved.alias || `wh-${id}`;
      draft = { id, revision: 0, phase: "connection", description, connection: resolved.connection, ssh: { ssh_host: alias, ssh_config_file: join(directory, "ssh", id, "config") }, mappings: [] };
      // Reject unsafe or legacy credentials before any consented writes.
      if (await exists(key)) await validateKey(key);
      else {
        if (await exists(key + ".pub")) throw new Error("Shared public key exists without its private key; nothing was overwritten");
        for (const entry of existing) if (await readManaged(entry, env)) throw new Error("Existing managed keys require /cluster-ssh-migrate first; no new key was generated");
      }
      if (await exists(sshLayout(env).trust)) await privateRead(sshLayout(env).trust);
      if (await exists(join(directory, "ssh", id))) throw new Error("SSH destination already exists; no credential or draft was written");
      const consent = `${await exists(key) ? `Reuse the shared key ${key}; never replace it.` : `Generate ONE shared, initially UNENCRYPTED Ed25519 key at ${key}. Protect it with ssh-keygen -p in your own terminal if desired.`} Create a private resumable setup and managed SSH config? Verified shared known_hosts is preserved; missing trust starts empty. No credentials, trust, environment files or secrets will be copied from another machine. After strict SSH succeeds, verify each mapping using one temporary shared-filesystem probe through an existing WH worker, with cleanup on every outcome. No Slurm allocation, deployment or remote credential installation will be performed.`;
      if (!await ui.confirm("Create shared SSH connection and save resumable setup?", consent)) throw new Cancelled();
      consented = true;
      await saveDraft(draft, env);
    } else consented = true;
    const credential = await prepareConnection(draft, env);
    if (credential.warning) ui.notify(credential.warning, "warning");
    await publish([`Cluster setup ${draft.id}: saved resumable connection; no target registered.`, ...(credential.warning ? [credential.warning] : []), ...sshInstructions([{ id: draft.id, ...draft.ssh }], credential, env)].join("\n\n"));
    draft.phase = "ssh";
    await saveDraft(draft, env);
    for (;;) {
      const action = await choose("Authorize the public key and verify the host fingerprint in your terminal", ["Key installed — test connection", "Save and exit"]);
      if (action === "Save and exit") throw new Cancelled();
      try {
        await transport(draft.ssh, "true", undefined, { timeout_ms: 15_000, signal });
        break;
      } catch (error) {
        if (signal?.aborted) throw error;
        ui.notify(`Strict SSH test failed: ${error instanceof Error ? error.message : "connection unavailable"}. Key and draft retained; no Slurm discovery has run.`, "warning");
      }
    }
    const collectMapping = async (): Promise<void> => {
      let choices: string[] = [];
      try { choices = await (options.catalogue ?? sharedFilesystemChoices)(); }
      catch { ui.notify("Shared filesystem catalogue unavailable; enter /data/shared/<id> explicitly.", "warning"); }
      choices = choices.filter((path) => /^\/data\/shared\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(path) && !draft!.mappings.some((mapping) => path === `/data/shared/${mapping.storage_id}`));
      const selected = choices.length ? await choose("WH shared filesystem", [...choices, "Enter /data/shared/<id>"]) : "Enter /data/shared/<id>";
      const path = selected === "Enter /data/shared/<id>" ? await input("WH shared path (/data/shared/<id>)", "", (value) => /^\/data\/shared\/[A-Za-z0-9][A-Za-z0-9_-]*$/.test(value) && !draft!.mappings.some((mapping) => value === `/data/shared/${mapping.storage_id}`)) : selected;
      const root = await input("Same filesystem root on cluster (absolute path)", "", (value) => value.startsWith("/") && posix.normalize(value) !== "/" && !value.split("/").some((part) => part === "." || part === "..") && !/[:,;]/.test(value));
      draft!.mappings.push({ storage_id: path.slice("/data/shared/".length), shared_root: posix.normalize(root).replace(/\/$/, "") });
      await saveDraft(draft!, env);
    };
    if (draft.blockers?.some((message) => message.startsWith("Filesystem identity verification failed:"))) {
      const action = await choose("Saved filesystem verification is unresolved", ["Retry saved mappings", "Edit filesystem mappings", "Save and exit"]);
      if (action === "Save and exit") throw new Cancelled();
      if (action === "Edit filesystem mappings") {
        draft.mappings = [];
        draft.mappings_complete = false;
        await saveDraft(draft, env);
        await collectMapping();
      }
    }
    if (!draft.mappings.length) {
      // An unambiguous managed connection may reuse its stable storage/runtime.
      const sources: ClusterProfile[] = [];
      for (const entry of existing) {
        if (entry.ssh_host !== draft.ssh.ssh_host) continue;
        const managed = await readManaged(entry, env);
        if (managed && JSON.stringify(managed.connection) === JSON.stringify(draft.connection)) sources.push(entry);
      }
      const source = sources[0];
      if (source && sources.every((entry) => JSON.stringify(sharedRoots(entry)) === JSON.stringify(sharedRoots(source)))) {
        draft.mappings = sharedRoots(source);
        draft.sif_path = source.sif_path;
        try { draft.runtime_setup = normalizeRuntimeSetup(source.runtime_setup); }
        catch { ui.notify("Existing runtime prelude is left unchanged in the old profile; automatic discovery will use only safe literal module loads.", "info"); }
        await saveDraft(draft, env);
      } else await collectMapping();
    }
    if (!draft.mappings_complete) {
      while (await choose("Add another filesystem mapping?", ["No", "Yes"]) === "Yes") await collectMapping();
      draft.mappings_complete = true;
      await saveDraft(draft, env);
    }
    draft.phase = "mappings";
    await saveDraft(draft, env);
    ui.notify("Verifying shared filesystem identities using temporary probes and cleanup; no Slurm allocation.", "info");
    try {
      await (options.verifyMappings ?? verifySharedMappings)(draft.ssh, draft.mappings, { transport, signal });
    } catch (error) {
      if (signal?.aborted) throw error;
      draft.phase = "blocked";
      draft.blockers = [`Filesystem identity verification failed: ${error instanceof Error ? error.message : "participating worker or SSH probe unavailable"}`];
      await saveDraft(draft, env);
      await publish(`Setup ${draft.id} blocked and saved for resume:\n${draft.blockers.join("\n")}\nNo mapping was assumed verified; no target registered. Correct the WH/cluster filesystem mapping, then /cluster-add ${draft.id}.`);
      return undefined;
    }
    draft.phase = "discovery";
    await saveDraft(draft, env);
    let result: DiscoveryResult;
    for (;;) {
      ui.notify("Discovering Slurm CPU/GPU targets and compatible runtime/image (read-only; no allocation).", "info");
      result = await (options.discover ?? discoverCluster)(draft.ssh, { mappings: draft.mappings, ...(draft.sif_path ? { sif_path: draft.sif_path } : {}), ...(draft.runtime_setup !== undefined ? { runtime_setup: draft.runtime_setup } : {}), ...(draft.account ? { account: draft.account } : {}) }, { transport, signal });
      draft.discovery = result;
      await saveDraft(draft, env);
      const unresolved = result.unresolved.filter((issue) => issue.blocking);
      const fixable = unresolved.some((issue) => issue.kind === "runtime" || issue.kind === "image");
      if (!unresolved.length) break;
      draft.phase = "blocked";
      draft.blockers = unresolved.map((issue) => issue.message);
      await saveDraft(draft, env);
      await publish(`Setup ${draft.id} blocked and saved for resume:\n${draft.blockers.join("\n")}\nNo target registered. Provision the missing prerequisites, then /cluster-add ${draft.id}.`);
      const action = await choose("Discovery prerequisites unresolved", [...(fixable ? ["Provide missing runtime / image"] : []), "Retry discovery", "Save and exit"]);
      if (action === "Save and exit") throw new Cancelled();
      if (action === "Provide missing runtime / image") {
        if (unresolved.some((issue) => issue.kind === "runtime")) {
          for (;;) {
            const prelude = await ui.editor("Trusted runtime shell prelude (safe module load / discovered PATH only; never paste secrets)", draft.runtime_setup ?? "");
            if (prelude === undefined) throw new Cancelled();
            try { draft.runtime_setup = normalizeRuntimeSetup(prelude); break; }
            catch (error) { ui.notify(error instanceof Error ? error.message : "Unsupported runtime prelude", "error"); }
          }
        }
        if (unresolved.some((issue) => issue.kind === "image")) draft.sif_path = posix.normalize(await input("Compatible Worker SIF on cluster (absolute path)", draft.sif_path, (value) => value.startsWith("/") && posix.normalize(value) !== "/" && !value.split("/").some((part) => part === "." || part === "..") && !/[:,;]/.test(value)));
        await saveDraft(draft, env);
      }
    }
    if (result.unresolved.some((issue) => issue.kind === "billing") && !draft.account) {
      const selection = await choose("Billing default unresolved (scheduler may choose its default)", ["Use scheduler default", ...result.accounts.map((account) => `Account ${account}`)]);
      if (selection !== "Use scheduler default") {
        draft.account = selection.slice("Account ".length);
        result = await (options.discover ?? discoverCluster)(draft.ssh, { mappings: draft.mappings, sif_path: result.sif_path, runtime_setup: result.runtime?.prelude, account: draft.account }, { transport, signal });
        draft.discovery = result;
        await saveDraft(draft, env);
      }
    }
    const hadSavedSelection = draft.selected_targets !== undefined;
    const availableTargetIds = new Set(result.targets.map((target) => discoveredTargetId(draft!.id, target)));
    if (draft.selected_targets) draft.selected_targets = draft.selected_targets.filter((id) => availableTargetIds.has(id));
    draft.selected_targets ??= result.targets.filter((target) => !target.review_required).map((target) => discoveredTargetId(draft!.id, target));
    if (!draft.selected_targets.length) {
      draft.phase = "blocked";
      draft.blockers = [hadSavedSelection ? "Saved targets are no longer available; select from fresh discovery." : "No ordinary partition candidate found; visible candidates require explicit site-policy review."];
      await saveDraft(draft, env);
      const candidates = result.targets.map((target) => `${target.partition} ${target.compute_kind}${target.gpu_type ? ` ${target.gpu_type}` : ""}`);
      if (!candidates.length) throw new Error("No supported discovered targets; correct scheduler/inventory prerequisites and resume");
      const selected = await choose(hadSavedSelection ? "Saved targets are no longer available — select a discovered target" : "All visible targets require explicit site-policy review (no permission guarantee)", [...candidates, "Save and exit"]);
      if (selected === "Save and exit") throw new Cancelled();
      draft.selected_targets = [discoveredTargetId(draft.id, result.targets[candidates.indexOf(selected)]!)];
    }
    await saveDraft(draft, env);
    const settingsPath = join(directory, "cluster-integration.json");
    const savedSettings = await safeRead(settingsPath);
    if (!draft.integration && savedSettings !== undefined) {
      await privateRead(settingsPath);
      const settings = JSON.parse(savedSettings);
      if (settings.version !== 1 || !settings.integration) throw new Error("Invalid global cluster integration settings");
      draft.integration = settings.integration;
    }
    if (!draft.integration && existing.length) {
      const settings = existing[0]!;
      if (existing.every((entry) => entry.tailnet_key_file === settings.tailnet_key_file && entry.tailnet_login_server === settings.tailnet_login_server && entry.orchestrator_host === settings.orchestrator_host && entry.orchestrator_registration_port === settings.orchestrator_registration_port)) {
        draft.integration = { tailnet_key_file: settings.tailnet_key_file, tailnet_login_server: settings.tailnet_login_server, orchestrator_host: settings.orchestrator_host, orchestrator_registration_port: settings.orchestrator_registration_port };
      }
    }
    if (!draft.integration) {
      draft.integration = {
        tailnet_key_file: await input("Local Tailnet enrollment KEY FILE (absolute path; not the key value)", env.WH_TAILNET_KEY_FILE ?? "", (value) => value.startsWith("/")),
        tailnet_login_server: await input("Tailnet login server URL", env.WH_TAILNET_LOGIN_SERVER ?? "", (value) => { try { const url = new URL(value); return ["http:", "https:"].includes(url.protocol) && !url.username && !url.password && !url.search && !url.hash; } catch { return false; } }),
        orchestrator_host: defaultRegistrationHost(),
        orchestrator_registration_port: 12888,
      };
    }
    if (draft.blockers?.some((message) => message.startsWith("Tailnet key FILE"))) {
      let validFile = false;
      try {
        const file = await lstat(draft.integration.tailnet_key_file);
        validFile = file.isFile() && !file.isSymbolicLink() && file.uid === process.getuid?.() && (file.mode & 0o077) === 0;
      } catch { /* Missing local key remains a resumable prerequisite. */ }
      if (!validFile) draft.integration.tailnet_key_file = await input("Local Tailnet enrollment KEY FILE (absolute path; not the key value)", draft.integration.tailnet_key_file, (value) => value.startsWith("/"));
    }
    await saveDraft(draft, env);
    // Record paths only. Never read, copy, publish or persist the enrollment key.
    const blockers = await (options.readiness ?? controlReadiness)(signal);
    try {
      const file = await lstat(draft.integration.tailnet_key_file);
      if (!file.isFile() || file.isSymbolicLink() || file.uid !== process.getuid?.() || (file.mode & 0o077) !== 0) blockers.push("Tailnet key FILE must be a user-owned private regular file (0600).");
    } catch { blockers.push("Tailnet key FILE is missing or inaccessible; provision it locally, then resume."); }
    if (blockers.length) {
      draft.phase = "blocked";
      draft.blockers = blockers;
      await saveDraft(draft, env);
      await publish(`Setup ${draft.id} blocked and resumable:\n${blockers.join("\n")}\nConnection/key retained. No target registered; no allocation submitted. Resume: /cluster-add ${draft.id}`);
      return undefined;
    }
    await withConfigLock(env, async () => {
      const current = await safeRead(settingsPath);
      if (current !== savedSettings) throw new Error("Global integration settings changed concurrently; resume to reuse the current settings");
      if (current === undefined) await replacePrivateJson(settingsPath, { version: 1, integration: draft!.integration });
    });
    draft.phase = "review";
    draft.blockers = [];
    await saveDraft(draft, env);
    let profiles: ClusterProfile[];
    for (;;) {
      profiles = buildDiscoveredProfiles(draft, result);
      const integration = draft.integration!;
      const review = [
        `Review cluster ${draft.id}${draft.description ? ` — ${draft.description}` : ""}`,
        `SSH: ${draft.ssh.ssh_host} (strict test passed)`,
        ...draft.mappings.map((mapping, index) => `${index ? "Additional" : "Primary"} filesystem: /data/shared/${mapping.storage_id} ↔ ${mapping.shared_root}`),
        `Runtime: ${result.runtime?.executable}; image: ${result.sif_path}`,
        `Billing: ${result.account ?? "scheduler default"}; enrollment key FILE: ${integration.tailnet_key_file}`,
        `Registration: ${draft.advanced?.orchestrator_host ?? integration.orchestrator_host}:${draft.advanced?.orchestrator_registration_port ?? integration.orchestrator_registration_port}`,
        ...profiles.map((profile) => `cluster:${profile.id}: ${profile.partition} ${profile.compute_kind}${profile.compute_kind === "gpu" ? ` ${profile.gpu_type} × up to ${profile.max_gpus}` : ""}; allocation ceiling ${profile.max_walltime_seconds}s (job duration remains explicit)`),
        ...result.targets.filter((target) => target.review_required).map((target) => `Site-policy review required: ${discoveredTargetId(draft!.id, target)} (${target.partition}); excluded by default unless explicitly selected in Edit review.`),
        "Defaults: failure grace 0s, termination 20s, warning 120s, poll 15s. Permissions and filesystem identity are verified again at allocation; discovery is not permission or enrollment proof.",
      ].join("\n");
      const action = await choose(review, ["Register targets", "Edit review", "Advanced", "Save and exit"]);
      if (action === "Save and exit") throw new Cancelled();
      if (action === "Register targets") break;
      try {
        if (action === "Advanced") {
          const edited = await ui.editor("Advanced allocation settings (JSON; failure grace holds idle resources and may violate site policy)", JSON.stringify(draft.advanced ?? { max_walltime_seconds: DEFAULT_ALLOCATION_WALLTIME_SECONDS, failure_grace_seconds: 0, termination_grace_seconds: 20, warning_seconds: 120, poll_seconds: 15, orchestrator_host: integration.orchestrator_host, orchestrator_registration_port: integration.orchestrator_registration_port }, null, 2));
          if (edited === undefined) throw new Cancelled();
          const advanced = JSON.parse(edited);
          const allowed: Record<string, true> = { max_walltime_seconds: true, failure_grace_seconds: true, termination_grace_seconds: true, warning_seconds: true, poll_seconds: true, max_cpus: true, max_memory_mb: true, qos: true, constraint: true, orchestrator_host: true, orchestrator_registration_port: true };
          if (!advanced || Array.isArray(advanced) || Object.keys(advanced).some((field) => !allowed[field])) throw new Error("Unknown advanced allocation setting");
          buildDiscoveredProfiles({ ...draft, advanced }, result);
          if (advanced.failure_grace_seconds > 0 && !await ui.confirm("Opt in to failure diagnosis idle holding?", "Nonzero failure grace holds idle allocations for diagnosis and may violate your site's utilization policy. Use 0 unless explicitly permitted. Apply this advanced override?")) continue;
          draft.advanced = advanced;
        } else {
          const editable = { description: draft.description, mappings: draft.mappings, selected_targets: draft.selected_targets };
          const edited = await ui.editor("Edit cluster review (JSON; mappings are reverified; remove target IDs to exclude partitions/types)", JSON.stringify(editable, null, 2));
          if (edited === undefined) throw new Cancelled();
          const update = JSON.parse(edited);
          if (typeof update.description !== "string" || controls.test(update.description) || !Array.isArray(update.selected_targets) || update.selected_targets.some((id: unknown) => typeof id !== "string" || !result.targets.some((target) => discoveredTargetId(draft!.id, target) === id))) throw new Error("Invalid edited cluster review");
          const candidate: SetupDraft = { ...draft, description: update.description, mappings: update.mappings ?? draft.mappings, selected_targets: update.selected_targets };
          buildDiscoveredProfiles(candidate, result);
          if (JSON.stringify(candidate.mappings) !== JSON.stringify(draft.mappings)) {
            await (options.verifyMappings ?? verifySharedMappings)(draft.ssh, candidate.mappings, { transport, signal });
            const discovered = await (options.discover ?? discoverCluster)(draft.ssh, { mappings: candidate.mappings, sif_path: result.sif_path, runtime_setup: result.runtime?.prelude, ...(draft.account ? { account: draft.account } : {}) }, { transport, signal });
            buildDiscoveredProfiles(candidate, discovered);
            result = discovered;
            candidate.discovery = result;
          }
          draft = candidate;
        }
        await saveDraft(draft, env);
      } catch (error) {
        if (error instanceof Cancelled || signal?.aborted) throw error;
        ui.notify(`Review edit not applied: ${error instanceof Error ? error.message : "invalid value"}. Correct the review or save and exit.`, "error");
      }
    }
    if (!await ui.confirm("Register discovered cluster targets?", "Atomically register the reviewed targets? No allocation, deployment, key installation or enrollment will be performed.")) throw new Cancelled();
    const committed = await commitSetupProfiles(draft, profiles, env, options.replaceConfig);
    await publish(`Cluster ${draft.id} registered: ${profiles.map((profile) => `cluster:${profile.id}`).join(", ")}.\nProfiles: ${configPath}\nNo allocation submitted. Per-job duration is still explicit; the allocation ceiling defaults to seven days or the lower partition limit.${committed.draftRetained ? "\nCompleted draft cleanup failed; targets are registered atomically. The retained private draft is not another target and cannot overwrite these IDs." : ""}`);
    events.emit("worker-harness:refresh", undefined);
    return profiles[0];
  } catch (error) {
    if (error instanceof Cancelled || signal?.aborted) {
      const retained = consented && draft && draft.revision > 0;
      const hasKey = retained && await exists(sshLayout(env).key);
      ui.notify(retained ? `Setup paused at ${draft!.phase}; ${hasKey ? "shared SSH key, managed connection and " : ""}private draft retained. No target registered. Resume: /cluster-add ${draft!.id}` : "Cluster setup cancelled before consent; no credentials or profile were created.", "info");
      return undefined;
    }
    ui.notify(`${error instanceof Error ? error.message : "Cluster setup failed"}${consented && draft && draft.revision > 0 ? ` Setup retained at ${draft.phase}; resume /cluster-add ${draft.id}. No incomplete target registered.` : ""}`, "error");
    return undefined;
  }
}

function sshInstructions(profiles: Pick<ClusterProfile, "id" | "ssh_host" | "ssh_config_file">[], result: { key: string; publicKey: string; encrypted: boolean }, env: Environment): string[] {
  const { trust } = sshLayout(env);
  return [
    `Shared SSH key: ${result.key}${result.encrypted ? " (encrypted; original bytes preserved)" : " (unencrypted)"}`,
    `Shared verified host trust: ${trust}. Existing contents are never replaced; a missing file starts empty.`,
    "No host trust was imported automatically. In your own terminal first connect and verify each host fingerprint against a trusted site source. Alternatively manually copy only independently verified known_hosts entries into this file (keep mode 0600). Never use ssh-keyscan output as automatic trust.",
    "The shared public key must be authorized on EACH distinct login host, especially hosts that previously used another per-profile key. No remote authorized_keys or host trust changes were performed.",
    ...profiles.flatMap((profile) => {
      const config = profile.ssh_config_file!;
      const bootstrap = join(dirname(config), "bootstrap_config");
      return [
        `${profile.id}: SSH config: ${config}\nBootstrap SSH config (ordinary original authentication): ${bootstrap}`,
        `ssh -F ${quote(bootstrap)} ${quote(profile.ssh_host)}`,
        "Install the shared public key yourself (original keys / agent / password / keyboard-interactive authentication remain available through bootstrap; never auto-executed):",
        `ssh-copy-id -F ${quote(bootstrap)} -i ${quote(result.key + ".pub")} ${quote(profile.ssh_host)}`,
        "After installation and fingerprint verification, verify strict noninteractive access:",
        `ssh -F ${quote(config)} -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 ${quote(profile.ssh_host)} 'true'`,
      ];
    }),
    "If ssh-copy-id is unavailable, use the first-connect command, then paste only the PUBLIC key below into ~/.ssh/authorized_keys on that login host (mkdir -p ~/.ssh; chmod 700 ~/.ssh; touch ~/.ssh/authorized_keys; chmod 600 ~/.ssh/authorized_keys). Never copy the private key.",
    "Public key:", result.publicKey,
    ...(result.encrypted ? [] : ["Protect the shared key with a passphrase in your own terminal:", `ssh-keygen -p -f ${quote(result.key)}`]),
    `ssh-add ${quote(result.key)}`,
    "Encrypted keys require a running ssh-agent accessible to the process doing BatchMode SSH. agent-sandbox deliberately unsets SSH_AUTH_SOCK: loading a key into the host agent alone does not make it accessible inside the sandbox. Use an explicitly authorized dedicated-agent arrangement or a protected unencrypted managed key according to your policy; the extension does not change sandbox agent policy.",
  ];
}

/** Explicit local cutover only: never connect, authorize keys, or import trust. */
export async function runClusterSshMigrate(ui: SetupUI, publish: (content: string) => void | Promise<void>, env: Environment = process.env, replaceConfig: typeof rename = rename): Promise<string[] | undefined> {
  try {
    const { configPath, key, home, trust, directory } = sshLayout(env);
    await safeDirectory(directory, false);
    const original = await safeRead(configPath);
    const profiles = await loadClusterProfiles(env);
    const recognized: { profile: ClusterProfile; managed: ManagedConnection }[] = [];
    const skipped: string[] = [];
    for (const profile of profiles) {
      const managed = await readManaged(profile, env);
      if (managed) recognized.push({ profile, managed });
      else skipped.push(profile.id);
    }
    if (!recognized.length) {
      ui.notify(`No recognized wizard SSH configs to migrate. External or manually modified profiles were left unchanged: ${skipped.join(", ") || "(none)"}.`, "warning");
      return undefined;
    }
    const sharedExists = await exists(key);
    let promote: string | undefined;
    if (!sharedExists) {
      const selected = await ui.select("Select existing managed key to promote", recognized.map(({ profile }) => profile.id));
      if (selected === undefined) throw new Cancelled();
      promote = recognized.find(({ profile }) => profile.id === selected)?.managed.key;
      if (!promote) throw new Error("No existing managed key was selected");
    }
    const warning = [
      sharedExists ? `Reuse existing shared key ${key}; it will NEVER be replaced.` : `Copy the existing key ${promote} byte-for-byte into ${key}, preserving encryption and every original key file.`,
      `Rewrite ONLY recognized wizard configs for: ${recognized.map(({ profile }) => profile.id).join(", ")}.`,
      `External or manually modified configs remain unchanged: ${skipped.join(", ") || "(none)"}.`,
      "Other login hosts may currently authorize a different key: you must authorize the shared public key on each distinct host using the provided bootstrap commands.",
      "Bootstrap configs retain each original managed login key without an IdentitiesOnly restriction, so authorization commands can still use that host's existing key plus normal original authentication.",
      "Existing shared known_hosts is preserved; missing trust starts empty. No automatic trust import.",
      "No SSH connection, key installation or allocation will be performed. Apply this local migration?",
    ].join("\n\n");
    if (!await ui.confirm("Confirm shared SSH migration", warning)) throw new Cancelled();
    const result = await withConfigLock(env, async () => {
      if (await safeRead(configPath) !== original) throw new Error("Cluster configuration changed concurrently; retry migration");
      for (const { managed } of recognized) {
        await safeDirectory(dirname(managed.config), false);
        if (await safeRead(managed.config) !== managed.before || await safeRead(join(dirname(managed.config), "bootstrap_config")) !== managed.bootstrap) throw new Error("Managed SSH configuration changed concurrently; retry migration");
      }
      // Existing shared identity wins even if another confirmed writer created
      // it while this command was awaiting user confirmation.
      await initializeTrust(env);
      const shared = await ensureSharedKey(env, promote);
      const staged: { path: string; temporary: string; rollback: string; replaced: boolean; preserveBackup: boolean }[] = [];
      const seen = new Set<string>();
      try {
        for (const { profile, managed } of recognized) {
          if (seen.has(managed.config)) continue;
          seen.add(managed.config);
          await chmod(dirname(managed.config), 0o700);
          for (const bootstrap of [false, true]) {
            const path = bootstrap ? join(dirname(managed.config), "bootstrap_config") : managed.config;
            const temporary = join(dirname(path), `.migrate-${randomUUID()}.tmp`);
            const rollback = join(dirname(path), `.rollback-${randomUUID()}.tmp`);
            await privateFile(rollback, bootstrap ? managed.bootstrap : managed.before);
            staged.push({ path, temporary, rollback, replaced: false, preserveBackup: false });
            await privateFile(temporary, managedConfig(managed.connection, profile.ssh_host, home, managed.systemConfig, bootstrap ? managed.bootstrapKey : shared.key, trust, !bootstrap));
          }
        }
        // Stage every rendering and original snapshot before replacing a pair.
        // A failed rename restores every already-committed original in reverse.
        try {
          for (const item of staged) {
            await replaceConfig(item.temporary, item.path);
            item.replaced = true;
          }
        } catch (commitError) {
          const failures: unknown[] = [commitError];
          for (const item of staged.toReversed()) {
            if (!item.replaced) continue;
            try { await rename(item.rollback, item.path); }
            catch (rollbackError) {
              item.preserveBackup = true;
              failures.push(new Error(`Could not restore ${item.path}; original snapshot retained at ${item.rollback}`, { cause: rollbackError }));
            }
          }
          if (failures.length > 1) throw new AggregateError(failures, `SSH migration failed and rollback needs manual restoration. Original snapshot -> destination: ${staged.filter((item) => item.preserveBackup).map((item) => `${item.rollback} -> ${item.path}`).join("; ")}`);
          throw commitError;
        }
      } finally {
        for (const item of staged) {
          await rm(item.temporary, { force: true });
          if (!item.preserveBackup) await rm(item.rollback, { force: true });
        }
      }
      return shared;
    });
    await publish([
      `Shared SSH migration complete for: ${recognized.map(({ profile }) => profile.id).join(", ")}.`,
      "All original per-profile key files were preserved. No SSH connection, key installation or allocation was performed.",
      `External or unrecognized profiles NOT migrated: ${skipped.join(", ") || "(none)"}.`,
      ...sshInstructions(recognized.map(({ profile }) => profile), result, env),
    ].join("\n\n"));
    events.emit("worker-harness:refresh", undefined);
    return recognized.map(({ profile }) => profile.id);
  } catch (error) {
    if (error instanceof Cancelled) { ui.notify("Shared SSH migration cancelled; nothing was saved.", "info"); return undefined; }
    ui.notify(error instanceof Error ? error.message : "Shared SSH migration failed", "error");
    return undefined;
  }
}
