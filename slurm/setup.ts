import { spawn } from "node:child_process";
import { constants, type Stats } from "node:fs";
import { chmod, lstat, mkdir, open, realpath, rename, rm, stat } from "node:fs/promises";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";
import { isIP } from "node:net";
import { events } from "../events.ts";
import { ClusterConfigError, loadClusterProfiles, parseClusterProfiles, resolveClusterConfigPath, type ClusterProfile, type Environment } from "./profiles.ts";

/** Deliberately limited to real extension UI methods; never request secrets in input(). */
export interface SetupUI {
  input(title: string, placeholder?: string): Promise<string | undefined>;
  select(title: string, options: string[]): Promise<string | undefined>;
  editor(title: string, prefill?: string): Promise<string | undefined>;
  confirm(title: string, message: string): Promise<boolean>;
  notify(message: string, type?: "info" | "warning" | "error" | "success"): void;
}
type Draft = Record<string, unknown>;
type Connection = { kind: "alias"; alias: string } | { kind: "direct"; hostname: string; user: string; port: number; jump?: string } | { kind: "reuse"; config: string; alias: string };
class Cancelled extends Error {}
const controls = /[\u0000-\u001f\u007f]/;
const quote = (text: string) => "'" + text.replace(/'/g, "'\\''") + "'";
// OpenSSH expands % tokens in IdentityFile; %% means a literal percent.
const sshPath = (text: string) => '"' + text.replace(/%/g, "%%").replace(/\\/g, "\\\\").replace(/"/g, '\\"') + '"';
const includePath = sshPath;
const identifier = /^[A-Za-z0-9][A-Za-z0-9_-]*$/;
const aliasPattern = /^[A-Za-z0-9][A-Za-z0-9_.-]*$/;
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

function managedConfig(connection: Exclude<Connection, { kind: "reuse" }>, alias: string, home: string, systemConfig: string | undefined, key?: string, trust?: string, restrictIdentity = true): string {
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

type ManagedConnection = { connection: Exclude<Connection, { kind: "reuse" }>; systemConfig?: string; key: string; bootstrapKey?: string; config: string; before: string; bootstrap: string };

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

/** Commit one profile and shared-key initialization under the same writer lock. */
async function saveProfile(profile: ClusterProfile, connection: Connection, env: Environment): Promise<{ key: string; publicKey: string; encrypted: boolean; warning?: string }> {
  return withConfigLock(env, async () => {
    const { configPath, directory, ssh, key, home, trust } = sshLayout(env);
    const before = await safeRead(configPath);
    const document = before === undefined ? { version: 1, clusters: [] as unknown[] } : JSON.parse(before);
    const profiles = parseClusterProfiles(document, configPath);
    if (profiles.some((entry) => entry.id === profile.id)) throw new Error(`Cluster ${profile.id} already exists; nothing was replaced`);
    parseClusterProfiles({ version: 1, clusters: [...document.clusters, profile] }, configPath);
    let selected: ManagedConnection["connection"];
    if (connection.kind === "reuse") {
      const source = profiles.find((entry) => entry.ssh_config_file === connection.config && entry.ssh_host === connection.alias);
      const managed = source ? await readManaged(source, env) : undefined;
      if (!managed) throw new Error("Source is not an unchanged wizard SSH config; use a new connection or /cluster-ssh-migrate");
      if (managed.key !== key) throw new Error("Legacy per-profile keys require explicit /cluster-ssh-migrate before copying; no login key was silently replaced");
      selected = managed.connection;
    } else selected = connection;
    const target = join(ssh, profile.id);
    if (await exists(target)) throw new Error(`SSH destination already exists: ${target}; no key was replaced`);
    // Legacy wizard keys must be explicitly promoted, not silently superseded
    // by generation of a new, initially unauthorized shared login identity.
    if (!await exists(key)) {
      for (const entry of profiles) if (await readManaged(entry, env)) throw new Error("Existing managed login keys require /cluster-ssh-migrate first; no new key was generated");
    }
    await initializeTrust(env);
    const result = await ensureSharedKey(env);
    await mkdir(target, { mode: 0o700 });
    let temporary: string | undefined;
    let committed = false;
    try {
      const system = await inspectSystemSshConfig();
      await privateFile(join(target, "config"), managedConfig(selected, profile.ssh_host, home, system.includePath, result.key, trust));
      await privateFile(join(target, "bootstrap_config"), managedConfig(selected, profile.ssh_host, home, system.includePath, undefined, trust));
      temporary = join(directory, `.clusters-${randomUUID()}.tmp`);
      await privateFile(temporary, JSON.stringify({ version: 1, clusters: [...document.clusters, profile] }, null, 2) + "\n");
      if (await safeRead(configPath) !== before) throw new Error("Cluster configuration changed concurrently; retry setup");
      await rename(temporary, configPath);
      temporary = undefined;
      committed = true;
      return { ...result, warning: system.warning };
    } finally {
      if (temporary) await rm(temporary, { force: true });
      if (!committed) await rm(target, { recursive: true, force: true });
    }
  });
}

const labels: Record<string, string> = {
  id: "Cluster ID", description: "Name / description (include site limits)", storage_id: "Shared storage ID",
  shared_root: "Shared root on cluster (absolute path)", sif_path: "Worker SIF on cluster (absolute path)",
  runtime_setup: "Trusted runtime shell prelude (empty is allowed; never paste secrets)", account: "Slurm account", partition: "Slurm partition",
  gpu_type: "Slurm GPU type", max_gpus: "Maximum GPUs per allocation", max_walltime_seconds: "Maximum walltime (seconds)",
  failure_grace_seconds: "Failure diagnosis grace (seconds; 0 for no idle holding)", termination_grace_seconds: "Termination grace (seconds)",
  tailnet_key_file: "Local Tailnet enrollment KEY FILE (absolute path; not the key value)", tailnet_login_server: "Tailnet login server URL",
  orchestrator_host: "Orchestrator hostname / IP (no scheme or port)", qos: "QoS (optional)", constraint: "Feature constraint (optional)",
  max_cpus: "Maximum CPUs (optional)", max_memory_mb: "Maximum memory MiB (optional)", warning_seconds: "Signal warning (seconds)",
  orchestrator_registration_port: "Orchestrator registration port", poll_seconds: "Scheduler poll interval (seconds)",
};
const numeric: Record<string, true> = { max_gpus: true, max_walltime_seconds: true, failure_grace_seconds: true, termination_grace_seconds: true, max_cpus: true, max_memory_mb: true, warning_seconds: true, orchestrator_registration_port: true, poll_seconds: true };
const optional: Record<string, true> = { qos: true, constraint: true, max_cpus: true, max_memory_mb: true };

/** Entire wizard is read-only until its final explicit confirmation. */
export async function runClusterAdd(ui: SetupUI, publish: (content: string) => void | Promise<void>, args = "", env: Environment = process.env): Promise<ClusterProfile | undefined> {
  try {
    const { configPath, key: sharedKey } = sshLayout(env);
    const existing = await loadClusterProfiles(env);
    const choose = async (title: string, options: string[]) => { const value = await ui.select(title, options); if (value === undefined) throw new Cancelled(); return value; };
    const input = async (title: string, current = "", validate?: (value: string) => boolean, allowEmpty = false): Promise<string> => {
      for (;;) {
        const value = await ui.input(title, current || undefined);
        if (value === undefined) throw new Cancelled();
        const result = allowEmpty && value.trim() === "-" ? "" : value === "" ? current : value.trim();
        if ((!result && !allowEmpty) || controls.test(result) || (validate && !validate(result))) { ui.notify("Invalid value; correct this field or Escape to cancel. Earlier answers are retained.", "error"); continue; }
        return result;
      }
    };
    const draft: Draft = { warning_seconds: 120, orchestrator_registration_port: 12888, poll_seconds: 15, runtime_setup: "" };
    const requestedId = args.trim();
    draft.id = requestedId && identifier.test(requestedId) && !existing.some((entry) => entry.id === requestedId)
      ? requestedId
      : await input("Cluster ID", requestedId, (value) => identifier.test(value) && !existing.some((entry) => entry.id === value));
    let source: ClusterProfile | undefined;
    if (existing.length) {
      const options = ["New connection and storage", ...existing.map((profile) => `Copy ${profile.id}: connection / storage / runtime`)];
      const copy = await choose("Reuse an existing cluster connection and shared storage?", options);
      source = existing[options.indexOf(copy) - 1];
      if (source) for (const field of ["storage_id", "shared_root", "sif_path", "runtime", "runtime_setup", "tailnet_key_file", "tailnet_login_server", "orchestrator_host", "orchestrator_registration_port"] as const) draft[field] = source[field];
    }
    let connection: Connection;
    const edit = async (field: string): Promise<void> => {
      if (field === "runtime_setup") {
        const value = await ui.editor(labels[field], String(draft[field] ?? ""));
        if (value === undefined) throw new Cancelled();
        if (value.includes("\0")) { ui.notify("Shell prelude cannot contain NUL", "error"); return edit(field); }
        draft[field] = value;
        return;
      }
      if (field === "runtime") { draft.runtime = await choose("Container runtime", draft.runtime === "singularity" ? ["singularity", "apptainer"] : ["apptainer", "singularity"]); return; }
      const value = await input((labels[field] ?? field) + (optional[field] ? " (- clears)" : ""), String(draft[field] ?? ""), numeric[field] ? (text) => optional[field] && !text || /^\d+$/.test(text) && Number.isSafeInteger(Number(text)) && Number(text) >= (["failure_grace_seconds", "termination_grace_seconds", "warning_seconds"].includes(field) ? 0 : 1) : undefined, Boolean(optional[field]));
      if (optional[field] && !value) delete draft[field]; else draft[field] = numeric[field] ? Number(value) : value;
    };
    await edit("description");
    draft.compute_kind = await choose("Compute kind", ["cpu", "gpu"]);
    const managedSource = source?.ssh_config_file === join(dirname(configPath), "ssh", source?.id ?? "", "config") ? source : undefined;
    if (source?.ssh_config_file && !managedSource) ui.notify("This profile uses an external SSH config, not a managed dedicated key. Choose a normal SSH alias or direct connection for the new dedicated key.", "info");
    connection = managedSource
      ? { kind: "reuse", config: managedSource.ssh_config_file!, alias: managedSource.ssh_host }
      : { kind: "alias", alias: source?.ssh_host ?? "" };
    const editConnection = async (): Promise<void> => {
      const mode = await choose("SSH connection", ["Existing SSH alias", "Direct hostname"]);
      if (mode === "Existing SSH alias") connection = { kind: "alias", alias: await input("Existing SSH alias (preserves HostName / ProxyJump / ControlMaster)", connection.kind === "alias" ? connection.alias : source?.ssh_host, (value) => aliasPattern.test(value)) };
      else {
        const previous = connection.kind === "direct" ? connection : undefined;
        const hostname = await input("SSH hostname / IP", previous?.hostname, (value) => Boolean(isIP(value)) || value.split(".").every((label) => /^[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?$/.test(label)));
        const user = await input("SSH username", previous?.user, (value) => /^[A-Za-z0-9_][A-Za-z0-9_.-]*$/.test(value));
        const port = Number(await input("SSH port", String(previous?.port ?? 22), (value) => /^\d+$/.test(value) && Number(value) >= 1 && Number(value) <= 65535));
        const jump = await input("ProxyJump (optional alias or user@host:port)", previous?.jump, (value) => !value || /^[A-Za-z0-9_][A-Za-z0-9_.@:[\],-]*$/.test(value), true);
        connection = { kind: "direct", hostname, user, port, jump: jump || undefined };
      }
      draft.ssh_host = connection.kind === "alias" ? connection.alias : `wh-${draft.id}`;
      draft.ssh_config_file = join(dirname(configPath), "ssh", String(draft.id), "config");
    };
    if (connection.kind === "reuse") {
      draft.ssh_host = connection.alias;
      draft.ssh_config_file = join(dirname(configPath), "ssh", String(draft.id), "config");
    } else await editConnection();
    draft.storage_id ??= draft.id;
    for (const field of ["storage_id", "shared_root", "sif_path", "runtime", "runtime_setup", "account", "partition"]) await edit(field);
    if (draft.compute_kind === "gpu") { await edit("gpu_type"); await edit("max_gpus"); } else draft.max_gpus = 0;
    for (const field of ["max_walltime_seconds", "failure_grace_seconds", "termination_grace_seconds", "tailnet_key_file", "tailnet_login_server", "orchestrator_host"]) await edit(field);
    if (await choose("Advanced settings", ["Use defaults (warning 120s, port 12888, poll 15s; no extra caps)", "Configure advanced settings"]) === "Configure advanced settings") {
      for (const field of ["qos", "constraint", "max_cpus", "max_memory_mb", "warning_seconds", "orchestrator_registration_port", "poll_seconds"]) await edit(field);
    }
    let profile: ClusterProfile;
    for (;;) {
      try { profile = parseClusterProfiles({ version: 1, clusters: [...existing, draft] }, configPath).at(-1)!; }
      catch (error) {
        if (!(error instanceof ClusterConfigError)) throw error;
        ui.notify(error.message, "error");
        const field = error.field?.split(".").at(-1);
        if (field && labels[field]) { await edit(field); continue; }
        throw error;
      }
      const review = await choose(`Review cluster ${profile.id}\n${JSON.stringify(profile, null, 2)}\nSSH: ${connection.kind === "direct" ? `${connection.user}@${connection.hostname}:${connection.port}` : connection.alias}`, ["Save profile", "Edit a field", "Cancel"]);
      if (review === "Cancel") throw new Cancelled();
      if (review === "Edit a field") {
        const fields = Object.keys(draft).filter((field) => labels[field] && field !== "id" && !(draft.compute_kind === "cpu" && field === "max_gpus"));
        const selected = await choose("Field to correct", ["SSH connection", ...fields.map((field) => labels[field]!)]);
        if (selected === "SSH connection") await editConnection();
        else await edit(fields.find((field) => labels[field] === selected)!);
        continue;
      }
      break;
    }
    const warning = (await exists(sharedKey)
      ? `Reuse the existing shared SSH key at ${sharedKey}; no key will be generated or replaced.`
      : `Generate ONE shared, initially UNENCRYPTED Ed25519 private key at ${sharedKey} for all managed profiles? Existing managed keys instead require /cluster-ssh-migrate. The UI cannot safely collect a passphrase. Protect it with ssh-keygen -p afterward.`)
      + " Save this profile? No SSH connection, key installation or allocation will be performed.";
    if (!await ui.confirm("Confirm cluster registration", warning)) throw new Cancelled();
    const result = await saveProfile(profile, connection, env);
    await publish([
      `Cluster ${profile.id} registered and visible immediately as cluster:${profile.id}.`,
      `Profiles: ${configPath}`,
      "No allocation submitted. Registration is not proof of connectivity.",
      ...(result.warning ? [result.warning] : []),
      ...sshInstructions([profile], result, env),
    ].join("\n\n"));
    events.emit("worker-harness:refresh", undefined);
    return profile;
  } catch (error) {
    if (error instanceof Cancelled) { ui.notify("Cluster registration cancelled; nothing was saved.", "info"); return undefined; }
    ui.notify(error instanceof Error ? error.message : "Cluster registration failed", "error");
    return undefined;
  }
}

function sshInstructions(profiles: ClusterProfile[], result: { key: string; publicKey: string; encrypted: boolean }, env: Environment): string[] {
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
