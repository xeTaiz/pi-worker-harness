import { spawn } from "node:child_process";
import { constants } from "node:fs";
import { chmod, lstat, mkdir, open, readFile, rename, rm } from "node:fs/promises";
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
async function safeDirectory(path: string): Promise<void> {
  const parent = dirname(path);
  if (parent !== path) await safeDirectory(parent);
  try {
    const info = await lstat(path);
    if (!info.isDirectory() || info.isSymbolicLink()) throw new Error(`Unsafe configuration directory: ${path}`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    await mkdir(path, { mode: 0o700 });
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
async function privateFile(path: string, contents: string): Promise<void> {
  const file = await open(path, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600);
  try { await file.writeFile(contents); await file.sync(); } finally { await file.close(); }
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
function managedConfig(connection: Exclude<Connection, { kind: "reuse" }>, alias: string, home: string, key?: string): string {
  const lines = [`Host ${alias}`, ...(key ? [`  IdentityFile ${sshPath(key)}`, "  IdentitiesOnly yes"] : [])];
  if (connection.kind === "direct") {
    lines.push(`  HostName ${connection.hostname}`, `  User ${connection.user}`, `  Port ${connection.port}`);
    if (connection.jump) lines.push(`  ProxyJump ${connection.jump}`);
  }
  // Reset Host context before Include: aliases inside the user's file still match.
  lines.push("Host *", `  Include ${includePath(join(home, ".ssh", "config"))}`, "Host *", "  Include /etc/ssh/ssh_config", "");
  return lines.join("\n");
}

/** Commit one new profile under an exclusive cooperating-writer lock; never replace keys or entries. */
async function saveProfile(profile: ClusterProfile, connection: Connection, env: Environment): Promise<{ key: string; publicKey?: string }> {
  const configPath = resolveClusterConfigPath(env);
  const directory = dirname(configPath);
  await safeDirectory(directory);
  const lockPath = configPath + ".lock";
  let lock;
  try { lock = await open(lockPath, constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW, 0o600); }
  catch { throw new Error(`Configuration is locked or lock path is unsafe: ${lockPath}. Retry after the other writer finishes; do not remove an active lock.`); }
  let created: string | undefined;
  let temporary: string | undefined;
  try {
    const before = await safeRead(configPath);
    const document = before === undefined ? { version: 1, clusters: [] as unknown[] } : JSON.parse(before);
    const profiles = parseClusterProfiles(document, configPath);
    if (profiles.some((entry) => entry.id === profile.id)) throw new Error(`Cluster ${profile.id} already exists; nothing was replaced`);
    parseClusterProfiles({ version: 1, clusters: [...document.clusters, profile] }, configPath);
    let key: string;
    let publicKey: string | undefined;
    if (connection.kind === "reuse") {
      if (profile.ssh_config_file !== connection.config) throw new Error("Copied SSH configuration changed during setup");
      await safeRead(connection.config).then((content) => { if (content === undefined) throw new Error("Copied SSH configuration no longer exists"); });
      key = join(dirname(connection.config), "id_ed25519");
      const info = await lstat(key);
      if (!info.isFile() || info.isSymbolicLink()) throw new Error("Copied dedicated SSH key is missing or unsafe");
      publicKey = (await safeRead(key + ".pub"))?.trim();
      if (!validPublicKey(publicKey)) throw new Error("Copied public key is missing or invalid; no key contents were published");
    } else {
      const home = env.HOME;
      if (!home || !home.startsWith("/") || controls.test(home)) throw new Error("An absolute HOME is required for managed SSH configuration");
      const sshDirectory = join(directory, "ssh");
      await safeDirectory(sshDirectory);
      await chmod(sshDirectory, 0o700);
      const target = join(sshDirectory, profile.id);
      if (await exists(target)) throw new Error(`SSH destination already exists: ${target}; no key was replaced`);
      await mkdir(target, { mode: 0o700 });
      created = target;
      key = join(target, "id_ed25519");
      await keygen(key);
      await chmod(key, 0o600);
      await chmod(key + ".pub", 0o600);
      publicKey = (await readFile(key + ".pub", "utf8")).trim();
      if (!validPublicKey(publicKey)) throw new Error("ssh-keygen did not produce a valid Ed25519 public key");
      await privateFile(join(target, "config"), managedConfig(connection, profile.ssh_host, home, key));
      // Bootstrap keeps ordinary default-file/agent/password authentication available.
      await privateFile(join(target, "bootstrap_config"), managedConfig(connection, profile.ssh_host, home));
    }
    temporary = join(directory, `.clusters-${randomUUID()}.tmp`);
    await privateFile(temporary, JSON.stringify({ version: 1, clusters: [...document.clusters, profile] }, null, 2) + "\n");
    // Also detect non-cooperating edits since the lock was acquired.
    if (await safeRead(configPath) !== before) throw new Error("Cluster configuration changed concurrently; retry setup");
    await rename(temporary, configPath);
    temporary = undefined;
    created = undefined;
    return { key, publicKey };
  } finally {
    if (temporary) await rm(temporary, { force: true });
    if (created) await rm(created, { recursive: true, force: true });
    await lock.close();
    await rm(lockPath);
  }
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
    const configPath = resolveClusterConfigPath(env);
    // OpenSSH expands environment references and Include globs even in quoted paths.
    // Refuse unrepresentable literal paths before any write or key generation.
    const home = env.HOME;
    if (configPath.includes("${") || !home || !home.startsWith("/") || controls.test(home) || /[\[\]*?]/.test(home) || home.includes("${")) {
      throw new Error("Managed SSH requires an absolute HOME without glob/environment expansion, and a config path without ${...} references");
    }
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
      draft.ssh_config_file = connection.config;
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
    const warning = connection.kind === "reuse"
      ? "Reuse the existing dedicated SSH config/key; no key will be generated or replaced. Save this new profile?"
      : "Generate a NEW dedicated, initially UNENCRYPTED Ed25519 private key on this machine and save this profile? The UI cannot safely collect a passphrase. Protect it with ssh-keygen -p and load it into ssh-agent afterward. No SSH connection, key installation or allocation will be performed.";
    if (!await ui.confirm("Confirm cluster registration", warning)) throw new Cancelled();
    const result = await saveProfile(profile, connection, env);
    const config = profile.ssh_config_file!;
    const bootstrap = join(dirname(config), "bootstrap_config");
    const destination = `${quote(bootstrap)} ${quote(profile.ssh_host)}`;
    const message = [
      `Cluster ${profile.id} registered and visible immediately as cluster:${profile.id}.`,
      `Profiles: ${configPath}`, `SSH config: ${config}`, `Bootstrap SSH config (ordinary authentication): ${bootstrap}`, `Dedicated key: ${result.key}${connection.kind === "reuse" ? " (reused; not replaced)" : " (initially unencrypted)"}`,
      "No allocation submitted. Registration is not proof of connectivity.",
      "In your own terminal, first connect and verify the host fingerprint against a trusted site source (do not bypass host checking):",
      `ssh -F ${destination}`,
      "Install the public key yourself (password / keyboard-interactive authentication remains available; this is never auto-executed):",
      `ssh-copy-id -F ${quote(bootstrap)} -i ${quote(result.key + ".pub")} ${quote(profile.ssh_host)}`,
      "If ssh-copy-id is unavailable, use the first-connect terminal command above, then paste the PUBLIC key below into ~/.ssh/authorized_keys on the login host (mkdir -p ~/.ssh; chmod 700 ~/.ssh; touch ~/.ssh/authorized_keys; chmod 600 ~/.ssh/authorized_keys). Never copy the private key.",
      ...(result.publicKey ? ["Public key:", result.publicKey] : [`Reuse the public key at ${result.key}.pub.`]),
      "Encrypt and load the dedicated key locally (encrypted keys require a running ssh-agent for BatchMode):",
      `ssh-keygen -p -f ${quote(result.key)}`, `ssh-add ${quote(result.key)}`,
      "After installation, fingerprint verification and ssh-agent loading, verify strict noninteractive access:",
      `ssh -F ${quote(config)} -o BatchMode=yes -o StrictHostKeyChecking=yes -o ConnectTimeout=10 ${quote(profile.ssh_host)} 'true'`,
    ].join("\n\n");
    await publish(message);
    events.emit("worker-harness:refresh", undefined);
    return profile;
  } catch (error) {
    if (error instanceof Cancelled) { ui.notify("Cluster registration cancelled; nothing was saved.", "info"); return undefined; }
    ui.notify(error instanceof Error ? error.message : "Cluster registration failed", "error");
    return undefined;
  }
}
