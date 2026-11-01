import { spawn } from "node:child_process";
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { chmod, link, mkdir, open, readFile, readdir, rename, rm } from "node:fs/promises";
import { dirname, join, posix } from "node:path";
import * as api from "../api.ts";
import type { ClusterTarget, Job, JobLogsResult, RunnerResult, SlurmEnqueueOptions, SlurmObservation } from "../types.ts";
import { loadClusterProfiles, sharedPath, sharedRoots, type ClusterProfile, type SharedRoot } from "./profiles.ts";
import { parseInventory, sizeResources, type Inventory, type RequestedResources } from "./resources.ts";

export interface SlurmContext {
  client_kind: string;
  session_id: string;
  recipient_id: string;
  /** Bound to the orchestrator at creation; never reuse across credential/session changes. */
  orchestrator_url?: string;
  /** Canonical private client/session namespace, shared with notification cursors. */
  state_dir?: string;
}
export interface ClusterEnqueueInput {
  command: string;
  name: string;
  expected_seconds: number;
  gpu_count?: number;
  no_pty?: boolean;
  slurm: SlurmEnqueueOptions;
  submission_key?: string;
  candidate_job_id?: string;
}
export interface SshConnection { ssh_host: string; ssh_config_file?: string }
export interface TransportOptions { timeout_ms?: number; max_bytes?: number; signal?: AbortSignal }
export type SlurmTransport = (profile: ClusterProfile, script: string, stdin?: string, options?: TransportOptions) => Promise<string>;
export type ClientApi = Pick<typeof api, "getOrchestratorUrl" | "getAuthorizationHeaders" | "reserveWorker" | "observeSlurmWorker" | "enqueueJob" | "getJob" | "getWorker" | "listJobs" | "listQueue" | "listDataPaths" | "startJob" | "getJobLogs" | "stopJob">;
interface SchedulerRecord { id: string; name: string; comment: string; user: string; state: string; reason: string }
interface Journal {
  version: 1;
  submission_key: string;
  digest: string;
  request_id: string;
  registration_token: string;
  context: SlurmContext;
  profile: ClusterProfile;
  input: ClusterEnqueueInput;
  created_at: number;
  worker_id?: string;
  job_id?: string;
  resources?: RequestedResources;
  paths?: { project_path: string; environment_path?: string; image_sha256: string; lock_sha256: string | null };
  command?: string;
  host_run_path?: string;
  run_path?: string;
  scheduler_user?: string;
  scheduler_cluster?: string;
  slurm_job_id?: string;
  candidates?: SchedulerRecord[];
  submission_attempted?: boolean;
  submission_failed?: boolean;
  terminal?: boolean;
}
export class SlurmClientError extends Error {
  constructor(readonly code: string, message: string, readonly submission_key?: string, readonly evidence?: unknown) {
    super(`${code}: ${message}${submission_key ? ` (submission_key=${submission_key})` : ""}`);
    this.name = "SlurmClientError";
  }
}
export function shellQuote(value: string): string {
  if (value.includes("\0")) throw new SlurmClientError("INVALID_SHELL_VALUE", "NUL is not permitted");
  return "'" + value.replace(/'/g, "'\\''") + "'";
}
/** Persisted in the ordinary job, not merely in the batch launcher environment. */
export function workloadCommand(project: string, environment: string | undefined, command: string): string {
  return `cd -- ${shellQuote(project)} && ${environment ? `export PATH=${shellQuote(environment + "/bin")}:"$PATH" && ` : ""}exec /bin/bash -c ${shellQuote(command)}`;
}
function canonical(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(canonical).join(",") + "]";
  if (value && typeof value === "object") return "{" + Object.entries(value).filter(([, v]) => v !== undefined).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => JSON.stringify(k) + ":" + canonical(v)).join(",") + "}";
  return JSON.stringify(value) ?? "null";
}
const sha256 = (value: string) => createHash("sha256").update(value).digest("hex");
const terminalState = (state: string) => /^(COMPLETED|CANCELLED|FAILED|TIMEOUT|PREEMPTED|NODE_FAIL|OUT_OF_MEMORY|BOOT_FAIL|DEADLINE|REVOKED|SPECIAL_EXIT)(?:\b|\+)/.test(state);
const terminalJob = (job: Job) => job.status === "done" || job.status === "failed";
const safeId = (value: string) => /^[A-Za-z0-9][A-Za-z0-9_.-]{0,199}$/.test(value);
const py = (source: string, args: string[] = []) => `python3 -c ${shellQuote(source)} ${args.map(shellQuote).join(" ")}`;

/** No shell joining locally: user SSH aliases retain normal ProxyJump/ControlMaster configuration. */
export const sshTransport = (profile: SshConnection, script: string, stdin = "", options: TransportOptions = {}): Promise<string> => {
  if (!/^(?:[A-Za-z0-9_][A-Za-z0-9_.-]*@)?[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(profile.ssh_host)) return Promise.reject(new SlurmClientError("INVALID_SSH_ALIAS", "unsupported SSH destination"));
  const configFile = profile.ssh_config_file;
  if (configFile !== undefined && (
    typeof configFile !== "string" || !posix.isAbsolute(configFile) || /[\u0000-\u001f\u007f]/.test(configFile) ||
    configFile.split("/").some((part) => part === "." || part === "..") || posix.normalize(configFile) === "/"
  )) return Promise.reject(new SlurmClientError("INVALID_SSH_CONFIG_FILE", "expected an absolute POSIX SSH configuration path without traversal, control characters or filesystem root"));
  const { promise, resolve, reject } = Promise.withResolvers<string>();
  const child = spawn("ssh", [...(configFile === undefined ? [] : ["-F", configFile]), "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10", "--", profile.ssh_host, `bash -c ${shellQuote(script)}`], { stdio: ["pipe", "pipe", "pipe"], signal: options.signal });
  const chunks: Buffer[] = [], errors: Buffer[] = [];
  let size = 0, failed = false;
  const fail = (error: Error) => { if (!failed) { failed = true; child.kill("SIGKILL"); reject(error); } };
  const timer = setTimeout(() => fail(new SlurmClientError("SSH_TIMEOUT", "local SSH timed out; submission may be uncertain")), options.timeout_ms ?? 60_000);
  const collect = (list: Buffer[], data: Buffer) => {
    size += data.length;
    if (size > (options.max_bytes ?? 2 * 1024 * 1024)) fail(new SlurmClientError("SSH_OUTPUT_LIMIT", "SSH output exceeded its bound"));
    else list.push(data);
  };
  child.stdout.on("data", (chunk: Buffer) => collect(chunks, chunk));
  child.stderr.on("data", (chunk: Buffer) => collect(errors, chunk));
  child.on("error", fail);
  child.stdin.on("error", (error: NodeJS.ErrnoException) => { if (error.code !== "EPIPE") fail(error); });
  child.on("close", (code) => {
    clearTimeout(timer);
    if (failed) return;
    if (code !== 0) reject(new SlurmClientError("SSH_COMMAND_FAILED", `SSH exited ${code}: ${Buffer.concat(errors).toString("utf8").slice(-8192)}`, undefined, { stdout: Buffer.concat(chunks).toString("utf8"), exit_code: code }));
    else resolve(Buffer.concat(chunks).toString("utf8"));
  });
  child.stdin.end(stdin);
  return promise;
};

export interface SharedMappingVerificationOptions {
  api?: Pick<ClientApi, "listDataPaths" | "startJob">;
  transport?: typeof sshTransport;
  signal?: AbortSignal;
}

/** Prove every mapping against an existing ordinary worker, without an allocation. */
export async function verifySharedMappings(connection: SshConnection, mappings: SharedRoot[], options: SharedMappingVerificationOptions = {}): Promise<void> {
  const workerApi = options.api ?? api, transport = options.transport ?? sshTransport;
  const ids = new Set<string>();
  if (!mappings.length) throw new SlurmClientError("SHARED_STORAGE_NOT_VERIFIED", "at least one shared mapping is required");
  for (const mapping of mappings) {
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]*$/.test(mapping.storage_id) || ids.has(mapping.storage_id)) throw new SlurmClientError("INVALID_SHARED_MAPPING", "shared mapping IDs must be valid and unique");
    ids.add(mapping.storage_id);
    if (!posix.isAbsolute(mapping.shared_root) || /[\u0000-\u001f\u007f:;,]/.test(mapping.shared_root) || posix.normalize(mapping.shared_root) === "/" || mapping.shared_root.split("/").some((part) => part === "." || part === "..")) throw new SlurmClientError("INVALID_SHARED_MAPPING", "shared roots must be safe absolute paths without traversal or bind delimiters");
  }
  options.signal?.throwIfAborted();
  const catalog = await workerApi.listDataPaths();
  for (const mapping of mappings) {
    options.signal?.throwIfAborted();
    const base = `/data/shared/${mapping.storage_id}`;
    const candidates = [...new Set(Object.entries(catalog).filter(([path]) => path === base || path.startsWith(base + "/")).flatMap(([, workers]) => workers.map((w) => w.worker_id)))];
    const probe = ".wh-probe-" + randomUUID(), contents = randomBytes(32).toString("hex");
    let verified = false, lastError: unknown;
    for (const worker_id of candidates) {
      options.signal?.throwIfAborted();
      const path = base + "/" + probe;
      try {
        const written = await workerApi.startJob({ worker_id, command: `umask 077; printf '%s' ${shellQuote(contents)} > ${shellQuote(path)}`, name: "WH shared-storage identity probe", sync: true, sync_timeout: 30, no_pty: true });
        if (written.status !== "done" || written.exit_code !== 0) throw new Error("ordinary probe job did not complete successfully");
        const remote = await transport(connection, "# wh:shared-probe\n" + py(String.raw`
import os,sys
root=os.path.realpath(sys.argv[1],strict=True)
if root=='/' or not os.path.isdir(root) or any(c in ':;,' or ord(c)<32 or ord(c)==127 for c in root): raise ValueError('invalid resolved shared root')
probe=os.path.realpath(sys.argv[2],strict=True)
if os.path.commonpath([root,probe])!=root: raise ValueError('resolved probe escapes shared storage')
with open(probe) as handle: sys.stdout.write(handle.read(1024))
`, [mapping.shared_root, mapping.shared_root + "/" + probe]), undefined, { max_bytes: 1024, signal: options.signal });
        if (remote !== contents) throw new Error("shared storage identity mismatch");
        verified = true;
      } catch (error) { lastError = error; }
      finally {
        // Cancellation must not strand a successfully written probe.
        let removed: Job;
        try { removed = await workerApi.startJob({ worker_id, command: `rm -f -- ${shellQuote(path)}`, name: "WH shared-storage probe cleanup", sync: true, sync_timeout: 30, no_pty: true }); }
        catch (error) { throw new SlurmClientError("SHARED_PROBE_CLEANUP_FAILED", `ordinary worker could not remove shared probe for ${mapping.storage_id}`, undefined, String(error)); }
        if (removed.status !== "done" || removed.exit_code !== 0) throw new SlurmClientError("SHARED_PROBE_CLEANUP_FAILED", `ordinary worker could not remove shared probe for ${mapping.storage_id}`);
      }
      options.signal?.throwIfAborted();
      if (verified) break;
    }
    if (!verified) throw new SlurmClientError("SHARED_STORAGE_NOT_VERIFIED", `no participating worker proved shared storage ${mapping.storage_id} at ${mapping.shared_root}`, undefined, String(lastError ?? "list_data contains no participating worker"));
  }
}

const PATH_PREFLIGHT = String.raw`
# wh:path-preflight
import hashlib,json,os,sys
p=json.load(sys.stdin)
roots={}
for mapping in p['mappings']:
 root=os.path.realpath(mapping['shared_root'],strict=True)
 if not os.path.isdir(root) or root=='/' or any(c in ':;,' or ord(c)<32 or ord(c)==127 for c in root): raise ValueError('invalid resolved shared root')
 if mapping['storage_id'] in roots: raise ValueError('duplicate storage ID')
 roots[mapping['storage_id']]=root
def inside(selection,path=None):
 root=roots[selection['storage_id']]
 r=os.path.realpath(path or selection['host_path'],strict=True)
 if os.path.commonpath([root,r])!=root: raise ValueError('resolved path escapes shared storage')
 return r
def container(selection,r):
 root=roots[selection['storage_id']]
 return '/data/shared/'+selection['storage_id']+('/'+os.path.relpath(r,root) if r!=root else '')
project=inside(p['project'])
if not os.path.isdir(project): raise ValueError('project_path must be a directory')
env=inside(p['environment']) if p.get('environment') else None
if env and not os.path.isdir(inside(p['environment'],os.path.join(env,'bin'))): raise ValueError('environment_path requires bin directory')
def digest(path):
 h=hashlib.sha256()
 with open(path,'rb') as f:
  for block in iter(lambda:f.read(1048576),b''): h.update(block)
 return h.hexdigest()
print(json.dumps(dict(project_path=container(p['project'],project),environment_path=container(p['environment'],env) if env else None,image_sha256=digest(p['sif_path']),lock_sha256=digest(os.path.join(project,'uv.lock')) if os.path.isfile(os.path.join(project,'uv.lock')) else None)))
`;
const STAGE = String.raw`
# wh:stage
import json,os,sys
p=json.load(sys.stdin); root=os.path.realpath(p['shared_root'],strict=True)
base=os.path.join(root,'.wh-slurm'); os.makedirs(base,mode=0o700,exist_ok=True)
if os.path.islink(base): raise ValueError('symlink run root forbidden')
os.chmod(base,0o700); run=os.path.join(base,p['worker_id'])
os.makedirs(run,mode=0o700,exist_ok=True)
if os.path.islink(run): raise ValueError('symlink run directory forbidden')
os.chmod(run,0o700)
for name,content in p['files'].items():
 if '/' in name or name in ('.','..'): raise ValueError('invalid asset filename')
 dest=os.path.join(run,name)
 # Existing staged settings/assets are immutable. Never replace a running allocation's secrets.
 if os.path.lexists(dest):
  if os.path.islink(dest): raise ValueError('symlink asset forbidden')
  with open(dest) as f:
   if f.read()!=content: raise ValueError('staged asset conflict: '+name)
  continue
 fd=os.open(dest,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(fd,'w') as f: f.write(content); f.flush(); os.fsync(f.fileno())
os.makedirs(os.path.join(run,'runtime'),mode=0o700,exist_ok=True)
print(json.dumps({'staged':True}))
`;
const SUBMIT = String.raw`
# wh:submit-fenced
import fcntl,json,os,subprocess,sys,tempfile,time
p=json.load(sys.stdin); run=p['run']; os.umask(0o077)
with open(os.path.join(run,'submit.lock'),'a') as lock:
 fcntl.flock(lock,fcntl.LOCK_EX)
 submitted=os.path.join(run,'submitted.json'); marker=os.path.join(run,'submission-started')
 if os.path.exists(submitted):
  with open(submitted) as f: print(f.read())
  sys.exit(0)
 if os.path.exists(marker): raise RuntimeError('submission already attempted; recover identity, never resubmit')
 fd=os.open(marker,os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
 with os.fdopen(fd,'w') as f: json.dump({'attempted_at':time.time()},f); f.flush(); os.fsync(f.fileno())
 env={k:v for k,v in os.environ.items() if not k.startswith('SBATCH_') and k not in ('SLURM_CLUSTERS','SLURM_HINT','SLURM_DEBUG_FLAGS','SLURM_EXIT_ERROR')}; env['SLURM_UMASK']='077'
 r=subprocess.run([p['sbatch']]+p['args'],env=env,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 result={'exit_code':r.returncode,'stdout':r.stdout[:16384],'stderr':r.stderr[:16384]}
 if r.returncode==0:
  value=r.stdout.strip(); parts=value.split(';')
  if len(parts)<=2 and parts[0].isdigit() and (len(parts)==1 or parts[1]==p['cluster']): result['slurm_job_id']=parts[0]
 fd,tmp=tempfile.mkstemp(prefix='.submitted.',dir=run)
 with os.fdopen(fd,'w') as f: json.dump(result,f); f.flush(); os.fsync(f.fileno())
 os.replace(tmp,submitted); print(json.dumps(result))
`;

export class SlurmClient {
  private readonly api: ClientApi;
  private readonly transport: SlurmTransport;
  private readonly env: NodeJS.ProcessEnv;
  private readonly inventory = new Map<string, Inventory>();
  constructor(options: { api?: Partial<ClientApi>; transport?: SlurmTransport; env?: NodeJS.ProcessEnv } = {}) {
    this.api = { ...api, ...options.api };
    this.transport = options.transport ?? sshTransport;
    this.env = options.env ?? process.env;
  }
  private namespace(context: SlurmContext): string {
    const url = context.orchestrator_url ?? this.api.getOrchestratorUrl();
    if (url !== this.api.getOrchestratorUrl()) throw new SlurmClientError("CLIENT_CONTEXT_CHANGED", "orchestrator changed; recreate client context");
    const base = this.env.WH_CLUSTER_STATE_DIR || join(this.env.XDG_STATE_HOME || join(this.env.HOME || "", ".local", "state"), "worker-harness", "slurm");
    if (!posix.isAbsolute(base)) throw new SlurmClientError("INVALID_STATE_DIRECTORY", "state directory must be absolute");
    return join(base, sha256(url), sha256(context.client_kind), sha256(context.session_id));
  }
  private async directory(path: string): Promise<void> { await mkdir(path, { recursive: true, mode: 0o700 }); await chmod(path, 0o700); }
  private async atomic(path: string, value: unknown): Promise<void> {
    await this.directory(dirname(path));
    const tmp = path + "." + randomUUID() + ".tmp";
    const file = await open(tmp, "wx", 0o600);
    try { await file.writeFile(JSON.stringify(value)); await file.sync(); } finally { await file.close(); }
    await rename(tmp, path);
    const directory = await open(dirname(path), "r");
    try { await directory.sync(); } finally { await directory.close(); }
  }
  private async submissionLock(path: string, key: string) {
    // flock attaches to the inherited open-file description. Keeping our fd open
    // retains the lock after the helper exits; process death releases it automatically.
    // Never unlink lock files: that would let two clients lock different inodes.
    const file = await open(path + ".lock", "a", 0o600);
    await file.chmod(0o600);
    const { promise, resolve, reject } = Promise.withResolvers<void>();
    const child = spawn("flock", ["-n", "-E", "73", "3"], { stdio: ["ignore", "ignore", "pipe", file.fd] });
    child.once("error", reject);
    child.stderr.resume();
    child.once("close", (code) => code === 0 ? resolve() : reject(new SlurmClientError(code === 73 ? "SUBMISSION_IN_PROGRESS" : "LOCAL_LOCK_FAILED", "cannot acquire submission lock; retry this key later", key)));
    try { await promise; return file; } catch (error) { await file.close(); throw error; }
  }
  async createContext(client_kind: string, session_id: string): Promise<SlurmContext> {
    if (!client_kind || !session_id) throw new SlurmClientError("INVALID_CLIENT_CONTEXT", "client kind and session ID required");
    const context: SlurmContext = { client_kind, session_id, recipient_id: "", orchestrator_url: this.api.getOrchestratorUrl() };
    context.state_dir = this.namespace(context);
    const path = join(context.state_dir, "recipient.json");
    await this.directory(dirname(path));
    const temporary = path + "." + randomUUID() + ".tmp";
    const file = await open(temporary, "wx", 0o600);
    try { await file.writeFile(JSON.stringify({ recipient_id: randomUUID() })); await file.sync(); } finally { await file.close(); }
    try {
      try { await link(temporary, path); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
      const directory = await open(dirname(path), "r");
      try { await directory.sync(); } finally { await directory.close(); }
    } finally { await rm(temporary, { force: true }); }
    const saved = JSON.parse(await readFile(path, "utf8"));
    if (typeof saved.recipient_id !== "string" || !saved.recipient_id) throw new SlurmClientError("INVALID_CLIENT_STATE", "invalid persisted recipient");
    return { ...context, recipient_id: saved.recipient_id };
  }
  private journalPath(context: SlurmContext, key: string): string { return join(this.namespace(context), "submissions", sha256(key) + ".json"); }
  private async save(journal: Journal): Promise<void> { await this.atomic(this.journalPath(journal.context, journal.submission_key), journal); }
  private async journals(context: SlurmContext): Promise<Journal[]> {
    const directory = join(this.namespace(context), "submissions");
    let files: string[];
    try { files = await readdir(directory); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return []; throw error; }
    return Promise.all(files.filter((name) => /^[a-f0-9]{64}\.json$/.test(name)).map(async (name) => JSON.parse(await readFile(join(directory, name), "utf8")) as Journal));
  }
  private async profile(profileOrId: ClusterProfile | string): Promise<ClusterProfile> {
    if (typeof profileOrId !== "string") return profileOrId;
    let id = profileOrId;
    if (id.startsWith('"')) {
      try { const decoded = JSON.parse(id); if (typeof decoded === "string") id = decoded; } catch { /* Unknown targets fail below. */ }
    }
    id = id.replace(/^cluster:/, "");
    const profile = (await loadClusterProfiles(this.env)).find((entry) => entry.id === id);
    if (!profile) throw new SlurmClientError("CLUSTER_NOT_FOUND", `unknown cluster target ${profileOrId}`);
    return profile;
  }
  async getInventory(profileOrId: ClusterProfile | string): Promise<Inventory> {
    const p = await this.profile(profileOrId);
    const script = `set -e\n${p.runtime_setup}\n` + py(String.raw`
# wh:inventory
import json,subprocess,sys
def run(args,optional=False):
 try:
  r=subprocess.run(args,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=25)
  if r.returncode and not optional: raise RuntimeError(r.stderr[-4096:])
  return r.stdout[:1048576] if not r.returncode else ''
 except (FileNotFoundError,subprocess.TimeoutExpired):
  if optional: return ''
  raise
print(json.dumps(dict(sinfo=run(['sinfo','--Node','--noheader','--partition='+sys.argv[1],'--format=NodeName=%N Partition=%P State=%T']),nodes=run(['scontrol','--oneliner','show','nodes']),partition=run(['scontrol','--oneliner','show','partition',sys.argv[1]]),ginfo=run(['ginfo'],True)[:8192])))
`, [p.partition]);
    const raw = JSON.parse(await this.transport(p, script, undefined, { timeout_ms: 90_000, max_bytes: 4 * 1024 * 1024 }));
    const inventory = parseInventory(p, { ...raw, observed_at: new Date().toISOString() });
    this.inventory.set(sha256(canonical(p)), inventory);
    return inventory;
  }
  clusterTarget(p: ClusterProfile, inventory = this.inventory.get(sha256(canonical(p)))): ClusterTarget {
    const observed = inventory ? Date.parse(inventory.observed_at) / 1000 : null;
    return { id: `cluster:${p.id}`, name: p.id, kind: "cluster", cluster_id: p.id, compute_kind: p.compute_kind, partition: p.partition, description: p.description, shared_path: sharedPath(p), resource_classes: { gpu_type: p.gpu_type, max_gpus: p.max_gpus, max_cpus: p.max_cpus, max_memory_mb: p.max_memory_mb, max_walltime_seconds: p.max_walltime_seconds }, availability: { status: observed === null ? "unknown" : Date.now() / 1000 - observed > p.poll_seconds * 2 ? "stale" : "fresh", observed_at: observed, detail: inventory ? "Scheduler observation only; permissions, fair-share and start time are not guaranteed." : "Not yet queried locally; get_worker refreshes scheduler inventory.", ...(inventory ? { scheduler: inventory.availability, sizing_limits: inventory.limits, ginfo: inventory.ginfo } : {}) } };
  }
  async listClusterTargets(_context?: SlurmContext): Promise<ClusterTarget[]> { return (await loadClusterProfiles(this.env)).map((p) => this.clusterTarget(p)); }
  async getClusterTarget(id: string, context?: SlurmContext): Promise<ClusterTarget> {
    const profile = await this.profile(id);
    const inventory = await this.getInventory(profile);
    const result = this.clusterTarget(profile, inventory);
    result.availability.defaults = sizeResources(profile, inventory, { walltime_seconds: Math.min(600, profile.max_walltime_seconds, inventory.limits.max_walltime_seconds ?? Infinity) });
    if (context) {
      const records = (await this.journals(context)).filter((j) => j.profile.id === profile.id && j.worker_id && j.submission_attempted && !j.terminal);
      if (records.length) {
        const evidence = await this.schedulerRecords(profile, records);
        result.availability.allocations = records.map((j) => ({ worker_id: j.worker_id, job_id: j.job_id, submission_key: j.submission_key, candidates: evidence.filter((r) => this.matches(j, r)) }));
      }
    }
    return result;
  }
  private hostPath(p: ClusterProfile, path: string): { storage_id: string; host_path: string } {
    if (!path || /[\u0000-\u001f\u007f]/.test(path) || path.split("/").some((v) => v === ".." || v === ".")) throw new SlurmClientError("INVALID_SHARED_PATH", "path must be beneath a configured /data/shared/<storage_id> mapping");
    const mapping = sharedRoots(p).find((root) => path === `/data/shared/${root.storage_id}` || path.startsWith(`/data/shared/${root.storage_id}/`));
    if (!mapping) throw new SlurmClientError("INVALID_SHARED_PATH", "path must be beneath a configured /data/shared/<storage_id> mapping");
    return { storage_id: mapping.storage_id, host_path: mapping.shared_root + path.slice(`/data/shared/${mapping.storage_id}`.length) };
  }
  private async preflight(p: ClusterProfile): Promise<{ sbatch: string; user: string; cluster: string }> {
    return JSON.parse(await this.transport(p, `set -e\n${p.runtime_setup}\n` + py(String.raw`
# wh:sbatch-preflight
import json,os,shutil,subprocess,sys
path=shutil.which('sbatch')
if not path: raise RuntimeError('sbatch not installed')
help=subprocess.check_output([path,'--help'],text=True)
for flag in ['--parsable','--no-requeue','--export','--nodes','--ntasks','--cpus-per-task','--mem','--comment','--signal','--time','--partition','--job-name','--output','--error'] + (['--gpus-per-node'] if sys.argv[1]=='gpu' else []) + (['--qos'] if sys.argv[2] else []) + (['--constraint'] if sys.argv[3] else []) + (['--account'] if sys.argv[4] else []):
 if flag not in help: raise RuntimeError('required sbatch flag unsupported: '+flag)
config=subprocess.check_output(['scontrol','show','config'],text=True)
cluster=''
for line in config.splitlines():
 if line.strip().startswith('ClusterName') and '=' in line: cluster=line.split('=',1)[1].strip().split()[0]
if not cluster: raise RuntimeError('could not establish local ClusterName')
print(json.dumps(dict(sbatch=os.path.realpath(path),user=subprocess.check_output(['id','-un'],text=True).strip(),cluster=cluster)))
`, [p.compute_kind, p.qos ?? "", p.constraint ?? "", p.account ?? ""])));
  }
  private async stage(j: Journal): Promise<void> {
    const p = j.profile, run = j.host_run_path!, resources = j.resources!;
    const assets = JSON.parse(await readFile(new URL("./runtime-assets.json", import.meta.url), "utf8"));
    if (assets.version !== 1 || typeof assets.assets?.["start-wh.sh"] !== "string" || typeof assets.assets?.["wh-allocation.sh"] !== "string") throw new SlurmClientError("RUNTIME_ASSETS_INVALID", "run just package-slurm-client with current root runtime");
    const settings = { version: 1, worker_id: j.worker_id, worker_name: `whc-${p.id}-${j.request_id}`, compute_kind: p.compute_kind, gpu_count: resources.gpu_count, cpus: resources.cpus, memory_mb: resources.memory_mb, failure_grace_seconds: p.failure_grace_seconds, termination_grace_seconds: p.termination_grace_seconds, warning_seconds: p.warning_seconds, checkpoint_signal: j.input.slurm.checkpoint_signal ?? "USR1", run_path: j.run_path, host_run_path: run, shared_root: p.shared_root, storage_id: p.storage_id, ...(p.additional_shared_roots === undefined ? {} : { additional_shared_roots: p.additional_shared_roots }), sif_path: p.sif_path, runtime: p.runtime, tailnet_key_file: run + "/tailnet.key", worker_token_file: run + "/worker.token", tailnet_login_server: p.tailnet_login_server, orchestrator_host: p.orchestrator_host, orchestrator_registration_port: p.orchestrator_registration_port };
    const files = { ...assets.assets, "settings.json": JSON.stringify(settings), "worker.token": j.registration_token, "tailnet.key": (await readFile(p.tailnet_key_file, "utf8")).trim(), "batch.sh": `#!/bin/bash\nset -euo pipefail\numask 077\n${p.runtime_setup}\nexec /bin/bash ${shellQuote(run + "/wh-allocation.sh")} ${shellQuote(run + "/settings.json")}\n` };
    await this.transport(p, py(STAGE), JSON.stringify({ shared_root: p.shared_root, worker_id: j.worker_id, files }));
  }
  private submissionArgs(j: Journal): string[] {
    const p = j.profile, run = j.host_run_path!;
    return ["--parsable", "--no-requeue", "--export=NIL", ...j.resources!.sbatch_flags, ...(p.account === undefined ? [] : [`--account=${p.account}`]), `--partition=${p.partition}`, ...(p.qos ? [`--qos=${p.qos}`] : []), ...(p.constraint ? [`--constraint=${p.constraint}`] : []), `--job-name=whc-${j.worker_id}`, `--comment=wh:${j.worker_id}`, `--output=${run}/batch.out`, `--error=${run}/batch.err`, ...(p.warning_seconds > 0 ? [`--signal=B:USR1@${p.warning_seconds}`] : []), run + "/batch.sh"];
  }
  private response(j: Journal): Record<string, unknown> {
    return { job_id: j.job_id, worker_id: j.worker_id, cluster_target_id: `cluster:${j.profile.id}`, submission_key: j.submission_key, slurm_job_id: j.slurm_job_id ?? null, scheduler_id: j.slurm_job_id ?? null, requested_resources: j.resources, shared_paths: { run_path: j.run_path, batch_script: j.run_path + "/batch.sh", batch_stdout: j.run_path + "/batch.out", batch_stderr: j.run_path + "/batch.err" }, evidence: j.paths, submission_attempted: !!j.submission_attempted };
  }
  async enqueueCluster(profileOrId: ClusterProfile | string, input: ClusterEnqueueInput, context: SlurmContext): Promise<Record<string, unknown>> {
    const key = input.submission_key ?? randomUUID();
    if (!safeId(key)) throw new SlurmClientError("INVALID_SUBMISSION_KEY", "submission key contains unsupported characters", key);
    const path = this.journalPath(context, key);
    await this.directory(dirname(path));
    const lock = await this.submissionLock(path, key);
    try {
      const p = await this.profile(profileOrId);
      if (!input.slurm || !input.command || !input.name || !Number.isFinite(input.expected_seconds)) throw new SlurmClientError("INVALID_CLUSTER_JOB", "command/name/expected_seconds/slurm required");
      if (input.candidate_job_id && !input.submission_key) throw new SlurmClientError("CANDIDATE_REQUIRES_SUBMISSION_KEY", "candidate ID only recovers an existing submission");
      const normalized = { command: input.command, name: input.name, expected_seconds: input.expected_seconds, gpu_count: input.gpu_count ?? (p.compute_kind === "cpu" ? 0 : 1), no_pty: input.no_pty ?? false, slurm: input.slurm };
      const digest = sha256(canonical({ profile: p, input: normalized, recipient_id: context.recipient_id }));
      let j: Journal;
      try { j = JSON.parse(await readFile(path, "utf8")); }
      catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
        if (input.candidate_job_id) throw new SlurmClientError("SUBMISSION_NOT_FOUND", "candidate requires an existing submission key");
        j = { version: 1, submission_key: key, digest, request_id: randomUUID(), registration_token: randomBytes(32).toString("hex"), context, profile: p, input: { ...normalized, submission_key: key }, created_at: Date.now() / 1000 };
        await this.save(j); // Scoped token and key exist durably before the first network operation.
      }
      if (j.digest !== digest) throw new SlurmClientError("SUBMISSION_KEY_CONFLICT", "same key was used with a different canonical request");
      if (j.submission_attempted) {
        await this.recover(j, input.candidate_job_id);
        return this.response(j);
      }
      const inventory = await this.getInventory(p); // Never submit against cached/free-only capacity.
      const sized = sizeResources(p, inventory, { ...input.slurm, gpu_count: normalized.gpu_count });
      if (j.worker_id && j.resources && (j.resources.cpus !== sized.cpus || j.resources.memory_mb !== sized.memory_mb || j.resources.gpu_count !== sized.gpu_count || j.resources.walltime_seconds !== sized.walltime_seconds)) throw new SlurmClientError("RESERVATION_RESOURCES_CHANGED", "fresh sizing differs from reserved resources; cancel and use a fresh submission key");
      j.resources = sized;
      const paths = JSON.parse(await this.transport(p, py(PATH_PREFLIGHT), JSON.stringify({ mappings: sharedRoots(p), project: this.hostPath(p, input.slurm.project_path), environment: input.slurm.environment_path ? this.hostPath(p, input.slurm.environment_path) : undefined, sif_path: p.sif_path }), { timeout_ms: 300_000 }));
      j.paths = { ...paths, environment_path: paths.environment_path ?? undefined };
      j.command = workloadCommand(paths.project_path, paths.environment_path ?? undefined, input.command);
      await verifySharedMappings(p, sharedRoots(p), { api: this.api, transport: (_connection, script, stdin, options) => this.transport(p, script, stdin, options) });
      const preflight = await this.preflight(p);
      j.scheduler_user = preflight.user; j.scheduler_cluster = preflight.cluster;
      await this.save(j);
      const r = j.resources;
      const reservation = await this.api.reserveWorker({ request_id: j.request_id, name: `whc-${p.id}-${j.request_id}`, gpu_count: r.gpu_count, recipient_id: context.recipient_id, registration_token_hash: sha256(j.registration_token), slurm: { cluster_id: p.id, storage_id: p.storage_id, compute_kind: p.compute_kind, cpus: r.cpus, memory_mb: r.memory_mb, walltime_seconds: r.walltime_seconds, failure_grace_seconds: p.failure_grace_seconds, warning_seconds: p.warning_seconds, termination_grace_seconds: p.termination_grace_seconds } });
      if (!safeId(reservation.worker_id) || reservation.worker_id.startsWith("cluster:")) throw new SlurmClientError("INVALID_RESERVED_WORKER", "server returned invalid allocation identity");
      if (j.worker_id && j.worker_id !== reservation.worker_id) throw new SlurmClientError("RESERVATION_ID_CONFLICT", "idempotent reservation changed worker ID");
      j.worker_id = reservation.worker_id;
      await this.save(j);
      const initial = await this.api.enqueueJob({ worker_id: j.worker_id, command: j.command, name: input.name, expected_seconds: input.expected_seconds, gpu_count: r.gpu_count, no_pty: input.no_pty, notify_recipient_id: context.recipient_id, submission_key: j.request_id });
      if (initial.worker_id !== j.worker_id || !safeId(initial.id)) throw new SlurmClientError("INITIAL_JOB_CONFLICT", "queue response does not match reserved worker");
      if (j.job_id && j.job_id !== initial.id) throw new SlurmClientError("INITIAL_JOB_CONFLICT", "idempotent queue changed job ID");
      j.job_id = initial.id; j.host_run_path = p.shared_root + "/.wh-slurm/" + j.worker_id; j.run_path = sharedPath(p) + "/.wh-slurm/" + j.worker_id;
      await this.save(j); // Never stage or sbatch before a confirmed attached ordinary job.
      if (terminalJob(initial)) throw new SlurmClientError("NO_ATTACHED_WORK", "initial job is terminal; use a fresh submission key");
      await this.stage(j);
      const confirmed = await this.api.getJob(j.job_id);
      if (confirmed.worker_id !== j.worker_id || terminalJob(confirmed)) throw new SlurmClientError("NO_ATTACHED_WORK", "attached job no longer nonterminal; allocation was not submitted");
      const allocation = await this.api.getWorker(j.worker_id);
      if (allocation.status === "draining" || allocation.slurm?.cancel_requested || allocation.slurm?.finished_at || allocation.slurm?.submission_failed) throw new SlurmClientError("ALLOCATION_ADMISSION_CLOSED", "allocation was cancelled or finished before submission; no sbatch was invoked");
      j.submission_attempted = true;
      await this.save(j);
      await this.api.observeSlurmWorker(j.worker_id, { submission_attempted: true, observed_at: Date.now() / 1000 });
      const result = JSON.parse(await this.transport(p, `set -e\n${p.runtime_setup}\n` + py(SUBMIT), JSON.stringify({ run: j.host_run_path, sbatch: preflight.sbatch, args: this.submissionArgs(j), cluster: preflight.cluster }), { timeout_ms: 120_000 }));
      await this.acceptSubmission(j, result);
      return this.response(j);
    } catch (error) {
      if (error instanceof SlurmClientError) {
        if (error.submission_key === key) throw error;
        throw new SlurmClientError(error.code, error.message.replace(/^\w+: /, ""), key, error.evidence);
      }
      throw new SlurmClientError("SLURM_ENQUEUE_FAILED", error instanceof Error ? error.message : String(error), key);
    } finally { await lock.close(); }
  }
  private async acceptSubmission(j: Journal, result: { exit_code?: number; slurm_job_id?: string; stdout?: string; stderr?: string }, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    if (result.exit_code !== 0 && result.exit_code !== undefined) {
      // Transport/controller timeouts and signal deaths can happen after Slurm committed.
      // Only explicit scheduler policy/validation refusals prove no allocation exists.
      if (result.exit_code < 1 || !/invalid (?:account|partition|qos|constraint|feature|time|node|generic resource)|requested (?:node configuration|time limit|partition configuration) is not available|job violates accounting[/]qos policy|access denied|permission denied/i.test(result.stderr ?? "")) throw new SlurmClientError("SUBMISSION_UNCERTAIN", "sbatch failed without a definite refusal; recover this key, never resubmit", j.submission_key, result);
      j.submission_failed = true; await this.save(j);
      this.namespace(j.context);
      await this.api.observeSlurmWorker(j.worker_id!, { submission_failed: true, observed_at: Date.now() / 1000, scheduler_reason: (result.stderr || "sbatch refused request").slice(0, 4096) }, signal);
      j.terminal = true; await this.save(j);
      throw new SlurmClientError("SBATCH_REFUSED", result.stderr || "sbatch refused request", j.submission_key);
    }
    if (!result.slurm_job_id || !/^\d+$/.test(result.slurm_job_id)) throw new SlurmClientError("SUBMISSION_UNCERTAIN", "scheduler reply has no verified local numeric ID; inspect get_worker evidence and retry this key", j.submission_key, result);
    if (j.slurm_job_id && j.slurm_job_id !== result.slurm_job_id) throw new SlurmClientError("SCHEDULER_ID_CONFLICT", "scheduler identity cannot change", j.submission_key);
    j.slurm_job_id = result.slurm_job_id; await this.save(j);
    this.namespace(j.context);
    await this.api.observeSlurmWorker(j.worker_id!, { slurm_job_id: j.slurm_job_id, submission_attempted: true, observed_at: Date.now() / 1000 }, signal);
  }
  private matches(j: Journal, r: SchedulerRecord): boolean { return r.user === j.scheduler_user && r.name === `whc-${j.worker_id}` && r.comment === `wh:${j.worker_id}` && /^\d+$/.test(r.id); }
  private async schedulerRecords(p: ClusterProfile, journals: Journal[], signal?: AbortSignal): Promise<SchedulerRecord[]> {
    if (!journals.length) return [];
    const user = journals[0].scheduler_user;
    if (!user || journals.some((j) => j.scheduler_user !== user)) throw new SlurmClientError("SCHEDULER_USER_UNKNOWN", "cannot inspect scheduler without recorded local user");
    return JSON.parse(await this.transport(p, `set -e\n${p.runtime_setup}\n` + py(String.raw`
# wh:scheduler-batch
import json,subprocess,sys
p=json.load(sys.stdin)
def run(args):
 r=subprocess.run(args,text=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=30)
 if r.returncode: raise RuntimeError(r.stderr[-4096:])
 return r.stdout
rows=[]
for line in run(['squeue','--noheader','--user='+p['user'],'--format=%i|%100j|%100k|%100u|%T|%R']).splitlines():
 v=line.split('|',5)
 if len(v)==6: rows.append(dict(zip(['id','name','comment','user','state','reason'],[s.strip() for s in v])))
args=['sacct','--noheader','--parsable2','--user='+p['user'],'--starttime='+p['since'],'--format=JobIDRaw,JobName%100,Comment%100,User%100,State%40,Reason%200']
if p['ids']: args.append('--jobs='+','.join(p['ids']))
else: args.append('--name='+','.join(p['names']))
for line in run(args).splitlines():
 v=line.split('|')
 if len(v)>=6: rows.append(dict(zip(['id','name','comment','user','state','reason'],[s.strip() for s in v[:6]])))
print(json.dumps(rows))
`), JSON.stringify({ user, ids: journals.every((j) => j.slurm_job_id) ? journals.map((j) => j.slurm_job_id) : [], names: journals.map((j) => `whc-${j.worker_id}`), since: new Date((Math.min(...journals.map((j) => j.created_at)) - 86400) * 1000).toISOString().slice(0, 19) }), { max_bytes: 2 * 1024 * 1024, signal }));
  }
  private async recover(j: Journal, candidate?: string, signal?: AbortSignal): Promise<void> {
    const submitted = await this.transport(j.profile, py(String.raw`
# wh:read-submitted
import json,os,sys
path=os.path.join(sys.argv[1],'submitted.json')
if os.path.isfile(path):
 with open(path) as f: print(f.read())
else: print('null')
`, [j.host_run_path!]), undefined, { signal });
    const receipt = JSON.parse(submitted);
    if (receipt?.stdout && /^\d+;/.test(receipt.stdout.trim()) && receipt.stdout.trim().split(";")[1] !== j.scheduler_cluster) throw new SlurmClientError("SUBMISSION_UNCERTAIN", "unexpected federation suffix; no local scheduler identity was bound", j.submission_key, receipt);
    if (receipt?.slurm_job_id) {
      if (candidate && receipt.slurm_job_id !== candidate) throw new SlurmClientError("CANDIDATE_MISMATCH", "candidate conflicts with durable scheduler receipt", j.submission_key);
      await this.acceptSubmission(j, receipt, signal);
      return;
    }
    if (receipt && receipt.exit_code !== 0) {
      try { await this.acceptSubmission(j, receipt, signal); }
      catch (error) { if (!(error instanceof SlurmClientError) || error.code !== "SUBMISSION_UNCERTAIN") throw error; }
    }
    signal?.throwIfAborted();
    const rows = (await this.schedulerRecords(j.profile, [j], signal)).filter((r) => this.matches(j, r));
    const unique = [...new Map(rows.map((r) => [r.id, r])).values()];
    j.candidates = unique; await this.save(j);
    if (candidate && (!/^\d+$/.test(candidate) || !unique.some((r) => r.id === candidate))) throw new SlurmClientError("CANDIDATE_MISMATCH", "candidate is not an exact user/name/comment match", j.submission_key, unique);
    const chosen = candidate ? unique.find((r) => r.id === candidate) : unique.length === 1 ? unique[0] : undefined;
    if (!chosen) throw new SlurmClientError("SUBMISSION_UNCERTAIN", unique.length ? "multiple matching jobs; choose candidate_job_id from get_worker evidence" : "no matching job after attempt; never automatically resubmit", j.submission_key, unique);
    await this.acceptSubmission(j, { exit_code: 0, slurm_job_id: chosen.id }, signal);
  }
  private async recoveredResults(journals: Journal[], signal?: AbortSignal): Promise<Record<string, Array<{ job_id: string; result: RunnerResult }>>> {
    if (!journals.length) return {};
    return JSON.parse(await this.transport(journals[0].profile, py(String.raw`
# wh:recover-results
import glob,json,os,sys
p=json.load(sys.stdin); out={}
for worker,run in p.items():
 out[worker]=[]
 for path in glob.glob(os.path.join(run,'runtime','harness','*','result.json'))[:4096]:
  if os.path.getsize(path)>16384: raise ValueError('oversized runner result')
  with open(path) as f: result=json.load(f)
  out[worker].append({'job_id':os.path.basename(os.path.dirname(path)),'result':result})
print(json.dumps(out))
`), JSON.stringify(Object.fromEntries(journals.map((j) => [j.worker_id, j.host_run_path]))), { max_bytes: 4 * 1024 * 1024, signal }));
  }
  async pollOnce(context: SlurmContext, signal?: AbortSignal): Promise<void> {
    signal?.throwIfAborted();
    const pending = (await this.journals(context)).filter((j) => j.submission_attempted && !j.terminal);
    // Restarting a client may recover a committed scheduler identity, never submit.
    for (const j of pending.filter((entry) => !entry.slurm_job_id)) {
      try {
        const lock = await this.submissionLock(this.journalPath(context, j.submission_key), j.submission_key);
        try { signal?.throwIfAborted(); await this.recover(j, undefined, signal); } finally { await lock.close(); }
      } catch {
        // Unknown submissions and SSH/control outages cannot block other allocations.
        // The next tick replays observations only; this path never invokes sbatch.
      }
    }
    const entries = pending.filter((j) => j.slurm_job_id && !j.terminal);
    const groups = new Map<string, Journal[]>();
    for (const j of entries) { const key = sha256(canonical(j.profile)); groups.set(key, [...(groups.get(key) ?? []), j]); }
    await Promise.all([...groups.values()].map(async (journals) => {
      signal?.throwIfAborted();
      const rows = await this.schedulerRecords(journals[0].profile, journals, signal);
      // If shared results cannot be read, do not apply terminal scheduler evidence first.
      const results = await this.recoveredResults(journals, signal);
      for (const j of journals) {
        signal?.throwIfAborted();
        const matching = rows.filter((r) => r.id === j.slurm_job_id && this.matches(j, r));
        const row = matching.find((r) => terminalState(r.state)) ?? matching[0];
        const observation: SlurmObservation = { slurm_job_id: j.slurm_job_id, submission_attempted: true, observed_at: Date.now() / 1000, recovered_results: results[j.worker_id!] ?? [] };
        if (row) { observation.scheduler_state = row.state; observation.scheduler_reason = row.reason; }
        this.namespace(context);
        await this.api.observeSlurmWorker(j.worker_id!, observation, signal);
        if (row && terminalState(row.state)) { j.terminal = true; await this.save(j); }
      }
    }));
  }
  startSlurmPolling(context: SlurmContext): () => void {
    let stopped = false, timer: NodeJS.Timeout | undefined;
    const controller = new AbortController();
    const url = this.api.getOrchestratorUrl();
    let credentials: string | undefined;
    const tick = async () => {
      try {
        const current = canonical(await this.api.getAuthorizationHeaders());
        if (stopped || url !== this.api.getOrchestratorUrl() || (credentials !== undefined && credentials !== current)) { stopped = true; controller.abort(); return; }
        credentials = current;
        await this.pollOnce(context, controller.signal);
      } catch { /* Local connection/accounting outages are not terminal evidence. */ }
      if (!stopped) {
        let seconds = 15;
        try {
          const intervals = (await this.journals(context)).filter((j) => !j.terminal).map((j) => j.profile.poll_seconds);
          if (intervals.length) seconds = Math.min(...intervals);
        } catch { /* Retry without resubmitting. */ }
        timer = setTimeout(tick, Math.max(1, seconds) * 1000);
        timer.unref?.();
      }
    };
    void tick();
    return () => { stopped = true; controller.abort(); clearTimeout(timer); };
  }
  private async jobJournal(jobId: string, context: SlurmContext): Promise<{ job: Job; journal?: Journal }> {
    const job = await this.api.getJob(jobId);
    return { job, journal: (await this.journals(context)).find((j) => j.worker_id === job.worker_id) };
  }
  async readClusterJobLogs(jobId: string, opts: { tail?: number; head?: number } = {}, context?: SlurmContext): Promise<JobLogsResult | null> {
    if (!context) return null;
    const { job, journal: j } = await this.jobJournal(jobId, context);
    if (!j || !j.host_run_path) return null;
    try {
      const worker = await this.api.getWorker(job.worker_id!);
      if (worker.status === "online" || worker.status === "draining") return null;
    } catch { /* Retained shared logs remain readable after transport loss. */ }
    const bound = Math.max(1, Math.min(5000, opts.head ?? opts.tail ?? 200));
    const logs = await this.transport(j.profile, py(String.raw`
# wh:shared-logs
import collections,os,sys
run,job,limit,head=sys.argv[1:]; limit=int(limit)
paths=[os.path.join(run,'runtime','harness',job,'output.log'),os.path.join(run,'batch.out'),os.path.join(run,'batch.err')]
for path in paths:
 if not os.path.isfile(path): continue
 print('\n== '+os.path.basename(path)+' ==')
 with open(path,'rb') as f:
  if head=='1': lines=[]; remaining=limit
  else:
   f.seek(max(0,os.path.getsize(path)-262144)); lines=list(collections.deque(f,maxlen=limit)); remaining=0
  budget=262144
  while remaining and budget:
   line=f.readline(min(65536,budget))
   if not line: break
   lines.append(line); remaining-=1; budget-=len(line)
  sys.stdout.write(b''.join(lines)[:262144].decode('utf-8','replace'))
`, [j.host_run_path, jobId, String(bound), opts.head !== undefined ? "1" : "0"]), undefined, { max_bytes: 1024 * 1024 });
    return { job_id: jobId, logs, tail: opts.head === undefined ? bound : null, head: opts.head !== undefined ? bound : null };
  }
  async stopClusterJob(jobId: string, options: { scope?: "job" | "allocation"; candidate_job_id?: string } = {}, context: SlurmContext): Promise<Record<string, unknown>> {
    const { job, journal: j } = await this.jobJournal(jobId, context);
    if (!job.worker_id) throw new SlurmClientError("JOB_WORKER_UNKNOWN", "job has no worker");
    const worker = await this.api.getWorker(job.worker_id);
    if (!worker.slurm) {
      if (options.scope === "allocation") throw new SlurmClientError("NOT_SLURM_ALLOCATION", "allocation scope requires a Slurm worker");
      return { ...await this.api.stopJob(jobId) };
    }
    if (!j) throw new SlurmClientError("LOCAL_ALLOCATION_STATE_MISSING", "this client session has no verified local scheduler identity");
    const lock = await this.submissionLock(this.journalPath(context, j.submission_key), j.submission_key);
    try {
      // Re-read after acquiring the same crash-safe lock used by enqueue.
      const current = JSON.parse(await readFile(this.journalPath(context, j.submission_key), "utf8")) as Journal;
      let cancel = options.scope === "allocation";
      let stopped: unknown;
      const wasPending = job.status === "pending";
      if (!cancel) {
        stopped = await this.api.stopJob(jobId);
        const remaining = (await this.api.listJobs({ worker_id: job.worker_id })).filter((other) => other.id !== jobId && other.queue_managed && !terminalJob(other));
        cancel = wasPending && !worker.slurm.started_at && !remaining.length;
        if (!cancel) return { ...(stopped as object), scope: "job" };
      }
      // Empty-queue cancellation and admission are checked atomically by the server.
      // A job admitted after our read wins: the guarded observation then stays false.
      const accepted = await this.api.observeSlurmWorker(job.worker_id, { cancel_requested: true, ...(options.scope === "allocation" ? {} : { cancel_if_empty: true }), observed_at: Date.now() / 1000 });
      if (!accepted.slurm?.cancel_requested) return { ...(stopped as object), scope: options.scope ?? "job", requested: false, allocation_cancel_requested: false };
      if (!current.submission_attempted) return { job_id: jobId, worker_id: job.worker_id, requested: true, scope: options.scope ?? "job", scheduler_id: null, submission_key: current.submission_key };
      await this.recover(current, options.candidate_job_id);
      const rows = (await this.schedulerRecords(current.profile, [current])).filter((r) => r.id === current.slurm_job_id && this.matches(current, r));
      if (!rows.length) throw new SlurmClientError("CANCELLATION_UNVERIFIED", "scheduler identity is not currently verifiable; cancellation not sent", current.submission_key);
      if (!rows.some((r) => terminalState(r.state))) await this.transport(current.profile, `set -e\n${current.profile.runtime_setup}\nscancel -- ${shellQuote(current.slurm_job_id!)}`);
      return { job_id: jobId, worker_id: job.worker_id, requested: true, scope: options.scope ?? "job", scheduler_id: current.slurm_job_id, submission_key: current.submission_key };
    } finally { await lock.close(); }
  }
}

const client = new SlurmClient();
export const createSlurmContext = (kind: string, session: string) => client.createContext(kind, session);
export const listClusterTargets = (context?: SlurmContext) => client.listClusterTargets(context);
export const getClusterTarget = (id: string, context?: SlurmContext) => client.getClusterTarget(id, context);
export const enqueueCluster = (profile: ClusterProfile | string, input: ClusterEnqueueInput, context: SlurmContext) => client.enqueueCluster(profile, input, context);
export const readClusterJobLogs = (job: string, opts?: { tail?: number; head?: number }, context?: SlurmContext) => client.readClusterJobLogs(job, opts, context);
export const stopClusterJob = (job: string, opts: { scope?: "job" | "allocation"; candidate_job_id?: string }, context: SlurmContext) => client.stopClusterJob(job, opts, context);
export const startSlurmPolling = (context: SlurmContext) => client.startSlurmPolling(context);
