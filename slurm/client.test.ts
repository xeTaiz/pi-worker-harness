import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SlurmClient, SlurmClientError, shellQuote, sshTransport, verifySharedMappings, workloadCommand, type ClientApi, type ClusterEnqueueInput, type SlurmTransport } from "./client.ts";
import { DEFAULT_ALLOCATION_WALLTIME_SECONDS, sharedRoots, type ClusterProfile } from "./profiles.ts";
import type { Job, QueuedJob, SlurmObservation, SlurmReservationRequest, Worker, WorkerDetail } from "../types.ts";

const directories: string[] = [];
afterEach(async () => { await Promise.all(directories.splice(0).map((path) => rm(path, { recursive: true, force: true }))); });

/** Run the actual emitted scripts locally; point WH_TEST_PYTHON at Python 3.9 for compatibility coverage. */
async function runRemoteScript(script: string, stdin = "", env?: NodeJS.ProcessEnv) {
  const python = process.env.WH_TEST_PYTHON;
  const command = python ? script.replace(/\bpython3 -c /g, `${shellQuote(python)} -c `) : script;
  const child = Bun.spawn(["bash", "-c", command], { stdin: new Blob([stdin]), stdout: "pipe", stderr: "pipe", env });
  const [stdout, stderr, code] = await Promise.all([new Response(child.stdout).text(), new Response(child.stderr).text(), child.exited]);
  if (code) throw new Error(stderr);
  return stdout;
}

/** Instrument Python's OS/clock boundaries, not the generated program or its output. */
async function probePythonShim(root: string, mode: "clock" | "immediate" | "permission" | "stat-permission" | "publish" | "observe", workerRoot?: string) {
  const directory = join(root, "python-shim");
  const events = join(directory, "events");
  await mkdir(directory);
  await writeFile(join(directory, "sitecustomize.py"), String.raw`
import builtins,os,sys,time
original_open=builtins.open
original_sleep=time.sleep
original_lstat=os.lstat
elapsed=0.0
def record(event):
 with original_open(${JSON.stringify(events)},'a') as handle: handle.write(event+'\n')
def sleep(seconds):
 global elapsed
 record('sleep:'+str(seconds))
 if ${JSON.stringify(mode)}=='observe':
  sys.stderr.write('waiting-for-probe\n'); sys.stderr.flush()
  return original_sleep(seconds)
 if ${JSON.stringify(mode)} not in ('clock','publish'): raise AssertionError('nontransient probe failure must not sleep')
 elapsed+=seconds
 if ${JSON.stringify(mode)}=='publish':
  with original_open(os.path.join(${JSON.stringify(workerRoot ?? "")},os.path.basename(sys.argv[2])),'rb') as source, original_open(sys.argv[2],'wb') as dest: dest.write(source.read())
  record('published')
time.sleep=sleep
if ${JSON.stringify(mode)}!='observe': time.monotonic=lambda: elapsed
def lstat_probe(path,*args,**kwargs):
 if ${JSON.stringify(mode)}=='stat-permission' and os.path.basename(str(path)).startswith('.wh-probe-'): raise PermissionError('probe stat denied by filesystem')
 return original_lstat(path,*args,**kwargs)
os.lstat=lstat_probe
def open_probe(path,*args,**kwargs):
 if os.path.basename(str(path)).startswith('.wh-probe-'):
  record('read')
  if ${JSON.stringify(mode)}=='permission': raise PermissionError('probe read denied by filesystem')
 return original_open(path,*args,**kwargs)
builtins.open=open_probe
record('started')
`);
  return { events, env: { ...process.env, PYTHONPATH: directory + (process.env.PYTHONPATH ? ":" + process.env.PYTHONPATH : "") } };
}

async function fixture(kind: "gpu" | "cpu" = "gpu") {
  const root = await mkdtemp(join(tmpdir(), "wh-client-test-"));
  directories.push(root);
  await writeFile(join(root, "tailnet.key"), "ephemeral-secret");
  const profile: ClusterProfile = {
    id: "test-" + kind, description: "test policy", ssh_host: "test-alias", storage_id: "shared", shared_root: join(root, "shared"), sif_path: join(root, "worker.sif"), runtime: "apptainer", runtime_setup: "export SITE_TEST=site", account: "test", partition: "compute", max_walltime_seconds: 3600, failure_grace_seconds: 120, warning_seconds: 30, termination_grace_seconds: 5, tailnet_key_file: join(root, "tailnet.key"), tailnet_login_server: "https://tailnet.test", orchestrator_host: "127.0.0.1", orchestrator_registration_port: 12888, poll_seconds: 2,
    ...(kind === "gpu" ? { compute_kind: "gpu" as const, gpu_type: "a100", max_gpus: 8 } : { compute_kind: "cpu" as const, max_gpus: 0 as const }),
  };
  await writeFile(join(root, "clusters.json"), JSON.stringify({ version: 1, clusters: [profile] }));
  const env = { HOME: root, WH_CLUSTER_STATE_DIR: join(root, "state"), WH_CLUSTER_CONFIG: join(root, "clusters.json") };
  const calls: string[] = [];
  const observations: SlurmObservation[] = [];
  const reservations: SlurmReservationRequest[] = [];
  const queued: Parameters<ClientApi["enqueueJob"]>[0][] = [];
  const jobs = new Map<string, Job>();
  const staged: Record<string, string>[] = [];
  const submissions: Array<{ args: string[]; run: string; sbatch: string }> = [];
  const state = {
    receipt: null as null | Record<string, unknown>, rows: [] as Array<Record<string, string>>, probe: "", unknown: false, loseQueueReply: false, loseSubmitReply: false,
    workerStatus: "offline", started: false, cancelled: false, resultFailure: false, cancelGuardWon: true,
    recovered: {} as Record<string, unknown[]>, submissionResult: { exit_code: 0, slurm_job_id: "123", stdout: "123;local", stderr: "" } as Record<string, unknown>,
  };
  const workerId = "allocation-worker-1";
  const fakeApi: Partial<ClientApi> = {
    getOrchestratorUrl: () => "http://control.test:12889",
    getAuthorizationHeaders: async () => ({ Authorization: "Bearer operator-test" }),
    listDataPaths: async () => ({ "/data/shared/shared": [{ worker_id: "ordinary-worker", worker_name: "ordinary" }] }),
    startJob: async (input) => {
      calls.push("probe");
      const match = input.command.match(/printf '%s' '([a-f0-9]+)'/);
      if (match) state.probe = match[1]!;
      return { id: "probe-job", worker_id: input.worker_id, status: "done", exit_code: 0 } as Job;
    },
    reserveWorker: async (input) => { calls.push("reserve"); reservations.push(input); return { worker_id: workerId }; },
    enqueueJob: async (input) => {
      calls.push("queue"); queued.push(input);
      const job = jobs.get("initial-job-1") ?? { id: "initial-job-1", worker_id: workerId, command: input.command, queue_managed: true, status: "pending", gpu_count: input.gpu_count, name: input.name } as Job;
      jobs.set(job.id, job);
      if (state.loseQueueReply) { state.loseQueueReply = false; throw new Error("lost queue reply"); }
      return job;
    },
    getJob: async (id) => { const job = jobs.get(id); if (!job) throw new Error("unknown job"); return job; },
    getWorker: async () => ({ id: workerId, status: state.workerStatus, slurm: { cluster_id: profile.id, started_at: state.started ? 123 : null } }) as WorkerDetail,
    listJobs: async () => [...jobs.values()],
    listQueue: async () => [...jobs.values()].filter((j) => j.status === "pending") as QueuedJob[],
    observeSlurmWorker: async (_id, input) => { calls.push("observe"); observations.push(input); return { id: workerId, slurm: { cancel_requested: !!input.cancel_requested && (!input.cancel_if_empty || state.cancelGuardWon) } } as Worker; },
    stopJob: async (id) => { calls.push("stop-job"); const job = jobs.get(id)!; job.status = "failed"; return { job_id: id, stopped: true, already_terminal: false, status: "failed" }; },
    getJobLogs: async (id) => ({ job_id: id, tail: 200, head: null, logs: "live logs" }),
  };
  const transport: SlurmTransport = async (_p, script, stdin) => {
    if (script.includes("# wh:inventory")) {
      calls.push("inventory");
      return JSON.stringify({ sinfo: "NodeName=n1 Partition=compute State=ALLOCATED", nodes: `NodeName=n1 Partitions=compute CPUEfctv=64 CPUTot=64 RealMemory=524288 MemSpecLimit=0 Gres=${kind === "gpu" ? "gpu:a100:8" : "(null)"} State=ALLOCATED AvailableFeatures=fast CfgTRES=cpu=64,mem=524288M${kind === "gpu" ? ",gres/gpu=8" : ""}`, partition: "PartitionName=compute MaxTime=01:00:00 MaxCPUsPerNode=UNLIMITED MaxMemPerNode=UNLIMITED MaxMemPerCPU=UNLIMITED", ginfo: "" });
    }
    if (script.includes("# wh:path-preflight")) {
      calls.push("paths");
      return JSON.stringify({ project_path: "/data/shared/shared/projects/project", environment_path: "/data/shared/shared/projects/project/.venvs/test", image_sha256: "image-hash", lock_sha256: "lock-hash" });
    }
    if (script.includes("# wh:shared-probe")) return state.probe;
    if (script.includes("# wh:sbatch-preflight")) return JSON.stringify({ sbatch: join(root, "sbatch"), user: "tester", cluster: "local" });
    if (script.includes("# wh:stage")) { calls.push("stage"); staged.push(JSON.parse(stdin!).files); return '{"staged":true}'; }
    if (script.includes("# wh:submit-fenced")) {
      calls.push("submit"); submissions.push(JSON.parse(stdin!));
      if (!state.unknown) state.receipt = state.submissionResult;
      if (state.loseSubmitReply) { state.loseSubmitReply = false; throw new Error("SSH disconnected after sbatch"); }
      return JSON.stringify(state.submissionResult);
    }
    if (script.includes("# wh:read-submitted")) { calls.push("receipt"); return JSON.stringify(state.receipt); }
    if (script.includes("# wh:scheduler-batch")) { calls.push("scheduler"); return JSON.stringify(state.rows); }
    if (script.includes("# wh:recover-results")) { calls.push("results"); if (state.resultFailure) throw new Error("shared read unavailable"); return JSON.stringify(state.recovered); }
    if (script.includes("# wh:shared-logs")) { calls.push("logs"); expect(script).toContain("'harness'"); return "retained shared logs"; }
    if (script.includes("scancel --")) { calls.push("scancel"); state.cancelled = true; return ""; }
    throw new Error("unexpected fake SSH boundary: " + script.slice(0, 200));
  };
  const client = new SlurmClient({ api: fakeApi, env, transport });
  const context = await client.createContext("omp", "session-one");
  const input: ClusterEnqueueInput = { command: "python train.py --name 'original'", name: "experiment", expected_seconds: 60, submission_key: "stable-key", slurm: { project_path: "/data/shared/shared/projects/project", environment_path: "/data/shared/shared/projects/project/.venvs/test", walltime_seconds: 600 } };
  function row(id = "123", schedulerState = "PENDING") { return { id, name: `whc-${workerId}`, comment: `wh:${workerId}`, user: "tester", state: schedulerState, reason: "Priority" }; }
  return { root, env, profile, client, context, input, calls, observations, reservations, queued, staged, submissions, state, jobs, workerId, row, fakeApi, transport };
}

async function savedJournals(root: string) {
  const files = await readdir(join(root, "state"), { recursive: true });
  return Promise.all(files.filter((name) => name.includes("submissions/") && name.endsWith(".json")).map(async (name) => ({ path: join(root, "state", name), value: JSON.parse(await readFile(join(root, "state", name), "utf8")) })));
}

describe("finite local Slurm client", () => {
  test("truthful discovery does not poll every configured cluster", async () => {
    const f = await fixture();
    const targets = await f.client.listClusterTargets();
    expect(f.calls).toEqual([]);
    expect(targets[0]).toMatchObject({ kind: "cluster", id: "cluster:test-gpu", availability: { status: "unknown" } });
    expect(targets[0]).not.toHaveProperty("status");
    expect(targets[0]).not.toHaveProperty("worker_ip");
    const fresh = await f.client.getClusterTarget(JSON.stringify("cluster:test-gpu"));
    expect(fresh.availability.status).toBe("fresh");
    expect(f.calls).toEqual(["inventory"]);
  });
  test("journal identity exists before network and reserve/queue precede staging and single sbatch", async () => {
    const f = await fixture();
    const transport: SlurmTransport = async (...args) => {
      const [{ value, path }] = await savedJournals(f.root);
      expect(value.registration_token).toMatch(/^[a-f0-9]{64}$/);
      expect(value.submission_key).toBe("stable-key");
      expect((await stat(path)).mode & 0o777).toBe(0o600);
      expect((await stat(join(path, ".."))).mode & 0o777).toBe(0o700);
      return f.transport(...args);
    };
    const client = new SlurmClient({ api: f.fakeApi, env: f.env, transport });
    const result = await client.enqueueCluster(f.profile, f.input, f.context);
    expect(result).toMatchObject({ worker_id: f.workerId, job_id: "initial-job-1", slurm_job_id: "123", submission_key: "stable-key" });
    expect(f.calls.indexOf("queue")).toBeGreaterThan(f.calls.indexOf("reserve"));
    expect(f.calls.indexOf("stage")).toBeGreaterThan(f.calls.indexOf("queue"));
    expect(f.calls.indexOf("submit")).toBeGreaterThan(f.calls.indexOf("stage"));
    const flags = f.submissions[0]!.args;
    expect(flags).toContain("--nodes=1"); expect(flags).toContain("--ntasks=1");
    expect(flags).toContain("--cpus-per-task=8"); expect(flags).toContain("--mem=65536M");
    expect(flags).toContain("--gpus-per-node=a100:1"); expect(flags).toContain("--export=NIL"); expect(flags).toContain("--no-requeue");
    expect(flags).toContain("--account=test");
    expect(flags).not.toContain("--exclusive");
    expect(f.queued[0]!.command).toBe(workloadCommand("/data/shared/shared/projects/project", "/data/shared/shared/projects/project/.venvs/test", f.input.command));
    expect(f.queued[0]!.submission_key).toBe(f.reservations[0]!.request_id);
    expect(f.staged[0]!["worker.token"]).toBe((await savedJournals(f.root))[0]!.value.registration_token);
    const settings = JSON.parse(f.staged[0]!["settings.json"]!);
    expect(settings).toMatchObject({ version: 1, compute_kind: "gpu", cpus: 8, memory_mb: 65536 });
    expect(settings.worker_name).toBe(f.reservations[0]!.name);
    expect(settings).not.toHaveProperty("registration_token");
    expect(settings).not.toHaveProperty("additional_shared_roots");
    const retried = await client.enqueueCluster(f.profile, f.input, f.context);
    expect(retried.slurm_job_id).toBe("123"); expect(f.submissions).toHaveLength(1);
    await expect(client.enqueueCluster(f.profile, { ...f.input, command: "changed" }, f.context)).rejects.toMatchObject({ code: "SUBMISSION_KEY_CONFLICT", submission_key: "stable-key" });
  });
  test("CPU defaults zero GPUs and ordinary queue uses persisted wrapper", async () => {
    const f = await fixture("cpu");
    await f.client.enqueueCluster(f.profile, { ...f.input, slurm: { project_path: f.input.slurm.project_path, walltime_seconds: 600 } }, f.context);
    expect(f.queued[0]!.gpu_count).toBe(0);
    expect(f.submissions[0]!.args).toContain("--cpus-per-task=1");
    expect(f.submissions[0]!.args).toContain("--mem=4096M");
    expect(f.submissions[0]!.args.some((flag) => flag.startsWith("--gpus"))).toBe(false);
  });
  test("lost queue reply repeats the same ordinary idempotency key, never duplicate sbatch", async () => {
    const f = await fixture(); f.state.loseQueueReply = true;
    await expect(f.client.enqueueCluster(f.profile, f.input, f.context)).rejects.toMatchObject({ submission_key: "stable-key" });
    expect(f.submissions).toHaveLength(0);
    await f.client.enqueueCluster(f.profile, f.input, f.context);
    expect(f.queued).toHaveLength(2);
    expect(f.queued[0]).toEqual(f.queued[1]);
    expect(f.reservations[0]).toEqual(f.reservations[1]);
    expect(f.jobs.size).toBe(1); expect(f.submissions).toHaveLength(1);
  });
  test("lost sbatch response recovers receipt without another submit", async () => {
    const f = await fixture(); f.state.loseSubmitReply = true;
    await expect(f.client.enqueueCluster(f.profile, f.input, f.context)).rejects.toMatchObject({ submission_key: "stable-key" });
    const result = await f.client.enqueueCluster(f.profile, f.input, f.context);
    expect(result.slurm_job_id).toBe("123"); expect(f.submissions).toHaveLength(1);
  });
  test("an ambiguous sbatch controller timeout recovers exact identity without repeating submission", async () => {
    const f = await fixture();
    f.state.submissionResult = { exit_code: 1, stdout: "", stderr: "sbatch: error: Socket timed out on send/recv operation" };
    await expect(f.client.enqueueCluster(f.profile, f.input, f.context)).rejects.toMatchObject({ code: "SUBMISSION_UNCERTAIN" });
    expect(f.observations.some((observation) => observation.submission_failed)).toBe(false);
    f.state.rows = [f.row()];
    const recovered = await f.client.enqueueCluster(f.profile, f.input, f.context);
    expect(recovered.slurm_job_id).toBe("123");
    expect(f.submissions).toHaveLength(1);
  });
  test("an omitted submission key is persisted and returned even on the first network failure", async () => {
    const f = await fixture();
    const client = new SlurmClient({ api: f.fakeApi, env: f.env, transport: async () => { throw new Error("network unavailable"); } });
    let failure: SlurmClientError | undefined;
    try { await client.enqueueCluster(f.profile, { ...f.input, submission_key: undefined }, f.context); }
    catch (error) { failure = error as SlurmClientError; }
    expect(failure?.submission_key).toBeTruthy();
    expect((await savedJournals(f.root))[0]!.value).toMatchObject({ submission_key: failure!.submission_key, registration_token: expect.stringMatching(/^[a-f0-9]{64}$/) });
    expect(f.reservations).toHaveLength(0);
  });
  test("unknown attempts never resubmit; exact candidates only, read views never bind", async () => {
    const f = await fixture(); f.state.unknown = true; f.state.loseSubmitReply = true;
    await expect(f.client.enqueueCluster(f.profile, f.input, f.context)).rejects.toBeInstanceOf(SlurmClientError);
    await expect(f.client.enqueueCluster(f.profile, f.input, f.context)).rejects.toMatchObject({ code: "SUBMISSION_UNCERTAIN" });
    f.state.rows = [f.row("123"), f.row("124"), { ...f.row("999"), user: "someone-else" }];
    const previous = f.observations.length;
    const evidence = await f.client.getClusterTarget("cluster:test-gpu", f.context);
    expect((evidence.availability.allocations as Array<{ candidates: unknown[] }>)[0]!.candidates).toHaveLength(2);
    expect(f.observations).toHaveLength(previous);
    await expect(f.client.enqueueCluster(f.profile, f.input, f.context)).rejects.toMatchObject({ code: "SUBMISSION_UNCERTAIN" });
    await expect(f.client.enqueueCluster(f.profile, { ...f.input, candidate_job_id: "999" }, f.context)).rejects.toMatchObject({ code: "CANDIDATE_MISMATCH" });
    const selected = await f.client.enqueueCluster(f.profile, { ...f.input, candidate_job_id: "124" }, f.context);
    expect(selected.slurm_job_id).toBe("124"); expect(f.submissions).toHaveLength(1);
  });
  test("federation suffix stays uncertain and policy refusal is terminal", async () => {
    const f = await fixture(); f.state.submissionResult = { exit_code: 0, stdout: "123;elsewhere", stderr: "" };
    await expect(f.client.enqueueCluster(f.profile, f.input, f.context)).rejects.toMatchObject({ code: "SUBMISSION_UNCERTAIN" });
    f.state.rows = [f.row()];
    await expect(f.client.enqueueCluster(f.profile, f.input, f.context)).rejects.toMatchObject({ code: "SUBMISSION_UNCERTAIN" });
    expect(f.submissions).toHaveLength(1);
    const g = await fixture(); g.state.submissionResult = { exit_code: 1, stdout: "", stderr: "sbatch: error: Batch job submission failed: Invalid account or account/partition combination specified" };
    await expect(g.client.enqueueCluster(g.profile, g.input, g.context)).rejects.toMatchObject({ code: "SBATCH_REFUSED" });
    expect(g.observations.at(-1)).toMatchObject({ submission_failed: true });
  });
  test("scheduler terminal observation contains recovered results first and unknown/missing is not terminal", async () => {
    const f = await fixture(); await f.client.enqueueCluster(f.profile, f.input, f.context);
    await f.client.pollOnce(f.context);
    expect(f.observations.at(-1)).not.toHaveProperty("scheduler_state");
    f.state.rows = [f.row("123", "COMPLETED")];
    f.state.resultFailure = true;
    const before = f.observations.length;
    await expect(f.client.pollOnce(f.context)).rejects.toThrow("shared read unavailable");
    expect(f.observations).toHaveLength(before);
    f.state.resultFailure = false;
    const result = { exit_code: 0, descendants_exited: true, finished_at: 123, finished_monotonic: 10, boot_id: "boot" };
    f.state.recovered = { [f.workerId]: [{ job_id: "initial-job-1", result }] };
    await f.client.pollOnce(f.context);
    expect(f.observations.at(-1)).toMatchObject({ scheduler_state: "COMPLETED", recovered_results: [{ job_id: "initial-job-1", result }] });
    const settled = f.observations.length;
    await f.client.pollOnce(f.context); expect(f.observations).toHaveLength(settled);
  });
  test("bounded shared logs before startup, online worker remains on normal streaming path", async () => {
    const f = await fixture(); await f.client.enqueueCluster(f.profile, f.input, f.context);
    expect(await f.client.readClusterJobLogs("initial-job-1", { tail: 12 }, f.context)).toMatchObject({ tail: 12, logs: "retained shared logs" });
    f.state.workerStatus = "online";
    expect(await f.client.readClusterJobLogs("initial-job-1", {}, f.context)).toBeNull();
  });
  test("job cancellation preserves other useful work; last pending job cancels verified allocation", async () => {
    const f = await fixture(); await f.client.enqueueCluster(f.profile, f.input, f.context); f.state.rows = [f.row()];
    f.jobs.set("other", { id: "other", worker_id: f.workerId, status: "pending", queue_managed: true } as Job);
    await f.client.stopClusterJob("initial-job-1", {}, f.context);
    expect(f.state.cancelled).toBe(false);
    f.jobs.delete("other"); f.jobs.get("initial-job-1")!.status = "pending";
    const result = await f.client.stopClusterJob("initial-job-1", {}, f.context);
    expect(result).toMatchObject({ requested: true, scheduler_id: "123" });
    expect(f.state.cancelled).toBe(true);
    expect(f.observations.some((o) => o.cancel_requested)).toBe(true);
  });
  test("a concurrent useful enqueue defeats guarded cancellation, while explicit allocation scope stays unconditional", async () => {
    const f = await fixture(); await f.client.enqueueCluster(f.profile, f.input, f.context); f.state.rows = [f.row()];
    f.state.cancelGuardWon = false;
    const stopped = await f.client.stopClusterJob("initial-job-1", {}, f.context);
    expect(stopped).toMatchObject({ requested: false, allocation_cancel_requested: false });
    expect(f.state.cancelled).toBe(false);
    expect(f.observations.at(-1)).toMatchObject({ cancel_requested: true, cancel_if_empty: true });
    const allocation = await f.client.stopClusterJob("initial-job-1", { scope: "allocation" }, f.context);
    expect(allocation).toMatchObject({ requested: true, scope: "allocation" });
    expect(f.state.cancelled).toBe(true);
    expect(f.observations.at(-2)).not.toHaveProperty("cancel_if_empty");
  });
  test("recipient and journal namespaces isolate URL/client/session; invalid paths fail before reserve", async () => {
    const f = await fixture();
    expect((await f.client.createContext("omp", "session-one")).recipient_id).toBe(f.context.recipient_id);
    expect((await f.client.createContext("omp", "session-two")).recipient_id).not.toBe(f.context.recipient_id);
    expect((await f.client.createContext("mcp", "session-one")).recipient_id).not.toBe(f.context.recipient_id);
    const other = new SlurmClient({ api: { ...f.fakeApi, getOrchestratorUrl: () => "http://other-control.test:12889" }, env: f.env, transport: f.transport });
    expect((await other.createContext("omp", "session-one")).recipient_id).not.toBe(f.context.recipient_id);
    await expect(f.client.enqueueCluster(f.profile, { ...f.input, slurm: { ...f.input.slurm, project_path: "/data/shared/shared/../escape" } }, f.context)).rejects.toMatchObject({ code: "INVALID_SHARED_PATH", submission_key: "stable-key" });
    expect(f.reservations).toHaveLength(0);
    const next = await f.client.createContext("omp", "different-session");
    await expect(f.client.enqueueCluster(f.profile, { ...f.input, submission_key: undefined, candidate_job_id: "123" }, next)).rejects.toMatchObject({ code: "CANDIDATE_REQUIRES_SUBMISSION_KEY" });
  });
});

test("shared path preflight resolves symlinks before persisting cwd and refuses escapes", async () => {
  const f = await fixture();
  await mkdir(join(f.profile.shared_root, "projects", "real"), { recursive: true });
  await mkdir(join(f.root, "outside"), { recursive: true });
  await writeFile(f.profile.sif_path, "base-image");
  await writeFile(join(f.profile.shared_root, "projects", "real", "uv.lock"), "tested-lock");
  await symlink(join(f.profile.shared_root, "projects", "real"), join(f.profile.shared_root, "projects", "project"));
  const transport: SlurmTransport = async (p, script, stdin, opts) => {
    if (!script.includes("# wh:path-preflight")) return f.transport(p, script, stdin, opts);
    return runRemoteScript(script, stdin);
  };
  const client = new SlurmClient({ api: f.fakeApi, env: f.env, transport });
  const input = { ...f.input, slurm: { project_path: f.input.slurm.project_path, walltime_seconds: 600 } };
  await client.enqueueCluster(f.profile, input, f.context);
  expect(f.queued[0]!.command).toBe(workloadCommand("/data/shared/shared/projects/real", undefined, input.command));
  expect((await savedJournals(f.root))[0]!.value.paths).toMatchObject({ image_sha256: expect.stringMatching(/^[a-f0-9]{64}$/), lock_sha256: expect.stringMatching(/^[a-f0-9]{64}$/) });
  await symlink(join(f.root, "outside"), join(f.profile.shared_root, "projects", "escape"));
  const previous = f.reservations.length;
  await expect(client.enqueueCluster(f.profile, { ...input, submission_key: "escape", slurm: { ...input.slurm, project_path: "/data/shared/shared/projects/escape" } }, f.context)).rejects.toThrow("resolved path escapes shared storage");
  expect(f.reservations).toHaveLength(previous);
  await mkdir(join(f.profile.shared_root, "env"));
  await symlink(join(f.root, "outside"), join(f.profile.shared_root, "env", "bin"));
  await expect(client.enqueueCluster(f.profile, { ...input, submission_key: "env-escape", slurm: { ...input.slurm, environment_path: "/data/shared/shared/env" } }, f.context)).rejects.toThrow("resolved path escapes shared storage");
  expect(f.reservations).toHaveLength(previous);
  await symlink(join(f.profile.shared_root, "missing-target"), join(f.profile.shared_root, "projects", "broken"));
  await symlink("cyclic", join(f.profile.shared_root, "projects", "cyclic"));
  for (const name of ["missing", "broken", "cyclic"]) {
    await expect(client.enqueueCluster(f.profile, { ...input, submission_key: name, slurm: { ...input.slurm, project_path: `/data/shared/shared/projects/${name}` } }, f.context)).rejects.toThrow(/FileNotFoundError|RuntimeError|OSError/);
  }
  expect(f.reservations).toHaveLength(previous);
});

test("real mapped project/environment preflight, all identity probes and staging retain primary run storage", async () => {
  const f = await fixture();
  f.profile.additional_shared_roots = [
    { storage_id: "projects", shared_root: join(f.root, "projects-store") },
    { storage_id: "envs", shared_root: join(f.root, "env-store") },
  ];
  const mappings = sharedRoots(f.profile);
  for (const mapping of mappings) await mkdir(mapping.shared_root, { recursive: true });
  await mkdir(join(f.root, "projects-store", "real"));
  await mkdir(join(f.root, "env-store", "venv", "bin"), { recursive: true });
  await symlink(join(f.root, "projects-store", "real"), join(f.root, "projects-store", "alias"));
  await writeFile(f.profile.sif_path, "image");
  await writeFile(join(f.root, "projects-store", "real", "uv.lock"), "lock");
  const executed: string[] = [];
  const run = runRemoteScript;
  const transport: SlurmTransport = async (p, script, stdin, options) => {
    if (["# wh:path-preflight", "# wh:shared-probe", "# wh:stage"].some((marker) => script.includes(marker))) return run(script, stdin);
    return f.transport(p, script, stdin, options);
  };
  const client = new SlurmClient({ api: {
    ...f.fakeApi,
    listDataPaths: async () => Object.fromEntries(mappings.map((mapping) => [`/data/shared/${mapping.storage_id}`, [{ worker_id: "ordinary-worker", worker_name: "ordinary" }]])),
    startJob: async (input) => {
      executed.push(input.command);
      let command = input.command;
      for (const mapping of mappings) command = command.replaceAll(`/data/shared/${mapping.storage_id}/`, `${mapping.shared_root}/`);
      await run(command);
      return { id: "probe-job", worker_id: input.worker_id, status: "done", exit_code: 0 } as Job;
    },
  }, env: f.env, transport });
  const input = { ...f.input, slurm: { project_path: "/data/shared/projects/alias", environment_path: "/data/shared/envs/venv", walltime_seconds: 600 } };
  const result = await client.enqueueCluster(f.profile, input, f.context);
  expect(f.queued[0]!.command).toBe(workloadCommand("/data/shared/projects/real", "/data/shared/envs/venv", input.command));
  expect(result.shared_paths).toMatchObject({ run_path: `/data/shared/shared/.wh-slurm/${f.workerId}` });
  const settings = JSON.parse(await readFile(join(f.profile.shared_root, ".wh-slurm", f.workerId, "settings.json"), "utf8"));
  expect(settings.additional_shared_roots).toEqual(f.profile.additional_shared_roots);
  expect(settings.shared_root).toBe(f.profile.shared_root);
  expect(settings.host_run_path).toBe(join(f.profile.shared_root, ".wh-slurm", f.workerId));
  expect(executed.filter((command) => command.startsWith("umask"))).toHaveLength(3);
  expect(executed.filter((command) => command.startsWith("rm"))).toHaveLength(0);
  for (const mapping of mappings) {
    const probes = (await readdir(mapping.shared_root)).filter((entry) => entry.startsWith(".wh-probe-"));
    expect(probes).toHaveLength(1);
    expect(await readFile(join(mapping.shared_root, probes[0]!), "utf8")).toMatch(/^[a-f0-9]{64}$/);
  }
  await symlink(f.profile.shared_root, join(f.root, "projects-store", "escape"));
  const previous = f.reservations.length;
  await expect(client.enqueueCluster(f.profile, { ...input, submission_key: "extra-project-escape", slurm: { ...input.slurm, project_path: "/data/shared/projects/escape" } }, f.context)).rejects.toThrow("resolved path escapes shared storage");
  await symlink(join(f.root, "projects-store"), join(f.root, "env-store", "escape"));
  await expect(client.enqueueCluster(f.profile, { ...input, submission_key: "extra-env-escape", slurm: { ...input.slurm, environment_path: "/data/shared/envs/escape" } }, f.context)).rejects.toThrow("resolved path escapes shared storage");
  await rm(join(f.root, "env-store", "venv", "bin"), { recursive: true });
  await symlink(f.profile.shared_root, join(f.root, "env-store", "venv", "bin"));
  await expect(client.enqueueCluster(f.profile, { ...input, submission_key: "extra-bin-escape" }, f.context)).rejects.toThrow("resolved path escapes shared storage");
  expect(f.reservations).toHaveLength(previous);
});

for (const unsafe of ["missing", "missing-root", "broken", "cyclic", "escape", "permission", "stat-permission", "mismatch"] as const) {
  test(`actual shared identity probe rejects ${unsafe} paths and retains the worker payload`, async () => {
    const f = await fixture();
    const workerRoot = join(f.root, "ordinary-worker");
    await mkdir(workerRoot);
    if (unsafe !== "missing-root") await mkdir(f.profile.shared_root, { recursive: true });
    const shim = await probePythonShim(f.root, unsafe === "missing" ? "clock" : unsafe === "permission" || unsafe === "stat-permission" ? unsafe : "immediate");
    let transports = 0;
    let workerProbe = "";
    await expect(verifySharedMappings(f.profile, sharedRoots(f.profile), {
      api: {
        listDataPaths: f.fakeApi.listDataPaths!,
        startJob: async (input) => {
          const result = await f.fakeApi.startJob!(input);
          const name = input.command.match(/\.wh-probe-[a-f0-9-]+/)![0];
          workerProbe = join(workerRoot, name);
          await runRemoteScript(input.command.replaceAll("/data/shared/shared", workerRoot));
          const probe = join(f.profile.shared_root, name);
          if (unsafe === "broken") await symlink(join(f.root, "missing-target"), probe);
          else if (unsafe === "cyclic") await symlink(name, probe);
          else if (unsafe === "escape") {
            await writeFile(join(f.root, "outside-probe"), f.state.probe);
            await symlink(join(f.root, "outside-probe"), probe);
          } else if (unsafe === "permission" || unsafe === "stat-permission" || unsafe === "mismatch") {
            await writeFile(probe, unsafe === "mismatch" ? "not-the-worker-token" : f.state.probe);
          }
          return result;
        },
      },
      transport: async (_connection, script, stdin, options) => {
        transports++;
        expect(options).toMatchObject({ timeout_ms: 45_000, max_bytes: 1024 });
        return runRemoteScript(script, stdin, shim.env);
      },
    })).rejects.toMatchObject({
      code: "SHARED_STORAGE_NOT_VERIFIED",
      evidence: expect.stringMatching(unsafe === "missing" ? /shared probe not visible after 30 seconds; possible writeback cache delay/ :
        unsafe === "escape" ? /resolved probe escapes shared storage/ :
        unsafe === "permission" ? /PermissionError: probe read denied by filesystem/ :
        unsafe === "stat-permission" ? /PermissionError: probe stat denied by filesystem/ :
        unsafe === "mismatch" ? /shared storage identity mismatch/ : /FileNotFoundError|RuntimeError|OSError/),
    });
    expect(await readFile(workerProbe, "utf8")).toBe(f.state.probe);
    expect(f.calls).toEqual(["probe"]);
    expect(transports).toBe(1);
    const events = (await readFile(shim.events, "utf8")).trim().split("\n");
    const sleeps = events.filter((event) => event.startsWith("sleep:"));
    expect(sleeps).toHaveLength(unsafe === "missing" ? 120 : 0);
    if (unsafe === "missing") expect(sleeps.every((event) => event === "sleep:0.25")).toBe(true);
    expect(events.filter((event) => event === "read")).toHaveLength(unsafe === "permission" || unsafe === "mismatch" ? 1 : 0);
  });
}

for (const delayed of [false, true]) {
  test(`actual shared probe ${delayed ? "waits for delayed visibility" : "reads immediately without sleeping"} and retains both copies`, async () => {
    const f = await fixture(), workerRoot = join(f.root, "ordinary-worker");
    await mkdir(workerRoot);
    await mkdir(f.profile.shared_root);
    const shim = await probePythonShim(f.root, delayed ? "publish" : "immediate", workerRoot);
    const events: string[] = [];
    let token = "", probeName = "";
    await verifySharedMappings(f.profile, sharedRoots(f.profile), {
      api: {
        listDataPaths: f.fakeApi.listDataPaths!,
        startJob: async (input) => {
          probeName = input.command.match(/\.wh-probe-[a-f0-9-]+/)![0];
          const workerProbe = join(workerRoot, probeName), clusterProbe = join(f.profile.shared_root, probeName);
          await runRemoteScript(input.command.replaceAll("/data/shared/shared", workerRoot));
          token = await readFile(workerProbe, "utf8");
          expect(token).toMatch(/^[a-f0-9]{64}$/);
          events.push("written");
          if (!delayed) await writeFile(clusterProbe, token);
          return { id: "local-probe", worker_id: input.worker_id, status: "done", exit_code: 0 } as Job;
        },
      },
      transport: async (_connection, script, stdin, options) => {
        expect(options).toMatchObject({ timeout_ms: 45_000, max_bytes: 1024 });
        const remote = await runRemoteScript(script, stdin, shim.env);
        expect(remote).toBe(token);
        events.push("read");
        return remote;
      },
    });
    const pythonEvents = (await readFile(shim.events, "utf8")).trim().split("\n");
    expect(pythonEvents).toEqual(delayed ? ["started", "sleep:0.25", "published", "read"] : ["started", "read"]);
    expect(events).toEqual(["written", "read"]);
    expect(await readdir(workerRoot)).toEqual([probeName]);
    expect(await readdir(f.profile.shared_root)).toEqual([probeName]);
    expect(await readFile(join(workerRoot, probeName), "utf8")).toBe(token);
    expect(await readFile(join(f.profile.shared_root, probeName), "utf8")).toBe(token);
  });
}

test("cancelling the actual Python visibility wait retains the ordinary worker payload", async () => {
  const f = await fixture(), controller = new AbortController();
  const workerRoot = join(f.root, "ordinary-worker");
  await mkdir(workerRoot);
  await mkdir(f.profile.shared_root);
  const shim = await probePythonShim(f.root, "observe");
  const reason = new Error("operator cancelled visibility wait");
  let probeName = "", token = "";
  await expect(verifySharedMappings(f.profile, sharedRoots(f.profile), {
    signal: controller.signal,
    api: {
      listDataPaths: f.fakeApi.listDataPaths!,
      startJob: async (input) => {
        probeName = input.command.match(/\.wh-probe-[a-f0-9-]+/)![0];
        await runRemoteScript(input.command.replaceAll("/data/shared/shared", workerRoot));
        token = await readFile(join(workerRoot, probeName), "utf8");
        return { id: "local-probe", worker_id: input.worker_id, status: "done", exit_code: 0 } as Job;
      },
    },
    transport: async (_connection, script, _stdin, options) => {
      expect(options?.signal).toBe(controller.signal);
      const python = process.env.WH_TEST_PYTHON ?? "python3";
      // exec makes the locally simulated SSH child the actual Python waiter.
      const command = script.replace(/\bpython3 -c /, `exec ${shellQuote(python)} -c `);
      const child = Bun.spawn(["bash", "-c", command], { env: shim.env, stdout: "pipe", stderr: "pipe" });
      const abort = () => child.kill();
      options!.signal!.addEventListener("abort", abort, { once: true });
      try {
        const reader = child.stderr.getReader();
        const waiting = await reader.read();
        expect(new TextDecoder().decode(waiting.value)).toBe("waiting-for-probe\n");
        controller.abort(reason);
        await child.exited;
        options!.signal!.throwIfAborted();
        throw new Error("cancelled transport unexpectedly continued");
      } finally {
        options!.signal!.removeEventListener("abort", abort);
        child.kill();
      }
    },
  })).rejects.toBe(reason);
  expect(token).toMatch(/^[a-f0-9]{64}$/);
  expect(await readdir(workerRoot)).toEqual([probeName]);
  expect(await readFile(join(workerRoot, probeName), "utf8")).toBe(token);
  expect(await readdir(f.profile.shared_root)).toEqual([]);
});

test("shared probe SSH failure is not retried and retains the ordinary worker payload", async () => {
  const f = await fixture();
  const workerRoot = join(f.root, "ordinary-worker");
  await mkdir(workerRoot);
  let probeName = "";
  let transports = 0;
  await expect(verifySharedMappings(f.profile, sharedRoots(f.profile), {
    api: {
      listDataPaths: f.fakeApi.listDataPaths!,
      startJob: async (input) => {
        const result = await f.fakeApi.startJob!(input);
        probeName = input.command.match(/\.wh-probe-[a-f0-9-]+/)![0];
        await runRemoteScript(input.command.replaceAll("/data/shared/shared", workerRoot));
        return result;
      },
    },
    transport: async () => { transports++; throw new Error("SSH disconnected"); },
  })).rejects.toMatchObject({ code: "SHARED_STORAGE_NOT_VERIFIED", evidence: expect.stringContaining("SSH disconnected") });
  expect(transports).toBe(1);
  expect(f.calls).toEqual(["probe"]);
  expect(await readdir(workerRoot)).toEqual([probeName]);
  expect(await readFile(join(workerRoot, probeName), "utf8")).toBe(f.state.probe);
});

for (const unsafe of ["missing", "broken", "cyclic"] as const) {
  test(`actual staging refuses a ${unsafe} shared root before creating run assets`, async () => {
    const f = await fixture();
    if (unsafe === "broken") await symlink(join(f.root, "missing-target"), f.profile.shared_root);
    if (unsafe === "cyclic") await symlink("shared", f.profile.shared_root);
    const client = new SlurmClient({ api: f.fakeApi, env: f.env, transport: async (p, script, stdin, options) => {
      if (script.includes("# wh:stage")) return runRemoteScript(script, stdin);
      return f.transport(p, script, stdin, options);
    } });
    await expect(client.enqueueCluster(f.profile, f.input, f.context)).rejects.toThrow(/FileNotFoundError|RuntimeError|OSError/);
    expect(f.submissions).toHaveLength(0);
    expect(await readdir(f.root)).not.toContain(".wh-slurm");
  });
}

test("additional mapping identity mismatch prevents reservation and submission", async () => {
  const f = await fixture();
  f.profile.additional_shared_roots = [{ storage_id: "archive", shared_root: "/different/archive" }];
  const client = new SlurmClient({ api: {
    ...f.fakeApi,
    listDataPaths: async () => ({
      "/data/shared/shared": [{ worker_id: "ordinary-worker", worker_name: "ordinary" }],
      "/data/shared/archive": [{ worker_id: "archive-worker", worker_name: "archive" }],
    }),
  }, env: f.env, transport: async (p, script, stdin, options) => {
    if (script.includes("# wh:shared-probe") && script.includes("/different/archive/")) return "different filesystem";
    return f.transport(p, script, stdin, options);
  } });
  await expect(client.enqueueCluster(f.profile, f.input, f.context)).rejects.toMatchObject({ code: "SHARED_STORAGE_NOT_VERIFIED" });
  expect(f.reservations).toHaveLength(0);
  expect(f.submissions).toHaveLength(0);
});

test("persisted workload wrapper uses safe cwd/PATH and preserves one original command argument", async () => {
  const root = await mkdtemp(join(tmpdir(), "wh-workload-command-")); directories.push(root);
  const project = join(root, "project 'quoted'"), environment = join(root, "env 'quoted'");
  await mkdir(project); await mkdir(join(environment, "bin"), { recursive: true });
  await writeFile(join(project, "module.txt"), "project-local");
  await writeFile(join(environment, "bin", "tested-interpreter"), "#!/bin/sh\nprintf '%s:' \"$PWD\"; cat module.txt\n", { mode: 0o700 });
  const command = workloadCommand(project, environment, "tested-interpreter");
  const result = Bun.spawn(["bash", "-c", command], { cwd: "/", stdout: "pipe", stderr: "pipe" });
  expect(await result.exited).toBe(0); expect(await new Response(result.stdout).text()).toBe(project + ":project-local");
  const missing = Bun.spawn(["bash", "-c", workloadCommand(join(root, "missing"), environment, "touch SHOULD_NOT_EXIST")], { cwd: root, stdout: "pipe", stderr: "pipe" });
  expect(await missing.exited).not.toBe(0);
  expect(await readdir(root)).not.toContain("SHOULD_NOT_EXIST");
});

test("actual fenced remote subprocess strips inherited Slurm options, commits receipt and never repeats sbatch", async () => {
  const f = await fixture();
  delete f.profile.account;
  f.profile.max_walltime_seconds = DEFAULT_ALLOCATION_WALLTIME_SECONDS;
  await mkdir(f.profile.shared_root, { recursive: true });
  const record = join(f.root, "sbatch-record.jsonl");
  const executable = join(f.root, "sbatch");
  await writeFile(executable, `#!/usr/bin/env python3\nimport json,os,sys\nif sys.argv[1:]==['--help']:\n print('--parsable --no-requeue --export --nodes --ntasks --cpus-per-task --mem --comment --signal --time --partition --job-name --output --error --gpus-per-node')\n sys.exit(0)\nwith open(${JSON.stringify(record)},'a') as f: f.write(json.dumps({'args':sys.argv[1:],'env':dict(os.environ)})+'\\n')\nprint('321;local')\n`, { mode: 0o700 });
  await writeFile(join(f.root, "scontrol"), "#!/bin/sh\nprintf 'ClusterName = local\\n'\n", { mode: 0o700 });
  const transport: SlurmTransport = async (p, script, stdin, opts) => {
    if (script.includes("# wh:stage") || script.includes("# wh:submit-fenced") || script.includes("# wh:read-submitted") || script.includes("# wh:sbatch-preflight")) {
      const process = Bun.spawn(["bash", "-c", script], { stdin: new Blob([stdin ?? ""]), stdout: "pipe", stderr: "pipe", env: { ...globalThis.process.env, PATH: `${f.root}:${globalThis.process.env.PATH}`, SBATCH_ARRAY_INX: "0-99", SBATCH_EXCLUSIVE: "yes", SLURM_CLUSTERS: "wrong", SLURM_HINT: "multithread", SLURM_DEBUG_FLAGS: "all", SLURM_EXIT_ERROR: "1", SITE_REQUIRED: "preserved" } });
      const [stdout, stderr, code] = await Promise.all([new Response(process.stdout).text(), new Response(process.stderr).text(), process.exited]);
      if (code) throw new Error(stderr);
      return stdout;
    }
    return f.transport(p, script, stdin, opts);
  };
  const client = new SlurmClient({ api: f.fakeApi, env: f.env, transport });
  const result = await client.enqueueCluster(f.profile, f.input, f.context);
  expect(result.slurm_job_id).toBe("321");
  await client.enqueueCluster(f.profile, f.input, f.context);
  const records = (await readFile(record, "utf8")).trim().split("\n").map((line) => JSON.parse(line));
  expect(records).toHaveLength(1);
  expect(Object.keys(records[0].env).some((key) => key.startsWith("SBATCH_"))).toBe(false);
  for (const key of ["SLURM_CLUSTERS", "SLURM_HINT", "SLURM_DEBUG_FLAGS", "SLURM_EXIT_ERROR"]) expect(records[0].env).not.toHaveProperty(key);
  expect(records[0].env.SLURM_UMASK).toBe("077"); expect(records[0].env.SITE_REQUIRED).toBe("preserved");
  expect(records[0].args.filter((arg: string) => arg.startsWith("--nodes="))).toEqual(["--nodes=1"]);
  expect(records[0].args.some((arg: string) => arg.startsWith("--account"))).toBe(false);
  expect(records[0].args).toContain("--time=0-00:10:00");
});

test("system ssh uses argv transport with strict host checking and no local shell interpretation", async () => {
  const f = await fixture();
  const bin = join(f.root, "bin"); await mkdir(bin);
  await writeFile(join(bin, "ssh"), "#!/usr/bin/env python3\nimport json,sys\nprint(json.dumps({'args':sys.argv[1:],'stdin':sys.stdin.read()}))\n", { mode: 0o700 });
  const previous = process.env.PATH;
  process.env.PATH = bin + ":" + previous;
  try {
    const script = "printf '%s' '$(touch definitely-not-a-local-command)'";
    const result = JSON.parse(await sshTransport(f.profile, script, "secret stdin"));
    expect(result.args).toEqual(["-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10", "--", "test-alias", "bash -c " + shellQuote(script)]);
    expect(result.stdin).toBe("secret stdin");
    const configFile = join(f.root, "ssh configs", "operator's $(touch definitely-not-a-local-command).conf");
    const configured = JSON.parse(await sshTransport({ ...f.profile, ssh_config_file: configFile }, script, "configured stdin"));
    expect(configured.args).toEqual(["-F", configFile, "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=10", "--", "test-alias", "bash -c " + shellQuote(script)]);
    expect(configured.stdin).toBe("configured stdin");
    const generic = JSON.parse(await sshTransport({ ssh_host: "researcher@login.example.test" }, script));
    expect(generic.args.slice(-2)).toEqual(["researcher@login.example.test", "bash -c " + shellQuote(script)]);
    expect(await readdir(f.root)).not.toContain("definitely-not-a-local-command");
    for (const ssh_config_file of [null, false, 42, [], {}, "", "relative/config", "~/.ssh/config", "-F/config", "/", "///", "/ssh/../config", "/ssh/./config", "/ssh\nconfig", "/ssh\0config", "/ssh\u007fconfig"]) {
      const profile = { ...f.profile, ssh_config_file } as unknown as ClusterProfile;
      await expect(sshTransport(profile, "true")).rejects.toMatchObject({ code: "INVALID_SSH_CONFIG_FILE" });
    }
    await expect(sshTransport({ ...f.profile, ssh_host: "-oProxyCommand=unsafe" }, "true")).rejects.toMatchObject({ code: "INVALID_SSH_ALIAS" });
  } finally { process.env.PATH = previous; }
});

/** Replace only this process's SSH lookup; the executable still runs the actual remote command. */
async function withFakeSsh(root: string, source: string, run: () => Promise<void>) {
  const bin = join(root, "fake-ssh"); await mkdir(bin);
  await writeFile(join(bin, "ssh"), "#!/usr/bin/env python3\n" + source, { mode: 0o700 });
  const previous = process.env.PATH;
  process.env.PATH = bin + ":" + previous;
  try { await run(); }
  finally { process.env.PATH = previous; }
}

const executeSshCommand = "import os,sys\nos.execv('/bin/bash', ['bash', '-c', sys.argv[-1]])\n";

function verboseStderr(mode: "huge-chunk" | "fragmented") {
  const output = mode === "huge-chunk"
    ? "sys.stderr.buffer.write(b'b' * (3 * 1024 * 1024))\nsys.stderr.buffer.flush()\n"
    : "for _ in range(12):\n sys.stderr.buffer.write(b'b' * 3072)\n sys.stderr.buffer.flush()\n time.sleep(0.005)\n";
  return "import sys,time\nsys.stderr.write('discarded-stderr-prefix\\n')\nsys.stderr.flush()\n" + output;
}

async function expectSshStopped(pidFile: string) {
  // Linux pidfd readiness waits for the actual exit, not a guessed sleep after SIGKILL.
  const waiter = Bun.spawn(["python3", "-c", String.raw`
import os,select,sys
try: fd=os.pidfd_open(int(sys.argv[1]))
except ProcessLookupError: sys.exit(0)
poll=select.poll()
poll.register(fd,select.POLLIN)
assert poll.poll(2000), 'fake SSH survived its output limit'
os.close(fd)
`, (await readFile(pidFile, "utf8")).trim()], { stdout: "pipe", stderr: "pipe" });
  expect(await new Response(waiter.stderr).text()).toBe("");
  expect(await waiter.exited).toBe(0);
}

test("SSH probe stdout budget does not hide a larger real Python traceback or expose private stdout", async () => {
  const f = await fixture();
  await withFakeSsh(f.root, executeSshCommand, async () => {
    const script = "printf '%s' 'private-structured-stdout'; python3 -c " + shellQuote("raise RuntimeError('trace-context-' * 150 + 'terminal-python-cause')");
    const error = await sshTransport(f.profile, script, "private-stdin", { max_bytes: 1024 }).catch((error) => error);
    expect(error).toBeInstanceOf(SlurmClientError);
    expect(error.code).toBe("SSH_COMMAND_FAILED");
    expect(error.message).toContain("Traceback");
    expect(error.message).toContain("terminal-python-cause");
    expect(Buffer.byteLength(error.message)).toBeGreaterThan(1024);
    expect(error.message).not.toContain("private-structured-stdout");
    expect(error.message).not.toContain("private-stdin");
    expect(error.evidence).toEqual({ stdout: "private-structured-stdout", exit_code: 1 });
    const fullStderr = await sshTransport(f.profile, "python3 -c " + shellQuote("import sys; sys.stderr.write('x' * (8192 - len('terminal-full-stderr-cause')) + 'terminal-full-stderr-cause'); sys.exit(7)"), undefined, { max_bytes: 1024 }).catch((error) => error);
    expect(fullStderr.code).toBe("SSH_COMMAND_FAILED");
    expect(fullStderr.message).toContain("SSH exited 7; stderr tail:");
    expect(fullStderr.message).toContain("terminal-full-stderr-cause");
    expect(String(fullStderr).length).toBeLessThan(8192);
    const stdout = await sshTransport(f.profile, "python3 -c " + shellQuote("import sys; sys.stderr.write('b' * 8192); sys.stdout.write('s' * 1024)"), undefined, { max_bytes: 1024 });
    expect(stdout).toBe("s".repeat(1024));
  });
});

for (const mode of ["banner", "fragmented-banner", "traceback", "stdout-noise", "stdout-prefix"] as const) {
  test(`shared mapping verifier uses production SSH with ${mode} and retains the actual worker payload`, async () => {
    const f = await fixture();
    await mkdir(f.profile.shared_root);
    const workerRoot = join(f.root, "ordinary-worker");
    await mkdir(workerRoot);
    const shim = mode === "traceback" ? await probePythonShim(f.root, "clock") : undefined;
    const prelude = verboseStderr(mode === "fragmented-banner" ? "fragmented" : "huge-chunk") +
      (mode === "stdout-noise" ? "sys.stdout.write('private-stdout-noise' * 100)\nsys.stdout.flush()\n" :
        mode === "stdout-prefix" ? "sys.stdout.write('unexpected-prefix')\nsys.stdout.flush()\n" : "");
    let probeName = "";
    const environment = shim ? `import os\nos.environ['PYTHONPATH']=${JSON.stringify(shim.env.PYTHONPATH)}\n` : "";
    await withFakeSsh(f.root, prelude + environment + executeSshCommand, async () => {
      const verification = verifySharedMappings(f.profile, sharedRoots(f.profile), {
        api: {
          listDataPaths: f.fakeApi.listDataPaths!,
          startJob: async (input) => {
            const result = await f.fakeApi.startJob!(input);
            probeName = input.command.match(/\.wh-probe-[a-f0-9-]+/)![0];
            await runRemoteScript(input.command.replaceAll("/data/shared/shared", workerRoot));
            if (mode !== "traceback") await writeFile(join(f.profile.shared_root, probeName), f.state.probe);
            return result;
          },
        },
      });
      if (mode === "banner" || mode === "fragmented-banner") await verification;
      else {
        const error = await verification.catch((error) => error);
        expect(error.code).toBe("SHARED_STORAGE_NOT_VERIFIED");
        expect(error.evidence).toContain(mode === "traceback" ? "SSH_COMMAND_FAILED" : mode === "stdout-noise" ? "SSH_OUTPUT_LIMIT" : "shared storage identity mismatch");
        if (mode === "traceback") expect(error.evidence).toContain("shared probe not visible after 30 seconds; possible writeback cache delay");
        if (mode === "stdout-noise") expect(error.evidence).toContain("stdout");
        expect(error.evidence).not.toContain("private-stdout-noise");
        expect(error.evidence).not.toContain("unexpected-prefix");
      }
      expect(f.calls).toEqual(["probe"]);
      expect(await readdir(workerRoot)).toEqual([probeName]);
      expect(await readFile(join(workerRoot, probeName), "utf8")).toBe(f.state.probe);
      expect(await readdir(f.profile.shared_root)).toEqual(mode === "traceback" ? [] : [probeName]);
    });
  });
}

for (const mode of ["huge-chunk", "fragmented"] as const) {
  test(`SSH ${mode} verbose nonzero stderr retains the final cause without exposing private stdout`, async () => {
    const f = await fixture();
    const source = verboseStderr(mode) +
      "sys.stdout.write('private-stdout-payload')\nsys.stdout.flush()\nraise RuntimeError('terminal-verbose-python-cause')\n";
    await withFakeSsh(f.root, source, async () => {
      const error = await sshTransport(f.profile, "unused", "private-stdin", { max_bytes: 1024, timeout_ms: 2000 }).catch((error) => error);
      expect(error).toBeInstanceOf(SlurmClientError);
      expect(error.code).toBe("SSH_COMMAND_FAILED");
      expect(error.message).toContain("SSH exited 1; stderr tail:");
      const tail = error.message.split("; stderr tail:\n")[1];
      expect(tail).toBeDefined();
      expect(Buffer.byteLength(tail)).toBe(4096);
      expect(tail).toContain("RuntimeError: terminal-verbose-python-cause");
      expect(tail).not.toContain("discarded-stderr-prefix");
      expect(error.message).not.toContain("private-stdout-payload");
      expect(error.message).not.toContain("private-stdin");
      expect(error.evidence).toEqual({ stdout: "private-stdout-payload", exit_code: 1 });
      expect(String(error).length).toBeLessThan(8192);
    });
  });
}

for (const max_bytes of [1024, undefined]) {
  test(`SSH stdout overflow still kills at ${max_bytes ?? "default 2 MiB"} and retains bounded stderr without stdout payload`, async () => {
    const f = await fixture(), pidFile = join(f.root, "ssh.pid");
    const source = `import os,signal,sys,time\nfrom pathlib import Path\nPath(${JSON.stringify(pidFile)}).write_text(str(os.getpid()))\nsys.stderr.write('discarded-stderr-prefix' + 'b' * 65536 + 'stdout-overflow-stderr-cause')\nsys.stderr.flush()\ntime.sleep(0.02)\nsys.stdout.write('private-stdout-payload' * 150000)\nsys.stdout.flush()\nsignal.pause()\n`;
    await withFakeSsh(f.root, source, async () => {
      const error = await sshTransport(f.profile, "unused", undefined, { max_bytes, timeout_ms: 2000 }).catch((error) => error);
      expect(error.code).toBe("SSH_OUTPUT_LIMIT");
      expect(error.message).toContain("stdout");
      expect(error.message).toContain(`${max_bytes ?? 2 * 1024 * 1024}-byte bound`);
      const tail = error.message.split("; stderr tail:\n")[1];
      expect(tail).toBeDefined();
      expect(Buffer.byteLength(tail)).toBe(4096);
      expect(tail).toContain("stdout-overflow-stderr-cause");
      expect(tail).not.toContain("discarded-stderr-prefix");
      expect(error.message).not.toContain("private-stdout-payload");
      expect(error.evidence).toBeUndefined();
      expect(String(error).length).toBeLessThan(8192);
      await expectSshStopped(pidFile);
    });
  });
}

test("poll cancellation reaches active scheduler transport and prevents later observations", async () => {
  const f = await fixture();
  await f.client.enqueueCluster(f.profile, f.input, f.context);
  const baseline = f.observations.length;
  const entered = Promise.withResolvers<AbortSignal>();
  const client = new SlurmClient({
    api: f.fakeApi, env: f.env,
    transport: async (profile, script, stdin, options) => {
      if (!script.includes("# wh:scheduler-batch")) return f.transport(profile, script, stdin, options);
      const pending = Promise.withResolvers<string>();
      options!.signal!.addEventListener("abort", () => pending.reject(options!.signal!.reason), { once: true });
      entered.resolve(options!.signal!);
      return pending.promise;
    },
  });
  const controller = new AbortController();
  const pending = client.pollOnce(f.context, controller.signal);
  expect(await entered.promise).toBe(controller.signal);
  controller.abort();
  await expect(pending).rejects.toThrow();
  expect(f.observations).toHaveLength(baseline);
});

test("stopping scheduler polling aborts an already active SSH observation", async () => {
  const f = await fixture();
  await f.client.enqueueCluster(f.profile, f.input, f.context);
  const entered = Promise.withResolvers<AbortSignal>();
  const aborted = Promise.withResolvers<void>();
  const client = new SlurmClient({
    api: f.fakeApi, env: f.env,
    transport: async (profile, script, stdin, options) => {
      if (!script.includes("# wh:scheduler-batch")) return f.transport(profile, script, stdin, options);
      const pending = Promise.withResolvers<string>();
      options!.signal!.addEventListener("abort", () => {
        pending.reject(options!.signal!.reason);
        aborted.resolve();
      }, { once: true });
      entered.resolve(options!.signal!);
      return pending.promise;
    },
  });
  const stop = client.startSlurmPolling(f.context);
  const signal = await entered.promise;
  stop();
  await aborted.promise;
  expect(signal.aborted).toBe(true);
});
