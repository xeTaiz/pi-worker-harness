import { afterEach, describe, expect, test } from "bun:test";
import { mkdtemp, mkdir, readFile, readdir, rm, stat, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { SlurmClient, SlurmClientError, shellQuote, sshTransport, workloadCommand, type ClientApi, type ClusterEnqueueInput, type SlurmTransport } from "./client.ts";
import type { ClusterProfile } from "./profiles.ts";
import type { Job, QueuedJob, SlurmObservation, SlurmReservationRequest, Worker, WorkerDetail } from "../types.ts";

const directories: string[] = [];
afterEach(async () => { await Promise.all(directories.splice(0).map((path) => rm(path, { recursive: true, force: true }))); });

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
      calls.push(input.name?.includes("cleanup") ? "probe-cleanup" : "probe");
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
    expect(f.calls).toContain("probe-cleanup");
    const flags = f.submissions[0]!.args;
    expect(flags).toContain("--nodes=1"); expect(flags).toContain("--ntasks=1");
    expect(flags).toContain("--cpus-per-task=8"); expect(flags).toContain("--mem=65536M");
    expect(flags).toContain("--gpus-per-node=a100:1"); expect(flags).toContain("--export=NIL"); expect(flags).toContain("--no-requeue");
    expect(flags).not.toContain("--exclusive");
    expect(f.queued[0]!.command).toBe(workloadCommand("/data/shared/shared/projects/project", "/data/shared/shared/projects/project/.venvs/test", f.input.command));
    expect(f.queued[0]!.submission_key).toBe(f.reservations[0]!.request_id);
    expect(f.staged[0]!["worker.token"]).toBe((await savedJournals(f.root))[0]!.value.registration_token);
    const settings = JSON.parse(f.staged[0]!["settings.json"]!);
    expect(settings).toMatchObject({ version: 1, compute_kind: "gpu", cpus: 8, memory_mb: 65536 });
    expect(settings.worker_name).toBe(f.reservations[0]!.name);
    expect(settings).not.toHaveProperty("registration_token");
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
    const process = Bun.spawn(["bash", "-c", script], { stdin: new Blob([stdin ?? ""]), stdout: "pipe", stderr: "pipe" });
    const [stdout, stderr, code] = await Promise.all([new Response(process.stdout).text(), new Response(process.stderr).text(), process.exited]);
    if (code) throw new Error(stderr);
    return stdout;
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
  await mkdir(f.profile.shared_root, { recursive: true });
  const record = join(f.root, "sbatch-record.jsonl");
  const executable = join(f.root, "sbatch");
  await writeFile(executable, `#!/usr/bin/env python3\nimport json,os,sys\nwith open(${JSON.stringify(record)},'a') as f: f.write(json.dumps({'args':sys.argv[1:],'env':dict(os.environ)})+'\\n')\nprint('321;local')\n`, { mode: 0o700 });
  const transport: SlurmTransport = async (p, script, stdin, opts) => {
    if (script.includes("# wh:stage") || script.includes("# wh:submit-fenced") || script.includes("# wh:read-submitted")) {
      const process = Bun.spawn(["bash", "-c", script], { stdin: new Blob([stdin ?? ""]), stdout: "pipe", stderr: "pipe", env: { ...globalThis.process.env, SBATCH_ARRAY_INX: "0-99", SBATCH_EXCLUSIVE: "yes", SLURM_CLUSTERS: "wrong", SLURM_HINT: "multithread", SLURM_DEBUG_FLAGS: "all", SLURM_EXIT_ERROR: "1", SITE_REQUIRED: "preserved" } });
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
    expect(await readdir(f.root)).not.toContain("definitely-not-a-local-command");
    for (const ssh_config_file of [null, false, 42, [], {}, "", "relative/config", "~/.ssh/config", "-F/config", "/", "///", "/ssh/../config", "/ssh/./config", "/ssh\nconfig", "/ssh\0config", "/ssh\u007fconfig"]) {
      const profile = { ...f.profile, ssh_config_file } as unknown as ClusterProfile;
      await expect(sshTransport(profile, "true")).rejects.toMatchObject({ code: "INVALID_SSH_CONFIG_FILE" });
    }
    await expect(sshTransport({ ...f.profile, ssh_host: "-oProxyCommand=unsafe" }, "true")).rejects.toMatchObject({ code: "INVALID_SSH_ALIAS" });
  } finally { process.env.PATH = previous; }
});

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
