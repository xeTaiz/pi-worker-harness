# pi-worker-harness — Extension Specification (HTTP API)

## Overview

`pi-worker-harness` integrates the worker-harness orchestrator into Pi/OMP and a
Bun stdio MCP server for Claude Code, sharing the same compute implementation.
It provides:
- Tooling for workers, jobs, and tunnels
- A bottom overlay panel (`/worker-harness`) with Workers / Jobs / Tunnels tabs
- A status/widget line below the editor, separate from the native OMP context-window display, showing fleet GPU totals or selected worker/job/log context

Ordinary control is HTTP-based, not worker-harness CLI subprocess execution.
Configured Slurm targets use local SSH only at the shared client boundary.

---

## Runtime assumptions

- Orchestrator is reachable at a base URL (default: `http://orchestrator.hs.d0me.xyz:12889`)
- API routes are under `/api/v1/*`
- Extension can read and write tool/widget/panel state inside pi session memory

No worker-harness CLI invocation is required by this extension.

---

## Architecture

```text
pi session
├── API clients
│   ├── api.ts: orchestrator `/api/v1/*` lifecycle and fleet requests
│   └── marimo.ts: direct Tailnet `/api/sessions` and `/api/kernel/execute`
├── Role-scoped grouped tools
│   ├── normal operator: complete worker, queue, tunnel, file, git, and Marimo surface
│   ├── orchestrator: fleet_status, list_projects, pm_send
│   ├── project manager: compute/Marimo plus task lifecycle and escalation
│   └── task agent: compute/Marimo plus ask_pm and notify_pm
├── Event bus (events.ts)
├── TUI panel (panel/index.ts)
└── Widget/status (widget.ts)
```

`WH_SESSION_ROLE` is read at grouped-tool registration and actions outside the
role are omitted from the schemas and rejected at execution. Unknown roles
register no harness tools. Normal sessions with the variable unset keep the
complete established surface and `wh_admin_*`; fleet roles do not receive
administrative tools. Orchestrators also omit the compute panel/widget and its
commands and shortcut.

### Shared compute boundary and standalone MCP

`tools/compute-actions.ts` owns the UI-independent compute schemas and handlers.
`tools/grouped.ts` retains Pi rendering, role filtering, fleet/Pi operations and
admin-only integration; `mcp.ts` consumes only the shared compute exports.
Handlers validate the same schemas used for discovery and reject privileged
actions and unknown fields at runtime. No MCP adapter copies operations or
imports the Pi/TUI runtime.

The Bun entry point uses pinned `@modelcontextprotocol/sdk` v1 `Server` and
`StdioServerTransport`. Launch:

```bash
bun /absolute/path/to/pi-worker-harness/mcp.ts --client-key NAME
```

Missing/invalid arguments fail on stderr. Stdout carries only MCP JSON-RPC.
The server advertises `tools:{}` and
`experimental:{"claude/channel":{}}`, with just `wh_read`/`wh_dispatch`.
Schemas and execution exclude Pi sessions/fleet messaging, `workers_prune`, and
all `wh_admin_*` actions and `grant_git_access`. The ordinary compute surface
includes jobs, data/file operations, tunnels and Marimo; no permission relay
capability is declared.
Tool requests use the session's canonical context and MCP cancellation signal.

The server's `oninitialized` callback starts the shared client session only
after the client sends `notifications/initialized`; connecting the stdio
transport alone does not start events. Closing stdin, SIGINT/SIGTERM and
transport closure stop SSE and polling. Diagnostics never enter stdout.

Register as MCP server `worker-harness`. During the Claude channels research
preview, actual custom-channel testing requires an interactive
`claude --dangerously-load-development-channels server:worker-harness`, user
consent, a compatible authenticated client, and organization policy permitting
channels. The development flag skips only the allowlist; `-p` and Agent SDK do
not register it. This is not a promise of headless notification delivery.
Protocol reference: https://code.claude.com/docs/en/channels-reference.

### Session notifications and lifecycle

`slurm/notifications.ts` is the sole session SSE/scheduler-poll lifecycle.
`createClientSession({kind,sessionId,deliver,client?})` supplies `getContext()`,
`start()` and `stop()`; tools and events reuse its persisted random recipient.
OMP keys it by `ctx.sessionManager.getSessionId()`; Claude requires a stable,
distinct `--client-key` for each independently active client. State is private
and namespaced by orchestrator/client kind/session beneath
`WH_CLUSTER_STATE_DIR` or the XDG worker-harness Slurm state directory.
URL/credential replacement aborts the old streams/polls without persisting
control credentials. A closed client is never resurrected.

Ordinary `exec`/`enqueue` and cluster reservations/initial jobs carry the same
`notify_recipient_id`. Authenticated `GET /api/v1/job-events?recipient_id=…&after=N`
replays/tails durable ordinary job events without Pi registration or worker
tokens. Only real state transitions produce notifications, not heartbeats or
unchanged scheduler observations. Content is concise, bounded and identifies
event, worker and job; failure/deadline/log context is untrusted workload data.

OMP hands off with `sendMessage(...,{deliverAs:"aside",triggerTurn:true})`; native
runtime policy suppresses wakeups after user-stop and during plan mode.
Claude emits `notifications/claude/channel` with
`params:{content:string,meta:Record<string,string>}`. Metadata keys contain only
letters/digits/underscores. Awaiting notification writes means transport
handoff, not Claude receipt or agent action. Cursor persistence follows handoff,
deduplication is session-local, and crashes can replay. Check current ordinary
job state before rerunning. Failure deadlines never await acknowledgement.
Recipient IDs are routing in today's single-user trust model, not ownership or
multi-user privacy.

### Local Slurm target behavior

`slurm/{profiles,resources,client}.ts` supplies both clients' existing compute
actions. Missing profile configuration gives no cluster targets; invalid
configuration fails explicitly. `list_workers` merges client-only
`cluster:<profile-id>` targets; `get_worker` refreshes inventory/policy and
`available_gpus` separates scheduler capacity from immediately free workers.
Actual allocated workers remain ordinary server Worker rows. Panel selection
and raw `api.ts` worker calls remain worker-only.

`enqueue` on cluster targets requires `slurm.project_path` and explicit bounded
`slurm.walltime_seconds`, with optional environment/CPU/RAM/checkpoint policy.
Fresh scheduler capacity drives proportional GPU defaults; CPU targets require
zero GPUs. Project and optional environment independently resolve beneath any
configured `/data/shared/<storage_id>` root, including `additional_shared_roots`.
Resolve symlinks (also environment `bin`) against the selected mapping and reject
escapes. The persisted ordinary command changes cwd/environment before the
original command. Bind every mapping through `WH_SHARED_ROOTS_JSON`; keep
run/settings/batch staging under the first/primary root's `.wh-slurm`.
Reserve OFFLINE, queue the confirmed initial task, then finite sbatch:
there is no idle submission, prepare/submit action, or self-chaining successor.

Ordinary worker IDs keep existing execution behavior. Cluster IDs are rejected
for exec/file/tunnel/Marimo operations with `CLUSTER_REQUIRES_ENQUEUE`.
Cluster-filtered jobs/queue aggregate actual allocated workers; logs use live
transport or bounded shared SSH fallback. Submission recovery reuses the
persisted `submission_key`; uncertain attempts never authorize blind resubmit.
Job stop defaults to `scope:"job"`; `scope:"allocation"` uses the job's verified
scheduler allocation, returns requested until terminal evidence, and is
rejected for ordinary workers. Finished Slurm workers reject new admission;
cancel/fresh enqueue is required to change allocation resources.

### Interactive cluster onboarding contract

`index.ts` registers `/cluster-add [name]` using `slurm/setup.ts`. This is
interactive Pi/OMP setup, not a new compute action or MCP tool. The argument
names a setup or resumes its draft; no argument offers draft selection.
Completed targets still use the existing validated version-1 profile document
and atomic target-set persistence. Invalid/duplicate/unreadable/inconsistent
completed configuration fails explicitly, never silently becomes no targets.

Prompt/progress order:

1. SSH alias or `user@host`; local OpenSSH `-G` resolves user/port/jump. Ask a
   separate username only if needed; reuse an unambiguous managed connection.
   Suggest the short cluster name (or argument), then optional description.
2. Explicit shared-key/draft consent before credential creation. Prepare managed
   SSH files, save phase `ssh`, publish only fenced `sh` blocks containing one
   command each with no comments or prose,
   and return without a test/select overlay or remote probe. New and resumed
   `connection` drafts take this path. No model turn or text-only custom renderer.
3. Optionally prefill `/cluster-add <name>` through UI `setEditorText`; the user
   submits it after running the commands. A fresh invocation of phase `ssh` or
   later attempts one strict BatchMode probe with no bootstrap modal. Failure
   republishes relevant commands, warns, retains the draft and returns without
   a retry loop. Old released `ssh` drafts follow this same resume behavior.
4. After strict login succeeds, select `list_data` WH storage or type
   `/data/shared/<storage_id>`, then the exact existing cluster-host root.
   Explicit **“Add another filesystem mapping?”** repeats the two mapping
   fields until No. First mapping remains primary. Verify every mapping against
   a participating WH worker and exact cluster path with a temporary random
   identity probe and cleanup; unavailable worker/catalog permissions or probe/
   cleanup failures block readiness, never produce “verified” evidence.
5. Bounded read-only SSH discovery inspects Slurm inventory/partition limits,
   account/default/QoS evidence, runtime/version and static SIF capability.
   No allocations, recursive filesystem scans or login-node container execution.
6. Prompt only for unresolved runtime/module, image or required explicit
   billing choice. Prefer a known default account or omit `account` to let Slurm
   select it. Hidden association evidence stays unknown; visible accounts/QoS
   do not authorize arbitrary billing, premium QoS or special partitions.
7. One editable summary offers Register targets / Edit review / Advanced /
   Save and exit. Generate CPU targets from supported CPU partitions and GPU
   targets per partition/exact physical type using configured physical capacity,
   not free capacity. Unsupported ambiguous/MIG cases remain explicit.
   Register all selected targets atomically with deterministic unique IDs and
   the same managed connection/key/trust and stable mapping IDs.

`DEFAULT_ALLOCATION_WALLTIME_SECONDS` is **604800**: a default seven-day
**profile allocation ceiling**, clamped by finite partition MaxTime. It never
defaults the explicit per-enqueue `slurm.walltime_seconds`. CPU targets retain
zero GPUs (default 1 CPU/4096 MiB); GPU requests still default to one GPU.
Defaults are failure grace0, termination20, warning120 and poll15 seconds,
registration12888. Any positive failure grace, including300, requires explicit
Advanced opt-in with a site idle-policy warning; idle holding is never inferred.

Drafts are private version-1 `dirname(clusters.json)/setup-drafts.json` (0600),
not completed profiles or submission journals. Preserve stable connection/
mappings and last valid phase across restart. Preconsent cancellation creates
no credentials/completed profile; after consent, report retained key/draft and
abort active discovery where supported. Missing runtime/image/credentials or
deployment prerequisites save a blocked resumable draft, not fake ready targets.

One-time private version-1 `dirname(clusters.json)/cluster-integration.json`
stores the enrollment **file path**, login server and optional registration
override. Reuse explicit integration/profile settings before prompting once.
Default registration is active control URL hostname +12888, not privileged
control12889. Key existence/private-permission checking is not successful
enrollment; no plaintext secret UI or arbitrary host `worker-harness.env` reads.

Readiness requires existing API Authorization (`WH_OPERATOR_TOKEN_FILE`,
`WH_OPERATOR_TOKEN`, or authorized session), then required OpenAPI routes.
Missing authorization blocks the draft; never request a token value. A missing
Tailnet key can be provisioned at the saved path before resume, prompting a
path correction only conditionally. After failed mapping identity verification,
resume offers Retry saved mappings / Edit filesystem mappings / Save and exit;
editing repeats the same catalogue/root/add-another loop. Wizard runtime
prelude edits allow safe module loads/generated PATH setup, not arbitrary
login-node shell execution; manual-profile runtime_setup remains trusted
operator configuration.

Presence alone is insufficient: after required live OpenAPI routes, readiness
performs bounded authenticated
`GET /api/v1/job-events?recipient_id=wh-setup-<fresh UUID>&after=0`.
Require HTTP200, immediately cancel the body/abort, and consume no events.
No reservation, registry mutation or allocation occurs. HTTP401/403 or timeout
blocks/resumes with existing-credential guidance; never prompt a token value.

Profiles retain primary `storage_id`/`shared_root` and optionally
`additional_shared_roots: {storage_id:string, shared_root:string}[]`.
`sharedRoots(profile)` returns primary first. Absent extras stay absent in old
canonical bodies, preserving journal request identities. IDs are unique;
safe normalized absolute roots and same-ID consistency apply across primary
and extra mappings/profile documents. Nested host roots with distinct IDs are
not categorically forbidden. Optional `account` preserves explicit selections;
when absent, omit `--account` rather than fabricating one.

The destination is `WH_CLUSTER_CONFIG` or the normal XDG file. A single managed
Ed25519 login identity is shared across onboarded profiles:
`dirname(clusters.json)/id_ed25519` and `id_ed25519.pub`, plus
`dirname(clusters.json)/known_hosts` (default `~/.config/worker-harness`).
Shared files are directly in the configuration parent, not `ssh/`, avoiding
collisions with valid legacy profile IDs. Per-connection
`config` and `bootstrap_config` remain under `ssh/<id>/`; reuse can retain the
original profile's config path. Directories are 0700/generated files 0600;
existing shared identity/trust files are preserved. Optional absolute
`ssh_config_file` is passed as argv `-F <path>` through all cluster SSH actions.
Without this field ordinary manual-profile SSH behavior is unchanged.

Managed configs retain ordinary user config without editing `~/.ssh/config`.
`/etc/ssh/ssh_config` is included only when readable, regular, root/current-UID
owned and not group/world writable; otherwise a UI warning directs
required site settings into user config. User config remains subject to strict
OpenSSH checks. Alias/ProxyJump/ControlMaster policy is preserved where safely
available; direct destinations retain resolved user/port/jump policy. Generated
targets share connection/trust/mappings rather than repeat a setup form per
partition or compute kind.

Both dedicated and bootstrap configs explicitly select shared `UserKnownHostsFile`.
Its empty 0600 file is created exclusively if absent; existing host trust is
never overwritten, cleared or imported automatically. The interactive commands
require independent fingerprint verification before acceptance into this file;
ssh-keyscan alone is not verification. No trust bootstrap command is executed
by setup, and no normal user trust file is edited.
A new profile's bootstrap retains destination/includes and original
key/agent/password/MFA authentication without the shared IdentityFile/IdentitiesOnly
restriction. A migrated bootstrap explicitly retains the old per-profile
IdentityFile **without** IdentitiesOnly, enabling old-key-only host access while
installing the shared public key.
The first fenced command is
`ssh-copy-id -f -F <bootstrap_config> -i <shared-public-key> -o StrictHostKeyChecking=ask <alias>`.
Force mode bypasses copy-id's authentication filter: existing bootstrap keys
must not make it falsely conclude the shared key is installed. Repeated runs
can append duplicate public-key entries.
The second, only when trust is absent/unprovable, is
`ssh -F <bootstrap_config> -o StrictHostKeyChecking=ask <alias> true`.
Both override normal strict policy only for the operator's interactive,
fingerprint-confirmed command. Read-only managed `ssh -G` evaluation determines
the effective hostname/port/HostKeyAlias; bounded `ssh-keygen -F -f <shared-trust>`
checks the actual lookup, including hashed and non-default-port entries.
A host-key probe failure always republishes the trust command.
The dedicated probe remains shared IdentityFile/IdentitiesOnly with BatchMode=yes,
StrictHostKeyChecking=yes and ConnectTimeout=10, using the existing bounded
`sshTransport` seam. No public-key dump, passphrase/agent instructions, manual
fallback, metadata wall or strict test command appears in bootstrap messages.
Missing/changed trust is never bypassed.

A newly generated shared key is initially unencrypted; existing encrypted keys
are preserved. The user can add a passphrase in their own terminal and unlock
it through an accessible dedicated agent. BatchMode never captures/unlocks a
passphrase or completes MFA. The current sandbox launcher unsets SSH_AUTH_SOCK;
ordinary host ssh-add is not sufficient (see the restart gate below).
No private key/passphrase/token enters Git, workload argv or an allocation.

`/cluster-ssh-migrate` explicitly migrates existing recognized wizard-generated
profiles. If no shared key exists, the user selects one legacy wizard key and
confirms an exclusive byte-copy preserving its encryption; an existing shared
key is reused. Legacy keys are not removed. Migration preserves alias/direct
destination policy, shared host trust, and the old per-profile bootstrap
IdentityFile without IdentitiesOnly; dedicated configs select the shared identity.
External/unrecognized configs are explicitly skipped rather than guessed or
overwritten. It prints the same fenced public-key installation command and
conditional interactive trust command for each migrated profile.
Different previously authorized profile keys require installing the shared
public key remotely; config/path changes alone cannot grant access.

Local profile creation is not compute-node connectivity or scheduler readiness
proof. The login SSH key, restricted ephemeral Tailnet enrollment file,
operator/session control bearer and scoped worker-registration token are
separate credentials. Clients need authenticated control12889; allocations
need registration12888 and orchestrator-to-worker Tailnet SSH22, never
worker-to-control access. Existing mounts and participating WH worker identity
probes are prerequisites for every mapping.

Both existing image definitions carry static
**`org.worker-harness.slurm-runtime=1`**. Discovery may use read-only
`apptainer|singularity inspect --json`, not execute a container or trust a
filename alone. Build the updated candidate SIF and deploy the matching server
before enqueue. Production control OpenAPI currently returns HTTP200 but lacks
reservation, Slurm-update and job-events routes (live gate observed 2026-11-01);
source implementation is **not deployment**. Missing routes/image/runtime/key
save blocked resumable drafts, not fake ready profiles.

CPU preparation is ordinary zero-GPU enqueue on a generated CPU target, with
explicit completion/import checks before GPU work. The standalone root
`specs/local-slurm.md` contains complete optional-account/multiple-root JSON,
transfer/deployment instructions, environment hashing/flock and live gates.

### Sandbox persistence and verification boundary

The interactive operator's default agent-sandbox allowlist at
`~/dotfiles/agent-sandbox/.config/agent-sandbox/paths` persists plain
`~/.config/worker-harness` and `~/.local/state/worker-harness` entries,
preserving unrelated legacy entries and the ordinary path parser.
Fleet launches instead select a generated session file through
`AGENT_SANDBOX_PATHS`. Root `src/worker_harness/sandbox.py:25–31,41–48,66–73`
defines/renders `ROLE_PATHS` without those credential directories and sets the
session-file override; `src/worker_harness/orchestration.py:450–466` generates
and writes that file before launch. This separation does not depend on
role-specific path syntax. A manual launch using the operator default receives
its ordinary binds regardless of `WH_SESSION_ROLE`; it is not hostile-user
isolation. Do not substitute the operator default for the fleet session file.
Before relaunch, an operator creates these directories with 0700 permissions
**outside the current masked sandbox HOME**; absent source directories are
skipped, not created by the launcher. This session's filesystem visibility
does not establish whether host profiles or state already exist.
For the next interactive operator launch, invoke the existing dotfiles launcher at
`"$HOME/dotfiles/agent-sandbox/.local/bin/agent-sandbox"` with the usual
tool/binary arguments, and set host
`AGENT_SANDBOX_PATHS="$HOME/dotfiles/agent-sandbox/.config/agent-sandbox/paths"`.
Alternatively, use the host's stow/symlink installation pointing to that launcher
and allowlist. No new launcher syntax is required. The active host
launcher/config symlinks have not been observed here.

Custom `XDG_CONFIG_HOME`/`WH_CLUSTER_CONFIG` and
`XDG_STATE_HOME`/`WH_CLUSTER_STATE_DIR` require allowlisting the actual host
configuration/managed-SSH and state locations as ordinary paths, and creating
them outside the sandbox before launch. Keep credential paths and broader
credential-containing parents out of the separate generated fleet allowlist.
Config persistence
alone loses recovery journals/cursors; state persistence alone loses login
identity/trust. Existing profiles require explicit `/cluster-ssh-migrate`;
adding mounts cannot repair legacy paths or authorize the shared key remotely.

Mount changes require a complete **agent-sandbox restart**, not just an OMP
module/process restart. The launcher still unsets `SSH_AUTH_SOCK`; an encrypted
key requires separately provisioned dedicated agent/socket access and explicit
SSH selection inside the new sandbox. Host `ssh-add` is insufficient.
SSH-agent forwarding policy is unchanged.

**Historical baseline recorded 2026-11-01 (predates minimal workflow):** the
actual restored launcher/private-HOME proof passed with the two plain default
paths and separate source-generated fleet allowlists. Interactive config/state
persisted; fleet credential paths stayed hidden. Work access, unrelated SSH
masking, unset `SSH_AUTH_SOCK` and role environment stripping were unchanged.
No new human/fleet sandbox policy is introduced. Config/state directories now
have persistent 0700 binds; absent completed profiles/default key/managed
trust still means onboarding has not completed.

The historical native suite passed **361 tests, zero failures**, 2,764
expectations/14 files (2.19 seconds); actual **OMP 18.14.2 shared-SSH/migration
smoke passed in 32.39 seconds**. These predate discovery/resume/additional-root
changes; current proof below passed with 439 Bun tests, 339 isolated root tests,
actual OMP 32.13s and multiple-filesystem smoke 1.70s. Publication, deployment and
real login/charged acceptance remain separate gates.

---

## HTTP API contract used by extension

Base URL is configured in `api.ts` (`getOrchestratorUrl` / `setOrchestratorUrl`).
`WH_ORCHESTRATOR_URL` wins over both persisted defaults and runtime URL changes.
All control-plane calls, bridge traffic, and log streams use that same base URL.
Bearer selection is `WH_SESSION_TOKEN` first; only unscoped operator sessions
may fall back to `WH_OPERATOR_TOKEN`, then the file at `WH_OPERATOR_TOKEN_FILE`.
Configured but unreadable token files fail closed. Fleet launchers must scrub
both operator variables and keep the credential file outside sandbox paths.
Bridge registration reports `resume_path` from the agent's session manager;
the service persists it for subsequent PM/orchestrator relaunches.

### Workers
- `GET /api/v1/workers` → `Worker[]`
- `GET /api/v1/workers/:id` → `WorkerDetail`
- `GET /api/v1/workers/summary` → `WorkersSummary`
- `DELETE /api/v1/workers/prune?minutes=:n` → `WorkersPruneResult`

### Reserved Slurm workers and events

- `POST /api/v1/workers/reservations` reserves an ordinary OFFLINE worker using
  request ID, capacity, recipient, scoped-token hash and typed Slurm resource policy.
- `PATCH /api/v1/workers/:id/slurm` records typed scheduler/submission/cancellation
  observations and linked recovered runner results; it never makes an allocation live.
- Existing `POST /register` is on registration port 12888 and requires the scoped
  worker bearer for reserved IDs; control routes are on 12889.
- `GET /api/v1/job-events?recipient_id=:id&after=:sequence` replays/tails durable
  SSE with the existing operator/session bearer, never the worker token.

The new control routes require credentials even in anonymous legacy development
mode. Recipients route within the single-user trust model, not ownership/privacy.
Same reservation request/body and initial queue submission_key recover the same
identity; conflicting reuse fails. Token values are never exposed in worker projections.

### Jobs
- `POST /api/v1/jobs` with `{ worker_id, command, name?, notify_recipient_id? }` → `Job`
- `GET /api/v1/jobs` (optional `worker_id`, `status`) → `Job[]`
- `GET /api/v1/jobs/queue?worker_id=:id` → active `QueuedJob[]` in authoritative queue order
- `POST /api/v1/jobs/queue` with `{ worker_id, command, name, expected_seconds, gpu_count?, no_pty?, notify_recipient_id?, submission_key? }` → `Job`
- `PATCH /api/v1/jobs/:id/queue` with at least one mutable queue field → `QueuedJob`
- `GET /api/v1/jobs/:id/logs?tail=&head=` → `JobLogsResult`
- `GET /api/v1/jobs/:id/logs/stream?tail=&poll_seconds=` (text stream) → live log lines
- `DELETE /api/v1/jobs/:id` → `StopJobResult`

### Tunnels
- `POST /api/v1/tunnels` with `{ worker_id, local_port, remote_port, name? }` → `Tunnel`
- `GET /api/v1/tunnels` → `Tunnel[]`
- `DELETE /api/v1/tunnels/:id` → `RemoveTunnelResponse`

### Marimo lifecycle
- `POST /api/v1/marimo` with `{ worker_id, notebook_path, environment, ready_timeout? }` → `MarimoSession`
- `GET /api/v1/marimo` (optional `worker_id`) → `MarimoSession[]`
- `GET /api/v1/marimo/:id` → `MarimoSession`
- `DELETE /api/v1/marimo/:id` → `RemoveMarimoResponse`

### Agent fleet
- `GET /api/v1/pi/sessions` → `PiSession[]`
- `GET /api/v1/pi/sessions/:id` → `PiSession`
- `POST /api/v1/pi/sessions/:id:send` with `{ message }`
- `POST /api/v1/pi/sessions/:id:ask-pm` with `{ question }`
- `POST /api/v1/pi/sessions/:id:notify-pm` with `{ note }`
- `POST /api/v1/pi/sessions/:id:submit-pr` with `{ summary }`
- `POST /api/v1/pi/sessions/:id:teardown` with `{ force }`
- `GET /api/v1/pi/projects` → `Project[]`
- `POST /api/v1/pi/projects/:project:send` with `{ message }`
- `POST /api/v1/pi/projects/:project/tasks` with `{ branch, briefing }`
- `POST /api/v1/pi/orchestrator:send` with `{ message }` — lazy-starting PM escalation

Kernel execution does not use the orchestrator API. `marimo.ts` fetches
`/api/sessions`, creates a kernel when none matches the notebook by attaching to
`/sse?session_id=…&file=…` until `kernel-ready` and then detaching, and posts to
`/api/kernel/execute` at the lifecycle resource's returned Tailnet URL. A
detached session stays alive in edit mode (marimo closes it on disconnect only
under an explicit `--session-ttl`) and is resumed by the first browser that
opens the notebook; staying attached would make that browser a viewer instead of
the editor. The launcher uses `--no-token`; Tailnet reachability and Headscale
ACLs are the access boundary.

A newly created kernel is empty: `/api/kernel/execute` calls
`session.instantiate(auto_run=False)`, so saved cells enter the graph unexecuted.
`start_marimo` therefore calls `hydrateNotebook`, which runs every saved cell
through code mode and reports the count; code mode's context exit awaits the
queued runs, so the call returns only after hydration finishes (verified with a
notebook whose cell sleeps five seconds). `/api/kernel/instantiate` is not used
because marimo's skew-protection middleware exempts only `/api/kernel/execute`,
`/ws`, and the login form, rejecting everything else without a
`Marimo-Server-Token`.

---

## Tools

Ordinary compute tools use `api.ts` helpers; configured cluster targets use the
shared local-SSH Slurm client, never the worker-harness CLI. Orchestrator API,
log-stream, job-event and session-bridge requests use the selected control bearer.

### Registered actions

- Unset:
  - `wh_read`: `list_workers`, `available_gpus`, `get_worker`,
    `get_worker_summary`, `list_data`, `list_jobs`, `list_queue`,
    `get_job_logs`, `list_tunnels`, `pi_sessions`
  - `wh_dispatch`: `data_copy`, `exec`, `stop_job`, `enqueue`,
    `update_queued_job`, `add_tunnel`, `remove_tunnel`, `workers_prune`,
    `upload_file`, `download_file`, `grant_git_access`, `list_marimo`,
    `get_marimo`, `start_marimo`, `stop_marimo`, `execute_marimo`
- Orchestrator:
  - `wh_read`: `fleet_status`, `list_projects`
  - `wh_dispatch`: `pm_send`
- Project manager:
  - `wh_read`: `list_workers`, `available_gpus`, `get_worker`,
    `get_worker_summary`, `list_data`, `list_jobs`, `list_queue`,
    `get_job_logs`, `pi_sessions`
  - `wh_dispatch`: `data_copy`, `exec`, `stop_job`, `enqueue`,
    `update_queued_job`, all five Marimo actions, `dispatch_task`, `answer`,
    `submit_pr`, `teardown_task`, `escalate`
- Task:
  - `wh_read`: `list_workers`, `available_gpus`, `get_worker`,
    `get_worker_summary`, `list_data`, `list_jobs`, `list_queue`, `get_job_logs`
  - `wh_dispatch`: `data_copy`, `exec`, `stop_job`, `enqueue`,
    `update_queued_job`, all five Marimo actions, `ask_pm`, `notify_pm`

Marimo list/get remains on `wh_dispatch` because its direct URL grants Python
execution. A task agent never pushes or opens a pull request; a project manager
reviews the worktree diff before `submit_pr`.

### Required tool behaviors
- Successful Marimo start/stop emits `worker-harness:refresh`; list/get/execute does not
- `execute_marimo` refreshes `/api/sessions` on each call, forwards aborts and output updates, and never accepts an ephemeral kernel-session ID
- Errors return structured tool errors from API errors (`status`, `code`, `message`, `detail`)
- `wh_dispatch exec` (sync mode), `wh_dispatch add_tunnel`, and Marimo actions include resource/result metadata in tool result details
- All grouped tools render their call header via `renderCall` so the action and key args appear in the TUI without expanding the JSON args block

### Queueing contract

- One strict FIFO queue exists per worker. The submitting agent selects the worker.
- Consecutive queue heads may run concurrently when their GPU requests fit. A multi-GPU head blocks all later work when it cannot fit; no backfill, priority, reservation, preemption, retry, deadline, or timeout policy exists.
- `expected_seconds` is required positive scheduling information only. The harness assigns physical GPU indices and exports them through `CUDA_VISIBLE_DEVICES`.
- `wh_read list_queue` is the shared scheduling board and must be inspected before scheduling a GPU experiment.
- `wh_dispatch enqueue` schedules GPU work. `wh_dispatch exec` remains an immediate bypass for short setup, diagnosis, and non-GPU commands.
- Any agent can move, reorder, edit, cancel, or stop any job. Tools must warn agents not to change another queued job or stop running work unless the user explicitly asks for faster scheduling, queue reorganization, cancellation, or termination.
- Queue mutations emit `worker-harness:refresh`. The panel and above-editor widget do not expose queue controls; their existing new-job actions continue to use immediate `POST /api/v1/jobs`.

---

## Panel behavior

Panel is opened by `/worker-harness` and should always render even when API calls fail.

### Core UX guarantees
- Open panel first, then fetch asynchronously
- Show loading and inline error row instead of silent failure
- Never crash on unknown theme colors or malformed resource values
- Keep keyboard input handlers guarded with try/catch

### Navigation and keys
- List movement: `j/k`, `↑/↓`
- Tab switching: `Tab`, `Shift+Tab`, `h/l`, `←/→`, `w/b`
- Refresh: `r`
- Close: `q` or `Esc`
- Jobs tab actions:
  - `f`: follow running/pending job logs via stream
  - `f` on terminal jobs: load saved logs via logs endpoint
  - `s`: stop running job
- Workers/Jobs tab actions:
  - `n`: start new job via inline prompt mode
  - `t`: create tunnel via inline prompt mode

### Inline input modes (no env-var dependency)

The panel must not require environment variables for interactive actions.

#### New job (`n`)
- Prompt inline for shell command (required)
- Prompt inline for optional job name (optional; blank = none)
- Submit via `POST /api/v1/jobs`
- Optimistically insert job into panel state and reconcile with API list refresh

#### New tunnel (`t`)
- Prompt inline for remote worker port (required)
- Prompt inline for local port (optional; blank = auto-random)
- Prompt inline for optional tunnel/service name (optional)
- Submit via `POST /api/v1/tunnels`
- On local-port conflict, retry with another random local port if user selected auto mode

No `PI_WORKER_HARNESS_PANEL_*` env vars are part of panel UX contract.

---

## Refresh and event model

- Extension startup must not block pi session startup on orchestrator availability
- Panel subscribes to refresh events while open and unsubscribes on close
- Widget/status data refresh is event-driven (plus explicit user refresh)
- Optimistic state reconciliation is required for newly-created jobs/tunnels to avoid race conditions with eventual API listing visibility

---

## Widget/status behavior

Widget tracks:
- selected or inferred worker
- selected or inferred job
- last non-empty log line

Widget should gracefully handle:
- no workers
- no selected job
- API errors
- disappearing/terminal jobs

---

## Configuration

### Runtime base URL
- Default: `http://orchestrator.hs.d0me.xyz:12889`
- Runtime command: `/worker-harness-url <http(s)://host:port>`

### Validation rules
- URL must parse and use `http` or `https`
- Invalid URL changes must be rejected with clear UI notification

### Persistence
- URL override behavior (session-only vs persisted) must be documented in README and kept consistent with implementation

---

## Error handling requirements

- API non-2xx responses must surface typed `ApiError`
- Follow-stream aborts due to user/tab switching should not trigger noisy error notifications
- Render/input exceptions must stay contained to panel and never crash pi

---

## Acceptance criteria

1. `/worker-harness` always opens, even if orchestrator is down
2. Starting a job from panel (`n`) shows job immediately in Jobs tab without manual refresh
3. Starting a job from tool path also appears in panel quickly (optimistic + reconcile)
4. `f` works for running jobs (stream) and finished jobs (saved logs)
5. Tunnel creation via `t` works without env vars and supports random local port behavior
6. Key hints are context-aware by tab/input mode
7. Panel remains stable across repeated open/close, refresh, and tab switching

---

## Current integration evidence (2026-11-01)

Current extension **`bun test`: 455 passed, zero failures**, **3,510 assertions
across 16 files, 4.98 seconds**, executed by the parent in the extension checkout.
The earlier minimal-workflow suite (439 passed, 2,938 assertions, 3.93 seconds)
predates the current bootstrap build.

The parent executed **actual OMP 18.14.2/native PTY and private local OpenSSH
bootstrap proof: passed in 14.29 seconds**, with normal plugin linking and the
default SSH authentication-attempt limit. Native `/export` preserved the original
custom messages and rejected prose/comments: each fence contained one command,
with initial/failed-missing-trust/known-nondefault-port counts of 2/2/1.
Initial setup had no probe or covering overlay and prefilled resume; failure
made exactly one strict probe and retained the draft. Generated interactive
trust verified the fixture fingerprint, preserved unrelated shared trust, and
generated `ssh-copy-id -f` deposited the actual WH public key despite a working
bootstrap identity. Server logs proved the subsequent strict probe accepted
the WH fingerprint; OMP resume reached mapping.

Zero model turns/model HTTP requests/forwarded external connections occurred;
seven startup provider CONNECT attempts were denied locally with 403. Only
private API GETs for workers/jobs/data occurred. Host SSH files were unchanged
and private fixtures removed. No live cluster/allocation or GUI copy-button
click was exercised. Ephemeral evidence: `/tmp/wh-bootstrap-evidence-gp1kowo9`.


Parent full **isolated root Python suite: 339 passed**, one existing Starlette
warning, **59.39 seconds**. Execution used `.venv/bin/python`, excluded the live
API test, supplied empty machine/project TOML paths, and unset inherited
control/session credentials. This excludes the now-visible personal registry
without altering production authorization. Slurm runtime assets were
regenerated. Local integration proofs passed as recorded here; publication,
deployment and real-cluster acceptance remain separate.

The actual **multiple-filesystem smoke passed in 1.70 seconds**, exercising
real SlurmClient preflight/three identity probes and cleanup/staging through
actual `wh-allocation.sh` → `start-wh.sh` → bwrap workload. Three binds/full
manifest were observed; primary runtime staging stayed primary. Real Python
project/import/input used an additional projects root, and venv imports a
separate additional environments root, with correct quoted paths. Project/
environment symlink escapes failed before reservation; explicit job600s stayed
unchanged; staged credentials were removed and wrapper/workload exited0.
Fixtures replaced SSH with local bash, scheduler/control API and SIF daemon;
this proves neither network/real SIF-daemon behavior nor actual Slurm jobs.

**Historical prior-workflow OMP 18.14.2 onboarding proof passed in 32.13 seconds**, with
private HOME, normal linked-plugin discovery and real PTY. Printed key setup/
pause survived a fresh OMP process/resume. Ordinary WH writes, SSH reads and
cleanup verified primary/additional mapping identity. Generated CPU/A100(max4)/
H100(max8) targets reflected physical capacity; CPU kept the 604800s ceiling,
GPU clamped to the two-hour partition limit. Missing image/API retained blocked
drafts without targets; global settings were reused once. Real OpenSSH `-G`
checked normal aliases/includes and strict invocation flags were recorded.
Local SSH/Slurm/static-inspect/control fixtures meant no actual cluster
allocation, secret leakage or model completion. Ephemeral evidence:
`/tmp/wh-minimal-onboarding-7i9sjkqj/evidence.json`.

After publication, replace an older installed OMP ref with
`omp plugin install --force 'github:xeTaiz/pi-worker-harness#feature/local-slurm'`,
then fully restart OMP. Publication is pending parent handoff, not implied by
the proof or command; matched server/SIF deployment is still required.

## Historical validation scope and rollout gates (recorded 2026-11-01)

Prior integrated results: 336 root Python tests passed with one existing
Starlette warning; 278 extension Bun tests passed with zero failures
(1,388 expectations/13 files). Historical per-profile onboarding/SSH-portability
`bun test` passed **345 tests**, zero failures, 2,341 expectations/14 files
(1.62 seconds), without bwrap or a system-config fixture.
Historical native interactive OMP onboarding passed in 15.14 seconds: isolated plugin
replacement/relink/discovery, artifact-free cancellation, existing-profile
preservation on save, actual Ed25519 public-key/fingerprint/permissions, real
bootstrap/dedicated OpenSSH `-G` host/user/port/jump/original-identity resolution,
and durable visible public instructions.

Both historical per-profile proofs ran without bwrap or system-SSH-config substitution. The
production inclusion guard handled this host's unsafe UID-65534-owned 0644
system config by omitting it with a durable warning. No product permission
bypass or host system edit was used. OMP startup provider/catalog CONNECT
attempts were recorded and denied by a local proxy with 403 before TLS:
zero forwarded connections/model completions, and no real cluster SSH/submission.
The candidate updated SIF is not built/executed, and live IBEX/charged acceptance
is not done.
The historical actual-dotfiles-launcher mount-fixture proof passed as recorded
above. The pre-minimal-workflow native extension suite passed **361 tests,
zero failures**, 2,764 expectations/14 files (2.19 seconds). Historical actual
**OMP 18.14.2 shared-SSH smoke passed in 32.39 seconds**:
normal plugin link/discovery, `/cluster-add` cancel/two independent profiles
sharing one configuration-root key and explicit managed host trust, migration
cancel/four legacy profiles including IDs `id_ed25519` and `known_hosts`,
selected-key promotion preserving its fingerprint and old keys/external config/
ordinary SSH config, real OpenSSH `-G` for both configs, and repeat migration
retaining the old bootstrap identity.

Native OMP creates no JSONL journal before a first model turn. Its supported
`/export` exposed actual persisted native `custom_message` entries, IDs,
commands and warnings, proving runtime handoff without a model call.
Startup CONNECT attempts were all locally denied with 403; no SSH network
connection or real host-trust autoacceptance occurred. Full sandbox restart and
real login/IBEX acceptance remain required separate gates.


Local proof covers real runtime/control/database/queue behavior through actual
Bun MCP stdio, including cluster enqueue, failure delivery during worker
liveness, ordinary exec/rerun recovery, finite exit without a successor, and
ordinary-job notification replay after client disconnect. The installed OMP
runtime loaded the actual extension; idle delivery and native user-stop and
interactive plan-mode suppression were exercised against a loopback provider
without external model calls.

The local harness substitutes SSH/Slurm/container-SIF-mount/Tailnet boundaries.
It does not establish actual GPU mapping, live Slurm/Tailnet operation or site
preemption behavior. Interactive Claude approval and actual channel
delivery/action remain gated by the preview/client/auth/organization
prerequisites above. The feature is isolated and unmerged, not a fleet
deployment; those external acceptance gates remain required.

---

## Source-of-truth note

This file and `README.md` must remain aligned with actual implementation behavior.
If behavior changes (keys, prompts, endpoints, state model), update both in the same change.