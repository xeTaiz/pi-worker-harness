# pi-worker-harness

`pi-worker-harness` integrates a running worker-harness HTTP orchestrator into Pi,
OMP, and Claude Code (Bun MCP stdio).

## Requirements

- Pi v0.70+ or OMP; alternatively Bun for the Claude MCP server
- worker-harness server reachable at `http://orchestrator.hs.d0me.xyz:12889` (default)

## Installation

Install for Pi:

```bash
pi install git:github.com/xeTaiz/pi-worker-harness
```

Install for OMP:

```bash
omp plugin install github:xeTaiz/pi-worker-harness
```

Update the Pi installation with `pi update git:github.com/xeTaiz/pi-worker-harness`.
Update the OMP installation by repeating its install command. Restart the agent
after installation or update so the extension module is initialized.

For pinned rollouts, package the reviewed checkout with `npm pack` and install the
resulting `pi-worker-harness-0.1.4.tgz` artifact rather than resolving a moving
branch. Record its checksum and install the same artifact on every host. A
running agent keeps its loaded module: restart after installation, and confirm
`omp plugin list` reports `pi-worker-harness@0.1.4` before launching fleet roles.

### Local Slurm feature checkout and replacement

The standalone root handoff is [`specs/local-slurm.md`](../../specs/local-slurm.md)
in the Worker Harness repository. It includes the complete CPU/GPU configuration,
control/runtime rollout, shared-storage and environment preparation, security,
local evidence, and live IBEX acceptance gates. This extension is also usable
standalone, but installing it alone does not add server reservation/event support
or the new SIF runtime.

`feature/local-slurm` is the transfer branch in both repositories; the root pins
the exact extension commit. Clone `git@github.com:xeTaiz/worker-harness.git` with
`--branch feature/local-slurm --recurse-submodules`. The extension origin is
`git@github.com:xeTaiz/pi-worker-harness.git`; keep the root-pinned detached
submodule checkout for a reviewed matching pair.

Record `omp plugin list` and preserve the old install source/version/path for
rollback. Replace an existing installation explicitly, rather than relying on a
link overwriting it:

C1
```bash
omp plugin uninstall pi-worker-harness
cd /absolute/path/to/worker-harness-local-slurm/extensions/pi-worker-harness
bun install
omp plugin link /absolute/path/to/worker-harness-local-slurm/extensions/pi-worker-harness
omp plugin list
```

Restart OMP completely. Rollback means uninstalling this link, reinstalling or
linking the preserved old source, and restarting; preserve personal OMP/model
login state. A linked development checkout is not a pinned fleet package.

## Configuration

Ordinary worker operations use HTTP endpoints (not the worker-harness CLI).
Configured cluster targets additionally use the submitting client's local SSH.

- Default base URL: `http://orchestrator.hs.d0me.xyz:12889`
- `WH_ORCHESTRATOR_URL` overrides the persisted default and runtime URL command.
  API calls, bridge registration/events/command polling/acknowledgements, and log
  streams all use this same normalized base URL.
- Ordinary operator authentication: `WH_OPERATOR_TOKEN`, or a UTF-8 bearer file
  named by `WH_OPERATOR_TOKEN_FILE`. Keep the file outside agent-readable roots.
  Fleet roles use `WH_SESSION_TOKEN` and never fall back to operator credentials.
  Unauthenticated ordinary sessions only work against an explicitly unconfigured
  development server; a configured fleet requires an operator credential.
- Persisted config file:
  - Pi: `~/.pi/worker-harness/config.json`
  - OMP: `~/.omp/worker-harness/config.json`
  - key: `orchestratorUrl`
- Runtime command:
  - `/worker-harness-url` (show current URL + config path)
  - `/worker-harness-url http://host:port` (set URL and persist to config file)

## Claude Code MCP and job notifications

Run `bun install` in this extension directory before launching the standalone
server. It pins `@modelcontextprotocol/sdk` to `1.32.2` ([npm registry](https://registry.npmjs.org/@modelcontextprotocol/sdk/1.32.2)).
Register the reviewed absolute checkout path as server `worker-harness`:

```json
{
  "mcpServers": {
    "worker-harness": {
      "command": "bun",
      "args": ["/absolute/path/to/pi-worker-harness/mcp.ts", "--client-key", "my-project"],
      "env": { "WH_ORCHESTRATOR_URL": "http://orchestrator.hs.d0me.xyz:12889" }
    }
  }
}
```

Supply the same control credentials described above through the process
environment or a private token file, not command arguments. `--client-key NAME`
is required: use a stable, distinct name for each independently active Claude
session. Reusing that key on restart resumes its persisted recipient and event
cursor; do not concurrently share a key between clients.

The standalone server exposes only shared compute `wh_read` and `wh_dispatch`
handlers: worker/cluster discovery, jobs/queue/logs, data/file operations,
tunnels and managed Marimo. Pi-session/fleet messaging, worker pruning,
`grant_git_access` and `wh_admin_*` are neither advertised nor dispatchable. It imports no Pi/TUI
runtime. Stdout is exclusively MCP protocol; diagnostics go to stderr.

For **interactive** custom-channel testing during the research preview:

```bash
claude --dangerously-load-development-channels server:worker-harness
```

Accept Claude's development-channel warning and MCP consent. Channels require a
compatible authenticated Claude Code session and remain subject to preview
availability and organization `channelsEnabled` policy. The development flag
bypasses only the custom-channel allowlist; it is ignored with `-p` and the Agent
SDK. See the [official channel reference](https://code.claude.com/docs/en/channels-reference).
Successful stdio transport delivery alone does not prove Claude received or
acted on an event.

OMP delivers ordinary and Slurm job events as native asides with a requested
turn, respecting the runtime's user-stop and plan-mode suppression. Claude uses
`notifications/claude/channel` after the MCP initialization handshake. Both
clients reuse one session recipient for tools, SSE replay and local scheduler
polling. Ordinary submitted jobs also receive that recipient. Streams and polls
stop when the client closes; credentials/URL changes replace the active
connection. Returned clients replay after their durable cursor.

Events include worker/job/event identifiers and bounded failure/log/deadline
context. Logs are untrusted workload content, not instructions. A transport
handoff advances the cursor, not an acknowledgement that the agent acted; crash
replay is possible and failure deadlines never wait for acknowledgement. Inspect
ordinary job state before diagnosing with `exec` or rerunning with `enqueue`.
Recipient routing is for today's single-user service, **not multi-user privacy**.

### Local Slurm through ordinary tools

Configure `WH_CLUSTER_CONFIG` or
`${XDG_CONFIG_HOME:-$HOME/.config}/worker-harness/clusters.json` with the version-1
cluster profiles described in the root Worker Harness README. `list_workers`
includes `cluster:<profile-id>` targets without pretending they are online
workers; `get_worker` refreshes scheduler evidence. `available_gpus` labels
cluster availability separately from free fleet GPUs.

Use `enqueue` with a cluster target and `slurm.project_path` plus an explicit
`slurm.walltime_seconds`; optional `slurm.environment_path` selects a tested
shared environment. GPU profiles default to one GPU with proportional configured
CPU/RAM sizing; CPU profiles require zero GPUs and default to 1 CPU/4096 MiB.
Shared project/environment paths must resolve beneath
`/data/shared/<storage_id>`. The project and optional environment are persisted in
the actual queued command, not only the batch shell.

Prepare source/data and uv environments through ordinary transfer/exec or a CPU
target's enqueue. Use the common SIF and shared storage, not copied virtualenvs or
per-experiment images. There is no separate prepare/submit tool or self-chaining
worker. `exec`, file, tunnel and Marimo operations require an actual worker ID.
Enqueue returns the ordinary job/reserved-worker IDs, scheduler evidence and
shared paths. Preserve `submission_key` when recovering an uncertain submission;
read-only inspection never submits a replacement. `stop_job` defaults to one job;
`scope:"allocation"` cancels its verified allocation. A finished allocation
requires fresh enqueue, not a hidden successor.

Local state lives in `WH_CLUSTER_STATE_DIR` or
`${XDG_STATE_HOME:-$HOME/.local/state}/worker-harness/slurm`, namespaced by
orchestrator/client/session with private files. Client closure pauses local
observations, not remote useful work or its finite shutdown policy.

### Add a cluster interactively

C2
```text
/cluster-add
/cluster-add ibex-cpu
```

The optional argument supplies the new profile ID. This is an interactive
Pi/OMP command, not a new `wh_dispatch` or MCP compute action. It validates a
complete version-1 profile, offers review/edit/cancel, and requires explicit
confirmation of the initially unencrypted login-key warning before any file
writes, key generation, or process spawning. It never logs in, installs a key
remotely, changes host trust, or submits an allocation.

Prompts cover a unique ID/description, CPU or GPU kind, connection reuse or
alias/direct SSH destination, shared storage ID/root and SIF, runtime/setup,
account and **actual** partition, GPU type/maximum for GPU only, walltime,
failure/termination grace, enrollment key file/login server, and registration
host. CPU profiles require `max_gpus:0` and omit `gpu_type`. Advanced options
are QoS, feature constraint, CPU/RAM caps, warning seconds (default 120),
registration port (12888), and polling seconds (15); `-` clears an optional
value. A site's actual values and permissions must come from its operator, not
guessed IBEX partition names.

Profiles are saved to `WH_CLUSTER_CONFIG` or the XDG configuration path above.
All onboarded clusters share **one** managed login identity:
`dirname(clusters.json)/id_ed25519` and `id_ed25519.pub`, plus
`dirname(clusters.json)/known_hosts` (default `~/.config/worker-harness`).
These shared files are directly in the configuration parent, not `ssh/`, so
valid legacy profile IDs cannot collide with them. Connection-specific
`config` and `bootstrap_config` remain under `ssh/<id>/`; connection reuse can
retain the original profile's config path. Directories are 0700/files 0600.
Existing shared key/trust files are reused, never overwritten. The profile's
`ssh_config_file` selects argv `-F` without modifying `~/.ssh/config`.

Alias mode retains ordinary user SSH/ProxyJump/ControlMaster policy.
`/etc/ssh/ssh_config` is included only if readable/regular, root/current-UID
owned, and not group/world writable; otherwise a durable warning directs
required site options into user config. Strict OpenSSH checks are not weakened.
Direct mode creates `wh-<id>` with supplied hostname/user/port and optional
ProxyJump. Reuse offers storage/SIF/runtime/enrollment defaults with retained-path
confirmation. CPU/GPU profiles sharing storage use the same `storage_id`,
`shared_root`, and SIF but their real authorized partitions.

Both dedicated and bootstrap configs explicitly select the shared
`UserKnownHostsFile`. An empty 0600 configuration-parent `known_hosts` is created only if absent;
existing trust is not replaced, cleared or automatically imported. Verify the
host's fingerprint against an independent site source before accepting it with
the printed bootstrap first-connect command. Copy existing entries only after
reviewing their independent verification; `ssh-keyscan` alone is not proof.
Install **only the shared public key** with the printed
`ssh-copy-id -F <bootstrap_config> -i <shared-public-key> <alias>` or manual
authorized_keys alternative. A new profile's bootstrap preserves existing
key/agent/password/MFA authentication and target/includes without imposing the
shared IdentityFile/IdentitiesOnly restriction. A migrated profile's bootstrap
retains its **old per-profile IdentityFile without IdentitiesOnly**, allowing
old-key-only hosts to authorize the shared public key. The final probe uses
dedicated `config`, shared identity and `IdentitiesOnly`, with `BatchMode=yes`,
`StrictHostKeyChecking=yes`, and `ConnectTimeout=10`. Commands are printed,
not executed. Never bypass missing/changed host trust.

A newly generated shared private key initially has no passphrase; an existing
shared key (including an encrypted one) is preserved. Add a passphrase in your
own terminal with `ssh-keygen -p -f <shared-private-key>` if desired, and unlock
through an accessible dedicated agent. Read the sandbox/agent gate below first:
the current launcher unsets `SSH_AUTH_SOCK`. BatchMode cannot unlock a key or
answer MFA. Profile discovery alone does not establish login readiness.

#### Migrate existing per-profile SSH setup

After the full sandbox restart below, run `/cluster-ssh-migrate` interactively.
If the shared key is absent, explicitly select one existing wizard-managed key
and confirm its exclusive byte-copy into the shared path; encrypted keys retain
their encryption. An existing shared key is always reused. Legacy keys are not
deleted. Only recognized wizard-generated configs are rewritten; external or
unrecognized configs are explicitly skipped, not guessed. Review the report.

Migration retains destination/alias policy, shared host trust and each old
profile's IdentityFile in bootstrap without IdentitiesOnly; dedicated configs
select only the shared identity. It prints bootstrap first-connect,
shared-public-key installation and strict dedicated-config probes for each
distinct migrated host. Install this **shared public key** on each affected
account that previously accepted a different profile key.
Host trust is not imported automatically; manually review verified entries or
verify each host interactively into the shared known_hosts. New mounts alone
cannot migrate old profiles or authorize the shared identity remotely.

This login key is **not** `tailnet_key_file` (a separately provisioned restricted
ephemeral allocation-enrollment key) or the WH operator/session control bearer.
Keep all secret files outside Git. The human client talks to control **12889**;
allocated workers register on **12888**, with orchestrator-to-worker Tailnet SSH
**22** allowed. Do not grant workers privileged control access. Host mounts,
shared-identity proof on an existing participating worker, a one-time updated
candidate SIF build, matching deployed control/runtime, and site/charged-submit
approval remain prerequisites; onboarding does not provision them.

### Sandbox persistence and required restart

The actual allowlist at
`~/dotfiles/agent-sandbox/.config/agent-sandbox/paths` includes both
`~/.config/worker-harness` and `~/.local/state/worker-harness`. Keep profiles,
managed SSH identity/host trust **and** state journals/cursors persistent.
The launcher skips absent host sources. In a **host terminal outside the
existing sandbox**, before launching its replacement:

```bash
mkdir -p "$HOME/.config/worker-harness" "$HOME/.local/state/worker-harness"
chmod 700 "$HOME/.config/worker-harness" "$HOME/.local/state/worker-harness"
```

Creating these inside the current masked HOME does not establish persistent
host directories. Their absence inside this sandbox says nothing about existing
host configuration. For `XDG_CONFIG_HOME`, `WH_CLUSTER_CONFIG`, `XDG_STATE_HOME`
or `WH_CLUSTER_STATE_DIR` overrides, allowlist and create the **actual** host
configuration/SSH and state paths; the default entries do not cover arbitrary
overrides.

For the new host launch, set
`AGENT_SANDBOX_PATHS="$HOME/dotfiles/agent-sandbox/.config/agent-sandbox/paths"`,
or verify that the host's active paths-file/symlink resolves to this edited
file. The active host configuration/symlink has not been observed here.

**Exit and restart agent-sandbox, then start OMP there. Restarting only OMP
cannot apply changed mounts.** Existing profiles require `/cluster-ssh-migrate`
after restart; new path mounts alone do not repair legacy SSH configuration.
Stop at this restart gate before attempting the real login test.

The current launcher unsets `SSH_AUTH_SOCK`. A passphrase-protected shared key
therefore needs deliberately provisioned **dedicated** agent/socket access
inside the new sandbox, explicitly selected for SSH. Host `ssh-add` and an OMP
restart alone do not provide it; even a sandbox restart under the unchanged
launcher does not forward an agent. Do not forward the general host agent or
change launcher policy as part of this setup.

The main agent's actual-launcher/private-HOME smoke passed with the modified
allowlist: both mounts were visible, writes persisted to the outside fixture,
unrelated `~/.ssh` stayed hidden and `SSH_AUTH_SOCK` stayed unset. It did not
create real host directories or keys. The final native extension suite passed
**361 tests, zero failures**, 2,764 expectations/14 files (2.19 seconds).
Actual **OMP 18.14.2 shared-SSH onboarding/migration smoke passed** in 32.39
seconds, including native `/export` message handoff evidence (details below).
Live login/cluster proof remains pending full sandbox restart, migration,
independent fingerprint verification and the strict probe. Local proofs do not
authorize charged work.

## Tools

The extension exposes role-scoped `wh_read` and `wh_dispatch` action schemas.
Actions outside `WH_SESSION_ROLE` are omitted at registration and rejected by
execution handlers. Unknown roles register no harness tools. All Worker Harness
HTTP requests carry the selected bearer, including bridge traffic and log streams.

### Normal operator session (`WH_SESSION_ROLE` unset)

- `wh_read`: `list_workers`, `available_gpus`, `get_worker`,
  `get_worker_summary`, `list_data`, `list_jobs`, `list_queue`, `get_job_logs`,
  `list_tunnels`, `pi_sessions`
- `wh_dispatch`: `data_copy`, `exec`, `stop_job`, `enqueue`,
  `update_queued_job`, `add_tunnel`, `remove_tunnel`, `workers_prune`,
  `upload_file`, `download_file`, `grant_git_access`, `list_marimo`,
  `get_marimo`, `start_marimo`, `stop_marimo`, `execute_marimo`
- Administration: `wh_admin_deploy_image`, `wh_admin_deploy_status`,
  `wh_admin_deploy_cancel`, `wh_admin_restart`

### Orchestrator

- `wh_read`: `fleet_status`, `list_projects`
- `wh_dispatch`: `pm_send`

The orchestrator routes project work to a project manager and has no compute,
Marimo, file, tunnel, git, or administrative actions.

### Project manager

- `wh_read`: `list_workers`, `available_gpus`, `get_worker`,
  `get_worker_summary`, `list_data`, `list_jobs`, `list_queue`, `get_job_logs`,
  `pi_sessions`
- `wh_dispatch`: `data_copy`, `exec`, `stop_job`, `enqueue`,
  `update_queued_job`, `list_marimo`, `get_marimo`, `start_marimo`,
  `stop_marimo`, `execute_marimo`, `dispatch_task`, `answer`, `submit_pr`,
  `teardown_task`, `escalate`

The project manager reviews a task agent's worktree diff before `submit_pr`.
`escalate` uses the orchestrator mailbox, which starts an absent orchestrator
before delivering the message instead of selecting a stale session from the roster.

### Task agent

- `wh_read`: `list_workers`, `available_gpus`, `get_worker`,
  `get_worker_summary`, `list_data`, `list_jobs`, `list_queue`, `get_job_logs`
- `wh_dispatch`: `data_copy`, `exec`, `stop_job`, `enqueue`,
  `update_queued_job`, `list_marimo`, `get_marimo`, `start_marimo`,
  `stop_marimo`, `execute_marimo`, `ask_pm`, `notify_pm`

A task agent edits only its allocated worktree. It never pushes or opens a pull
request; it asks or notifies its project manager when coordination is needed.

### Managed Marimo behavior

Marimo list/get stays on `wh_dispatch` because responses contain a Tailnet URL
for an unauthenticated notebook server capable of arbitrary Python execution.
Managed startup returns a direct `100.64.0.0/10` Tailnet URL, attaches once to
spawn the kernel, waits for `kernel-ready`, detaches, and runs saved cells.
`execute_marimo` re-resolves the kernel on every call and directly reaches the
returned notebook URL; it does not use an orchestrator execution proxy.

The launcher uses `--no-token`; Tailnet reachability and Headscale ACLs are the
Marimo access boundary.

## UI

The orchestrator (and unknown roles) does not register the compute panel, widget,
or associated commands/shortcut.

### Status widget
Pinned below the editor so the native OMP context-window display remains unobstructed. Shows worker/job/GPU totals when no worker is selected; otherwise tracks the selected worker/job and streamed log lines.

### Panel (`/worker-harness`)
Tabs: Workers / Jobs / Tunnels

Key bindings:
- `j` / `↓`: down
- `k` / `↑`: up
- tab switching: `Tab`, `Shift+Tab`, `h`/`l`, `←`/`→`, `w`/`b`
- `f`: follow selected job logs (jobs tab); for finished jobs, loads recent saved logs instead of streaming
- `s`: stop selected running job (jobs tab)
- `n`: staged prompt flow on selected worker/job worker (workers/jobs tabs): command (required) → job name (optional) → start
- `t`: staged prompt flow on selected worker/job worker (workers/jobs tabs): remote port (required) → local port (optional, blank = random) → tunnel name (optional) → create
- `r`: refresh
- `q` / `Esc`: close panel

## Notes from validation

### Local Slurm and client proof

Prior integrated suites passed 336 root Python tests (one existing Starlette
warning) and 278 Bun tests (zero failures, 1,388 expectations/13 files).
Historical per-profile onboarding/SSH-portability `bun test` passed **345
tests**, zero failures, 2,341 expectations/14 files (1.62 seconds), without bwrap
or a system-config fixture. Historical native interactive OMP onboarding also
passed (15.14 seconds): isolated plugin replacement/relink and discovery,
cancel without artifacts, save preserving existing profiles, real Ed25519
public-key/fingerprint/permissions, bootstrap/dedicated OpenSSH `-G` resolution,
and visible durable public setup instructions.

Both historical per-profile proofs ran without bwrap or system-config substitution. The production
guard omitted this host's unsafe UID-65534-owned 0644 system SSH config with a
durable warning; no product permission bypass or host system edit was used.
OMP startup provider/catalog CONNECT attempts were recorded and denied locally
with 403 before TLS; there were zero forwarded connections/model completions,
and no real cluster SSH connection or submission occurred.
The updated candidate SIF has not been built/executed, and no live IBEX/charged
submission has been performed.
Current sandbox mount-fixture proof passed as recorded above. The final native
extension suite passed **361 tests, zero failures**, 2,764 expectations/14 files
(2.19 seconds). Actual **OMP 18.14.2 shared-SSH smoke passed in 32.39 seconds**:
normal plugin link/discovery; `/cluster-add` cancel and two independent
profiles sharing one configuration-root key and explicit managed host trust;
migration cancel and four legacy profiles including IDs `id_ed25519` and
`known_hosts`; selected-key promotion preserving its fingerprint, old keys,
external config and ordinary SSH config; real OpenSSH `-G` for both configs;
and repeat migration retaining the old bootstrap identity.

Before a first model turn native OMP creates no JSONL journal. Its supported
`/export` exposed the actual persisted native `custom_message` entries, IDs,
commands and warnings as runtime-handoff evidence; no model call was needed.
Startup CONNECT attempts were all locally blocked with 403. No SSH network
connection or real host-trust autoacceptance occurred. Real login/IBEX proof
still requires the full sandbox restart and explicit live gates above.

Isolated local validation exercised the real control API, database, queue,
daemon and job runner through actual Bun MCP stdio: cluster discovery/enqueue,
failure notification while the worker remained online, ordinary exec diagnosis
and CPU rerun, finite shutdown without a successor, and ordinary-job event
delivery/disconnected replay. The installed OMP runtime loaded the actual
extension and delivered an idle job event without a new prompt; native user-stop
and interactive plan-mode suppression were also exercised with a loopback model
provider, without external model calls.

SSH, Slurm scheduling, container/SIF mounting and Tailnet enrollment were local
test boundaries, not live-cluster proof. Actual GPU allocation, live Slurm and
Tailnet behavior, site preemption, and interactive Claude custom-channel
approval/delivery remain external acceptance gates. This feature remains in an
isolated, unmerged worktree and has not been deployed to the fleet.

### Earlier ordinary HTTP integration

Integration was validated against `http://orchestrator.hs.d0me.xyz:12889` for:
- health
- workers list
- jobs list
- tunnels list
- start job
- stop job
- logs fetch
- logs stream endpoint
- tunnel create/delete