# pi-worker-harness

`pi-worker-harness` integrates a running worker-harness HTTP orchestrator into Pi and OMP.

## Requirements

- Pi v0.70+ or OMP
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

## Configuration

The extension uses HTTP endpoints (not CLI subprocesses).

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
Pinned above editor. Tracks selected worker/job and updates from streamed log lines.

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