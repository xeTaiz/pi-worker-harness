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
- Ordinary authentication: `WH_API_KEY`, or a UTF-8 bearer file named by
  `WH_API_KEY_FILE` (environment key wins). Obtain a personal key from the web UI.
  Keep credentials outside agent-readable roots; missing credentials fail closed.
- Internal roles (`WH_SESSION_ROLE=orchestrator|pm|task`) require
  `WH_SESSION_TOKEN` and never fall back to a human key. Only these sessions
  register the Pi bridge/relay; ordinary keys never publish Pi discovery.
- Private caches are cleared on credential changes. Credentials are not persisted
  in extension configuration.
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

### Ordinary API-key session (`WH_SESSION_ROLE` unset)

- `wh_read`: `list_workers`, `available_gpus`, `get_worker`,
  `get_worker_summary`, `list_data`, `list_jobs`, `list_queue`, `get_job_logs`,
  `get_usage`, `get_queue_history`, `list_tunnels`
- `wh_dispatch`: `exec`, `enqueue`, `update_queued_job`, `preview_queue_move`,
  `move_queued_job`, `stop_job`, `add_tunnel`, `remove_tunnel`, `list_marimo`,
  `get_marimo`, `start_marimo`, `stop_marimo`, `execute_marimo`

API keys cannot administer workers/users/keys, transfer files, copy data, or
access Pi sessions. An administrator's key has the same ordinary capabilities.
Private jobs/logs, tunnels and notebooks are owner-only; shared queue rows
contain public names, owners, priority, estimates and attributable delay history.

`exec` is CPU-only and requires a public `name` and positive `expected_seconds`.
`enqueue` additionally requires positive `gpu_count`. Priority, priority note
and deadline annotate scheduling but never sort FIFO. `update_queued_job` requires
`update_status=pending|active`: pending edits queue metadata, active edits only
duration/priority/note/deadline of starting/running work. Neither reorders.
Reordering requires a reason, preview, and explicit commit with its preview
ID/revision. After a 409, re-list and re-preview rather than blind retry.
Positive delays display rounded-up minutes; unknown timing stays explicit.
Usage is allocated wall/GPU time, not utilization or quota.

Raw TCP tunnels expose their destination service to the trusted Tailnet;
the destination must provide authentication. Managed notebooks instead use the
owner-authenticated proxy.

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

All five Marimo actions remain on `wh_dispatch` because they expose Python
execution. Managed startup is CPU-only. GPU notebooks use queued GPU commands.
Discovery, SSE attachment and execution go through the configured orchestrator's
`/api/v1/marimo/<lifecycle-id>/proxy/` subtree with the current bearer on every
request, never through the returned browser link as a transport authority.
The private native token is managed by the server and is never returned to tools.
Kernel lookup, saved-cell hydration, output streaming and cancellation are
preserved; opening the returned browser link requires the owner's web login.

## UI

The orchestrator (and unknown roles) does not register the compute panel, widget,
or associated commands/shortcut.

### Status widget
Pinned below the editor so the native OMP context-window display remains unobstructed. Shows worker/job/GPU totals when no worker is selected; otherwise tracks the selected worker/job and streamed log lines.

### Panel (`/worker-harness`)
Tabs: Workers / Shared queue / My jobs / Tunnels / Marimo

Key bindings:
- `j` / `↓`: down
- `k` / `↑`: up
- tab switching: `Tab`, `Shift+Tab`, `h`/`l`, `←`/`→`, `w`/`b`
- Shared queue shows public owner/priority/ETA/delay metadata, never commands or logs.
- `f`: follow an owned job's logs (My jobs); finished jobs load saved logs.
- `s`: stop an owned job (My jobs), or stop the selected owned notebook (Marimo).
- `n`: staged new-job form: command, required public name, positive duration,
  CPU/GPU mode (CPU default), GPU count only for GPU mode
- `t`: staged prompt flow on selected worker/job worker (workers/jobs tabs): remote port (required) → local port (optional, blank = random) → tunnel name (optional) → create
- `d`: remove the selected generic tunnel (Tunnels).
- `m`: create an owned CPU notebook (Marimo); `o`: open its authenticated browser link.
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