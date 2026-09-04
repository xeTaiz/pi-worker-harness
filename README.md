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

## Configuration

The extension uses HTTP endpoints (not CLI subprocesses).

- Default base URL: `http://orchestrator.hs.d0me.xyz:12889`
- Persisted config file:
  - Pi: `~/.pi/worker-harness/config.json`
  - OMP: `~/.omp/worker-harness/config.json`
  - key: `orchestratorUrl`
- Runtime command:
  - `/worker-harness-url` (show current URL + config path)
  - `/worker-harness-url http://host:port` (set URL and persist to config file)

## Tools

The extension exposes **two grouped tools plus the admin tools**. Subagent
configs use the grouped names to grant an entire tier with a single entry
(`tools: ["wh_read"]` / `tools: ["wh_dispatch"]`).

**Agent access policy:** agents must never invoke the `wh` CLI/binary from a
shell or call Worker Harness APIs directly. They must use only the provided
`wh_*` tools. If no provided tool supports an operation, the agent must report
the limitation rather than working around it through the CLI, API, or another
interactive Pi session. The `wh` CLI remains an operator-facing interface.

### `wh_read` (RO)
Inspect workers, jobs, tunnels, data paths, and Pi sessions.
- `list_workers`
- `get_worker` (worker_id)
- `get_worker_summary`
- `list_data`
- `list_jobs` (worker_id?, status?, origin_session_id?)
- `get_job_logs` (job_id; tail? | head? | follow?)
- `list_tunnels`
- `pi_sessions` (worker_id?)
- `pi_delegation` (delegation_id)

`wh_read` intentionally exposes no Marimo action. Marimo list/get responses
contain a Tailnet URL for an unauthenticated notebook server capable of
arbitrary Python execution, so those operations require `wh_dispatch`.

`list_data` is a shallow directory-to-worker catalog. Paths below
`/data/shared/<name>` identify the same deploy-managed network collection on
every advertising worker; `/data/local/<name>` is worker-specific; `/code`
contains repositories from the worker's configured code roots. Empty
collections and nested descendants are not indexed.

### `wh_dispatch` (RW and capability-bearing)
Mutations plus managed Marimo lifecycle and execution.
- `data_copy` (src_worker, src_path, dst_worker, dst_path, ttl_seconds?)
- `exec` (worker_id, command, name?, no_pty?, sync?, sync_timeout?)
- `stop_job` (job_id)
- `add_tunnel` (worker_id, local_port, remote_port, name?)
- `remove_tunnel` (tunnel_id)
- `workers_prune` (minutes?)
- `upload_file` (worker_id, path, content_b64)
- `download_file` (worker_id, path, max_bytes?)
- `delegate` (task, worker_id?, parent_session_id?, cwd?, timeout_seconds?, sync?)
- `grant_git_access` (worker_id, repo?)
- `list_marimo` (worker_id?)
- `get_marimo` (marimo_session_id)
- `start_marimo` (worker_id, notebook_path, environment, ready_timeout?)
- `stop_marimo` (marimo_session_id)
- `execute_marimo` (marimo_session_id, code)

Managed Marimo startup returns a direct `100.64.0.0/10` Tailnet URL. The user
must open that URL before execution because server readiness does not create a
browser kernel. `execute_marimo` resolves the browser's ephemeral kernel ID on
every call and reaches `/api/sessions` and `/api/kernel/execute` directly at the
returned URL; it does not use an orchestrator execution proxy.

Marimo operations are operator-side only and are not exposed to delegated
agents. The launcher intentionally uses `--no-token`; Tailnet reachability and
Headscale ACLs are the access boundary.

### Admin (individual, not subagent-eligible)
Image deployment and worker restart are exposed as their own tools and are not
folded into `wh_dispatch`:
- `wh_admin_deploy_image` (worker_id, image_path)
- `wh_admin_deploy_status` (transfer_id?)
- `wh_admin_deploy_cancel` (transfer_id)
- `wh_admin_restart` (worker_id)

## UI

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