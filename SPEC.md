# pi-worker-harness — Extension Specification (HTTP API)

## Overview

`pi-worker-harness` integrates the worker-harness orchestrator into pi via the orchestrator's HTTP API.
It provides:
- Tooling for workers, jobs, and tunnels
- A bottom overlay panel (`/worker-harness`) with Workers / Jobs / Tunnels tabs
- A status/widget line tracking selected worker/job/log context

This spec intentionally reflects the current HTTP-based architecture (not CLI subprocess execution).

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

### Jobs
- `POST /api/v1/jobs` with `{ worker_id, command, name? }` → `Job`
- `GET /api/v1/jobs` (optional `worker_id`, `status`) → `Job[]`
- `GET /api/v1/jobs/queue?worker_id=:id` → active `QueuedJob[]` in authoritative queue order
- `POST /api/v1/jobs/queue` with `{ worker_id, command, name, expected_seconds, gpu_count?, no_pty? }` → `Job`
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

Tools use `api.ts` helpers, not direct fetch or CLI subprocesses. Orchestrator
API, log-stream, and session-bridge requests include
`Authorization: Bearer $WH_SESSION_TOKEN` when the token is set.

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

## Source-of-truth note

This file and `README.md` must remain aligned with actual implementation behavior.
If behavior changes (keys, prompts, endpoints, state model), update both in the same change.