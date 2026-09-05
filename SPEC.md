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

```
pi session
├── API clients
│   ├── api.ts: orchestrator `/api/v1/*` lifecycle requests
│   └── marimo.ts: direct Tailnet `/api/sessions` and `/api/kernel/execute`
│
├── Tools (tools/*.ts)
│   ├── wh_read (RO, action-based)
│   │   ├── list_workers
│   │   ├── get_worker
│   │   ├── get_worker_summary
│   │   ├── list_data
│   │   ├── list_jobs
│   │   ├── list_queue
│   │   ├── get_job_logs
│   │   ├── list_tunnels
│   │   ├── pi_sessions
│   │   └── pi_delegation
│   ├── wh_dispatch (RW and capability-bearing, action-based)
│   │   ├── data_copy
│   │   ├── exec
│   │   ├── enqueue
│   │   ├── update_queued_job
│   │   ├── stop_job
│   │   ├── add_tunnel
│   │   ├── remove_tunnel
│   │   ├── workers_prune
│   │   ├── upload_file
│   │   ├── download_file
│   │   ├── delegate
│   │   ├── grant_git_access
│   │   ├── list_marimo
│   │   ├── get_marimo
│   │   ├── start_marimo
│   │   ├── stop_marimo
│   │   └── execute_marimo
│   └── wh_admin_* (admin, individual tools, not subagent-eligible)
│       ├── wh_admin_deploy_image
│       ├── wh_admin_deploy_status
│       ├── wh_admin_deploy_cancel
│       └── wh_admin_restart
│
├── Event bus (events.ts)
│   └── worker-harness:refresh (+ future typed domain events)
│
├── TUI Panel (panel/index.ts)
│   ├── Tabs: Workers / Jobs / Tunnels
│   ├── Inline input modes: new job command, tunnel port input
│   ├── Follow stream for running jobs
│   └── Saved-log fallback for terminal jobs
│
└── Widget/status (widget.ts)
    └── tracked worker/job + last log line summary
```

---

## HTTP API contract used by extension

Base URL is configured in `api.ts` (`getOrchestratorUrl` / `setOrchestratorUrl`).

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

Kernel execution does not use the orchestrator API. `marimo.ts` fetches
`/api/sessions` and posts to `/api/kernel/execute` at the lifecycle resource's
returned Tailnet URL. The user must open the URL before a browser kernel exists.
The launcher uses `--no-token`; Tailnet reachability and Headscale ACLs are the
access boundary.

---

## Tools

Tools must use `api.ts` helpers, not direct fetch or CLI subprocesses.

The extension exposes **only two grouped tools** (`wh_read`, `wh_dispatch`) for
all worker-harness operations, plus the `wh_admin_*` individual tools for image
deployment and worker restart. Subagent configs can grant a full tier with one
name (`tools: ["wh_read"]` / `tools: ["wh_dispatch"]`).

`wh_read` exposes no Marimo action. Even lifecycle list/get reveal a direct URL
to an unauthenticated notebook server capable of arbitrary Python execution, so
all five Marimo actions stay on `wh_dispatch`. Marimo is operator-side only and
is not available to delegated agents.

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