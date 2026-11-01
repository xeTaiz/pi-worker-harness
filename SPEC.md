# pi-worker-harness — Extension Specification (HTTP API)

## Overview

`pi-worker-harness` integrates the worker-harness orchestrator into pi via the orchestrator's HTTP API.
It provides:
- Tooling for workers, jobs, and tunnels
- A bottom overlay panel (`/worker-harness`) with Workers / Shared queue / My jobs / Tunnels / Marimo tabs
- A status/widget line below the editor, separate from the native OMP context-window display, showing fleet GPU totals or selected worker/job/log context

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
│   └── marimo.ts: owner-authorized orchestrator Marimo proxy transport
├── Role-scoped grouped tools
│   ├── ordinary API key: shared worker/queue/usage and owned jobs/tunnels/Marimo
│   ├── orchestrator: fleet_status, list_projects, pm_send
│   ├── project manager: compute/Marimo plus task lifecycle and escalation
│   └── task agent: compute/Marimo plus ask_pm and notify_pm
├── Event bus (events.ts)
├── TUI panel (panel/index.ts)
└── Widget/status (widget.ts)
```

`WH_SESSION_ROLE` is read at grouped-tool registration and actions outside the
role are omitted from the schemas and rejected at execution. Unknown roles
register no harness tools. Ordinary sessions receive no admin, Pi, file-transfer
or data-copy actions. Only verified internal role sessions with a session token
register bridge/relay hooks. Missing internal credentials fail without human-key
fallback. Orchestrators omit the compute panel/widget, commands and shortcut.

---

## HTTP API contract used by extension

Base URL is configured in `api.ts` (`getOrchestratorUrl` / `setOrchestratorUrl`).
`WH_ORCHESTRATOR_URL` wins over both persisted defaults and runtime URL changes.
All control-plane calls, bridge traffic, and log streams use that same base URL.
Internal roles (`orchestrator`, `pm`, `task`) use only `WH_SESSION_TOKEN`.
Ordinary sessions use `WH_API_KEY`, else the UTF-8 file `WH_API_KEY_FILE`.
Missing credentials and configured unreadable files fail closed. Credentials
are never persisted. Credential-dependent caches clear when identity changes.
Bridge registration reports `resume_path` from the agent's session manager;
the service persists it for subsequent PM/orchestrator relaunches.

### Workers
- `GET /api/v1/workers` → `PublicWorker[]`
- `GET /api/v1/workers/:id` → `PublicWorker` (no addresses/processes/paths except shared data roots)
- `GET /api/v1/workers/summary` → `WorkersSummary`

### Jobs, queue and usage
- `POST /api/v1/jobs` → CPU-only `OwnedJob`; requires worker, command, public name and positive expected_seconds, rejects gpu_count.
- `POST /api/v1/jobs/queue` → GPU `OwnedJob`; additionally requires positive gpu_count.
- `GET /api/v1/jobs` and `GET /api/v1/jobs/:id` → caller-owned details only.
- `PATCH /api/v1/jobs/:id/queue` → owner-only pending metadata edit; never accepts position.
- `PATCH /api/v1/jobs/:id` → owner-only active estimate/priority/note/deadline edit.
- Submissions and edits accept priority (`flexible|normal|blocking|time_critical`), priority_note and deadline_at. Time-critical needs a note; these fields never reorder.
- `GET /api/v1/jobs/queue` → `QueueBoard {workers:[{worker_id,worker_name,revision,jobs,cpu_jobs}],as_of}`. Public rows contain owner, public name, priority, estimates and delay history, never commands/logs.
- `POST /api/v1/jobs/:id/queue/preview` accepts before_job_id (null=end), expected_revision, reason; returns changed=false or a 60-second preview with ID/revision/impacts.
- `POST /api/v1/jobs/:id/queue/move` requires preview_id/expected_revision; 409 requires re-list and re-preview, never blind retry.
- `GET /api/v1/jobs/queue/history` returns attributable public events.
- `GET /api/v1/usage` returns owner/worker allocated wall/GPU totals (not utilization or quotas).
- Log GET/stream and DELETE stop are owner-only. Unconfirmed stop returns 503 and retains allocation; results show execution_state and termination_reason.

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

Kernel discovery, `/sse` attachment and `/api/kernel/execute` are transported
through `${WH_ORCHESTRATOR_URL}/api/v1/marimo/<encoded-lifecycle-id>/proxy`.
Every request carries the selected bearer, including SSE and discovery.
The lifecycle `url` is only an authenticated browser link, never an authority
to receive a key. The server owns the private native token and loopback listener.
Kernel attachment detaches after kernel-ready so a browser can become editor;
AbortSignal and output streaming remain supported. Managed startup is CPU-only.

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

Tools use authenticated `api.ts` helpers; Marimo proxy fetches use the same bearer.

### Registered actions

- Ordinary API key:
  - `wh_read`: `list_workers`, `available_gpus`, `get_worker`,
    `get_worker_summary`, `list_data`, `list_jobs`, `list_queue`,
    `get_job_logs`, `get_usage`, `get_queue_history`, `list_tunnels`
  - `wh_dispatch`: `exec`, `enqueue`, `update_queued_job`,
    `preview_queue_move`, `move_queued_job`, `stop_job`, `add_tunnel`,
    `remove_tunnel`, `list_marimo`, `get_marimo`, `start_marimo`,
    `stop_marimo`, `execute_marimo`
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

All five Marimo actions remain on `wh_dispatch` because they expose Python
execution. A task agent never pushes or opens a pull request; a project manager
reviews the worktree diff before `submit_pr`. Raw TCP tunnels are trusted-Tailnet
exposures whose target service must authenticate.

### Required tool behaviors
- Successful Marimo start/stop emits `worker-harness:refresh`; list/get/execute does not
- `execute_marimo` refreshes `/api/sessions` on each call, forwards aborts and output updates, and never accepts an ephemeral kernel-session ID
- Errors return structured tool errors from API errors (`status`, `code`, `message`, `detail`)
- `wh_dispatch exec` (sync mode), `wh_dispatch add_tunnel`, and Marimo actions include resource/result metadata in tool result details
- All grouped tools render their call header via `renderCall` so the action and key args appear in the TUI without expanding the JSON args block

### Queueing contract

- One strict FIFO queue exists per worker. The submitting agent selects the worker.
- Consecutive heads may run when their requests fit; a blocked head blocks all later work (no backfill or priority sorting).
- Positive expected_seconds is a soft estimate, not a limit; internal unknown estimates display as unknown. Physical assignments set CUDA_VISIBLE_DEVICES.
- CPU work uses immediate `exec` with an empty GPU allocation; GPU work uses `enqueue` and strict FIFO.
- Metadata/logs/stop are owner-only (administrators have no ownership override).
- Any ordinary user may reorder same-worker pending jobs only through reasoned preview/commit, audited atomically by revision.
- Public delay entries show `+ceil(delay_seconds/60) minutes estimated delay because of JOB by OWNER`, actor, reason and time. Null delay is unavailable; zero/negative values are not delay claims.
- A running GPU allocation can be idle-stopped after 600 seconds of valid display-exempt idle observations only when GPU work waits; release requires confirmed process exit and a fresh idle sample.
- Queue changes emit refresh events; shared rows never expose command text. The panel has CPU-default submission with required name/duration and a separate shared queue.

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
- Shared queue is public metadata only (owner, priority, ETA/unknown reason and delay history).
- My jobs actions are owner-only: `f` streams active logs or loads saved terminal logs; `s` stops/cancels.
- Workers/My jobs: `n` opens the CPU-default form; `t` creates a tunnel.
- Tunnels: `d` removes an owned generic forward; help warns about trusted-Tailnet exposure.
- Marimo: `m` creates a CPU notebook; `o` opens its protected browser link; `s` stops it.

### Inline input modes (no env-var dependency)

Credentials are configured as above; interactive action inputs need no extra
environment variables.

#### New job (`n`)
- Prompt inline for shell command, public name and positive expected duration.
- Choose CPU (default) or GPU; ask for positive GPU count only for GPU mode.
- Submit CPU via `/api/v1/jobs`, GPU via `/api/v1/jobs/queue`.
- Reconcile with owner-scoped job refresh; foreign shared queue rows cannot invoke logs/stop.

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