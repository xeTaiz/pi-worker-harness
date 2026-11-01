# Worker-Harness HTTP API Specification

## Transport and authentication

The base URL is configured by `WH_ORCHESTRATOR_URL` (default `http://orchestrator.hs.d0me.xyz:12889`). Requests use `Authorization: Bearer …`. Ordinary sessions select `WH_API_KEY`, else the UTF-8 file `WH_API_KEY_FILE`; internal `orchestrator|pm|task` roles require `WH_SESSION_TOKEN` and never fall back to a human key. Missing credentials fail closed. Credential changes invalidate private caches. Only internal sessions register Pi bridge/relay hooks.

401 means unauthenticated; 403 denied capability; 404 missing or foreign private object; 409 conflict; 422 invalid input. Errors use `{detail: string}` or `{detail:{code,message}}`. A 409 queue move requires re-fetching the board and a new preview, not a blind retry.

## Ordinary endpoints

### Workers and shared data

- `GET /api/v1/workers`, `GET /api/v1/workers/:id`: explicit `PublicWorker` projections (GPU availability/utilization, CPU count and advertised shared data roots; no host addresses/processes).
- `GET /api/v1/workers/summary`
- `GET /api/v1/data?include_offline=`

### Owned jobs

- `POST /api/v1/jobs`: immediate CPU/setup; `{worker_id,command,name,expected_seconds,no_pty?,sync?,sync_timeout?,priority?,priority_note?,deadline_at?}`. Public name and positive estimate required; gpu_count is rejected.
- `POST /api/v1/jobs/queue`: GPU FIFO; `{worker_id,command,name,expected_seconds,gpu_count,no_pty?,priority?,priority_note?,deadline_at?}` with positive GPU count.
- `GET /api/v1/jobs?worker_id=&status=` and `GET /api/v1/jobs/:id`: owned details only, including execution_state and termination_reason.
- `PATCH /api/v1/jobs/:id/queue`: pending owner metadata `{worker_id?,name?,expected_seconds?,gpu_count?,priority?,priority_note?,deadline_at?}`. No position; worker transfer appends to destination.
- `PATCH /api/v1/jobs/:id`: active owner metadata `{expected_seconds?,priority?,priority_note?,deadline_at?}`.
- `GET /api/v1/jobs/:id/logs?tail=&head=` and `/logs/stream?tail=&poll_seconds=`: owned logs with active credential revalidation.
- `DELETE /api/v1/jobs/:id`: cancel/confirmed stop; unconfirmed termination is 503 and retains allocation.

Priority is `flexible|normal|blocking|time_critical` (normal default), never a sorting instruction. Time-critical needs a nonblank note; note length is at most 1000. Deadline is epoch seconds or null. Estimates are soft, not runtime limits. Internal zero estimates mean unknown.

### Shared scheduling board, reordering and usage

`GET /api/v1/jobs/queue?worker_id=` returns `{workers:[{worker_id,worker_name,revision,jobs,cpu_jobs}],as_of}`. GPU jobs are active then FIFO pending; CPU setup jobs are separate. Shared rows whitelist public name/owner, status/execution_state, priority, estimate and latest delay history. They never contain command, logs or private session fields.

Each worker is strict FIFO without backfill or automatic priority sorting. CPU work never occupies queue slots. Unknown/offline/overrunning allocations yield unknown ETA rather than invented readiness.

- `POST /api/v1/jobs/:id/queue/preview`: `{before_job_id:string|null,expected_revision,reason}`; null means end. Returns `{changed:false}` or `{changed:true,preview_id,revision,expires_at,before_order,after_order,impacts}`. Preview is read-only, expires after 60 seconds and is credential-bound.
- `POST /api/v1/jobs/:id/queue/move`: `{preview_id,expected_revision}`; returns `{event,revision}`. Same actor's successful repeated commit returns the same event.
- `GET /api/v1/jobs/queue/history?worker_id=&job_id=&before_event_id=&limit=`: `{events,next_before_event_id}` with public names/owner/actor snapshots and attributable impacts.
- `GET /api/v1/usage?worker_id=&user_id=`: `{rows,as_of,provisional}`. Rows expose job_seconds, cpu_job_seconds and allocated_gpu_seconds per user/worker, not utilization or quotas.

Positive delay entries display `+ceil(delay_seconds/60) minutes estimated delay because of JOB by OWNER`, with actor, reason and time. Null is `Moved later; delay estimate unavailable`; zero/negative values are not rendered as delays. Do not sum historical estimates into a current ETA.

### Owned tunnels

- `POST /api/v1/tunnels`: `{worker_id,local_port,remote_port,name?}`; local port 1024–65535, remote port 1–65535.
- `GET /api/v1/tunnels`: owned generic forwards only.
- `DELETE /api/v1/tunnels/:id`: foreign targets 404, Marimo-linked forwards 409 `MANAGED_BY_MARIMO`.

Raw TCP listeners are reachable on the trusted Tailnet; the target service must authenticate connections. Managed notebooks must use their protected proxy instead.

### Owned managed Marimo

- `POST /api/v1/marimo`: `{worker_id,notebook_path,environment,ready_timeout?}` (CPU-only).
- `GET /api/v1/marimo?worker_id=`, `GET /api/v1/marimo/:id`, `DELETE /api/v1/marimo/:id`.
- Responses contain lifecycle ID, worker/name, notebook/environment, linked job ID, status, owner, created_at and browser URL; no internal ports or native token.
- Discovery `/api/sessions`, `/sse` and execution `/api/kernel/execute` use the configured orchestrator plus `/api/v1/marimo/<encoded-lifecycle-id>/proxy`, with the bearer attached on every request. Never send a credential to the response's `url` authority. That URL is an owner-authenticated browser link only.

## Internal workflows

Existing Pi/session/bridge/attach, internal data-copy and file-transfer tools remain role-scoped internal workflows. Ordinary API keys, including an administrator-owned key, cannot use them. Web administration and key management require browser sessions, never API keys.
