# Worker-Harness HTTP API Specification

## Base URL
`http://orchestrator.hs.d0me.xyz:12889`

## Endpoints
### Workers
- `GET /api/v1/workers`
- `GET /api/v1/workers/:id`
- `DELETE /api/v1/workers/prune`
- `GET /api/v1/workers/summary`

### Jobs
- `POST /api/v1/jobs`
- `GET /api/v1/jobs`
- `GET /api/v1/jobs/queue?worker_id=:id`
- `POST /api/v1/jobs/queue`
- `PATCH /api/v1/jobs/:id/queue`
- `GET /api/v1/jobs/:id/logs`
- `GET /api/v1/jobs/:id/logs/stream`
- `DELETE /api/v1/jobs/:id`

#### Scheduled GPU queue

`POST /api/v1/jobs/queue` accepts:

- `worker_id`: selected worker ID or name
- `command`: shell command
- `name`: required nonblank display name
- `expected_seconds`: required positive duration estimate
- `gpu_count`: positive GPU count, default `1`
- `no_pty`: optional boolean

`GET /api/v1/jobs/queue` returns active queue-managed jobs in authoritative server order. `position` is `0` for `starting` and `running` jobs and one-based among `pending` jobs on the same worker.

`PATCH /api/v1/jobs/:id/queue` accepts at least one of `worker_id`, one-based `position`, `name`, `expected_seconds`, or `gpu_count`. Only pending jobs can be changed. Moving without a position appends to the destination queue; positions beyond its length clamp to the tail.

Each worker has a strict FIFO queue. Consecutive heads may run concurrently when enough telemetry-free GPUs exist. A head requesting more GPUs than are currently free blocks every later job; the server never backfills around it. Assigned physical GPU indices are exported through `CUDA_VISIBLE_DEVICES`.

`POST /api/v1/jobs` remains an immediate queue bypass. `DELETE /api/v1/jobs/:id` cancels pending queue entries without SSH and stops active jobs through the existing termination path.

### Tunnels
- `POST /api/v1/tunnels`
- `GET /api/v1/tunnels`
- `DELETE /api/v1/tunnels/:id`

### Marimo lifecycle
- `POST /api/v1/marimo`
- `GET /api/v1/marimo`
- `GET /api/v1/marimo/:id`
- `DELETE /api/v1/marimo/:id`

The lifecycle response supplies the direct Tailnet Marimo URL.
`GET /api/sessions` and `POST /api/kernel/execute` belong to that returned
Marimo URL, not to the orchestrator API.

### Events
- `GET /api/v1/events`
