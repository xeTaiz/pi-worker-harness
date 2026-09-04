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
- `GET /api/v1/jobs/:id/logs`
- `GET /api/v1/jobs/:id/logs/stream`
- `DELETE /api/v1/jobs/:id`

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
