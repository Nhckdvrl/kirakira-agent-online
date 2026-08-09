# Kirakira Cloud Agent

Kirakira is a durable, multi-user Agent application backend. It is designed to
run as an online service: browser clients submit messages to an API, PostgreSQL
stores tenant-scoped conversations and execution state, workers execute the
unchanged Agent algorithms, and all code/file tools run through an isolated
remote sandbox.

[简体中文](./README-cn.md) · [Documentation](./docs/INDEX.md) ·
[Cloud design and implementation](./docs/cloud-engineering/plan.md)

## What is implemented

- Opaque cookie authentication, user isolation, bounded conversation/message
  queries, account/conversation deletion, Origin checks, rate limits and security
  headers.
- Transactional user-message + queued-Run admission; PostgreSQL leases,
  heartbeats, cancellation, recovery, per-conversation serialization and
  idempotent submission.
- Durable ordered Run events and resumable SSE output/tool timelines.
- The original ReAct, context projection, compaction, Default Memory scoring,
  Proactive energy/gate/judge chain and Drift hazard/fairness/journal algorithms.
- User-scoped PostgreSQL Memory and profile state. pgvector/HNSW is candidate
  recall only; the original Python scoring remains authoritative.
- A durable background Agent scheduler. Proactive consumes API/webhook inbox
  events; Drift runs when the proactive path has nothing to deliver. The user
  selects one active Agent conversation as the delivery target; workers trigger
  both paths automatically.
- Remote sandbox attestation, tool checkpoints, deterministic automation delivery
  keys, Prometheus metrics, JSON logs, readiness checks and hardened systemd units.

Local TUI/setup/bootstrap is not a product mode and is not installed as a public
command. Some legacy adapters remain in the source tree solely as algorithm and
contract regression fixtures.

## Run the service

Requirements: Python 3.11+, PostgreSQL with pgvector, an OpenAI-compatible chat
and embedding endpoint, and an independently isolated sandbox service implementing
the contract in `agent/tools/execution_backend.py`.

```bash
cp .env.example .env
cp config.example.toml config.toml
uv sync --group dev
uv run alembic upgrade head
uv run kirakira-cloud-api
uv run kirakira-cloud-worker
```

For production, install multiple worker instances with the units in
[`deploy/systemd`](./deploy/systemd). API requests never execute the Agent inline;
they return a Run ID, and clients follow durable events through SSE.

Core endpoints:

```text
POST   /v1/auth/register | /v1/auth/login
GET    /v1/conversations
POST   /v1/conversations
GET    /v1/conversations/{id}/messages
POST   /v1/conversations/{id}/messages
PUT    /v1/conversations/{id}/automation
POST   /v1/conversations/{id}/proactive-events
GET    /v1/runs/{id}
POST   /v1/runs/{id}/cancel
GET    /v1/runs/{id}/events/stream
GET    /readyz | /metrics
```

The automation configuration chooses the active delivery conversation and
operationally enables Proactive and/or Drift. It does not manually trigger them;
the background scheduler owns their timing.

## Verification

```bash
uv run pytest -q
KIRAKIRA_TEST_POSTGRES_URL='postgresql://…' \
  uv run pytest -q tests/test_cloud_postgres_integration.py
```

The PostgreSQL integration test exercises real `SKIP LOCKED` competition,
concurrent rate limits, message sequence locking and 1024-dimensional pgvector
mirroring. See the [verification record](./docs/operations/verification.md) for
the latest evidence and the [Cloud document](./docs/cloud-engineering/plan.md) for
architecture, trade-offs and remaining deployment decisions.

## Repository map

```text
cloud/            API, durable stores, Run/automation workers, observability
agent/            ReAct, context, tools, MCP and execution contracts
core/             memory and shared runtime contracts
memory2/          canonical structured-memory algorithms
plugins/          Memory, Proactive and Drift implementations
proactive_v2/     proactive frame and orchestration
deploy/systemd/   production process units
tests/            unit, contract, concurrency and opt-in PostgreSQL tests
```

## License

MIT. See [LICENSE](./LICENSE).
