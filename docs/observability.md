# Observability

cairn emits standard **OpenTelemetry** signals; it does **not** bundle a
tracing backend. You bring your own — point the engine at any OTLP endpoint, or
run the optional collector. Pick the tier that fits your deployment.

## What the engine emits

When tracing is enabled, every run produces OTLP spans:

- `agent.<name>.handle` — the top-level invocation
- `agent.<name>.<node>` — each graph node
- `llm.complete` — every model call (through the governed gateway)
- `mcp.<server>.<tool>` — every MCP tool call

Logs carry the trace/span id for correlation.

## One pipeline run is one trace

A declared pipeline produces a single trace, whichever request or process starts each
leg (a gate approved hours later, a restarted server, a Temporal worker):

```
cairn.pipeline                     launch: pipeline, pipeline_ref, content_hash, run_id, workspace_id, legs
├─ cairn.run                       a template leg (agentic): run_id, workspace_id, lane, template
│  └─ agent.* / llm.complete / mcp.*
├─ cairn.operator                  a task leg: operator, node, executor, mocked
├─ cairn.flow_leg                  a flow leg: flow, step, leg_run_id, engine, budget_usd, outcome
│  └─ cairn.flow_stage             one per stage the engine reports: stage, stages_done, stages_total
├─ cairn.gate                      a gate decision: kind, decision, actor — duration is the wait
└─ cairn.pipeline.settle           the pipeline went terminal: status
```

How it works, and what to expect:

- **The root is short.** `cairn.pipeline` opens and closes at launch, because a gate wait
  can outlive the request and the process. Its W3C trace context is stored on the pipeline
  record (`trace_context`, in the record's JSON, no migration). Every later span starts as
  a child of that stored context, so a leg started after a gate approval lands in the
  launch trace. Jaeger shows the later spans after the root's end; that is expected.
- **Terminal.** `cairn.pipeline.settle` is emitted once, when the pipeline reaches `done`,
  `failed` or `cancelled`.
- **Gates.** `cairn.gate` is written by whoever decides: it starts when the pause began and
  ends at the approval or rejection (`decision` = `approved`, `advanced`, `rejected`;
  `actor` = the decider). A data gate (a predicate) is an instant span with `decision`
  `data` or `blocked`.
- **Stages.** The dataflow runner reports a stage when it finishes, and the engine is
  polled, so a `cairn.flow_stage` ends when its completion is first seen; stages that finish
  between two polls collapse into one span naming the latest.
- **Runs.** Each leg's `Run.trace_id` is the pipeline's trace id.
- **Remote jobs.** The job's environment gets `TRACEPARENT` (the leg's span) next to
  `CAIRN_PARENT_RUN`, for the job to use as its parent.
- **Temporal owner.** The workflow carries no trace state (it stays deterministic). The
  activities read the stored context from the pipeline record and start their spans under
  it; templates run by the Temporal *template* runtime and DBOS are not yet pipeline-linked
  inside the worker.
- **Tracing off, or no `telemetry` extra.** Nothing is created or stored
  (`trace_context` is `{}`); no span code imports OpenTelemetry.

### View it in Jaeger

```
docker run --rm -p 16686:16686 -p 4318:4318 jaegertracing/all-in-one
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318      # needs cairndev[telemetry]
cairn-server                                                   # launch a pipeline, then open http://localhost:16686
```

Search service `cairn` for the operation `cairn.pipeline`; the trace id is on the pipeline
record's `trace_context.traceparent` (its second field).

## Four tiers — least to most infrastructure

### 1. Off (default)
Leave `OTEL_EXPORTER_OTLP_ENDPOINT` unset. No spans are exported, zero overhead.

### 2. Console (local debug)
```
OTEL_CONSOLE=1
```
Spans print to the process log. No endpoint, no backend — handy while developing.

### 3. Direct OTLP to a managed backend — recommended for most deployments
The engine speaks OTLP directly, so **no collector is needed**. The OTLP exporter ships in the `telemetry` extra: install `cairndev[telemetry]` before setting the endpoint (without it the engine logs a warning and runs with tracing off). In your `.env`:
```
OTEL_EXPORTER_OTLP_ENDPOINT=https://<your-otlp-endpoint>
OTEL_EXPORTER_OTLP_HEADERS=authorization=<token>
```
Works with Grafana Cloud, Honeycomb, Axiom, Dash0 — any OTLP-native backend.
Your endpoint + token are **your** config: they live in `.env` (or a secret
store), never in the repo.

### 4. Collector — advanced
Run the optional collector for fan-out to multiple backends, tail sampling,
redaction, an on-disk buffer when the backend is down, or a backend that isn't
OTLP-native (e.g. AWS X-Ray):
```
docker compose -f docker-compose.yml --profile otel up -d
```
…and set `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318` in `.env`.
Configure exporters in `deploy/otel-collector.yaml` (commented examples for
Grafana Cloud / Jaeger / AWS X-Ray are included).

## Repo vs. your deployment

- **Repo ships:** the OTLP mechanism (in the engine) and the collector as an
  *opt-in example* with commented exporter templates. No backend, no credentials.
- **You provide:** the backend of your choice and its credentials, in `.env` or
  a secret store.

Same principle as Postgres, Temporal, and MLflow — backing services are the
deployment's choice; the engine ships the mechanism.
