# Runbook — pipeline stuck

**Symptom.** `GET /pipelines/{id}` shows `status: running` (or a leg `running`) long after
the leg's work ended; or `paused` with nobody waiting on it.

## 1. Read the record, not the console
```
cairn pipeline show <pipeline_id>            # step_state, cursor, handles
curl -s $CAIRN_ENDPOINT/pipelines/<id>/findings | jq '.steps'
curl -s $CAIRN_ENDPOINT/runs/<run_id> | jq '{status, error, last_heartbeat_at, attached_count}'
```
`StepState.handle` (`{job_id, engine, meta}`) is the engine's re-attach token; `Run.last_heartbeat_at`
says whether a control loop is still watching (`leg_executor.py:520-556` stamps it).

## 2. Decide which shape it is
| Evidence | Shape | Action |
|---|---|---|
| step `awaiting_approval`, record `paused` | a gate is waiting for a human (by design) | approve/advance: `POST /pipelines/{id}/steps/{step}/approve`; refuse with a reason: `POST …/reject` (`cairn remote reject <pipeline> <step> --reason TEXT`, ends the pipeline `failed`, ADR-0156) — or cancel |
| step `queued`, run `queued`, a `compute_target` with `max_in_flight` | waiting for a slot on a capped target (ADR-0086 H5) | free a slot or raise the cap; the leg is never refused |
| run `running`, heartbeat stale, engine job gone | orphaned leg after a control-plane restart | on restart the bridge **adopts** the persisted handle first (ADR-0086 H1); if the process table is lost (`builtin`: "unknown builtin job … process table lost"), the attempt fails and retries per the leg's `retry:`; otherwise cancel the pipeline |
| run `running` forever under `CAIRN_RUNTIME=temporal`, workflow terminal in Temporal | a zombie run (#976) | the reconciler sweeps these on the temporal runtime (`src/cairn/core/runtime/reconciler.py`); if it is not running, restart the server (it starts in the lifespan) |
| run `done`, step still `running` | the terminal event never reached the bridge (bus/subscription) | restart the server — the bridge re-subscribes and re-reads the record on start (`pipeline_bridge.py:341-348`) |

## 3. Cancel when nothing can advance
```
curl -sX POST $CAIRN_ENDPOINT/pipelines/<id>/cancel -H 'content-type: application/json' -d '{}'
```
Cancel kills engine compute (`DataflowEngine.cancel` — the builtin engine kills the process
group) and settles the budget reservation; a leg queued on a capped target is skipped cleanly.

## 4. Afterwards
File the incident with `/incident <pipeline_id>` if the shape was not in the table above.
