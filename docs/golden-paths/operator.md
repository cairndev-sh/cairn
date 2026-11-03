# Golden path — a basic operator execution (agentic template run)

## What this workflow does
Runs one agentic template (a `flows/*.md` / `templates/*.md` `state_machine` or `prompt`)
whose operation nodes invoke operators through the single invocation chokepoint. This is
also what a pipeline `template:` leg does, and what `cairn run <pack>/<template>` does
locally. Example: `incident/incident-rca`, `training/finetune` (template path),
`serving/serve`.

## Trigger
- `POST /start {"ref": "<pack>/<pipeline>"}` → `launch_pipeline`; a bare template ref is refused (`FlowNotRunnableError`, ADR-0106 D1) — wrap it in a declared one-leg pipeline.
- `POST /packs/{pack}/{template}/run` (refuses a bare template the same way, `routers/templates.py:117`).
- A `template:` leg in a pipeline.
- `cairn run <pack>/<template> --interactive|--mock|--dry-run` (the dev loop: in-process, no record — ADR-0106 D6).
- Webhook/schedule via skills (`Runner.run`, the incident-era path).

## Execution diagram
```mermaid
flowchart TD
  A[template: leg · PipelineBridge._submit_step<br/>pipeline_bridge.py:862] --> B[RunDispatcher.run_template<br/>dispatcher.py:737]
  B --> B1[doors: provisioning · holdout · intake · license · job-channel · skypilot · pull-worker]
  B1 --> B2[reserve series budget<br/>budget_reservations.reserve]
  B2 --> B3[Run created · template_ref stamped · queued event]
  B3 --> C[_run_template_task :907]
  C --> C1[compile_template<br/>engine/compiler.py:92]
  C1 --> C2[_build_state_machine :602 → _compile_bt :1135]
  C2 --> C3[checkpointer attach if gated :703 · runtime :792]
  C --> D[obs.cost_scope · run_within_budget<br/>engine/runner.py:45]
  D --> E[Runtime.invoke → LangGraph node]
  E --> F[operation_node_fn<br/>compiler.py:1574]
  F --> F1[resolve_runtime_gate<br/>approval/enforce.py:88]
  F1 -->|gate required, no approval| F2[interrupt() :1610 → awaiting_approval]
  F1 --> G[invoke_operator<br/>operator/runtime.py:888]
  G --> G1[enforce_gate :950]
  G1 --> G2[pick_executor<br/>operator/executor.py:113]
  G2 -->|in_process| H[invoke_operator_inprocess :295 → invoke_operator_core :374 → Operation.run]
  G2 -->|mcp_tool| H2[invoke_operator_mcp_tool :508]
  G2 -->|remote_job| H3[invoke_operator_job :615 · durable path :788]
  H --> I[output binding patch → state]
  I --> J[store.update · events · settle · run.finished hook]
```

## Exact source trace
```
POST /start {ref: <pipeline>}          src/cairn/core/triggers/routers/start.py:114 start_anything
  └ launch_pipeline → template: leg    src/cairn/core/dispatch/pipeline_launch.py:162 → pipeline_bridge.py:862 _submit_step
      └ RunDispatcher.run_template     src/cairn/core/dispatch/dispatcher.py:737
          ├ pack provisioning          src/cairn/core/pack/provisioning.py         (:~608)
          ├ holdout door               src/cairn/core/holdout.py                   check_holdout_for_inputs (:~636)
          ├ intake door                src/cairn/core/intake.py                    check_intake_for_inputs (:~655)
          ├ license door               src/cairn/core/model_license_gate.py        check_license_for_inputs (:~676)
          ├ job-channel / skypilot / pull-worker doors
          │                            src/cairn/core/job_channel_door.py · dataflow/engine_door.py · pull_worker_door.py
          ├ run id + budget            src/cairn/core/storage/protocols.py:112 new_run_id · budget_reservations.reserve (:765)
          ├ Run(...) + store.create    src/cairn/core/storage/protocols.py:136 Run  (:~783-814)
          ├ emit "queued"              dispatcher.py:338 _emit (store.append_event + bus)
          └ task: _run_template_task   src/cairn/core/dispatch/dispatcher.py:907
              ├ compile                src/cairn/core/engine/compiler.py:92 compile_template
              │   ├ prompt flow        compiler.py:313 _build_prompt
              │   └ state machine      compiler.py:602 _build_state_machine → :1135 _compile_bt
              │       ├ operation node compiler.py:856 _ActionNodeFactory · :1574 operation_node_fn
              │       ├ subgraph       compiler.py:1043 _flatten_subgraph
              │       ├ checkpointer   src/cairn/core/runtime/checkpointer.py:91 get_checkpointer (compiler.py:703)
              │       └ runtime        compiler.py:792 _resolve_runtime → runtime/{inprocess,dbos,temporal}
              ├ workflow config        dispatcher.py:865 _resolve_workflow_config (run > workflow > env > default)
              ├ cost scope             src/cairn/core/obs (cost_scope) · engine/runner.py:45 run_within_budget
              └ node execution
                  ├ approval           src/cairn/core/approval/enforce.py:88 resolve_runtime_gate → resolver.py:76 resolve_gate
                  │                    policy source: approval/policy_source.py:61 runtime_policy_engine
                  │                    pause: langgraph interrupt() compiler.py:1610 · audit_emit obs/audit.py:140
                  ├ invoke_operator    src/cairn/core/operator/runtime.py:888
                  │   ├ enforce_gate   src/cairn/core/approval/enforce.py:116 (call :950)
                  │   ├ pick_executor  src/cairn/core/operator/executor.py:113
                  │   ├ in_process     operator/runtime.py:295 invoke_operator_inprocess
                  │   │   ├ bindings   operator/runtime.py:149 resolve_simple_binding  ($state/$input/$nodes/$env)
                  │   │   ├ compute    operator/runtime.py:255 resolve_declared_compute (ctx.compute)
                  │   │   ├ core       operator/runtime.py:374 invoke_operator_core (config layering, validation, reporter)
                  │   │   └ run        src/cairn/core/operator/protocol.py:385 Operation.run / :397 mock
                  │   ├ mcp_tool       operator/runtime.py:508
                  │   └ remote_job     operator/runtime.py:615 · :788 durable (job store, control channel)
                  └ LLM nodes          src/cairn/services/llm/factory.py:108 build_chat_model (redaction + BudgetCallback)
              ├ terminal update        dispatcher.py store.update(status, cost, …) · _emit done/failed
              ├ settle budget          budget_reservations.settle / release
              └ terminal hooks         dispatcher.py:574 _terminal_hooks → cairn.run_hooks (the platform scores: eval ledger)
resume after approval                  src/cairn/core/dispatch/dispatcher.py:1115 resume_run → Command(resume)
```

## Important state transitions
`Run.status`: `queued` → `running` → (`awaiting_approval` ⇄ `running`) → `done` | `failed`
| `cancelled`. Events appended with monotonic `seq`. Approval decision recorded in graph
state (`approved_state`, `enforce.py:225`) and review store. Cost aggregated into
`Run.cost` (`dispatcher.py:137 _aggregate_cost`). Reservation settled with actual spend.

## Governance
Doors at admission; series budget reservation; per-operation gate (`resolve_runtime_gate`
in the node, `enforce_gate` in `invoke_operator` — belt and braces); redaction and
per-call cost in `build_chat_model`; audit events on gate request/decision.
**Coverage note (OBSERVED):** `cairn run` compiles the same template directly
(`src/cairn/cli/commands/run.py:332,348`) — the per-operation gate and redaction still apply
because they live in the compiled graph and the chat-model factory, but the admission
doors and the reservation do not run.

## Execution
Operators execute in-process by default; `remote_job` for long-running providers (RunPod,
Vast, Modal) with the durable job store and control channel (ADR-0038); `mcp_tool` for MCP
servers. LLM calls go through the gateway (`services/llm/gateway.py`).

## Output
The template's `output_schema` payload in `Run.result_payloads`, events, artifacts
harvested (`engine/artifacts.py`), cost, verdicts; SSE stream `/runs/{id}/stream`.

## Tests
`tests/core/test_dispatcher_template_run.py`, `test_dispatcher.py`, `test_run_template_ref.py`,
`test_approval_chokepoint.py`, `test_approval_gate_e2e.py`, `test_invoke_operator_core.py`,
`test_operator_config.py`, `test_operator_testing_harness.py`, `test_llm_redaction_chokepoint.py`,
`test_budget_reservation.py`, `test_template_content_hash.py`, `test_subgraph_*.py`,
`test_durable_actions.py`; operator packs' own tests via `cairn.core.operator.testing`.

## Where to modify it
| Change | File |
|---|---|
| add an admission check | `src/cairn/core/dispatch/dispatcher.py:run_template` **and** `src/cairn/core/preflight.py` (keep the door pure like `holdout.py`) |
| operator invocation semantics | `src/cairn/core/operator/runtime.py` (`invoke_operator*`) |
| binding grammar | `src/cairn/core/operator/binding.py`, `engine/expr.py` |
| gate resolution | `src/cairn/core/approval/resolver.py` (pure) then `enforce.py` |
| node compilation | `src/cairn/core/engine/compiler.py:_compile_bt` (amend, never replace — ADR-0084 D8) |
| a new operator | see [../architecture/05-operators.md](../architecture/05-operators.md) |
