# Support matrix

The internal source of truth for what cairn supports. Public claims (cairndev.sh) are
derived from here, never the reverse. Statuses: **SUPPORTED** (works, tested, documented,
maintainers will fix it) · **EXPERIMENTAL** (implemented and tested, not production-proven or
with known gaps) · **IN DEVELOPMENT** (partially implemented, an issue owns it) · **PLANNED**
(an ADR/roadmap entry, no code). Evidence levels: IMPLEMENTED · CI TESTED (runs in `ci.yml`)
· INTEGRATION TESTED (needs an extra or a service; run locally) · PRODUCTION PROVEN (used on
the prod box with a real outcome). "Code exists" alone never earns SUPPORTED. Updated
2026-09-11 against `main`.

## Language / runtime
| Item | Status | Evidence |
|---|---|---|
| Python 3.12, 3.13 | SUPPORTED | `requires-python >=3.12`; CI matrix tests 3.13; prod image 3.13 |
| In-process runtime (agentic graphs) | SUPPORTED | CI TESTED (`tests/core`) |
| Temporal runtime (`CAIRN_RUNTIME=temporal`) | SUPPORTED for flow, template and gate legs | CI TESTED (`test_temporal_wiring.py`), PRODUCTION PROVEN (prod compose profile); the Temporal pipeline owner sends `flow:` legs to the engine and every other leg through `run_template`, so `task:` legs and `finalizer:` steps are refused at launch (#59, `07-runtime.md`); the public feature proof covers DBOS, not Temporal (ADR-0127 D6) |
| DBOS runtime | EXPERIMENTAL | `dbos` tests run in the deploy matrix and the hello-cairn feature proof's `dbos` cells (`tests/e2e/hello`, run outside CI), not in CI; an SQLite DBOS system DB needs SQLite >= 3.42 (`unixepoch('subsec')`; boot refuses older — use a uv-managed Python or a Postgres `CAIRN_DBOS_DSN`) |

## Storage
| Item | Status | Evidence |
|---|---|---|
| sqlite (runs, pipelines, artifacts, registry) | SUPPORTED | CI TESTED (the suite's default) |
| Postgres (`CAIRN_STORAGE=postgres://`) | SUPPORTED | PRODUCTION PROVEN; twins tested in the deploy matrix |
| S3 artifact relay (`CAIRN_ARTIFACT_S3_BUCKET`) | SUPPORTED | IMPLEMENTED, used on prod; `docs/contracts/artifacts.md` |
| Checkpoint mirror (`CAIRN_CHECKPOINT_BUCKET`) | IN DEVELOPMENT | silent no-op when unset (#2191) |

## Execution engines (selected by profile — `docs/contracts/execution.md`)
| Item | Status | Evidence |
|---|---|---|
| builtin (local subprocess) | SUPPORTED | CI TESTED (conformance + CORE harness) |
| builtin → `compute.ssh` box (ssh dispatch) | SUPPORTED | PRODUCTION PROVEN (2026-08-28 GPU campaign) |
| Metaflow (substrate; Kubernetes/Batch) | EXPERIMENTAL | INTEGRATION TESTED locally (conformance, bridge, the CORE flow through Metaflow); no cluster run |
| SkyPilot (provisioning) | IN DEVELOPMENT | engine + door tests; no preemption recovery (#2192); no live run |
| Provider task legs (`train/runpod\|vast\|modal\|kaggle`) | EXPERIMENTAL | mocked-transport tests; ssh is the proven path |
| `compute/acquire` / `compute/release` (start and stop the EC2 box a `compute.ssh` connection names; public since 0.3.1) | EXPERIMENTAL | CI TESTED with a mocked EC2 client (`packages/cairn-operator-remote/tests`); no live start/stop on a public release yet (cairn-extensions#9) |
| GPU leg pricing (`hourly_usd × timeout` estimate at the launch door, per-attempt settle; ADR-0151, ADR-0152; since 0.3.1) | EXPERIMENTAL | CI TESTED (`tests/core/test_gpu_leg_pricing.py`); no priced live run yet (cairn-extensions#9) |
| Deployment budget default, ceiling and local rate (`[profile.budget]`; ADR-0155; since 0.3.2) | EXPERIMENTAL | CI TESTED (`tests/core/test_deployment_budget.py`) |

## Trainers and training capabilities
| Item | Status | Evidence |
|---|---|---|
| SFT with LoRA / QLoRA (Unsloth + PEFT + TRL + Transformers) | SUPPORTED | PRODUCTION PROVEN (08-28 campaign, ssh box); stub tier CI TESTED (CORE harness) |
| Stub trainer tier (`stub: true`) | SUPPORTED | CI TESTED |
| DPO (`training/dpo-train`, `dpo_entry`, TRL) | EXPERIMENTAL | pack tests (`test_dpo_stages`); no GPU run recorded |
| GRPO / online RL | PLANNED | #1623, #2202–#2204 (backend research first) |
| Distillation (GKD, `compress/distill`) | EXPERIMENTAL | pack test `test_distill`; no GPU run recorded |
| Quantization (bitsandbytes, `compress/quantize`) | EXPERIMENTAL | 2 pack tests; calibration methods deferred (#2206) |
| Adapter merge (`model/merge`) | EXPERIMENTAL | pack test `test_merge` |
| Checkpoint resume across a restart | IN DEVELOPMENT | relay + `resume_from` exist; durable requirement #2191 |
| Immutable candidate identity for models (`candidate_artifact_id`) | SUPPORTED | CI TESTED (CORE harness C6; `tests/core/test_candidate_identity.py`), T04 #2189 |

## Registry / tracking
| Item | Status | Evidence |
|---|---|---|
| MLflow (register, card, baseline; `report_to: mlflow`) | SUPPORTED | pack tests; `deploy_check` gate; optional prod compose profile — production use of registration unverified this cycle |
| Local artifact registry (content-addressed) | SUPPORTED | CI TESTED |
| W&B (`report_to: wandb`) | EXPERIMENTAL | trainer knob only |
| Langfuse (eval tracing) | EXPERIMENTAL | `test_langfuse_eval.py`; prod uses Langfuse Cloud via the otel fan-out |

## Serving
| Item | Status | Evidence |
|---|---|---|
| vLLM serve/stop legs, batch inference, bench | EXPERIMENTAL | ADR-0098 accepted, code-complete; live halves deferred (§8); 7 pack tests |
| Champion/challenger, rollback | PLANNED | #1628, #2197–#2199 |

## Data capabilities (kernels in `cairn-operator-dataset-checks`)
| Item | Status | Evidence |
|---|---|---|
| ingest.normalize (jsonl/csv/parquet/HF; `dataset:` refs with hash verification) | SUPPORTED | CI TESTED; PRODUCTION PROVEN |
| dedup.exact (python / DuckDB) | SUPPORTED | CI TESTED |
| dedup.near (MinHash) | SUPPORTED with the `[minhash]` extra; honest skip without | INTEGRATION TESTED (skips in CI, no extra) |
| pii.redact (regex; presidio cascade with `[pii]`) | SUPPORTED | CI TESTED (regex), INTEGRATION TESTED (presidio) |
| decontam.apply / decontam.scan (exact, n-gram, MinHash) | SUPPORTED | CI TESTED |
| expectations.check | SUPPORTED | CI TESTED (`cairn-operator-data-expectations`) |
| profile.tokens (over-length policy) | SUPPORTED | CI TESTED (whitespace fallback without a tokenizer) |
| split | SUPPORTED | CI TESTED |
| curate.score (LLM judge) | EXPERIMENTAL | pack test with a stubbed judge |
| preference pairs (`dataset.dpo_pairs`) | EXPERIMENTAL | `test_dpo_stages` |
| check-evidence map on versions | SUPPORTED | CI TESTED (T03, #2188) |
| Intake verdict + holdout + licence doors (agentic path per run; pipeline launch door once per launch with the policy's required checks) | SUPPORTED | CI TESTED (`test_intake_verdict.py`, `test_holdout_door.py`, `test_admission_door.py`, CORE harness C3/C9) — T05 #2190 |

## Evaluation
| Item | Status | Evidence |
|---|---|---|
| Closed-book measure/compare, model gate, delta, forgetting | SUPPORTED | 15 pack tests; used on the 08-28 campaign |
| Judged eval (pairwise, inspect) | EXPERIMENTAL | 2 pack tests |
| lm-eval harness | EXPERIMENTAL | 1 pack test |
| Output-format validity capability | PLANNED | #2195 |

## Surfaces
The CLI (`cairn`), the HTTP + SSE API (`cairn-server`) and `cairn-client` ship in the
public core (the `cairndev` and `cairn-client` wheels): SUPPORTED (CI TESTED in `ci.yml`).
The console, its MCP surface and `cairn-sdk` are part of cairn-platform (ADR-0135). Remote deployment
(compose, EC2 script, Helm lint): SUPPORTED for compose (PRODUCTION PROVEN); Helm chart
IN DEVELOPMENT (ADR-0104 D4).
A pipeline run's governance trail (since 0.3.2): its decisions and spend in the hash-chained audit
log, `cairn audit verify`, an attributed gate reject (`POST /pipelines/{id}/steps/{step}/reject`,
`cairn remote reject <pipeline> <step>`) and one OpenTelemetry trace per run: EXPERIMENTAL (CI TESTED in
`test_pipeline_audit_trail.py`, `test_audit_chain_writers.py`, `test_pipeline_reject.py`,
`test_pipeline_trace.py`).
