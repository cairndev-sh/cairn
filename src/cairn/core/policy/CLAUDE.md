# cairn/core/policy — YAML policy evaluation and plan-time structural gates

**Purpose.** Load policies (pack `policies/` dirs, the env overlay, the console store),
evaluate `ActionRequest`s to a `PolicyDecision` (`deny > require_hitl > require_dry_run >
allow_auto`), and inject environment-mandated gates into a `PipelinePlan` before launch.
**Owns.** `engine.py:PolicyEngine`, `pipeline_gates.py:inject_policy_gates`
(deterministic ids, no double-gating by a gate that always asks, `injected_by` provenance, `PolicyDeniedError`),
`store.py` (console-authored policies, triad).
**Does not own.** The runtime evaluator singleton (`approval/policy_source.py`), where gates
are enforced (`approval/`), the policy CRUD routes (`triggers/routers/policies.py`).

**Contracts.** `inject_policy_gates(plan, environment)` is called ONCE at
`dispatch/pipeline_launch.py:410` (and in `pipeline_explain`); the evaluator used there and
at dispatch time must be the same object so plan-time and run-time never disagree.
Pack policies are inherited/adopted per `pack/profile.py` + `runner.py` attach lists.

**Allowed deps.** `engine.schema` (types), `pack` (dirs), `storage`, `approval` (lazy), `refs` (the one ref stripper and pack fold — `bare_ref`, `pack_key`, `spellings`; stdlib-only; #2539, #2546).
**Forbidden.** operator packs, `cairn.packs.*`, `triggers`, `dispatch` (except via lazy
import in tests).

**Testing.** `tests/core/test_policy_engine.py`, `test_policy_injected_gates.py`,
`test_policy_named_environment.py`, `test_policy_capability_attachment.py`,
`test_policy_pg_store.py`, `test_pack_policy_adoption.py`.

**Common mistakes.** Non-deterministic gate ids (plan hash instability); gating a leg that is
already gated upstream; a new effect level without updating `_more_restrictive`; writing a
policy through the store without `reset_runtime_policy_engine()`.

**Read.** `docs/architecture/08-governance.md`, ADR-0014, ADR-0053, ADR-0087 P0b.
