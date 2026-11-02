# Developer guide — how to work in this repository without losing architectural control

This guide is procedural. The rules that are *owner rulings* live in the root `CLAUDE.md`
and `CONTRIBUTING.md`; this page tells you how to navigate and how to change things safely.

## 1. Progressive disclosure — the reading ladder
1. **What is Cairn?** → [00-overview.md](00-overview.md) §1–2.
2. **What are the boundaries?** → [00-overview.md](00-overview.md) §3–4, [01-system-map.md](01-system-map.md) A–C.
3. **What does this subsystem own?** → `04`–`13`, plus the `CLAUDE.md` in that directory (`src/cairn/core/engine/CLAUDE.md`, `src/cairn/core/dispatch/CLAUDE.md`, `src/cairn/core/operator/CLAUDE.md`, `src/cairn/core/approval/CLAUDE.md`, `src/cairn/core/policy/CLAUDE.md`, `src/cairn/core/runtime/CLAUDE.md`, `src/cairn/core/storage/CLAUDE.md`, `src/cairn/core/triggers/CLAUDE.md`, `src/cairn/core/pack/CLAUDE.md`, `cairn/packs/CLAUDE.md`, `cairn/operators/CLAUDE.md`, and per-domain operator packs).
4. **How does this workflow execute?** → `docs/golden-paths/*.md`.
5. **Which files implement it?** → every golden path lists `path:line` anchors; `grep -n` from there.
5b. **What can cairn run today?** → 16-capability-catalog.md (refresh with `/catalog`).
6. **What can I safely change?** → 15-architecture-risks.md + the "Where to modify" section of each golden path + the invariant tables.

## 2. Environment facts you cannot skip
(The owner rulings behind these live in the directory `CLAUDE.md` files: `cairn/CLAUDE.md`
for Python invocation, imports and record regeneration; `cairn/operators/CLAUDE.md` for
pack-local tests, installed-pack verification and `DESIGN.md`; `tests/CLAUDE.md` for running
the suite; `docs/CLAUDE.md` for where documentation lives. The root `CLAUDE.md` is the
architectural constitution and no longer carries operational rules.)

- Python is always `.venv/bin/python`; tests `.venv/bin/pytest tests/core -q` (a bare `uv sync`/`uv run` strips extras — CONTRIBUTING).
- The suite neutralises `.env` (`tests/conftest.py`); a shell-exported `CAIRN_STORAGE` still steers it: `env -u CAIRN_STORAGE .venv/bin/pytest …`.
- A pack (domain or operator) is invisible to the engine until installed (or on `CAIRN_PACK_PATH` for dev). Manifest gates pass **vacuously** on uninstalled packs (`cairn/operators/CLAUDE.md` §B).
- CI gates (`.github/workflows/ci.yml`): ruff `F`+`E9` only; full pytest (`-m "not dbos"`); `scripts/audit_status.py` (docs honesty); `cairn pack lock --check`; console `check-design-rules.mjs`; `scripts/dump_openapi.py --check` + oasdiff (breaking API change needs the `api-break` label); wheels build; console build + `check:api`; engine image build + discovery smoke.

## 3. Finding things
| I am looking for… | Look in |
|---|---|
| the entry point of a user action | `src/cairn/cli/main.py` (verbs) → `src/cairn/cli/commands/<verb>.py`; `src/cairn/core/triggers/server.py:build_app` (routers) |
| where a ref is resolved | `src/cairn/core/dispatch/start.py`, `engine/pipeline_resolve.py`, `dataflow/defs.py:333 resolve_flow_key` |
| a governance rule | `src/cairn/core/approval/`, `policy/`, `obs/`, the `*_door.py` / `holdout.py` / `intake.py` modules |
| an operator by name (`x/y`) | `grep -rn 'name="x/y"' cairn/operators` or its pack's `pack.toml` `[operator."x/y"]` |
| a flow stage by name | `grep -rn '"<stage>"' cairn/operators/*/*/pyproject.toml` (groups `cairn.flow_stages`, `cairn.dataflow_stages`) |
| a store | `src/cairn/core/storage/` first; else `grep -rn "class .*Store" src/cairn/core` (many live at the core root) |
| an env knob | `src/cairn/core/settings.py INVENTORY` (35 documented) — but 153 `CAIRN_*` names exist; `grep -rn "CAIRN_<NAME>" cairn` |
| the console call for a route | `console/openapi.json` → `console/src/api/openapi.d.ts` → `console/src/api/schemas.ts` / `hooks.ts` |
| why something is the way it is | `docs/decisions/README.md` (ADR index; `§24.N` = `ADR-00NN`), then `docs/audits/` |

## 4. Making a change — the loop
```
idea/bug → `/triage-issue` → issue → `/work-issue` (investigate → contracts → plan → approve if MEDIUM/HIGH → implement → review agent → verify → doc impact) → PR → merge → `/release`; incidents → `/incident`
```
The full procedure — authority order, dynamic discovery of the applicable architecture,
the impact policy and the blocker protocol — is `17-issue-workflow.md`. In practice:
`/work-issue #N` (loads only the applicable authority, spawns one bounded read-only `/review-issue` agent, runs `/verify` on the changed area) →
`/explain-change` → PR (template section *Architecture compliance*).
The `.claude/skills/` directory carries one skill per step (`understand`, `trace`,
`architecture-review`, `plan-change`, `implement`, `verify`, `explain-change`), two
orchestrators for the issue lifecycle (`create-issue`, `work-issue`), the independent
post-implementation reviewer (`review-issue`), `catalog` (inventory of
runnable units) and `verify-upstream` (before proposing to build net-new capability or
quoting an estimate). Project skills load from the repository root: start Claude Code inside
`cairn/`, not the parent folder, or the slash commands are unknown.

Checklist before you edit:
1. Which boundary owns the behaviour? (00 §3). If the fix wants to live in two boundaries, stop and review.
2. Which chokepoint does the path pass through? Never add a second copy of `enforce_gate`, redaction or budget logic; call the function.
3. Is there a Temporal twin? Changes to `PipelineBridge` leg handling usually need the same change in `runtime/temporal/pipeline_activities.py` (or in the shared `dispatch/leg_hooks.py`).
4. Is there a record to regenerate? `cairn.lock` (pack edits), `console/openapi.json` (routes), `DESIGN.md` (operator packs), `docs/status.yaml` (status claims).
5. Does the test exist and can it fail? Run the real gate, not a lookalike.

## 5. Adding things — recipes
**A new operator** (code): copy an existing pack dir → `pyproject.toml` (name `operator-<x>`, entry point `"<pack>/<op>" = "operator_<x>.operators.<mod>:<Class>"`) → `pack.toml` `[operator."<pack>/<op>"]` with `risk`, `executors`, flags → `src/operator_<x>/operators/<mod>.py` subclassing `Operation` with `@register_operator` → `schemas/` Pydantic → `tests/` golden fixtures → `DESIGN.md` per `docs/templates/operator-pack-DESIGN.md` → root `pyproject.toml` (workspace member, dependency, `[tool.uv.sources]`) → `uv pip install -e <pack> --no-deps` → run `tests/core/test_operator_manifest_sync.py`, `test_pack_manifest_drift.py`, `test_pack_design_docs.py` → `uv run cairn pack lock`.

**A new pipeline step in a pack** (data): edit `cairn/packs/<pack>/pipelines/<name>.yaml`; wires are `$input.*` / `$steps.<id>.<field>` / `<step>.<produced>`; declare `produces/consumes` types; `cairn run --dry-run <pack>/<name>`; `uv run cairn pack lock`.

**A new flow stage** (kernel): a function in an operator pack registered under `[project.entry-points."cairn.flow_stages"]`; declare its risk class where the pack does (see `operator_training/flow_stages.py`); it is governed at flow admission by aggregate risk, so an `external_write` stage makes the whole flow gated.

**A new HTTP route**: a router builder in `src/cairn/core/triggers/routers/<surface>.py`, included in `server.py:build_app` (feature-gated with `require_feature` if the surface is optional) → regenerate `console/openapi.json` → `npm run gen:api` → add to vite `API_PATHS` if the console calls it.

## 6. What "done" looks like for a change
Files changed · boundaries affected · tests run (with output) · architecture rules checked
(the invariant tables) · records regenerated · remaining uncertainty stated. The
`verify` and `explain-change` skills produce exactly this report.
