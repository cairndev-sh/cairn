"""``cairn run`` — start a pipeline and follow it, run the dev loop, or fire one trigger event.

Two paths live here, and ADR-0105 D1 (#2118) is the line between them:

* **the governed start** — REFUSED for a bare flow ref since ADR-0106 D1
  (#2134): only a declared pipeline runs, so ``cairn run <pack>/<flow>``
  answers with the one-leg wrapper to add (``$inherit`` makes it ~14 lines)
  and ``cairn run pipeline:<pack>/<name>`` starts it, through the launch door,
  and follows it (ADR-0165 D3). See :func:`_startable` and
  :func:`_start_governed_sync`.
* **the local dev loop** — ``--interactive`` (HITL at the terminal), ``--mock``
  and ``--dry-run`` compile and run the flow IN THIS PROCESS: no pipeline
  record, no console row, no leg — and, since #2128 item A, **no ``Run`` row
  either**. Trace storage uses the JSONL sink in :mod:`cairn.cli.runs`, so those
  runs stay replayable via ``cairn replay <run-id>``. The trigger-file path
  below is local too.

The two are not variants of one behaviour: one starts a governed object that
outlives the terminal, the other is a debugger.

**Why the dev loop writes no ``Run`` row (#2128 item A).** The invariant is that
every run is created under a pipeline id and is trackable by its pipeline. The
in-process modes do not go through ``RunDispatcher.run_template``, so the
chokepoint that enforces that cannot reach them: whatever they wrote to the run
store was an orphan by construction. It used to write one — but only under
``--series``, to roll a cost up into a budgeted series. A row that exists for
bookkeeping alone, for a debugger run with no admission, no policy, no approvals
and no audit trail, is not a run of anything; the honest fix is not to write it.
So ``--series`` is REFUSED with the dev-loop flags (a series is a budgeted group
of GOVERNED runs) and the run's LLM cost is recorded in its own trace instead.

Input handling — a trigger JSON file in one of two shapes:

  1. **Already a TriggerEvent** (has top-level ``source`` + ``payload``):
     parsed directly through ``TriggerEvent.model_validate``.
  2. **Raw external payload** (e.g. ``examples/pagerduty-incident.json``):
     wrapped using ``--source`` (default ``manual``) and ``--service``
     (default: ``raw.get("service")``).

Heuristic auto-detection means most example payloads run as-is, while
strict TriggerEvent JSON also works for tests / fixtures.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import inspect
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Optional

import typer

from cairn.cli.runs import JsonlEventSink, new_run_id, runs_root, write_meta


class MockModeUnsupportedOperatorsError(RuntimeError):
    """Raised when a template contains operators without a safe mock path."""


def _load_trigger(
    path: Path,
    *,
    source: Optional[str],
    service: Optional[str],
) -> "TriggerEvent":  # noqa: F821 — forward-ref to avoid eager import
    """Parse ``path`` into a TriggerEvent.

    Imported lazily so ``cairn run --help`` doesn't pull in the engine
    (compiler, LangGraph, …) until the user actually fires a run.
    """
    from cairn.core.engine.schema import TriggerEvent

    raw = json.loads(path.read_text())
    if isinstance(raw, dict) and "source" in raw and "payload" in raw:
        # Shape 1: already a TriggerEvent — respect it, override only if
        # the user explicitly passed flags.
        evt = TriggerEvent.model_validate(raw)
        if source:
            evt = evt.model_copy(update={"source": source})
        if service:
            evt = evt.model_copy(update={"service": service})
        return evt

    # Shape 2: raw external payload — wrap it.
    payload: dict[str, Any] = raw if isinstance(raw, dict) else {"value": raw}
    return TriggerEvent(
        source=source or "manual",   # "manual" is in the literal set
        payload=payload,
        service=service or payload.get("service"),
    )


async def _run_event(
    event: "TriggerEvent",  # noqa: F821
    *,
    environment: str,
    trigger_label: str = "(in-memory)",
) -> tuple[str, int]:
    """Fire one TriggerEvent through the pipeline.

    Reusable from anywhere — the platform's ``cairn eval`` calls this
    per case so each eval run gets a real ``run_id`` + JSONL trace and
    can be debugged via ``cairn replay <run-id>``.

    ``trigger_label`` is a human-readable string recorded in meta.json
    in place of the trigger file path (e.g. ``"eval:db-pool-payment"``).
    """
    from cairn.core.engine.runner import Runner

    run_id = new_run_id()

    write_meta(run_id, {
        "id": run_id,
        "status": "running",
        "trigger_file": trigger_label,
        "trigger": event.model_dump(),
        "environment": environment,
    })

    sink = JsonlEventSink(run_id)
    runner = Runner()
    status = "done"
    summaries: list[dict[str, Any]] = []
    error: Optional[str] = None
    try:
        results = await runner.run(
            event,
            environment=environment,  # type: ignore[arg-type]
            event_sink=sink,
            # #1851 — the id meta.json and the event sink already carry; without
            # it the runner mints its own `run-<spec>-<ts>` and the durable
            # workflow id no longer matches the run the CLI reported.
            run_id=run_id,
        )
        for r in results:
            summaries.append({
                "skill_name": r.skill_name,
                "template_name": getattr(r, "template_name", None),
                # §24.11.3b — every run records the exact template version
                # that produced it. Surfaces in meta.json + cairn replay
                # so a future eval --compare-against can identify the pin.
                "template_hash": getattr(r, "template_hash", "") or "",
                # §24.22 (AST3) — the compiled AST that produced this trace,
                # so replay/debug doesn't need the original template file.
                "template_ast": getattr(r, "template_ast", {}) or {},
                "duration_seconds": getattr(r, "duration_seconds", None),
                "action_count": len(getattr(r, "actions", []) or []),
            })
    except Exception as e:  # surfaces in meta + a clear CLI line
        status = "failed"
        error = f"{type(e).__name__}: {e}"
    finally:
        sink.close()

    write_meta(run_id, {
        "id": run_id,
        "status": status,
        "trigger_file": trigger_label,
        "trigger": event.model_dump(),
        "environment": environment,
        "matched_skills": len(summaries),
        "results": summaries,
        "error": error,
    })
    return run_id, (0 if status == "done" else 1)


async def _run_async(
    trigger_path: Path,
    *,
    source: Optional[str],
    service: Optional[str],
    environment: str,
) -> tuple[str, int]:
    """Wire up Runner + JSONL sink from a trigger file. Returns (run_id, exit_code).

    Thin wrapper over :func:`_run_event` that loads the event from disk.
    """
    event = _load_trigger(trigger_path, source=source, service=service)
    return await _run_event(
        event, environment=environment, trigger_label=str(trigger_path),
    )


def _print_summary(run_id: str, exit_code: int) -> None:
    """Last lines of output: the run id (so ``replay`` is one copy away)
    and an exit status."""
    location = runs_root() / run_id
    print()
    print(f"run_id:  {run_id}")
    print(f"trace:   {location}")
    print(f"replay:  cairn replay {run_id}")
    print(f"status:  {'ok' if exit_code == 0 else 'failed'}")


def _run_sync(
    trigger_path: str,
    *,
    source: Optional[str],
    service: Optional[str],
    environment: str,
) -> int:
    path = Path(trigger_path)
    if not path.exists():
        print(f"trigger file not found: {path}", file=sys.stderr)
        return 2
    run_id, rc = asyncio.run(_run_async(
        path, source=source, service=service, environment=environment,
    ))
    _print_summary(run_id, rc)
    return rc


# ─── direct-template mode (D2.D) ─────────────────────────────────────
#
# The demo's literal roadmap command is:
#
#   cairn run incident/orchestrated-incident --input examples/sample-alert.json
#
# When ``--input`` is provided, the first positional is treated as a
# ``<pack>/<template>`` namespaced template reference (resolved through
# the same machinery subgraph_node uses), and the JSON at ``--input`` is
# fed in as the template's ``input_schema`` payload. This bypasses skill
# matching + policy entirely — useful for the demo and for quickly
# exercising a template against a fixture without authoring a skill.


def _prompt_hitl(output: dict) -> dict:
    """Render the paused gate and prompt approve/reject in the terminal (#56).

    Returns the decision dict the paused ``interrupt()`` resumes with —
    ``{"approved": bool, "approver": ...}``."""
    from rich.console import Console
    from rich.panel import Panel
    from rich.prompt import Confirm

    console = Console()
    intr = output.get("__interrupt__")
    first = intr[0] if isinstance(intr, (list, tuple)) and intr else intr
    payload = getattr(first, "value", first)
    if isinstance(payload, dict):
        body = "\n".join(f"  [bold]{k}[/]: {str(v)[:120]}" for k, v in list(payload.items())[:8])
    else:
        body = str(payload)
    console.print(Panel(body or "(approval requested)",
                        title="⏸  approval required", border_style="yellow"))
    approved = Confirm.ask("[bold green]✓ Approve[/] this step?  [dim](n = reject)[/]", default=True)
    console.print("[green]approved[/]" if approved else "[red]rejected[/]")
    return {"approved": approved, "approver": "cli-interactive"}


async def _run_template_async(
    template_ref: str,
    input_path: Path,
    *,
    environment: str,
    mock: bool = False,
    sandbox: bool = False,
    interactive: bool = False,
    parent_run: str = "",
) -> tuple[str, int]:
    """Compile a namespaced template ref and run it against ``input_path``.

    Lazy imports — keeps ``cairn run --help`` cheap. When ``mock`` is set,
    operators that declare ``supports_mock`` take their ``.mock()`` path, and
    LLM calls receive a deterministic response. The run is refused before any
    node executes if a reachable operator lacks mock support.

    ``sandbox`` (``--mode sandbox``, ADR-0101 D3 / #1975) is the NARROWER
    Playground switch, not a second one: the same ``sandbox_mode`` ContextVars
    the dispatcher opens for a ``sandbox=True`` run (#826/#836/#844), so
    write-class operators are stubbed while ``side_effect_free`` reads run live
    on real data. ``--mock`` still wins where both are given, which is what
    ``resolve_is_mocked`` already does.
    """
    from cairn.core.engine.compiler import compile_template
    from cairn.core.engine.runner import run_within_budget
    from cairn.core.engine.skill_engine import resolve_template_path
    from cairn.core.operator import (
        default_registry as op_reg,
        load_all_operators,
        mock_mode,
        sandbox_mode,
    )
    from cairn.core.pack.loader import load_all_packs

    # Operator + pack discovery normally runs inside SkillEngine; in
    # direct-template mode we have to prime both ourselves so YAML
    # schemas + operation bindings resolve.
    op_reg.clear()
    load_all_operators(strict=False)
    load_all_packs()

    template_path = resolve_template_path(template_ref)
    input_data: dict[str, Any] = json.loads(input_path.read_text())

    run_id = new_run_id()
    # Thread the run_id into state as the graph thread_id / durable workflow id.
    # #56 did this only for --interactive, because back then the id mattered only
    # to an in-process resume(). Under CAIRN_RUNTIME=dbos it is the workflow's
    # idempotency key and the id every downstream write uses, so leaving it out
    # made the compiler fall back to the TEMPLATE NAME: prod showed
    # `workflow_uuid = promote`, two runs of one template collided on the same
    # workflow, and a gated run's approval could not surface — KeyError('promote')
    # on the store (#1851). The dispatcher has always done this unconditionally
    # (run_id=inv.id, #38); this now genuinely mirrors it.
    input_data["run_id"] = run_id

    # #2128 item A — no series gate here any more, because a dev-loop run cannot
    # JOIN a series: `--series` is refused alongside `--interactive`/`--mock` in
    # `_run_template_sync`, so `series_id` can no longer reach this function.

    write_meta(run_id, {
        "id": run_id,
        "status": "running",
        "trigger_file": str(input_path),
        "template_ref": template_ref,
        "environment": environment,
        "parent_run": parent_run,  # #989 — run lineage (`--from`)
        # ADR-0101 D3 (#1975) — the trace has to say which mode produced it, or
        # a sandbox result reads as a real one on replay.
        "sandbox": sandbox,
    })

    sink = JsonlEventSink(run_id)
    status = "done"
    error: Optional[str] = None
    output: dict[str, Any] | None = None
    duration_s: float | None = None
    template_hash: str = ""
    template_ast: dict[str, Any] = {}
    run_llm_usd: float = 0.0  # tallied into this run's own trace (meta.json)
    # #993 — an obs cost scope tallies per-call LLM spend. run_within_budget opens
    # no scope of its own, so this OUTER scope captures the run's cost (record_llm
    # writes to the active ctx). #2128 A — it lands in meta.json now, not in a
    # `Run` row: a dev-loop run's cost belongs to its trace, like everything else
    # it produces.
    from cairn.core import obs
    try:
        compiled = compile_template(template_path)
        # A compiled template predating the field (or a test double) has none.
        unsupported = getattr(compiled, "unsupported_mock_operators", ()) or ()
        if mock and unsupported:
            operators = ", ".join(unsupported)
            raise MockModeUnsupportedOperatorsError(
                "--mock cannot run this template because these operators do not "
                f"declare supports_mock: {operators}. No operators or LLM calls were executed."
            )
        template_hash = compiled.content_hash
        # §24.22 (AST3) — snapshot the compiled AST for self-contained replay.
        from cairn.core.engine.compiled import ast_dict
        template_ast = ast_dict(compiled.spec)
        t0 = time.perf_counter()
        # One mode context, chosen once: full mock, the Playground sandbox split,
        # or nothing. `--mock` first because it is the strictly wider stub — the
        # same precedence `resolve_is_mocked` applies per operator.
        if mock:
            mode_ctx: Any = mock_mode(True)
        elif sandbox:
            from cairn.core.dispatch.dispatcher import is_isolated_playground_env

            mode_ctx = sandbox_mode(True, isolated=is_isolated_playground_env(environment))
        else:
            mode_ctx = contextlib.nullcontext()
        with obs.cost_scope(f"cli-run-{template_ref}") as cost_ctx, mode_ctx:
            output = await run_within_budget(compiled, input_data)
        run_llm_usd = round(cost_ctx.cost_usd, 6)
        # #56 — interactive HITL: at each governed gate, prompt approve/reject
        # and resume IN-PROCESS (no server). Loops until a terminal state.
        if interactive:
            from cairn.core.runtime import InProcessRuntime
            rt = InProcessRuntime()
            # `prompt` / `agent_loop` flows have no graph (and never interrupt)
            graph = getattr(compiled.runnable, "_compiled_graph", None)
            while isinstance(output, dict) and "__interrupt__" in output:
                if not sys.stdin.isatty():
                    break  # headless — can't prompt; fall through to the paused-status note
                decision = _prompt_hitl(output)
                output = await rt.resume(graph, decision, run_id=run_id)
        duration_s = time.perf_counter() - t0
        # A run still holding an interrupt never reached a terminal state.
        if isinstance(output, dict) and "__interrupt__" in output:
            status = "failed"
            error = ("run paused at a HITL gate (not resumed) — re-run with "
                     "--interactive in a TTY, or drive it via the server")
    except Exception as e:  # surfaces in meta + a clear CLI line
        status = "failed"
        error = f"{type(e).__name__}: {e}"
        if isinstance(e, MockModeUnsupportedOperatorsError):
            print(error, file=sys.stderr)
    finally:
        sink.close()

    write_meta(run_id, {
        "id": run_id,
        "status": status,
        "trigger_file": str(input_path),
        "template_ref": template_ref,
        "environment": environment,
        "parent_run": parent_run,  # #989 — run lineage (`--from`)
        "sandbox": sandbox,  # ADR-0101 D3 (#1975)
        # #2128 A — the dev loop's cost lives in the dev loop's own record.
        "llm_cost_usd": run_llm_usd,
        "matched_skills": 0,
        "results": [{
            "skill_name": None,
            "template_name": template_ref,
            "template_hash": template_hash,
            "template_ast": template_ast,
            "duration_seconds": duration_s,
            "action_count": 0,
            "output": output,
        }],
        "error": error,
    })

    # #2128 item A — the `default_store().create(Run(...))` + `attach_run` that
    # used to live here is GONE. It was the last producer of a run row that no
    # pipeline owns, and the chokepoint in `RunDispatcher.run_template` cannot
    # reach it: this path never calls the dispatcher. It existed only to roll a
    # cost up into a `--series`, and `--series` is now refused for the dev-loop
    # modes (`_run_template_sync`), so there is nothing left for it to do. The
    # cost is in `meta.json` above; the trace replays with `cairn replay`.

    return run_id, (0 if status == "done" else 1)


# ─── preflight verdict (#800, ADR-0072 §3) ──────────────────────────
#
# `cairn run --dry-run` and `cairn preflight` are the CLI faces of the SAME
# side-effect-free verdict the console Run tab (#798) and `cairn ask` render:
# one `build_preflight` composition, many faces. It answers "will this run"
# (ready/blocked) + what it costs + what pauses for a human + what's blocking —
# no run row, no spend, no external writes.


def _render_preflight(pf: Any) -> list[str]:
    """Format a :class:`~cairn.core.preflight.Preflight` as terminal lines. Plain
    text (no rich) so it stays trivially testable and pipe-friendly."""
    out: list[str] = []
    out.append(f"preflight · {pf.ref}")
    if not pf.found:
        out.append("  ✕ NOT FOUND — no such template")
        for b in pf.blockers:
            out.append(f"    → {b.fix}" if b.fix else f"    {b.what}")
        return out

    out.append(f"  {'READY ✓' if pf.ready else 'BLOCKED ✕'}")
    if pf.command:
        out.append(f"  command:  {pf.command}")

    # cost — honest: a numeric static estimate if the plan has one, else the
    # truth that cost is tracked at runtime and cap-enforced (BudgetExceeded).
    lo, hi = pf.cost.get("min_usd"), pf.cost.get("max_usd")
    if lo is not None or hi is not None:
        span = f"${lo}" if lo == hi else f"${lo}–${hi}"
        out.append(f"  cost:     est {span}")
    else:
        out.append("  cost:     no static estimate — runtime-tracked + cap-enforced")

    # gates — heads-up, not blockers: the run can start, it won't finish unattended.
    if pf.gates:
        steps = ", ".join(g.step for g in pf.gates)
        out.append(f"  gates:    {len(pf.gates)} human approval(s) — {steps}")

    if pf.resolved_config:
        cfg = ", ".join(f"{k}={v}" for k, v in pf.resolved_config.items())
        out.append(f"  config:   {cfg}")

    if pf.blockers:
        out.append("")
        out.append("  blockers:")
        for b in pf.blockers:
            out.append(f"    ✕ [{b.kind}] {b.what}")
            if b.fix:
                out.append(f"      → {b.fix}")

    if pf.warnings:
        out.append("")
        out.append("  warnings:")
        for w in pf.warnings:
            out.append(f"    ⚠ {w}")
    return out


async def _preflight_async(template_ref: str, values: dict, *, profile: Optional[str],
                           check_connections: bool) -> Any:
    """Prime pack/operator discovery (the server does this at startup; the CLI
    must do it itself) then compose the verdict. Lazy imports keep --help cheap."""
    from cairn.core.preflight import build_preflight
    from cairn.core.operator import default_registry as op_reg, load_all_operators
    from cairn.core.pack.loader import load_all_packs

    op_reg.clear()
    load_all_operators(strict=False)
    load_all_packs()
    return await build_preflight(template_ref, values, profile=profile,
                                 check_connections=check_connections)


def _not_an_agentic_template(ref: str) -> Optional[tuple[str, str]]:
    """What ``ref`` names when it is not an agentic template (#2536), as ``(kind,
    detail)``: ``("pipeline", "")``, ``("data flow", "")`` / ``("ml flow", "")``,
    ``("ambiguous flow ref", <why>)``, or ``None``.

    Preflight and the dev loop compile agentic templates only, so a pipeline or a
    data/ml flow ref came back as "no such template … install its pack" although it
    is installed. Read through the existing lookups — pipelines by
    :func:`resolve_pipeline_ref` (``pipeline:`` prefix included), flows by
    :func:`flow_def_for` (the one the leg executor and the bridge use) — never a
    second resolver. Packs must already be loaded."""
    from cairn.core.dataflow.defs import DataflowDefError, flow_def_for
    from cairn.core.engine.pipeline_resolve import AmbiguousRefError, resolve_pipeline_ref

    # A declared pipeline wins over a flow at the same ref (ADR-0106 D1), so ask first.
    try:
        if resolve_pipeline_ref(ref) is not None:
            return ("pipeline", "")
    except AmbiguousRefError:
        return ("pipeline", "")  # names a pipeline twice over; `pipeline explain` says how
    try:
        flow = flow_def_for(ref)
    except DataflowDefError as e:  # two packs fold onto one address: say so, never guess
        return ("ambiguous flow ref", str(e))
    return (f"{flow.kind} flow", "") if flow is not None else None


def _not_a_template_lines(ref: str, kind: str, *, dev_loop: bool, detail: str = "") -> list[str]:
    """The verdict for a ref that names a pipeline or a data/ml flow (#2536)."""
    if kind == "ambiguous flow ref":
        return [f"preflight · {ref}", f"  ✕ {ref} is an ambiguous flow ref", f"    {detail}"]
    what = ("the dev loop (--dry-run, --mock, --interactive) runs agentic templates only"
            if dev_loop else "preflight checks agentic templates only")
    out = [f"preflight · {ref}", f"  ✕ {ref} is a {kind} — {what}"]
    if kind == "pipeline":
        bare = ref.removeprefix("pipeline:")
        out.append(f"    → cairn run {bare} --dry-run   (every leg, gate and config "
                   f"value, offline)")
        if dev_loop:
            out.append(f"    → cairn run {bare}")
    else:
        flag = "" if dev_loop else " --dry-run"
        out.append(f"    → a {kind} runs as a leg of a declared pipeline: `cairn run`, "
                   f"then `cairn run <pipeline>{flag}`")
    return out


def _preflight_sync(template_ref: str, input_file: Optional[str], *,
                    profile: Optional[str] = None, check_connections: bool = True,
                    dev_loop: bool = False) -> int:
    """Load optional input values, print the verdict, exit 0=ready / 1=blocked.

    ``input_file`` is optional here: with no inputs the verdict simply surfaces
    the missing-input blockers — still a useful "what would I need" answer.
    ``dev_loop`` words the not-a-template verdict for ``cairn run --dry-run``."""
    values: dict[str, Any] = {}
    if input_file:
        p = Path(input_file)
        if not p.exists():
            print(f"--input file not found: {p}", file=sys.stderr)
            return 2
        try:
            loaded = json.loads(p.read_text())
        except Exception as e:  # noqa: BLE001
            print(f"✗ --input is not valid JSON: {e}", file=sys.stderr)
            return 1
        if not isinstance(loaded, dict):
            print("✗ --input must be a JSON object of input values", file=sys.stderr)
            return 1
        values = loaded

    pf = asyncio.run(_preflight_async(
        template_ref, values, profile=profile, check_connections=check_connections,
    ))
    other = None if pf.found else _not_an_agentic_template(template_ref)
    if other:
        kind, detail = other
        print("\n".join(_not_a_template_lines(template_ref, kind, dev_loop=dev_loop,
                                               detail=detail)))
        return 1
    print("\n".join(_render_preflight(pf)))
    return 0 if pf.ready else 1


def _dry_run_template(template_ref: str, input_path: str, *, check_connections: bool = True) -> int:
    """``cairn run --dry-run`` — the preflight verdict (ready/blocked + cost +
    gates + blockers) followed by the execution plan (the node DAG). No operators
    run, no trace written. Exit 0 when ready, 1 when blocked."""
    in_path = Path(input_path)
    if not in_path.exists():
        print(f"--input file not found: {in_path}", file=sys.stderr)
        return 2

    # 1. the verdict (shared composition — same one the console + ask render).
    rc = _preflight_sync(template_ref, input_path, dev_loop=True, check_connections=check_connections)

    # 2. the plan DAG — extra detail unique to the CLI dry-run. Best-effort: a
    #    compile failure already shows up as a verdict blocker, so don't double-fault.
    from cairn.core.engine.compiler import compile_template
    from cairn.core.engine.skill_engine import resolve_template_path
    from cairn.core.engine.visualize import template_plan_lines

    try:
        compiled = compile_template(resolve_template_path(template_ref))
    except Exception:  # noqa: BLE001 — the verdict already reported why
        return rc
    print()
    print("\n".join(template_plan_lines(compiled)))
    return rc


def _run_template_sync(
    template_ref: str,
    input_path: str,
    *,
    environment: str,
    mock: bool = False,
    sandbox: bool = False,
    interactive: bool = False,
    parent_run: str = "",
    series_id: str = "",
) -> int:
    in_path = Path(input_path)
    if not in_path.exists():
        print(f"--input file not found: {in_path}", file=sys.stderr)
        return 2
    # #2128 item A — a series is a budgeted group of GOVERNED runs. The dev-loop
    # modes run in this process with no admission, no policy, no approvals and
    # (now) no run row, so there is nothing for a series to cap or to compare.
    # Refused rather than ignored: silently dropping a budget flag is exactly the
    # failure this invariant exists to end.
    if series_id:
        print(f"--series {series_id!r} cannot be combined with --interactive/--mock: "
              f"those run IN THIS PROCESS and are not governed runs, so they join no "
              f"budgeted series. Drop the dev-loop flag to start the flow as its "
              f"one-leg pipeline, which does.", file=sys.stderr)
        return 2
    run_id, rc = asyncio.run(_run_template_async(
        template_ref, in_path, environment=environment, mock=mock, sandbox=sandbox,
        interactive=interactive, parent_run=parent_run,
    ))
    _print_summary(run_id, rc)
    return rc


# ─── the governed door (ADR-0105 D1, #2118 · ADR-0106 D1, #2134) ────
#
# Only a declared pipeline runs. `cairn run <pack>/<flow>` used to synthesize a
# one-leg pipeline for a bare flow ref; ADR-0106 D1 removed the synthesis from
# every door, so this one now REFUSES with the wrapper to add — and, because
# this is the command a developer already has in their fingers, with the
# dev-loop flags that need no pipeline at all (D6). `cairn run` keeps its two
# paths: the dev loop still runs, and a pipeline ref is redirected to the
# command that carries a pipeline's presets and reporting.


def _start_governed_sync(
    template_ref: str,
    inputs: dict[str, Any],
    *,
    environment: str = "",
    sandbox: bool = False,
    parent_run: str = "",
    series_id: str = "",
    detach: bool = False,
) -> int:
    """Refuse a bare flow start; redirect a pipeline ref. Exit 2 either way.

    The keyword arguments are kept so the two call sites are unchanged and a
    user who passed ``--sandbox`` or ``--series`` gets the RULE rather than an
    "unexpected argument". Nothing here starts anything: since ADR-0106 D1 the
    thing this function used to build — a one-leg pipeline synthesized from a
    bare ref — is exactly what the rule forbids, so building it here would make
    the CLI the one door that disagreed.
    """
    # lazy, like every other import in this module: `cairn run --help` must not
    # pull in the dispatcher, the bridge or the engine.
    from cairn.cli.commands import pipeline as pipeline_cli
    from cairn.core.dispatch.start import (
        PIPELINE,
        FlowNotRunnableError,
        UnknownStartRefError,
        resolve_start_ref,
    )

    console = pipeline_cli.console
    try:
        kind, _bare = resolve_start_ref(template_ref)
    except FlowNotRunnableError as e:
        console.print(f"[red]{e}[/red]", soft_wrap=True)
        return 2
    except (UnknownStartRefError, ValueError) as e:
        console.print(f"[red]{e}[/red]", soft_wrap=True)
        return 1
    if kind == PIPELINE:
        # Not a refusal of the intent — a redirect to the command that carries a
        # pipeline's presets, declared inputs and multi-leg reporting.
        console.print(f"[red]{template_ref!r} names a pipeline, not a flow[/red] — "
                      f"start it with: cairn run {template_ref}", soft_wrap=True)
        return 2
    # unreachable: `resolve_start_ref` returns PIPELINE or raises (ADR-0106 D1)
    raise AssertionError(f"resolve_start_ref returned {kind!r} for {template_ref!r}")


def _start_governed_from_file(template_ref: str, input_path: str, **kw: Any) -> int:
    """``--input <file.json>`` → the governed door. The file is all this adds:
    the door takes VALUES, and the flow's own contract validates them (a bad key
    is refused at the door, not a compile error mid-run)."""
    in_path = Path(input_path)
    if not in_path.exists():
        print(f"--input file not found: {in_path}", file=sys.stderr)
        return 2
    try:
        values = json.loads(in_path.read_text())
    except ValueError as e:
        print(f"--input is not valid JSON ({in_path}): {e}", file=sys.stderr)
        return 2
    if not isinstance(values, dict):
        print(f"--input must be a JSON object of input values, got "
              f"{type(values).__name__} ({in_path})", file=sys.stderr)
        return 2
    return _start_governed_sync(template_ref, values, **kw)


# ─── entry points ───────────────────────────────────────────────────


# Help text refs: every start is a one-leg pipeline (ADR-0105 D1); the command owns
# that pipeline (#1702); the dev loop writes no run row (#2128).
def cmd(
    target: Optional[str] = typer.Argument(
        None,
        help="A pipeline ref (<pack>/<name>). Also: a flow or template with the dev-loop flags, "
             "a pipeline file (.py), or a trigger JSON file. With nothing: list what can run.",
    ),
    input_: Optional[list[str]] = typer.Option(
        None, "--input", "-i", help="An input as key=value, or a JSON file of inputs as @FILE; repeatable.",
    ),
    set_: Optional[list[str]] = typer.Option(
        None, "--set", "-s", help="An input override as key=value; repeatable. JSON-parsed when possible.",
    ),
    preset: Optional[str] = typer.Option(
        None, "--preset", "-p", help="Start from a saved or pack-shipped input set; --input overrides it per key.",
    ),
    save_preset: Optional[str] = typer.Option(
        None, "--save-preset", help="After a launch that worked, save its inputs under this name.",
    ),
    environment: Optional[str] = typer.Option(
        None, "--environment", "-e", "--env", help="The environment to run in.",
    ),
    workspace: Optional[str] = typer.Option(
        None, "--workspace", "-w", help="With --dry-run: whose variables and saved presets apply.",
    ),
    detach: bool = typer.Option(False, "--detach", help="Start it and return, without following it."),
    interval: float = typer.Option(2.0, "--interval", help="Seconds between polls while following."),
    series: Optional[str] = typer.Option(
        None, "--series", help="Attach a pipeline file's run to a budgeted series.",
    ),
    from_run: Optional[str] = typer.Option(
        None, "--from", help="The run this one is derived from, recorded as `parent_run`.",
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would happen and run nothing.",
    ),
    no_connections: bool = typer.Option(
        False, "--no-connections", help="With --dry-run: skip the connection-health check.",
    ),
    mock: bool = typer.Option(
        False, "--mock",
        help="Dev loop: stub model calls and mockable operators, in this process, with no record. "
             "With --check: run the check's operators in mock mode.",
    ),
    interactive: bool = typer.Option(
        False, "--interactive", "-I",
        help="Dev loop: run in this process, with no record, and prompt at each gate.",
    ),
    mode: str = typer.Option(
        "live", "--mode", help="Dev loop: 'sandbox' stubs write operators and runs reads for real.",
    ),
    check: bool = typer.Option(
        False, "--check", help="Run a read-only check and exit on its verdict (for CI).",
    ),
    fail_on: str = typer.Option(
        "fail", "--fail-on", help="With --check: the severity that exits non-zero, 'fail' or 'warn'.",
    ),
    skill: bool = typer.Option(False, "--skill", help="Treat the target as a skill name."),
    service: Optional[str] = typer.Option(
        None, "--service", help="With --skill or a trigger file: the service it targets.",
    ),
    wait: bool = typer.Option(False, "--wait", help="With --skill: block on the server until done."),
    source: Optional[str] = typer.Option(
        None, "--source", help="A trigger file's source: pattern, alert, metric, schedule, manual or cascade.",
    ),
    timeout: Optional[float] = typer.Option(
        None, "--timeout", help="Seconds to wait at a prompt before --on-timeout.",
    ),
    on_timeout: str = typer.Option(
        "detach", "--on-timeout", help="At a prompt timeout: detach, reject or abort.",
    ),
    native: bool = typer.Option(False, "--native", help="Prompt with an OS dialog."),
    dump: Optional[str] = typer.Option(None, "--dump", help="Write a pause's context to this file."),
    json_out: bool = typer.Option(False, "--json", help="Print machine-readable results."),
    endpoint: Optional[str] = typer.Option(None, "--endpoint", help="server URL (overrides profile/env)"),
    token: Optional[str] = typer.Option(None, "--token", help="bearer token (overrides profile/env)"),
    profile: Optional[str] = typer.Option(None, "--profile", help="config profile name"),
) -> None:
    """Run a pipeline and follow it, asking at each gate.

    `cairn run <pack>/<pipeline>` goes through the launch door, stays attached, prompts at each gate,
    follows any pipeline its events start, and ends with the results. With no terminal to ask on, it
    says the `cairn decide` command and exits 2. Exit codes: 0 done, 1 failed or cancelled, 2 waiting
    on a person, 77 session expired.

    `--dry-run`, `--mock` and `--interactive` are the dev loop: they run in-process, record nothing,
    and are not a governed pipeline.
    """
    from cairn.cli.commands import pipeline as pipeline_cli  # noqa: PLC0415 — keeps --help light
    from cairn.cli.commands import remote  # noqa: PLC0415
    from cairn.cli.commands import status as status_cli  # noqa: PLC0415
    from cairn.profiles import default_dev_profile  # noqa: PLC0415 — #2241 D: local compiles get local-dev

    default_dev_profile()
    if target is None:
        pipeline_cli.ls_cmd()
        print("\ncairn run <pipeline> starts one; --dry-run shows what it would do.")
        return
    if on_timeout not in ("detach", "reject", "abort"):
        print("--on-timeout must be detach, reject or abort", file=sys.stderr)
        raise typer.Exit(2)
    # ADR-0101 D3 (#1975) — one launch shape across the surfaces: `--mode` is what `POST /start`
    # calls `mode`, and it reaches the SAME sandbox switch the dispatcher opens for a Playground run.
    wanted_mode = (mode or "live").strip().lower()
    if wanted_mode not in ("live", "sandbox"):
        print(f"--mode must be 'live' or 'sandbox', got {mode!r}", file=sys.stderr)
        raise typer.Exit(2)
    sandbox = wanted_mode == "sandbox"
    values = _collect_inputs(input_ or [], set_ or [])

    if skill:
        remote._submit_skill(target, values, follow=not detach, wait=wait, service=service,
                             endpoint=endpoint, token=token, profile=profile)
        return
    if check:
        from cairn.cli.commands import gate as gate_cli  # noqa: PLC0415 — keeps --help light

        with _inputs_file(values) as path:
            gate_cli.cmd(template_ref=target, input_file=path, sets=None, profile=None,
                         fail_on=fail_on, json_out=json_out, mock=mock)
        return
    # #1092 — pipeline-FILE mode: a `.py` holding one cairn.pipeline_file.Pipeline.
    if target.endswith(".py"):
        raise typer.Exit(_run_pipeline_file_sync(
            target, environment_override=environment or "production", mock=mock, sandbox=sandbox,
            interactive=interactive, parent_run=from_run or "", series_override=series or "",
            set_values=list(set_ or []), dry_run=dry_run, detach=detach,
        ))
    # A trigger JSON file: local skill matching, unchanged.
    if not values and Path(target).is_file() and not (dry_run or mock or interactive):
        raise typer.Exit(_run_sync(target, source=source, service=service,
                                   environment=environment or "production"))
    if dry_run:
        with _inputs_file(values) as path:
            if _names_a_pipeline(target):
                inspect.unwrap(pipeline_cli.explain_cmd)(
                    ref=target, input_=[f"{k}={json.dumps(v)}" for k, v in values.items()],
                    environment=environment or "", preset=preset or "", workspace=workspace or "default",
                    json_out=json_out)
                return
            raise typer.Exit(_dry_run_template(target, path, check_connections=not no_connections))
    if interactive or mock:
        with _inputs_file(values) as path:
            raise typer.Exit(_run_template_sync(
                target, path, environment=environment or "production", mock=mock, sandbox=sandbox,
                interactive=interactive, parent_run=from_run or "", series_id=series or "",
            ))
    # The governed start. A flow is refused here, with the wrapper to add (ADR-0106 D1), before any
    # engine is reached. A ref this machine doesn't have may still be on a configured server.
    if not _startable(target, explicit_server=remote._explicit_endpoint(endpoint, profile)):
        raise typer.Exit(_start_governed_sync(target, values))
    for flag, value, why in (("--series", series, "a pipeline's series is in its governance block"),
                             ("--from", from_run, "it applies to a pipeline file"),
                             ("--workspace", workspace, "it goes with --dry-run"),
                             ("--mode sandbox", sandbox, "it goes with --mock or --interactive")):
        if value:
            print(f"{flag} doesn't apply to starting a pipeline: {why}", file=sys.stderr)
            raise typer.Exit(2)
    raise typer.Exit(status_cli.launch_and_follow(
        target, values, environment=environment, preset=preset, save_preset=save_preset, detach=detach,
        interval=interval, timeout=timeout, on_timeout=on_timeout, native=native, dump=dump,
        json_out=json_out, endpoint=endpoint, token=token, profile=profile))


def _parse_value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except ValueError:
        return raw  # a bare string is the common case


def _collect_inputs(inputs: list[str], sets: list[str]) -> dict[str, Any]:
    """``-i K=V``, ``-i @FILE``, a bare ``-i FILE`` (the old spelling) and ``--set K=V``, in order,
    later keys winning. Exit 2 on anything else, before anything starts."""
    values: dict[str, Any] = {}
    for item in [*inputs, *sets]:
        key, eq, raw = item.partition("=")
        as_file = item[1:] if item.startswith("@") else item if Path(item).is_file() else None
        if as_file is not None:
            try:
                loaded = json.loads(Path(as_file).read_text(encoding="utf-8"))
            except (OSError, ValueError) as e:
                print(f"--input {item}: can't read a JSON file: {e}", file=sys.stderr)
                raise typer.Exit(2) from None
            if not isinstance(loaded, dict):
                print(f"--input {item}: must be a JSON object of input values, got "
                      f"{type(loaded).__name__}", file=sys.stderr)
                raise typer.Exit(2)
            values.update(loaded)
        elif eq and key:
            values[key.strip()] = _parse_value(raw)
        else:
            print(f"--input must be key=value or @FILE, got {item!r}", file=sys.stderr)
            raise typer.Exit(2)
    return values


@contextlib.contextmanager
def _inputs_file(values: dict[str, Any]) -> Any:
    """The dev loop and the check read a file: hand them these values as one."""
    with tempfile.NamedTemporaryFile("w", suffix=".json", encoding="utf-8") as fh:
        json.dump(values, fh)
        fh.flush()
        yield fh.name


def _names_a_pipeline(ref: str) -> bool:
    from cairn.core.pack import ensure_loaded  # noqa: PLC0415 — keeps --help light

    ensure_loaded()
    return (_not_an_agentic_template(ref) or ("", ""))[0] == "pipeline"


def _startable(ref: str, *, explicit_server: bool) -> bool:
    """May this ref go to the launch door? A declared pipeline may; a flow never (the refusal in
    :func:`_start_governed_sync` says why); an unknown ref only when a server was named, which may
    have it installed."""
    from cairn.core.dispatch.start import FlowNotRunnableError, UnknownStartRefError, resolve_start_ref  # noqa: PLC0415

    try:
        resolve_start_ref(ref)
    except FlowNotRunnableError:
        return False
    except (UnknownStartRefError, ValueError):
        return explicit_server
    return True


def _parse_set(values: list[str]) -> dict[str, Any]:
    """``["rows=100", "model=gpt-4o-mini"]`` → a typed input dict (#1092).

    JSON first (so ``rows=100`` is an int and ``flag=true`` a bool), falling back
    to the raw string — which is what a bare model name or path needs.
    """
    out: dict[str, Any] = {}
    for item in values:
        key, sep, raw = str(item).partition("=")
        if not sep or not key.strip():
            raise ValueError(f"--set expects key=value, got {item!r}")
        try:
            out[key.strip()] = json.loads(raw)
        except (ValueError, TypeError):
            out[key.strip()] = raw
    return out


def _run_pipeline_file_sync(
    file_path: str,
    *,
    environment_override: str,
    mock: bool,
    sandbox: bool,
    interactive: bool,
    parent_run: str,
    series_override: str,
    set_values: list[str],
    dry_run: bool,
    detach: bool = False,
) -> int:
    """``cairn run <file.py>`` — resolve a pipeline file, then run it (#1092).

    Install-then-run in spirit and in effect: the file only NAMES an installed
    workflow, so what executes is the pack's governed record — same compile, same
    gates, same run record. This function is the resolution + precedence layer,
    nothing more. It therefore takes the SAME door as `cairn run <ref> --input`,
    governed or in-process, or the sentence above stops being true (ADR-0105 D1,
    #2118).

    Env precedence: an explicit ``--env`` wins; otherwise the file's ``env``. We
    read ``sys.argv`` to tell "explicitly passed" from "typer's default", because
    the flag has a non-None default and Typer cannot report provenance here.
    """
    from cairn.pipeline_file import PipelineFileError, load_pipeline_file, resolve_run_args

    try:
        cls = load_pipeline_file(file_path)
        overrides = _parse_set(set_values)
    except (PipelineFileError, ValueError) as e:
        print(f"{e}", file=sys.stderr)
        return 2

    env_explicit = any(a == "--env" or a.startswith("--env=") for a in sys.argv[1:])
    args = resolve_run_args(
        cls, env=environment_override if env_explicit else None, inputs=overrides,
    )
    series_id = series_override or args["series"]

    print(f"pipeline: {cls.__name__}  ({file_path})")
    print(f"workflow: {args['workflow']}")
    print(f"env:      {args['environment']}" + ("  [--env]" if env_explicit else "  [file]"))
    if series_id:
        print(f"series:   {series_id}")
    if args["inputs"]:
        shown = ", ".join(f"{k}={v!r}" for k, v in list(args["inputs"].items())[:6])
        more = "" if len(args["inputs"]) <= 6 else f", +{len(args['inputs']) - 6} more"
        print(f"inputs:   {shown}{more}")
    print()

    # The governed door takes VALUES, so the file's inputs go straight to it; the
    # temp-file round trip below exists only for the in-process path, whose
    # signature is a `--input` PATH.
    if not (dry_run or interactive or mock):
        return _start_governed_sync(
            args["workflow"], dict(args["inputs"]),
            environment=args["environment"], sandbox=sandbox,
            parent_run=parent_run, series_id=series_id, detach=detach,
        )

    # The inputs reach the existing direct-template path the way it expects them:
    # as a JSON file. Written to a temp file rather than reworking that signature,
    # so the file path and the `--input` path stay literally the same code.
    with tempfile.NamedTemporaryFile(
        "w", suffix=".json", delete=False, encoding="utf-8",
    ) as fh:
        json.dump(args["inputs"], fh)
        input_path = fh.name
    try:
        if dry_run:
            return _dry_run_template(args["workflow"], input_path)
        return _run_template_sync(
            args["workflow"], input_path,
            environment=args["environment"], mock=mock, sandbox=sandbox,
            interactive=interactive, parent_run=parent_run, series_id=series_id,
        )
    finally:
        try:
            Path(input_path).unlink()
        except OSError:
            pass


def preflight_cmd(
    template_ref: str = typer.Argument(
        ..., help="Namespaced agentic template reference, e.g. 'dataset-readiness/check'. "
                  "A pipeline is checked with `cairn pipeline explain`.",
    ),
    input_file: Optional[str] = typer.Option(
        None, "--input", "-i",
        help="Path to a JSON object of input values. Optional — with no inputs "
             "the verdict just lists what's missing.",
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", "-p",
        help="Environment/profile the plan resolves under. Default: the env-default profile.",
    ),
    no_connections: bool = typer.Option(
        False, "--no-connections",
        help="Skip the advisory connection-health check (faster; offline).",
    ),
) -> None:
    """Preflight a workflow: the side-effect-free ready/blocked verdict — cost,
    gates, and blockers — without starting a run. Exit 0=ready, 1=blocked."""
    from cairn.profiles import default_dev_profile  # noqa: PLC0415 — #2241 D: local compiles get local-dev

    default_dev_profile()
    raise typer.Exit(_preflight_sync(
        template_ref, input_file, profile=profile, check_connections=not no_connections,
    ))


def main(argv: Optional[list[str]] = None) -> int:
    """argv-style entry point. Used by tests + ``python -m`` invocations.

    Takes the same doors as :func:`cmd`, and carries none of the dev-loop flags,
    so ``--input`` here is the governed start (ADR-0105 D1). A second entry point
    that started a flow differently from the command it shadows is the exact
    split #2118 closes.
    """
    from cairn.profiles import default_dev_profile  # noqa: PLC0415 — #2241 D, as cmd

    default_dev_profile()
    parser = argparse.ArgumentParser(
        prog="cairn run",
        description="Start a flow as its one-leg pipeline, or fire one trigger "
                    "through the pipeline.",
    )
    parser.add_argument("trigger_file")
    parser.add_argument("--source", default=None)
    parser.add_argument("--service", default=None)
    parser.add_argument("--env", dest="environment", default="production")
    parser.add_argument("--input", dest="input_file", default=None)
    args = parser.parse_args(argv)
    if args.input_file is not None:
        return _start_governed_from_file(
            args.trigger_file,
            args.input_file,
            environment=args.environment,
        )
    return _run_sync(
        args.trigger_file,
        source=args.source,
        service=args.service,
        environment=args.environment,
    )


if __name__ == "__main__":
    raise SystemExit(main())
