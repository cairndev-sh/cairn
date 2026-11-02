"""``cairn gate`` — run a check workflow read-only in CI and gate on its verdict.

#990 (ADR-0076, D2) — the CI/CD face of cairn's core primitive. Runs a check
workflow, reads its deterministic
pass/warn/fail verdict (ADR-0075 outcome channel — the SAME verdict the console
and ``cairn ask`` render), prints a machine-readable JSON verdict, and **exits
non-zero on FAIL** so a PR / pipeline is blocked.

CI-agnostic by design: "non-zero exit blocks the pipeline" is universal, so this
works in GitHub Actions, GitLab CI, Jenkins, a git hook, or a bare shell with no
server and no GitHub integration — ``pip install cairndev && cairn gate …``. See
``examples/ci/`` for ready-to-copy GitHub / GitLab workflows (incl. posting the
cited findings as a PR comment via ``gh pr comment``).

Read-only: it runs the check operators (which are themselves read-only — they
read the dataset and emit a verdict) and writes no run trace. Point it at a
*check* workflow, not one that mutates (a promote / train).

Exit codes: **0** = PASS (or WARN unless ``--fail-on warn``), **1** = gate FAIL,
**2** = error (bad inputs, couldn't run, a step failed, a pause, or no usable verdict).
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
from pathlib import Path
from typing import Any, Optional

import typer

# verdict vocabulary (matches the console's domainVerdict + dataset/gate operator).
_FAIL = "fail"
_WARN = "warn"
_PASS = "pass"

# The outcome item that carries the headline verdict. Mirrors the console's
# domainVerdict regex (ADR-0075) so CLI + UI classify identically.
_VERDICT_HEAD = re.compile(r"^(readiness|verdict|gate)$", re.I)

# What counts as a pass. `_classify` mirrors the console and calls anything else a pass,
# which a gate can't: `unknown` or `skipped` is not a check that passed.
_PASS_WORDS = frozenset({"pass", "passed", "ok", "ready", "success", "succeeded", "clean"})


def _classify(value: str) -> str:
    """Map a raw verdict string (e.g. "FAIL", "pass", "warn") → canonical severity."""
    v = str(value).lower()
    if re.search(r"fail|block|reject", v):
        return _FAIL
    if re.search(r"warn|degrad", v):
        return _WARN
    return _PASS


def _graded_outcome(outcome: list) -> Optional[str]:
    """The verdict of an ``outcome`` list with no head, from how its items are graded.
    A failure stands; otherwise None when nothing is graded or a grade isn't a known word."""
    items = [o for o in outcome if isinstance(o, dict)]
    severities = [str(o["severity"]) for o in items if o.get("severity")]
    grades = {_classify(s) for s in severities}
    if _FAIL in grades or any(o.get("pass") is False for o in items):
        return _FAIL
    if any(_classify(s) == _PASS and s.strip().lower() not in _PASS_WORDS for s in severities):
        return None
    if _WARN in grades:
        return _WARN
    return _PASS if grades or any(o.get("pass") is True for o in items) else None


def _extract_verdict(output: dict) -> dict:
    """Pull the domain pass/warn/fail verdict + cited reasons out of a run's output.

    Mirrors the console's ``domainVerdict`` (ADR-0075 outcome channel): prefer the
    ``outcome`` list's verdict head; fall back to a top-level ``verdict`` scalar, then to
    how the ``outcome`` items are graded. ``verdict`` is None when the run gave none of
    these or a word that isn't a verdict. Cited reasons are the non-ok checks."""
    output = output if isinstance(output, dict) else {}
    outcome = output.get("outcome") if isinstance(output.get("outcome"), list) else []
    head = next(
        (o for o in outcome
         if isinstance(o, dict) and _VERDICT_HEAD.search(str(o.get("name", "")))),
        None,
    )
    value = head.get("value") if head is not None else output.get("verdict")
    raw = "" if value is None else str(value)
    if raw:
        verdict = _classify(raw)
        if verdict == _PASS and raw.strip().lower() not in _PASS_WORDS:
            verdict = None
    else:  # a head with no value says nothing was decided, whatever the other items say
        verdict = _graded_outcome(outcome) if head is None else None
    reasons = [
        {"name": o.get("name"), "value": o.get("value"), "severity": o.get("severity")}
        for o in outcome
        if isinstance(o, dict) and o is not head
        and (o.get("severity") not in (None, "ok") or o.get("pass") is False)
    ]
    return {
        "verdict": verdict,
        "label": (raw or verdict or "").upper(),
        "blocking": output.get("blocking") or [],
        "warnings": output.get("warnings") or [],
        "reasons": reasons,
    }


def _step_error_keys(node: Any) -> set[str]:
    """The ``findings`` keys that mean a step of this template failed.

    The engine catches operator and python-node errors into ``findings.<node>_error`` and
    carries on, even inside a sequence, so a failed step doesn't raise here, and a later
    step can overwrite ``last_action_status``. Named from the template, because a finding
    like ``mean_absolute_error`` is a measurement.

    Only a selector of single steps counts as recovering: each one writes the status the
    next is chosen on, so only its last can fail it. With a compound child, a later child
    may not run or may do nothing, so every failed step counts, which can turn a real
    recovery into exit 2. A failure inside a ``subgraph_node`` isn't visible here."""
    from cairn.core.engine import schema  # noqa: PLC0415 — keeps --help light

    if isinstance(node, schema.OperationBTNode):
        return {f"{node.node_ref}_error"}
    if isinstance(node, schema.PythonBTNode):
        return {f"{node.function}_error"}
    if isinstance(node, schema.SelectorBTNode) and node.children:
        steps = all(isinstance(c, (schema.OperationBTNode, schema.PythonBTNode)) for c in node.children)
        return set().union(*(_step_error_keys(c) for c in (node.children[-1:] if steps else node.children)))
    if isinstance(node, (schema.SequenceBTNode, schema.ParallelBTNode)):
        return set().union(*(_step_error_keys(c) for c in node.children))
    if isinstance(node, schema.GuardBTNode):
        return _step_error_keys(node.then) | (_step_error_keys(node.else_) if node.else_ else set())
    return set()


def _run_problem(output: dict, verdict: dict, step_error_keys: set[str]) -> Optional[str]:
    """Why this run's result can't be taken as a check that passed, or None."""
    output = output if isinstance(output, dict) else {}
    if output.get("__interrupt__"):
        return "the check paused at an approval, and nothing here can answer it"
    findings = output.get("findings") if isinstance(output.get("findings"), dict) else {}
    failed = sorted(k for k in step_error_keys if k in findings)
    if failed:
        return "a step failed: " + "; ".join(f"{k}: {findings[k]}" for k in failed)
    items = output.get("map_results") if isinstance(output.get("map_results"), list) else []
    if any(isinstance(r, dict) and "error" in r for r in items):
        return "a step failed: " + "; ".join(str(r["error"]) for r in items if isinstance(r, dict) and "error" in r)
    if output.get("last_action_status") in ("failure", "rejected"):
        return f"the last step ended {output['last_action_status']}"
    if verdict["verdict"] is None:
        if output.get("budget_exceeded"):
            return "the check hit its budget before giving a verdict"
        if verdict["label"]:
            return f"the check's verdict {verdict['label']!r} isn't a pass, a warning or a failure"
        return "the check gave no usable verdict: it must write `$state.verdict` or a graded `$state.outcome`"
    return None


def _exit_code(verdict: str, fail_on: str) -> int:
    """Map a verdict → process exit code. ``fail_on`` widens what blocks: 'fail'
    (default) blocks only FAIL; 'warn' blocks WARN too. 0 = pass-through."""
    fail_set = {_FAIL} if fail_on == "fail" else {_FAIL, _WARN}
    return 1 if verdict in fail_set else 0


async def _run_readonly(template_ref: str, input_data: dict, *, mock: bool) -> tuple[dict, set[str]]:
    """Prime pack/operator discovery (the server does this at startup; the CLI must
    do it itself), compile the ref, run it, and return the merged output with the
    keys that would mark a failed step. Lazy imports keep ``cairn gate --help`` cheap."""
    from cairn.core.engine.compiler import compile_template
    from cairn.core.engine.runner import run_within_budget
    from cairn.core.engine.skill_engine import resolve_template_path
    from cairn.core.operator import (
        default_registry as op_reg,
        load_all_operators,
        mock_mode,
    )
    from cairn.core.pack.loader import load_all_packs

    op_reg.clear()
    load_all_operators(strict=False)
    load_all_packs()
    compiled = compile_template(resolve_template_path(template_ref))
    step_error_keys = _step_error_keys(getattr(compiled.spec.flow, "root", None))
    if mock:
        with mock_mode(True):
            return await run_within_budget(compiled, input_data), step_error_keys
    return await run_within_budget(compiled, input_data), step_error_keys


def _load_inputs(input_file: Optional[str], sets: Optional[list[str]]) -> dict[str, Any]:
    """Merge a JSON ``--input`` file with inline ``--set k=v`` overrides (strings)."""
    values: dict[str, Any] = {}
    if input_file:
        p = Path(input_file)
        if not p.exists():
            raise ValueError(f"--input file not found: {p}")
        loaded = json.loads(p.read_text())
        if not isinstance(loaded, dict):
            raise ValueError("--input must be a JSON object of input values")
        values.update(loaded)
    for kv in sets or []:
        if "=" not in kv:
            raise ValueError(f"--set expects k=v, got: {kv!r}")
        k, v = kv.split("=", 1)
        values[k] = v
    return values


def _render_human(template_ref: str, v: dict, rc: int) -> str:
    """A compact terminal summary (goes to stderr; stdout stays clean for --json)."""
    icon = {"pass": "✓", "warn": "⚠", "fail": "✕"}.get(v["verdict"], "•")
    lines = [f"{icon} gate {v['verdict'].upper()} · {template_ref}"]
    for b in v["blocking"][:20]:
        lines.append(f"  ⛔ {b}")
    for w in v["warnings"][:20]:
        lines.append(f"  ⚠ {w}")
    for r in v["reasons"][:20]:
        sev = "⚠" if r.get("severity") == "warn" else "⛔"
        lines.append(f"  {sev} {r.get('name')}: {r.get('value')}")
    lines.append(f"→ exit {rc}")
    return "\n".join(lines)


def _gate_error(template_ref: str, err: str, json_out: bool) -> None:
    if json_out:
        print(json.dumps({"template": template_ref, "gate": "error", "error": err}))
    print(f"✗ gate ERROR · {template_ref}: {err}", file=sys.stderr)


def cmd(
    template_ref: str = typer.Argument(
        ..., help="Namespaced check workflow, e.g. 'my-pack/check'.",
    ),
    input_file: Optional[str] = typer.Option(
        None, "--input", "-i", help="Path to a JSON object of input values (typed).",
    ),
    sets: Optional[list[str]] = typer.Option(
        None, "--set", help="Inline input override k=v (repeatable; string-valued).",
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", "-p", help="Environment/profile to resolve bindings under.",
    ),
    fail_on: str = typer.Option(
        "fail", "--fail-on",
        help="Minimum severity that exits non-zero: 'fail' (default) or 'warn'.",
    ),
    json_out: bool = typer.Option(
        False, "--json", help="Emit the machine-readable JSON verdict to stdout.",
    ),
    mock: bool = typer.Option(
        False, "--mock",
        help="Run operators in mock mode (secretless CI — no real backends).",
    ),
) -> None:
    """Run a check workflow read-only and gate on its verdict (CI). Non-zero on FAIL.

    The CI face of cairn's gate primitive: emits a JSON verdict + a blocking exit
    code, so `cairn gate my-pack/check -i in.json` fails the PR when the check fails.
    Exit 2 means nothing was decided: bad inputs, a step that failed, a pause at an
    approval, or no usable verdict.
    See examples/ci/ for copy-paste GitHub / GitLab workflows.
    """
    if fail_on not in ("fail", "warn"):
        print("✗ --fail-on must be 'fail' or 'warn'", file=sys.stderr)
        raise typer.Exit(2)

    # #305 — resolve bindings under the named profile (parity with `cairn run`).
    if profile:
        import os
        os.environ["CAIRN_PROFILE"] = profile

    try:
        values = _load_inputs(input_file, sets)
    except (ValueError, json.JSONDecodeError) as e:
        print(f"✗ {e}", file=sys.stderr)
        raise typer.Exit(2) from e

    try:
        output, step_error_keys = asyncio.run(_run_readonly(template_ref, values, mock=mock))
    except Exception as e:  # a compile/run failure is a gate ERROR (exit 2), not a FAIL verdict
        _gate_error(template_ref, f"{type(e).__name__}: {e}", json_out)
        raise typer.Exit(2) from e

    v = _extract_verdict(output)
    # A FAIL stands whatever else went wrong: the check already found a reason to block.
    problem = None if v["verdict"] == _FAIL else _run_problem(output, v, step_error_keys)
    if problem:
        _gate_error(template_ref, problem, json_out)
        raise typer.Exit(2)
    rc = _exit_code(v["verdict"], fail_on)

    if json_out:
        print(json.dumps({
            "template": template_ref,
            "gate": "fail" if rc else "pass",
            **v,
        }))
    print(_render_human(template_ref, v, rc), file=sys.stderr)
    raise typer.Exit(rc)
