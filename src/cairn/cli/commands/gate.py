"""``cairn gate`` — run a check workflow read-only in CI and gate on its verdict.

#990 (ADR-0076, D2) — the CI/CD face of cairn's core primitive. Runs a check
workflow (e.g. ``dataset-readiness/check``), reads its deterministic
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
**2** = error (couldn't run / bad inputs).
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


def _classify(value: str) -> str:
    """Map a raw verdict string (e.g. "FAIL", "pass", "warn") → canonical severity."""
    v = str(value).lower()
    if re.search(r"fail|block|reject", v):
        return _FAIL
    if re.search(r"warn|degrad", v):
        return _WARN
    return _PASS


def _extract_verdict(output: dict) -> dict:
    """Pull the domain pass/warn/fail verdict + cited reasons out of a run's output.

    Mirrors the console's ``domainVerdict`` (ADR-0075 outcome channel): prefer the
    ``outcome`` list's verdict head; fall back to a top-level ``verdict`` scalar
    (what the dataset-readiness gate writes). Cited reasons are the non-ok checks."""
    output = output if isinstance(output, dict) else {}
    outcome = output.get("outcome") if isinstance(output.get("outcome"), list) else []
    head = next(
        (o for o in outcome
         if isinstance(o, dict) and _VERDICT_HEAD.search(str(o.get("name", "")))),
        None,
    )
    raw = str(head.get("value", "")) if head is not None else str(output.get("verdict", "") or "")
    verdict = _classify(raw) if raw else _PASS
    reasons = [
        {"name": o.get("name"), "value": o.get("value"), "severity": o.get("severity")}
        for o in outcome
        if isinstance(o, dict) and o is not head
        and (o.get("severity") not in (None, "ok") or o.get("pass") is False)
    ]
    return {
        "verdict": verdict,
        "label": (raw or verdict).upper(),
        "blocking": output.get("blocking") or [],
        "warnings": output.get("warnings") or [],
        "reasons": reasons,
    }


def _exit_code(verdict: str, fail_on: str) -> int:
    """Map a verdict → process exit code. ``fail_on`` widens what blocks: 'fail'
    (default) blocks only FAIL; 'warn' blocks WARN too. 0 = pass-through."""
    fail_set = {_FAIL} if fail_on == "fail" else {_FAIL, _WARN}
    return 1 if verdict in fail_set else 0


async def _run_readonly(template_ref: str, input_data: dict, *, mock: bool) -> dict:
    """Prime pack/operator discovery (the server does this at startup; the CLI must
    do it itself), compile the ref, run it, and return the merged output. Lazy
    imports keep ``cairn gate --help`` cheap."""
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
    if mock:
        with mock_mode(True):
            return await run_within_budget(compiled, input_data)
    return await run_within_budget(compiled, input_data)


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


def cmd(
    template_ref: str = typer.Argument(
        ..., help="Namespaced check workflow, e.g. 'dataset-readiness/check'.",
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
    code, so `cairn gate dataset-readiness/check -i in.json` fails the PR when the
    dataset is bad. See examples/ci/ for copy-paste GitHub / GitLab workflows.
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
        output = asyncio.run(_run_readonly(template_ref, values, mock=mock))
    except Exception as e:  # a compile/run failure is a gate ERROR (exit 2), not a FAIL verdict
        err = f"{type(e).__name__}: {e}"
        if json_out:
            print(json.dumps({"template": template_ref, "gate": "error", "error": err}))
        print(f"✗ gate ERROR · {template_ref}: {err}", file=sys.stderr)
        raise typer.Exit(2) from e

    v = _extract_verdict(output)
    rc = _exit_code(v["verdict"], fail_on)

    if json_out:
        print(json.dumps({
            "template": template_ref,
            "gate": "fail" if rc else "pass",
            **v,
        }))
    print(_render_human(template_ref, v, rc), file=sys.stderr)
    raise typer.Exit(rc)
