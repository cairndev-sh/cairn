"""`cairn preflight` / `cairn run --dry-run` CLI face (#800, ADR-0072).

The verdict composition itself is covered in tests/ask/test_preflight.py; here
we pin the CLI's rendering + exit-code contract, which is what scripts/CI gate
on. Hermetic — builds Preflight objects directly, no engine/pack load.
"""
from __future__ import annotations

from cairn.core.preflight import Blocker, GateInfo, Preflight
from cairn.cli.commands.run import _render_preflight


def _text(pf: Preflight) -> str:
    return "\n".join(_render_preflight(pf))


def test_ready_verdict_renders_ready_and_command() -> None:
    pf = Preflight(ref="dataset-readiness/check", found=True, ready=True,
                   command="cairn run dataset-readiness/check")
    out = _text(pf)
    assert "preflight · dataset-readiness/check" in out
    assert "READY ✓" in out
    assert "BLOCKED" not in out
    assert "cairn run dataset-readiness/check" in out


def test_blocked_verdict_lists_blockers_with_fixes() -> None:
    pf = Preflight(
        ref="finetune/run", found=True, ready=False,
        blockers=[Blocker(kind="input-missing", what="'path' is a required property",
                          fix="fill the field or fix its type")],
    )
    out = _text(pf)
    assert "BLOCKED ✕" in out
    assert "[input-missing] 'path' is a required property" in out
    assert "→ fill the field" in out


def test_gates_and_warnings_are_surfaced_but_not_blockers() -> None:
    pf = Preflight(ref="finetune/run", found=True, ready=True,
                   gates=[GateInfo(step="gate"), GateInfo(step="model_gate")],
                   warnings=["connection 'openai' last probed unhealthy: 401"])
    out = _text(pf)
    assert "READY ✓" in out  # gates don't block readiness
    assert "2 human approval(s) — gate, model_gate" in out
    assert "⚠ connection 'openai'" in out


def test_cost_renders_numeric_estimate_when_present_else_honest_note() -> None:
    numeric = _text(Preflight(ref="x/y", ready=True, cost={"min_usd": 1, "max_usd": 3}))
    assert "est $1–$3" in numeric
    none = _text(Preflight(ref="x/y", ready=True, cost={"min_usd": None, "max_usd": None}))
    assert "no static estimate" in none


def test_not_found_verdict() -> None:
    pf = Preflight(ref="nope/nope", found=False, ready=False,
                   blockers=[Blocker(kind="template-not-found", what="no template",
                                     fix="check the ref")])
    out = _text(pf)
    assert "NOT FOUND" in out
    assert "check the ref" in out


# ── a ref that is installed but is not an agentic template (#2536) ──────────────────────
# The example pack ships a pipeline and a data flow and no agentic template, so both used
# to come back as "NOT FOUND — no such template … install its pack".


def test_preflight_names_a_pipeline_and_points_at_pipeline_explain(example_pack, capsys) -> None:
    from cairn.cli.commands.run import _preflight_sync

    for ref in ("hello-cairn/profile-and-publish", "pipeline:hello-cairn/profile-and-publish"):
        assert _preflight_sync(ref, None, check_connections=False) == 1
        out = capsys.readouterr().out
        assert f"{ref} is a pipeline — preflight checks agentic templates only" in out
        assert "cairn run hello-cairn/profile-and-publish --dry-run" in out
        assert "NOT FOUND" not in out


def test_dry_run_names_a_data_flow_and_points_at_pipeline_run(example_pack, tmp_path, capsys) -> None:
    from cairn.cli.commands.run import _dry_run_template

    inp = tmp_path / "in.json"
    inp.write_text("{}")
    assert _dry_run_template("hello-cairn/engine-check", str(inp)) == 1
    out = capsys.readouterr().out
    assert "hello-cairn/engine-check is a data flow — the dev loop" in out
    assert "runs agentic templates only" in out
    assert "cairn run <pipeline>" in out
    assert "NOT FOUND" not in out


def test_an_unknown_ref_is_still_not_found(example_pack, capsys) -> None:
    from cairn.cli.commands.run import _preflight_sync

    assert _preflight_sync("hello-cairn/nope", None, check_connections=False) == 1
    assert "NOT FOUND — no such template" in capsys.readouterr().out


def test_a_ref_that_is_both_a_template_and_a_pipeline_previews_the_template(
        scratch_pack, dev_packs, capsys) -> None:
    """The template is what preflight checks; the pipeline redirect fires only when no
    template is found (pf.found)."""
    from cairn.cli.commands.run import _preflight_sync

    d = scratch_pack("scratch_both", "scratch-both")
    (d / "pipelines").mkdir()
    (d / "pipelines" / "echo.yaml").write_text(
        "name: echo\nkind: pipeline\nsteps:\n  - id: echo\n    template: echo\n")
    dev_packs(d)
    _preflight_sync("scratch-both/echo", None, check_connections=False)
    out = capsys.readouterr().out
    assert "preflight · scratch-both/echo" in out
    assert "READY" in out or "BLOCKED" in out
    assert "is a pipeline" not in out and "NOT FOUND" not in out


def test_an_ambiguous_flow_ref_is_reported_not_guessed(example_pack, monkeypatch, capsys) -> None:
    from cairn.cli.commands import run as run_cmd
    from cairn.core.dataflow import defs

    def tied(ref):
        raise defs.DataflowDefError(f"flow ref {ref!r} names 2 installed flows")

    monkeypatch.setattr(defs, "flow_def_for", tied)
    assert run_cmd._preflight_sync("hello-cairn/nope", None, check_connections=False) == 1
    out = capsys.readouterr().out
    assert "hello-cairn/nope is an ambiguous flow ref" in out
    assert "names 2 installed flows" in out
