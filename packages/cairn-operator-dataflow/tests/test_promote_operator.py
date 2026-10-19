"""artifact/promote (#1052): promotion as a GATED run, not a bare API call.

The point of these tests is the governance shape — that the gate is declared at
the operator chokepoint (so the existing run queue handles it), that the
displaced target is captured for rollback, and that a rejected gate can never
report a move that didn't happen."""
from __future__ import annotations

import asyncio
from importlib.metadata import PackageNotFoundError, distribution

import pytest

from cairn.core.operator import OperationContext, default_registry, load_all_operators
from cairn.core.storage.artifacts import ArtifactDescriptor, SqliteArtifactStore

from operator_dataflow.operators import promote as promote_mod
from operator_dataflow.operators.promote import ArtifactPromoteOp
from operator_dataflow.schemas.promote import ArtifactPromoteInput


def _ctx() -> OperationContext:
    return OperationContext(pipeline_id="run-promote-1", node_name="promote")


async def _seed(store: SqliteArtifactStore, aid: str) -> str:
    return await store.register(
        ArtifactDescriptor(uri=f"file:///data/{aid}.jsonl", content_hash=aid, parents=[], stats={}),
        producer_run_id="run-clean-1", producer_job="job-1",
    )


def test_declares_the_gate_at_the_operator_chokepoint():
    """requires_approval on the OPERATOR (pack.toml) is what routes promotion
    into the existing run gate queue — no second approval mechanism."""
    load_all_operators(strict=False)
    md = default_registry.get_metadata("artifact/promote")
    assert md is not None
    assert getattr(md, "requires_approval", False) is True
    assert getattr(md, "risk", None) == "external_write"


def test_promote_moves_tag_and_records_what_it_displaced():
    store = SqliteArtifactStore()
    promote_mod._reset_store(store)
    try:
        async def go():
            first = await _seed(store, "aaaa1111")
            second = await _seed(store, "bbbb2222")
            op = ArtifactPromoteOp()

            out1 = await op.run(ArtifactPromoteInput(tag="dataset@prod", artifact_id=first), _ctx())
            assert out1.promoted is True and out1.artifact_id == first
            assert out1.previous_artifact_id == ""  # first promotion displaces nothing

            out2 = await op.run(ArtifactPromoteInput(tag="dataset@prod", artifact_id=second), _ctx())
            # the rollback target is explicit in the record
            assert out2.previous_artifact_id == first and out2.artifact_id == second
            # and the tag really moved in the store
            assert (await store.resolve("dataset@prod")).id == second
        asyncio.run(go())
    finally:
        promote_mod._reset_store(None)


def test_mock_never_promotes():
    """A sandbox/mock run must not move a governed tag."""
    out = ArtifactPromoteOp().mock(
        ArtifactPromoteInput(tag="dataset@prod", artifact_id="x"), _ctx())
    assert out.promoted is False


def _installed(name: str) -> bool:
    try:
        distribution(name)
    except PackageNotFoundError:
        return False
    return True


@pytest.mark.skipif(not _installed("cairn-pack-dataset-readiness"),
                    reason="the template is cairn/packs/dataset_readiness/flows/promote.md, shipped by the "
                           "platform-only cairn-pack-dataset-readiness, which is not installed")
def test_promote_template_compiles_gated():
    """The template binds the operator; the gate flag makes the compiled graph
    checkpointed so the run can durably pause and resume."""
    from pathlib import Path

    from cairn.core.engine.compiler import compile_template
    from cairn.core.pack.loader import load_all_packs

    load_all_operators(strict=False)
    load_all_packs()
    compiled = compile_template(Path("cairn/packs/dataset_readiness/flows/promote.md"))
    node_fns = getattr(compiled.runnable, "_node_fns", {})
    joined = " ".join(node_fns)
    assert "promote_artifact" in joined, list(node_fns)
    assert "assemble_artifact_promote_report" in joined, list(node_fns)


def test_report_never_claims_an_unapproved_move():
    """Gate rejected → the operator never ran → findings carry no promoted flag,
    and the report must say promoted=False rather than inventing success."""
    from cairn.core.engine.python_function_registry import assemble_artifact_promote_report

    rejected = assemble_artifact_promote_report({"findings": {}})
    assert rejected["promoted"] is False and rejected["artifact_id"] == ""

    approved = assemble_artifact_promote_report({"findings": {
        "promote_tag": "dataset@prod", "promote_artifact_id": "bbbb2222",
        "promote_previous": "aaaa1111", "promote_uri": "file:///d.jsonl", "promoted": True,
    }})
    assert approved["promoted"] is True and approved["previous_artifact_id"] == "aaaa1111"
