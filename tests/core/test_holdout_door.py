"""The holdout door (ADR-0102 D3): a training version that shares a row with a golden or test
version in the same workspace is refused, at every governed path that asks.

Hermetic and real where the claim is: a temp sqlite file behind ``CAIRN_STORAGE``, JSONL files on
disk, and the door builds its store from that environment exactly as a server does. Nothing here
stubs the door's decision, the row comparison or the registry's role and workspace queries. The
stubs that remain replace compute (the compiled graph, the run itself) or the record the door has
to stay in front of (the bridge's ``start_pipeline``).

The near-duplicate pass needs datasketch, which is not a dependency of this repository, so its
refusal is not proved here; what is proved is that its absence is said out loud.
"""
from __future__ import annotations

import asyncio
import hashlib
import itertools
import json
import os
import sys
from datetime import date, datetime
from types import SimpleNamespace

import pytest

from cairn.core import holdout as holdout_module
from cairn.core.admission_door import ADMITTED, REFUSED, AdmissionRefusedError, decide_admission
from cairn.core.dataflow.contract import ArtifactDescriptor
from cairn.core.dataset_roles import DatasetRoleError, row_id
from cairn.core.dispatch.pipeline_launch import launch_pipeline_def
from cairn.core.engine.pipeline import parse_pipeline_def
from cairn.core.holdout import (
    canonical_row_hash,
    check_holdout,
    check_holdout_for_inputs,
    dataset_refs_in,
    row_shingles,
)
from cairn.core.storage.artifacts import build_artifact_store, resolve_dataset_refs_in_config
from cairn.core.storage.judge_labels import JudgeLabel, build_judge_label_store

WS = "default"

SHARED = {"id": "shared-row", "prompt": "At what temperature does water boil at sea level?",
          "completion": "One hundred degrees Celsius."}
HOLDOUT = [SHARED, {"id": "held-2", "prompt": "Name the largest planet in the solar system.",
                    "completion": "Jupiter."}]
TRAIN = [{"id": "train-1", "prompt": "Which ocean lies between Africa and Australia?",
          "completion": "The Indian Ocean."},
         {"id": "train-2", "prompt": "Who wrote the play Hamlet?",
          "completion": "William Shakespeare."}]


class _Registry:
    """Registers real versions of real files through the store the door reads."""

    def __init__(self, store, root):
        self.store, self.root = store, root
        self._files = itertools.count()

    def write(self, rows) -> str:
        path = self.root / f"rows-{next(self._files)}.jsonl"
        path.write_text("".join(json.dumps(r) + "\n" for r in rows), encoding="utf-8")
        return str(path)

    async def version(self, name, rows, *, role="training", workspace=WS, dated_at=None, rate=True) -> str:
        path = self.write(rows)
        with open(path, "rb") as fh:
            digest = hashlib.sha256(fh.read()).hexdigest()
        await self.store.register(
            ArtifactDescriptor(uri=f"file://{path}", kind="dataset", content_hash=digest),
            producer_run_id="prep", workspace_id=workspace)
        if role == "golden" and rate:
            labels = build_judge_label_store()
            for i, row in enumerate(rows):
                await labels.put_label(JudgeLabel(judge_name="reviewer", workspace_id=workspace,
                                                  dataset_ref=f"{name}@1", row_id=row_id(row, i),
                                                  human_label="pass"))
        v = await self.store.register_dataset_version(name, digest, actor="prep", role=role,
                                                      workspace_id=workspace, dated_at=dated_at)
        return f"dataset:{name}@{v}"

    async def delete_rows_of(self, ref: str, workspace=WS) -> None:
        name, _, version = ref.removeprefix("dataset:").partition("@")
        meta = await self.store.get_dataset_version_meta(name, int(version), workspace_id=workspace)
        artifact = await self.store.get(meta["artifact_id"], workspace_id=workspace)
        os.remove(artifact.uri.removeprefix("file://"))


@pytest.fixture
async def registry(tmp_path, monkeypatch):
    monkeypatch.setenv("CAIRN_STORAGE", f"sqlite:///{tmp_path}/cairn.db")
    store = build_artifact_store()
    yield _Registry(store, tmp_path)
    await store.close()


@pytest.fixture
def no_datasketch(monkeypatch):
    """The install this repository's own dependencies give: the optional import fails."""
    monkeypatch.setitem(sys.modules, "datasketch", None)


async def _check(inputs, *, workspace=WS, **kw):
    return await check_holdout_for_inputs(inputs, workspace_id=workspace, **kw)


# ── the refuse/admit pair ───────────────────────────────────────────────────


@pytest.mark.parametrize("role", ["golden", "test"])
async def test_one_shared_row_refuses_and_the_same_data_without_it_is_admitted(registry, role):
    held = await registry.version("eval-set", HOLDOUT, role=role)
    dirty = await registry.version("corpus", [*TRAIN, SHARED])
    clean = await registry.version("corpus", TRAIN)

    refused = await _check({"train": dirty})
    assert refused.ok is False and refused.checked is True
    (overlap,) = refused.overlaps
    assert (overlap.training, overlap.holdout, overlap.holdout_role, overlap.method, overlap.row_ids) == (
        dirty, held, role, "exact", ["shared-row"])
    assert "shared-row" in refused.reasons()[0] and held in refused.reasons()[0]

    admitted = await _check({"train": clean})
    assert admitted.ok is True and admitted.checked is True and admitted.overlaps == []
    assert [v.ref for v in admitted.training] == [clean]
    assert [v.ref for v in admitted.holdouts] == [held]


async def test_a_golden_version_needs_a_human_rating_before_it_can_hold_anything_out(registry):
    """How a golden version comes to exist: registering one with no rating is refused at the
    write. A ``test`` version has no such rule."""
    with pytest.raises(DatasetRoleError, match="carry no human rating"):
        await registry.version("unrated", HOLDOUT, role="golden", rate=False)
    await registry.version("unrated-test", HOLDOUT, role="test")


async def test_a_refusal_names_a_row_with_no_id_by_its_position(registry):
    await registry.version("eval-set", [{"prompt": "same", "completion": "row"}], role="test")
    dirty = await registry.version("corpus", [{"prompt": "other", "completion": "row"},
                                              {"prompt": "same", "completion": "row"}])
    assert (await _check({"train": dirty})).overlaps[0].row_ids == ["row-1"]


async def test_a_long_refusal_is_capped_but_counts_every_row(registry):
    rows = [{"id": f"r{i}", "text": f"row number {i}"} for i in range(60)]
    await registry.version("eval-set", rows, role="test")
    verdict = await _check({"train": await registry.version("corpus", rows)})
    assert "(+50 more)" in verdict.reasons()[0]
    record = verdict.as_record()["overlaps"][0]
    assert record["row_count"] == 60 and len(record["row_ids"]) == 50


# ── what "shares a row" means: the exact pass ───────────────────────────────


@pytest.mark.parametrize("same_row, refused", [
    ({"completion": SHARED["completion"], "prompt": SHARED["prompt"], "id": SHARED["id"]}, True),
    ({**SHARED, "prompt": SHARED["prompt"].replace(" water", "   water")}, True),
    ({**SHARED, "prompt": SHARED["prompt"].upper()}, False),
    ({**SHARED, "id": "another-id"}, False),
    ({**SHARED, "source": "added later"}, False),
    ({**SHARED, "prompt": SHARED["prompt"].replace(" water", "\twater")}, False),
])
async def test_the_exact_pass_compares_whole_rows(registry, same_row, refused, no_datasketch):
    """Key order and runs of spaces do not make a different row. Case, a tab, a different id or
    one extra key do: the hash covers the whole row, not its text."""
    await registry.version("eval-set", HOLDOUT, role="test")
    verdict = await _check({"train": await registry.version("corpus", [same_row])})
    assert verdict.ok is not refused


async def test_without_datasketch_a_paraphrase_is_admitted_and_the_verdict_says_only_hashes_ran(
        registry, no_datasketch):
    await registry.version("eval-set", HOLDOUT, role="test")
    reworded = {**SHARED, "prompt": "So: " + SHARED["prompt"]}
    verdict = await _check({"train": await registry.version("corpus", [reworded])})
    assert verdict.ok is True and verdict.near_dup_checked is False
    assert any("near-duplicate pass skipped" in w and "only exact row hashes" in w for w in verdict.warnings)
    assert verdict.as_record()["near_dup_checked"] is False


async def test_a_caller_that_turns_the_near_dup_pass_off_gets_no_warning_about_it(registry, no_datasketch):
    await registry.version("eval-set", HOLDOUT, role="test")
    verdict = await _check({"train": await registry.version("corpus", TRAIN)}, near_dup=False)
    assert verdict.ok is True and verdict.near_dup_checked is False and verdict.warnings == []


# ── which refs and which versions the door looks at ─────────────────────────


async def test_a_ref_inside_a_list_input_is_found(registry):
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    assert (await _check({"train": ["not-a-ref", dirty]})).ok is False


async def test_the_door_sees_the_refs_a_launch_resolves_and_no_others(registry):
    """Top-level strings and top-level lists, as the resolver: a ref nested deeper is one the
    launch never reads, so the door neither sees it nor refuses on it."""
    await registry.version("eval-set", HOLDOUT, role="test")
    top = await registry.version("corpus", TRAIN)
    in_list = await registry.version("corpus-b", TRAIN)
    nested = await registry.version("corpus-c", [SHARED])
    inputs = {"a": top, "again": top, "b": [in_list], "c": {"deep": nested}, "d": "plain"}
    assert dataset_refs_in(inputs) == [top, in_list]

    resolved = await resolve_dataset_refs_in_config(registry.store, inputs, workspace_id=WS)
    assert resolved["c"] == {"deep": nested}
    assert resolved["a"] != top and resolved["b"] != [in_list]

    verdict = await _check({"c": {"deep": nested}})
    assert verdict.ok is True and verdict.checked is False


async def test_a_ref_with_no_version_means_the_head_and_a_pinned_ref_means_that_version(registry):
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [*TRAIN, SHARED])
    await registry.version("corpus", TRAIN)
    assert (await _check({"train": "dataset:corpus"})).ok is True
    assert (await _check({"train": dirty})).ok is False


async def test_a_validation_split_is_not_a_holdout(registry):
    await registry.version("val-split", HOLDOUT, role="validation")
    verdict = await _check({"train": await registry.version("corpus", [SHARED])})
    assert verdict.ok is True and verdict.checked is True and verdict.holdouts == []


@pytest.mark.parametrize("role", ["validation", "test"])
async def test_a_version_that_is_not_training_data_is_not_a_candidate(registry, role):
    await registry.version("eval-set", HOLDOUT, role="test")
    other = await registry.version("not-training", [SHARED], role=role)
    verdict = await _check({"data": other})
    assert verdict.ok is True and verdict.checked is False and verdict.training == []


async def test_no_golden_or_test_version_is_checked_and_clean(registry):
    verdict = await _check({"train": await registry.version("corpus", [SHARED])})
    assert verdict.ok is True and verdict.checked is True
    assert verdict.holdouts == [] and verdict.overlaps == []


async def test_inputs_with_no_dataset_ref_are_not_checked(registry):
    verdict = await _check({"path": "/data/train.jsonl"})
    assert verdict.ok is True and verdict.checked is False and verdict.warnings == []


# ── workspace scoping ───────────────────────────────────────────────────────


async def test_the_door_never_compares_across_workspaces(registry):
    held = await registry.version("eval-set", HOLDOUT, role="golden", workspace="tenant-a")
    theirs = await registry.version("corpus-a", [SHARED], workspace="tenant-a")
    ours = await registry.version("corpus-b", [SHARED], workspace="tenant-b")

    # The same rows refuse where the holdout lives...
    refused = await _check({"train": theirs}, workspace="tenant-a")
    assert refused.ok is False and refused.overlaps[0].holdout == held

    # ...and tenant-a's golden set does not exist for tenant-b: not a row of it is read, which
    # a deleted file proves (reading it would warn), and the verdict never names it.
    await registry.delete_rows_of(held, workspace="tenant-a")
    admitted = await _check({"train": ours}, workspace="tenant-b")
    assert admitted.ok is True and admitted.checked is True and admitted.holdouts == []
    assert "eval-set" not in json.dumps(admitted.as_record())
    assert not any("could not be read" in w for w in admitted.warnings)


async def test_another_workspaces_version_reads_as_not_registered(registry):
    theirs = await registry.version("corpus-a", [SHARED], workspace="tenant-a")
    await registry.version("eval-set", HOLDOUT, role="test", workspace="tenant-b")
    verdict = await _check({"train": theirs}, workspace="tenant-b")
    assert verdict.ok is True and verdict.checked is False and verdict.training == []
    assert any("not registered" in w for w in verdict.warnings)


# ── the eval cutoff (the holdout side only) ─────────────────────────────────


async def test_a_holdout_version_dated_after_the_cutoff_is_excluded_and_named(registry):
    later = await registry.version("eval-later", HOLDOUT, role="test", dated_at="2026-06-02")
    dirty = await registry.version("corpus", [SHARED])
    assert (await _check({"train": dirty})).ok is False

    verdict = await _check({"train": dirty, "eval_cutoff": "2026-06-01"})
    assert verdict.ok is True and verdict.checked is True and verdict.holdouts == []
    assert verdict.versions_excluded_by_cutoff == [later]
    assert verdict.as_record()["eval_cutoff"] == "2026-06-01" and later in verdict.cutoff_note()


async def test_a_date_only_cutoff_covers_its_whole_day_and_an_undated_holdout_stays(registry):
    await registry.version("eval-same-day", HOLDOUT, role="test", dated_at="2026-06-01T23:59:00+00:00")
    dirty = await registry.version("corpus", [SHARED])
    assert (await _check({"train": dirty, "eval_cutoff": "2026-06-01"})).ok is False

    await registry.version("eval-undated", [SHARED], role="test")
    verdict = await _check({"train": dirty, "eval_cutoff": "2020-01-01"})
    assert verdict.ok is False and [o.holdout for o in verdict.overlaps] == ["dataset:eval-undated@1"]


async def test_a_holdout_row_dated_after_the_cutoff_is_excluded_and_counted(registry):
    dated = {**SHARED, "dated_at": "2026-06-02"}
    await registry.version("eval-set", [dated, HOLDOUT[1]], role="test")
    dirty = await registry.version("corpus", [dated])
    assert (await _check({"train": dirty})).ok is False

    verdict = await _check({"train": dirty, "eval_cutoff": "2026-06-01"})
    assert verdict.ok is True and verdict.rows_excluded_by_cutoff == 1
    assert verdict.as_record()["eval_versions"][0]["rows_excluded_by_cutoff"] == 1


# ── what the door does when it cannot check: documented as "not checked", never a refusal ──
# holdout.py: "Refusal is only ever on a PROVEN intersection" (or on input it can't use, below), and
# the launch path never sees an exception. These pin that the verdict SAYS so; they are not a claim
# that admitting is right.


async def test_an_unreadable_holdout_is_reported_not_scored_clean(registry):
    held = await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    await registry.delete_rows_of(held)

    verdict = await _check({"train": dirty})
    assert verdict.ok is True  # no intersection was proved
    assert any(held in w and "could not be read" in w for w in verdict.warnings)
    (record,) = verdict.as_record()["eval_versions"]
    assert record["checked"] is False and "FileNotFoundError" in record["note"]


async def test_an_unreadable_training_version_is_reported_not_scored_clean(registry):
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    await registry.delete_rows_of(dirty)

    verdict = await _check({"train": dirty})
    assert verdict.ok is True and verdict.overlaps == []
    assert any(dirty in w and "could not be read" in w for w in verdict.warnings)
    assert verdict.as_record()["training_versions"][0]["checked"] is False


class _Failing:
    """The real store, except the named reads fail the way a database that went away does."""

    def __init__(self, store, *methods):
        self._store, self._methods = store, methods

    def __getattr__(self, name):
        if name in self._methods:
            async def _boom(*a, **k):
                raise ConnectionError("registry went away")
            return _boom
        return getattr(self._store, name)


@pytest.mark.parametrize("method, says", [
    ("get_dataset_version_meta", "registry lookup failed (ConnectionError"),
    ("list_dataset_versions_by_role", "holdout NOT checked"),
])
async def test_a_registry_that_cannot_answer_reads_as_not_checked_never_an_error(registry, method, says):
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    verdict = await _check({"train": dirty}, store=_Failing(registry.store, method))
    assert verdict.ok is True and verdict.checked is False
    assert any(says in w for w in verdict.warnings)


async def test_a_registry_with_no_dataset_roles_reads_as_not_checked(registry):
    verdict = await _check({"train": "dataset:corpus@1"}, store=SimpleNamespace())
    assert verdict.ok is True and verdict.checked is False
    assert verdict.warnings == ["registry does not support dataset roles — holdout NOT checked"]


async def test_no_storage_configured_reads_as_not_checked(monkeypatch):
    monkeypatch.setenv("CAIRN_STORAGE", "")
    monkeypatch.setenv("OBS_STORAGE", "")
    verdict = await _check({"train": "dataset:corpus@1"})
    assert verdict.ok is True and verdict.checked is False
    assert verdict.warnings == ["dataset:corpus@1: not registered — nothing to check"]


async def test_an_unexpected_failure_in_the_check_is_a_warning_not_a_500(registry):
    """A registry row the door can't make sense of fails past every guard; the launch path's
    catch-all turns it into a verdict."""
    class _Garbled:
        def __getattr__(self, name):
            return getattr(registry.store, name)

        async def list_dataset_versions_by_role(self, *a, **k):
            return [{"role": "test"}]  # no name, no version

    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    verdict = await _check({"train": dirty}, store=_Garbled())
    assert verdict.ok is True and verdict.checked is False
    assert len(verdict.warnings) == 1 and "holdout check failed (KeyError" in verdict.warnings[0]


async def test_check_holdout_called_directly_takes_the_same_decision(registry):
    """The lower entry point takes refs, not inputs, and is what the launch path wraps."""
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    verdict = await check_holdout([dirty], store=registry.store, workspace_id=WS)
    assert verdict.ok is False and verdict.overlaps[0].row_ids == ["shared-row"]
    assert (await check_holdout([], store=registry.store, workspace_id=WS)).checked is False


# ── the pipeline launch door ────────────────────────────────────────────────

_PIPELINE = """
name: fine-tune
kind: pipeline
inputs:
  train_version: {type: str}
steps:
  - gate: approve
    approvers: owners
"""


class _Bridge:
    """Stands where the record is created: ``start_pipeline`` is the first write of a launch."""

    def __init__(self):
        self.calls = []

    async def start_pipeline(self, plan, **kw):
        self.calls.append(kw)
        return SimpleNamespace(pipeline_id="p-1", **kw)


async def test_decide_admission_refuses_on_the_shared_row_and_records_why(registry):
    held = await registry.version("eval-set", HOLDOUT, role="golden")
    dirty = await registry.version("corpus", [SHARED])
    decision = await decide_admission({"train_version": dirty}, workspace_id=WS, policies=[])
    assert decision.refused and decision.verdict == REFUSED
    assert any("shared-row" in r and held in r for r in decision.refusing_reasons)
    assert decision.as_record()["holdout"]["overlaps"][0]["row_ids"] == ["shared-row"]


async def test_decide_admission_admits_the_clean_version_and_records_the_hash(registry):
    await registry.version("eval-set", HOLDOUT, role="golden")
    clean = await registry.version("corpus", TRAIN)
    decision = await decide_admission({"train_version": clean}, workspace_id=WS, policies=[])
    assert not decision.refused and decision.verdict == ADMITTED
    (held,) = decision.holdout["eval_versions"]
    assert held["ref"] == "dataset:eval-set@1" and held["content_hash"]


async def test_decide_admission_scopes_by_the_launchs_workspace(registry):
    await registry.version("eval-set", HOLDOUT, role="test", workspace="tenant-a")
    ours = await registry.version("corpus", [SHARED], workspace="tenant-b")
    decision = await decide_admission({"train_version": ours}, workspace_id="tenant-b", policies=[])
    assert not decision.refused and decision.holdout["eval_versions"] == []


async def test_a_launch_is_refused_before_any_record_exists(registry):
    """Through the real launch door: the refusal reaches the caller and start_pipeline, where
    the record would be created, is never asked."""
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [*TRAIN, SHARED])
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError, match="shared-row") as refused:
        await launch_pipeline_def(parse_pipeline_def(_PIPELINE, pack="finetune"),
                                  inputs={"train_version": dirty}, bridge=bridge, workspace_id=WS)
    assert refused.value.decision.refused
    assert bridge.calls == []


async def test_a_clean_launch_reaches_the_bridge_with_the_verdict(registry):
    await registry.version("eval-set", HOLDOUT, role="test")
    clean = await registry.version("corpus", TRAIN)
    bridge = _Bridge()
    await launch_pipeline_def(parse_pipeline_def(_PIPELINE, pack="finetune"),
                              inputs={"train_version": clean}, bridge=bridge, workspace_id=WS)
    (call,) = bridge.calls
    assert call["admission"]["verdict"] == ADMITTED
    assert [v["ref"] for v in call["admission"]["holdout"]["eval_versions"]] == ["dataset:eval-set@1"]


async def test_an_unreadable_holdout_does_not_refuse_a_launch_and_the_record_says_it_was_unread(registry):
    held = await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    await registry.delete_rows_of(held)
    decision = await decide_admission({"train_version": dirty}, workspace_id=WS, policies=[])
    assert decision.verdict == ADMITTED
    assert decision.holdout["eval_versions"][0]["checked"] is False
    assert any("could not be read" in w for w in decision.holdout["warnings"])


# ── the run door ────────────────────────────────────────────────────────────


def _dispatcher(monkeypatch):
    """A real dispatcher over the real run store. Stubbed: the template lookup, its compilation
    and the run itself, plus operator and pack loading and the operator list those need, which
    also turns off the job-channel and engine doors. The holdout door and the store are real."""
    from cairn.core.dispatch.dispatcher import RunDispatcher
    from cairn.core.storage import build_storage

    async def _run(compiled, input_data):
        return {"summary": "ran"}

    monkeypatch.setattr("cairn.core.engine.skill_engine.resolve_template_path", lambda ref: "X")
    monkeypatch.setattr("cairn.core.engine.compiler.compile_template", lambda path: SimpleNamespace())
    monkeypatch.setattr("cairn.core.engine.runner.run_within_budget", _run)
    monkeypatch.setattr("cairn.core.operator.load_all_operators", lambda **k: [])
    monkeypatch.setattr("cairn.core.pack.loader.load_all_packs", lambda **k: None)
    monkeypatch.setattr("cairn.core.engine.workflow_config.collect_template_operators", lambda ref: [])
    return RunDispatcher(SimpleNamespace(), backend=build_storage())


# A pipeline's leg (what the bridge submits), and a bare call on a dispatcher with no bridge
# (`_wrap_orphan_run`'s documented fallback). With a bridge, a bare call becomes a launch and
# meets the launch door above first.
_RUN_SHAPES = [{"bundle_pipeline_id": "p-1", "step_id": "train"}, {}]


@pytest.mark.parametrize("shape", _RUN_SHAPES)
async def test_a_run_is_refused_before_any_run_exists(registry, monkeypatch, shape):
    held = await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [*TRAIN, SHARED])
    d = _dispatcher(monkeypatch)
    with pytest.raises(ValueError, match="holdout door") as refused:
        await d.run_template("demo/train", {"train": dirty}, **shape)
    assert "shared-row" in str(refused.value) and held in str(refused.value)
    assert d._tasks == {}
    assert await d.store.list(workspace_id=WS) == []


@pytest.mark.parametrize("shape", _RUN_SHAPES)
async def test_a_clean_run_starts_and_records_which_sets_it_was_held_out_from(registry, monkeypatch, shape):
    held = await registry.version("eval-set", HOLDOUT, role="test")
    clean = await registry.version("corpus", TRAIN)
    d = _dispatcher(monkeypatch)
    run = await d.run_template("demo/train", {"train": clean}, **shape)
    await asyncio.gather(*list(d._tasks.values()), return_exceptions=True)

    assert [r.id for r in await d.store.list(workspace_id=WS)] == [run.id]  # what the refusal leaves empty
    stored = await d.store.get(run.id)
    assert stored is not None and stored.holdout["ok"] is True and stored.holdout["checked"] is True
    (record,) = stored.holdout["eval_versions"]
    assert record["ref"] == held and record["content_hash"]
    assert stored.holdout["training_versions"][0]["ref"] == clean


async def test_a_run_in_another_workspace_is_not_refused(registry, monkeypatch):
    from cairn.core.workspace import workspace_scope

    await registry.version("eval-set", HOLDOUT, role="test", workspace="tenant-a")
    ours = await registry.version("corpus", [SHARED], workspace="tenant-b")
    d = _dispatcher(monkeypatch)
    with workspace_scope("tenant-b"):
        run = await d.run_template("demo/train", {"train": ours}, bundle_pipeline_id="p-1", step_id="train")
        await asyncio.gather(*list(d._tasks.values()), return_exceptions=True)
        stored = await d.store.get(run.id, workspace_id="tenant-b")
    assert stored.holdout["ok"] is True and stored.holdout["eval_versions"] == []


async def test_an_unreadable_holdout_does_not_stop_a_run_and_its_record_says_it_was_unread(registry, monkeypatch):
    held = await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    await registry.delete_rows_of(held)
    d = _dispatcher(monkeypatch)
    run = await d.run_template("demo/train", {"train": dirty}, bundle_pipeline_id="p-1", step_id="train")
    await asyncio.gather(*list(d._tasks.values()), return_exceptions=True)
    stored = await d.store.get(run.id)
    assert stored.holdout["eval_versions"][0]["checked"] is False
    assert any("could not be read" in w for w in stored.holdout["warnings"])


# ── preflight ───────────────────────────────────────────────────────────────


async def _preflight(monkeypatch, values):
    from cairn.core import preflight as pf
    from cairn.core.ui_contract import UIContract

    plan = {"template_found": True, "operators": []}  # the template lookup; no pack is installed
    monkeypatch.setattr(pf, "build_ui_contract", lambda ref, profile=None: UIContract(template_ref=ref, plan=plan))
    return await pf.build_preflight("demo/train", values, check_connections=False)


async def test_preflight_blocks_on_the_shared_row(registry, monkeypatch):
    held = await registry.version("eval-set", HOLDOUT, role="golden")
    dirty = await registry.version("corpus", [*TRAIN, SHARED])
    verdict = await _preflight(monkeypatch, {"train": dirty})
    assert verdict.ready is False
    (blocker,) = [b for b in verdict.blockers if b.kind == "holdout-overlap"]
    assert "shared-row" in blocker.what and held in blocker.what and blocker.fix


async def test_preflight_has_no_holdout_blocker_on_the_clean_version(registry, monkeypatch):
    await registry.version("eval-set", HOLDOUT, role="golden")
    clean = await registry.version("corpus", TRAIN)
    verdict = await _preflight(monkeypatch, {"train": clean})
    assert [b for b in verdict.blockers if b.kind == "holdout-overlap"] == []
    assert verdict.ready is True


async def test_preflight_passes_on_what_the_door_could_not_check(registry, monkeypatch, no_datasketch):
    held = await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    await registry.delete_rows_of(held)
    verdict = await _preflight(monkeypatch, {"train": dirty})
    assert [b for b in verdict.blockers if b.kind == "holdout-overlap"] == []
    assert any("could not be read" in w for w in verdict.warnings)


# ── a malformed eval cutoff: invalid input, refused whatever the data ───────
# The door doesn't compare, so it can't vouch for anything; and a typo must not read as "no cutoff"
# or as "no holdout". Real door, real registry, real run store, at the launch and at the run.


_MALFORMED_CUTOFFS = ["last tuesday", "2026-13-01", "06/01/2026", "2026-06", 5, True,
                      ["2026-06-01"], {"on": "2026-06-01"}]
_THE_DATA = [pytest.param([SHARED], id="overlapping"), pytest.param(TRAIN, id="clean")]
_NESTED = [pytest.param(lambda c: {"params": {"eval_cutoff": c}}, id="nested"),
           pytest.param(lambda c: {"legs": [{"eval_cutoff": c}]}, id="in-a-list")]


def _says_the_cutoff_is_invalid(text: str) -> None:
    assert "eval_cutoff" in text and "ISO-8601" in text and "'2026-06-01'" in text
    assert "nothing was compared" in text and "overlaps" not in text
    assert "last tuesday" not in text  # the reason reaches the audit row, which carries no inputs


async def _assert_the_launch_is_refused(inputs) -> None:
    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError) as refused:
        await launch_pipeline_def(parse_pipeline_def(_PIPELINE, pack="finetune"),
                                  inputs=inputs, bridge=bridge, workspace_id=WS)
    assert bridge.calls == []
    decision = refused.value.decision
    assert decision.verdict == REFUSED and len(decision.refusing_reasons) == 1
    _says_the_cutoff_is_invalid(decision.refusing_reasons[0])
    assert decision.holdout["ok"] is False and decision.holdout["checked"] is False
    assert decision.holdout["refusal"] == decision.refusing_reasons[0]


async def _assert_the_run_is_refused(d, inputs, **shape) -> None:
    with pytest.raises(ValueError, match="^holdout door: ") as refused:
        await d.run_template("demo/train", inputs, **shape)
    _says_the_cutoff_is_invalid(str(refused.value))
    assert d._tasks == {}
    assert await d.store.list(workspace_id=WS) == []


async def test_a_malformed_cutoff_says_nothing_was_compared(registry):
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    verdict = await _check({"train": dirty, "eval_cutoff": "last tuesday"})
    assert verdict.ok is False and verdict.checked is False
    assert verdict.training == [] and verdict.holdouts == [] and verdict.overlaps == []
    (reason,) = verdict.reasons()
    _says_the_cutoff_is_invalid(reason)
    assert verdict.as_record()["refusal"] == reason


async def test_a_malformed_cutoff_passed_by_the_caller_is_refused_too(registry):
    clean = await registry.version("corpus", TRAIN)
    verdict = await _check({"train": clean}, cutoff="last tuesday")
    assert verdict.ok is False
    _says_the_cutoff_is_invalid(verdict.reasons()[0])


async def test_a_malformed_cutoff_does_not_admit_an_overlapping_launch(registry):
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    decision = await decide_admission({"train": dirty, "eval_cutoff": "last tuesday"}, workspace_id=WS, policies=[])
    assert decision.verdict == REFUSED


@pytest.mark.parametrize("shape", _RUN_SHAPES)
async def test_a_malformed_cutoff_does_not_let_an_overlapping_run_start(registry, monkeypatch, shape):
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    d = _dispatcher(monkeypatch)
    await _assert_the_run_is_refused(d, {"train": dirty, "eval_cutoff": "last tuesday"}, **shape)


@pytest.mark.parametrize("rows", _THE_DATA)
@pytest.mark.parametrize("cutoff", _MALFORMED_CUTOFFS, ids=str)
async def test_each_malformed_cutoff_is_refused_at_the_launch(registry, cutoff, rows):
    await registry.version("eval-set", HOLDOUT, role="test")
    ref = await registry.version("corpus", rows)
    await _assert_the_launch_is_refused({"train_version": ref, "eval_cutoff": cutoff})


@pytest.mark.parametrize("rows", _THE_DATA)
@pytest.mark.parametrize("cutoff", _MALFORMED_CUTOFFS, ids=str)
async def test_each_malformed_cutoff_is_refused_at_the_run(registry, monkeypatch, cutoff, rows):
    await registry.version("eval-set", HOLDOUT, role="test")
    ref = await registry.version("corpus", rows)
    d = _dispatcher(monkeypatch)
    await _assert_the_run_is_refused(d, {"train": ref, "eval_cutoff": cutoff},
                                     bundle_pipeline_id="p-1", step_id="train")


@pytest.mark.parametrize("rows", _THE_DATA)
@pytest.mark.parametrize("place", _NESTED)
async def test_a_malformed_cutoff_nested_in_the_inputs_is_refused_at_both_doors(registry, monkeypatch, place, rows):
    await registry.version("eval-set", HOLDOUT, role="test")
    ref = await registry.version("corpus", rows)
    await _assert_the_launch_is_refused({"train_version": ref, **place("last tuesday")})
    d = _dispatcher(monkeypatch)
    await _assert_the_run_is_refused(d, {"train": ref, **place("last tuesday")},
                                     bundle_pipeline_id="p-1", step_id="train")


async def test_a_malformed_cutoff_is_refused_with_no_dataset_ref_at_all(registry, monkeypatch):
    await _assert_the_launch_is_refused({"train_version": "/data/train.jsonl", "eval_cutoff": "last tuesday"})
    d = _dispatcher(monkeypatch)
    await _assert_the_run_is_refused(d, {"path": "/data/train.jsonl", "eval_cutoff": "last tuesday"},
                                     bundle_pipeline_id="p-1", step_id="train")


@pytest.mark.parametrize("cutoff", [
    "2026-06-01", " 2026-06-01 ", "2026-06-01T10:00:00+00:00", "2026-06-01T10:00:00Z", "2026-06-01 10:00",
    "20260601", "2026-W22-1", 20260601, date(2026, 6, 1), datetime(2026, 6, 1, 10),
], ids=str)
async def test_every_cutoff_form_accepted_before_still_compares_and_still_excludes(registry, cutoff):
    later = await registry.version("eval-later", HOLDOUT, role="test", dated_at="2026-07-01")
    dirty = await registry.version("corpus", [SHARED])
    clean = await _check({"train": dirty, "eval_cutoff": cutoff})
    assert clean.ok is True and clean.checked is True and clean.refusal == ""
    assert clean.versions_excluded_by_cutoff == [later] and clean.eval_cutoff

    earlier = await registry.version("eval-earlier", HOLDOUT, role="test", dated_at="2026-01-01")
    refused = await _check({"train": dirty, "eval_cutoff": cutoff})
    assert refused.ok is False and [o.holdout for o in refused.overlaps] == [earlier]


async def test_a_valid_cutoff_reaches_the_launch_and_the_run_records(registry, monkeypatch):
    later = await registry.version("eval-later", HOLDOUT, role="test", dated_at="2026-07-01")
    dirty = await registry.version("corpus", [SHARED])
    bridge = _Bridge()
    await launch_pipeline_def(parse_pipeline_def(_PIPELINE, pack="finetune"),
                              inputs={"train_version": dirty, "eval_cutoff": "2026-06-01"},
                              bridge=bridge, workspace_id=WS)
    (call,) = bridge.calls
    assert call["admission"]["verdict"] == ADMITTED
    assert call["admission"]["holdout"]["versions_excluded_by_cutoff"] == [later]

    d = _dispatcher(monkeypatch)
    run = await d.run_template("demo/train", {"train": dirty, "eval_cutoff": "2026-06-01"},
                               bundle_pipeline_id="p-1", step_id="train")
    await asyncio.gather(*list(d._tasks.values()), return_exceptions=True)
    stored = await d.store.get(run.id)
    assert stored.holdout["eval_cutoff"] == "2026-06-01"
    assert stored.holdout["versions_excluded_by_cutoff"] == [later]


async def test_the_last_representable_day_is_a_cutoff_that_excludes_nothing(registry):
    """A well-formed date whose day has no next day: it must not fail past the door's guards."""
    held = await registry.version("eval-set", [{**SHARED, "dated_at": "2026-06-01"}], role="test",
                                  dated_at="2026-06-01")
    dirty = await registry.version("corpus", [{**SHARED, "dated_at": "2026-06-01"}])
    verdict = await _check({"train": dirty, "eval_cutoff": "9999-12-31"})
    assert verdict.ok is False and verdict.checked is True
    assert [o.holdout for o in verdict.overlaps] == [held]
    assert not any("holdout check failed" in w for w in verdict.warnings)


@pytest.mark.parametrize("absent", [{}, {"eval_cutoff": None}, {"eval_cutoff": ""}, {"eval_cutoff": "   "}], ids=str)
async def test_an_absent_cutoff_is_unchanged(registry, absent):
    held = await registry.version("eval-set", HOLDOUT, role="test", dated_at="2026-07-01")
    dirty = await registry.version("corpus", [SHARED])
    clean = await registry.version("corpus-clean", TRAIN)

    refused = await _check({"train": dirty, **absent})
    assert refused.ok is False and refused.refusal == "" and [o.holdout for o in refused.overlaps] == [held]
    assert refused.as_record()["eval_cutoff"] == "" and "refusal" not in refused.as_record()

    admitted = await _check({"train": clean, **absent})
    assert admitted.ok is True and admitted.checked is True and admitted.eval_cutoff == ""


async def test_a_valid_cutoff_with_no_dataset_ref_is_unchanged(registry):
    verdict = await _check({"path": "/data/train.jsonl", "eval_cutoff": "2026-06-01"})
    assert verdict.ok is True and verdict.checked is False and verdict.warnings == []
    assert verdict.eval_cutoff == "2026-06-01"


async def test_preflight_blocks_a_nested_malformed_cutoff_and_does_not_call_it_an_overlap(registry, monkeypatch):
    await registry.version("eval-set", HOLDOUT, role="test")
    clean = await registry.version("corpus", TRAIN)
    verdict = await _preflight(monkeypatch, {"train": clean, "params": {"eval_cutoff": "last tuesday"}})
    assert verdict.ready is False
    (blocker,) = [b for b in verdict.blockers if b.kind.startswith("holdout")]
    assert blocker.kind == "holdout-invalid-cutoff" and "ISO-8601" in blocker.fix
    _says_the_cutoff_is_invalid(blocker.what)


# ── a holdout row that can't be dated ───────────────────────────────────────
# A row with an unreadable date can't be shown to be after the cutoff, so it stays in the comparison
# and the verdict says so. It voids nothing: not the other rows, not the other holdouts.


_UNDATABLE = {"id": "undatable", "text": "the words of this row", "dated_at": "not a date"}
_VALID = {"eval_cutoff": "2026-06-01"}


def _warned_about(verdict, ref: str) -> list[str]:
    return [w for w in verdict.warnings if ref in w]


async def test_one_bad_row_date_does_not_void_the_other_holdouts(registry):
    held = await registry.version("eval-set", [SHARED], role="test")
    bad = await registry.version("eval-bad-date", [_UNDATABLE], role="test")
    dirty = await registry.version("corpus", [SHARED])
    verdict = await _check({"train": dirty, **_VALID})
    assert verdict.ok is False and verdict.checked is True
    assert [o.holdout for o in verdict.overlaps] == [held]
    assert len(_warned_about(verdict, bad)) == 1


async def test_one_bad_row_date_refuses_the_clean_overlap_elsewhere_at_both_doors(registry, monkeypatch):
    await registry.version("eval-set", [SHARED], role="test")
    await registry.version("eval-bad-date", [_UNDATABLE], role="test")
    dirty = await registry.version("corpus", [*TRAIN, SHARED])

    bridge = _Bridge()
    with pytest.raises(AdmissionRefusedError, match="shared-row"):
        await launch_pipeline_def(parse_pipeline_def(_PIPELINE, pack="finetune"),
                                  inputs={"train_version": dirty, **_VALID}, bridge=bridge, workspace_id=WS)
    assert bridge.calls == []

    d = _dispatcher(monkeypatch)
    with pytest.raises(ValueError, match="overlaps a held-out set.*shared-row"):
        await d.run_template("demo/train", {"train": dirty, **_VALID}, bundle_pipeline_id="p-1", step_id="train")
    assert d._tasks == {} and await d.store.list(workspace_id=WS) == []


async def test_a_bad_row_date_leaves_the_other_rows_of_its_version_compared_and_filtered(registry):
    later = {"id": "later", "text": "dated after the cutoff", "dated_at": "2026-07-01"}
    await registry.version("eval-set", [_UNDATABLE, SHARED, later], role="test")

    refused = await _check({"train": await registry.version("corpus", [SHARED]), **_VALID})
    assert refused.ok is False and refused.overlaps[0].row_ids == ["shared-row"]
    assert refused.rows_excluded_by_cutoff == 1

    excluded = await _check({"train": await registry.version("corpus-b", [later]), **_VALID})
    assert excluded.ok is True and excluded.rows_excluded_by_cutoff == 1


@pytest.mark.parametrize("bad_date", ["not a date", "2026-13-01", 5, ["2026-06-01"], {"on": "2026-06-01"}], ids=str)
async def test_a_row_that_cannot_be_dated_stays_in_the_comparison_and_is_warned_about(registry, bad_date):
    row = {**_UNDATABLE, "dated_at": bad_date}
    held = await registry.version("eval-set", [row], role="test")

    refused = await _check({"train": await registry.version("corpus", [row]), **_VALID})
    assert refused.ok is False and refused.checked is True
    assert refused.overlaps[0].holdout == held and refused.overlaps[0].row_ids == ["undatable"]
    assert refused.rows_excluded_by_cutoff == 0

    clean = await _check({"train": await registry.version("corpus-clean", TRAIN), **_VALID})
    assert clean.ok is True and clean.checked is True
    (warning,) = _warned_about(clean, held)
    assert "1 row(s)" in warning and "kept in the comparison" in warning and "2026-06-01" in warning


async def test_the_warning_names_the_version_and_not_the_rows_content(registry):
    rows = [{**_UNDATABLE, "dated_at": "some spring"}, {**_UNDATABLE, "id": "undatable-2", "dated_at": "some autumn"}]
    held = await registry.version("eval-set", rows, role="test")
    verdict = await _check({"train": await registry.version("corpus", TRAIN), **_VALID})
    (warning,) = _warned_about(verdict, held)
    assert "2 row(s)" in warning
    for content in ("some spring", "some autumn", "undatable", "the words of this row"):
        assert content not in warning


async def test_a_bad_row_date_means_nothing_with_no_cutoff(registry):
    held = await registry.version("eval-set", [_UNDATABLE], role="test")
    verdict = await _check({"train": await registry.version("corpus", TRAIN)})
    assert verdict.ok is True and verdict.checked is True and _warned_about(verdict, held) == []


# ── the record keeps what the door found ────────────────────────────────────


async def test_a_launch_record_keeps_the_bad_row_date_warning(registry):
    held = await registry.version("eval-set", [_UNDATABLE], role="test")
    clean = await registry.version("corpus", TRAIN)
    bridge = _Bridge()
    await launch_pipeline_def(parse_pipeline_def(_PIPELINE, pack="finetune"),
                              inputs={"train_version": clean, **_VALID}, bridge=bridge, workspace_id=WS)
    (call,) = bridge.calls
    assert call["admission"]["verdict"] == ADMITTED
    assert any(held in w for w in call["admission"]["holdout"]["warnings"])


async def test_a_run_record_keeps_the_bad_row_date_warning_and_the_hashes(registry, monkeypatch):
    held = await registry.version("eval-set", [_UNDATABLE], role="test")
    clean = await registry.version("corpus", TRAIN)
    d = _dispatcher(monkeypatch)
    run = await d.run_template("demo/train", {"train": clean, **_VALID}, bundle_pipeline_id="p-1", step_id="train")
    await asyncio.gather(*list(d._tasks.values()), return_exceptions=True)
    stored = await d.store.get(run.id)
    assert stored.holdout["checked"] is True and any(held in w for w in stored.holdout["warnings"])
    (record,) = stored.holdout["eval_versions"]
    assert record["ref"] == held and record["content_hash"]


async def test_a_run_the_door_could_not_check_keeps_why_on_its_record(registry, monkeypatch):
    """The run still starts: what the door does when it can't compare is not decided here. What it
    found no longer disappears."""
    await registry.version("eval-set", HOLDOUT, role="test")
    dirty = await registry.version("corpus", [SHARED])
    monkeypatch.setattr("cairn.core.storage.artifacts.build_artifact_store",
                        lambda *a, **k: _Failing(registry.store, "list_dataset_versions_by_role"))
    d = _dispatcher(monkeypatch)
    run = await d.run_template("demo/train", {"train": dirty}, bundle_pipeline_id="p-1", step_id="train")
    await asyncio.gather(*list(d._tasks.values()), return_exceptions=True)
    stored = await d.store.get(run.id)
    assert stored.holdout["ok"] is True and stored.holdout["checked"] is False
    assert any("holdout NOT checked" in w for w in stored.holdout["warnings"])


async def test_a_run_the_door_had_nothing_to_say_about_stamps_nothing(registry, monkeypatch):
    d = _dispatcher(monkeypatch)
    run = await d.run_template("demo/train", {"path": "/data/train.jsonl"}, bundle_pipeline_id="p-1", step_id="train")
    await asyncio.gather(*list(d._tasks.values()), return_exceptions=True)
    assert (await d.store.get(run.id)).holdout == {}


# ── preflight and the door on an eval cutoff ───────────────────────────────


async def _dated_overlap(registry):
    """A holdout dated 2026-06-02 that shares a row with the training version: a cutoff of
    2026-06-01 excludes it, and a cutoff of 2026-06-03 keeps it."""
    await registry.version("eval-later", HOLDOUT, role="test", dated_at="2026-06-02")
    return await registry.version("corpus", [SHARED])


def _overlap_blockers(verdict):
    return [b.what for b in verdict.blockers if b.kind == "holdout-overlap"]


async def test_preflight_agrees_with_the_door_on_a_cutoff(registry, monkeypatch):
    dirty = await _dated_overlap(registry)
    values = {"train": dirty, "eval_cutoff": "2026-06-01"}
    assert (await _check(values)).ok is True
    assert (await decide_admission(values, workspace_id=WS, policies=[])).verdict == ADMITTED
    verdict = await _preflight(monkeypatch, values)
    assert _overlap_blockers(verdict) == [] and verdict.ready is True


async def test_preflight_and_the_door_both_refuse_when_the_cutoff_keeps_the_holdout(registry, monkeypatch):
    dirty = await _dated_overlap(registry)
    values = {"train": dirty, "eval_cutoff": "2026-06-03"}
    door = await _check(values)
    assert door.ok is False
    assert (await decide_admission(values, workspace_id=WS, policies=[])).verdict == REFUSED
    verdict = await _preflight(monkeypatch, values)
    assert _overlap_blockers(verdict) == door.reasons() and verdict.ready is False


async def test_preflight_finds_a_nested_cutoff_where_the_door_does(registry, monkeypatch):
    dirty = await _dated_overlap(registry)
    values = {"train": dirty, "params": {"eval_cutoff": "2026-06-01"}}
    assert (await _check(values)).ok is True
    assert _overlap_blockers(await _preflight(monkeypatch, values)) == []


@pytest.mark.parametrize("typed_cutoff, raw_cutoff, ok", [
    ("2026-06-01", "2026-06-03", True),   # the typed cutoff excludes the holdout
    ("2026-06-03", "2026-06-01", False),  # the typed cutoff keeps it, whatever raw says
    (None, "2026-06-01", True),           # coercion dropped the field: raw fills it in
])
async def test_the_typed_cutoff_wins_because_it_is_what_the_run_door_gets(registry, typed_cutoff, raw_cutoff, ok):
    from cairn.core.preflight import _holdout_verdict

    dirty = await _dated_overlap(registry)
    typed = {"train": dirty} | ({"eval_cutoff": typed_cutoff} if typed_cutoff else {})
    verdict = await _holdout_verdict(typed, {"train": dirty, "eval_cutoff": raw_cutoff})
    assert verdict.ok is ok
    assert verdict.eval_cutoff == (typed_cutoff or raw_cutoff)


async def test_a_default_does_not_shadow_the_ref_the_user_typed(registry):
    from cairn.core.preflight import _holdout_verdict

    await registry.version("eval-set", HOLDOUT, role="test")
    default = await registry.version("clean", TRAIN)
    typed_ref = await registry.version("corpus", [SHARED])
    values = {"train": default, "eval_cutoff": "2026-06-01"}
    verdict = await _holdout_verdict(values, {**values, "train": typed_ref})
    assert verdict.ok is False and {o.training for o in verdict.overlaps} == {typed_ref}


async def test_preflight_blocks_a_malformed_top_level_cutoff_as_the_door_refuses_it(registry, monkeypatch):
    clean = await registry.version("corpus", TRAIN)
    values = {"train": clean, "eval_cutoff": "last tuesday"}
    door = await _check(values)
    assert door.ok is False
    verdict = await _preflight(monkeypatch, values)
    assert verdict.ready is False
    assert [b.what for b in verdict.blockers if b.kind == "holdout-invalid-cutoff"] == [door.refusal]
    assert _overlap_blockers(verdict) == []


@pytest.mark.parametrize("typed_cutoff, raw_cutoff, refused_too", [
    ("last tuesday", "2026-06-01", True),   # a bad typed cutoff isn't rescued by raw
    (0, "2026-06-01", True),                # nor is a falsy one, which isn't "no cutoff"
    ("2026-06-01", "last tuesday", False),  # the run door gets the typed one, and it's fine
])
async def test_the_typed_cutoff_wins_when_one_is_malformed(registry, typed_cutoff, raw_cutoff, refused_too):
    from cairn.core.preflight import _holdout_verdict

    clean = await registry.version("corpus", TRAIN)
    verdict = await _holdout_verdict({"train": clean, "eval_cutoff": typed_cutoff},
                                     {"train": clean, "eval_cutoff": raw_cutoff})
    assert bool(verdict.refusal) is refused_too
    assert (await _check({"train": clean, "eval_cutoff": typed_cutoff})).refusal == verdict.refusal


# ── golden vectors: the door's definitions, pinned ──────────────────────────
# The dataset operators ship in cairn-extensions, so a direct comparison would always skip here.
# These literals were checked once against that pack's ``_canon.canonical_hash`` and
# ``_ngram.shingles``; the same vectors can be asserted on that side.


@pytest.mark.parametrize("row, expected", [
    ({"prompt": "What is the capital of France?", "completion": "Paris"},
     "3c9d879748552ee46963a15742912c647e90ea1cd6c73ac2efbf7dac9e266958"),
    ({"completion": "Paris", "prompt": "What   is the capital of France?"},
     "3c9d879748552ee46963a15742912c647e90ea1cd6c73ac2efbf7dac9e266958"),
    ({"completion": "Paris", "prompt": "What   is the capital\tof France?"},
     "699e58fbc9e51cd2028f3c691c4fb5b983ae7f830d4356af3ddfde8364673f82"),
    ({"b": 1, "a": ["x", {"c": "é"}], "d": None, "e": True, "f": 1.5},
     "6cbefa9eb45090c1a8f2ca4147232d8d1c7ee9902187b204fa356dfedc08427b"),
    ({"text": "line one\nline two"},
     "2e2fb76d98fa2a1ad1d46d9298d69ad3032d55a1105c60f21f74852b4e128fdc"),
    ({}, "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"),
])
def test_the_door_hashes_a_row_exactly_as_the_pack_does(row, expected):
    assert canonical_row_hash(row) == expected


@pytest.mark.parametrize("row, n, expected", [
    ({"text": "One two Three four five six"}, 5,
     {("one", "two", "three", "four", "five"), ("two", "three", "four", "five", "six")}),
    ({"text": "too short"}, 5, {("too", "short")}),
    ({"prompt": "alpha beta", "messages": [{"role": "user", "content": "Gamma"}], "n": 7}, 2,
     {("alpha", "beta"), ("beta", "user"), ("user", "gamma")}),
    ({"n": 7}, 5, set()),
])
def test_the_door_shingles_a_row_exactly_as_the_pack_does(row, n, expected):
    assert row_shingles(row, n) == expected


# The door's known gaps, as what its own text promises. Each fails today, on purpose: strict, so a
# fix has to remove the marker. What it should do when it can't compare at all is #399.

@pytest.mark.xfail(strict=True, reason="#398 a capped read isn't reported")
async def test_a_version_past_the_row_cap_is_reported_as_truncated(registry, monkeypatch):
    monkeypatch.setattr(holdout_module, "MAX_ROWS", 2)
    await registry.version("eval-set", HOLDOUT, role="test")
    verdict = await _check({"train": await registry.version("corpus", [*TRAIN, SHARED])})
    assert not verdict.ok or any("truncated" in w for w in verdict.warnings)


@pytest.mark.xfail(strict=True, reason="#398 data the door didn't read scores clean")
async def test_a_directory_with_no_jsonl_rows_is_not_scored_clean(registry, tmp_path):
    shards = tmp_path / "shards"
    shards.mkdir()
    (shards / "part-0.parquet").write_bytes(b"PAR1 not read by the door")
    digest = hashlib.sha256(b"dir").hexdigest()
    await registry.store.register(ArtifactDescriptor(uri=f"file://{shards}", kind="dataset", content_hash=digest),
                                  producer_run_id="prep", workspace_id=WS)
    await registry.store.register_dataset_version("eval-dir", digest, actor="prep", role="test", workspace_id=WS)
    verdict = await _check({"train": await registry.version("corpus", [SHARED])}, near_dup=False)
    assert verdict.warnings
