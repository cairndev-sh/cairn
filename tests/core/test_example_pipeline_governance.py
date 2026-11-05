"""The governance a pipeline's composition provides, on the example pack (#2424; the
four pipelines of hello-cairn WP6, ADR-0127 D1).

``test_batch_inference_pipeline.py`` pins these properties on the serving pack's
batch-inference pipeline, which is the platform's; this is the same check on
``examples/hello-cairn`` so the public set proves it. What is asserted is not YAML
syntax (the loader covers that) but the ordering and declarations a well-meaning edit
could silently remove: nothing downstream runs before a data gate, nothing is published
or tagged before a person, every run is attributable and bounded, the declared triggers
parse with the same grammar the engine arms them with, each agentic flow compiles, and
the pack's run-hook listener resolves from its entry point.

``tiny-finetune`` needs the builtin train-local pack (``train.lora`` stage,
``train/eval-loss``, hello-cairn WP3), which the public core installs.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import tomllib

import pytest
import yaml

from cairn.core.dataflow.defs import FlowDef
from cairn.core.engine.pipeline import load_pipeline_def, plan_pipeline
from cairn.core.engine.triggers_grammar import (
    ScheduleTrigger, WatchTrigger, WebhookTrigger, parse_declared_triggers,
)
from tests import _layout

_PACK = _layout.REPO / "examples" / "hello-cairn"
_PIPELINES = _PACK / "pipelines"
_FLOWS = _PACK / "flows"
_PROFILE = _PACK / "profiles" / "hello.toml"
PIPELINES = ("profile-and-publish", "tiny-finetune")
# Stops at its budget before any gate, on its own series; test_hello_cairn.py proves the refusal.
OVER_BUDGET = "over-budget"


def _pipeline(name: str = "profile-and-publish"):
    return load_pipeline_def(_PIPELINES / f"{name}.yaml")


def _step(name: str, pipeline: str = "profile-and-publish"):
    for s in _pipeline(pipeline).steps:
        if (s.id or s.gate) == name:
            return s
    raise AssertionError(f"no step {name!r} in {pipeline}")


def _flow(name: str) -> FlowDef:
    return FlowDef.model_validate(yaml.safe_load((_FLOWS / f"{name}.yaml").read_text(encoding="utf-8")))


@pytest.fixture
def example_loaded(monkeypatch):
    """The example as a dev pack, under its own profile, in a fresh registry."""
    from cairn.core import pack as pack_api
    from cairn.core.pack import loader

    monkeypatch.setenv(loader.DEV_PACK_PATH_ENV, str(_PACK))
    monkeypatch.setenv("CAIRN_PROFILE", str(_PROFILE))
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-dummy")
    pack_api.clear()
    loader.load_all_packs(strict=False)
    yield
    pack_api.clear()
    monkeypatch.delenv(loader.DEV_PACK_PATH_ENV)
    loader.load_all_packs(strict=False)


# ── every pipeline ──────────────────────────────────────────────────────────

def test_the_example_ships_its_pipelines():
    assert sorted(p.stem for p in _PIPELINES.glob("*.yaml")) == sorted((*PIPELINES, OVER_BUDGET))


def test_the_over_budget_pipeline_declares_an_estimate_above_its_cap():
    gov = _pipeline(OVER_BUDGET).governance
    assert gov["estimated_usd"] > gov["budget"]["usd"] and gov["series"]


@pytest.mark.parametrize("name", PIPELINES)
def test_each_pipeline_loads_and_plans(name):
    """A pipeline that does not plan admits, spends, then dies (ADR-0092 §1)."""
    plan_pipeline(_pipeline(name))


@pytest.mark.parametrize("name", PIPELINES)
def test_each_pipeline_declares_a_budget_series_and_approver(name):
    gov = _pipeline(name).governance
    assert gov["series"] == "hello"
    assert gov["budget"]["usd"] > 0
    assert gov["approvers"]


@pytest.mark.parametrize("name", PIPELINES)
def test_each_pipeline_stops_for_a_person(name):
    """Every pipeline stops for a person somewhere: a gate with approvers, or a task leg
    whose operator declares requires_approval (promote / propose)."""
    steps = _pipeline(name).steps
    person_gates = [s for s in steps if s.gate and s.approvers]
    operator_gates = [s for s in steps if s.task in ("artifact/promote", "monitor/propose")]
    assert person_gates or operator_gates


@pytest.mark.parametrize("name", ("profile-and-publish",))
def test_each_pipeline_is_runnable_on_the_public_core(name, example_loaded):
    from cairn.core.engine.pipeline_resolve import assert_runnable

    assert_runnable(_pipeline(name))


def test_tiny_finetune_is_runnable_on_the_public_core(example_loaded):
    """`train.lora` and `monitor/threshold` come from builtin packs (train-local since WP3), base
    dependencies of the public core."""
    from cairn.core.engine.pipeline_resolve import assert_runnable

    assert_runnable(_pipeline("tiny-finetune"))


# ── profile-and-publish (data) ──────────────────────────────────────────────

def test_the_flows_load_with_their_stages():
    assert [s.stage for s in _flow("engine-check").stages] == ["noop"]
    assert _flow("engine-check").kind == "data"
    assert [s.stage for s in _flow("summarise").stages] == ["noop", "noop.slow"]
    lora = _flow("lora")
    assert lora.kind == "ml" and [s.stage for s in lora.stages] == ["train.lora"]


def test_the_data_gate_stands_between_the_profile_and_everything_after_it():
    """Ordering IS the governance: a leg that stops depending on the gate runs before it."""
    assert _step("data-ok").gate == "data-ok"
    assert _step("data-ok").depends_on == ["profile"]
    assert _step("redact").depends_on == ["data-ok"]
    assert _step("engine-check").depends_on == ["redact"]
    assert _step("stage").depends_on == ["engine-check"]


def test_the_data_gate_compares_the_profile_against_a_declared_threshold():
    predicate = _step("data-ok").predicate
    assert "steps.profile.verdict" in predicate and "input.min_rows" in predicate


def test_only_redacted_text_is_stored():
    assert _step("redact").task == "redact/text"
    assert _step("stage").inputs["content"] == "$steps.redact.text"
    assert _step("publish").inputs["content"] == "$steps.redact.text"


def test_nothing_is_tagged_or_published_before_the_person_gate():
    assert _step("review").approvers and _step("review").depends_on == ["stage"]
    assert _step("promote").task == "artifact/promote" and _step("promote").depends_on == ["review"]
    assert _step("publish").depends_on == ["promote"]
    assert _step("audit").task == "audit/store" and _step("audit").depends_on == ["publish"]


def test_the_published_artifact_and_its_audit_are_the_pipeline_outputs():
    outputs = _pipeline().outputs
    assert outputs["published"] == "publish.uri"
    assert outputs["promoted"] == "promote.artifact_id"
    assert outputs["audit_hash"] == "audit.hash"


def test_the_data_pipeline_declares_watch_schedule_and_webhook_none_armed():
    decls = parse_declared_triggers(_pipeline().triggers)
    assert [type(d) for d in decls] == [WatchTrigger, ScheduleTrigger, WebhookTrigger]
    watch, schedule, webhook = decls
    assert watch.is_source_watch and watch.source.kind == "dataset" and watch.on_change == "start"
    assert schedule.cron == "0 6 * * 1"
    assert webhook.source == "custom" and webhook.path == "/triggers/hello-sample"
    assert not any(d.enabled for d in decls)   # declared, never armed by a fresh install


def _materialised_targets(name: str) -> list[tuple[str, str]]:
    """``(row id, what it starts)`` for every row the pipeline's triggers materialise
    into — the cron store's ``workflow`` and the webhook receiver's ``run.template``."""
    from cairn.core.engine.triggers_grammar import materialise_declared_triggers
    from cairn.core.storage.schedule_store import InMemoryScheduleStore
    from cairn.core.storage.trigger_store import InMemoryTriggerStore

    schedules, hooks = InMemoryScheduleStore(), InMemoryTriggerStore()
    asyncio.run(materialise_declared_triggers(
        f"pipeline:hello-cairn/{name}", parse_declared_triggers(_pipeline(name).triggers),
        schedule_store=schedules, trigger_store=hooks))
    rows = [(s.id, s.workflow) for s in asyncio.run(schedules.list())]
    rows += [(t.id, yaml.safe_load(t.yaml)["run"]["template"]) for t in asyncio.run(hooks.list())]
    return rows


def _in_example(target: str) -> bool:
    return target.removeprefix("pipeline:").split("/", 1)[0] == "hello-cairn"


@pytest.mark.parametrize("name", PIPELINES)
def test_no_schedule_or_webhook_starts_anything_outside_the_example(name):
    """The example runs on the public core alone: a trigger that starts a platform pipeline
    reads as wired and never fires there."""
    rows = [r for r in _materialised_targets(name) if "--watch--" not in r[0]]
    assert all(_in_example(target) for _, target in rows), rows


def test_the_dataset_watch_starts_its_own_pipeline():
    """ADR-0131 D3 — profile-and-publish's `dataset` watch arms a row that starts
    profile-and-publish itself (core probes the dataset first), not a platform pipeline."""
    rows = [r for r in _materialised_targets("profile-and-publish") if "--watch--" in r[0]]
    assert rows and all(target == "pipeline:hello-cairn/profile-and-publish" for _, target in rows), rows




@pytest.mark.parametrize("env, gated", [("hello-prod", True), ("sandbox", False),
                                        ("production", False), ("dev", False)])
def test_the_pack_policy_gates_storage_writes_in_hello_prod_only(env, gated):
    """A pack policy is adopted by every environment that does not list its own: scoped to
    the `hello-prod` NAME and the storage/put operator, it must not touch `sandbox` (which
    resolves to the production tier), `production` or `dev`, nor any other operator."""
    from cairn.core.engine.schema import ActionCandidate, ActionRequest, PolicySpec
    from cairn.core.policy.engine import policy_applies

    spec = PolicySpec.model_validate(yaml.safe_load(
        (_PACK / "policies" / "require-review-on-publish.yaml").read_text(encoding="utf-8")))

    def _applies(op: str) -> bool:
        return policy_applies(spec, ActionRequest(
            candidate=ActionCandidate(id="x", type=op), environment=env))

    assert _applies("storage/put") is gated
    assert not _applies("data/profile") and not _applies("artifact/promote")
    assert spec.rules[0].effect == "require_hitl"


def test_the_quickstart_profile_turns_the_audit_trail_on_and_the_default_stays_off():
    """The Quickstart runs under hello.toml so a first run's log shows its gate decisions;
    local-dev leaves `audit.sink` unbound (ADR-0028)."""
    import cairn.profiles

    hello = tomllib.loads((_PACK / "profiles" / "hello.toml").read_text())
    assert hello["profile"]["bindings"]["audit.sink"] == "audit/store"
    local_dev = tomllib.loads(cairn.profiles.local_dev_path().read_text())
    assert "audit.sink" not in local_dev["profile"]["bindings"]


@pytest.mark.parametrize("profile", ("hello.toml",))
def test_the_example_profiles_pin_no_runtime(profile):
    """A profile's `durable_runtime` beats CAIRN_RUNTIME (cairn/core/settings.py runtime()),
    so a pin here would make `CAIRN_RUNTIME=dbos` a no-op for the example."""
    from cairn.core.settings import profile_runtime

    path = _PACK / "profiles" / profile
    assert profile_runtime(str(path)) is None
    assert "durable_runtime" not in tomllib.loads(path.read_text()).get("profile", {}).get("backends", {})


# ── ask-the-docs (agentic) ──────────────────────────────────────────────────









# ── nightly-ops (flow + hooks) ──────────────────────────────────────────────







def test_the_run_hook_entry_point_resolves_and_writes_receipts(tmp_path, monkeypatch):
    eps = tomllib.loads((_PACK / "pyproject.toml").read_text())["project"]["entry-points"]
    assert eps["cairn.run_hooks"] == {"hello-receipts": "cairn_pack_hello.hooks:receipts"}
    module, _, attr = eps["cairn.run_hooks"]["hello-receipts"].partition(":")
    assert module == "cairn_pack_hello.hooks"           # the wheel's import name for hooks.py
    inc = tomllib.loads((_PACK / "pyproject.toml").read_text())["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]
    assert inc["hooks.py"] == "cairn_pack_hello/hooks.py"
    spec = importlib.util.spec_from_file_location(module, _PACK / "hooks.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    hook = getattr(mod, attr)

    from cairn.core.pack.run_hooks import EVENTS
    assert set(mod.EVENTS) <= set(EVENTS)               # it listens only to real events

    monkeypatch.setenv("CAIRN_HOME", str(tmp_path))
    asyncio.run(hook("leg.finished", {"event": "leg.finished", "pipeline_id": "pl_1", "step_id": "job",
                                      "run_id": "r1", "status": "done"}))
    asyncio.run(hook("run.finished", {"event": "run.finished", "run_id": "r1", "status": "done",
                                      "store": object()}))
    asyncio.run(hook("run.launched", {"event": "run.launched", "run_id": "r1"}))   # not a receipt
    lines = [json.loads(x) for x in (tmp_path / "hello-cairn" / "receipts.jsonl").read_text().splitlines()]
    assert [(r["event"], r["run_id"]) for r in lines] == [("leg.finished", "r1"), ("run.finished", "r1")]
    assert "store" not in lines[1]                      # objects never reach the receipt


# ── tiny-finetune (ml) ──────────────────────────────────────────────────────

def test_the_finetune_trains_after_the_data_gate_and_tags_after_the_loss_gate():
    assert _step("data-ok", "tiny-finetune").depends_on == ["profile"]
    assert _step("train", "tiny-finetune").flow == "hello-cairn/lora"
    assert _step("train", "tiny-finetune").depends_on == ["data-ok"]
    assert [s for s in _pipeline("tiny-finetune").steps if s.id == "eval"] == []   # #252: no leg scores training rows
    assert _step("loss-ok", "tiny-finetune").depends_on == ["train"]
    assert "steps.train.findings.train_lora_eval_loss" in _step("loss-ok", "tiny-finetune").predicate
    assert _step("promote", "tiny-finetune").depends_on == ["loss-ok"]
    review = _step("review", "tiny-finetune")
    assert review.approvers and review.depends_on == ["promote"]
    assert _step("card", "tiny-finetune").depends_on == ["review"]


def test_profile_and_publish_emits_dataset_ready_when_it_publishes():
    """#2605 — the declared grammar carries `emits:` onto the plan, so the bridge
    publishes `dataset.ready` when `publish` completes (ADR-0127 D1)."""
    plan = {s.id: s for s in plan_pipeline(_pipeline()).steps}
    assert plan["publish"].emits == "dataset.ready"
    assert all(s.emits is None for sid, s in plan.items() if sid != "publish")




def test_the_finetune_legs_read_the_adapter_and_loss_the_train_flow_reports():
    """The proof (all eight cells) found a leg failing on `$steps.train.adapter_path` —
    the ml flow leg's stats are namespaced by stage, and nothing mapped them. The leg's
    declared product, the loss gate's wire and the review gate's `show:` now resolve against exactly the stats
    the runner reports for `train.lora` (`runner.py`: `stats[f"{stage}.{k}"]`)."""
    from types import SimpleNamespace

    from cairn.core.dispatch.pipeline_bridge import _flow_leg_output
    from cairn.core.engine.expr import step_field
    from cairn.core.engine.pipeline_gate import resolve_show

    plan = {s.id: s for s in plan_pipeline(_pipeline("tiny-finetune")).steps}
    stats = {"train.lora.adapter_path": "/w/adapter", "train.lora.output": "/w/adapter",
             "train.lora.train_loss": 3.2, "train.lora.eval_loss": 3.4,
             "candidate_artifact_id": "sha256:abc"}
    out = _flow_leg_output(plan["train"], SimpleNamespace(ok=True, stats=stats))
    assert step_field(out, "adapter_path") == "/w/adapter"              # outputs.adapter
    assert step_field(out, "candidate_artifact_id") == "sha256:abc"     # promote's wire
    shown = resolve_show(["train.train_lora_train_loss", "train.train_lora_eval_loss"], {"train": out})
    assert shown == {"train.train_lora_train_loss": 3.2, "train.train_lora_eval_loss": 3.4}   # the gate's figure
