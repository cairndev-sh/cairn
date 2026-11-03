"""train-local on a BASE install (no torch): the pack registers, its manifest agrees with its
classes, the mock paths are deterministic and conform, model selection layers through the
operator config, and both operations refuse loudly without the ``[train]`` extra."""
from __future__ import annotations

import asyncio
import builtins
import sys
import tomllib
from pathlib import Path

import pytest

from cairn.core.operator.testing import run_operator_for_test

from operator_train_local import finetune as F
from operator_train_local.operators.eval_loss import EvalLossOp
from operator_train_local.operators.lora import TrainLoraOp
from operator_train_local.schemas.train import DEFAULT_BASE_MODEL, TrainLoraOutput

PACK = Path(__file__).resolve().parents[1]
ROWS = str(Path(__file__).parent / "fixtures" / "tiny.jsonl")
MANIFEST = tomllib.loads((PACK / "pack.toml").read_text(encoding="utf-8"))


def test_the_manifest_declares_both_operations_as_the_classes_do():
    ops = MANIFEST["operator"]
    assert sorted(ops) == ["train/eval-loss", "train/lora"]
    assert MANIFEST["pack"]["trust_level"] == "builtin"
    assert TrainLoraOp.implements == ("contract://train.finetune/1.1.0",)
    assert ops["train/lora"]["implements"] == list(TrainLoraOp.implements)
    assert ops["train/lora"]["capabilities"]["provides"] == ["train.finetune"]
    assert ops["train/lora"]["risk"] == TrainLoraOp.risk == "external_write"
    assert ops["train/eval-loss"]["risk"] == EvalLossOp.risk == "read_only"
    assert EvalLossOp.implements == ("contract://train.eval_loss/1.1.0",)
    assert ops["train/eval-loss"]["implements"] == list(EvalLossOp.implements)


def test_the_stage_is_registered_with_its_risk():
    pyproject = tomllib.loads((PACK / "pyproject.toml").read_text(encoding="utf-8"))
    eps = pyproject["project"]["entry-points"]["cairn.dataflow_stages"]
    assert eps == {"train.lora": "operator_train_local.stages:train_lora"}
    from operator_train_local.stages import train_lora

    assert train_lora.risk_class == "external_write"


def test_the_mock_is_deterministic_and_conforms():
    a = asyncio.run(run_operator_for_test(TrainLoraOp, {"dataset": ROWS}, mocked=True))
    b = asyncio.run(run_operator_for_test(TrainLoraOp, {"dataset": ROWS}, mocked=True))
    assert a == b and isinstance(a, TrainLoraOutput)
    assert a.base_model == DEFAULT_BASE_MODEL and a.steps_run == 8 and a.tokens_seen > 0
    assert a.adapter_path == "mock://train-local/test-run/test-node" and a.eval_loss is not None
    pinned = asyncio.run(run_operator_for_test(TrainLoraOp, {"dataset": ROWS, "output_dir": "/srv/adapters/x"},
                                               mocked=True))
    assert pinned.adapter_path.startswith("mock://"), "a mock never returns a real-looking adapter path"
    out = asyncio.run(run_operator_for_test(EvalLossOp, {"adapter_path": a.adapter_path, "dataset": ROWS},
                                            mocked=True))
    assert out.eval_loss > 0 and out.tokens_seen > 0


def test_the_base_model_layers_run_over_profile_over_default():
    profile = {"base_model": "sshleifer/tiny-gpt2"}
    pinned = asyncio.run(run_operator_for_test(TrainLoraOp, {"dataset": ROWS}, config=profile, mocked=True))
    assert pinned.base_model == "sshleifer/tiny-gpt2"
    bound = asyncio.run(run_operator_for_test(TrainLoraOp, {"dataset": ROWS, "base_model": "my/model"},
                                              config=profile, mocked=True))
    assert bound.base_model == "my/model"


@pytest.fixture
def no_train_stack(monkeypatch):
    """The base install: torch, transformers and peft are not importable."""
    real = builtins.__import__

    def _import(name, *args, **kwargs):
        if name.split(".")[0] in ("torch", "transformers", "peft", "datasets"):
            raise ImportError(f"No module named {name!r}", name=name.split(".")[0])
        return real(name, *args, **kwargs)
    for mod in [m for m in sys.modules if m.split(".")[0] in ("torch", "transformers", "peft", "datasets")]:
        monkeypatch.delitem(sys.modules, mod)
    monkeypatch.setattr(builtins, "__import__", _import)


@pytest.mark.usefixtures("no_train_stack")
@pytest.mark.parametrize("op,inp", [(TrainLoraOp, {"dataset": ROWS}),
                                    (EvalLossOp, {"adapter_path": "/nowhere", "dataset": ROWS})])
def test_without_the_extra_both_operations_refuse_naming_it(op, inp):
    with pytest.raises(F.TrainStackMissing, match=r"install cairndev\[train\]"):
        asyncio.run(run_operator_for_test(op, inp))


@pytest.mark.usefixtures("no_train_stack")
def test_without_the_extra_the_stage_refuses_and_its_mock_still_runs(tmp_path):
    from operator_train_local.stages import train_lora

    with pytest.raises(F.TrainStackMissing, match=r"cairndev\[train\]"):
        train_lora(tmp_path, {"dataset": ROWS, "steps": 1}, [])
    stats = train_lora(tmp_path, {"dataset": ROWS, "mock": True, "output_dir": str(tmp_path / "a")}, [])
    assert TrainLoraOutput(**{k: stats[k] for k in TrainLoraOutput.model_fields})
    assert stats["mock"] is True and stats["cost_usd"] == 0.0 and stats["adapter_path"].startswith("mock://")
    assert not {"kind", "output", "content_hash"} & set(stats), "a mock describes no model artifact"


def test_the_stage_takes_its_dataset_from_the_flow_input_and_refuses_unknown_keys(tmp_path):
    from operator_train_local.stages import train_lora

    stats = train_lora(tmp_path, {"mock": True, "steps": 3}, [f"file://{ROWS}"])
    assert stats["steps_run"] == 3
    with pytest.raises(ValueError, match=r"unknown config keys \['step'\]"):
        train_lora(tmp_path, {"dataset": ROWS, "step": 3, "mock": True}, [])


def test_rows_load_and_one_in_eight_is_held_out(tmp_path):
    texts = F.load_texts(ROWS, 64)
    assert len(texts) == 8 and F.split_holdout(texts) == (texts[:7], texts[7:])
    assert F.split_holdout(texts[:7]) == (texts[:7], [])
    assert F.load_texts(f"file://{ROWS}", 3) == texts[:3]
    bad = tmp_path / "bad.jsonl"
    bad.write_text('{"text": "ok"}\n{"prompt": "no text"}\n', encoding="utf-8")
    with pytest.raises(ValueError, match=r"bad.jsonl:2: a row must be an object with a string 'text' field"):
        F.load_texts(str(bad), 64)
    with pytest.raises(FileNotFoundError, match="no such file"):
        F.load_texts(str(tmp_path / "missing.jsonl"), 64)


def test_the_default_output_dir_is_per_run_and_never_shared():
    from types import SimpleNamespace

    from operator_train_local.operators.lora import default_output_dir

    pinned = default_output_dir(SimpleNamespace(pipeline_id="p-1", node_name="train"))
    assert pinned == default_output_dir(SimpleNamespace(pipeline_id="p-1", node_name="train"))
    a, b = (default_output_dir(SimpleNamespace(pipeline_id=None, node_name=None)) for _ in range(2))
    assert a != b and "/run/" not in a, (a, b)


def test_the_adapter_digest_covers_only_the_files_named(tmp_path):
    (tmp_path / "adapter_model.safetensors").write_bytes(b"weights")
    (tmp_path / "adapter_config.json").write_text("{}")
    names = ["adapter_config.json", "adapter_model.safetensors"]
    before = F.adapter_digest(str(tmp_path), names)
    (tmp_path / "stray.bin").write_bytes(b"left by someone else")
    assert F.adapter_digest(str(tmp_path), names) == before and before[1] == len(b"weights") + 2
