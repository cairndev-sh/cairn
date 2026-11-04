"""Device and precision selection (``finetune.resolve_device`` / ``resolve_dtype``): ``auto`` on a
machine without CUDA is the fp32 CPU run train-local always did; on CUDA it is bf16 or fp16 by
GPU; an unavailable device or a half precision off CUDA is refused, never silently downgraded.
CUDA is faked with monkeypatch, so these run on any machine; the last test trains for real and
runs only where a GPU is present (Colab)."""
from __future__ import annotations

import math

import pytest

torch = pytest.importorskip("torch")
pytest.importorskip("transformers")
pytest.importorskip("peft")

from operator_train_local import finetune as F  # noqa: E402

from _train_local_tiny import ROWS, build_tiny_model  # noqa: E402


def _fake_cuda(monkeypatch, *, available: bool, bf16: bool = False) -> None:
    monkeypatch.setattr(torch.cuda, "is_available", lambda: available)
    monkeypatch.setattr(torch.cuda, "get_device_capability", lambda *a, **k: (8, 0) if bf16 else (7, 5))


def test_auto_is_the_cpu_without_cuda_and_never_mps(monkeypatch):
    _fake_cuda(monkeypatch, available=False)
    monkeypatch.setattr(torch.backends.mps, "is_available", lambda: True)
    assert F.resolve_device("auto").type == "cpu"


def test_auto_is_cuda_when_cuda_is_there(monkeypatch):
    _fake_cuda(monkeypatch, available=True)
    assert F.resolve_device("auto").type == "cuda"


@pytest.mark.parametrize("device", ["cuda", "mps"])
def test_an_unavailable_device_is_refused(monkeypatch, device):
    _fake_cuda(monkeypatch, available=False)
    monkeypatch.setattr(torch.backends.mps, "is_available", lambda: False)
    with pytest.raises(RuntimeError, match=device):
        F.resolve_device(device)


def test_an_unknown_device_is_refused():
    with pytest.raises(ValueError, match="tpu"):
        F.resolve_device("tpu")


def test_auto_precision_is_fp32_on_the_cpu():
    assert F.resolve_dtype("auto", torch.device("cpu")) == (torch.float32, "fp32")


@pytest.mark.parametrize(("bf16", "expected"), [(True, (torch.bfloat16, "bf16")), (False, (torch.float16, "fp16"))])
def test_auto_precision_on_cuda_follows_the_gpu(monkeypatch, bf16, expected):
    _fake_cuda(monkeypatch, available=True, bf16=bf16)
    assert F.resolve_dtype("auto", torch.device("cuda")) == expected


@pytest.mark.parametrize("precision", ["fp16", "bf16"])
def test_half_precision_off_cuda_is_refused(precision):
    with pytest.raises(ValueError, match="CUDA"):
        F.resolve_dtype(precision, torch.device("cpu"))


@pytest.mark.train
def test_a_cpu_run_reports_where_it_ran(tmp_path):
    out = F.finetune(dataset=str(ROWS), base_model=build_tiny_model(tmp_path / "m"), lora_rank=2, steps=1,
                     max_samples=8, learning_rate=1e-3, max_seq_len=32, output_dir=str(tmp_path / "a"),
                     device="cpu")
    assert (out["device"], out["precision"], out["peak_gpu_mem_gb"]) == ("cpu", "fp32", 0.0)


@pytest.mark.train
@pytest.mark.skipif(not torch.cuda.is_available(), reason="needs a CUDA GPU (run on Colab)")
def test_two_steps_train_on_the_gpu_in_half_precision(tmp_path):
    model = build_tiny_model(tmp_path / "m")
    out = F.finetune(dataset=str(ROWS), base_model=model, lora_rank=2, steps=2, max_samples=8,
                     learning_rate=1e-3, max_seq_len=32, output_dir=str(tmp_path / "a"))
    expected = "bf16" if torch.cuda.get_device_capability()[0] >= 8 else "fp16"
    assert out["device"] == "cuda" and out["precision"] == expected
    assert math.isfinite(out["train_loss"]) and out["peak_gpu_mem_gb"] > 0
    back = F.evaluate(adapter_path=out["adapter_path"], base_model=model, dataset=str(ROWS), max_samples=8,
                      max_seq_len=32)
    assert math.isfinite(back["eval_loss"])
