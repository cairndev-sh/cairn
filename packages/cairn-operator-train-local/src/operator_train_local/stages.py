"""The ``train.lora`` ml flow stage — the same fine-tune as ``train/lora``, as an engine kernel.

A stage is ``stage(workdir, config, inputs) -> stats`` (``cairn.dataflow_stages``, resolved by
name by the builtin engine and every other adapter). ``config`` takes the ``train.finetune``
input keys, validated by the operation's own input model, plus ``mock: true`` for the
torch-free stand-in; ``dataset`` defaults to the flow's first input artifact. The stats are the
contract's output keys plus what the runner turns into the leg's artifact descriptor
(invariant 5: the control plane registers it): ``output`` (the adapter directory), ``kind:
model``, ``content_hash`` and ``bytes`` over the adapter files this run wrote, and ``cost_usd``
0 — a CPU fine-tune bills nothing. The mock carries ``mock: true`` and a ``mock://`` adapter path,
and none of ``output``/``kind``/``content_hash``: no model artifact is described for weights that
do not exist (the runner records its usual placeholder for the stage).

The stage declares no ``implements``: ``train.finetune`` is a capability contract (input,
configuration, output), and a stage contract is the ``cairn.stage/1`` kind without an input.
The contract holds here because the input and output models are the operation's.
"""
from __future__ import annotations

import os
from pathlib import Path

from cairn.core.operator import getenv

from operator_train_local import finetune as F
from operator_train_local.schemas.train import TrainLoraInput, TrainLoraOutput


def _submit_relative(path: str) -> str:
    """The builtin engine runs a kernel in a temp workdir; a RELATIVE local path in the flow
    resolves against the submitter's cwd (the dataset_checks ingest convention)."""
    cwd = getenv("CAIRN_SUBMIT_CWD", "") or ""
    if cwd and "://" not in path and not os.path.isabs(path) and (Path(cwd) / path).exists():
        return str(Path(cwd) / path)
    return path


def train_lora(workdir: Path, config: dict, inputs: list[str]) -> dict:
    """Fine-tune per ``config``; the adapter lands in ``<workdir>/adapter`` unless
    ``output_dir`` is set."""
    cfg = dict(config)
    mock = bool(cfg.pop("mock", False))
    if not cfg.get("dataset") and inputs:
        cfg["dataset"] = inputs[0]
    unknown = sorted(set(cfg) - set(TrainLoraInput.model_fields))
    if unknown:  # a typo'd knob must not silently fall back to its default
        raise ValueError(f"train.lora: unknown config keys {unknown}; accepted: "
                         f"{sorted(TrainLoraInput.model_fields)} and mock")
    params = TrainLoraInput.model_validate(cfg)
    params.dataset = _submit_relative(params.dataset)
    output_dir = params.output_dir or str(Path(workdir) / "adapter")
    if mock:
        out = F.mock_result(run=Path(workdir).name, **params.model_dump())
        return {**TrainLoraOutput(**out).model_dump(), "mock": True, "cost_usd": 0.0}
    written: list[str] = []
    out = F.finetune(dataset=params.dataset, base_model=params.base_model, lora_rank=params.lora_rank,
                     steps=params.steps, max_samples=params.max_samples, learning_rate=params.learning_rate,
                     max_seq_len=params.max_seq_len, output_dir=output_dir, written=written,
                     device=params.device, precision=params.precision)
    digest, size = F.adapter_digest(output_dir, written)
    ran_on = {k: out[k] for k in ("device", "precision", "peak_gpu_mem_gb")}
    return {**TrainLoraOutput(**out).model_dump(), **ran_on, "kind": "model", "cost_usd": 0.0,
            "output": str(Path(output_dir).resolve()), "content_hash": digest, "bytes": size}


# ADR-0081 D2 (#1051): undeclared = external_write anyway; declared because it writes an
# adapter (inside the workdir by default, anywhere ``output_dir`` points otherwise).
train_lora.risk_class = "external_write"
