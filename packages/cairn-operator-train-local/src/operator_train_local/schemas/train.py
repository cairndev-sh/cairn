"""Schemas for ``train/lora`` (contract ``train.finetune/1.0.0``) and ``train/eval-loss``.

``TrainLoraInput`` / ``TrainLoraConfig`` / ``TrainLoraOutput`` mirror the three schemas
``cairn-contracts-train`` ships (``contracts/train/definitions/schemas``); the conformance test
in ``tests/core/test_builtin_operator_contracts_train.py`` fails when they drift. The ml stage
``train.lora`` takes the same keys as its stage config and reports the same output keys.
"""
from __future__ import annotations

from typing import Optional, Literal

from pydantic import BaseModel, ConfigDict, Field

#: The default base model (ADR-0127 D4). A deployment pins another through
#: ``[profile.config."train/lora"] base_model``; a run's bound ``base_model`` wins over both.
DEFAULT_BASE_MODEL = "Qwen/Qwen2.5-0.5B"

Device = Literal["auto", "cpu", "cuda", "mps"]
Precision = Literal["auto", "fp32", "fp16", "bf16"]

class TrainLoraInput(BaseModel):
    """What to fine-tune, on what, and how long: a LoRA adapter over base_model trained on the
    dataset's {"text": ...} rows."""

    dataset: str = Field(..., description="Where the training rows are: a local JSONL path (or file:// URI) of "
                                          "{\"text\": ...} rows, or a dataset ref the provider can load.")
    base_model: str = Field(DEFAULT_BASE_MODEL, description="The causal LM to adapt: a Hugging Face model id or a "
                                                            "local model directory.")
    lora_rank: int = Field(8, ge=1, description="LoRA rank r.")
    steps: int = Field(8, ge=1, description="Optimizer steps to run.")
    max_samples: int = Field(64, ge=1, description="At most this many rows are read from the dataset (train plus "
                                                   "held-out eval).")
    learning_rate: float = Field(2e-4, gt=0)
    max_seq_len: int = Field(256, ge=8, description="Each row is truncated to this many tokens.")
    output_dir: Optional[str] = Field(None, description="Where the adapter is written; the provider chooses when "
                                                        "unset.")
    device: Device = Field("auto", description="Where to run: auto is CUDA when available, else the CPU.")
    precision: Precision = Field("auto", description="Weights and compute dtype: auto is bf16 or fp16 on "
                                                     "CUDA (by GPU), fp32 elsewhere.")


class TrainLoraConfig(BaseModel):
    """Deployment-tunable knob (ADR-0068): a subset of :class:`TrainLoraInput`. Operational
    scope, so a run may still bind its own ``base_model`` (run > workflow > profile > default)."""

    model_config = ConfigDict(extra="forbid")

    base_model: str = Field(DEFAULT_BASE_MODEL, description="The base model a run uses when it binds none.")


class TrainLoraOutput(BaseModel):
    """Scalars and a path: the adapter's weights never appear here."""

    adapter_path: str = Field(..., description="Directory holding the saved LoRA adapter.")
    train_loss: float = Field(..., description="Mean training loss over the steps run.")
    eval_loss: Optional[float] = Field(None, description="Mean loss on the held-out rows; null when too few rows to "
                                                         "hold any out.")
    tokens_seen: int = Field(..., description="Non-padding tokens the optimizer trained on.")
    steps_run: int = Field(..., description="Optimizer steps actually run.")
    base_model: str = Field(..., description="The base model the adapter was trained on.")
    elapsed_s: float = Field(..., description="Wall-clock seconds the fine-tune took.")


class EvalLossInput(BaseModel):
    """Score a saved adapter: mean causal-LM loss over the first ``max_samples`` rows."""

    adapter_path: str = Field(..., description="Directory of a LoRA adapter (what train.finetune returned).")
    base_model: str = Field(DEFAULT_BASE_MODEL, description="The base model the adapter was trained on.")
    dataset: str = Field(..., description="JSONL path (or file:// URI) of {\"text\": ...} rows, or a dataset ref.")
    max_samples: int = Field(64, ge=1, description="At most this many rows are scored.")
    max_seq_len: int = Field(256, ge=8, description="Each row is truncated to this many tokens.")
    device: Device = Field("auto", description="Where to run: auto is CUDA when available, else the CPU.")
    precision: Precision = Field("auto", description="Weights and compute dtype: auto is bf16 or fp16 on "
                                                     "CUDA (by GPU), fp32 elsewhere.")

class EvalLossOutput(BaseModel):
    eval_loss: float = Field(..., description="Mean loss over the scored rows.")
    tokens_seen: int = Field(..., description="Non-padding tokens scored.")
