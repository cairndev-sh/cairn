"""train/lora — a LoRA fine-tune on CPU, the operation face of ``train.finetune/1.0.0``.

The fine-tune itself is :func:`operator_train_local.finetune.finetune`, the same function the
``train.lora`` ml stage runs, so the contract holds on both paths. The blocking torch loop
runs in a worker thread; per-step losses stream to the reporter as the ``loss`` series.
"""
from __future__ import annotations

import asyncio
import tempfile
import threading
import uuid
from pathlib import Path

from cairn.core.operator import (
    Operation,
    OperationContext,
    ReportSpec,
    Series,
    register_operator,
)

from operator_train_local import finetune as F
from operator_train_local.schemas.train import TrainLoraConfig, TrainLoraInput, TrainLoraOutput


def default_output_dir(ctx: OperationContext) -> str:
    """Where an adapter lands when the run names no ``output_dir``: per run and node, so a
    retry of the same leg overwrites its own attempt and never another run's. A context with no
    run id gets a fresh one: never a directory shared between runs."""
    return str(Path(tempfile.gettempdir()) / "cairn-train-local" / (ctx.pipeline_id or f"adhoc-{uuid.uuid4().hex}")
               / (ctx.node_name or "train") / "adapter")


@register_operator(
    name="train/lora",
    version="0.1.0",
    capabilities=["train.finetune"],
    implements=["contract://train.finetune/1.0.0"],  # ADR-0127 D4 — cairn-contracts-train
)
class TrainLoraOp(Operation[TrainLoraInput, TrainLoraOutput]):
    """Fine-tune ``base_model`` with a LoRA adapter on a JSONL of ``{"text": ...}`` rows."""

    input_schema = TrainLoraInput
    output_schema = TrainLoraOutput
    config_schema = TrainLoraConfig
    supports_mock = True
    supports_cancel = True
    # It writes an adapter to disk and spends minutes of CPU: not read_only, so a restart
    # never silently re-runs it (#1105/#2026). Nothing is billed, so not paid_compute.
    risk = "external_write"
    activity_timeout_seconds = 1800.0  # Qwen2.5-0.5B on a laptop CPU: minutes, not the IO default
    reports = ReportSpec(stages=("train",), series=[Series("loss", "")],
                         scalars=("train_loss", "eval_loss", "tokens_seen", "steps_run"))

    async def run(self, inp: TrainLoraInput, ctx: OperationContext) -> TrainLoraOutput:
        F.require_train_stack()  # before any thread or file: the one-line reason a caller sees
        ctx.report.stage("train", percent=0.0)

        def _on_step(step: int, loss: float) -> None:
            ctx.report.series(step, loss=loss)

        cancel_event = threading.Event()
        worker = asyncio.create_task(asyncio.to_thread(
            F.finetune, dataset=inp.dataset, base_model=inp.base_model, lora_rank=inp.lora_rank,
            steps=inp.steps, max_samples=inp.max_samples, learning_rate=inp.learning_rate,
            max_seq_len=inp.max_seq_len, output_dir=inp.output_dir or default_output_dir(ctx),
            on_step=_on_step, cancel_event=cancel_event))
        try:
            out = await asyncio.shield(worker)
        except asyncio.CancelledError:
            cancel_event.set()
            while not worker.done():
                try:
                    await asyncio.shield(worker)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not worker.cancelled():
                error = worker.exception()
                if error is not None and not isinstance(error, F.TrainingCancelled):
                    ctx.logger.warning("train/lora worker failed while cancelling: %s", error)
            raise
        ctx.report.stage("train", percent=100.0)
        ctx.report.scalars(**{k: out[k] for k in ("train_loss", "eval_loss", "tokens_seen", "steps_run")})
        return TrainLoraOutput(**out)

    def mock(self, inp: TrainLoraInput, ctx: OperationContext) -> TrainLoraOutput:
        """Deterministic stand-in for ``--mock`` runs: no torch, no files, a ``mock://`` path."""
        run = f"{ctx.pipeline_id or 'run'}/{ctx.node_name or 'train'}" if ctx is not None else "run"
        return TrainLoraOutput(**F.mock_result(run=run, **inp.model_dump()))


__all__ = ["TrainLoraOp", "default_output_dir"]
