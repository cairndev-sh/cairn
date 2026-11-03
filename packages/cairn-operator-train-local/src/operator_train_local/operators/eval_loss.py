"""train/eval-loss — score a saved LoRA adapter: mean causal-LM loss over a JSONL's rows.

Reads the adapter ``train/lora`` wrote and the rows; writes nothing. The pipeline's loss gate
reads ``eval_loss``. Implements ``train.eval_loss/1.0.0`` (``cairn-contracts-train``, beside
``train.finetune``): every builtin operator implements a contract (the tier guard).
"""
from __future__ import annotations

import asyncio

from cairn.core.operator import Operation, OperationContext, ReportSpec, register_operator

from operator_train_local import finetune as F
from operator_train_local.schemas.train import EvalLossInput, EvalLossOutput


@register_operator(
    name="train/eval-loss",
    version="0.1.0",
    capabilities=["train.eval_loss"],
    implements=["contract://train.eval_loss/1.0.0"],  # cairn-contracts-train
)
class EvalLossOp(Operation[EvalLossInput, EvalLossOutput]):
    """Mean loss of ``base_model`` + the adapter at ``adapter_path`` over ``dataset``."""

    input_schema = EvalLossInput
    output_schema = EvalLossOutput
    supports_mock = True
    risk = "read_only"  # reads an adapter and rows; writes nothing anywhere
    activity_timeout_seconds = 900.0
    reports = ReportSpec(scalars=("eval_loss", "tokens_seen"))

    async def run(self, inp: EvalLossInput, ctx: OperationContext) -> EvalLossOutput:
        F.require_train_stack()
        out = await asyncio.to_thread(F.evaluate, adapter_path=inp.adapter_path, base_model=inp.base_model,
                                      dataset=inp.dataset, max_samples=inp.max_samples,
                                      max_seq_len=inp.max_seq_len, device=inp.device,
                                      precision=inp.precision)
        ctx.report.scalars(**out)
        return EvalLossOutput(**out)

    def mock(self, inp: EvalLossInput, ctx: OperationContext) -> EvalLossOutput:
        """Deterministic stand-in for ``--mock`` runs: no torch, nothing read."""
        return EvalLossOutput(**F.mock_eval(**inp.model_dump()))


__all__ = ["EvalLossOp"]
