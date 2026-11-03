# Changelog

## Unreleased

- `train/lora`, `train/eval-loss` and the `train.lora` stage take `device` (`auto`, `cpu`,
  `cuda`, `mps`) and `precision` (`auto`, `fp32`, `fp16`, `bf16`). `auto` trains on one CUDA
  GPU when available, in bf16 from compute capability 8.0 and fp16 below it, and is the
  unchanged fp32 CPU run otherwise (ADR-0166).
- Implements `contract://train.finetune/1.1.0` and `contract://train.eval_loss/1.1.0`
  (`cairn-contracts-train` 1.1.0; the two input schemas gain the fields above).
- The stage's stats report `device`, `precision` and `peak_gpu_mem_gb`.

## 0.1.0

- Initial release (ADR-0127 D4, #2572 WP3): a builtin pack (fourteen with secrets-infisical, D3).
- `train/lora` implements `contract://train.finetune/1.0.0` (`cairn-contracts-train` 1.0.0);
  the same fine-tune ships as the ml flow stage `train.lora`.
- `train/eval-loss` scores a saved adapter; implements `contract://train.eval_loss/1.0.0`.
- The training stack (torch, transformers, peft, datasets) is the `[train]` extra; mock paths
  for both operations and the stage need none of it.
