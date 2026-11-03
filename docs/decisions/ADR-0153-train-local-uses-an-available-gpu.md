# ADR-0153 — train-local uses an available CUDA GPU

- **Status:** PROPOSED 2026-11-03
- **Date:** 2026-11-03
- **Amends:** ADR-0127 D4 (train-local is a CPU-only LoRA fine-tune) and the pack's DESIGN.md §1
  ("It deliberately does NOT: use a GPU").
- **Keeps:** ADR-0127 D4's scope otherwise: no quantization, no `Trainer`/`accelerate`/`trl`, no
  multi-GPU or distributed training, no hub push. The same module serves `train/lora`, the
  `train.lora` stage and `train/eval-loss`. The `[train]` extra and its pins are unchanged.

## Context

`train-local` is the public core's only real training step, and it ran on the CPU in fp32 even
on a machine with a GPU: `_load` never chose a device, and nothing moved the batches. A user
with one GPU (a workstation, a cloud VM, a free Colab T4) could not train the `tiny-finetune`
example on it without a second training pack, and the private one is not public. The change
needed is small and contained in one module, and a CPU-only machine can keep exactly the
behaviour it has.

## Decision

**D1 — Two run inputs, `device` and `precision`, default `auto`.** `device` is `auto`, `cpu`,
`cuda` or `mps`; `precision` is `auto`, `fp32`, `fp16` or `bf16`. They are inputs of
`train/lora`, `train/eval-loss` and the `train.lora` stage (the stage validates its config with
the operation's input model, so it takes them unchanged).

**D2 — `auto` never changes a machine without CUDA.** `device: auto` is `cuda` when
`torch.cuda.is_available()` and the CPU otherwise. It never picks MPS: a Mac keeps the CPU run
it had, and `mps` is opt-in at fp32. `precision: auto` is fp32 off CUDA.

**D3 — On CUDA, the precision follows compute capability.** bf16 from capability 8.0 (Ampere and
later), fp16 below it. `torch.cuda.is_bf16_supported()` is not used: it counts emulation and
reports True on a T4 (7.5), where bf16 is emulated. Measured on a Colab T4, `tiny-finetune` with
Qwen2.5-0.5B for 50 steps took 22.9 s with emulated bf16 and 12.3 s with fp16, at the same peak
memory (1.13 GB).

**D4 — Refuse, never downgrade.** A named device that is unavailable, and fp16/bf16 off CUDA,
raise before any weights load. Only `auto` falls back, and the stage's stats say where the run
went (`device`, `precision`, `peak_gpu_mem_gb`; not contract output, since the engine discards
the job's stdout).

**D5 — Mixed precision the plain way.** The base model loads in the resolved dtype; peft keeps
the LoRA weights in fp32 (`autocast_adapter_dtype`); the forward pass runs under
`torch.autocast`; fp16 uses `torch.amp.GradScaler`, bf16 and fp32 do not.

**D6 — The contracts move to 1.1.0, additively.** `train.finetune` and `train.eval_loss` and
their input schemas move to revision 1.1.0 (`cairn-contracts-train` 1.1.0); the configuration,
output and report schemas stay 1.0.0. Every input valid at 1.0.0 is valid at 1.1.0 with the
same meaning.

## Consequences

- On a machine with a working CUDA torch, an existing pipeline that binds neither input now
  trains on the GPU. Its numbers differ from a CPU run (precision and kernels), and its adapter's
  `content_hash` with them; `device: cpu` restores the CPU run.
- `auto` falls back silently when torch cannot see CUDA (no GPU, or a driver older than the
  build). The findings show it; `device: cuda` turns it into a refusal.
- One GPU only: `cuda` is the current device, chosen with `CUDA_VISIBLE_DEVICES`.
- A deployment must reinstall `cairndev` for the 1.1.0 definitions to be installed; until then a
  launch is refused with "no such capability revision is installed".
- Not verified here: `device: mps` on Apple Silicon, and the SSH engine's remote boxes on a GPU.
