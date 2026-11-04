# ADR-0167 — train-local uses an available CUDA GPU

- **Status:** PROPOSED 2026-11-03
- **Date:** 2026-11-03
- **Issue:** cairndev-sh/cairn#454 (the contracts it touches: `train.finetune`, `train.eval_loss`)
- **Amends:** ADR-0127 D4 (train-local is a CPU-only LoRA fine-tune) and the pack's DESIGN.md §1
  ("It deliberately does NOT: use a GPU").
- **Keeps:** ADR-0127 D4's scope otherwise: no quantization, no `Trainer`/`accelerate`/`trl`, no
  multi-GPU or distributed training, no hub push. The same module serves `train/lora`, the
  `train.lora` stage and `train/eval-loss`. The `[train]` extra and its pins are unchanged.

## Context

`train-local` is the training step the public core ships: a builtin inside the `cairndev`
wheel (ADR-0137), the provider of `train.finetune` that the `hello-cairn` `tiny-finetune`
example runs. It ran on the CPU in fp32 even on a machine with a GPU: `_load` never chose a
device, and nothing moved the batches.

`cairn-operator-training`, now public in `cairndev-sh/cairn-extensions`, is the training pack for
heavier work: SFT with LoRA or 4-bit QLoRA (Unsloth, TRL, bitsandbytes), DPO and distillation,
behind a different contract (`train.run` and the `training.stage.*` stages). Its GPU trainer pins
its stack exactly, is installed from a checkout of that repository, and runs where the profile's
ml engine puts it, typically an ssh GPU box. Its `train/local` also runs plain LoRA on a GPU, but
behind `train.run`, outside the core wheel.

So the two do not overlap in what they are for. Someone with one GPU (a workstation, a cloud VM,
a Colab T4) running the core's own example should not need a second pack, its exact pins and a
remote engine to use the GPU in front of them. In `train-local` the change is small, adds no
dependency, sits in one module, and leaves a machine without CUDA exactly as it was. Anything
past one GPU and plain LoRA (QLoRA, multi-GPU, a remote box, preference tuning) stays the
training pack's.

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

**D6 — 1.1.0 is a new revision beside 1.0.0.** Revisions are immutable (ADR-0111): an admitted
pipeline records the digest of the revision it ran against. `cairn-contracts-train` 1.1.0 keeps
the 1.0.0 capability and input-schema files and their `contracts.toml` rows byte for byte, and
adds `train.finetune` and `train.eval_loss` 1.1.0 and their input schemas as new files
(`*.1.1.0.json`) and new rows; the 1.1.0 capabilities point at the new input schemas and at the
unchanged 1.0.0 configuration, output and report schemas. The input schemas gain `device` and
`precision` and nothing else, so every input valid at 1.0.0 is valid at 1.1.0 with the same
meaning. `train/lora` and `train/eval-loss` declare only 1.1.0. The pre-fold snapshot keeps every
1.0.0 digest and adds the four 1.1.0 definitions.

## Consequences

- On a machine with a working CUDA torch, an existing pipeline that binds neither input now
  trains on the GPU. Its numbers differ from a CPU run (precision and kernels), and its adapter's
  `content_hash` with them; `device: cpu` restores the CPU run.
- `auto` falls back silently when torch cannot see CUDA (no GPU, or a driver older than the
  build). The findings show it; `device: cuda` turns it into a refusal.
- One GPU only: `cuda` is the current device, chosen with `CUDA_VISIBLE_DEVICES`.
- The 1.1.0 definitions ship in the same `cairndev` wheel as the operators that declare them, so
  an upgrade brings both. In a checkout, where the contract files are copied into the installed
  package, a `cairndev` that was not reinstalled after this change still has only 1.0.0, and a
  launch is refused with "no such capability revision is installed" until it is
  (`uv sync --reinstall-package cairndev`).
- 1.0.0 stays installed, so the records of runs admitted against it keep resolving to the digests
  they recorded.
- Not verified here: `device: mps` on Apple Silicon, and the SSH engine's remote boxes on a GPU.
