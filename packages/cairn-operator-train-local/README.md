# cairn-operator-train-local

A real LoRA fine-tune on CPU or a CUDA GPU, the builtin provider of the `train.finetune` capability
(contract `contract://train.finetune/1.1.0`, distribution `cairn-contracts-train`; ADR-0127 D4).

| Ref | Kind | What it does |
|---|---|---|
| `train/lora` | operation (`external_write`) | Fine-tunes a causal LM with a LoRA adapter on a JSONL of `{"text": ...}` rows; returns the adapter path, train and held-out eval loss, tokens seen. Implements `train.finetune/1.1.0`. |
| `train.lora` | ml flow stage (`cairn.dataflow_stages`) | The same fine-tune as an engine kernel; its stats carry the adapter as the leg's `model` artifact. |
| `train/eval-loss` | operation (`read_only`) | Scores a saved adapter: mean loss over a JSONL's rows. Implements `train.eval_loss/1.1.0`. |

```bash
pip install 'cairndev[train]'
```

Without the `[train]` extra the pack still installs, imports and registers (it is in the base
install), and both operations refuse at run with `install cairndev[train]`.
`--mock` runs need no extra: they return a deterministic stand-in with a `mock://` adapter path,
and the stage's mock describes no model artifact.

**Model.** The default base model is `Qwen/Qwen2.5-0.5B` (a download of about 1 GB; minutes of
CPU for 8 steps). CI runs the base and mock tests (no extra); the `-m train` tests, which train
a one-layer GPT-2 built on the spot, need `--extra train` and are not a CI cell until hello-cairn
WP7 adds one. `sshleifer/tiny-gpt2` is the hub stand-in. A deployment pins its own default in its profile, and a run's bound
`base_model` still wins:

```toml
[profile.config."train/lora"]
base_model = "sshleifer/tiny-gpt2"
```

There is no environment variable for the model: the operator config layer reads profiles, not
the process environment (`cairn/core/operator/config.py`).

**GPU.** `device` and `precision` (both `auto` by default) train on one CUDA GPU when torch can
see one, in bf16 on compute capability 8.0+ (Ampere and later) and fp16 below (a T4), and on
the CPU in fp32 otherwise; a Mac stays on the CPU unless `device: mps` is asked for. Naming
`cuda` where it is unavailable, or `fp16`/`bf16` off CUDA, is refused. Findings report where a
run trained: `device`, `precision`, `peak_gpu_mem_gb`. One GPU only: pick it with
`CUDA_VISIBLE_DEVICES`. Verified on a Colab T4 (ADR-0167).

**Platforms.** The `[train]` extra pins `torch>=2.11,<2.12`
(the private training pack's floor), which publishes wheels for Linux x86_64/aarch64, macOS
arm64 and Windows, **not Intel macOS**.

**Not in the engine image.** `Dockerfile.engine` does not install `[train]`: CPU torch would add
gigabytes to every control-plane image. Install the extra where the ml engine runs the stage.

Design record: [DESIGN.md](DESIGN.md).
