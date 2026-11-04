# train_local — design

- **Distribution:** `cairn-operator-train-local` (`cairn/operators/builtins/train_local/`)
- **Capabilities provided:** `train.finetune` (contract `contract://train.finetune/1.1.0`), `train.eval_loss` (contract `contract://train.eval_loss/1.1.0`)
- **Status:** experimental (new; proven on a one-layer GPT-2 in the pack's tests)
- **Decisions:** ADR-0127 D4, #2572 (WP3)

## 1. Purpose

Give the public core one real machine-learning step a new user can run on a laptop: a LoRA
fine-tune of a small causal LM on a JSONL of `{"text": ...}` rows, the adapter it produces, and
a held-out loss a gate can read. It is what the `tiny-finetune` pipeline of
`examples/hello-cairn` trains with, and it is the builtin provider of `train.finetune`, so a
private or third-party trainer can implement the same contract and be bound in its place.

It uses one CUDA GPU when one is available (`device`/`precision`, default `auto`; ADR-0167),
so the same pack trains on a laptop CPU, a workstation GPU or a Colab T4. It deliberately does
NOT: quantize (no bitsandbytes), use `Trainer`/`accelerate`/`trl`, run on several GPUs or
distributed, stream checkpoints, push to a hub or register the model (promotion is the
pipeline's `artifact/promote` step on the leg's `model` artifact). Those belong to the private
training pack, which may implement `train.finetune` too.

## 2. Architecture

One implementation module, `operator_train_local/finetune.py`, called from three faces:

```
train/lora (operation) ─┐
                        ├─> finetune.finetune(dataset, base_model, lora_rank, steps, ...)
train.lora (ml stage) ──┘     load_texts -> split_holdout -> resolve_device/resolve_dtype -> AutoModelForCausalLM (dtype, device)
                              -> peft.get_peft_model(LoraConfig(r, alpha=2r)) -> AdamW x steps (autocast; GradScaler at fp16)
                              -> mean held-out loss -> model.save_pretrained(output_dir)
train/eval-loss ──────────> finetune.evaluate(adapter_path, base_model, dataset, ...)
                              -> PeftModel.from_pretrained -> token-weighted mean loss
```

- **Data.** A local JSONL path or `file://` URI is read with the standard library (the first
  `max_samples` rows with a string `text`); a path-shaped value naming no file is refused; any
  other value is a dataset ref loaded with `datasets.load_dataset(ref, split="train")`.
  One row in eight is held out (none below eight rows, and `eval_loss` is then `null`).
- **Training.** `torch.manual_seed(0)`; batches of 4 rows (wrapping when the data is smaller),
  right-padded, padding masked out of the labels; LoRA target modules inferred by peft per
  architecture (GPT-2 `c_attn`, Qwen2 `q_proj`/`v_proj`). A non-finite loss raises before the
  adapter is saved.
- **Device and precision.** `resolve_device("auto")` is `cuda` when `torch.cuda.is_available()`
  and the CPU otherwise; it never picks MPS, so a Mac keeps its CPU behaviour (`mps` is opt-in).
  A named device that is unavailable raises. `resolve_dtype("auto")` is fp32 off CUDA, and on
  CUDA bf16 from compute capability 8.0 (Ampere) and fp16 below it (a T4 is 7.5):
  `torch.cuda.is_bf16_supported()` also counts emulation and reports True on a T4, so it is not
  used. fp16/bf16 off CUDA are refused. The base model loads in that dtype; peft keeps the LoRA
  weights in fp32 (`autocast_adapter_dtype`), the forward pass runs under `torch.autocast`, and
  fp16 uses a `GradScaler`. `train/eval-loss` resolves the same way. The stage's stats add
  `device`, `precision` and `peak_gpu_mem_gb` (not contract output), because the engine
  discards the job's stdout.
- **Where it runs.** The operation runs the blocking loop in a worker thread of the calling
  process. The stage runs inside the engine's job process (the builtin engine's
  `cairn-dataflow-run` subprocess, with its workdir as cwd).
- **Where state lives.** Only the adapter directory: `output_dir` when given; otherwise
  `<tmp>/cairn-train-local/<pipeline_id>/<node>/adapter` for the operation (a fresh
  `adhoc-<uuid>` when the context has no run id) and
  `<workdir>/adapter` for the stage.
- **Retries.** A retry of the same leg re-trains from the same seed (`torch.manual_seed(0)`;
  CPU float results are not promised bit-identical across machines or thread counts) and
  rewrites the adapter files in its directory. Other files already in `output_dir` are left in
  place and are not part of the stage's `content_hash`, which covers only the files this run
  wrote.

The stage reports the contract's output keys plus `output` (the adapter directory, absolute),
`kind: model`, `content_hash`/`bytes` over the adapter files this run wrote and `cost_usd: 0`.
The mock (`mock: true` in the config) returns `mock: true`, a `mock://train-local/<run>` adapter
path and none of `output`/`kind`/`content_hash`, so no model artifact is described for weights
that do not exist. The runner
turns those into the leg's artifact descriptor, which the control plane registers (invariant
5). The stage declares no `implements`: `train.finetune` is a capability contract and a stage
contract is the input-less `cairn.stage/1` kind; it holds because the stage validates its
config with the operation's input model and its output with the operation's output model, and
refuses unknown config keys.

## 3. Operators

| Operator | Capability | Risk | Mock | Input -> Output | Reports (stages · series · scalars) |
|---|---|---|---|---|---|
| `train/lora` | `train.finetune` | external_write | yes | `TrainLoraInput` -> `TrainLoraOutput` | `train` · `loss` · `train_loss`, `eval_loss`, `tokens_seen`, `steps_run` |
| `train/eval-loss` | `train.eval_loss` | read_only | yes | `EvalLossInput` -> `EvalLossOutput` | `—` stages · `—` series · `eval_loss`, `tokens_seen` |

The ml stage `train.lora` (entry point `cairn.dataflow_stages`, `risk_class = external_write`)
takes the `TrainLoraInput` keys as its config plus `mock: true`; `dataset` defaults to the
flow's first input artifact; a relative local path resolves against the submitter's cwd
(`CAIRN_SUBMIT_CWD`, which the builtin engine sets).

`TrainLoraInput`: `dataset` (required), `base_model` (`Qwen/Qwen2.5-0.5B`), `lora_rank` (8),
`steps` (8), `max_samples` (64), `learning_rate` (2e-4), `max_seq_len` (256), `output_dir`
(optional), `device` (`auto`), `precision` (`auto`). `TrainLoraOutput`: `adapter_path`, `train_loss` (mean over steps), `eval_loss`
(held-out mean, or null), `tokens_seen` (non-padding training tokens), `steps_run`,
`base_model`, `elapsed_s`. Both mirror `contracts/train/definitions/schemas/train.finetune.*`.

`EvalLossInput`: `adapter_path`, `base_model`, `dataset`, `max_samples` (64), `max_seq_len`
(256), `device` (`auto`), `precision` (`auto`). `EvalLossOutput`: `eval_loss`, `tokens_seen`. Both mirror
`contracts/train/definitions/schemas/train.eval_loss.*` (configuration: the empty closed object).

## 4. Configuration

| Setting | Where | Scope | Default | What it does |
|---|---|---|---|---|
| `base_model` | `config_schema` (`TrainLoraConfig`) | operational | `Qwen/Qwen2.5-0.5B` | The deployment's default base model, set under `[profile.config."train/lora"]`; a run's bound `base_model` wins (run > workflow > profile > default) |
| `dataset`, `lora_rank`, `steps`, `max_samples`, `learning_rate`, `max_seq_len`, `output_dir`, `device`, `precision` | `input_schema` | run | see §3 | Per-run knobs |
| `mock` | stage config only | run | `false` | The stage's torch-free stand-in |
| `CAIRN_SUBMIT_CWD` | env (set by the builtin engine) | job | — | Resolves a relative `dataset` in the stage |

The config knob is named `base_model`, not `model_default`: core requires `config_schema` to be
a subset of `input_schema` (`validate_config_declaration`), so the deployment default must be
the input field it fills. There is no environment variable for the model; the config layer
reads profiles only. No connections. No secrets: the default model is public; a gated hub
model needs the Hugging Face token in the job's environment, which this pack never reads.

## 5. Governance

`train/lora` is `external_write`: it writes an adapter and spends minutes of CPU, so it must
never be treated as `read_only` and silently re-run on restart (#1105, #2026). Nothing is
billed, so it is not `paid_compute`. `train/eval-loss` is `read_only`. The stage is
`external_write`, which makes any flow containing it aggregate to `external_write` at flow
admission. A pipeline places a loss gate on `eval_loss` and a person gate before promoting the
adapter (hello-cairn `tiny-finetune`). The door records `train/lora` against
`train.finetune/1.1.0` and `train/eval-loss` against `train.eval_loss/1.1.0`, with the
distribution's digests; a non-conforming input refuses before
the operation runs and a non-conforming output fails the run.

## 6. Failure modes

| Symptom | Cause | What the operator returns | Fail closed? |
|---|---|---|---|
| `install cairndev[train]` | The `[train]` extra is absent (always on the engine image) | `TrainStackMissing` at run | Yes |
| `torch` cannot be installed | `torch>=2.11` publishes no Intel-macOS wheel | install error | Yes |
| `dataset '…': no such file` | A path-shaped dataset names no file | `FileNotFoundError` | Yes |
| `a row must be an object with a string 'text' field` | A malformed JSONL row | `ValueError` naming file and line | Yes |
| `train loss is nan at step N` | Divergence (learning rate too high for the model) | `RuntimeError`; no adapter saved | Yes |
| `unknown config keys [...]` | A typo in the stage config | `ValueError` | Yes |
| `eval_loss` is null | Fewer than eight rows: nothing held out | a valid output | n/a |
| Slow first run | The default model downloads from the Hugging Face hub (~1 GB) | — | n/a |
| `device 'cuda' was asked for, but torch.cuda.is_available() is False` | `device: cuda` on a machine whose torch has no working CUDA (no GPU, or a driver older than the build) | `RuntimeError` | Yes |
| `precision 'fp16' needs a CUDA device` | `fp16`/`bf16` with the run on the CPU or MPS | `ValueError` | Yes |
| `device` reads `cpu` on a GPU machine | `auto` fell back: torch cannot see CUDA | a valid output; pass `device: cuda` to make it fail instead | No (by design) |

## 7. Testing

Pack-local tests in `cairn/operators/builtins/train_local/tests/`. Without the extra:
manifest/class agreement, the mock paths, the loud refusal (torch made unimportable), data
loading and the stage through the builtin engine (mock). With the extra (`-m train`, skipped
when torch is absent): a one-layer, eight-wide GPT-2 and a word-level tokenizer built in the
test (no network), two LoRA steps on an eight-row JSONL, the adapter read back by
`train/eval-loss`, and the real stage through the builtin engine. Device and precision
selection is tested with CUDA faked (`tests/test_train_local_device.py`); its one real GPU test
runs only where CUDA is present (verified on a Colab T4: fp16, `peak_gpu_mem_gb` > 0). The shared-contract proof
is `tests/core/test_builtin_operator_contracts_train.py`. From a worktree, install the pack
into a scratch venv (`uv sync --frozen --extra dev --extra train`), never the shared one. CI
installs no `[train]` extra today, so it runs the base and mock tests and skips `-m train`
until hello-cairn WP7 adds that cell.

## 8. History

| Date | Change | PR |
|---|---|---|
| 2026-10-15 | Initial pack: `train/lora`, `train.lora`, `train/eval-loss`; contracts `train.finetune/1.0.0`, `train.eval_loss/1.0.0` (every builtin operator implements a contract) | #2572 WP3 |
| 2026-11-03 | `device`/`precision` inputs: one CUDA GPU when available, fp16/bf16 by compute capability; contracts `train.finetune/1.1.0`, `train.eval_loss/1.1.0` (additive) | ADR-0167 |
