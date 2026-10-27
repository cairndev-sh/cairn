---
title: Fine-tune a model on a GPU box
description: Prepare a dataset, fine-tune a base model with LoRA or QLoRA on a GPU box that cairn starts and stops for you, approve the spend, and promote the result, all from the CLI.
---

This guide walks through a governed fine-tune with the
[`cairn-pack-dataset-prep`](https://pypi.org/project/cairn-pack-dataset-prep/)
and [`cairn-pack-finetune`](https://pypi.org/project/cairn-pack-finetune/)
packs. Each step ends with a command you can run and an explanation of what
cairn checks before it lets the run go further.

What you get:

- **A person approves the data source** before anything is downloaded.
- **The launch checks data roles and leakage.** It refuses a test set passed
  as training data, and training data that overlaps the eval sets.
- **A GPU cost estimate**, which a person approves before the box starts. The
  budget is capped.
- **The GPU box starts after approval and stops right after training.** It
  also stops on failure and on cancel.
- **The spend is settled per attempt** from the box's wall-clock time.
- **Promotion only after a person decides,** with the losses on the card.
- **Every step recorded.** You can read the findings and replay the run later.

## The layout

| Machine | Runs | Installs |
|---|---|---|
| **Control box**: any small Linux machine | the cairn engine: runs, gates, budget, the data leg, and the box's start and stop | `cairndev`, the two packs |
| **GPU box**: an EC2 GPU instance | only the trainer, started over SSH by the engine | `cairn-operator-training[gpu]` |
| **Your laptop** (optional) | the `cairn remote …` CLI, through an SSH tunnel | `cairndev` |

The engine never runs on the GPU box, and nothing listens on it: the
control box reaches out over SSH. Datasets, checkpoints and the adapter cross
between the machines through one S3 bucket.

**Before you start**, you need:

1. **Two EC2 instances** in the same VPC. Give the GPU instance an Elastic
   IP, or use its private IP from the control box.
2. **SSH from the control box to the GPU box,** with an SSH key on the
   control box and port 22 open between them.
3. **An instance role on both boxes** that can read and write the S3
   bucket. On the control box, the role also needs `ec2:DescribeInstances`,
   `ec2:CreateTags`, `ec2:StartInstances` and `ec2:StopInstances` on the GPU
   instance.
4. **Network access** to wherever your dataset and base model come from (for
   example the Hugging Face Hub).

To manage the box yourself, or to use one that isn't on EC2, use
`finetune/sft-own-box`. It is the same pipeline without the start and stop
steps, and its connection needs no `instance_id`.

## 1. Install on the control box

All public wheels from PyPI. The `[all]` extra turns on near-duplicate
detection and token-length profiling:

```bash
uv venv .venv --python 3.13
uv pip install "cairndev[ssh,storage-postgres]" \
  "cairn-pack-dataset-prep[all]" "cairn-pack-finetune"
```

## 2. Configure and start the engine

`cairn ls finetune` prints the installed path of the pack's `finetune-ssh`
profile. Put the settings in one file. Both the engine and the CLI must read
the **same** `CAIRN_STORAGE`, or the CLI writes connections to a store the
engine never reads. When the engine answers at the endpoint `cairn remote` uses
(`http://localhost:9090` unless a login or `CAIRN_REMOTE_ENDPOINT` names another),
`cairn connect add` checks this and refuses with exit 3 when the stores differ;
`cairn doctor` reports it as `engine_store`.

```bash
# engine.env
CAIRN_PROFILE=<path printed by `cairn ls finetune`>/finetune-ssh.toml
CAIRN_STORAGE=sqlite:///$HOME/.cairn/train.db        # or postgres://…
CAIRN_ARTIFACT_S3_BUCKET=<bucket>
CAIRN_ARTIFACT_S3_PREFIX=cairn/artifacts
CAIRN_CHECKPOINT_BUCKET=<bucket>
CAIRN_CHECKPOINT_PREFIX=cairn/checkpoints
AWS_DEFAULT_REGION=<region>
CAIRN_REMOTE_ENV=CAIRN_CHECKPOINT_BUCKET,CAIRN_CHECKPOINT_PREFIX,AWS_DEFAULT_REGION
CAIRN_DATAFLOW_REMOTE_CMD=<trainer venv on the GPU box>/bin/cairn-dataflow-run
```

- **`CAIRN_REMOTE_ENV`** forwards these variables, by name, to the training
  job on the GPU box. The engine refuses any name containing `KEY`, `SECRET`,
  `TOKEN` or `PASSWORD`.
- **`CAIRN_DATAFLOW_REMOTE_CMD`** is the stage runner's full path on the GPU
  box. Its virtualenv is created in step 5.

Start the engine with the GPU box's SSH key in its environment. The key is
read from the control box's own file and is never stored in a connection:

```bash
set -a; . ./engine.env; set +a
export GPU_SSH_KEY="$(cat ~/.ssh/<key-to-the-gpu-box>)"
cairn dev --engine-only --port 9090 --db "$HOME/.cairn/train.db"
```

`cairn dev` is the quickest way to start. For a server you keep, run the
[`deploy/light`](/install/) stack (Postgres, sign-in) instead.

## 3. Register the GPU box

A `compute.ssh` connection named `gpu`, the name the profile's `ml_target`
uses. `instance_id` and `region` let cairn start and stop it.
`hourly_usd` prices it, which turns on the launch estimate and the budget
check:

```bash
cairn connect add ssh --name gpu \
  --set host=<gpu host or private ip> --set user=<ssh user> \
  --set known_hosts=$HOME/.ssh/known_hosts --secret PRIVATE_KEY=GPU_SSH_KEY \
  --set instance_id=<i-…> --set region=<region> --set hourly_usd=<rate>
cairn doctor --strict
```

`cairn doctor --strict` should show:

- `ml_target=gpu`, `remote_exec_mode=venv` and the forwarded `remote_env`;
- every pack's `[compat]` satisfied;
- `compute/acquire` and `compute/release` loaded.

Log the CLI in to the engine. `cairn dev` prints its development login:

```bash
cairn remote login -u <user> -p <password> -e http://127.0.0.1:9090
```

## 4. Prepare the dataset

`dataset-prep/prepare` pins the source, removes exact and near duplicates,
profiles lengths with the base model's tokenizer, splits by a group key so
related rows never straddle train and test, and registers three dataset
versions: training, validation and test.

```bash
cairn remote launch dataset-prep/prepare \
  -i path=<hf dataset id | s3://… | local .jsonl/.csv/.parquet> \
  -i revision=<commit or tag, to pin a Hub dataset> \
  -i register=<name> --save-preset <name>
```

`cairn remote presets dataset-prep/prepare` lists ready-made presets the pack
ships; `--preset <name>` launches one. To see the plan a preset would launch
first, `cairn pipeline explain <ref> --preset <name>` shows each input with the layer
it comes from and refuses what the launch would refuse.

Rows may be in any of these formats:

- **Alpaca:** `instruction`, `input`, `output`;
- **Prompt and completion:** `prompt`, `completion`;
- **Chat:** `messages`.

The pipeline pauses at **`source-ok`**. A person checks the source, its pin
and the split before anything is read:

```bash
cairn remote advance <pl_id> source-ok --approver <you>
cairn remote findings <pl_id>
```

**Read the findings before you train.** They are the evidence for every
decision after this:

| Finding | What to look for |
|---|---|
| `checks_dedup_exact_dropped`, `checks_dedup_near_dropped` | How much the cleaning removed. A large near-duplicate share usually means template-generated data. Decide on purpose whether to keep that threshold (`near_threshold`), or compare it with a looser one. |
| `checks_profile_tokens_over_length_rows` | Rows longer than the sequence length you plan to train with |
| `split_rows_train / val / test` | Split sizes. Small eval sets give noisy losses. |
| `split_no_leakage` | Must be `true` |

## 5. Install the trainer on the GPU box

Do this right before training, because the box bills while it is up. It
needs only the trainer wheel and the same contract versions as the control
box:

```bash
uv venv <trainer venv> --python 3.13
VIRTUAL_ENV=<trainer venv> uv pip install \
  "cairn-operator-training[gpu]" \
  "cairn-contracts-training==<same as the control box>" \
  "cairn-contracts-training-stages==<same as the control box>"
```

The `cairndev` package comes along as a dependency. Only
`cairn-dataflow-run` is ever called on this box. You can leave the box
stopped: `finetune/sft` starts it after you approve.

## 6. Launch the fine-tune

```bash
cairn remote launch finetune/sft \
  -i train_version=dataset:<name>-train@latest \
  -i val_version=dataset:<name>-val@latest \
  -i test_version=dataset:<name>-test@latest \
  -i base_model=<hugging face model id> --save-preset <name>
```

Before any record exists, the launch door checks:

- each dataset version's **role** (train, val and test can't be swapped) and
  its intake verdict;
- the **holdout**: the training data must not overlap the eval sets, exactly
  or near;
- the **GPU estimate**: `hourly_usd` × each priced leg's timeout × its
  attempts, against `governance.budget`. A leg with no timeout can't be
  priced, so the launch is refused.

The run pauses at **`approve-run`**, showing that estimate and the training
settings:

```bash
cairn remote advance <pl_id> approve-run --approver <you>
```

Then, in order:

1. **`acquire`** starts the box. It writes a lease tag first, so
   `cairn compute sweep` can find an orphaned box.
2. **`smoke`** runs a few steps on a small sample, the whole path on the
   box.
3. **`train`** runs the full training. A killed process is retried once and
   resumes from its last S3 checkpoint.
4. **`release-box`** stops the box.
5. **`improved`** checks that the candidate beat the base model's eval loss.

A final `release` step stops the box on every outcome, as a backstop.

Follow it, and read the results:

```bash
cairn remote pipeline <pl_id>
cairn remote findings <pl_id>
```

The training findings include:

- `train_qlora_baseline_eval_loss`: the base model's eval loss;
- `train_qlora_eval_loss` and `train_qlora_train_loss`: the candidate's;
- `train_qlora_steps`;
- the adapter's S3 URI and content hash.

## 7. Decide, promote, check the spend

The run pauses at **`promote-model`** with the losses on the card. If you
promote, the `artifact/promote` operator asks for its own approval before it
moves the tag:

```bash
cairn remote advance <pl_id> promote-model --approver <you>
cairn remote advance <pl_id> promote --approver <you>
```

Each priced leg settles its real box time in `Run.cost.gpu_usd`, which you
can compare with the estimate you approved. Check that the box is stopped:

```bash
cairn compute status gpu
```

## 8. Check it didn't forget (optional, outside cairn for now)

A lower loss on your own data says the model learned the task. It doesn't
say whether general ability dropped. Compare the base model and base +
adapter with
[lm-evaluation-harness](https://github.com/EleutherAI/lm-evaluation-harness),
using the same settings for both, on the GPU box:

```bash
uv venv <eval venv> --python 3.13
VIRTUAL_ENV=<eval venv> uv pip install "lm-eval[hf]" peft bitsandbytes accelerate
for ARGS in "pretrained=<base model>" "pretrained=<base model>,peft=<adapter>"; do
  lm_eval --model hf --model_args "$ARGS" --tasks arc_easy,arc_challenge,hellaswag,winogrande,piqa \
    --limit <n> --batch_size auto --seed 1234
  lm_eval --model hf --model_args "$ARGS" --tasks mmlu --limit <n per subject> --batch_size auto --seed 1234
done
```

- **Time a small run first.** Run about 50 items, measure the speed, then
  choose `--limit`.
- **MMLU's limit is per subject** (there are 57), so give it a separate,
  smaller limit.
- **Small samples are noisy.** A few hundred items per task can't resolve
  changes of a few points.

Stop the box afterwards. A governed eval step inside `finetune/sft`, priced
and recorded like training, is in progress.

## What to change for your own run

| | |
|---|---|
| Data | `dataset-prep/prepare -i path=… -i register=…`, plus the split ratios, `group_by` and the dedup threshold |
| Base model | `-i base_model=<any Hugging Face causal model>`. The GPU's memory limits the size: about 4B with QLoRA on a 16 GB GPU. |
| Recipe | `method` (`lora`/`qlora`), `lora_r`, `lora_alpha`, `learning_rate`, `max_seq_len`, `epochs`, `per_device_batch_size`, `gradient_accumulation_steps` |
| Your own box | `finetune/sft-own-box` |
| Tracking | Optional: `-i report_to=mlflow` with `MLFLOW_TRACKING_URI` set on the control box |

## Known limits

- **Supported scope:** one GPU box per leg, supervised fine-tuning only, text
  only.
- **Same box, unchecked:** cairn does not yet check that `compute_connection`
  matches the profile's `ml_target`
  ([cairn#216](https://github.com/cairndev-sh/cairn/issues/216)). Keep both
  `gpu`.
- **No audit trail by default:** the `finetune-ssh` profile does not bind an
  audit sink yet
  ([cairn-extensions#38](https://github.com/cairndev-sh/cairn-extensions/issues/38)).
  Approvals and run events are still recorded in the run store.
- **Token expiry:** the CLI's login expires and is not renewed. A command that
  meets an expired login, a watch included, prints `session expired at HH:MM — run
  cairn remote login` and exits 77; the pipeline keeps running on the server. Log in
  again and re-attach with `cairn remote pipeline <id>`.
