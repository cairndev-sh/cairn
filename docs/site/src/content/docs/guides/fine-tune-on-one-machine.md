---
title: Fine-tune with governance on one machine
description: Run a governed fine-tune end to end on one machine, with no cloud account and no API key. Prepare a dataset, train a LoRA adapter, evaluate it on held-out data, approve and promote it, then check the record and the audit chain.
---

This guide runs a complete governed fine-tune on one machine: your laptop,
a Linux box, or a Linux box with an NVIDIA GPU. It needs no cloud account,
no API key and no GPU. One script installs everything into a virtualenv, a
second runs it:

```
data prep → LoRA train → held-out eval → automatic gate → your approval → promote
```

What you see along the way:

- **A person approves the data source** before the data is used.
- **The dataset is versioned with roles.** `train`, `val` and `test` are
  registered separately, and the launch refuses a test set given as training
  data.
- **A person approves the training run** before any training starts.
- **An automatic gate** stops the run if the trained model is not better
  than the base model on validation loss.
- **A person approves promotion,** with the losses in front of them.
- **Every decision is recorded,** who and when, and the hash-chained audit
  log verifies.

The scripts and packs live in
[`cairndev-sh/cairn-extensions`](https://github.com/cairndev-sh/cairn-extensions)
([`demos/laptop/`](https://github.com/cairndev-sh/cairn-extensions/tree/main/demos/laptop)).
For a GPU box that cairn starts and stops for you, see
[Fine-tune a model on a GPU box](/guides/fine-tune-a-model/).

## Before you start

- **A machine:** macOS on Apple silicon, or Linux (CPU, or an NVIDIA GPU,
  which the training then uses). Not an Intel Mac: PyTorch publishes no
  macOS x86_64 wheels for the torch version the demo pins.
- **[uv](https://docs.astral.sh/uv/).** It fetches Python 3.12 if you have
  another version.
- **About 5 GB of disk,** mostly torch. On a Linux machine without a GPU,
  `TORCH_INDEX_URL=https://download.pytorch.org/whl/cpu` selects the much
  smaller CPU build.
- **Network** to PyPI, and to the Hugging Face Hub for the default model
  (about 270 MB, once). With `--tiny` the run needs no Hub access.

## 1. Install

```bash
git clone https://github.com/cairndev-sh/cairn-extensions && cd cairn-extensions
demos/laptop/install.sh
```

This creates `demos/laptop/.venv` with:

- the released core, `cairndev`;
- the contracts, the dataset-checks, training and eval operators, and the
  `dataset-prep` and `finetune` packs, from this checkout;
- `moto`, a local S3 stand-in. The registered dataset versions and the
  adapter go through it, so nothing leaves your machine.

It ends by printing the versions and the device it found:

```
  cairndev 0.3.2
  cairn-operator-training 0.1.2
  ...
  accelerator: cuda        # or mps, or cpu
```

## 2. Run it

```bash
demos/laptop/run.sh
```

`run.sh` starts the local S3 and the control plane (`cairn dev --engine-only`)
on `127.0.0.1`, signs in as `dev`, and runs two pipelines. At each human gate
it prints what the person is shown and how to decide:

```
  A person is asked:
  gate approve-run (approvers: ml-owners)
    input.train_version  dataset:laptop-demo-train@1
    input.base_model     HuggingFaceTB/SmolLM2-135M
    input.method         lora
    ...
  Press Enter to approve here as dev, type n to cancel the pipeline, or run the command above in another terminal.
```

Press Enter to approve. Add `--auto-approve` to answer every gate as `dev`
and watch the whole path; the gates still run and the approver is still
recorded. Add `--tiny` to train a tiny offline model instead (about 2.5
minutes on 4 CPU cores; the numbers mean nothing, but the path is real).

### What runs

**1. `dataset-prep/prepare`** on the pack's 200-row sample of IFC/BIM
instructions:

| Step | What happens | Who decides |
|---|---|---|
| `source-ok` | you are shown the source path and the split ratios | a person |
| `prepare` | normalise, drop exact duplicates, split by group, register `laptop-demo-train`, `-val` and `-test` with their roles and intake verdicts | |
| `data-review` | a person reviews the run when dedup drops a large share of the source; skipped on the sample | a person, only when needed |
| `data-ok` | the registered versions pass their checks | the data |

**2. `finetune/sft-local`** on those three versions:

| Step | What happens | Who decides |
|---|---|---|
| `approve-run` | you are shown the versions, the base model and the method | a person |
| `smoke` | a short LoRA run on a 16-row subsample, the whole path once | |
| `smoke-ok` | the smoke run ended in a packaged adapter | the data |
| `train` | the full LoRA run (30 steps), with the base model's validation loss as the baseline | |
| `eval` | the base model and the candidate scored on held-out `val` and `test` with the same settings | |
| `improved` | the candidate's validation loss is below the base model's, or the run stops here | the data |
| `promote-model` | you are shown the losses | a person |
| `promote` | the adapter is tagged `sft-adapter@candidate` | a person |

## 3. Read the result

From a run on a Linux machine with an NVIDIA T4 (`--auto-approve`, the
default `SmolLM2-135M`, 228 seconds in all):

```
==> result
  pipeline                              pl_fc3b5926f4514c3f  done
  base model                            HuggingFaceTB/SmolLM2-135M
  optimizer steps                       30
  validation loss, base model           3.7713
  validation loss, candidate            3.4701
  held-out val loss  base -> candidate  4.5644 -> 4.2746
  held-out test loss base -> candidate  4.5305 -> 4.2469
  eval device                           cuda:0
  promoted                              sft-adapter@candidate -> cf8117fb9835c873…

==> who decided what (the pipeline records, the durable trail of the gates)
  pl_87d0d1caa6c04e41  source-ok      approved by dev    asked 13:09:07  decided 13:09:19
  pl_fc3b5926f4514c3f  approve-run    approved by dev    asked 13:09:36  decided 13:09:49
  pl_fc3b5926f4514c3f  promote-model  approved by dev    asked 13:11:33  decided 13:11:45
  pl_fc3b5926f4514c3f  promote        approved by dev    asked 13:11:45  decided 13:12:00

==> audit log: .../audit.jsonl
    29 event(s) written by the audit sink
    cairn lineage --json | .chain_ok  ->  True
```

The test loss is measured on rows the model never trained on, and the base
model is scored the same way, so the two numbers are comparable. A CPU or
an Apple-silicon Mac prints the same table with `eval device cpu` or `mps`
and its own numbers.

If the `improved` gate stops the run, the candidate did not beat the base
model on validation loss. That is the gate working; try more steps with
`-i max_steps=100`.

## 4. Check the record yourself

`run.sh` stops its servers when it ends, but keeps the work directory (its
path is printed at the start and the end): the store, the audit log and the
logs. To read the record afterwards, start the control plane again on that
store:

```bash
export LAPTOP_DEMO_WORK=<the work directory run.sh printed>
. demos/laptop/env.sh
cairn dev --engine-only --port "$LAPTOP_DEMO_PORT" --db "$LAPTOP_DEMO_WORK/cairn.db" &
cairn remote login -u dev -p dev -e "$CAIRN_URL"
cairn remote pipeline <pl_id>          # the durable record: plan, step states, admission, approvals
cairn remote findings <pl_id>          # every step's findings: losses, steps, adapter, versions
cairn audit verify --audit-log "$CAIRN_AUDIT_LOG"
```

`cairn audit verify` needs no server. It recomputes the hash chain over every
event (the admission, each gate decision with its actor, each leg's submit
and finish, the end of the pipeline) and prints `chain OK`, or
`chain BROKEN at row N` with the reason when a line was changed or removed.

### Reject instead of approve

A gate can be rejected with a reason instead of approved. Run `run.sh`
without `--auto-approve`, and when it waits at `approve-run`, reject from a
second shell (with the same `LAPTOP_DEMO_WORK`, so `env.sh` points at the
same control plane):

```bash
export LAPTOP_DEMO_WORK=<the work directory run.sh printed>
. demos/laptop/env.sh
cairn remote reject <pl_id> approve-run --reason "wrong base model"
```

The pipeline fails at that gate; the record shows `verdict: rejected` with
the approver, and the audit log has the rejection. `run.sh` prints why and
stops.

### See the run as one trace

Each pipeline run is one OpenTelemetry trace: the pipeline, each gate, each
leg and its stages are spans in it. Tracing is off until an exporter is
configured. To print the spans to the control plane's log:

```bash
OTEL_CONSOLE=1 demos/laptop/run.sh --auto-approve
```

To send them to a collector (Tempo, Jaeger, Langfuse), set
`OTEL_EXPORTER_OTLP_ENDPOINT` and install `cairndev[telemetry]` in the
virtualenv. The record's `trace_context` holds the trace's id.

## What to change

| | |
|---|---|
| Model | `--base-model <Hugging Face id or local directory>` |
| Steps and recipe | `-i max_steps=100 -i learning_rate=0.0001` (any `finetune/sft-local` input) |
| Ports and work directory | `LAPTOP_DEMO_PORT`, `LAPTOP_DEMO_S3_PORT`, `LAPTOP_DEMO_WORK` |
| Your own data | `cairn remote launch dataset-prep/prepare -i path=<your.jsonl> -i register=<name>`, then `finetune/sft-local` with the three versions it registers |
| A real GPU box | [Fine-tune a model on a GPU box](/guides/fine-tune-a-model/) |

## Known limits

- **Nothing is priced on one machine.** A local leg has no hourly rate, so
  the launch estimates $0 and the pipeline's budget is what its author
  declared.
- **Cancel does not stop a trainer that is already running.** Stop the
  process yourself.
- **Not a production setup:** one process, a throwaway store, and the
  adapter stays in the leg's temporary directory.
- **4-bit QLoRA needs the GPU trainer.** The demo trains plain LoRA. QLoRA
  (`-i method=qlora`) needs `cairn-operator-training[gpu]` on a CUDA machine,
  as in the GPU-box guide.
- **macOS virtual machines** (hosted CI runners) report an MPS device that
  cannot allocate memory. Use a physical Mac or Linux.
