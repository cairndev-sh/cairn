---
title: Operators
description: Every operator that ships with cairn, the capabilities it provides, its declared risk, and how it runs, from the operator manifests.
---

An **operator** is the code that does one kind of work. A step asks for a
**capability**, and the environment binds that capability to an operator (see
[Capabilities and environments](/concepts/capabilities/)). This page lists the
22 operators that ship with the open-source core, grouped by the pack that
contains them. The builtin packs ship inside the `cairndev` wheel; the Metaflow
and SkyPilot engines are wheels of their own (the `metaflow` and `skypilot`
extras), and the training stack `train/lora` needs is the `train` extra.

:::note[From the manifests]
Every row comes from the operator package's `pack.toml` manifest
(`packages/cairn-operator-*`). The manifest is what the engine reads, and tests
check that it matches the operator code. `cairn ls operators` prints the same
map for what is installed where you run it.
:::

## Reading the table

**Risk** is declared by the operator's author, and policies can match on it:

| Risk | Means | Policy level |
|---|---|---|
| `read_only` | reads or computes; changes nothing outside cairn | low |
| `external_write` | writes to a system outside cairn (a store, a channel, a repository) | medium |
| `paid_compute` | starts work that costs money (GPUs, paid APIs) | high |
| `production_mutation` | changes something running in production | high |

A policy's `applies_to.risks` uses the levels in the last column.

**Runs as**: `in_process` runs inside the engine; `remote_job` runs off the
engine's process (an engine job, an SSH box) and the run waits for it.

**Notes**: *long-running* reports progress while it works; *has a mock path*
means it can stand in with a stub in mock and sandbox runs; *asks for approval*
means the operator pauses for a person on every call,
wherever it runs (see [Approvals an operator asks for](/concepts/gates/#approvals-an-operator-asks-for)).


## Built in

Inside the `cairndev` wheel; each heading is the pack's directory under `packages/`.

### `cairn-operator-artifact-local`

Local filesystem artifact store — provides storage.put + storage.get capabilities.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `storage/get` | `storage.get` | `read_only` | in_process | has a mock path |
| `storage/put` | `storage.put` | `external_write` | in_process | has a mock path |

### `cairn-operator-audit-store`

Zero-dep hash-chained append-only audit log — provides the audit.sink capability.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `audit/store` | `audit.sink` | `read_only` | in_process | has a mock path |

### `cairn-operator-data-profile`

Semantic data profiler — resolve a workflow's data roles, verdict PASS/DEGRADE/BLOCK (ADR-0074).

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `data/profile` | `data.profile` | `read_only` | in_process | has a mock path |

### `cairn-operator-dataflow-metaflow`

The Metaflow dataflow engine behind dataflow/run and ml/run (cairndev[metaflow]).

No operators: it adds the engine, selected with `dataflow_engine` / `ml_engine` under `[profile.backends]`.

### `cairn-operator-dataflow-skypilot`

The SkyPilot provisioning engine behind dataflow/run and ml/run (cairndev[skypilot]).

No operators: it adds the engine, selected with `dataflow_engine` / `ml_engine` under `[profile.backends]`.

### `cairn-operator-dataflow`

Governed dataflow execution behind one operator (dataflow.run).

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `artifact/promote` | `artifact.promote` | `external_write` | in_process | asks for approval; has a mock path |
| `dataflow/run` | `dataflow.run` | `external_write` | remote_job | long-running; has a mock path |
| `ml/run` | `ml.run` | `external_write` | remote_job | long-running; has a mock path |

### `cairn-operator-mcp`

Universal MCP dispatcher — calls any tool on any configured MCP server.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `mcp/call` | `mcp.call` | `external_write` | in_process | has a mock path |

### `cairn-operator-memory-local`

Local SQLite-vec backed memory store — provides memory.search + memory.write capabilities.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `memory/search` | `memory.search` | `read_only` | in_process | has a mock path |
| `memory/write` | `memory.write` | `production_mutation` | in_process | has a mock path |

### `cairn-operator-monitor-threshold`

Zero-dep signal/threshold detector — provides the monitor.threshold capability.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `monitor/dispatch` | `monitor.dispatch` | `paid_compute` | in_process | has a mock path |
| `monitor/propose` | `monitor.propose` | `read_only` | in_process | asks for approval |
| `monitor/threshold` | `monitor.threshold` | `read_only` | in_process | has a mock path |

### `cairn-operator-rag-local`

Local SQLite-vec backed RAG retrieval — provides rag.search + rag.fetch.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `rag/fetch` | `rag.fetch` | `read_only` | in_process | has a mock path |
| `rag/search` | `rag.search` | `read_only` | in_process | has a mock path |

### `cairn-operator-rag-rerank`

Cross-corpus re-ranking for RAG hits — identity | cross_encoder | cohere.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `rag/rerank` | `rag.rerank` | `read_only` | in_process | has a mock path |

### `cairn-operator-redact-regex`

Zero-dep regex PII/secret redactor — provides the redact.text capability.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `redact/text` | `redact.text` | `read_only` | in_process | has a mock path |

### `cairn-operator-remote`

Run a command/job on any SSH box you own (remote.exec).

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `remote/exec` | `remote.exec` | `external_write` | remote_job, in_process | long-running; has a mock path |

### `cairn-operator-secrets-env`

Env-var SecretStore — provides the secrets.store capability (zero-dep default).

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `secrets/env` | `secrets.store` | `read_only` | in_process | has a mock path |

### `cairn-operator-secrets-infisical`

Infisical SecretStore — provides the secrets.store capability (universal-auth machine identity).

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `secrets/infisical` | `secrets.store` | `read_only` | in_process | has a mock path |

### `cairn-operator-train-local`

LoRA fine-tuning on CPU — provides train.finetune (train/lora, stage train.lora) and train/eval-loss.

| Operator | Provides | Risk | Runs as | Notes |
|---|---|---|---|---|
| `train/eval-loss` | `train.eval_loss` | `read_only` | in_process | has a mock path |
| `train/lora` | `train.finetune` | `external_write` | in_process | has a mock path |

Operators for GPU providers, managed training, evaluation, serving and the
other domains are part of cairn-platform. Your own operators ship as a package
with a `cairn.operators` entry point; `cairn init operator <name>` scaffolds
one.

## Next

- [Capabilities and environments](/concepts/capabilities/): how a step reaches an operator.
- [Gates and approvals](/concepts/gates/#approvals-an-operator-asks-for): operators that ask for approval.
- [CLI](/reference/cli/)
