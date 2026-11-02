<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./assets/logo-dark.svg">
    <img src="./assets/logo.svg" alt="cairn" width="72">
  </picture>
</p>

<h1 align="center">cairn</h1>

<p align="center">
  <b>An open-source control plane for governed ModelOps.</b><br>
  Fine-tune, evaluate, promote, and serve models as single governed commands —
  cost-capped before the GPU spins up, policy-gated, and audited end to end.
</p>

<p align="center">
  <a href="https://docs.cairndev.sh"><img alt="Docs" src="https://img.shields.io/badge/docs-cairndev.sh-3b82f6"></a>
  <a href="https://cairndev.sh"><img alt="Website" src="https://img.shields.io/badge/website-cairndev.sh-111827"></a>
  <a href="https://demo.cairndev.sh"><img alt="Live demo" src="https://img.shields.io/badge/live%20demo-demo.cairndev.sh-16a34a"></a>
  <a href="https://github.com/cairndev-sh/cairn/actions/workflows/ci.yml?query=branch%3Amain"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/cairndev-sh/cairn/ci.yml?branch=main&label=ci"></a>
  <a href="https://pypi.org/project/cairndev/"><img alt="PyPI" src="https://img.shields.io/pypi/v/cairndev"></a>
  <a href="https://pypi.org/project/cairndev/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/cairndev"></a>
  <a href="./LICENSE"><img alt="License" src="https://img.shields.io/github/license/cairndev-sh/cairn"></a>
</p>

<p align="center">
  <a href="https://docs.cairndev.sh">Docs</a> ·
  <a href="https://demo.cairndev.sh">Live demo</a> ·
  <a href="https://docs.cairndev.sh/install/">Install</a> ·
  <a href="https://docs.cairndev.sh/reference/catalog/">Pipeline catalog</a> ·
  <a href="https://docs.cairndev.sh/concepts/gates/">Gates and approvals</a> ·
  <a href="https://github.com/cairndev-sh/cairn-extensions">Extensions</a>
</p>

---

## What is cairn?

cairn runs the work behind AI as **governed commands**. You declare an
outcome — gate a dataset, fine-tune a model, evaluate it against a baseline,
promote it, serve it — and cairn drives the tools underneath, enforces the
guardrails, and records exactly what ran.

The engine is **domain-neutral**. Everything that knows about a *domain* lives
in a **pack**; everything that touches a *tool* is an **operator**. The engine
never imports a pack — that one-way dependency is what lets the same machinery
run fine-tuning, dataset checks, model evaluation, or incident response, and
lets any operator inherit the budget, policy gate, and audit for free.

cairn is **open source** (Apache 2.0) and **self-hosted**: your keys, your
GPUs, your logs — nothing leaves your perimeter.

## Extensions

Domain packs and the operators they use are separate wheels, released from
[**cairn-extensions**](https://github.com/cairndev-sh/cairn-extensions). Install
the ones you need next to `cairndev`:

| Package | What it adds |
|---|---|
| [`cairn-pack-dataset-prep`](https://github.com/cairndev-sh/cairn-extensions/tree/main/packs/cairn-pack-dataset-prep) | Pin a dataset, audit and clean it, split it by group and register `train` / `val` / `test` versions |
| [`cairn-pack-finetune`](https://github.com/cairndev-sh/cairn-extensions/tree/main/packs/cairn-pack-finetune) | LoRA / QLoRA fine-tuning on a GPU box over SSH, with approvals, a priced budget, base-vs-candidate evaluation and a box that starts after approval and stops on every outcome |
| [Operator packs](https://github.com/cairndev-sh/cairn-extensions#packages) | Dataset checks, training, lm-evaluation-harness benchmarks, IFC validation, and the contract wheels they implement |

```bash
pip install cairndev cairn-pack-finetune
```

Walkthrough: [Fine-tune a model](https://docs.cairndev.sh/guides/fine-tune-a-model/).

## Why governed?

This is what separates cairn from a bare agent loop or a black-box tool — the
work runs behind deterministic guardrails you can inspect afterward:

- **Hard budget** — every run accrues cost in a scope; over the cap it raises
  `BudgetExceeded`, so a fine-tune can't quietly overspend.
- **Policy + human approval** — proposed actions pass declarative policy rules
  before any executor runs; high-risk ones (promote a model, destroy a
  resource) pause for a human and resume durably from a checkpoint.
- **Recorded runs** — every run on an engine stores its inputs, the operator or flow
  it ran, and its events. `cairn status <run-id> --replay` prints them. It doesn't
  execute anything again: it shows what happened, and doesn't reproduce the result.
- **Tamper-evident audit** — with `audit.sink` bound, approvals and every governed
  pipeline step land in a hash-chained, append-only log: what ran, what it cost, who
  approved it. So do direct model completions and `mcp/call` tool calls; a template's own
  prompt and agent nodes don't write a row yet. The quickstart's profile, the
  `secure` profile and the Compose stack bind it; `local-dev` leaves it off.
- **Citation validation** — for synthesized answers, every identifier the model
  cites is exact-string-matched against its context; unmatched citations are
  stripped.

## Proof it works

Every number here comes from a script you can run yourself, and each result records what it was
measured on: see [`benchmark/`](./benchmark/README.md).

<!-- benchmark:begin (generated by benchmark/render.py; do not edit) -->

**The quickstart's path on a local engine** ([reproduce](./benchmark/quickstart_path.py))

| Measured | Result |
|---|---|
| Engine start to answering | 2.0 s |
| Launch to a pipeline paused at its person gate | 1.6 s (median of 5, after one warm-up run) |
| Both approvals to a finished pipeline, nobody waiting | 0.9 s (median of 5, after one warm-up run) |
| Nothing published before the person gate | held on all 6 runs |
| A false data gate stops the pipeline before any later step starts, and is logged | held |
| Each decision a person made is in the audit log with the signed-in user | held for all 12 decisions |
| Audit chain verifies at the end | held (143 rows) |

cairndev 0.3.3, `uv.lock` `43c69f31e985`, Linux x86_64 with 4 CPUs and Python 3.13.13, 2026-11-02.

<!-- benchmark:end -->

## Quickstart

Run a governed pipeline on your machine: it checks a dataset, stops at a gate for
you, and writes a hash-chained audit trail. No API key, no spend, about ten minutes.

```bash
curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_EXTRAS=hello sh   # or: pip install 'cairndev[hello]'
mkdir cairn-quickstart && cd cairn-quickstart
curl -LsSfO https://raw.githubusercontent.com/cairndev-sh/cairn/main/examples/hello-cairn/data/sample.jsonl
curl -LsSfO https://raw.githubusercontent.com/cairndev-sh/cairn/main/examples/hello-cairn/profiles/hello.toml
export CAIRN_PROFILE=$PWD/hello.toml                        # the example's profile: audit trail on

cairn setup --port 8471                                     # local engine on SQLite, signed in as dev
cairn run hello-cairn/profile-and-publish \
  -i path=$PWD/sample.jsonl --environment sandbox
```

The pipeline profiles the file, passes its **data gate** (the profile must pass and
count at least 10 rows), redacts and versions the file, and stops at `review`, a
**person gate**:

```text
  paused
    profile            done                 run=run-task-data--profile-1f9a14
    data-ok            done                 run=None
    redact             done                 run=run-task-redact--text-85ecfb
    engine-check       done                 run=run-flow-hello-cairn--engine-check-55b570
    stage              done                 run=run-task-storage--put-1295cd
    review             awaiting_approval    run=None
    promote            pending              run=None
    publish            pending              run=None
    audit              pending              run=None
  gate review  (approvers: data-owners)
    profile.verdict       pass
    profile.record_count  20
    …
→ approve   cairn decide pl_3f5b6ec6a25d414d approve --step review --approver you
→ findings  cairn status pl_3f5b6ec6a25d414d --results
→ watch     cairn status pl_3f5b6ec6a25d414d --follow
Approve this step? [a]pprove / [r]eject / [s]kip (detach)
```

Answer `a`, then `a` again at `promote`, the gate the operator itself adds. With `s`, or with
no terminal to ask on, the pipeline stays waiting, and these answer from any shell. The last
one reads the record:

```bash
cairn decide <pipeline_id> approve --step review     # the person gate
cairn decide <pipeline_id> approve --step promote    # the operator's own gate
cairn status <pipeline_id> --results                 # what every leg produced
```

```text
pipeline pl_3f5b6ec6a25d414d  status=done
  …
  promote  [done] run=run-task-artifact--promote-3b4d2f
    artifact_id           sha256:d2c09d27b2874cee
    tag                   hello-sample@published
  audit  [done] run=run-task-audit--store-39b32a
    hash  af362a96212cf0d5298dfe71e81b5109d08a0219f3079387550e753f2fa25466
```

`cairn status <pipeline_id> --json` prints the whole record as JSON: the plan, the
inputs, and every step's state and decision. The audit trail is in `./audit.jsonl`:
rows for the launch, each step, each decision a person made, with the signed-in user
(`pipeline.gate_approved`), and the row the pipeline's own `audit` step writes. Each
row is chained to the one before it by its hash, and `cairn admin audit verify` checks the
file. Try `-i min_rows=1000` to watch the data gate stop the pipeline before any later
step starts. The full walkthrough is the [Quickstart](https://docs.cairndev.sh/quickstart/);
to look around without installing, use the live demo at https://demo.cairndev.sh (a
shared sandbox, so don't enter real credentials).

To watch a budget refuse a run, with no key and no spend, start a pipeline whose estimate
is above its cap:

```bash
cairn run hello-cairn/over-budget --environment sandbox
```

```text
pipeline_id=pl_592c6c850aec40a5 status=running
  failed
    engine-check       failed               run=run-flow-hello-cairn--engine-check-78f6c9
  engine-check: series 'hello-over-budget': this run's estimate $5.00 would exceed the $1.00 cap ($0.00 spent, $0.00 reserved by in-flight runs). Raise the budget or start a new series.
→ logs      cairn status run-flow-hello-cairn--engine-check-78f6c9 --logs   # leg engine-check
→ findings  cairn status pl_592c6c850aec40a5 --results
```

The flow leg's reservation failed, so its engine never started, and the command exits 1. The
$5 is a figure the pipeline file declares, not a measured price, and the flow itself would cost
nothing. The refusal is the real one every flow leg under a `series` goes through. This pipeline
needs the release after cairndev 0.4.0. See [Cost and budgets](https://docs.cairndev.sh/concepts/cost/).

From source:

```bash
git clone https://github.com/cairndev-sh/cairn && cd cairn
uv sync --extra dev
CAIRN_PACK_PATH=examples/hello-cairn uv run cairn run --dry-run hello-cairn/profile-and-publish
```

The engine ships as the `cairndev` distribution (import name `cairn`), carrying the
builtin operator packs and the core contracts; the container image is built from
`Dockerfile.engine` and the Helm chart lives under `deploy/helm/cairn`. Optional
extras and team setup: [Install and deploy](https://docs.cairndev.sh/install/) ·
[CLI reference](https://docs.cairndev.sh/reference/cli/).

## The ModelOps lifecycle

Each stage is a normal governed run; chained together they form one auditable
spine — as a template's subgraphs, or as an explicit
[pipeline](https://docs.cairndev.sh/concepts/pipelines/).

```mermaid
flowchart LR
  D[Gate dataset] --> P[Prep · scrub · split]
  P --> T[Train · local or remote GPU]
  T --> E[Evaluate · deterministic gates]
  E --> H{{Human approval}}
  H --> R[Register · MLflow]
  R --> S[Serve · vLLM]
```

Deterministic gates decide pass/fail; LLM judges are advisory and off by
default. Promotion pauses for a human. The shipped pipelines are listed in the
[pipeline catalog](https://docs.cairndev.sh/reference/catalog/).

## One engine, many surfaces

The same governed core is driven from two places in this repository:

- **CLI** — the full control surface: run, preflight, replay, approve, deploy.
  → [CLI reference](https://docs.cairndev.sh/reference/cli/)
- **HTTP + SSE API** — every control-plane action is an endpoint, including live
  run streaming.

## Documentation

| | |
|---|---|
| [Overview](https://docs.cairndev.sh/) | What cairn is and the governance thesis |
| [Architecture](./docs/architecture/00-overview.md) | Engine, packs, profiles, request lifecycle |
| [Write a pipeline](https://docs.cairndev.sh/guides/write-a-pipeline/) · [Capabilities and operators](https://docs.cairndev.sh/concepts/capabilities/) | Packs, and how operators pick up a provider |
| [Gates and approvals](https://docs.cairndev.sh/concepts/gates/) · [Cost and budgets](https://docs.cairndev.sh/concepts/cost/) | Policies, HITL gates, budget, audit |
| [Pipelines](https://docs.cairndev.sh/concepts/pipelines/) | Chain governed workflows with gates and events |
| [Install and deploy](https://docs.cairndev.sh/install/) | Install, Docker Compose, Helm, sign-in |
| [Operators catalog](https://docs.cairndev.sh/reference/operators/) | Every shipped operator, by domain |

## Contributing

Start with [`CONTRIBUTING.md`](./CONTRIBUTING.md): the dev loop, where each kind of test
lives, and the DCO sign-off every commit needs. Self-contained starting points are
labelled [`good first issue`](https://github.com/cairndev-sh/cairn/labels/good%20first%20issue).

<a href="https://github.com/cairndev-sh/cairn/graphs/contributors">
  <img alt="Contributors" src="https://contrib.rocks/image?repo=cairndev-sh/cairn">
</a>

## Status & license

cairn is in **early access** and under active development. The API and command
surface may still change between releases; see [`CHANGELOG.md`](./CHANGELOG.md).

Licensed under the **Apache License 2.0** — see [`LICENSE`](./LICENSE).
Contributions are welcome: read [`CONTRIBUTING.md`](./CONTRIBUTING.md) first.
