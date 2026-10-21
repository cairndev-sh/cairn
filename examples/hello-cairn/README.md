# hello-cairn: the example pack

`hello-cairn` is a domain pack: data plus a little glue, no operators. It runs on the cairn
engine and its builtin operator packs alone. Its four pipelines each do one thing a new user
recognises, and together they exercise what the engine adds around a piece of work: gates
answered by data or by a person, budgets, triggers, an agentic template, an MCP tool,
retrieval, a run hook, SSH, secrets, and an audit record.

| Pipeline | Kind | What it does |
|---|---|---|
| `profile-and-publish` | data | Profile a dataset, stop unless the profile passes, redact the summary, run a flow, stage it, ask a person, tag it, publish it, audit it. |
| `ask-the-docs` | agentic | Retrieve from the pack's docs, ask a model, measure the answer with a tool on the pack's own MCP server, remember it, check its length, ask a person. |
| `nightly-ops` | operations | Check a secret, run a report on an SSH box, carry it through a flow, propose the follow-up to a person, audit it. Nightly once enabled; a run hook writes receipts. |
| `tiny-finetune` | ml | Profile training rows, LoRA fine-tune a small model on CPU, gate on its held-out loss, tag it, ask a person, write its card. |

## Quickstart

Run everything from the repository root: relative paths in the pipelines' defaults resolve
against the engine's working directory.

### 1. Install, and point the engine at the example

```bash
uv sync --extra dev --extra hello --extra rag   # add --extra auth if your engine needs it
export CAIRN_PROFILE=examples/hello-cairn/profiles/hello.toml
```

`--extra rag` is what lets the engine index the docs corpus at boot (LlamaIndex ingest);
without it ask-the-docs still runs and retrieves nothing. `auth` is optional: `cairn dev`
signs you in as dev/dev without it.

`cairndev[hello]` installs this pack as the wheel `cairn-pack-hello` plus what its pipelines
reach beyond the base install: the sqlite-vec store (retrieval and memory), asyncssh
(`remote/exec`) and DBOS (`CAIRN_RUNTIME=dbos`). Without installing it,
`export CAIRN_PACK_PATH=examples/hello-cairn` loads the same directory as a dev pack; a dev
pack declares no entry points, so the run-hook listener does not fire.

`profiles/hello.toml` is the deployment profile the example runs under. It carries
`cairn/profiles/local-dev.toml`'s bindings for the builtin operators (memory, storage, rag,
mcp), but not local-dev's `eval.delta` binding (a platform operator) or its `llm_gateway` and
`embedder` backend entries: the model and the embedder come from the engine's environment
(step 3). It adds `secrets.store` → `secrets/env` (nightly-ops' secret check is a capability
node and compiles only with a binding, which local-dev does not carry) and the `hello-calc`
MCP server the pack declares. It pins no runtime, so `CAIRN_RUNTIME=dbos` selects DBOS.

### 2. See what will happen before anything runs

```bash
cairn pipeline explain hello-cairn/profile-and-publish -e sandbox
cairn validate examples/hello-cairn          # with CAIRN_PROFILE set as in step 1
```

`cairn validate` needs the example's profile: under local-dev the nightly-job template does
not compile (no `secrets.store` binding).

`explain` lists every leg with the operator class or flow file it resolves to, the gate that
answers it, and the budget, series and approvers.

### 3. Start an engine

```bash
export OPENAI_API_KEY=sk-...                 # ask-the-docs' model call and embeddings
export RAG_EMBED_PROVIDER=openai             # retrieval and memory embed with OpenAI
export CAIRN_RAG_AUTOINDEX=1                 # index rag/docs at boot (needs the `rag` extra)
export CAIRN_MCP_SERVERS='{"hello-calc": {"transport": "stdio", "command": "python",
  "args": ["examples/hello-cairn/tools/calc_server.py"]}}'
cairn dev --engine-only --port 8471 &        # sqlite engine, in-process runtime, login dev/dev
cairn remote login -u dev -p dev -e http://localhost:8471
```

Any OpenAI-compatible endpoint works in place of a key: `OPENAI_BASE_URL`, or
`LLM_GATEWAY=proxy` with `LLM_GATEWAY_URL`. The in-process `cairn run --mock` dev loop
uses a deterministic model response; governed pipelines still use the configured provider.
`CAIRN_BACKEND=mock` is the telemetry backend, not a model. `profile-and-publish` and
`nightly-ops` call no model.

### 4. Run each pipeline

**profile-and-publish**

```bash
cairn remote launch hello-cairn/profile-and-publish --environment sandbox --watch
cairn remote advance <pipeline_id> review        # the person gate
cairn remote advance <pipeline_id> promote       # the operator's own gate
```

The pipeline stops a second time at `promote`: `artifact/promote` declares
`requires_approval`, and a pipeline can add a gate but never remove one an operator declares.
`cairn remote advance` approves either kind and records the signed-in user as the approver.

The record ends `done`: `data-ok` has verdict `data`, `review` `approved`, `promote` the
tag `hello-sample@published` on the flow leg's registered artifact, and `audit` a
hash-chained row in `CAIRN_AUDIT_LOG` (default `./audit.jsonl`). `storage/put` writes under
`CAIRN_ARTIFACTS_DIR` (default `~/.cairn/artifacts`). With `-i min_rows=1000` the data gate
stops the pipeline (`gate_blocked`) before anything is written. Launch with
`--environment hello-prod` and the pack's policy
(`policies/require-review-on-publish.yaml`) stops `stage` for an approval before it writes.

**ask-the-docs**

```bash
cairn remote launch hello-cairn/ask-the-docs --environment sandbox --watch
cairn remote advance <pipeline_id> review
cairn remote findings <pipeline_id>              # hits, answer, answer_stats, remembered
```

The `assistant` leg (`flows/assistant.md`) searches the `docs` corpus (`rag/docs`, declared
in `pack.toml` as `[requires.rag.docs]`), re-ranks, asks the model, calls `word_count` on
`tools/calc_server.py` through `mcp/call`, and writes the exchange to the `hello_answers`
memory store. `length` flags answers longer than `max_words`. An un-indexed corpus returns no
hits and the model says the docs do not cover the question. Retrieval uses sqlite-vec unless
`RAG_VECTOR_STORE=pgvector`.

**nightly-ops**

```bash
export HELLO_REPORT_TOKEN=...                    # the secret the job checks for
export HELLO_SSH_KEY="$(cat ~/.ssh/id_ed25519)"  # in the ENGINE's environment
CAIRN_STORAGE=sqlite:///$HOME/.cairn/dev.db \
cairn connect add ssh --name hello-ssh --set host=<box> --set user=<user> \
  --set known_hosts=$HOME/.ssh/known_hosts --secret PRIVATE_KEY=HELLO_SSH_KEY
cairn remote launch hello-cairn/nightly-ops --environment sandbox --watch
```

The connection goes into the engine's store, hence `CAIRN_STORAGE`; its secret is an env var
NAME the engine reads. The box needs `sh`, an SFTP subsystem, and `setsid` (Linux) or
`perl` (macOS): `remote/exec` detaches the job into its own session through whichever is
present (#2606). `job` (`flows/nightly-job.md`) checks the secret through the profile's
`secrets.store` binding, then runs `command` on the box and reads back `metrics.json`.
`propose` waits for a person (`monitor/propose` declares `requires_approval`;
`cairn remote advance <pipeline_id> propose`). With the wheel installed, every settled leg
appends a line to `$CAIRN_HOME/hello-cairn/receipts.jsonl` (default `~/.cairn`).

For secrets in Infisical, run the engine with
`CAIRN_PROFILE=examples/hello-cairn/profiles/hello-infisical.toml`, `uv sync --extra
infisical`, and `INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`, `INFISICAL_PROJECT_ID`
(and `INFISICAL_HOST` for a self-hosted server). Nothing in the pipeline changes.

**tiny-finetune**

Needs the builtin train-local pack (inside `cairndev`) and its training stack on the engine's
box: `cairndev[train]` (torch, transformers, peft, datasets; CPU is enough;
there is no torch wheel for Intel macOS).

```bash
cairn remote launch hello-cairn/tiny-finetune --environment sandbox --watch
cairn remote launch hello-cairn/tiny-finetune -i base_model=Qwen/Qwen2.5-0.5B --watch
```

`base_model` defaults to the stand-in `sshleifer/tiny-gpt2` (seconds); Qwen2.5-0.5B trains
in minutes. `loss-ok` stops the pipeline when the held-out loss is above `target_loss`;
`promote` and `review` each wait for a person.

Until the train-local pack was merged (hello-cairn WP3), tiny-finetune was pending in two
strict xfails and one strict list entry. WP3 removed all three in the change that added the
pack: `test_tiny_finetune_is_runnable_on_the_public_core`
(`tests/core/test_example_pipeline_governance.py`) and `test_the_example_pack_validates_clean`
(`tests/cli/test_validate_example_pack.py`, under `profiles/hello.toml`) are ordinary tests,
and `tests/core/test_validate_author_gate.py` admits tiny-finetune like every other shipped
pipeline.

### Triggers

The engine arms every declared trigger when it starts: it installs each discovered pipeline
into the `default` workspace, and each trigger becomes a row that starts its own pipeline
(ADR-0131, #2631). A trigger fires only when its declaration says `enabled: true`; to turn one
on, set that in the pipeline file and restart the engine. Out of the box only nightly-ops' cron
is on. profile-and-publish's dataset `watch` (`on_change: start`), weekly `schedule` and custom
`webhook` (`POST /triggers/hello-sample`) are `enabled: false`. Turned on, the watch makes each
tick check whether `hello-sample` gained a registered version, and starts the pipeline only
when it did. A version is registered by the `engine-check` leg, which records the checked file's content as a version of `hello-sample` (#2632): a changed sample is a new version, and the same sample adds none. With the watch on, a run that registers a new version is followed by one watch-started run on that same content; moving the registration to the job that lands the data is tracked separately. nightly-ops' `table` watch is declared but not armed: a `table` (or `s3`) watch
needs cairn-platform's governed import, so core names it unarmed. Every armed row is pinned to
start a `hello-cairn` pipeline.

### Events

`profile-and-publish`'s `publish` leg declares `emits: dataset.ready`: when it completes,
the bus carries that topic with the leg's output as the payload. A leg declared
`trigger: on:dataset.ready` (in any pipeline) waits for it; `trigger:` also takes `manual`
and `schedule:<cron>` (#2605). On the Postgres bus the event is durable and replayed to a
leg that starts waiting later; on the in-process bus only a leg already waiting sees it.

A whole pipeline can start on a topic too: `ask-the-docs` declares a `kind: event` trigger on
`dataset.ready` (`enabled: false`). Turned on, each published `dataset.ready` starts one
ask-the-docs run (ADR-0131 D4). The listener runs on the engine's leader, so a message
published while no leader is subscribed starts nothing, on either bus. A pipeline may not
start on a topic its own legs emit.

### How it runs inside the engine

[docs/golden-paths/hello.md](../../docs/golden-paths/hello.md) traces each pipeline from the
start door to its legs, names where governance fires, and lists the proof for each feature.

### Seeded demo data

`cairn dev --seed` and `cairn demo` seed three records of profile-and-publish
(`cairn/core/demo_seed.py`). The seeder takes the steps from the loaded pipeline's plan, so
each record carries every step in plan order (`redact`, `promote` and `audit` included):
the published record has all of them done, the blocked one stops at `data-ok`, and the
waiting one stops at `review` with the later steps pending (#2608).

## Files

```
pack.toml                         manifest (kind = domain): the docs corpus, the MCP server
__init__.py                       the one-line shim the entry point targets
pipelines/*.yaml                  the four pipelines (the runnable surface)
flows/engine-check.yaml           data flow: one noop stage
flows/summarise.yaml              data flow: noop + noop.slow
flows/lora.yaml                   ml flow: train.lora
flows/assistant.md                agentic template of ask-the-docs
flows/nightly-job.md              agentic template of nightly-ops (no model)
rag/docs/*.txt                    the docs corpus
data/sample.jsonl                 20 invented rows: id, text, label
data/train.jsonl                  48 invented sentences for the fine-tune
policies/require-review-on-publish.yaml   a hello-prod storage policy
profiles/hello.toml               the example's deployment profile
profiles/hello-infisical.toml     the same, with secrets in Infisical
hooks.py                          run-hook listener: receipts.jsonl
tools/calc_server.py              stdio MCP server: add, word_count
pyproject.toml                    builds the data wheel cairn-pack-hello
```

Tests: `tests/examples/test_hello_cairn.py`, `tests/core/test_example_pipeline_governance.py`,
`tests/cli/test_validate_example_pack.py`. The feature proof (ADR-0127 D1) runs all four pipelines
on real servers in every storage × runtime × bus cell, plus the crash cells: `tests/e2e/hello`,
driven by `scripts/oss/feature_proof.sh`, which writes `FEATURE_PROOF.md`. Its model is a keyless
OpenAI-compatible stub, `tests/e2e/model_stub.py`.
