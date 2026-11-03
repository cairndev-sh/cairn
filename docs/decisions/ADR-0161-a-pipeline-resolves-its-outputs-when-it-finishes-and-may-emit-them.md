# ADR-0161 — A pipeline resolves its outputs when it finishes, and may emit them as an event

- **Status:** ACCEPTED 2026-10-30 by the owner.
- **Date:** 2026-10-30
- **Issue:** cairndev-sh/cairn#293
- **Amends:** the pipeline YAML (`outputs:` gains runtime meaning; a pipeline-level `emits:`),
  `PipelineRecord` (an additive `outputs` field), `GET /pipelines/{id}`.
- **Keeps:** a step's `emits:` (#2605) unchanged; ADR-0131 D4–D6 (event triggers, no pipeline
  starting on a topic it emits); ADR-0084 D5 (a pack never references another pack's
  executables); the expression grammar of `predicate:` and `show:`.

## Context

A pipeline cannot hand its results to another pipeline when it finishes.

- `outputs:` is parsed and documented as "Names this pipeline's results" (`reference/yaml.md`),
  but nothing resolves it, stores it or shows it: the plan and the record never carry it. Six
  pipelines in cairn-extensions declare it.
- `emits:` exists only on a task or flow step, and publishes that step's output when the step
  finishes. A gate cannot emit, so an event can only leave before the pipeline's last gate.

The laptop demo chains `dataset-prep/prepare` to `finetune/sft-local` through an event
(ADR-0131 D4). The event must leave only after `prepare`'s `data-ok` gate, and carry the three
registered versions as `dataset:` refs by role. Without this ADR that takes a new operator whose
only job is to reshape and emit; the owner does not want a demo-only operator released.

## Decision

**D1 — `outputs:` are resolved when the pipeline ends `done`.** Each value is an expression in the
grammar `predicate:` and `show:` use, over `steps` and `input`; the existing `<step>.<field>`
shorthand keeps its meaning. They are resolved once, when the pipeline settles `done`, after its
finalizers. The result is stored on the record (`PipelineRecord.outputs`, additive, in the JSON
blob, no migration) and returned by `GET /pipelines/{id}`. An expression that does not resolve
is recorded as an error on the record and never fails a pipeline that has finished.

**D2 — A pipeline-level `emits: <topic>` publishes the resolved outputs.** Once, when the
pipeline ends `done`, through the same bus publish a step's `emits:` uses
(`publisher: pipeline:<id>`, `correlation_id: <id>`), so a `kind: event` trigger maps its inputs
from `$event.<output name>`. Never on `failed` or `cancelled`. The topic grammar is a step's. A
pipeline still cannot start on a topic it emits (ADR-0131 D6 extends to this `emits:`).

**D3 — Both owners.** The in-process/DBOS bridge and the Temporal workflow resolve and emit
through one shared helper (`leg_hooks.py`), as the other leg side-channels do.

## Consequences

- The laptop demo is YAML only: `prepare` declares
  `outputs: {train_version: "join('', ['dataset:', steps.prepare.findings.dataset_versions[?contains(@, '-train@')] | [0]])", …}`
  and `emits: dataset.prepared`; `sft-local` declares the event trigger. No new operator.
- The six pipelines that already declare `outputs:` start showing them. A declaration that never
  resolved shows its error instead of nothing.
- A pipeline's result becomes readable without reading its steps' findings.

## Alternatives rejected

- **A new operator after the last gate** that reshapes and emits: code whose only job is plumbing,
  released with an operator wheel.
- **Let a gate step emit:** a gate has no output of its own, so the payload would still need a
  mapping, which is what `outputs:` already is.
- **Map the shape in the trigger** (`$event…` with expressions): the consumer would have to know
  the producer's internals, and every consumer would repeat the mapping.
