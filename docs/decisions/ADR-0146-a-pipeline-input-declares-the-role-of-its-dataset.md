# ADR-0146 — A pipeline input declares the role of the dataset version it takes

- **Status:** ACCEPTED 2026-10-22 by the owner (option 1: roles and a required verdict declared
  on pipeline inputs and checked at the launch door; the manifest check leaves
  cairn-extensions#7's acceptance).
- **Date:** 2026-10-22
- **Issue:** cairndev-sh/cairn-extensions#7 (the fine-tune pipeline that takes prepared train,
  val and test versions).
- **Amends:** the `PipelineDef` input spec (an input may carry `role:` and `verdict:`) and the
  pipeline admission decision (#2190, `admission_door.decide_admission`).
- **Keeps:** ADR-0102 D1/D3 (dataset roles, intake verdicts, the holdout door), #2190 (one
  admission decision per launch, before any record exists), ADR-0106 D1 (only a declared
  pipeline runs).

## Context

A fine-tune pipeline takes three registered versions: train, validation and test. The launch
door's holdout check compares the launch's `training` versions with every `test` and `golden`
one, and refuses an overlap; the intake door refuses a version whose verdict is blocked or
invalid. Neither knows which input a version was given for. A test version passed as
`train_version` is not a training version, so the holdout door never compares it, and the run
trains on the test set and is admitted. A training version passed as `val_version` makes the
validation loss a training loss. Resolving a `dataset:` ref yields a location and a hash, not a
role, and an operator pack may not read the registry, so no pack can check this. The intake door
also admits a version that has no verdict at all, with a note; a pipeline that is only meant to
take prepared data has no way to say it needs one.

## Decision

**D1 — An input spec may declare what its version must be.** `role: training | validation | test
| golden` and `verdict: required`, beside `type`, `default` and `description`:

```yaml
inputs:
  train_version: {type: str, role: training, verdict: required}
  val_version:   {type: str, role: validation}
  test_version:  {type: str, role: test}
```

An unknown role, or a `verdict:` other than `required`, fails `parse_pipeline_def`
(`engine.pipeline.dataset_expectations`), so a typo is caught when the pipeline loads instead of
silently checking nothing.

**D2 — The launch door checks them, once, before any record exists.** `decide_admission` takes
the pipeline's expectations (`pipeline_launch` passes them) and, for each declared input with a
value, refuses the launch when:
- the value is not a `dataset:` ref, so it cannot be checked;
- the registry does not hold that ref;
- the version has another role;
- `verdict: required` is declared and the version has no intake verdict.

A registry that cannot answer refuses too. The check is workspace-scoped like the intake and holdout doors: another workspace's version is never read and is reported as "is not registered", the same answer as a name nobody holds, so a refusal tells one tenant nothing about another's data (ADR-0051 §4). An unreadable verdict counts as none. Every check, passed or not, is recorded under
`admission.expectations` on the pipeline record (input, ref, expected and found role and
verdict, `ok`), next to the holdout, intake and licence decisions.

**D3 — Nothing changes for a pipeline that declares nothing.** No expectations means no new
check; the door decides as before.

## Consequences

- `finetune/sft` declares its three inputs. A test version given for training, or a training
  version given for validation, refuses the launch, and the record says which input and why.
- The check is per input; the holdout door's cross-version comparison is unchanged and still runs.
- An input that declares a role must be given a `dataset:` ref: a path or URI cannot be checked
  and is refused. A pipeline that accepts files declares no role.
- The manifest is not checked. `dataset-prep/prepare` registers it as a report on its run, and
  nothing links it to the versions yet.
