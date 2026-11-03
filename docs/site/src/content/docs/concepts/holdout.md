---
title: Holdout and eval cutoff
description: The holdout door refuses training data that shares rows with an eval set, before any compute is spent. An eval cutoff says which rows a run may be measured against.
---

If a model trains on rows from its own eval set, the score says little. The **holdout
door** checks for that at launch: when a run names a training dataset version that
shares rows with a test or golden version in the same workspace, the run is refused
before it has a record and before anything is spent.

## See it refuse

[`examples/holdout`](https://github.com/cairndev-sh/cairn/tree/main/examples/holdout)
registers an eval set and two training sets, then launches a run on each. It needs no
key and spends nothing. The leaky set is refused:

```text
admission door: dataset:train-leaky@1 shares 1 row(s) with the test set dataset:eval-set@1 (exact): e1
```

The clean set starts and waits at its gate.

## What the door compares

The door reads every `dataset:` ref in a run's inputs. A version registered with the role
`training` is compared with every `golden` and `test` version in the same workspace.

- **Exact:** a hash of each whole row. This always runs.
- **Near-duplicate:** MinHash over word shingles, which catches a paraphrased row. It
  needs `datasketch`. Without it the door says the pass didn't run, in `near_dup_checked`
  on its record.

A workspace's rows are never compared with another workspace's. A refusal names the
versions and the row ids, and it doesn't include the rows.

## Roles

A version gets its role when it's registered. A flow stage does this with
`register_role`. The role `golden` is refused unless a person has rated every row. See the
[`role:` input](/reference/yaml/) for how a pipeline declares what each version is for.

## Eval cutoff

A run can declare `eval_cutoff`: a date or a timestamp, anywhere in its inputs. The door
then skips holdout versions and rows dated after it, because a model can't have leaked
what didn't exist when it was trained.

- A date alone includes that whole day: `2026-06-01` keeps a row stamped
  `2026-06-01T23:59`.
- A row with no date stays in the comparison.
- A row whose date can't be read stays in the comparison, with a warning.
- A cutoff the door can't read is refused, whatever the data, and the refusal says what
  form it accepts. The run isn't started on an unchecked assumption.

## What it records

A run the door admitted records what it checked in `Run.holdout`: the eval sets, at their
hashes, and any warnings. So you can ask a finished model what it was held out from.

## Where the door doesn't reach

These are real limits, so don't read a clean result as more than it is:

- **A door that can't check lets the run start.** If a version can't be read or the
  registry doesn't answer, the run is admitted, with a warning on its record.
- **Some data isn't compared and still reads as clean.** Rows past the first 200,000 aren't
  compared. A version pointing at a directory with no `.jsonl` files, or at an empty file,
  compares nothing.
- **The door runs at launch and at a template leg's start.** A dataset produced partway
  through a pipeline isn't checked unless it reaches a later template leg. A flow leg has
  no check of its own.
- **Only what's registered counts.** The door compares registered versions, so a path
  that isn't a `dataset:` ref isn't compared.

## Next

- [Gates and approvals](/concepts/gates/): the checks that come after launch.
- [Troubleshooting](/troubleshooting/): what to do about a refusal.
