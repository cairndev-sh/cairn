---
title: Gate CI on a check
description: Run a read-only check in CI with cairn run --check, fail the build on a bad dataset, read the exit codes, and know what to do when a check exits 2.
---

A bad dataset is cheapest to catch in the pull request that changes it. `cairn run --check`
runs a **check** over a file, prints a verdict, and exits non-zero when the verdict is bad,
so the build stops before a training run starts. It needs no engine, no server and no
GitHub or GitLab integration: a non-zero exit blocks a job in any CI system, a pre-commit
hook or a bare shell.

You need `cairndev` 0.4.1 or later: before that, a check that didn't finish exited 0. This guide uses the check in
[`examples/ci`](https://github.com/cairndev-sh/cairn/tree/main/examples/ci), which runs on
the core alone: no model, no API key.

## What a check is

A check is a template, a file in `templates/`, that reads something and decides a verdict.
It is not a pipeline: it has no legs, no gates that ask a person and no record. `--check`
compiles it, runs it once in this process, writes nothing to a store, and exits.

The example, `dataset-check`, runs the `data/profile` operator over a JSONL file and asks for
three roles: `text` and `label` are required, `id` is optional. A role is a name with
aliases, not a column name, so a `body` column counts as `text`. It reads the first 1000
rows and counts a role as present if any of them has it.

| The file | Verdict | `label` | Exit |
|---|---|---|---|
| has every required role and every optional one | pass | `PASS` | `0` |
| has every required role but misses an optional one | warn | `DEGRADE` | `0`, or `1` with `--fail-on warn` |
| misses a required role, or has no rows | fail | `BLOCK` | `1` |
| can't be read, or `path` wasn't given | fail | `BLOCK` | `1` |

The last row is on purpose. `dataset-check` ends in a second step that blocks when the file
can't be read, so a missing file is a failure with a reason, not an error.

Set up a repository with the same layout `examples/ci` has: copy
[`dataset-check.md`](https://github.com/cairndev-sh/cairn/blob/main/examples/ci/templates/dataset-check.md)
to `templates/dataset-check.md` and edit the roles. A bare check name is looked up in
`./templates`, relative to where you run the command, so run it from the repository root.
A namespaced ref, `<pack>/<name>`, resolves against an installed pack instead.

## 1. Run it

From `examples/ci`, on a file that passes:

```text
$ cairn run --check dataset-check --set path=../hello-cairn/data/sample.jsonl --json
✓ gate PASS · dataset-check
→ exit 0
{"template": "dataset-check", "gate": "pass", "verdict": "pass", "label": "PASS", "blocking": [], "warnings": [], "reasons": []}
$ echo $?
0
```

On a file that isn't there:

```text
$ cairn run --check dataset-check --set path=data/missing.jsonl --json
✕ gate FAIL · dataset-check
  ⛔ dataset file could not be read
→ exit 1
{"template": "dataset-check", "gate": "fail", "verdict": "fail", "label": "BLOCK", "blocking": ["dataset file could not be read"], "warnings": [], "reasons": []}
$ echo $?
1
```

The summary for a person goes to stderr. The JSON goes to stdout, and only with `--json`, so
`> gate.json` captures it cleanly. The `-i key=value` form of `--set` does the same, and
`-i @inputs.json` reads a JSON object of inputs. Values are parsed as JSON when they can be.

| Field | Is |
|---|---|
| `verdict` | what the check decided: `pass`, `warn` or `fail` |
| `label` | the word the check wrote, upper-cased: `PASS`, `DEGRADE`, `BLOCK` |
| `gate` | what the exit code says: `pass` or `fail` |
| `blocking`, `warnings` | the reasons the check listed |
| `reasons` | failed or warned items of a check that grades an `outcome` list |

`gate` and `verdict` differ when `--fail-on warn` is set. On a file that has `text` and
`label` but no `id`:

```text
$ cairn run --check dataset-check --set path=partial.jsonl --json
⚠ gate WARN · dataset-check
  ⚠ id
→ exit 0
{"template": "dataset-check", "gate": "pass", "verdict": "warn", "label": "DEGRADE", "blocking": [], "warnings": ["id"], "reasons": []}

$ cairn run --check dataset-check --set path=partial.jsonl --fail-on warn --json
⚠ gate WARN · dataset-check
  ⚠ id
→ exit 1
{"template": "dataset-check", "gate": "fail", "verdict": "warn", "label": "DEGRADE", "blocking": [], "warnings": ["id"], "reasons": []}
```

## 2. Read the exit code

| Code | Meaning |
|---|---|
| `0` | pass, or warn unless `--fail-on warn` |
| `1` | fail: the check found a reason to block, or a warning with `--fail-on warn` |
| `2` | error: **nothing was decided** |

`--fail-on` takes `fail` (the default) or `warn`. Anything else exits 2 before the check
runs.

Exit 2 is not a failing dataset. It means the check couldn't give an answer, and CI should
treat it as a failure all the same: a gate that passes when it didn't run protects nothing.
It is returned when:

- the check can't be found or compiled, or the inputs are bad;
- a step of the check failed;
- the check paused at an approval, which nothing in CI can answer;
- the check gave no verdict, a verdict that isn't a pass, a warning or a failure, or hit its
  budget before it gave one.

A failing verdict stands even if a step also failed, so that case exits 1.

Two options of `cairn run` don't apply here. `--profile` names a sign-in profile and
`--check` doesn't read it; to resolve bindings under a named profile, set the `CAIRN_PROFILE`
environment variable. And `--mock` runs the check's operators in mock mode, where
`data/profile` always reports a pass, so don't use it to gate on data.

## 3. A GitHub Actions job

This job runs the check on every pull request that touches `data/` and fails the build when
the check exits non-zero:

```yaml
name: dataset check

on:
  pull_request:
    paths: ['data/**']

jobs:
  dataset-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install cairndev
      - run: cairn run --check dataset-check --set path=data/train.jsonl
```

GitHub runs a `run` step with `bash -e`, so a non-zero exit fails the step and the job, and
the summary on stderr is in the log. The checkout puts `templates/dataset-check.md` where the
command looks for it. Add `--fail-on warn` to block on warnings too, and make the job a
required status check in the branch protection rules so a red job blocks the merge.

To post the findings on the pull request as well,
[`examples/ci/github-actions-gate.yml`](https://github.com/cairndev-sh/cairn/blob/main/examples/ci/github-actions-gate.yml)
does it in three steps:

1. Run the check with `set +e`, send the JSON to `gate.json` and keep the exit code in a step
   output, so the job doesn't stop yet.
2. With `if: always()`, build a comment from `gate.json` with `jq` and post it with
   `gh pr comment`. The job needs `pull-requests: write` for this.
3. Exit with the kept code, so the job still fails when the check did.

The token of a pull request from a fork can't write comments, so on a public repository the
comment step fails there, and with it the job, whatever the check said.
[`gitlab-ci-gate.yml`](https://github.com/cairndev-sh/cairn/blob/main/examples/ci/gitlab-ci-gate.yml)
does the same for a GitLab merge request.

## 4. When a check exits 2

With `--json`, an error has no `verdict`. It has `gate: "error"` and the reason in `error`;
the same reason goes to stderr.

A reference to a check that isn't in `./templates`:

```text
$ cairn run --check no-such-check --json
✗ gate ERROR · no-such-check: FileNotFoundError: Template 'no-such-check' not found in any search path: [PosixPath('templates')]
{"template": "no-such-check", "gate": "error", "error": "FileNotFoundError: Template 'no-such-check' not found in any search path: [PosixPath('templates')]"}
$ echo $?
2
```

A check that ran but never wrote `$state.verdict`:

```text
✗ gate ERROR · noverdict: the check gave no usable verdict: it must write `$state.verdict` or a graded `$state.outcome`
{"template": "noverdict", "gate": "error", "error": "the check gave no usable verdict: it must write `$state.verdict` or a graded `$state.outcome`"}
```

Read the reason, then:

| The reason says | Cause | Fix |
|---|---|---|
| `Template '…' not found in any search path` | the command ran outside the directory that holds `templates/`, or the file isn't checked in | add `actions/checkout`, run from the repository root, check the name |
| `--fail-on must be 'fail' or 'warn'` | a typo in the flag | fix it; no JSON is printed |
| `a step failed: …` | an operator of the check raised; the text after it is the operator's error | fix what it names, such as a missing file, a missing extra or a pack that isn't installed; test the check on a missing file |
| `the check paused at an approval` | the check runs something that needs a person | keep approvals out of a check; run that part as a pipeline |
| `the check gave no usable verdict` | no `$state.verdict`, and no graded `$state.outcome` | write one, see below |
| `the check's verdict '…' isn't a pass, a warning or a failure` | the verdict is a word cairn can't classify | write `pass`, `warn` or `fail` |
| `the check hit its budget before giving a verdict` | a budget ran out first | raise it or make the check cheaper |
| `the last step ended failure` or `rejected` | the final step reported failure | look at the step; a selector recovers only if all its children are single steps |

Don't turn an exit 2 into a pass with `|| true` or `continue-on-error`. If a check keeps
exiting 2 on data you know is fine, the check is wrong, and the fix belongs in the check.

### Writing a check

`--check` reads `verdict`, `blocking` and `warnings` from the top of the run's state, so
write them to `$state.verdict`, not `$state.findings.verdict`. It also reads a list named
`outcome` the same way: a head item named `readiness`, `verdict` or `gate` carries the
verdict, and with no head the items' `severity` and `pass` values decide. Words containing
`fail`, `block` or `reject` are failures, `warn` or `degrad` are warnings, and `pass`,
`passed`, `ok`, `ready`, `success`, `succeeded` and `clean` are passes. The
`output:` block of `dataset-check` shows the wiring:

```yaml
output:
  verdict: $state.verdict
  missing_required: $state.blocking
  degraded: $state.warnings
```

Before you rely on a check, run it on a file that doesn't exist and on one that fails, and
confirm each exits the way you expect. A failure inside a `subgraph_node` isn't detected yet,
so keep a check to plain steps.

## What was recorded

Nothing in cairn's store. `cairn run --check` compiles the check, runs it once
in the CLI's own process and exits: it writes no pipeline record, no run, no
artifact and no run trace. A check is a template, not a pipeline, so it has no legs, gates or
`pl_…` id to read back with `cairn status`.

What is left is what you keep from the call:

- **The exit code**: `0`, `1` or `2`. The CI job's status is its record.
- **The summary on stderr**, which is in the job log.
- **The JSON on stdout**, only with `--json`. It is gone unless you keep it. Redirect
  it (`> gate.json`) and keep the file with your CI system's own artifact feature:
  `examples/ci/gitlab-ci-gate.yml` lists `gate.json` under `artifacts:`.

**Audit rows.** None from `dataset-check`. The rows in
[What a pipeline run writes](/concepts/runs/#what-a-pipeline-run-writes) come
from the launch door and the pipeline's legs, which `--check` never goes
through, and `dataset-check` makes no model call, `mcp/call` call or approval.
A check of your own that calls `mcp/call`, or an operator that makes a direct
model completion, writes that call's row when `CAIRN_PROFILE` binds `audit.sink`.
A template's own prompt and agent nodes write none.

To keep a record of what a dataset was when it was checked, run the same check
as a leg of a pipeline, as
[Run your first pipeline](/guides/first-pipeline/) does with `data/profile`.

## Next

- [Run your first pipeline](/guides/first-pipeline/): the same data check as a governed pipeline with a gate.
- [Gates and approvals](/concepts/gates/): who decides when a pipeline waits.
- [CLI reference](/reference/cli/): every `cairn run` option.
