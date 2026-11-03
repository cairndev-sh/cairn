# `cairn run --check` in CI: copy-paste workflows

`cairn run --check` runs a check **read-only**, prints a machine-readable JSON verdict, and
**exits non-zero on FAIL**, so a bad dataset blocks the PR or merge before it reaches a
training run. It needs no cairn server and no GitHub or GitLab integration. Because
"non-zero exit blocks the pipeline" is universal, the same command works in GitHub
Actions, GitLab CI, Jenkins, a pre-commit hook, or a bare shell.

## Quick start (any shell)

Copy [`templates/dataset-check.md`](templates/dataset-check.md) to `templates/` in your
repository and edit the roles it expects. cairn looks for a bare template name in
`./templates`, so run it from the repository root:

```bash
pip install cairndev
cairn run --check dataset-check --set path=data/train.jsonl --json
echo "exit: $?"   # 0 = PASS, 1 = gate FAIL, 2 = error
```

To try it from this directory on a file that passes:

```bash
cairn run --check dataset-check --set path=../hello-cairn/data/sample.jsonl --json
```

## In GitHub Actions: one `uses:` line

The repository ships a composite action, [`actions/gate`](../../actions/gate/action.yml). It
installs a pinned `cairndev` from PyPI, runs `cairn run --check` and exits with its code:

```yaml
- uses: actions/checkout@v6
- uses: cairndev-sh/cairn/actions/gate@<ref>   # a release tag or a commit SHA
  with:
    check: dataset-check
    set: path=data/train.jsonl
```

Its inputs are `check`, `set` (one `key=value` per line), `fail-on`, `working-directory`,
`cairndev-version` and `python-version`; its outputs are `verdict`, `exit-code` and `report`.
The [guide](https://docs.cairndev.sh/guides/gate-ci/) lists them. Use the workflow below
instead when you want to post the findings on the pull request.

## What the check does

It runs the `data/profile` operator, which ships with `cairndev`, over the file. No model
and no API key are involved. It's a quick check, not a full validation: it reads the first
1000 rows, and a role counts as present if any of them has it.

| The file | Verdict | Exit |
|---|---|---|
| has every required role and every optional one | `PASS` | `0` |
| has every required role, but an optional one is missing | `DEGRADE` | `0`, or `1` with `--fail-on warn` |
| has a required role missing, or has no rows | `BLOCK` | `1` |
| is missing or malformed, or `path` wasn't given | `BLOCK`, "dataset file could not be read" | `1` |

## Writing your own check

`cairn run --check` reads `verdict`, `blocking` and `warnings` from the top of the run's state, so
write them to `$state.verdict`, not `$state.findings.verdict`. It also reads a list named
`outcome` the same way.

**A check that leaves no usable verdict there exits 2,** and so does one with a step that
failed or that paused at an approval. cairndev 0.3.3 and earlier exit 0 in all of these, so
test yours on a missing file. `dataset-check.md` ends in a fallback that blocks, so an unreadable
file is a FAIL with a reason on every version.

A selector recovers from a failed step only when all its children are single steps. With a
compound child, a failed step exits 2 even if a later child recovered. A failure inside a
`subgraph_node` isn't detected yet.

## Exit codes

| code | meaning |
|------|---------|
| `0`  | PASS (or WARN, unless `--fail-on warn`) |
| `1`  | gate FAIL: a blocking check failed |
| `2`  | error: nothing was decided. Bad inputs, the check couldn't be found or compiled, a step failed, it paused at an approval, or it gave no usable verdict |

## Flags

- `-i, --input <file.json>`: typed input values (a JSON object).
- `--set k=v`: inline input override, parsed as JSON when it can be (repeatable). `--input` and `--set` merge.
- `--fail-on {fail,warn}`: the minimum severity that exits non-zero (default `fail`).
- `--json`: emit the machine-readable verdict on stdout (the human summary always goes to stderr).
- `--mock`: run operators in mock mode. A mocked `data/profile` always reports `pass`, so don't use it to gate on data.
- To resolve bindings under a named profile, set `CAIRN_PROFILE`. `--profile` on `cairn run` names a sign-in profile, and `--check` does not use it.

## The JSON verdict (for a PR comment)

From a file with no `text`, `label` or `id` field:

```json
{
  "template": "dataset-check",
  "gate": "fail",
  "verdict": "fail",
  "label": "BLOCK",
  "blocking": ["text", "label"],
  "warnings": ["id"],
  "reasons": []
}
```

## Files here

- [`templates/dataset-check.md`](templates/dataset-check.md): the check.
- [`../../actions/gate`](../../actions/gate/action.yml): the composite action (`uses: cairndev-sh/cairn/actions/gate@<ref>`).
- [`github-actions-gate.yml`](github-actions-gate.yml): GitHub Actions workflow (gates the PR and posts findings with `gh pr comment`).
- [`gitlab-ci-gate.yml`](gitlab-ci-gate.yml): GitLab CI job (gates the MR and posts an MR note).
