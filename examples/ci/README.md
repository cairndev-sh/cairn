# `cairn gate` in CI — copy-paste workflows

`cairn gate` runs a check workflow **read-only**, prints a machine-readable JSON
verdict, and **exits non-zero on FAIL** — so a bad dataset (or model) blocks the
PR/merge before it ever reaches a training run. It needs no cairn server and no
GitHub/GitLab integration: `pip install cairndev && cairn gate …`. Because
"non-zero exit blocks the pipeline" is universal, the same verb works in GitHub
Actions, GitLab CI, Jenkins, a pre-commit hook, or a bare shell.

## Quick start (any shell)

```bash
pip install "cairndev[pii]"
cairn gate dataset-readiness/check --set path=data/train.jsonl --json
echo "exit: $?"   # 0 = PASS, 1 = gate FAIL, 2 = error
```

## Exit codes

| code | meaning |
|------|---------|
| `0`  | PASS (or WARN, unless `--fail-on warn`) |
| `1`  | gate FAIL — a blocking check failed |
| `2`  | error — bad inputs, or the workflow couldn't run |

## Flags

- `-i, --input <file.json>` — typed input values (a JSON object).
- `--set k=v` — inline string input override (repeatable). `--input` + `--set` merge.
- `--fail-on {fail,warn}` — the minimum severity that exits non-zero (default `fail`).
- `--json` — emit the machine-readable verdict on stdout (the human summary always goes to stderr).
- `--mock` — run operators in mock mode for secretless CI (no real backends).
- `-p, --profile <name>` — resolve bindings under a named environment/profile.

## The JSON verdict (for a PR comment)

```json
{
  "template": "dataset-readiness/check",
  "gate": "fail",
  "verdict": "fail",
  "label": "FAIL",
  "blocking": [],
  "warnings": [],
  "reasons": [
    {"name": "PII rows", "value": 7, "severity": "warn"},
    {"name": "format conformant", "value": 98.1, "severity": "block"}
  ]
}
```

## Files here

- [`github-actions-gate.yml`](github-actions-gate.yml) — GitHub Actions workflow (gates the PR + posts findings via `gh pr comment`).
- [`gitlab-ci-gate.yml`](gitlab-ci-gate.yml) — GitLab CI job (gates the MR + posts an MR note).

> A published Marketplace Action (`uses: cairndev/gate-action@v1`) is a planned
> convenience wrapper over exactly these steps; until then the workflows above
> are the supported way to run the gate in CI — and they need nothing extra.
