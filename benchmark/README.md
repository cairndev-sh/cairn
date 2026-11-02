# Benchmarks

Every number in the README's "Proof it works" table comes from a script in this folder, and every
result records what it was measured on. Nothing here needs an API key or spends money unless its section says so.

## Run a measurement from a clean clone

```bash
git clone https://github.com/cairndev-sh/cairn && cd cairn
uv sync --locked --extra dev
.venv/bin/python benchmark/quickstart_path.py
```

Each run starts its own engine in a scratch directory on a free port, so it leaves your `~/.cairn`
alone. If any check fails, the script exits non-zero and writes no result. By default it overwrites
the published `benchmark/results/<measurement>.json`, and the README test then fails until you revert
that file or run `render.py`; pass `--out DIR` to keep your result separate.

## Measurements

| Script | What it measures | Takes |
|---|---|---|
| [`quickstart_path.py`](./quickstart_path.py) | The quickstart's pipeline on a local engine: time to the paused gate, approvals to done, the gates holding, each person's decision in the audit log, the audit chain verifying | a minute or two |
| [`audit_tamper.py`](./audit_tamper.py) | Alterations to a real audit log: the ones `cairn admin audit verify` detects and the row it names, the ones it can't detect, and what a saved head adds and misses | 3 min 45 s, measured once on the author's 8-CPU Intel Mac; other machines differ |

## Where the published numbers come from

The README's "Proof it works" table is generated from `benchmark/results/*.json` by `render.py`, and
`tests/core/test_benchmark_results.py` fails if they differ, if a result doesn't record where it came
from, or if its script is missing.

The `public` CI job runs each measurement on the locked public install and uploads the result in its
`test-results-public` artifact. To publish a number, commit that file and run `render.py`. Each result
records:

| Field | Meaning |
|---|---|
| `cairndev_version` | the version under test |
| `uv_lock_sha256` | the exact locked dependencies |
| `git_sha`, `code_clean` | the commit, and that `src`, `packages`, `examples`, `benchmark`, `uv.lock` and `pyproject.toml` had no uncommitted changes (a result without this is refused) |
| `machine` | operating system, architecture, CPU count and Python version; never a host name |
| `measured_at` | the date |
| `ci_run` | the CI run that produced it, if one did |

Timings depend on the machine, so compare them only between runs on the same one. On a CI run, a pull
request's `git_sha` is GitHub's merge commit for that pull request.

## Add a measurement

1. Add `benchmark/<name>.py`. Use `LocalEngine` from `_common.py`, and drive the engine through
   `cairn-client` rather than CLI commands, so a renamed command can't change the result.
2. Check the claim as well as timing it, and raise on any failure before calling `write_result`.
3. Add it to the table above and to the `public` job in `.github/workflows/ci.yml`.
4. Commit the result from CI, then run `.venv/bin/python benchmark/render.py`.
