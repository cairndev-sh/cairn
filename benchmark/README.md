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
| [`crash_matrix.py`](./crash_matrix.py) | What happens when the server is killed: what each crash cell saw, for each storage, runtime and bus. It adds no checks and needs Postgres; see [below](#what-happens-when-the-server-is-killed) | 3 min 33 s for the 20 crash cells, four at a time, measured once on the author's 8-CPU Intel Mac; other machines differ. In CI the script reads the `public-e2e` run instead of repeating it |
| [`audit_tamper.py`](./audit_tamper.py) | Alterations to a real audit log: the ones `cairn admin audit verify` detects and the row it names, the ones it can't detect, and what a saved head adds and misses | 3 min 45 s, measured once on the author's 8-CPU Intel Mac; other machines differ |

## Where the published numbers come from

The README's "Proof it works" table is generated from `benchmark/results/*.json` by `render.py`, and
`tests/core/test_benchmark_results.py` fails if they differ, if a result doesn't record where it came
from, or if its script is missing.

The `public` CI job runs each measurement on the locked public install and uploads the result in its
`test-results-public` artifact. The crash matrix needs Postgres, so the `public-e2e` job writes it, into
`test-results-public-e2e`. To publish a number, commit that file and run `render.py`. Each result
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

## What happens when the server is killed

[`crash_matrix.py`](./crash_matrix.py) publishes what the crash cells in
[`tests/e2e/hello/test_crash.py`](../tests/e2e/hello/test_crash.py) prove. It adds no check of its own.
A cell counts only if its case passed in that run's junit and it left a receipt. If any cell failed, was
skipped or didn't run, the script prints the table with that cell as it was, exits non-zero and writes no
result. A skipped cell therefore never reads as passed, and a run without Postgres publishes nothing.

A passing cell doesn't mean the work survived. Its entry in the table says what happened, and **of these
cells, only the template leg on DBOS resumes**.

To run it on your machine, start Postgres on the port the cells expect, then run the script:

```bash
uv sync --locked --extra dev --extra rag --extra rag-sqlite-vec --extra storage-postgres --extra runtime-sqlite \
  --extra runtime-postgres --extra runtime-dbos --extra hello
docker run -d --name cairn-crash-pg -p 55432:5432 -e POSTGRES_USER=cairn -e POSTGRES_PASSWORD=cairn \
  -e POSTGRES_DB=cairn pgvector/pgvector:pg16
.venv/bin/python benchmark/crash_matrix.py --out /tmp/crash-matrix
docker rm -f cairn-crash-pg
```

Set `CAIRN_PROOF_PG_DSN` to use another Postgres. The DBOS cells on SQLite need a Python whose SQLite is
3.42 or newer, which a uv-managed Python has. CI doesn't run the cells twice: the `public-e2e` job gives the
script the junit and receipts of the run it just finished, with `--junit` and `--receipts`, and a red cell
step skips the script.

There are eight setups (SQLite or Postgres storage × in-process or DBOS runtime × in-process or Postgres
bus), and three columns. The third exists only on the four DBOS setups, because only DBOS owns a template run
this way; the others read `n/a`. That makes 20 cells.

### Killed at a person gate

Table entry: `still waiting, then finishes`.

- **What is killed:** The server process, with SIGKILL, running the `hello-crash/slow-publish` pipeline.
- **When:** While the `review` person gate waits for a decision, after the three legs before it have finished.
- **What the cell requires afterwards:** The gate is still waiting after the restart and no finished leg has a new run. Approving it finishes the pipeline with one run per leg and one published artifact.
- **How it works, which the cell doesn't assert:** A person gate has no Run: it is a step waiting in the pipeline's stored record, with no process behind it. ADR-0125's liveness check judges leg Runs, so it has nothing to judge here.

### Killed inside a flow leg

Table entry: `fails closed, not resumed`.

- **What is killed:** The server process, with SIGKILL, running the `hello-crash/slow-publish` pipeline.
- **When:** About a second into the `slow` flow leg on the builtin engine, after the leg's engine handle is saved.
- **What the cell requires afterwards:** Right after the restart the leg is still `running` on the same run. Within 90 s it settles `failed` on that run, with an engine error saying the job was lost and a new owner recorded. The pipeline fails, the leg gets no new run, the `stage` and `publish` legs after it never start, and no artifact is staged or published. The work isn't resumed.
- **How it works, which the cell doesn't assert:** ADR-0125 D3 calls a dead owner's lock found free once undetermined, and a second check more than one re-acquire interval later says the owner is dead; D4 then re-attaches the leg. Settling `failed` comes from the builtin engine, whose job table died with the old process, and isn't an ADR-0125 outcome. The harness signals only the server process, so the leg's engine subprocess isn't killed with it.

### Killed inside a template leg on DBOS

Table entry: `resumes, then finishes`.

- **What is killed:** The server process, with SIGKILL, running the `hello-cairn/ask-the-docs` pipeline.
- **When:** While the `assistant` template leg, a DBOS workflow, waits on its model call.
- **What the cell requires afterwards:** Before the kill the run names no in-process owner and its workflow is `PENDING`. After the restart the model is called again, the same run finishes `done`, its workflow reads `SUCCESS`, and the pipeline reaches its person gate. Approving it finishes the pipeline with one run per leg.
- **How it works, which the cell doesn't assert:** ADR-0125 D1 leaves a durable runtime's liveness to that runtime, so this recovery isn't an ADR-0125 outcome. DBOS runs a `PENDING` workflow again at boot, and the LangGraph checkpointer decides where the graph picks up.

### Where this holds

The result names the machine it ran on and the packs installed. CI runs the cells on Linux x86_64 with
Postgres 16. All 20 have also passed once on an Intel Mac with Postgres 18.

The cells need the locked install above, because a pack's policy can apply to every pipeline. If a pack requires
a person for every high-risk operator, the DBOS cell fails at its 180 s limit: DBOS recovers the run, then the
run stops at `memory/write` for an approval the cell never gives. That is the policy working, not a failed
recovery. So the script refuses to write a result if an installed pack isn't in `uv.lock`, and it doesn't
print the pack's name, which could be private.

### Not covered

No cell tests these. Some have tests under `tests/core`, which don't kill a real server.

- The Temporal runtime. No cell runs it: the cells run on the in-process runtime and on DBOS.
- Whether a node's side effects repeat. A node running at the kill runs again, which the DBOS integration leaves to operator idempotency. No cell shows that side effects aren't repeated or that earlier nodes are skipped.
- A kill inside a task leg, or inside a template leg on the in-process runtime.
- A kill while a template run is parked on its own approval (on DBOS, at `recv`).
- A kill inside a flow leg before its engine handle is saved.
- A flow leg on an engine whose jobs outlive the server, such as Metaflow or SkyPilot. Every cell uses the builtin engine, so no cell shows a re-attached leg finishing.
- What the leg's engine subprocess goes on to do after its leg is marked `failed`.
- A kill while a scheduled or event step fires, while a finalizer runs, after a cancel, or while a pipeline starts. A cell tests only the moment its column names.
- A second kill during recovery.
- More than one server: a second replica, or a server on another host.
- Anything other than the server process dying. Postgres and the disk stay up in every cell, so a Postgres restart, a failover or a power loss is not tested.
- How fast recovery is. A cell only has a time limit.

## Add a measurement

1. Add `benchmark/<name>.py`. Use `LocalEngine` from `_common.py`, and drive the engine through
   `cairn-client` rather than CLI commands, so a renamed command can't change the result.
   `crash_matrix.py` is the exception: it starts no engine, because it reads what the crash cells did.
2. Check the claim as well as timing it, and raise on any failure before calling `write_result`.
3. Add it to the table above and to the `public` job in `.github/workflows/ci.yml` (`public-e2e` if it
   reads that job's cells).
4. Commit the result from CI, then run `.venv/bin/python benchmark/render.py`.
