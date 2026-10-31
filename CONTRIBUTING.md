# Contributing to cairn

Welcome! cairn is a governed ML control plane — identity, policy, HITL gates,
cost, lineage and audit for data/ml/agentic pipelines. This guide gets you from
clone to a running dev environment in about 15 minutes, with no tribal
knowledge required.

> **New here?** Start with this file, then the public
> [Quickstart](https://docs.cairndev.sh/quickstart/) for the runtime side
> (choosing SQLite vs Postgres, seeding demo fixtures, starting the server).
> This file covers *contributing*; that one covers *running*.

## Quickstart (the 15-minute path)

```bash
git clone https://github.com/cairndev-sh/cairn && cd cairn

# 1. Python env (uv: https://docs.astral.sh/uv/; it provisions the Python in .python-version, 3.13, as CI does)
uv sync --extra dev --extra rag --extra rag-sqlite-vec \
        --extra runtime-sqlite --extra runtime-temporal \
        --extra runtime-dbos --extra telemetry --extra auth \
        --extra ssh

# ⚠ Any later bare `uv sync` REMOVES every extra not named on that command
# line — auth, temporal, dbos and anything else you added (e.g. `--extra pii`
# for presidio) vanish silently. After any sync, re-run the full command above.
# (`uv run <cmd>` adds what is missing but removes nothing.)

# 2. A local engine on a pinned sqlite config, with demo data
uv run cairn dev --engine-only --seed

# 3. In another shell: sign in and drive it
uv run cairn remote login -u dev -p dev -e http://localhost:8000
uv run cairn remote launch hello-cairn/profile-and-publish --environment sandbox \
    -i path=$PWD/examples/hello-cairn/data/sample.jsonl --watch
```

`cairn dev` starts the engine on http://localhost:8000 on a **pinned** sqlite
config (`~/.cairn/dev.db`): a stale `.env` can never redirect your dev session to
someone's postgres. Sign in with **dev / dev**. From a checkout it adds
`examples/hello-cairn` to `CAIRN_PACK_PATH`. `--seed` writes deterministic demo
records (three pipelines, one paused at a person gate) into the `demo` workspace;
`cairn remote` reads the default workspace, so send `X-Cairn-Workspace: demo` to
the HTTP API to see them. The web console is not part of this repository, so pass
`--engine-only`. Other flags: `--port`, `--db`.

## Working on a pack

A pack is discovered through the `cairn.packs` entry-point group, which is an
*install*-time fact — so a freshly scaffolded pack is invisible to the engine
until it is installed. Two loops (#1545):

```bash
cairn init pack mypack

# 1. Zero-install loop: the engine scans these dirs on every start.
export CAIRN_PACK_PATH="$PWD/mypack"     # `cairn pack add --dev mypack` prints this
cairn ls mypack                          # edits take effect on the next run

# 2. Make it permanent: wires the three root-pyproject.toml points
#    (workspace member + dependency + [tool.uv.sources]) and prints the sync
#    to run. Idempotent; it never syncs for you (a bare `uv sync` strips extras).
cairn pack add mypack
```

An **installed** pack always wins a name collision with a `CAIRN_PACK_PATH`
copy, so a dev pack can never silently shadow a deployed one; a broken dev pack
is skipped with a warning (`cairn ls` lists load failures) rather than taking
the engine down. A pack's `[compat] cairn_engine` range is checked against the
installed engine at load time — a mismatch warns, in the log and as a
`pack_compat:<name>` row in `cairn doctor`, and still loads.

## Running tests

```bash
.venv/bin/pytest tests/core -q     # engine/core
.venv/bin/pytest -q                # everything, incl. operator-pack tests
uvx ruff check .                   # lint, the rule set CI runs
```

### Where tests live, and how to run one layer

| Directory | What it covers | Run it alone |
|---|---|---|
| `tests/core` | the engine: dispatch, pipelines, gates, budget, storage, policy, the HTTP control plane | `.venv/bin/pytest tests/core -q` |
| `tests/cli` | the `cairn` command, driven through its real entry point | `.venv/bin/pytest tests/cli -q` |
| `tests/services` | RAG, vector stores, MCP and the other services under `src/cairn/services` | `.venv/bin/pytest tests/services -q` |
| `tests/deploy` | the Helm chart and the deploy examples | `.venv/bin/pytest tests/deploy -q` |
| `tests/examples` | the example pack, `examples/hello-cairn` | `.venv/bin/pytest tests/examples -q` |
| `tests/dataflow_conformance` | the dataflow engine contract every engine must meet | `.venv/bin/pytest tests/dataflow_conformance -q` |
| `tests/e2e` | the hello-cairn feature proof: a real `cairn-server` per cell, driven as a user would | `.venv/bin/pytest -m e2e tests/e2e -q` (or `scripts/oss/feature_proof.sh` with services) |
| `packages/*/tests` | each builtin operator pack's own tests | `.venv/bin/pytest packages/cairn-operator-data-profile/tests -q` |

Run one file or one test while you work (`.venv/bin/pytest tests/cli/test_gate.py -q`,
add `-k <name>` for one case), and the layer it belongs to before you push. What each
kind of test is allowed to prove, and which kinds a change needs, is in
[docs/testing/strategy.md](docs/testing/strategy.md). A bug fix lands with the
regression test, and the PR shows that test failing without the fix.

`tests/core/test_dbos_runtime.py` needs the `runtime-dbos` extra (in the sync line
above). CI's `public` job runs `tests/core`, `tests/cli`, `tests/deploy` and the
optional engines' own tests with `-m "not dbos and not timing and not e2e"`; its
`public-e2e` job runs the feature proof (`-m e2e tests/e2e`, Postgres and DBOS cells
included). `scripts/oss/feature_proof.sh` runs the same proof locally with services.

The builtin operator packs and the core contracts are part of the `cairndev`
project (ADR-0137). An editable install reads the packs' code and `pack.toml`
from `packages/`, but holds COPIES of the contract definitions
(`packages/cairn-contracts-*/definitions`). After editing a contract, refresh
them: `uv sync --reinstall-package cairndev` (with the extras you use).

`.venv/bin/pytest` and `uv run pytest` run the same environment: `uv run` installs
what is missing but removes nothing, so the extras from the sync line stay.

The suite is hermetic: no keys, no databases, no network. If a test you write
needs a real backend, it must opt in explicitly — the repo `.env` is
neutralized in `tests/conftest.py` on purpose. That neutralization covers the
`.env` file only: a storage variable **exported in your shell** (a value that
differs from `.env`) is treated as deliberate and steers the suite. If your
shell exports one, unset it for the run:

```bash
env -u CAIRN_STORAGE .venv/bin/pytest tests/core -q
```


## Other CI gates

- **`uv.lock`** — the install is `uv sync --locked`, so a dependency or version
  change needs `uv lock` in the same PR (`uv lock --check` tells you).
- **The wheels** — `scripts/check_folded_wheels.py` builds the five wheels and
  checks that `cairndev` carries the builtin operators and the core contracts.
- **The engine image** — the `image` job builds `Dockerfile.engine` and starts it.

## Documentation

The user docs at [docs.cairndev.sh](https://docs.cairndev.sh) are the Astro/Starlight site in
`docs/site/`. Every pull request gets a preview deployment, and `main` deploys to production.
Build it locally with `npm ci && npm run build` in `docs/site/`. The one-line installer is
`docs/site/public/install.sh`, served at `https://docs.cairndev.sh/install.sh`.

## Pull requests

1. **One issue, one branch, one PR.** Open or claim a GitHub issue first;
   branch from `main` (`fix/<issue>-slug` / `feat/<issue>-slug`). Never commit
   to `main`.
2. **CI must be green** — the required checks are the merge gate, not a
   suggestion. PRs test one Python version for fast feedback; `main` re-proves
   the full matrix.
3. Comment progress on the issue as slices land; close it with a summary.
4. Code style: match the surrounding code. Imports at module top (lazy imports
   only for optional extras or real cycles, with a one-line reason). Operator
   pack tests live inside the pack's own `tests/`, not top-level `tests/`.
5. New operators ship with golden-fixture tests. Packs are data (md + YAML) —
   code belongs in operator packs.
6. **Regenerate what your change invalidates**: `uv lock` after a dependency or
   version change, and the docs page a behaviour change makes wrong.
7. **Sign off every commit (DCO).** Contributions are Apache-2.0 under the
   [Developer Certificate of Origin](https://developercertificate.org/): commit
   with `git commit -s`, which adds a `Signed-off-by: Your Name <you@example.com>`
   line matching the commit author. The public repository's `dco` check refuses a
   pull request with an unsigned commit. To fix it after the fact, use
   `git commit --amend -s --no-edit` for the last commit, or `git rebase --signoff main`
   for the whole branch, then force-push your branch.

   ```bash
   git config user.name  "Your Name"
   git config user.email "you@example.com"   # the address on your GitHub account
   git commit -s -m "fix(cli): …"
   ```

   If your GitHub email is private, use your `<id>+<login>@users.noreply.github.com`
   address for both the author and the sign-off, or GitHub refuses the push.

### What review looks for

Reviews here are about evidence, not style policing — the formatter handles
style. In rough priority:

1. **Is the claim verified?** The PR says how it was checked, and the check
   actually proves the thing. For anything user-facing, that means the real
   surface was driven, not just a green unit test.
2. **Was the problem re-verified before it was fixed?** Issues go stale.
   Several "bugs" here turned out to be already fixed, or scoped wrong in the
   issue. Saying "the issue was wrong, here's what's actually true" is a good
   PR, not a failed one.
3. **Does this rebuild something that exists?** Upstream libraries, existing
   stores, shipped mechanisms. Extending beats duplicating.
4. **Are failures reported honestly?** A known-failing test, an unverified
   corner, a skipped step — all fine when stated. Not fine when discovered
   later.

Two things worth saying out loud, because they're easy to get wrong under
time pressure: **never inherit a test baseline** (diagnose each failure's
cause rather than memorising a count — an "11-failure baseline" here was
4 wiped-venv, 2 stale tests, and 0 real defects), and **an empty grep proves
nothing** until you've checked your search terms against the codebase's own
vocabulary.

### What gates a merge to `main`

`main` is a protected branch:

- **Every change lands through a PR** with one approving review; conversations
  must be resolved, and force pushes and deletion are refused.
- **Required checks**: `public` and `public-e2e` (`ci.yml`) and `dco` (`dco.yml`),
  on a branch up to date with `main`. The `image` job runs on every PR too; a red
  one is fixed in the PR, not merged around.

## Where decisions live (reading order)

1. [docs/architecture/00-overview.md](docs/architecture/00-overview.md) — the system, top-down.
2. [docs/decisions/](docs/decisions/README.md) — the ADR log. Start with
   ADR-0020 (the locked pack contract), ADR-0084 (control plane: one flow
   concept, pipelines as the runnable surface), ADR-0085 (this contributor
   experience and where it's going).
3. The GitHub issue tracker — what's in flight. Issues labeled
   [`good first issue`](https://github.com/cairndev-sh/cairn/labels/good%20first%20issue)
   are curated to be self-contained.

Changing a locked surface needs a **new ADR**, not an edit to an old one.

## Layout in one paragraph

`src/cairn/core/` is the engine (dispatch, storage, policy, triggers/API server) and
`src/cairn/cli/` the `cairn` command; `packages/cairn-operator-*` are the builtin
operator packs and `packages/cairn-contracts-*` the core capability contracts, both
shipped inside the `cairndev` wheel (ADR-0137), except the two optional engines
(`cairn-operator-dataflow-metaflow`, `cairn-operator-dataflow-skypilot`), which are
wheels of their own; `packages/cairn-client` is `cairn-client`, the typed API client
(no `cairn.core` imports — CI-enforced); `examples/hello-cairn` is the example
domain pack (data only, its own `cairn-pack-hello` wheel); `deploy/` holds the
compose stack and the Helm chart; `docs/site/` is docs.cairndev.sh. The web console
and the product modules are the private platform (ADR-0120 D3, ADR-0121), which the
public core reaches only through entry-point groups (`cairn.routers`, `cairn.cli`,
`cairn.run_hooks`, `cairn.model_catalogs`, `cairn.seeds`); `docs/decisions/` is why
everything above is the way it is.

## The public core and the platform

`uv sync --extra dev` installs the public set (the engine with the builtin operator
packs and the core contracts, and the client) and nothing private; the `public` CI
job proves that set installs alone and passes `tests/core`, and
`scripts/check_public_set.py` refuses any private distribution. Three rules the
boundary tests enforce:
the public core never imports `cairn_platform` or a domain pack; an operator pack
imports only `cairn.core.operator` (its re-exports cover what a pack needs from the
core) plus `cairn.core.dataflow` for engine providers; product logic lives in the platform,
and what stays in core by decision is justified in `cairn/core/CLAUDE.md` (#2497).
