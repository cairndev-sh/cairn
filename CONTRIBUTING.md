# Contributing to cairn

Welcome! cairn is a governed ML control plane — identity, policy, HITL gates,
cost, lineage and audit for data/ml/agentic pipelines. This guide gets you from
clone to a running dev environment in about 15 minutes, with no tribal
knowledge required.

> **New here?** Start with this file, then the public
> [Quickstart](https://docs.cairndev.sh/quickstart/) for the runtime side
> (choosing SQLite vs Postgres, seeding demo fixtures, starting the server
> and console). This file covers *contributing*; that one covers *running*.

## Quickstart (the 15-minute path)

```bash
git clone https://github.com/cairndev-sh/cairn && cd cairn

# 1. Python env (uv: https://docs.astral.sh/uv/)
uv sync --extra dev --extra rag --extra rag-sqlite-vec \
        --extra runtime-sqlite --extra runtime-temporal \
        --extra telemetry --extra auth \
        --extra ssh

# ⚠ Any later bare `uv sync` (or a plain `uv run <cmd>`, which re-syncs the
# environment) REMOVES every extra not named on that command line — auth,
# temporal, and anything else you added (e.g. `--extra pii` for presidio)
# vanish silently. After any sync, re-run the full command above.

# 2. Console deps (only needed for UI work)
npm ci --prefix console

# 3. Everything, one command
uv run cairn dev --seed
```

`cairn dev` starts the engine (http://localhost:8000) and the console's vite
dev server on a **pinned** sqlite config — a stale `.env` can never redirect
your dev session to someone's postgres. Log in with **dev / dev**. `--seed`
fills every console surface with deterministic demo data (including one run
paused at an approval gate, so the HITL flow is explorable immediately).

Flags: `--engine-only`, `--web-only`, `--port`, `--db`.

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
.venv/bin/pytest tests/core -q     # engine/core (the PR gate runs the full suite)
.venv/bin/pytest -q                # everything, incl. operator-pack tests
npm run build --prefix console     # console typecheck + bundle
npm run lint  --prefix console     # oxlint
```

The builtin operator packs and the core contracts are part of the `cairndev`
project (ADR-0137). An editable install reads the packs' code and `pack.toml`
from `packages/`, but holds COPIES of the contract definitions
(`packages/cairn-contracts-*/definitions`). After editing a contract, refresh
them: `uv sync --reinstall-package cairndev` (with the extras you use).

Prefer `.venv/bin/pytest` over `uv run pytest`: `uv run` re-syncs the
environment first, which strips any extras not named on that exact command
(see the quickstart warning).

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

- **`cairn.lock`** — the shipped pack closure. If your change touches shipped
  packs/operators, regenerate with `uv run cairn lock` (exactly that
  invocation — it is what CI verifies via `uv run cairn lock --check`).
- **Docs honesty** — `uv run python scripts/audit_status.py` probes the
  claims in `status.yaml` against the code.

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
6. **Regenerate what your change invalidates.** A pack edit needs
   `uv run cairn lock`. It is a
   CI gate, and it has been missed before.
7. **Sign off every commit (DCO).** Contributions are Apache-2.0 under the
   [Developer Certificate of Origin](https://developercertificate.org/): commit
   with `git commit -s`, which adds a `Signed-off-by: Your Name <you@example.com>`
   line matching the commit author. The public repository's `dco` check refuses a
   pull request with an unsigned commit; `git rebase --signoff main` fixes a branch after the fact.

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

Install the hooks once and pre-commit will catch the quick stuff locally
(pre-commit is not a project dependency, so run it as a `uvx` tool — plain
`uv run pre-commit` will not find it):

```bash
uvx pre-commit install
```

### What gates a merge to `main`

`main` has **no GitHub branch protection today** — the private repository's
plan does not offer it, so nothing in GitHub itself blocks a push or a red
merge. The rule is a working agreement:

- **Every change lands through a PR**, and the maintainer reviews and merges
  it. Nobody pushes to `main` directly.
- **Green CI is the merge condition**: the `ci.yml` jobs **test (3.13)**,
  **build**, **web** and **docker** must pass. A red check is fixed in the PR,
  not merged around.
- Branch protection with these required checks will be switched on when the
  repository becomes public; this section will then describe what GitHub
  enforces.

## Where decisions live (reading order)

1. [docs/architecture/00-overview.md](docs/architecture/00-overview.md) — the system, top-down.
2. [docs/decisions/](docs/decisions/README.md) — the ADR log. Start with
   ADR-0020 (the locked pack contract), ADR-0084 (control plane: one flow
   concept, pipelines as the runnable surface), ADR-0085 (this contributor
   experience and where it's going).
3. [docs/roadmap.md](docs/roadmap.md) + the GitHub issue tracker — what's in
   flight. Issues labeled `good-first-issue` are curated to be self-contained.

Changing a locked surface needs a **new ADR**, not an edit to an old one.

## Layout in one paragraph

`cairn/core/` is the engine (dispatch, storage, policy, triggers/API server);
`cairn/packs/` are domain packs (data only, each its own `cairn-pack-<name>` wheel
with its `cairn.packs` entry point in `cairn/packs/<dir>/pyproject.toml`);
`cairn/operators/` are operator packs (each its own wheel, tests inside; `builtins/`
ship with the public core, `official/` are the platform's); `contracts/` are the
capability contracts (data wheels); `client/` is `cairn-client`, the typed API client
over the committed contract (no `cairn.core` imports — CI-enforced); `console/` is
the React console (top-level per ADR-0085); `packages/cairn-platform/` is the
private platform (ADR-0120 D3, ADR-0121): the product modules, the console-only
routers, the product CLI commands and the run-event listeners that the public core
reaches only through entry-point groups (`cairn.routers`, `cairn.cli`,
`cairn.run_hooks`, `cairn.model_catalogs`, `cairn.seeds`); `docs/decisions/` is why
everything above is the way it is.

## The public core and the platform

`uv sync --extra dev` alone installs the PUBLIC SET (the engine, the builtin operator
packs, the core contracts, client) and nothing private; the `public` CI job
proves that set installs alone and passes the `tests/core` files listed in
`tests/core` (exactly the public set, as assembled) (plus `scripts/check_public_set.py`, which refuses
any private distribution). The `ssh` extra adds the rest, which
is what this repository's CI and image run. Three rules the boundary tests enforce:
the public core never imports `cairn_platform` or a domain pack; an operator pack
imports only `cairn.core.operator` (its re-exports cover what a pack needs from the
core) plus `cairn.core.dataflow` for engine providers; product logic lives in the platform,
and what stays in core by decision is justified in `cairn/core/CLAUDE.md` (#2497).
