---
title: Install and deploy
description: Install the cairn CLI and engine, pick the extras you need, run a server on one host with Docker Compose or on Kubernetes with the Helm chart, and turn on sign-in.
---

## Install the CLI and engine

```bash
curl -LsSf https://docs.cairndev.sh/install.sh | sh
```

The installer puts `cairn` on your PATH as a [uv](https://docs.astral.sh/uv/)
tool, in an environment of its own; re-run it to upgrade. If uv is missing, it
installs it first with astral.sh's installer, into `~/.local/bin` and without
sudo. Choose extras or pin a version with environment variables:

```bash
curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_EXTRAS=hello,rag sh
curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_VERSION=0.1.2 sh
```

| Variable | Default | |
|---|---|---|
| `CAIRN_EXTRAS` | none | comma-separated extras, from the table below |
| `CAIRN_VERSION` | the latest release | an exact version |
| `CAIRN_PYTHON` | `3.13` | the Python uv provisions for the tool |
| `CAIRN_RELEASE` | `CAIRN_VERSION`, else `0.1.2` | the GitHub Release whose wheels back the install; empty means PyPI only |

The same without the script. Until the 0.1.2 packages are on PyPI, point the installer at the
release's wheels (third-party packages still come from PyPI):

```bash
L=https://github.com/cairndev-sh/cairn/releases/expanded_assets/v0.1.2
pip install cairndev --find-links "$L"            # inside a virtual environment, Python 3.12 or later
uv tool install cairndev --find-links "$L"        # or: pipx install cairndev --pip-args="--find-links $L"
```

The distribution is `cairndev`; the command and the import name are `cairn`.
The builtin operator packs and the core contracts ship inside the `cairndev`
wheel, so there is nothing else to install for them; their extras are
`cairndev` extras (below). To remove it: `uv tool uninstall cairndev`.

Check the install:

```bash
cairn --version
cairn doctor        # packs load, profile bindings resolve, settings, service reachability
cairn ls            # the installed packs and what they contribute
```

### Extras

| Extra | Adds |
|---|---|
| `hello` | the example pack (`cairn-pack-hello`) with the sqlite-vec store, asyncssh and DBOS |
| `rag` | the retrieval ingest stack (parse, embed, index) |
| `rag-sqlite-vec` · `rag-pgvector` · `rag-qdrant` | a vector store |
| `ssh` | `remote/exec` over SSH |
| `train` | the CPU training stack for `train/lora` (torch, transformers, peft, datasets) |
| `infisical` | the Infisical SDK for `secrets/infisical` |
| `dataflow-engine` | the builtin dataflow engine's job-machine libraries (polars, duckdb, pyarrow) |
| `rerank-cross-encoder` · `rerank-cohere` | a scorer for `rag/rerank`: a local cross-encoder, or Cohere |
| `storage-postgres` | the Postgres store and bus driver |
| `runtime-dbos` | the DBOS durable runtime |
| `runtime-temporal` | the Temporal runtime |
| `runtime-sqlite` · `runtime-postgres` | a durable checkpointer for paused agentic flows |
| `pii` | Presidio-backed PII detection |
| `telemetry` | OpenTelemetry export |
| `auth` | argon2 password hashing for server sign-in |
| `oauth` | sign-in with GitHub or Google |
| `nats` | the NATS remote-job channel |
| `metaflow` | the Metaflow dataflow engine (`cairn-operator-dataflow-metaflow`) |
| `skypilot` | the SkyPilot provisioning engine (`cairn-operator-dataflow-skypilot`) |

### From source

```bash
git clone https://github.com/cairndev-sh/cairn && cd cairn
uv sync --extra dev
uv run cairn --help
```

`uv sync` keeps only the extras you name: a later `uv sync` without them
removes them again.

## Choose the backends

Where cairn keeps its state and how durable a run is are three settings on the
engine. Pipelines don't change.

| Choice | Variable | Options | Default |
|---|---|---|---|
| storage | `CAIRN_STORAGE` | `sqlite:///<path>` · `postgres://…` | unset: runs are kept in memory and lost on restart |
| bus | `CAIRN_BUS` | `inprocess` · `postgres://…` | `inprocess` |
| runtime | `CAIRN_RUNTIME` | `inprocess` · `dbos` · `temporal` | `inprocess` |

Always set `CAIRN_STORAGE` for a server you want to keep. SQLite is for one
host; use Postgres (the `storage-postgres` extra) when more than one process
touches the data. `dbos` (the `runtime-dbos` extra) recovers a run at the next
boot without an extra server. Under `temporal` a `task:` leg fails today, so
use `inprocess` or `dbos` for pipelines with task legs.

The deployment **profile** decides which operator serves each capability. Set
it with `CAIRN_PROFILE`; see
[Capabilities and environments](/concepts/capabilities/). `cairn dev` sets all
of this for you on a laptop; a server is started with `cairn-server`.

## Single host with Docker Compose

`deploy/light` in the repository runs the engine image behind Caddy, with
Postgres for runs, checkpoints and feature flags. No accounts, no keys and no
`.env` are needed to start it.

```bash
git clone https://github.com/cairndev-sh/cairn && cd cairn/deploy/light
docker compose up -d --build
```

```
client ──▶ caddy :8080 ──▶ server (in-process runtime + scheduler)
                              └ runs · checkpoints · flags ▶ postgres
```

The first build compiles the engine image (several minutes). When
`docker compose ps` shows `server` as healthy, the API answers on
`http://localhost:8080`:

```bash
curl -s http://localhost:8080/healthz
cairn remote ps --endpoint http://localhost:8080
```

The stack runs the `secure` profile: every run leaves a hash-chained audit
row, and text is redacted before it reaches a model. A pipeline with a model
call needs a provider key: put `OPENAI_API_KEY=sk-…` in `deploy/light/.env`
and run `docker compose up -d` again.

### Optional services

Add them with compose profiles: `--profile <name>`, or list them in
`COMPOSE_PROFILES` in `.env`.

| Profile | Adds | Also set |
|---|---|---|
| `llm-gateway` | a LiteLLM proxy (`litellm-config.yaml`): every governed model call through one door | `LITELLM_MASTER_KEY` before you enable it; then `LLM_GATEWAY=proxy`, `LLM_GATEWAY_URL=http://litellm:4000`, `LLM_GATEWAY_API_KEY=<that key>` |
| `backup` | a `pg_dump` of the database every `CAIRN_BACKUP_INTERVAL_SECONDS` (default daily), keeping `CAIRN_BACKUP_KEEP` (default 14) | nothing |
| `infisical` | a self-hosted Infisical server on `127.0.0.1:8085` for `secrets/infisical` | `INFISICAL_ENCRYPTION_KEY`, `INFISICAL_AUTH_SECRET` and `INFISICAL_DB_PASSWORD` in `.env.infisical` (not `.env`: everything in `.env` reaches the engine) |

Check a deployment's `.env` and the profiles you enabled before anything
starts:

```bash
cairn deploy doctor --env .env
```

### Operating it

| Task | Command (from `deploy/light`) |
|---|---|
| Status | `docker compose ps` |
| Logs | `docker compose logs -f server` |
| Stop | `docker compose down` (keeps the volumes) |
| Reset | `docker compose down -v` (drops runs, flags, backups) |
| Update | `git pull && docker compose up -d --build` |
| Change the port | `CAIRN_PORT=9000 docker compose up -d` |

## Kubernetes with Helm

The chart in `deploy/helm/cairn` is a translation of the compose file, not a
second architecture: a test fails the build if the two set different engine
variables.

```bash
kubectl create namespace cairn
cp deploy/helm/cairn/values-postgres.example.yaml my-values.yaml   # edit every CHANGE-ME
cairn deploy doctor --values my-values.yaml --strict
helm upgrade --install cairn deploy/helm/cairn -n cairn -f my-values.yaml
```

`--strict` turns warnings into failures; it fails on an unpinned `latest`
image tag, so pin `image.tag`. Three example values files ship with the
chart: bundled Postgres (`values-postgres.example.yaml`), an external database
(`values-external-dsn.example.yaml`) and Temporal
(`values-temporal.example.yaml`). While the engine image on GHCR is private,
create a `docker-registry` pull secret in the namespace and list it in
`imagePullSecrets`.

Then `kubectl -n cairn rollout status deploy/cairn-server` and
`curl -fsS https://<your host>/healthz`.

## Sign-in and sign-up

The API is open while no JWT secret is set: fine on a laptop, never on a box
other people can reach. To turn sign-in on, set in `.env` (or the chart's
`secrets.*`):

```bash
CAIRN_JWT_SECRET=<openssl rand -hex 32>
CAIRN_API_USER=admin
CAIRN_API_PASSWORD=<choose one>     # or CAIRN_API_PASSWORD_HASH from `cairn remote hash-password`
```

Every API call then needs a token: `cairn remote login --endpoint <url>`.
Only `/healthz`, `/token`, the sign-up, invite and provider sign-in routes
under `/auth/`, and webhook receivers that check their own signature answer
without one.

**Who can sign up: `CAIRN_SIGNUP`.**

| Mode | Who can create an account through `POST /auth/signup` |
|---|---|
| `invite` (default) | nobody; people join with an invite link (`POST /auth/accept-invite`) |
| `domain` | anyone whose work-email domain an account has registered; they join that account as a member. A personal-mail domain (gmail.com and the like) is never registered at sign-up, so it opens no door. |
| `open` | anyone; each sign-up creates a new account with its founder as owner |

On a deployment with no accounts yet, the first person to sign up becomes the
owner of a new account, whatever the mode. An unrecognised value falls back to
`invite`.

**Sign in with GitHub or Google** (the `oauth` extra):

1. Set `CAIRN_PUBLIC_URL` to the public origin, for example
   `https://cairn.example.com`. Behind a proxy this is required: without it the
   callback is built from the internal address and the provider rejects it.
2. Register an OAuth app with the provider, with the callback
   `https://cairn.example.com/auth/oauth/github/callback` (or
   `/auth/oauth/google/callback`).
3. Set `CAIRN_OAUTH_GITHUB_CLIENT_ID` and `CAIRN_OAUTH_GITHUB_CLIENT_SECRET`
   (or the `GOOGLE` pair) and restart the server.

Signing in with a provider doesn't bypass `CAIRN_SIGNUP`. See
[Set up your team](/guides/team/).

Settings are read as `CAIRN_<NAME>`. The former `OBS_<NAME>` spelling is still
read, with a warning, until 1.0.

## Next

- [Quickstart](/quickstart/): run a pipeline on a local engine.
- [Set up your team](/guides/team/): sign-in, sign-up and workspaces.
- [Capabilities and environments](/concepts/capabilities/): what the deployment profile binds.
