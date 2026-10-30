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
curl -LsSf https://docs.cairndev.sh/install.sh | CAIRN_VERSION=0.3.2 sh
```

| Variable | Default | |
|---|---|---|
| `CAIRN_EXTRAS` | none | comma-separated extras, from the table below |
| `CAIRN_VERSION` | the latest release | an exact version |
| `CAIRN_PYTHON` | `3.13` | the Python uv provisions for the tool |
| `CAIRN_RELEASE` | unset (PyPI only) | also look for cairn's wheels in this GitHub Release and pin `cairndev` and `cairn-client` to it (unless `CAIRN_VERSION` is set); versions before 0.1.3 are only there, e.g. `CAIRN_RELEASE=0.1.2` |

The same without the script, from PyPI:

```bash
pip install cairndev              # inside a virtual environment, Python 3.12 or later
uv tool install cairndev          # or: pipx install cairndev
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

### Upgrade from 0.1.x

Re-run the installer, or `uv tool upgrade cairndev`. For a server:
`git pull && docker compose up -d --build` in `deploy/light`, or
`helm upgrade … --version 0.3.2` for the chart.

Single-node and sign-in-off deployments, where everything belongs to the
`default` workspace and account, are unaffected by 0.2.0's workspace, tenant
and dataset changes. Some steps apply to every deployment; the others only with
sign-in on, or with more than one account or workspace.

**Every deployment:**

1. **Run-hook providers.** Return early on events you don't handle, and raise
   only `AdmissionRefused` on `pipeline.launching`: any other exception there
   refuses every launch.
2. **Knowledge.** Re-index pgvector collections whose name exceeds 63 bytes.
3. **Library callers** of `mcp_server_spec`, `resolve_corpus_source` and
   `index_workspace_corpus`: pass `allow_stdio=True`,
   `allow_private_network=True` or `allow_local=True` for specs and sources the
   deployment itself owns.
4. **Corpus clones behind a proxy or a private CA:** set `HTTPS_PROXY`,
   `SSL_CERT_FILE` or `GIT_SSL_CAINFO` in the environment instead of
   `http.proxy` / `http.sslCAInfo` in the gitconfig.
5. **Behind a TLS-terminating proxy**, set `CAIRN_PUBLIC_URL` (see
   [Sign-in and sign-up](#sign-in-and-sign-up)).

**With sign-in on, or more than one account or workspace:**

6. **Pull workers.** Mint a token per pool with
   `cairn remote worker-token --name <pool>` (where `CAIRN_JWT_SECRET` is set)
   and restart each worker with `cairn worker --pull --token <that token>`. A
   signed-in user's token now gets `403` on `/jobs/claim`.
7. **Team accounts' connections.** Each team account creates, under its own
   names, the connections its runs used from the deployment, and its pipelines
   and flows reference those names. Enter again the credentials of connections
   created before account namespacing. A connection bound in the deployment's
   profile (`ml_target`, `dataflow_target`) belongs to the deployment; a team
   account's flow legs need a target connection of their own, named in the
   pipeline or flow. See [Connect a provider](/guides/connect-a-provider/).
8. **Operators** that need credentials read them from `ctx.secrets`: a team
   account's run does not receive its account's credentials in the process
   environment.
9. **Scripts** that resolve connections, secrets or datasets outside a request
   or a run must enter a workspace first:
   `with workspace_scope("<id>"):` from `cairn.core.workspace`.
10. **Datasets** shared between workspaces: register them in each workspace;
    `dataset:<name>@latest` is per workspace.
11. **Knowledge.** Re-index the corpora of a workspace whose id is not a
    lowercase slug, or is longer than 40 characters.
12. **Saga approvals.** Deciding a paused saga through
    `POST /triggers/approve/{workflow_id}` needs the saga's workspace: its
    action handler implements `workspace_of`. Without it, only the deployment
    admin can decide it.

The 0.2.0 Migration section of
[CHANGELOG.md](https://github.com/cairndev-sh/cairn/blob/main/CHANGELOG.md)
has the details, including the exact re-index steps.

### Extras

| Extra | Adds |
|---|---|
| `hello` | the example pack (`cairn-pack-hello`) with the sqlite-vec store, asyncssh and DBOS |
| `rag` | the retrieval ingest stack (parse, embed, index) |
| `rag-sqlite-vec` · `rag-pgvector` · `rag-qdrant` · `rag-pinecone` | a vector store |
| `ssh` | `remote/exec` over SSH |
| `train` | the CPU training stack for `train/lora` (torch, transformers, peft, datasets) |
| `optuna` | Optuna, to create hyperparameter studies |
| `kernel` | only what a box or container that runs a flow needs (no control plane) |
| `infisical` | the Infisical SDK for `secrets/infisical` |
| `dataflow-engine` | the builtin dataflow engine's job-machine libraries (polars, duckdb, pyarrow) |
| `rerank-cross-encoder` · `rerank-cohere` | a scorer for `rag/rerank`: a local cross-encoder, or Cohere |
| `storage-postgres` | the Postgres store and bus driver |
| `runtime-dbos` | the DBOS durable runtime |
| `runtime-temporal` | the Temporal runtime |
| `runtime-sqlite` · `runtime-postgres` | a durable checkpointer for paused agentic flows |
| `pii` | Presidio-backed PII detection |
| `telemetry` | OpenTelemetry export |
| `auth` | kept for compatibility: argon2 password hashing is in the base install since 0.1.2 |
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

### Software bill of materials

Every [GitHub release](https://github.com/cairndev-sh/cairn/releases) after v0.3.2
attaches CycloneDX SBOMs built from that tag's lock files:

| File | What it lists |
|---|---|
| `cairn-<version>.cdx.json` | every package in `uv.lock` that the published wheels can install: all extras, every platform. No licence data. |
| `cairn-engine-<version>.cdx.json` | the engine image's Python packages that `uv.lock` resolves, on linux/x86_64, with licences. Packages the image adds with `uv pip install` (the spaCy models, the production tier) are not in it. |
| `cairn-docs-site-<version>.cdx.json` | the documentation site's runtime npm packages, with licences |

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
boot without an extra server. Under `temporal` the launch is refused, before
anything is recorded, for a pipeline with a `task:` leg or a `finalizer:` step:
the Temporal pipeline owner does not run either yet. The refusal names the step
(HTTP 403, `cairn pipeline run` exits 3); use `inprocess` or `dbos` for such
pipelines.

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

## The engine image on its own

The image the compose file and the chart run, `ghcr.io/cairndev-sh/cairn-engine`, also
runs with plain `docker run`. Without `CAIRN_PROFILE` it runs the packaged `secure`
profile. The server binds every interface only when sign-in is on, so give it a JWT
secret and an admin password:

```bash
docker run -d -p 9090:9090 \
  -e CAIRN_JWT_SECRET="$(openssl rand -hex 32)" -e CAIRN_API_PASSWORD=<choose one> \
  -e CAIRN_STORAGE=sqlite:////data/cairn.db -v cairn-data:/data \
  ghcr.io/cairndev-sh/cairn-engine:0.3.2
curl -s http://localhost:9090/healthz
cairn remote login --endpoint http://localhost:9090 -u admin
```

With sign-in off the server listens on the container's own loopback, so a published
port answers nothing. For a keyless try-out on your own machine, bind it explicitly
and publish on the host's loopback only:

```bash
docker run --rm -p 127.0.0.1:9090:9090 -e CAIRN_TRIGGER_HOST=0.0.0.0 ghcr.io/cairndev-sh/cairn-engine:0.3.2
```

Without `CAIRN_STORAGE` runs are kept in memory and lost when the container stops.

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
(`values-temporal.example.yaml`). The engine image
(`ghcr.io/cairndev-sh/cairn-engine`) and the chart are public on GHCR, so no
pull secret is needed.

The same chart is published as an OCI artifact, so you can install a released
version without cloning the repository:

```bash
helm upgrade --install cairn oci://ghcr.io/cairndev-sh/charts/cairn --version 0.3.2 -n cairn -f my-values.yaml
```

Then `kubectl -n cairn rollout status deploy/cairn-server` and
`curl -fsS https://<your host>/healthz`.

The chart can also run a pull worker (`cairn worker --pull`), off by default.
With `pullWorker.enabled: true`, set `pullWorker.command`, and set either
`pullWorker.token` (a worker-pool token from
`cairn remote worker-token --name <pool>`) or `pullWorker.existingSecret` (a
Secret holding `CAIRN_ENGINE_TOKEN`); otherwise rendering refuses. See the
[chart README](https://github.com/cairndev-sh/cairn/blob/main/deploy/helm/cairn/README.md#the-pull-worker)
for the values and the upgrade notes.

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
The token goes in the `Authorization: Bearer` header. The one exception is the run
event stream, `GET /runs/{id}/stream`, which also accepts it as `?access_token=`
because a browser's `EventSource` cannot set headers; every other route refuses a
token in the query string with `401`.

Only `/healthz`, `/token`, the sign-up, invite and provider sign-in routes
under `/auth/`, `/oauth/callback` (the redirect `cairn connect --via oauth`
completes on; it checks its own single-use `state`), and webhook receivers that require a
signature (`signature.required: true`) answer without one. A receiver is open only for
its own method and only while its signing secret is set to a non-empty value. Its runs
land in the workspace that declared it, and these open routes ignore `X-Cairn-Workspace`.

A console sign-in is a short access token (`CAIRN_ACCESS_TOKEN_EXPIRE_MINUTES`,
default 30) plus an httpOnly `cairn_refresh` cookie that `POST /auth/refresh`
trades for a new token. A remembered sign-in lasts `CAIRN_REFRESH_TOKEN_DAYS`
(default 30), and `POST /auth/logout` ends it on the server. **Behind a
TLS-terminating proxy (Caddy with a domain, a Kubernetes ingress, a load
balancer), or one that rewrites `Host`, set `CAIRN_PUBLIC_URL` to the
browser-facing `https://` origin.** Without it the cookie is not marked
`Secure`, and refresh and sign-out can fail the same-origin check. See
[Set up your team](/guides/team/).

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
