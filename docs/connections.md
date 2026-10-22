# Connections

A **connection** is one reusable, typed binding to external infra — a model API,
a compute target, an MCP server, or an OAuth-brokered SaaS. Connections are
managed with `cairn connect` and consumed at runtime via `resolve_connection`.

Two principles hold for every kind:

- **Secrets are references, never values.** A connection stores a SecretStore
  reference (e.g. `OPENAI_API_KEY`), resolved at use time. `cairn connect` never
  reads, prints, or persists a raw secret value.
- **Durable by default.** Connections live in sqlite at `~/.cairn/connections.db`
  (override with `CAIRN_CONNECTIONS_DB`; `=:memory:` opts into a volatile store).
  The CLI, the admin panel, and `resolve_connection` all agree on this location.

## The CLI

```bash
cairn connect providers              # what you can connect (the catalog)
cairn connect add openai             # zero-config: refs OPENAI_API_KEY
cairn connect add docker --set host=tcp://host:2375
cairn connect add anthropic --name claude-prod --secret API_KEY=MY_ANTHROPIC_KEY
cairn connect ls                     # names + kinds + secret KEYS (never values)
cairn connect test <name>            # resolves refs; reports presence only
cairn connect rm <name>
```

`add` builds from the **provider catalog** — each entry knows the connection
`kind` and the secret refs a provider needs, so `cairn connect add openai`
produces a working `api_key` connection pointed at `OPENAI_API_KEY` with no
flags. Aliases: `hf`→huggingface, `claude`→anthropic, `google`→gemini, `k8s`→kube.

## Accounts

With auth on, a connection belongs to the account that created it. A connection
of any account other than the deployment's own `default` resolves only the secret
references stored for that account (`mint_ref(conn, key, account_id=…)` makes
`cairn://secrets#<account>/<conn>-<KEY>`; `resolve_secret(ref, account_id=…)` and
`put_secret(ref, value, account_id=…)` hold to it). A bare name or `${env:NAME}`
(the server's environment), the deployment's stored secrets and another account's
resolve to nothing, the catalog's environment-variable defaults are not applied
(`connection_from_provider(…, account_id=…)` refuses a reference from outside the
account), and an OAuth connection reads the token stored under its own name. The
`default` account, and with it every single-node or auth-off deployment, is
unchanged, and references stored before accounts were namespaced keep resolving
for it.

A run resolves connections and secrets in its own account: the account that owns
the run's workspace (`workspace_account(workspace_id)`). These run in the workspace
their record names (`workspace_scope`), so the account follows the record rather
than whichever request or worker happens to execute them: a template run, an
in-process resume, a pipeline's template, task and flow legs (in-process and under
the Temporal owner), and a remote job's poll and relaunch. With auth on, a record
that names no workspace, or a workspace no store holds, is refused rather than
resolved as the deployment's, and so is a lookup made outside any workspace scope
(no request and no run scoped from its record, as for a template's operation
nodes on a durable runtime's worker): it raises `WorkspaceScopeRequired`. The
deployment's own tools (`cairn connect test`, `cairn doctor`, `cairn compute
status`) look up the deployment's connections explicitly. Work an operator starts
from inside a run (`ctx.launch`) starts in the run's workspace; with auth on and
no workspace in scope it is refused. With auth on, a pipeline step whose record
names no workspace ends failed (verdict `workspace_unresolved`) and fails its
pipeline once; a leg in that state marks its run failed.
`resolve_connection(name, account_id=…)` looks a name up in one account only:
when `account_id` is omitted it is the current run's (`run_account()`). A tenant
account's run finds only its own rows; the deployment (`default`) finds only the
deployment's. No row is shared across accounts by name. The same holds for
`target_cap`, the model licence gate's token check, preflight's connection
warnings and the templates contract's connected-MCP list. A profile-bound backend
connection (`backends.<field>_connection`) is the deployment's and always resolves
in `default`. A flow leg's engine target (`dataflow_target` / `ml_target`) is
looked up in the run's account, so a tenant's flow legs run on targets in its own
account. A target bound in the deployment's profile is a deployment connection: a
tenant's flow leg that names it does not resolve it, and the leg fails.

An operator's declared secrets (`secrets:` in its binding) are resolved in the
run's account too (ADR-0143). For the deployment they are bridged into the process
environment as before (`inject_declared_secrets`). A tenant account's values are
never written to the process environment: `declared_secret_values` reads them
from the account's own connections, and they back that invocation's
`ctx.secrets` alone; the deployment's bound SecretStore is not consulted. A
declared key matches a tenant connection's secret when the logical key is the
declared name, or when the provider catalog entry the connection is named after
maps that key to it (`kaggle`: `KEY` → `KAGGLE_KEY`). An operator that reads a
credential from `os.environ` instead of `ctx.secrets` receives nothing for a
tenant run.

Connection names are still unique across the whole deployment (the `connections`
table's `name` column and the id derived from it), so two accounts cannot use the
same name.

An MCP connection becomes a client spec through `mcp_server_spec`, which fails
closed: a `stdio` server (a command run on the cairn server) is built only when
the caller passes `allow_stdio=True`, and an http/sse `url` must pass the probe's
address guard (no loopback, link-local, cloud metadata or private address) unless
the caller passes `allow_private_network=True`. Only the deployment's own callers
pass either. Without `allow_private_network` the spec also carries an
`httpx_client_factory` whose client does not follow redirects, so the checked
address is the only one the client connects to; with it the spec keeps the MCP
SDK's default client. A tenant spec therefore holds a callable and is not
JSON-serialisable.

A knowledge corpus source (`resolve_corpus_source`, `index_workspace_corpus`) is
cloned only from a public `https://` git URL unless the caller passes
`allow_local=True`. The clone is limited to https, does not follow redirects, and
runs without the server's system or global git config (credential helpers, extra
headers, URL rewrites), credential helper, askpass or a terminal prompt. Its
environment is an allowlist: `PATH`, `LANG` / `LC_*`, `TMPDIR`, the proxy
variables (`HTTP_PROXY`, `HTTPS_PROXY`, `NO_PROXY`, either case) and the CA
variables (`SSL_CERT_FILE`, `SSL_CERT_DIR`, `GIT_SSL_CAINFO`,
`REQUESTS_CA_BUNDLE`); `HOME` and `XDG_CONFIG_HOME` point at an empty temporary
directory. So a deployment that sets `http.proxy` or `http.sslCAInfo` only in its
gitconfig must set the matching environment variable for tenant clones to use it.

Known limits of the address guard, shared by the probe, MCP specs and corpus
clones: it resolves the host once, before connecting, so it does not defend
against a DNS answer that changes between the check and the connection (DNS
rebinding); and a host that does not resolve at check time passes it. Setting
`CAIRN_PROBE_ALLOW_PRIVATE` lifts the private-address rule for all three, so on a
multi-tenant deployment it also lets tenant MCP URLs and corpus clones reach
private addresses (loopback, link-local and cloud metadata stay refused).

Library callers: `mcp_server_spec`, `resolve_corpus_source` and
`index_workspace_corpus` now default to the tenant-safe behaviour. Code that
calls them for the deployment itself and relied on stdio servers, internal MCP
URLs or local / ssh corpus sources must pass `allow_stdio=True`,
`allow_private_network=True` or `allow_local=True`.

## OAuth connections (ADR-0037)

Providers that use OAuth 2.0 (GitHub, Slack, Jira, Notion, Google) are brokered.
cairn depends on a **replaceable `OAuthBroker` seam**, not a vendor SDK. The
broker runs the auth-code dance and owns token refresh; cairn fetches a **fresh
access token** at resolve time and **makes the actual API call itself** — so the
request/response never leave your infra and every use still passes the
`invoke_operator` chokepoint (cost / policy / redaction / audit / HITL).

```bash
cairn connect add github --via oauth          # persists a kind=oauth connection,
                                              # starts the flow, prints a redirect URL
cairn connect authorize github                # re-run the flow (retry / re-consent)
cairn connect test github                     # ✓ once the token exists at the broker
```

Flags: `--broker <name>` (pins the broker per-connection; otherwise the global
`CAIRN_OAUTH_BROKER` default applies at resolve time), `--connection-id <id>`
(default: the connection name), `--scope <s>` (repeatable), `--name <name>` (for
multiple accounts of the same provider).

Downstream, an OAuth connection resolves to `secrets={"ACCESS_TOKEN": <fresh>}` —
operators consume it identically to an `api_key` connection. The broker is
invisible below `resolve_connection`.

### Why a broker (and why Nango, not Composio)

- **Nango is auth-only**: it hands cairn a token; **cairn makes the call**
  (`cairn → target`). Tokens live in Nango's own Postgres, on your infra.
- **Composio executes tools on its cloud** (`cairn → Composio → target`) with a
  closed, cloud-only token vault — the call runs where cairn can't govern it.

The deciding factor is token custody + execution locus, not the license. See
ADR-0037. (OAuth *providers* — Hydra/Authentik/Zitadel/Dex — are the inbound
"SSO into cairn-server" role and are unrelated to this.)

## Running the Nango sidecar (default broker)

Nango is impl #1. "Just auth" needs Postgres + Redis + the Nango server (logs/ES
and S3 are omitted). It's wired as the `nango` profile in `docker-compose.dev.yml`.

### 1. Set the encryption key and start it

The image (`nangohq/nango-server:hosted`, Nango's OSS self-host) is pinned in the
compose. You only need a base64 32-byte encryption key (it's `linux/amd64`, so on
Apple Silicon it runs under emulation — fine, just slower to boot):

```bash
export NANGO_ENCRYPTION_KEY=$(openssl rand -base64 32)
# optional: NANGO_DASHBOARD_USERNAME / NANGO_DASHBOARD_PASSWORD (default admin/admin)
docker compose -f docker-compose.dev.yml --profile nango up -d
# server REST API → http://localhost:3003   ·   Connect UI → http://localhost:3009
curl -s http://localhost:3003/health      # → {"result":"ok"} once migrations finish (~15s)
```

### 2. Get the environment Secret Key and point cairn at it

Either copy it from the dashboard (`http://localhost:3003` → Environment Settings →
Secret Key), or read it from Nango's DB (self-host stores it on the `dev`
environment):

```bash
export CAIRN_NANGO_HOST=http://localhost:3003
export NANGO_SECRET_KEY=$(docker exec cairn-nango-db-1 \
  psql -U nango -d nango -tA -c "SELECT secret_key FROM _nango_environments WHERE name='dev';")
# cairn resolves NANGO_SECRET_KEY via the SecretStore (ref CAIRN_NANGO_SECRET_REF), env fallback
```

### 3. Configure the GitHub integration in Nango

This is Nango's job, not cairn's, and it needs *your* GitHub OAuth app:

1. GitHub → Settings → Developer settings → **OAuth Apps** → New OAuth App.
   Authorization callback URL: `http://localhost:3003/oauth/callback`.
2. In the Nango dashboard → Integrations → add **GitHub**, paste the OAuth app's
   Client ID + Client Secret, set the scopes you need.

## Live verification checklist

This cannot be claimed from cairn's hermetic tests — run it against the real
sidecar (steps 1–3 above first):

1. `cairn connect add github --via oauth` → prints a redirect URL.
2. Open the URL, complete GitHub consent → Nango stores the connection.
3. `cairn connect test github` → **✓ usable** (cairn fetched a fresh token).
4. `cairn connect rm github` to clean up.

> Verified so far against Nango v0.70.8: the sidecar boots from this compose, the
> secret key authenticates, and cairn's `--via oauth` reaches `POST /connect/sessions`
> correctly (it 400s only until the GitHub integration in step 3 exists). The
> browser-consent round-trip is the part this checklist covers.

## Swapping the broker later (replaceability)

The seam is the point. To replace Nango:

1. Implement `OAuthBroker` (`src/cairn/core/oauth/broker.py`: `start_authorization` /
   `get_access_token` / `revoke`) and `register_broker("yourbroker", YourBroker)`.
2. Select it: `CAIRN_OAUTH_BROKER=yourbroker` (global) or `--broker yourbroker`
   (per-connection, stored in `config["broker"]`).

No call site changes — cairn only ever touches the Protocol and
`Connection.config["broker"]`. A future Authlib-based broker that keeps tokens in
cairn's own store drops in the same way.
