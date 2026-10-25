# Security model

One page; the code is the authority (`path:line`, OBSERVED on `main`). Deployment specifics
live in `deploy/light/README.md`; connections in `docs/connections.md`.

## Trust boundaries
| Boundary | Trusts | Does not trust | Enforced by |
|---|---|---|---|
| Control plane (server, dispatcher, bridge, stores) | its own database and profile | anything a box returns beyond scalars + descriptors (ADR-0081 D3) | `DataflowResult` shape (`src/cairn/core/dataflow/contract.py:262`); registration control-side |
| Execution box / engine subprocess | the spec it was handed | ambient credentials (engines never read them; `engine.py:76` docstring) | scoped secret injection (ADR-0089 D3); `CAIRN_DATAFLOW_SPEC` env |
| Packs and operator packs | — installed packs are **trusted code** (entry points execute on load) | — | install is the trust decision; manifest ↔ class drift tests; `cairn.lock` pins the shipped closure |
| Console / API clients | a bearer token (`CAIRN_JWT_SECRET`) and workspace membership (`assert_can_write_workspace`) | the `X-Cairn-Workspace` header alone (`routers/start.py:150-157`); a workspace id no store holds, with auth on (404, like another account's); the implicit workspace (`CAIRN_WORKSPACE`, else `default`) when the caller's account does not own it or it does not exist (the request acts in the account's home workspace, or 404 without one) | `require_auth`; workspace middleware (`_WorkspaceMiddleware`, `triggers/server.py`); `assert_workspace_allowed` (`workspace.py`) |
| Webhook senders | a valid signature over the body, keyed with a non-empty secret, on the receiver's own method + path under `/triggers/` | an empty secret (refused, never public); any other method or route at that path; the `X-Cairn-Workspace` header (the run lands in the workspace that declared the receiver, queued or inline); a webhook path, trigger id or name another workspace already holds (not armed; a path that still reaches the router from two workspaces mounts no receiver). Exception: a workspace's webhook still replaces a pack/overlay receiver at the same path | `_mount_trigger_routes` (`triggers/server.py`); `verify_signature` and `TriggerRouter.from_installed` (`triggers/router.py`); `_converge` (`engine/triggers_grammar.py`) and the trigger store's `create` (`storage/trigger_store.py`); `ingress_path_problem` (`engine/triggers_spec.py`) |
| Human approvers | the identity recorded on the gate (`StepState.approver`); the gate's `approvers:` (a username or a role; owner/admin always; an unknown name → owner/admin only, #2393) | a body `approver` field (#2310); a name the gate does not list (403, audited `operation.approval` `deny`); `/advance` on a gate (409, #2388) | `POST /pipelines/{id}/steps/{step}/approve` (auth-required, `_assert_may_decide`; declared gate steps only) |

**Console sign-ins (ADR-0142).** A console sign-in is a short access token plus an httpOnly,
`SameSite=Strict` refresh cookie (`Path=/auth`); the server stores only the cookie token's
SHA-256 (`src/cairn/core/refresh_tokens.py`). `POST /auth/refresh` needs `X-Cairn-Refresh: 1`
and a same-origin `Origin`/`Referer`, rotates the token, and revokes the whole family when a
rotated token comes back ten seconds or more after its rotation; within ten seconds it renews
instead, revoking the child that rotation issued, so a replay inside that window is not
detected as theft at first (ADR-0144); the displaced child presented ten seconds or more later
revokes the family and logs a WARNING (ADR-0145); `POST /auth/logout` (same header and origin check), a password
change and disabling a user revoke the user's families
(`triggers/auth.py`, `account.py`). An issued access token stays valid until it expires, and
rotating `CAIRN_JWT_SECRET` does not end refresh families.

**Artifact registry (ADR-0147, ADR-0148).** Tags, artifact reads and lineage are no longer
deployment-wide: a tag is keyed by `(workspace_id, name)`, and an artifact is visible only to the
workspaces that registered it. Identical bytes stay one content-addressed identity, but each
registering workspace keeps and reads only its own registration (location, durable copy,
provenance, stats) and the parents it declared (`artifact_owners`, `artifact_parents`). Another
workspace's artifact, tag or lineage node reads as not found, and `promote` refuses an artifact
the workspace did not register (`src/cairn/core/storage/artifacts.py`). With auth on, an artifact
or tag call with no workspace named or in scope raises `WorkspaceScopeRequired`. `count` is still
the deployment's row count.

**Known gap:** with `CAIRN_JWT_SECRET` unset the control plane starts OPEN and says so at
startup (`AUTH DISABLED …`, `src/cairn/core/triggers/server.py`); any authenticated user can
approve any gate. Never expose an unset-secret server beyond localhost.

## Accounts and workspaces (0.2.0)
With auth on, one deployment serves several accounts. The boundaries below are what 0.2.0
added; with auth off, or on a single-node deployment where everything belongs to the
`default` account and workspace, they change nothing.

- **Connections and secrets are scoped to the run's account** (the account that owns the
  run's workspace; ADR-0143). A team account resolves only its own connections, and only
  secrets stored under `cairn://secrets#<account>/…`; an environment-variable name,
  `${env:NAME}` and the deployment's stored secrets resolve to nothing for it
  (`src/cairn/core/connections.py`, `src/cairn/core/secrets_store.py`). Its declared
  secrets reach `ctx.secrets` (a team account's run does not receive them in the process
  environment), and the bound `secrets.store` provider is not read for it. A connection bound
  in the deployment's profile (`ml_target`, `dataflow_target`) belongs to the deployment and
  is not used for a team account's flow legs; they use a target connection of the account's
  own, named in the pipeline or flow.
- **No workspace in scope is refused.** With auth on, resolving a connection, a secret or a
  dataset reference outside any workspace raises `WorkspaceScopeRequired`
  (`src/cairn/core/workspace.py`) instead of resolving as the deployment.
- **Datasets** resolve within the run's workspace: `dataset:` refs and `@latest` see only
  that workspace's versions.
- **Pull-lane claims and heartbeats** (`POST /jobs/claim`) need a worker-pool token
  (`cairn remote worker-token`) or the deployment admin's token; a signed-in user's token
  gets `403`.
- **Pack install and uninstall** over the API (`POST /registry/install|uninstall`) are
  deployment-admin only (`CAIRN_API_USER`).
- **Deciding a paused saga** through `POST /triggers/approve/{workflow_id}` checks the
  saga's account through its action handler's `workspace_of`; without one, only the
  deployment admin may decide it.
- **MCP servers and corpus sources** default to closed: `mcp_server_spec`,
  `resolve_corpus_source` and `index_workspace_corpus` take a stdio server, a private or
  local address, or a local path only with `allow_stdio`, `allow_private_network` or
  `allow_local`, which are for the deployment's own callers. The address check runs when
  the spec is built and resolves the host at that time; `CAIRN_PROBE_ALLOW_PRIVATE` allows
  private addresses for deployments that need them (`src/cairn/core/clients/mcp_client.py`).
- **Run hooks on `pipeline.launching` fail closed:** a provider that raises anything other
  than `AdmissionRefused` refuses the launch. Every other event still fails open.

## Secrets and credentials
Declared connections, never values in pack data or specs: `secrets/env`, `secrets/infisical`
operators; `.env` on the box (Infisical profile replaces it); provider tokens live in
connections. Secrets reach a flow as a scoped set of what its stages declare (ADR-0089 D3),
not wholesale env (the pre-#1344 shape is recorded debt for the Metaflow adapter).
For a team account's run, the
connections and secrets in scope are that account's own (see *Accounts and workspaces*
above). Never log or echo a secret; the redactor (`src/cairn/core/obs/redact.py`, `redact.text` in
`src/cairn/profiles/secure.toml`) runs on LLM traffic and logs; every LLM call goes through
`build_chat_model` (`services/llm/factory.py:97`) — invariant 2.

## Subprocess and shell safety
The builtin engine runs the kernel process with `create_subprocess_exec` (argv, no shell),
in its own session so cancel kills the whole tree (`operator_dataflow/engines.py:76-80`);
stdout/stderr are not inherited. Operators that shell out must use argv lists, never
`shell=True` or string interpolation into a command (Semgrep rule `cairn.subprocess-shell`).
`cairn run --interactive|--mock|--dry-run` executes flows in the caller's process by
decision (ADR-0106 D6) and writes no record.

## Remote and cloud execution
Work moves as an **image + a spec**: the GPU trainer image is built by CI and pinned by tag
(never `:latest`); the box is a declared `compute.ssh`
connection; provisioning engines (SkyPilot) sit behind a door and a cost gate that run
**before** any cloud spend (`src/cairn/core/dataflow/engine_door.py` (the flow-engine door, ADR-0134 D3c; SkyPilot answers through its `check_ready()`); ADR-0104 D5); compute
acquisition/release is a governed pair with a lease sweep (ADR-0097). Budget is reserved
per leg before submit (`leg_executor.py:303`) and settled after.
On a `compute.ssh` box the job receives only the spec, the live channel, `MLFLOW_*` and the
env NAMES the deployment lists in `remote_env`; a name that looks like a credential (`KEY`,
`SECRET`, `TOKEN`, `PASSWORD`) refuses the leg, so cloud credentials stay box-side (an instance
role). Forwarded values sit only in the job's `runner.sh`, created exclusively with mode 0600, never on a
command line; the persisted job handle (served by `GET /pipelines/{id}`) records their names
only. Job records written before this release may still hold the forwarded values, including
`MLFLOW_TRACKING_PASSWORD`; the API no longer shows them (each value reads `[hidden]`, the
names stay), but they were readable before, so a deployment that ran flow legs on a
`compute.ssh` box should rotate those credentials. Cancel and checkpoint pull-back authenticate with the `PRIVATE_KEY` of the connection the
job was submitted with, resolved in the account recorded at submit, and log a warning, without
secret material, when the box cannot be reached (a failed cancel is reported as failed)
(ADR-0149; `packages/cairn-operator-dataflow/DESIGN.md` §4).

## Approval and audit
Every operator invocation passes `enforce_gate` (`approval/enforce.py:116`); flow legs pass
`enforce_flow_gate` on aggregate risk; policy `deny` refuses at the launch door before a
record exists (`policy/pipeline_gates.py:93`). Governed actions emit audit events
(`obs/audit.py:140`; `pipeline.<event>` on the bus); the durable trail is the record itself.
Admission doors for dataset verdicts/holdout/licence run per run on the agentic path and once per
launch at the pipeline door (`src/cairn/core/admission_door.py`, #2190), which also applies the
policies' required checks and records the decision on the pipeline record.

## Network assumptions
The server binds behind Caddy on prod (TLS at the edge); the job channel is the only path a
box calls back on (idempotent ingest, spooled); the control plane never opens inbound ports
to boxes. Console assets are static; the API is the only console↔engine contract.

## Reporting
Security findings go to the maintainer privately, as `SECURITY.md` at the repository root
describes, not to a public issue.
