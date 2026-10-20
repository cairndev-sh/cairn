# Security model

One page; the code is the authority (`path:line`, OBSERVED on `main`). Deployment specifics
live in `deploy/light/README.md`; connections in `docs/connections.md`.

## Trust boundaries
| Boundary | Trusts | Does not trust | Enforced by |
|---|---|---|---|
| Control plane (server, dispatcher, bridge, stores) | its own database and profile | anything a box returns beyond scalars + descriptors (ADR-0081 D3) | `DataflowResult` shape (`src/cairn/core/dataflow/contract.py:262`); registration control-side |
| Execution box / engine subprocess | the spec it was handed | ambient credentials (engines never read them; `engine.py:76` docstring) | scoped secret injection (ADR-0089 D3); `CAIRN_DATAFLOW_SPEC` env |
| Packs and operator packs | — installed packs are **trusted code** (entry points execute on load) | — | install is the trust decision; manifest ↔ class drift tests; `cairn.lock` pins the shipped closure |
| Console / API clients | a bearer token (`CAIRN_JWT_SECRET`) and workspace membership (`assert_can_write_workspace`) | the `X-Cairn-Workspace` header alone (`routers/start.py:150-157`) | `require_auth`; workspace middleware |
| Webhook senders | a valid signature over the body, keyed with a non-empty secret, on the receiver's own method + path under `/triggers/` | an empty secret (refused, never public); any other method or route at that path; the `X-Cairn-Workspace` header (the run lands in the workspace that declared the receiver) | `_mount_trigger_routes` (`triggers/server.py`); `verify_signature` (`triggers/router.py`); `ingress_path_problem` (`engine/triggers_spec.py`) |
| Human approvers | the identity recorded on the gate (`StepState.approver`); the gate's `approvers:` (a username or a role; owner/admin always; an unknown name → owner/admin only, #2393) | a body `approver` field (#2310); a name the gate does not list (403, audited `operation.approval` `deny`); `/advance` on a gate (409, #2388) | `POST /pipelines/{id}/steps/{step}/approve` (auth-required, `_assert_may_decide`; declared gate steps only) |

**Known gap:** with `CAIRN_JWT_SECRET` unset the control plane starts OPEN and says so at
startup (`AUTH DISABLED …`, `src/cairn/core/triggers/server.py`); any authenticated user can
approve any gate (`15-architecture-risks.md` C2, #1486). Never expose an unset-secret
server beyond localhost.

## Secrets and credentials
Declared connections, never values in pack data or specs: `secrets/env`, `secrets/infisical`
operators; `.env` on the box (Infisical profile replaces it); provider tokens live in
connections. Secrets reach a flow as a scoped set of what its stages declare (ADR-0089 D3),
not wholesale env (the pre-#1344 shape is recorded debt for the Metaflow adapter).
Never log or echo a secret; the redactor (`src/cairn/core/obs/redact.py`, `redact.text` in
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
(never `:latest`, `RUNBOOK-gpu-finetune-ssh.md`); the box is a declared `compute.ssh`
connection; provisioning engines (SkyPilot) sit behind a door and a cost gate that run
**before** any cloud spend (`src/cairn/core/dataflow/engine_door.py` (the flow-engine door, ADR-0134 D3c; SkyPilot answers through its `check_ready()`); ADR-0104 D5); compute
acquisition/release is a governed pair with a lease sweep (ADR-0097). Budget is reserved
per leg before submit (`leg_executor.py:303`) and settled after.

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
Security findings go to the maintainer privately (see `SECURITY.md` at the repo root if
present; otherwise a private issue), not to a public issue.
