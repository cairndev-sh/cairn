# ADR-0163 — A versioned app-facing API: `/v1`, an event feed, service tokens, asymmetric signing and a governed LLM door

- **Status:** PROPOSED 2026-10-30 (approval pending maintainer; open decisions at the end)
- **Date:** 2026-10-30
- **Issue:** cairndev-sh/cairn#301 (related #297)
- **Would amend:** ADR-0120 D2 (the public HTTP API gains the `/v1` router and
  `/.well-known/jwks.json`); the auth configuration (`src/cairn/core/auth_tokens.py`);
  the event contract (`docs/contracts/pipelines-and-runs.md` §Events: a feed cursor and durable
  pipeline events); `docs/compatibility.md` (a stability rule for `/v1`).
- **Keeps:** the one launch door and ADR-0106 D1 (only a declared pipeline runs); invariant 2
  (every LangChain LLM call is built by `build_chat_model`); the per-run SSE stream and its
  `after_seq` resume; every unversioned route, token and event as it is; ADR-0120 D4 (in-process
  providers through `cairn.routers`); ADR-0133 (public text names a downstream consumer only as
  such).

## Context

A downstream consumer is moving product features out of an in-process `cairn.routers`
provider and into separate applications ("apps"). An app depends on cairn only at runtime and
never imports `cairn.core`. It needs three things from cairn:

- **Identity:** it verifies the caller's token itself, and it holds a credential of its own.
- **Governed work:** it starts a pipeline through the launch door, or makes an LLM call under
  the same governance a leg's call gets.
- **The record:** it reads runs and pipelines by ID, and follows what happens to them.

#297 removes the private-name imports for code that stays in process. This ADR decides the
out-of-process half. Verified on `main` at 15bbcfe, these are the gaps:

| Need | Today | Gap |
|---|---|---|
| A stable surface | Every route is unversioned. The routes serve the CLI and `cairn-client`, are gated by oasdiff and follow the pre-1.0 policy (`docs/compatibility.md` §Change classes: a MINOR may break). | An app pinned to one cairn version breaks on the next MINOR. Nothing says which routes an app may rely on. |
| Following the record | `GET /runs/{id}/stream` is SSE for one run, resumed by `after_seq` (`src/cairn/core/triggers/routers/run_control.py`, `dispatcher.subscribe`). `run_events` is keyed `(run_id, seq)` (`src/cairn/core/storage/sqlite/runs.py:177`, `postgres/runs.py:145`). `pipeline.<event>` goes to the bus only (`PipelineBridge._audit`, `src/cairn/core/dispatch/pipeline_bridge.py`), and the bridge's own docstring says "the durable trail is the PipelineRecord itself; this is the live signal". The Postgres bus keeps `bus_messages` and replays by `created_at` (`src/cairn/core/bus/postgres.py:48,67`). The in-process bus cannot replay. | There is no "everything since my cursor" across runs, so an app that was down misses events. Pipeline events cannot be replayed on the light stack. `created_at` is not a safe cursor, because a row committed late can carry an earlier time. |
| A service identity | The scoped tokens are run-ingest and worker-pool JWTs with route allowlists (`src/cairn/core/triggers/auth.py:214-280`). A worker-pool token "cannot be revoked short of" rotating the secret (`auth.py:255`). The console sign-in already exchanges an opaque, hashed, revocable credential for a 30-minute access token (`src/cairn/core/refresh_tokens.py`, ADR-0142). | No credential exists for an app: one bound to one workspace, revocable, and never an admin. |
| Local verification | HS256 with `CAIRN_JWT_SECRET` (`src/cairn/core/auth_tokens.py:25,102,108`). `CAIRN_JWT_ALGORITHM` exists, but encoding and decoding both use the shared secret, so setting `ES256` cannot work. | To verify a token, an app must hold the secret. Holding the secret also lets it mint any token, including an admin's. |
| Governed LLM calls | `build_chat_model` (`src/cairn/services/llm/factory.py:97`) is the chokepoint: gateway routing, `BudgetCallback` (cost into the `cost_scope`, the per-call cap) and `RedactingChatOpenAI`. Series budgets are reserved and settled in `src/cairn/core/budget_reservations.py:423,463`. Audit goes through `audit_emit` (`src/cairn/core/obs/audit.py:139`). | Only in-process code can reach the chokepoint. An app would either call a provider directly, with no redaction, budget or audit, or have to stay in process. |

## Decision

**D1 — A versioned app-facing API under `/v1`, grown only as an app needs an endpoint.**

- **Where it lives.** A public router, `triggers/routers/app_v1.py`, mounted by `build_app` with
  the ADR-0120 D2 routers. It is listed in `openapi.json` and is part of the public release.
- **Growth.** An endpoint is added only when an app needs it, by its own issue, with a
  `cairn-client` method in the same PR.
- **Every endpoint is an adapter over code that already exists.**
  - Starting work calls `resolve_start_ref` → `launch_pipeline`, the one door, which refuses
    a bare flow ref.
  - Reads use the same store reads and workspace guard as the unversioned route.
  - No endpoint has logic of its own that an unversioned route lacks.
- **The first set, each built only when needed:**
  - `GET /v1/runs/{id}`
  - `GET /v1/pipelines/{id}`
  - `POST /v1/pipelines` (the launch door)
  - `GET /v1/events` (D2)
  - `POST /v1/llm/chat` (D5)
  - `POST /v1/token` (D3)
- **The workspace comes from the token, never from the path.** A service token carries
  exactly one workspace (D3). A user token keeps today's workspace middleware.
- **Stability.** Within `/v1`, a route never changes or loses a field; adding is the only
  change. A breaking change is a new `/v2` route, served next to the old one for at least one
  MINOR. oasdiff applies this to the `/v1` paths, and `docs/compatibility.md` states it (open
  decision 5). Unversioned routes keep the current policy.

**D2 — A cursor-based event feed over the record, built on `(subject, seq)`.**

- **Identity is unchanged.** A run event is `(run_id, seq)`, as today. A pipeline event becomes
  `(pipeline_id, seq)`, in the same shape.
  - `PipelineBridge._audit` and the pipeline's terminal transitions append each
    `pipeline.<event>` to a **pipeline event log in the pipeline store**: a table with
    sqlite and Postgres twins, `{pipeline_id, seq, ts, type, data}` (open decision 1).
  - The bus publish stays as the live signal.
- **Position.** Each log gains an additive, monotonic position:
  - **sqlite:** the table's `rowid`. One writer, and the logs are append-only.
  - **Postgres:** a `BIGINT GENERATED BY DEFAULT AS IDENTITY` column plus an `xid8` column
    defaulting to `pg_current_xact_id()`. The feed returns only rows whose `xid8` is older than
    `pg_snapshot_xmin(pg_current_snapshot())`. A reader therefore never steps past a row that
    is still to commit with a lower position, which is the late-commit gap a plain sequence or
    `created_at` has.
  - These are columns on existing tables, added through the usual `add_missing_columns`
    migration. Rows written before the change get a position too, because Postgres fills an
    identity column on `ADD COLUMN`.
- **The cursor** is opaque to the app. It encodes the last position read in each log. The feed
  merges the logs by position.
- **Guarantees:**
  - Delivery is at-least-once, so a consumer de-duplicates on `(subject, seq)`.
  - Order within one run or one pipeline is its `seq`.
  - Order across subjects is by position, best effort.
- **The endpoint** is `GET /v1/events?after=<cursor>&types=run.*,pipeline.*&limit=&wait=`.
  - `types` uses the bus topic names unchanged.
  - `wait` long-polls up to a bound, woken by the store's existing event hub or the bus.
  - Results are scoped to the token's workspace.
  - SSE is not used: a long-poll page resumes by the cursor alone and passes through any proxy.
- **Retention.** The logs are the record, and the feed never deletes from them. If a retention
  policy ever deletes events, a cursor older than the retained tail gets `410`, and the app
  re-reads by ID.

**D3 — Workspace-scoped service credentials, exchanged for short-lived access tokens.**

- **Creating one.** A workspace owner or admin creates a service credential with
  `POST /v1/service-credentials` or `cairn remote service-credential create`.
  - It is an opaque random secret, shown once.
  - It is stored as a SHA-256 hash, as refresh tokens and invites are, in the existing
    refresh-token store, as a non-rotating row kind with an additive `workspace_id` and
    `name` (open decision 2).
- **Using one.** The app exchanges the credential at `POST /v1/token` (the OAuth 2.0
  client-credentials grant) for an access JWT that lives at most 15 minutes.
- **The token's claims:**
  - `sub: "service:<workspace>/<name>"`
  - `scope: "service"`
  - `ws: <workspace_id>`
  - `aud: "cairn"`
  - `iss: <the deployment's issuer>`
  - `exp`, `jti`
- **Enforcement.** The auth guard enforces `ws` against the request's workspace on every route,
  the precedent being the run-ingest and worker-pool scopes.
  - A service token never passes `require_admin`, `is_env_admin` or any account-management
    route.
  - It reaches only `/v1` routes.
- **Revocation** happens at the exchange: a revoked credential gets no new access token, and
  the old one expires within 15 minutes. That is why access tokens are short.
- **Audit.** Creating, revoking and exchanging a credential are all audited with the actor.

**D4 — Asymmetric signing, with a JWKS endpoint, so an app verifies tokens locally.**

- **The key.** A deployment that sets `CAIRN_JWT_PRIVATE_KEY` (a PEM, or a path to one) signs
  every access token with that key. The algorithm is ES256 by default (open decision 3), and
  the token header carries a `kid`.
- **Publishing.** `GET /.well-known/jwks.json` is an open path. It publishes the public half of
  the current key, plus the previous key while it rotates out (`CAIRN_JWT_PREVIOUS_PUBLIC_KEY`).
- **Local verification.** An app verifies with any JOSE library. In Python that is
  `jwt.PyJWKClient`, which PyJWT ships. The app checks `iss`, `aud`, `exp` and, for a service
  token, `ws`. An app never holds a signing key.
- **Compatibility:**
  - With only `CAIRN_JWT_SECRET` set, behaviour is unchanged (HS256).
  - With both set, new tokens are signed asymmetrically, and HS256 tokens are still accepted
    until they expire. Removing the secret then ends HS256.
  - `CAIRN_JWT_SECRET` stays the switch that turns auth on (`auth_enabled`), so no deployment
    is surprised.
- **Dependency.** `cryptography` is installed today only as a transitive dependency. The core
  declares `pyjwt[crypto]` explicitly.
- **New settings.** Two new env knobs (`CAIRN_JWT_PRIVATE_KEY`, `CAIRN_JWT_PREVIOUS_PUBLIC_KEY`)
  and an issuer (`CAIRN_JWT_ISSUER`, defaulting to the public base URL). This is what the ADR
  asks approval for (root `CLAUDE.md` §5).

**D5 — A governed LLM endpoint: a second door to the chokepoint, not a second copy.**

- **The endpoint.** `POST /v1/llm/chat` takes a subset of the OpenAI chat-completions request:
  `model`, `messages`, `temperature`, `max_tokens`, and optionally `run_id`. It returns the
  message, usage and the cost charged.
- **The handler does five things, all with existing code:**
  1. **Resolves the budget to charge** (open decision 4).
     - With a `run_id` (a run in the token's workspace, started through the launch door), the
       call is charged to that run's series and recorded in the run's cost.
     - Without one, it is charged to the service's own series, `svc:<workspace>/<name>`, under
       a cap the deployment configures. The cap is at most the deployment ceiling (ADR-0155).
  2. **Reserves** an estimate (`max_tokens` × the model's price, from `obs.cost`) with
     `budget_reservations.reserve`. A refusal is `402`, and it is audited.
  3. **Builds the model with `build_chat_model(model, max_cost_usd=…, job_tag=…)`**, invokes it
     inside `obs.cost_scope`, and so gets the gateway, redaction, `BudgetCallback` and cost
     recording of every other LangChain call.
  4. **Settles** the reservation with the actual cost: on success, failure or cancel.
  5. **Emits `audit_emit("llm.call", …)`** with the actor (the token's `sub`), the workspace,
     the model, the token counts, the cost and the `run_id`. It never logs the prompt.
- **What the handler never does:** call litellm or a provider SDK itself, redact by itself,
  or price by itself. A test asserts that the route reaches `build_chat_model`, so invariant 2
  covers this door too.
- **Model choice.** A request may name only a model the deployment's model roles allow. Any
  other model is `422`.
- **Deferred:** streaming responses, tool calls and embeddings. Each would be its own
  additive endpoint later.

## Architecture review (`/architecture-review`, 2026-10-30)

- **Affected boundaries:**
  - Surfaces: the new router, and the guard's service scope.
  - Governance: D5 calls `reserve`/`settle`, `audit_emit` and `build_chat_model`. No rule is
    added.
  - Storage: additive columns and one pipeline-event table (D2), and an additive row kind (D3).
  - Dispatch: the bridge appends pipeline events where it already publishes them.
  - Services: D5 calls the LLM factory through a function-local import, as the rule requires.
- **Dependency changes:** none forbidden.
  - The router imports dispatch, storage and governance, as the other routers do.
  - No lower layer imports `cairn.core.triggers` (ADR-0120 D6).
  - `cairn-client` stays free of `cairn.core`.
- **Ownership changes:** none. Launching stays in dispatch, the record in storage, and the
  budget and redaction rules in governance and services. D3 extends the refresh-token store
  rather than adding a store.
- **Contract changes, all additive:**
  - The `/v1` paths, `/v1/token` and `/.well-known/jwks.json` in `openapi.json`.
  - A feed cursor and durable `pipeline.<event>` rows (§Events).
  - Columns on `run_events`.
  - New `cairn-client` methods.
  - Not touched: `Run`, `RunEvent`'s fields, `PipelineDef`, `Operation`, `DataflowSpec`, and
    the entry-point groups.
- **Chokepoints:**
  - Launch: `/v1` reaches the one door. There is no second launch path.
  - LLM: D5 is a second entry to `build_chat_model`, not a copy of it. Budget reservation goes
    through the existing `reserve`/`settle`.
  - Auth: one guard, one more scope.
  - Paths not affected: template, flow and task legs, the CLI's local run, and the Temporal
    owner.
- **Invariants:**
  - 1 (`enforce_gate`): not affected. D5 invokes no operator.
  - 2 (`build_chat_model`): CONFIRMED and extended to D5, with a test.
  - 3 (policy at the launch door) and 8 (only a declared pipeline runs): CONFIRMED, since D1
    calls the door.
  - 4 (a leg's identity is recorded at admission): untouched.
  - 5 (artifacts are registered from descriptors): untouched.
- **Twins to keep in step:**
  - sqlite ↔ Postgres for the event position, the pipeline event log and the credential
    column.
  - route ↔ `openapi.json` ↔ `cairn-client`.
  - The bridge's `_audit` ↔ the Temporal pipeline activities (`_audit_step_terminal`,
    `_audit_pipeline_terminal`, `runtime/temporal/pipeline_activities.py:683,705`), which must
    append pipeline events the same way.
- **Risks:**
  - HIGH: D4 key handling, through misconfiguration or a leaked private key. Mitigations: a
    PEM in the secret store, `kid` rotation, `cairn doctor` checking the key pair.
  - MEDIUM: D2's watermark query on Postgres under load. Mitigation: an index on the position,
    with the page bound by `limit`.
  - MEDIUM: D5 becoming a general proxy. Mitigations: the allowlist of model roles, the
    per-service cap, and the audit.
  - LOW: D1 sprawl. Mitigation: an endpoint only per app need.
- **Evidence this ADR does not have yet:** a feed throughput target and an LLM endpoint latency
  budget. Each slice states its own workload and limits, and none is invented here.
- **Recommendation:** NEEDS ADR, which is this document. PROCEED slice by slice once accepted.

## Verification (`/verify-upstream`, 2026-10-30)

- **A, LangGraph:** not applicable. Nothing here is graph state.
- **B, runtimes:** Temporal and DBOS give no app-facing HTTP surface or event feed. The record
  is cairn's.
- **C, the ecosystem, in the base install:**
  - PyJWT 2.13 signs and verifies ES256, RS256 and EdDSA, and ships `PyJWKClient` for JWKS.
    It needs `cryptography`, which is installed (46.0.7) but only as a transitive dependency,
    hence `pyjwt[crypto]`.
  - FastAPI's `APIRouter(prefix="/v1")` covers versioning, so no versioning library is needed.
  - Postgres 13+ provides `xid8`, `pg_current_xact_id()` and `pg_snapshot_xmin()`. This is the
    transactional-outbox watermark, with no new infrastructure.
  - LiteLLM 1.86 ships a proxy with virtual keys and per-key budgets. It is rejected as the
    D5 door: it would be a second budget ledger and a second redaction path, beside the
    series reservations and `RedactingChatOpenAI`. It stays usable *behind* `build_chat_model`
    as the `proxy` gateway (`src/cairn/services/llm/gateway.py`, ADR-0130).
- **D, this codebase:**
  - The launch door: `dispatch/start.py`, `pipeline_launch.py:199`.
  - The scoped-token precedent: `auth.py:214-280`.
  - The hashed, revocable credential precedent: `refresh_tokens.py`, `account._hash_token`.
  - The per-run SSE and the event hub: `run_control.py`, `storage/protocols.py:362` (`_EventHub`).
  - Durable bus replay: `bus/postgres.py`.
  - The budget: `budget_reservations.py`.
  - Cost: `obs/base.py:190` (`cost_scope`).
  - Audit: `obs/audit.py:139`.
- **Not building:** a second launch path, a second LLM client, a token library, a message
  broker or a new credential store.

## Slices (each its own issue once this is accepted; one core MINOR)

1. **D4:** signing keys, `kid`, `/.well-known/jwks.json`, `pyjwt[crypto]`, and the `cairn doctor`
   row.
2. **D3:** service credentials in the refresh-token store, `POST /v1/token`, the guard's
   `service` scope, the CLI, `cairn-client`, and the audit.
3. **D1:** the `/v1` router with the first endpoints an app asks for, the oasdiff rule for
   `/v1`, and `docs/compatibility.md`.
4. **D2:** the positions on `run_events`, the pipeline event log (bridge and Temporal twins),
   and `GET /v1/events`.
5. **D5:** `POST /v1/llm/chat` over `build_chat_model`, with reservation, settlement and audit,
   and the test that pins invariant 2 to the route.

## Consequences

- An app can be built with `cairn-client`, or plain HTTP and a JOSE library, against a stable
  `/v1`, with no import of `cairn.core`.
- In-process providers keep working (#297, ADR-0120 D4). An app can move out of process one
  feature at a time.
- Two auth modes exist while a deployment migrates from HS256. `cairn doctor` reports which is
  active.
- `run_events` and the pipeline store grow additive columns, and the pipeline store gains one
  table. Back up before the migration, as for any schema change.

## Alternatives rejected

- **Version the existing routes in place** (a header, or `/v1` aliases of every route). That
  freezes surfaces built for the CLI and the console, which change by design, and it doubles
  the oasdiff surface for no consumer.
- **Feed from `bus_messages` replay.** It is durable only with `CAIRN_BUS=postgres`, has no
  replay on the light stack, and its `created_at` cursor has the late-commit gap.
- **Long-lived service JWTs plus a denylist.** An app that verifies locally cannot see the
  denylist, so a revoked token would keep working at every app until it expired.
- **Share the HS256 secret with apps.** Verifying would then also mean being able to mint any
  token, including an admin's.
- **Apps call providers directly, or through a LiteLLM proxy key.** That bypasses redaction,
  the series budget and the audit. It is the second copy the root `CLAUDE.md` §5 forbids.

## Open decisions (maintainer)

1. **Where durable pipeline events live (D2).**
   - (a) A pipeline event log table in the pipeline store, with sqlite and Postgres twins.
     **Recommended:** one shape with run events, works on every stack, additive.
   - (b) Pipeline rows in `run_events` under the `pl_…` id. No new table, but `run_id` would
     then name something that is not a `Run`, which changes the meaning of a public record.
   - (c) `bus_messages` replay. Postgres-bus only.
2. **The service credential's form (D3).**
   - (a) An opaque credential in the refresh-token store, exchanged for 15-minute JWTs.
     **Recommended.**
   - (b) Long-lived JWTs plus a denylist checked only by core.
   - (c) A new credential store.
3. **The signing algorithm (D4).**
   - (a) ES256. **Recommended:** small keys, and every JOSE library verifies it.
   - (b) EdDSA (Ed25519). Faster, but some older libraries lack it.
   - (c) RS256. The widest support, with large keys.
4. **What the LLM endpoint charges (D5).**
   - (a) The `run_id`'s series when given, otherwise the service's own capped series.
     **Recommended.**
   - (b) A `run_id` is always required, so an app first launches a pipeline for any LLM use.
   - (c) The service series only.
5. **Stability of `/v1` (D1).**
   - (a) Additive only within `/v1`, and a break is `/v2` next to it for at least one MINOR.
     **Recommended:** an app needs a promise the pre-1.0 policy does not give.
   - (b) Follow the pre-1.0 policy (a MINOR may break) until 1.0.
