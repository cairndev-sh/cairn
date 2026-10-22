# ADR-0143 — A tenant run's declared secrets come from its own account

**Status:** accepted (owner-approved 2026-10-22). Amends how ADR-0033 decision 3 applies to a
tenant account's run; ADR-0033 is unchanged for the deployment. Builds on ADR-0054 (accounts),
ADR-0094 (workspaces belong to an account) and the account-scoped secret references in
`secrets_store` (`mint_ref` / `resolve_secret(…, account_id=…)`).

## Context
ADR-0033 decision 3 builds an operation's `ctx.secrets` as a `ScopedSecretStore` over the
deployment's profile-bound `secrets.store` provider, which by default reads the server's
environment. #532 added a bridge that copies stored connection values for the declared keys into
`os.environ`, so operators that read the environment see them.

With accounts, one deployment runs work for several accounts. The deployment's bound provider
and the process environment are shared by every run in the process. A store that answers from
them answers with the deployment's values, whatever account the run belongs to.

## Decision
**D1 — The run's account.** A run's account is the account that owns its workspace
(`Workspace.account_id`). The deployment is the account `default` (none, empty or `default`).

**D2 — Deployment runs are unchanged.** For a deployment run `ctx.secrets` is ADR-0033 decision 3
exactly. Declared keys stored on the deployment's own connections are still bridged into
`os.environ`.

**D3 — A tenant run's declared secrets come from that account's own connections.** For a tenant
account's run, `ctx.secrets` is still scoped to the declared keys (ADR-0033 decision 3's
allow-list), but it holds only the values that account's own enabled connections supply for those
keys (`declared_secret_values`, `build_scoped_secrets(…, account_id=, values=)`). The
deployment's bound `secrets.store` provider is not consulted. Nothing is written to the process
environment. A declared key that the account does not supply raises `KeyError` on read, as an
unbacked key does today.

**D4 — Matching.** A tenant connection supplies a declared key when the connection's logical key
is the declared name, when its reference is the declared name, or when the provider catalog entry
the connection is named after maps the logical key to it (`kaggle`: `KEY` → `KAGGLE_KEY`).

## Consequences
- An operator that reads a credential from `os.environ` rather than `ctx.secrets` receives
  nothing for a tenant run. Operators that need credentials read `ctx.secrets`.
- Swapping the deployment's `secrets.store` backing (sops, OpenBao, …) affects deployment runs
  only. A tenant's credentials live behind its own connections' account-scoped references.
- Single-node and auth-off deployments, where every workspace belongs to `default`, behave as
  before.
- With auth on, resolving connections or declared secrets with no workspace in scope (no request
  and no run scoped from its record, as for a template's operation nodes on a durable runtime's
  worker) is refused (`WorkspaceScopeRequired`) rather than resolved as the deployment
  (owner decision 2026-10-22).
- A tenant run's flow legs do not use engine targets bound in the deployment's profile
  (`dataflow_target` / `ml_target`): those name deployment connections, so a tenant's flow legs
  run on targets in its own account (owner decision 2026-10-22).
