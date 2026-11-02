# Runbook — the engine refuses to start: no deployment profile

**Symptom.** `cairn-server` or `cairn-worker` exits at startup with
`refusing to start: CAIRN_PROFILE is unset — a deployment must name its profile` (or
`CAIRN_PROFILE=… cannot be loaded`). `docker compose up` (the unified file and both DBOS tiers) stops with
`set CAIRN_PROFILE to the deployment profile`. `helm template` fails with
`env.profile is empty`. `cairn admin deploy doctor` reports `CAIRN_PROFILE  REQUIRED`.

**Cause (#2296).** Every deployment names its profile. The profile holds the capability
bindings (`[profile.bindings]`), redaction and audit, and the backends. Without a profile,
a `capability:` operation has no provider (ADR-0111 D10, no fallback), so the engine
refuses at startup rather than failing legs mid-run. A file that is missing or malformed
refuses too. There is no default for prod or Helm, by decision.

## Which profile

The profile sets the governance posture. The **runtime** is set separately: `CAIRN_RUNTIME` in
compose, `runtime:` in Helm (default in-process). A profile overrides it only when it pins
`durable_runtime` (#1852). Since #2296 the `local-dev` base layer pins nothing, so `secure.toml`
follows the deployment's runtime.

The engine ships only profiles whose every binding resolves against the builtin operators
(ADR-0134 D5): `local-dev`, `sandbox`, `secure` and `prod-infisical`.

| Deployment | `CAIRN_PROFILE` (path inside the engine image) |
|---|---|
| Production, any runtime (in-process, DBOS, Temporal; compose or Helm) | `/app/.venv/lib/python3.13/site-packages/cairn/profiles/secure.toml` |
| Production reading secrets from Infisical (the `infisical` compose profile) | `…/cairn/profiles/prod-infisical.toml` |
| Light quickstart (`deploy/light`) | `…/cairn/profiles/secure.toml` (already set in its compose file) |
| Local development | none to set: `cairn setup`, `make serve`, `.claude/launch.json`, `docker-compose.dev.yml` and the dev-loop commands (`cairn run`, `cairn run --dry-run`, `cairn pack check`) use `local-dev.toml` |

`local-dev` and `secure` are base layers: your own profile can `extends = "secure.toml"` (the
path is resolved beside the file that names it, so keep your profile in the same directory, or
give the full path). A profile binds every capability its templates use, including the ones
an installed operator pack adds, or those templates refuse to compile (ADR-0111 D10: no
fallback provider). Pin `durable_runtime` in `[profile.backends]` to fix the runtime by profile
rather than by `CAIRN_RUNTIME`.

## Fix
- **Compose:** set `CAIRN_PROFILE=` in `.env`, then run
  `cairn admin deploy doctor --env .env` and `docker compose up -d server worker`.
- **Helm:** set `env.profile` in your values file, then run
  `cairn admin deploy doctor --values <file> --strict` and `helm upgrade`.

## Verify
On the box, `cairn setup --check` shows the loaded profile and its bindings instead of
`CAIRN_PROFILE unset — defaults active`.
