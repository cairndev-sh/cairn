# cairn-light — the quickstart stack

Three containers, no accounts, no keys, no `.env`: the engine image built from
this repository, in its **light** posture (the `light` feature preset in
`src/cairn/core/features.py`), serving the HTTP API behind Caddy.

```
client ──▶ caddy :8080 ──▶ server :9090 (in-process runtime + scheduler)
                              └ runs · checkpoints · flags ▶ postgres
```

## Bring it up

```bash
git clone https://github.com/cairndev-sh/cairn && cd cairn/deploy/light
docker compose up -d --build
```

The first build compiles the engine image (several minutes; later builds reuse
the cached layers). When `docker compose ps` shows `server` as **healthy**, the
API answers on **http://localhost:8080**. Check it and drive it with the CLI:

```bash
curl -s http://localhost:8080/healthz
cairn remote ps --endpoint http://localhost:8080
```

Light runs the `secure` profile: every run leaves a hash-chained audit receipt,
and text is redacted before it reaches an LLM. A workflow with an LLM step needs
a provider key: put `OPENAI_API_KEY=sk-…` in `deploy/light/.env` and run
`docker compose up -d` again.

## Three add-ons

All are compose profiles. Enable with `--profile <name>` on the command
line, or `COMPOSE_PROFILES=llm-gateway,backup` in `.env`.

| Profile | Adds | Needs |
|---|---|---|
| `llm-gateway` | the bundled LiteLLM proxy (`litellm-config.yaml` beside this file) — every governed LLM call through one audited door | `LITELLM_MASTER_KEY` (`openssl rand -hex 24`), set **before** enabling the profile, since an empty key starts the proxy open; then `LLM_GATEWAY=proxy`, `LLM_GATEWAY_URL=http://litellm:4000`, `LLM_GATEWAY_API_KEY=<that key>` |
| `backup` | a `pg_dump` of the app database every `CAIRN_BACKUP_INTERVAL_SECONDS` (default daily), keeping `CAIRN_BACKUP_KEEP` (default 14) in the `pgbackups` volume | nothing |
| `infisical` | a self-hosted [Infisical](https://infisical.com) secrets server on `127.0.0.1:8085` (its own Postgres and Redis) for the builtin `secrets/infisical` operator — operations read their `secrets:` from it instead of the plaintext env | `INFISICAL_ENCRYPTION_KEY` (`openssl rand -hex 16`), `INFISICAL_AUTH_SECRET` (`openssl rand -base64 32`) and `INFISICAL_DB_PASSWORD` in **`.env.infisical`** (copy `.env.infisical.example`), not `.env` — everything in `.env` reaches the engine container, `.env.infisical` only the Infisical services; the server refuses to start without the two keys; then a machine identity (universal auth) made in its web UI, and for the engine `CAIRN_PROFILE=…/cairn/profiles/prod-infisical.toml`, `INFISICAL_HOST=http://infisical:8080`, `INFISICAL_CLIENT_ID`, `INFISICAL_CLIENT_SECRET`, `INFISICAL_PROJECT_ID` (see `.env.example`); the engine image already carries the Infisical SDK (the `infisical` extra) |

## Feature flags

`CAIRN_LIGHT=on` applies the `light` preset. To turn one feature back on for
this deployment, set `CAIRN_FEATURE_<NAME>=on` in `.env`; `CAIRN_LIGHT=off` is
the everything-on posture.

## Deploy for a team

Light is keyless so the first run costs nothing. To put it in front of other
people, set in `.env`:

```
CAIRN_JWT_SECRET=<openssl rand -hex 32>
CAIRN_API_USER=admin
CAIRN_API_PASSWORD=<choose one>
# invite (default) | domain | open
CAIRN_SIGNUP=invite
# How long a remembered console sign-in lasts, in days (default 30)
# CAIRN_REFRESH_TOKEN_DAYS=30
```

and `docker compose up -d`; every API call then needs a token
(`cairn remote login --endpoint http://localhost:8080`).

To serve it on a domain, set `CADDY_DOMAIN=cairn.example.com` (Caddy then
provisions TLS; publish port 443 as well) and set `CAIRN_PUBLIC_URL` to the
same browser-facing origin:

```
CADDY_DOMAIN=cairn.example.com
CAIRN_PUBLIC_URL=https://cairn.example.com
```

`CAIRN_PUBLIC_URL` is required once Caddy serves https: the console's
`cairn_refresh` sign-in cookie is marked `Secure` only when it is an `https://`
origin, and refresh and sign-out check the request against it.

A `cairn worker --pull` against this stack needs a worker-pool token
(`cairn remote worker-token --name <pool>`, run where `CAIRN_JWT_SECRET` is
set) passed as `--token`; a signed-in user's token gets `403` on `/jobs/claim`. For a cluster
with TLS, backups and the durable runtimes, use the Helm chart in
[`../helm/cairn`](../helm/cairn/README.md).

## Ops

| Task | Command (from `deploy/light`) |
|---|---|
| Status | `docker compose ps` |
| Logs | `docker compose logs -f server` |
| Stop | `docker compose down` (keeps the volumes) |
| Reset | `docker compose down -v` (drops runs, flags, backups) |
| Update | `git pull && docker compose up -d --build` |
| Change the port | `CAIRN_PORT=9000 docker compose up -d` |

## The `proof` profile (the feature proof)

`--profile proof` adds what `tests/e2e/hello` needs beside the app Postgres: the
`infisical` profile's three services, a key-only `sshd` (`linuxserver/openssh-server`,
`127.0.0.1:2222`) for the SSH legs, and `pg-proxy`, a loopback relay to the app Postgres
(`127.0.0.1:55432`) so the matrix can run its Postgres cells from the host. Nothing is
published off-box. `scripts/oss/proof_seed.py` generates the ssh keypair into `.proof/` (gitignored, never
printed), throwaway Infisical secrets into `.env.infisical` when you have none (#2577), and
writes the env the profile reads; `scripts/oss/feature_proof.sh` seeds, brings up the services the
cells reach (`postgres`, `pg-proxy`, `sshd`) under its own compose project (`-p cairn-proof`, its
own volumes, so a `light` stack beside it is never stopped or reused), runs the matrix with
`CAIRN_E2E=1`, writes `FEATURE_PROOF.md` and tears that project down (`down -v`). The Infisical
services stay down: the Infisical variant needs a machine identity made by hand in its UI, so
it is reported NOT PROVED. ADR-0127 D1.
