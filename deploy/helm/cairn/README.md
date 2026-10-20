# cairn Helm chart

The cairn control plane on Kubernetes: **server**, **worker**, **Postgres**
(bundled or external), and an **Ingress in place of Caddy**. ADR-0104 D4, #26.

This chart is a *translation* of `docker-compose.yml`, not a second
architecture. Every `CAIRN_*` variable compose sets on the `server` service is set
here too, and `tests/deploy/test_helm_chart.py` fails the build if the two drift
apart — it parses the compose file, renders the chart, and asserts the set
difference is empty.

## The two supported shapes

ADR-0104's Consequences say there are exactly two, and both run the same image:

| | Single host | Cluster |
|---|---|---|
| Artifact | `docker-compose.yml` | this chart |
| TLS | Caddy (automatic Let's Encrypt) | Ingress + cert-manager |
| Optional services | compose `--profile` | `profiles.*` values |
| Preflight | `cairn deploy doctor --env .env` | `cairn deploy doctor --values my.yaml` |

Compose is not deprecated. If one VM is enough, use one VM.

## Install, in four commands

```sh
# 1. a namespace
kubectl create namespace cairn

# 2. your values, starting from a shipped example
cp deploy/helm/cairn/values-postgres.example.yaml my-values.yaml   # then edit every CHANGE-ME

# 3. preflight BEFORE the cluster ever sees it
cairn deploy doctor --values my-values.yaml --strict

# 4. install
helm upgrade --install cairn deploy/helm/cairn -n cairn -f my-values.yaml
```

Then: `kubectl -n cairn rollout status deploy/cairn-server` and
`curl -fsS https://<your host>/healthz`. The public engine image serves the API
only; the web console is part of the cairn platform, not this chart.

Without a clone, step 4 is `helm upgrade --install cairn oci://ghcr.io/cairndev-sh/charts/cairn --version <x.y.z> -n cairn -f my-values.yaml`
(the chart is published on every `v*` tag by `.github/workflows/release.yml`).

## Values

The full list with comments is `values.yaml`. The ones that decide the shape:

| Value | Default | What it decides |
|---|---|---|
| `image.repository` / `image.tag` | `ghcr.io/cairndev-sh/cairn-engine` / `""` | The engine image. **Pin the tag** — `--strict` fails on `latest`, and `engine-image.yml` publishes `:<sha>` for every commit on main. |
| `imagePullSecrets` | `[]` | Required while the GHCR package is private. |
| `runtime` | `dbos` | `dbos` (durable, one process — what prod runs), `temporal` (server submits, worker executes; its pipeline owner does not yet run `task:` legs, #1378), `inprocess` (dev). |
| `checkpointer` | `postgres` | `memory` with a durable runtime is **refused**: recovery would replay the whole graph, charging every LLM call twice (#532/#1847). |
| `postgres.enabled` | `true` | Bundled StatefulSet (pgvector, one PVC). |
| `postgres.external.dsn` | `""` | A managed Postgres instead. **Exactly one of these two** — both, or neither, refuses to render. |
| `ingress.host` | `""` | Required. The chart's `CADDY_DOMAIN`. |
| `ingress.certManager.clusterIssuer` | `letsencrypt-prod` | The cert-manager issuer that mints the TLS secret. |
| `secrets.*` / `secrets.existingSecret` | `""` | `CAIRN_JWT_SECRET` + `CAIRN_API_PASSWORD_HASH` (+ `POSTGRES_PASSWORD` with a bundled database) either rendered into a chart-managed Secret, or taken from one you already have. |
| `server.persistence` | `enabled, 5Gi` | `~/.cairn` — the SQLite-backed config stores that do not yet honour `CAIRN_STORAGE`. ReadWriteOnce, so keep `server.replicaCount: 1`. |
| `pullWorker.enabled` / `.command` | `false` / `[]` | The D2 (#565) dial-out worker. Shape only — see below. |
| `env.extra` | `{}` | Any other variable, verbatim into the ConfigMap. Name cairn variables `CAIRN_*`. |
| `artifacts.s3.*` | disabled | `CAIRN_ARTIFACT_S3_*`, the dbos-postgres tier's artifact store. |
| `observability.otlp.endpoint` | `""` | Where traces go. The chart runs no collector — see profiles. |

Three example files ship with the chart and are rendered by CI on every change:

- `values-postgres.example.yaml` — bundled Postgres, DBOS runtime (the closest
  thing to what prod runs today).
- `values-external-dsn.example.yaml` — a managed Postgres (RDS / Cloud SQL / Neon).
- `values-temporal.example.yaml` — the Temporal runtime with a separate worker.

### The Secret wins over the ConfigMap

Workloads take env from the ConfigMap **then** the Secret. Kubernetes applies
`envFrom` sources in order, so a key in the Secret overrides the same key in the
ConfigMap. That is the supported way to keep a DSN with an embedded password out
of a ConfigMap: set `secrets.existingSecret` and put `CAIRN_STORAGE`,
`CAIRN_CHECKPOINTER_DSN` and `CAIRN_DBOS_DSN` in it.

**Until 1.0 an `OBS_*`-spelled Secret keeps working** (ADR-0122, #2429). cairn
reads `CAIRN_<NAME>` first and `OBS_<NAME>` only when it is unset, so a
`CAIRN_STORAGE` rendered into the ConfigMap would silently shadow an
`OBS_STORAGE` in your Secret. The chart therefore renders the DSN family
(`STORAGE`, `CHECKPOINTER_DSN`, `DBOS_DSN`) under **both** names when it manages
the Secret, and under the **`OBS_` name only** when `secrets.existingSecret` is
set: a `CAIRN_` key in your Secret then beats it, and an `OBS_` key replaces it.
If your Secret carries no DSN, the engine reads the ConfigMap's `OBS_` value and
logs one deprecation warning per name. Every other chart key is `CAIRN_*` only;
the `OBS_` twins go at 1.0.

### Profiles

`profiles.*` mirrors the ten compose profiles **exactly** — a test asserts the
two sets are identical, so a profile cannot be quietly invented or dropped here.

**Implemented** (cairn's own topology): `temporal` · `nats` · `mlflow` · `backup`
(compose's `while true; sleep` dump loop becomes a CronJob).

**Delegated** (`observability`, `otel`, `infisical`, `llm-gateway`,
`metaflow-service`, `dedicated-temporal-db`): Kubernetes already has a maintained
chart for each, and vendoring a worse copy is exactly the "build infra with no
consumer" the verify-upstream gate exists to stop. Enabling one **fails at render
time** naming the chart to install instead — a loud refusal, never a silent
no-op, because a silent no-op is how compose deploys got a running service the
engine was never pointed at.

### The pull worker is shape-only

`pullWorker` renders a Deployment that runs `pullWorker.command`. It is **off by
default and has no default command**: `cairn worker --pull` is being built in the
D2 slice (#565) in parallel with this one, and a chart that guessed its flags
would ship a guess. Enabling it without a command refuses to render.

### Postgres connection assumptions (in-process leg liveness)

Every server and every `cairn pipeline run` process holds a Postgres **session**
advisory lock for its whole life; that lock is how other replicas, and the
reconciler, tell that its in-process legs are still alive (ADR-0125). Losing
the session for longer than one check interval (15 s) lets the reconciler
judge the process's legs dead. So the database connection must be:

- **direct or session-mode** — not PgBouncer *transaction* pooling, which hands
  a session lock to whichever client holds the server connection next;
- free of an idle timeout: `idle_session_timeout = 0` on the server (cairn sets
  it on its own owner session where the role may), and no proxy or load
  balancer that closes a quiet connection;
- on a network that passes TCP keepalives (cairn turns them on for that
  connection, client and server side).

A Postgres restart or failover drops every owner lock at once; each process
takes its lock again within one interval, and the reconciler needs two probes
an interval apart before it calls an owner dead, so that is not a false
failure.

## Upgrade and rollback

```sh
helm upgrade cairn deploy/helm/cairn -n cairn -f my-values.yaml   # or --set image.tag=<sha>
helm history cairn -n cairn
helm rollback cairn <revision> -n cairn
```

The server Deployment carries a `checksum/config` annotation, so a values-only
change still rolls the pods (otherwise the old env keeps running). Both PVCs and
the Postgres volume are annotated `helm.sh/resource-policy: keep`, so
`helm uninstall` does **not** delete data; remove the PVCs by hand when you mean it.

A rollback rolls back the *manifests*. It does not undo a database migration —
the DSN is the same database before and after.

## What this chart does not do

- **Run your observability stack.** Point `observability.otlp.endpoint` at your
  collector. `deploy/otel-collector.yaml` is a working config to start from.
- **Provision the cluster, the GPU nodes or the storage classes.** ADR-0104: that
  is infrastructure, not cairn code.
- **Execute workloads on Kubernetes.** That is D3 — the Metaflow engine with
  Kubernetes compute (ADR-0089), configured in the *environment*, not here.
- **Bundle a sub-chart.** No bitnami dependency to pin and audit; either the
  60-line StatefulSet or your managed database.
- **Persist console-edited environments.** Compose mounts the `profiles`
  directory beside its compose file over the image's baked copy
  (#1443) so an environment edited
  in the console reaches the running process. A chart has no repo to mount: the
  image is the record. Environments edited in the console live on the container
  filesystem and are **lost on pod restart** — author them in the image (or in
  `~/.cairn`, which is on the PVC) until the profile store moves onto
  `CAIRN_STORAGE`. Filed as an adjacent finding on the D4 PR, not fixed here.

## Where the image and the chart come from

`.github/workflows/release.yml` pushes `ghcr.io/cairndev-sh/cairn-engine:<version>` and
the chart to `oci://ghcr.io/cairndev-sh/charts/cairn` on every `v*` tag. Both packages
are public, so no pull secret is needed.

## Verified / not verified

**Verified:** `helm lint` clean; `helm template` renders for bundled Postgres,
external DSN and the Temporal runtime; the rendered server env carries every
`CAIRN_*` compose sets on `server` (and every one the dbos-postgres tier sets);
each refusal in `_helpers.tpl` actually refuses; `cairn deploy doctor --values`
passes the examples and fails a values file with a missing secret.

**Not verified:** no `helm install` against a live cluster, no TLS issued (the 0.1.2
verification pulled the image anonymously and rendered the published chart). The chart has never run. Treat the first install as an
acceptance run, not a deploy.
