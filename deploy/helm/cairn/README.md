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
| `image.repository` / `image.tag` | `ghcr.io/cairndev-sh/cairn-engine` / `""` | The engine image. **Pin the tag** — `--strict` fails on `latest`. `release.yml` publishes `:<version>` (and `:<sha>`) on every `v*` tag. |
| `imagePullSecrets` | `[]` | Only for a private registry or mirror; the GHCR package is public. |
| `runtime` | `dbos` | `dbos` (durable, one process — what prod runs), `temporal` (server submits, worker executes; its pipeline owner does not yet run `task:` legs, #1378), `inprocess` (dev). |
| `checkpointer` | `postgres` | `memory` with a durable runtime is **refused**: recovery would replay the whole graph, charging every LLM call twice (#532/#1847). |
| `postgres.enabled` | `true` | Bundled StatefulSet (pgvector, one PVC). |
| `postgres.external.dsn` | `""` | A managed Postgres instead. **Exactly one of these two** — both, or neither, refuses to render. |
| `ingress.host` | `""` | Required. The chart's `CADDY_DOMAIN`. |
| `ingress.certManager.clusterIssuer` | `letsencrypt-prod` | The cert-manager issuer that mints the TLS secret. |
| `secrets.*` / `secrets.existingSecret` | `""` | `CAIRN_JWT_SECRET` + `CAIRN_API_PASSWORD_HASH` (+ `POSTGRES_PASSWORD` with a bundled database) either rendered into a chart-managed Secret, or taken from one you already have. |
| `server.persistence` | `enabled, 5Gi` | `~/.cairn` — the SQLite-backed config stores that do not yet honour `CAIRN_STORAGE`. ReadWriteOnce, so keep `server.replicaCount: 1`. |
| `pullWorker.enabled` / `.command` | `false` / `[]` | The `cairn worker --pull` dial-out worker (#565). No default command — see below. |
| `pullWorker.token` / `.existingSecret` | `""` / `""` | The worker's own Secret: a worker-pool token (`CAIRN_ENGINE_TOKEN`). One is required when the worker is enabled. |
| `env.extra` | `{}` | Any other variable, verbatim into the ConfigMap. Name cairn variables `CAIRN_*`. |
| `env.extra.CAIRN_PUBLIC_URL` | unset | `https://<ingress.host>`. Set it behind the chart's TLS ingress: the console's `cairn_refresh` sign-in cookie is `Secure` only when it is https, and the OAuth callback is built from it. The example values files set it. |
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

### The pull worker

`pullWorker` renders a Deployment that runs `pullWorker.command`. It is **off by
default and has no default command**; enabling it without one refuses to render.

```yaml
pullWorker:
  enabled: true
  command: ["cairn", "worker", "--pull", "--engine-url", "https://cairn.example.com"]
  token: "<worker-pool token>"        # or: existingSecret: cairn-pool-gpu
```

With auth on (the chart's default), claiming work needs a worker credential: a
worker-pool token, minted where `CAIRN_JWT_SECRET` is set with
`cairn remote worker-token --name <pool>`, read from `CAIRN_ENGINE_TOKEN`. A
signed-in user's token gets `403` on `/jobs/claim`.

The worker reads that token from a **Secret of its own**: `pullWorker.token` is
rendered into `<release>-pull-worker`, or `pullWorker.existingSecret` names a
Secret you manage holding the key `CAIRN_ENGINE_TOKEN` (plus anything the
worker's jobs need). The worker no longer reads the control plane's Secret
(`CAIRN_JWT_SECRET`, `CAIRN_API_PASSWORD_HASH`, `POSTGRES_PASSWORD`, the model
key, the SSH key).

Its other settings come from a **ConfigMap of its own** (`<release>-pull-worker`),
an allowlist: `CAIRN_ENGINE_URL` (`pullWorker.engineUrl`, else
`env.extra.CAIRN_ENGINE_URL`, else `env.extra.OBS_ENGINE_URL`, else none — the
command's `--engine-url` works either way), `CAIRN_BACKEND`, `CAIRN_PROFILE`,
`MLFLOW_TRACKING_URI`, `OTEL_EXPORTER_OTLP_ENDPOINT` and, with
`artifacts.s3.enabled`, the `CAIRN_ARTIFACT_S3_*` endpoint, bucket and region.
The worker does not read the shared ConfigMap, so it receives no database
settings, no `OTEL_EXPORTER_OTLP_HEADERS` and no `env.extra` (apart from the
`CAIRN_ENGINE_URL` fallback), and none of `CAIRN_RUNTIME`, `CAIRN_REQUIRE_AUTH`,
`CAIRN_TEMPORAL_ADDRESS` or `CAIRN_CHECKPOINTER`.

Anything else is opt-in through `pullWorker.extraEnv`, a map rendered into that
ConfigMap. The chart refuses a **name** that looks like a secret or a database
setting — a `KEY`, `SECRET`, `TOKEN`, `PASSWORD` or `DSN` segment or segment
suffix, case-insensitive (`HF_TOKEN` and `APITOKEN` are refused,
`TOKENIZERS_PARALLELISM` is not), `*_STORAGE`, anything containing
`CONNECTION_STRING`, the `CAIRN_CONN_` prefix, `OTEL_EXPORTER_OTLP_HEADERS` — and a
**value** with a password in a URL (`scheme://user:pass@…`). That check covers
names and URL credentials only: any other credential belongs in the worker's
Secret. The same applies to `MLFLOW_TRACKING_URI` and
`OTEL_EXPORTER_OTLP_ENDPOINT`, which the worker gets verbatim from
`profiles.mlflow` / `observability.otlp`.

It runs under a **ServiceAccount of its own** with no API token mounted
(`automountServiceAccountToken: false` on the pod in every case). By default the
chart renders it as `<release>-pull-worker`, with
`pullWorker.serviceAccount.annotations` for its own cloud identity (IRSA, GKE
Workload Identity). With `pullWorker.serviceAccount.create: false`, name a
pre-provisioned one in `pullWorker.serviceAccount.name` (required). It may never
be the control plane's ServiceAccount; the chart refuses that. The control
plane's ServiceAccount and its annotations are not used. `imagePullSecrets` still
applies to the worker's pod.

The token is trimmed. Rendering refuses an enabled worker with no token (empty
or whitespace only) and no `existingSecret`, with both, with `existingSecret`
set to the control plane's Secret, with a token equal to `secrets.obsJwtSecret`,
or with a token that is not a string (use `--set-string`). `cairn deploy doctor
--values` reports the same cases, and the `extraEnv` and ServiceAccount ones (it
warns where the answer depends on the release name, which it does not know).

Changing `pullWorker.token` rolls the worker (`checksum/token`), and changing its
ConfigMap rolls it too (`checksum/config`). Rotating the token inside a Secret you
manage (`existingSecret`) does not; restart the worker (`kubectl rollout restart
deployment/<release>-pull-worker`). With `pullWorker.token`, the chart's Secret
holds only `CAIRN_ENGINE_TOKEN`; any other secret the worker's jobs need means
using `existingSecret` instead.

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

**Upgrading a chart with the pull worker enabled:**

- *Token.* The worker no longer reads the control plane's Secret. Mint a
  worker-pool token (`cairn remote worker-token --name <pool>`) and set
  `pullWorker.token`, or put `CAIRN_ENGINE_TOKEN` in a Secret of its own and set
  `pullWorker.existingSecret`; the render fails until one is set. If the token was
  in the control plane's Secret (or `env.extra`), move it. Anything else the
  worker's jobs read from the control plane's Secret (a model key, an SSH key)
  must be put in the worker's own Secret, deliberately; that needs
  `existingSecret`, since the chart's Secret holds only the token.
- *Settings.* The worker no longer reads the shared ConfigMap. An `env.extra` key
  an out-of-tree trainer relied on (an MLflow experiment, a checkpoint bucket, a
  region) moves to `pullWorker.extraEnv`, or to the worker's Secret if it is a
  credential. The worker also stops receiving `CAIRN_RUNTIME`,
  `CAIRN_REQUIRE_AUTH`, `CAIRN_TEMPORAL_ADDRESS`, `CAIRN_CHECKPOINTER`, the
  database DSN family and `OTEL_EXPORTER_OTLP_HEADERS`. The `CAIRN_ENGINE_URL`
  fallback reads `env.extra.CAIRN_ENGINE_URL`, then `env.extra.OBS_ENGINE_URL`.
- *Cloud identity.* The worker now runs under its own ServiceAccount, and every
  cloud-side binding is keyed to the ServiceAccount NAME: an IRSA role's trust
  policy (`sub = system:serviceaccount:<namespace>:<name>`), a GKE Workload
  Identity binding (`<project>.svc.id.goog[<namespace>/<name>]`) and an EKS Pod
  Identity association. Moving the annotation to
  `pullWorker.serviceAccount.annotations` is not enough: update the binding for
  `<release>-pull-worker` (or set `pullWorker.serviceAccount.create: false` and
  `pullWorker.serviceAccount.name` to a pre-provisioned ServiceAccount whose
  binding you manage). Until then the worker has no cloud identity.
- *Name collisions.* The chart renders a ConfigMap, a ServiceAccount (unless
  `create: false`) and, with `pullWorker.token`, a Secret named
  `<release>-pull-worker`. An existing object of that kind and name in the
  namespace collides; rename or remove it, or use `existingSecret` /
  `serviceAccount.create: false` for the Secret and the ServiceAccount.

**Upgrading from a 0.1.x image:** see *Upgrade from 0.1.x* in the
[install docs](https://docs.cairndev.sh/install/#upgrade-from-01x) (pull-worker
tokens, `CAIRN_PUBLIC_URL`, re-indexing some knowledge collections) and the
0.2.0 Migration section of [CHANGELOG.md](../../../CHANGELOG.md).

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
