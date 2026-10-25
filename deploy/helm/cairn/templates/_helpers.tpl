{{/*
Names, labels, the Postgres DSN, and the render-time validation gate.

The validation here is the chart's launch door (ADR-0099's shape, one layer
down): a configuration that would deploy something broken must REFUSE to
render, not render something that fails at 3am. Every `fail` below names the
exact value to change.
*/}}

{{- define "cairn.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cairn.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "cairn.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "cairn.labels" -}}
helm.sh/chart: {{ include "cairn.chart" . }}
{{ include "cairn.selectorLabels" . }}
app.kubernetes.io/version: {{ include "cairn.imageTag" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: cairn
{{- end -}}

{{- define "cairn.selectorLabels" -}}
app.kubernetes.io/name: {{ include "cairn.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "cairn.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "cairn.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "cairn.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag -}}
{{- end -}}

{{- define "cairn.image" -}}
{{- printf "%s:%s" .Values.image.repository (include "cairn.imageTag" .) -}}
{{- end -}}

{{/* The Secret every workload takes its env overrides from. */}}
{{- define "cairn.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{- .Values.secrets.existingSecret -}}
{{- else -}}
{{- printf "%s-secrets" (include "cairn.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/* The pull worker's OWN Secret (CAIRN_ENGINE_TOKEN), never the control plane's. */}}
{{- define "cairn.pullWorkerSecretName" -}}
{{- if trim (toString (default "" .Values.pullWorker.existingSecret)) -}}
{{- trim (toString .Values.pullWorker.existingSecret) -}}
{{- else -}}
{{- printf "%s-pull-worker" (include "cairn.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
pullWorker.token, trimmed. A non-string value (an unquoted `--set
pullWorker.token=123`) refuses to render: a token must arrive as text.
*/}}
{{- define "cairn.pullWorkerToken" -}}
{{- $tok := .Values.pullWorker.token -}}
{{- if and $tok (not (kindIs "string" $tok)) -}}
{{- fail (printf "pullWorker.token must be a string, not %s — quote it, or pass it with --set-string pullWorker.token=…" (kindOf $tok)) -}}
{{- end -}}
{{- trim (toString (default "" $tok)) -}}
{{- end -}}

{{/*
The pull worker's envFrom: its OWN ConfigMap, then its own Secret (it wins). Never
the shared ConfigMap (the DSN family, the OTEL headers, env.extra) nor the control
plane's Secret.
*/}}
{{- define "cairn.pullWorkerEnvFrom" -}}
- configMapRef:
    name: {{ include "cairn.pullWorkerName" . }}
- secretRef:
    name: {{ include "cairn.pullWorkerSecretName" . }}
{{- end -}}

{{/* The worker's ConfigMap (and, by default, its ServiceAccount): <release>-pull-worker. */}}
{{- define "cairn.pullWorkerName" -}}
{{- printf "%s-pull-worker" (include "cairn.fullname" .) -}}
{{- end -}}

{{/*
The worker's ServiceAccount: pullWorker.serviceAccount.name, else <release>-pull-worker
(create=true). With create=false the name is required (cairn.validate).
*/}}
{{- define "cairn.pullWorkerServiceAccountName" -}}
{{- $n := trim (toString (default "" .Values.pullWorker.serviceAccount.name)) -}}
{{- if $n -}}
{{- $n -}}
{{- else -}}
{{- include "cairn.pullWorkerName" . -}}
{{- end -}}
{{- end -}}

{{/*
The worker's ConfigMap data: an ALLOWLIST. What the claim loop reads
(CAIRN_ENGINE_URL) and the non-secret settings an in-process (--exec-local)
operator may read, then pullWorker.extraEnv (checked in cairn.validate).
*/}}
{{- define "cairn.pullWorkerConfig" -}}
{{- $url := .Values.pullWorker.engineUrl | default (index .Values.env.extra "CAIRN_ENGINE_URL") | default (index .Values.env.extra "OBS_ENGINE_URL") | default "" -}}
CAIRN_BACKEND: {{ .Values.env.backend | quote }}
{{- if $url }}
CAIRN_ENGINE_URL: {{ $url | quote }}
{{- end }}
CAIRN_PROFILE: {{ .Values.env.profile | quote }}
MLFLOW_TRACKING_URI: {{ include "cairn.mlflowUri" . | quote }}
OTEL_EXPORTER_OTLP_ENDPOINT: {{ .Values.observability.otlp.endpoint | quote }}
{{- if .Values.artifacts.s3.enabled }}
CAIRN_ARTIFACT_S3_ENDPOINT: {{ .Values.artifacts.s3.endpoint | quote }}
CAIRN_ARTIFACT_S3_BUCKET: {{ .Values.artifacts.s3.bucket | quote }}
CAIRN_ARTIFACT_S3_REGION: {{ .Values.artifacts.s3.region | quote }}
{{- end }}
{{- range $k, $v := .Values.pullWorker.extraEnv }}
{{ $k }}: {{ $v | toString | quote }}
{{- end }}
{{- end -}}

{{/*
A pullWorker.extraEnv NAME that must not go in a ConfigMap; empty when it is fine.
Case-insensitive. A marker counts as an underscore-separated segment or a segment
suffix (HF_TOKEN, APITOKEN, MY_API_KEYS: refused; TOKENIZERS_PARALLELISM: allowed):
KEY, SECRET, TOKEN, PASSWORD, DSN. Also the DSN family (*_STORAGE), the OTEL headers,
anything containing CONNECTION_STRING, and the CAIRN_CONN_ prefix (the namespace an
engine mints per submission for declared connections).
*/}}
{{- define "cairn.pullWorkerUnsafeName" -}}
{{- $u := upper . -}}
{{- range $seg := splitList "_" $u -}}
{{- if regexMatch "(KEY|SECRET|TOKEN|PASSWORD|DSN)S?$" $seg -}}unsafe{{- end -}}
{{- end -}}
{{- if or (hasSuffix "STORAGE" $u) (eq $u "OTEL_EXPORTER_OTLP_HEADERS") (contains "CONNECTION_STRING" $u) (hasPrefix "CAIRN_CONN_" $u) -}}unsafe{{- end -}}
{{- end -}}

{{/*
A pullWorker.extraEnv VALUE carrying a credential in a URL: userinfo with a password
(`scheme://user:pass@host`). Empty when it is fine. Only this shape is detected.
*/}}
{{- define "cairn.pullWorkerUnsafeValue" -}}
{{- if regexMatch "[A-Za-z][A-Za-z0-9+.-]*://[^/?#@\\s]*:[^/?#@\\s]+@" (toString .) -}}unsafe{{- end -}}
{{- end -}}

{{- define "cairn.configMapName" -}}
{{- printf "%s-config" (include "cairn.fullname" .) -}}
{{- end -}}

{{- define "cairn.postgresName" -}}
{{- printf "%s-postgres" (include "cairn.fullname" .) -}}
{{- end -}}

{{/*
The one DSN every store uses (run store · LangGraph checkpointer · DBOS system
schema). Compose derives it from POSTGRES_PASSWORD; the chart derives it from
the bundled StatefulSet or takes the external one verbatim.
*/}}
{{- define "cairn.dsn" -}}
{{- if .Values.postgres.enabled -}}
{{- printf "postgresql://%s:%s@%s:%v/%s" .Values.postgres.user .Values.postgres.password (include "cairn.postgresName" .) .Values.postgres.port .Values.postgres.database -}}
{{- else -}}
{{- .Values.postgres.external.dsn -}}
{{- end -}}
{{- end -}}

{{- define "cairn.temporalAddress" -}}
{{- if .Values.profiles.temporal.address -}}
{{- .Values.profiles.temporal.address -}}
{{- else -}}
{{- printf "%s-temporal:7233" (include "cairn.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "cairn.natsUrl" -}}
{{- printf "nats://%s-nats:4222" (include "cairn.fullname" .) -}}
{{- end -}}

{{- define "cairn.mlflowUri" -}}
{{- if .Values.profiles.mlflow.trackingUri -}}
{{- .Values.profiles.mlflow.trackingUri -}}
{{- else if .Values.profiles.mlflow.enabled -}}
{{- printf "http://%s-mlflow:5000" (include "cairn.fullname" .) -}}
{{- end -}}
{{- end -}}

{{/*
One DSN-family key (STORAGE / CHECKPOINTER_DSN / DBOS_DSN) — ADR-0122, #2429. Until
1.0: both names with no existingSecret; only the OBS_ name with one, so the Secret's
key wins in either spelling (configmap.yaml says why). Called as
`include "cairn.dsnEnv" (list . "STORAGE")`.
*/}}
{{- define "cairn.dsnEnv" -}}
{{- $root := index . 0 -}}
{{- $name := index . 1 -}}
{{- $dsn := include "cairn.dsn" $root | quote -}}
{{- if not $root.Values.secrets.existingSecret -}}
CAIRN_{{ $name }}: {{ $dsn }}
{{ end -}}
OBS_{{ $name }}: {{ $dsn }}
{{- end -}}

{{/* envFrom: ConfigMap first, Secret LAST so a secret key wins on collision. */}}
{{- define "cairn.envFrom" -}}
- configMapRef:
    name: {{ include "cairn.configMapName" . }}
- secretRef:
    name: {{ include "cairn.secretName" . }}
{{- end -}}

{{/*
── the door ─────────────────────────────────────────────────────────────
Called once from the ConfigMap (which always renders). Each branch is a
deployment that has actually gone wrong somewhere, on compose or on a box.
*/}}
{{- define "cairn.validate" -}}

{{- /* Postgres: bundled XOR external. Both = two databases, silently one used. */ -}}
{{- if and .Values.postgres.enabled .Values.postgres.external.dsn -}}
{{- fail "postgres.enabled=true AND postgres.external.dsn is set — pick one. The bundled StatefulSet and an external DSN are mutually exclusive; set postgres.enabled=false to use the external DSN." -}}
{{- end -}}
{{- if and (not .Values.postgres.enabled) (not .Values.postgres.external.dsn) -}}
{{- fail "no database: postgres.enabled=false and postgres.external.dsn is empty. Set one of them — cairn's run store, LangGraph checkpointer and DBOS system schema all live in Postgres." -}}
{{- end -}}
{{- if and .Values.postgres.enabled (not .Values.postgres.password) -}}
{{- fail "postgres.enabled=true but postgres.password is empty — generate one (openssl rand -hex 16)." -}}
{{- end -}}

{{- /* The canonical doctor rule (#390), now enforced before anything starts. */ -}}
{{- if and (eq .Values.runtime "temporal") (not .Values.profiles.temporal.enabled) -}}
{{- fail "runtime=temporal but profiles.temporal.enabled=false — the Temporal service and the worker would never start, so every submitted run would hang. Enable profiles.temporal, or set runtime=dbos." -}}
{{- end -}}
{{- if and .Values.profiles.temporal.enabled (ne .Values.runtime "temporal") -}}
{{- fail (printf "profiles.temporal.enabled=true but runtime=%s — the server would not submit to Temporal, leaving an idle cluster. Set runtime=temporal, or disable the profile." .Values.runtime) -}}
{{- end -}}

{{- /* #390/#532/#1847 — a durable runtime on a memory checkpointer replays the
       whole graph on recovery, charging every LLM call twice. */ -}}
{{- if and (ne .Values.runtime "inprocess") (eq .Values.checkpointer "memory") -}}
{{- fail (printf "runtime=%s with checkpointer=memory — recovery re-runs the whole graph from zero (every operator and LLM call charged again) because the LangGraph checkpoint died with the process. Set checkpointer=postgres." .Values.runtime) -}}
{{- end -}}

{{- /* Ingress needs a host; it is this chart's CADDY_DOMAIN. */ -}}
{{- if and .Values.ingress.enabled (not .Values.ingress.host) -}}
{{- fail "ingress.enabled=true but ingress.host is empty — set the public hostname (the chart's equivalent of CADDY_DOMAIN)." -}}
{{- end -}}

{{- /* #2296 — every deployment names its profile; the engine refuses to start without one. */ -}}
{{- if not (trim (toString (default "" .Values.env.profile))) -}}
{{- fail "env.profile is empty — every deployment names its profile (CAIRN_PROFILE); cairn-server and cairn-worker refuse to start without one. Use /app/.venv/lib/python3.13/site-packages/cairn/profiles/secure.toml (or prod-infisical.toml) or your own — see docs/runbooks/deployment-profile.md." -}}
{{- end -}}

{{- /* Secrets: present here, or promised by an existing Secret. */ -}}
{{- if not .Values.secrets.existingSecret -}}
{{- if not .Values.secrets.obsJwtSecret -}}
{{- fail "secrets.obsJwtSecret is empty and secrets.existingSecret is unset — auth is ON in prod (issue #2) and the engine refuses to start without a JWT secret." -}}
{{- end -}}
{{- if not .Values.secrets.obsApiPasswordHash -}}
{{- fail "secrets.obsApiPasswordHash is empty and secrets.existingSecret is unset — generate it with `cairn remote hash-password`." -}}
{{- end -}}
{{- end -}}

{{- /* #2073 — a PROVISIONING engine with ephemeral state leaks money. SkyPilot
       keeps its cluster state in ~/.sky; without a volume behind it a pod
       restart forgets every machine it launched, and `sky down` can no longer
       reach them. Refuse rather than rent something nothing can return. */ -}}
{{- if and (eq ((index .Values.env.extra "CAIRN_ML_ENGINE") | default (index .Values.env.extra "OBS_ML_ENGINE") | default "") "skypilot") (not .Values.server.persistence.enabled) -}}
{{- fail "CAIRN_ML_ENGINE=skypilot with server.persistence.enabled=false — SkyPilot keeps its cluster state in ~/.sky, so a pod restart would forget every machine it provisioned and `sky down` could not terminate them. They would keep billing. Enable server.persistence, or select an engine that does not provision." -}}
{{- end -}}

{{- /* The pull worker (`cairn worker --pull`, #565) has no default command. */ -}}
{{- if and .Values.pullWorker.enabled (not .Values.pullWorker.command) -}}
{{- fail "pullWorker.enabled=true but pullWorker.command is empty — the chart has no default command for the pull worker. Supply pullWorker.command explicitly, e.g. [cairn, worker, --pull, --engine-url, https://<host>]." -}}
{{- end -}}
{{- /* Its token comes from a Secret of its own, never the control plane's. */ -}}
{{- if .Values.pullWorker.enabled -}}
{{- $tok := include "cairn.pullWorkerToken" . -}}
{{- $existing := trim (toString (default "" .Values.pullWorker.existingSecret)) -}}
{{- if and $tok $existing -}}
{{- fail "pullWorker.token AND pullWorker.existingSecret are both set — pick one: the chart renders a Secret from pullWorker.token only when pullWorker.existingSecret is empty." -}}
{{- end -}}
{{- $_ := required "pullWorker.enabled=true needs the worker's own token: set pullWorker.token to a worker-pool token (`cairn remote worker-token --name <pool>`), or pullWorker.existingSecret to a Secret with the key CAIRN_ENGINE_TOKEN. A token that is empty or only whitespace counts as unset. The pull worker never reads the control plane's Secret." ($tok | default $existing) -}}
{{- if and $tok (eq $tok (trim (toString (default "" .Values.secrets.obsJwtSecret)))) -}}
{{- fail "pullWorker.token equals secrets.obsJwtSecret — the worker must never hold the control plane's JWT secret. Mint a worker-pool token with `cairn remote worker-token --name <pool>`." -}}
{{- end -}}
{{- range $k, $_ := .Values.pullWorker.extraEnv -}}
{{- if not (regexMatch "^[A-Za-z_][A-Za-z0-9_]*$" $k) -}}
{{- fail (printf "pullWorker.extraEnv key %q is not an environment variable name." $k) -}}
{{- end -}}
{{- if include "cairn.pullWorkerUnsafeName" $k -}}
{{- fail (printf "pullWorker.extraEnv.%s looks like a secret or a database setting (a KEY, SECRET, TOKEN, PASSWORD or DSN segment, *_STORAGE, CONNECTION_STRING, CAIRN_CONN_*, OTEL_EXPORTER_OTLP_HEADERS) — extraEnv is rendered into a ConfigMap. Put it in the worker's own Secret (pullWorker.existingSecret) instead." $k) -}}
{{- end -}}
{{- end -}}
{{- range $k, $v := .Values.pullWorker.extraEnv -}}
{{- if include "cairn.pullWorkerUnsafeValue" $v -}}
{{- fail (printf "pullWorker.extraEnv.%s has a URL with a password in it (scheme://user:pass@…) — extraEnv is rendered into a ConfigMap. Put the credential in the worker's own Secret (pullWorker.existingSecret) instead." $k) -}}
{{- end -}}
{{- end -}}
{{- $sa := .Values.pullWorker.serviceAccount -}}
{{- $saName := trim (toString (default "" $sa.name)) -}}
{{- if and (not $sa.create) (not $saName) -}}
{{- fail "pullWorker.serviceAccount.create=false needs pullWorker.serviceAccount.name — the pre-provisioned ServiceAccount the worker runs under." -}}
{{- end -}}
{{- if eq (include "cairn.pullWorkerServiceAccountName" .) (include "cairn.serviceAccountName" .) -}}
{{- fail (printf "the pull worker's ServiceAccount %q is the control plane's — the worker must not run under the control plane's identity. Name another one (pullWorker.serviceAccount.name)." (include "cairn.pullWorkerServiceAccountName" .)) -}}
{{- end -}}
{{- if and $existing (eq $existing (include "cairn.secretName" .)) -}}
{{- fail (printf "pullWorker.existingSecret is %q, the control plane's Secret — the pull worker must not receive the JWT secret or the admin password hash. Give it a Secret of its own holding only CAIRN_ENGINE_TOKEN." $existing) -}}
{{- end -}}
{{- end -}}

{{- /* Delegated profiles: refuse loudly, never pretend. */ -}}
{{- if (index .Values.profiles "observability").enabled -}}
{{- fail "profiles.observability is not implemented by this chart — Prometheus and Grafana on Kubernetes are prometheus-community/kube-prometheus-stack. Install that chart and point observability.otlp.endpoint at your collector." -}}
{{- end -}}
{{- if (index .Values.profiles "otel").enabled -}}
{{- fail "profiles.otel is not implemented by this chart — use open-telemetry/opentelemetry-collector (deploy/otel-collector.yaml is a working config to start from) and set observability.otlp.endpoint." -}}
{{- end -}}
{{- if (index .Values.profiles "infisical").enabled -}}
{{- fail "profiles.infisical is not implemented by this chart — use infisical/infisical-standalone, then bind the engine to it with env.profile (CAIRN_PROFILE, #408)." -}}
{{- end -}}
{{- if (index .Values.profiles "llm-gateway").enabled -}}
{{- fail "profiles.llm-gateway is not implemented by this chart — use berriai/litellm-helm and set env.extra.LLM_GATEWAY_URL (#182)." -}}
{{- end -}}
{{- if (index .Values.profiles "metaflow-service").enabled -}}
{{- fail "profiles.metaflow-service is not implemented by this chart — use outerbounds/metaflow-tools and set env.extra.CAIRN_METAFLOW_SERVICE_URL (#1365)." -}}
{{- end -}}
{{- if (index .Values.profiles "dedicated-temporal-db").enabled -}}
{{- fail "profiles.dedicated-temporal-db is not implemented by this chart — give Temporal its own database via the official temporal chart, or a managed Postgres." -}}
{{- end -}}

{{- end -}}
