# ADR-0149 — Remote SSH legs: declared environment and the connection's credential

- **Status:** ACCEPTED 2026-10-23 by the owner ("fix pipeline first": item 1 of the GPU-run
  investigation).
- **Date:** 2026-10-23
- **Issue:** cairndev-sh/cairn#193.
- **Amends:** ADR-0043's cancel-credential line ("Slice 1 SSH-cancel falls back to the ambient
  `SSH_PRIVATE_KEY`"), for jobs submitted after this change. ADR-0043 itself is not edited.
- **Keeps:** ADR-0081 D3 (the box never holds control-plane credentials; rows and registry
  access stay control-side), ADR-0089 D3 (credentials reach a task only as declared
  connections), ADR-0104 D1 (venv and image modes), ADR-0148 (each workspace reads its own
  registration).

## Context

A `flow:` leg on the builtin engine with a `<kind>_target` runs `cairn-dataflow-run` on a
`compute.ssh` box through `remote/exec`, with the control plane elsewhere. Three things did not
work for a fine-tune leg on that path:

- The job got only the spec, the parent run, the live channel and `MLFLOW_*`. A trainer that
  mirrors checkpoints and publishes to S3 also reads `CAIRN_CHECKPOINT_BUCKET`,
  `CAIRN_CHECKPOINT_PREFIX` and `AWS_DEFAULT_REGION`, so publishing failed and mirroring was a
  silent no-op.
- A `dataset:` ref resolved to the location the version was registered at, often a
  control-plane path, even when the registry held a durable copy in the object store. The box
  could not read the path.
- Submit authenticated with the connection's `PRIVATE_KEY`. Cancel and the checkpoint pull-back
  run later without an operation context and reconnected with `SSH_PRIVATE_KEY` from the
  control plane's environment, swallowing any failure. With the documented setup the cancel
  could not authenticate, and the job kept running on a paid box.

## Decision

**D1 — The deployment declares the extra env names a remote job receives.** `remote_env`
(`[profile.backends]`, else `CAIRN_REMOTE_ENV`) lists NAMES, comma- or space-separated. Each one
set on the control plane is forwarded; nothing else of its environment crosses beyond the
engine's own set (spec, lineage, live channel, `MLFLOW_*`). A flow or a tenant cannot extend the
list. The SSH engine's `validate` refuses the leg before submit, naming the variable and never
its value, for:

- a name containing `KEY`, `SECRET`, `TOKEN` or `PASSWORD` (this covers `AWS_ACCESS_KEY_ID`,
  `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`): credentials stay on the box, as an instance
  role or the box's own environment;
- an entry that is not an environment variable name, reported by its position only; no part of
  the entry is echoed, since any of it may be a pasted value (an upper-case base32 seed with `=`
  padding reads exactly like `NAME=`);
- a name the engine sets itself for each submission (the spec, lineage and live-channel names).

**D2 — Values reach only the job's runner file.** In venv mode they are exported from the
job's `runner.sh`, which `remote/exec` creates exclusively with mode 0600: a stale copy is
removed first, and the SFTP open uses `FXF_EXCL` with the mode, so an existing file is never
truncated and reused with its old mode (the submit fails instead), and the new file is never
readable by another account on the box when the server honours the create mode, as OpenSSH
does. In image mode `runner.sh` runs
`docker run -e NAME` with no value, so docker takes it from that file's environment. No value is
on a command line.

The job handle the engine returns is persisted on the pipeline step (`StepState.handle`) and
returned by `GET /pipelines/{id}`. It records the exec input WITHOUT its environment, plus the
forwarded NAMES (`env_names`); poll, cancel, adopt and result need nothing more. Before this ADR
the handle recorded the whole environment, including the job-channel token and
`MLFLOW_TRACKING_PASSWORD`.

`MLFLOW_TRACKING_PASSWORD` is the one credential the engine forwards itself, with the other
`MLFLOW_*` names (#1235), never through `remote_env`. Its forwarding is unchanged by this ADR; it
lands only in the 0600 runner file.

**D3 — A remote leg reads dataset versions from their durable copy.** An engine whose job runs
on another machine declares `needs_durable_inputs = True` (the SSH engine does), read through
`cairn.core.dataflow.engine.needs_durable_inputs` (`is True`, like `carries_stage_contracts`).
For such an engine, the leg executor resolves `dataset:` refs with `prefer_durable`, and each ref resolves to
the workspace's own `durable_uri` when there is one. Any other leg keeps the registered
location, except that a local path that no longer exists falls back to the durable copy. The SSH
engine refuses, in `validate`, a pinned version whose only location is a control-plane path (an
absolute path or any `file:` URI, followed by `#sha256=…`) anywhere in the spec's or a stage's
config, nested dicts and lists included, with how to give it a durable copy. A plain upstream
path with no pin is still uploaded (#1180).

**D4 — Cancel and checkpoint pull-back use the job's connection credential.** At submit,
`remote/exec` records in the job handle the connection's NAME, the ACCOUNT it resolved in
(`ResolvedConnection.account_id`) and the workspace. Cancel and the pull-back resolve that
connection with `account_id=` (as the remote-job control worker resolves a job's compute), so no
workspace needs to be in scope (a learning-gate abort, a run cancel), and take its `PRIVATE_KEY`
through the same `as_ssh_params` mapping submit uses. The connection the handle names wins over
the SSH engine's current target; the target is used only for a handle saved before names were
recorded. A named connection that does not resolve is reported, never replaced by another key. A
handle that names no connection and has no target keeps the ambient `SSH_PRIVATE_KEY`.

A cancel that does not reach the box logs a warning with the host, workdir, connection and error
type (no secret material) and fails: the engine raises `EngineError` and the `remote/exec`
operator raises, so `cancel_recorded_job` reports `False` and `cancel_remote_jobs_for_run` does
not count the job (its record is still closed). A pull-back that does not reach the box logs the
same warning and returns `False`.

## Consequences

- A deployment with a remote trainer sets e.g.
  `remote_env = "CAIRN_CHECKPOINT_BUCKET,CAIRN_CHECKPOINT_PREFIX,AWS_DEFAULT_REGION"`.
  Without it, nothing beyond the earlier set is forwarded.
- A co-located leg's dataset locations are unchanged.
- `needs_durable_inputs` is an optional engine attribute read with `getattr`. An engine that
  does not declare it is treated as co-located.
- A step handle written before this ADR keeps its env values in storage; `GET /pipelines/{id}`
  shows each `env` value in it as `[hidden]`, names kept (owner decision 2026-10-24).
- Job handles from `remote/exec` gain `connection`, `account_id` and `workspace_id` (names only).
  The SSH engine's handle drops `exec_input.env` and gains `env_names`.
- The resolved location is part of the checkpoint `spec_key`, so the first run of a leg whose
  ref switches to the durable copy starts that stage fresh once, instead of resuming.

## Follow-ups

- The SkyPilot engine, and the Metaflow engine on remote compute, also run jobs elsewhere. They
  should declare `needs_durable_inputs` and refuse control-plane-only dataset versions in the
  same way.
- On the `remote/exec` OPERATOR path, `RemoteJobRecord.input` keeps the resolved operator input,
  including the binding's own `env`, and it is served to worker-pool claims. Check whether a
  binding can interpolate secret values into that `env`; this ADR does not change it.
- `cairn-extensions`: the `finetune-ssh` profile comment names `remote_env`,
  `CAIRN_DATAFLOW_REMOTE_CMD` and the wheel matching.
