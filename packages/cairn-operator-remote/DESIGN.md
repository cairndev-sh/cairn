# remote — design

- **Distribution:** `obs-operator-remote`, folded into the `cairndev` wheel (`packages/cairn-operator-remote/`, ADR-0137 D2); SSH is the `ssh` extra
- **Capabilities provided:** `remote.exec`, `compute.acquire`, `compute.release`
- **Status:** proven (`remote/exec`, the 2026-08-28 SSH campaign); `compute/acquire` and `compute/release` hermetically tested, live EC2 proof owed (cairn-extensions#9)
- **Decisions:** ADR-0043 (job channel), ADR-0097 (governed box lifecycle), ADR-0120 D7 (a pack imports only the operator contract), ADR-0151 D6–D9 (the lifecycle pair is public here), #70, #601, #603, #1787, #201

## 1. Purpose

Use a machine you own over SSH from a governed pipeline. `remote/exec` runs a
command on a `compute.ssh` box (ship files in, run detached, pull outputs and a
`metrics.json` back). `compute/acquire` starts the EC2 instance a `compute.ssh`
connection names, once a person approved the spend, and `compute/release` stops it
on every outcome of the pipeline.

It deliberately does NOT: provision, terminate, resize or create instances (the
lifecycle pair starts and stops an instance that already exists; per-job cloud
GPUs are the platform's provider operators and the SkyPilot engine), price compute
(dispatch prices a leg from the connection's `hourly_usd`, ADR-0151 D1), or hold
SSH credentials of its own (the SecretStore does).

## 2. Architecture

```
remote/exec (remote_job)   submit: SFTP uploads -> runner.sh launched detached (setsid) -> pid
                           poll:   exit_code file, corroborated by pid liveness (#337, #2606)
                           cancel: kill -TERM/-KILL the job's process group (#603)
compute/acquire            load_box(connection) -> describe_state -> stamp_lease -> start_instance
                           -> wait_port_open (bounded)                                (ADR-0151 D8)
compute/release            load_box(connection) -> describe_state -> (stopped? no-op) -> stop_instance
```

`remote/exec` wraps asyncssh (the `ssh` extra, lazy-imported); host keys are always
verified against the connection's `known_hosts`.

The lifecycle pair adds no lifecycle logic. Every EC2 call is a function of
`cairn.core.compute_lifecycle`, the module `cairn compute status|sweep` calls too, so
"what starting a box means" cannot drift between the operators and the sweep
(#1376). The pack reaches those functions through the operator contract's
re-exports in `cairn.core.operator` (ADR-0120 D7), never by importing the core module
(`tests/core/test_architecture_boundaries.py`, R_PACK_CORE). boto3 is a base
dependency of the engine wheel and is imported lazily inside `ec2_client`.

**Lease before start (ADR-0151 D8).** `acquire` writes the `cairn:managed` tag (plus
`cairn:release-after` when `max_hours > 0` and `cairn:acquired-by`) before it starts
the instance. `cairn compute sweep` selects boxes by that tag, so a crash between the
two calls leaves a tagged box that is not running, never a billing box the sweep
cannot see. The platform's 0.1.0 operator did the reverse.

**Idempotency.** `acquire` on a running box is a reported no-op
(`already_running: true`); EC2's `StartInstances` is idempotent. `release` on a
stopped, stopping, shutting-down or terminated instance returns
`already_stopped: true` and is a success, which is what lets the bridge re-run a
`finalizer:` release after a control-plane restart (ADR-0097 D3). Neither retries.

## 3. Operators

| Operator | Capability | Risk | Mock | Input -> Output | Reports (stages · series · scalars) |
|---|---|---|---|---|---|
| `remote/exec` | `remote.exec` | external_write | yes | `RemoteExecInput` -> `RemoteExecOutput` | `—` |
| `compute/acquire` | `compute.acquire` | paid_compute | yes | `AcquireInput` -> `AcquireOutput` | `start, wait_ssh · — · ssh_ready, already_running` |
| `compute/release` | `compute.release` | external_write | yes | `ReleaseInput` -> `ReleaseOutput` | `—` |

### `remote/exec`

`executors = ["remote_job", "in_process"]`, long-running, cancellable. Implements
`contract://remote.exec/1.0.0` (`cairn-contracts-remote`). The in-process fallback
submits and polls once.

### `compute/acquire`

Input `{connection (required), max_hours ≥ 0 = 0, wait_ready_s ≥ 0 = 600,
acquired_by = ""}`. Output `{target, instance_id, host, state, hourly_usd, started_at,
release_after, ssh_ready, already_running, note}`; `target` is the connection name
downstream legs bind. It refuses a connection with no `instance_id`, or one that is
not `compute.ssh`, before any EC2 call. An SSH port that does not answer within
`wait_ready_s` is reported (`ssh_ready: false`), not raised, so the pipeline's release
still runs. The mock path makes no EC2 call. Stage `start` covers the lease and the
start; stage `wait_ssh` the readiness probe; scalar `ssh_ready` says whether the port
answered and `already_running` whether the box was up before the call.

### `compute/release`

Input `{connection (required), instance_id = ""}` (the override is rarely needed).
Output `{instance_id, state, stopped, already_stopped, stopped_at, note}`.
`requires_approval = False` on the class. The mock path makes no EC2 call.

No `implements` contract URI on either lifecycle operator yet (ADR-0151 D7); a
`cairn-contracts-compute` package is a follow-up on the contract track.

## 4. Configuration

| Setting | Where | Scope | Default | What it does |
|---|---|---|---|---|
| `host`, `user`, `port`, `known_hosts` | `compute.ssh` connection | deployment | port 22 | where `remote/exec` connects; `host`/`port` are the readiness probe for `acquire` |
| `instance_id`, `region`, `hourly_usd` | `compute.ssh` connection | deployment | — | the instance the lifecycle pair starts and stops, its region, and its rate (reported by `acquire`; priced by dispatch) |
| `AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL`, AWS credentials | env | deployment | — | the boto3 client `ec2_client` builds when the connection names no region |
| `CAIRN_EC2_CLIENT_FACTORY` | env | dev/test only | unset | `module:callable` that replaces the boto3 client; production leaves it unset |
| `SSH_PRIVATE_KEY` | env | deployment | — | the key `remote/exec`'s cancel reconnects with when it has no context (#127) |
| `max_hours`, `wait_ready_s`, `acquired_by` | `input_schema` | run | `0`, `600`, `""` | per-run lease length, readiness wait, owner tag |

No operator here declares a `config_schema`. A connection that does not resolve, or
is not `compute.ssh`, fails closed with the `cairn connect ssh` command that fixes it.

## 5. Governance

- `compute/acquire` is `paid_compute`, rated high by the approval plane. It passes
  `enforce_gate` like every task leg, so a profile with
  `approval.mode = enforce, default_on_risk = [paid_compute]` pauses it for its own
  approval (after the pipeline's approval gate, if it has one). The class risk is what
  the bridge's restart rule reads (`_rerun_is_safe`): a half-done acquire is parked for
  a person after a restart, never re-started silently.
- `compute/release` is `external_write` and never approval-gated: a teardown paused
  behind an unanswered gate is a box billing while it waits. Declare it
  `finalizer: true`, wired off `$input` (not off `acquire`'s output), so it resolves
  even when `acquire` never ran.
- On a pipeline cancel, the bridge first cancels the engine job of every running flow
  leg whose recorded compute target is the connection the `compute/release` finalizer
  stops, then submits the release (ADR-0151 D9). A job cancel that fails is logged and
  audited (`leg_job_cancel`, `sent: false`); the release runs regardless.
- `remote/exec` is `external_write`.
- In-process and DBOS run finalizers on done, failed and cancelled. The Temporal
  pipeline owner schedules by `depends_on` only, so a trailing release would run on
  success alone; ADR-0151 D10 refuses such a plan at launch (a separate change: #196, #197, #199).
- Task legs reserve no budget (`08-governance.md`, partial): `acquire` is a paid task
  leg on top of that; the minutes between `acquire` and the first priced flow leg are
  unpriced (ADR-0151 consequences).

## 6. Failure modes

| Symptom | Cause | What the operator returns | Fail closed? |
|---|---|---|---|
| `ComputeLifecycleError: … has no instance_id` | the connection lacks `instance_id` | raises before any EC2 call | yes |
| `ComputeLifecycleError: … did not resolve` / `is kind …` | missing, disabled or non-SSH connection | raises before any EC2 call | yes |
| `ssh_ready: false`, note "did not accept within …" | the box is slow to boot | success; the next leg hits its own error | no (by design: release must still run) |
| a crash between the lease and the start | control plane died | tagged, not running (ADR-0151 D8) | yes |
| release of a stopped box | finalizer re-run after a restart | `already_stopped: true`, success | n/a |
| an orphaned trainer after cancel | `kill <pid>` reached only the wrapper (#603) | `remote/exec` kills the process group | yes |

## 7. Testing

`packages/cairn-operator-remote/tests/`: `test_remote_exec.py` (fake asyncssh
connection), `test_compute_lifecycle.py` (the platform's lifecycle tests plus the
lease-before-start and crash cases, fake EC2 through `CAIRN_EC2_CLIENT_FACTORY`),
`test_compute_lifecycle_reports.py` (ADR-0103 conformance on the real engine path),
`test_compute_lifecycle_pipeline.py` (`enforce_gate` sees `paid_compute`; a finalizer
release stops the box on done, failed and cancelled on the in-process bridge). All
hermetic: no SSH, no AWS. The manifest gates (`tests/core/test_operator_manifest_sync.py`,
`test_pack_manifest_drift.py`) read installed metadata: `uv sync --reinstall-package
cairndev` after changing `pack.toml` or the entry points. A live start/stop of a real
box is owed by the qualification run (cairn-extensions#9).

## 8. History

| Date | Change | PR |
|---|---|---|
| 2026-10-24 | `compute/acquire` and `compute/release` move here from the platform's `cairn-operator-compute` 0.1.0; lease stamped before start | #201 |
