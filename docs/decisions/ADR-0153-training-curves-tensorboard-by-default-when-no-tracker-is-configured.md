# ADR-0153 — Training curves: TensorBoard by default when no tracker is configured

- **Status:** PROPOSED 2026-10-26. The owner decided D1–D3 on 2026-10-26 ("TensorBoard can be the default if no MLflow URI is provided"). D5 is the recommendation the owner asked for ("does cairn need a built-in charting tool?") and is accepted with this ADR's merge.
- **Date:** 2026-10-26
- **Issue:** cairndev-sh/cairn-extensions#36 (implementation)
- **Builds on:** ADR-0102 D4 (live metrics over the job channel; the run store is the log), ADR-0149 (the public SSH path), ADR-0151 D11 (the box is stopped as soon as the training is done).
- **Keeps:** ADR-0102 D4 unchanged. Bring-your-own MLflow or W&B through `report_to` stays as it is (ADR-0038; the training contract's tracking options).

## Context

A fine-tune has two kinds of results.

- **The outcome.** Base against candidate loss, steps, the gate decision, the cost and the lineage. The run record keeps it on every path: findings, `Run.cost` and the admission record.
- **The training curve.** Train and eval loss, learning rate and gradient norm at every logging step. The curve shows a run that is too short or too long, overfitting (eval loss turning up) or instability. A launch write-up needs it.

Today the curve reaches someone only in one of two cases.

- **Live metrics in the run store (ADR-0102 D4).** The training pack's callback emits `metrics` run events at `logging_steps` over the job channel: NATS, or HTTPS to the engine. This needs the box to reach the control plane. On the public SSH path, the control plane is often a laptop behind NAT that the box cannot reach. Then no per-step event arrives, and the curve is lost when the box stops.
- **A tracker the user runs.** The user sets `report_to: [mlflow]` with an `MLFLOW_TRACKING_URI`, or uses W&B. Both need a server or an account the box can reach.

A new user who configures neither gets the outcome and no curve.

## Decision

**D1 — With no tracker configured, the trainer writes TensorBoard event files.** No tracker is configured when `report_to` is empty and no MLflow URI is set (`MLFLOW_TRACKING_URI` or `CAIRN_MLFLOW_TRACKING_URI`). In that case the training pack's trainer logs to TensorBoard: `report_to: ["tensorboard"]`, with `logging_dir` in the job workdir. TensorBoard needs no server at training time. It is a dependency of the training pack's `[gpu]` extra.

**D2 — The event files outlive the box.** They are uploaded next to the run's checkpoints under the checkpoint prefix (`s3://<CAIRN_CHECKPOINT_BUCKET>/<prefix>/<run-id>/tensorboard/`). The upload runs with the checkpoint mirror and once more at the end of the run, on success and on failure. The location is recorded on the run as a finding (`tensorboard_uri`). Without a checkpoint bucket the files stay in the job workdir, which is enough when the leg ran on the same machine. The user reads them with `tensorboard --logdir <uri or synced dir>`.

**D3 — A configured tracker wins.** When the user asks for MLflow or W&B, the trainer logs to that tracker only and adds no TensorBoard. The user asks either by setting a `report_to` value with that tracker's URI, or by setting `report_to` explicitly. An explicit `report_to: []` with no URI is treated as "not configured" and gets D1.

**D4 — The run store stays the system of record.** The outcome, the decision, the cost and the lineage stay in the run record. Where the box can reach the control plane, the live `metrics` events of ADR-0102 D4 still flow, unchanged. TensorBoard is how the curve survives when they cannot. It is not a second record of the outcome, and no gate reads it.

**D5 — The public core ships no charting.** Curves are viewed in the standard tools: TensorBoard, MLflow or W&B. A chart inside a product UI belongs to the platform console, where ADR-0102 D4 already places it (the run's Training tab). The core and the extensions record and expose metrics through the run store, the API and the CLI, and link to the tracker. They do not draw them.

## Consequences

- Every training run on the public path leaves a viewable curve, without a tracker server and without the box reaching the control plane.
- `tensorboard` is added to the training pack's `[gpu]` extra. It is not added to the control plane's install.
- A durable curve needs the checkpoint bucket, the same requirement as resuming from a checkpoint.
- The event files hold only scalars and the run's hyperparameters, never dataset rows. They sit under the same prefix and access as the checkpoints.
- A user who runs MLflow or W&B sees no change.

## Alternatives rejected

- **Require MLflow.** It needs a server the GPU box can reach. That is the common failure on the public SSH path (a laptop behind NAT), and it is a heavy first step for a new user.
- **MLflow's file store as the default** (`file:./mlruns`). It has the same copy-back need as TensorBoard, adds a second on-disk format, and its UI reads a local directory, not S3.
- **W&B offline mode.** It needs an account and a later `wandb sync` to see anything.
- **A charting view in the core.** It would rebuild what TensorBoard, MLflow and W&B already do well, and add UI code to a core that has none. In-product charts are a platform feature (ADR-0102 D4).
