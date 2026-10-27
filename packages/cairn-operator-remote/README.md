# cairn-operator-remote

`remote/exec` — run a command (and ship files in/out) on **any SSH box you own**:
raw CPU/GPU compute (#70 Axis-2). Submit launches the command detached, poll reads
the captured exit code (+ pulls outputs and any `metrics.json`), cancel kills it —
on the existing `remote_job` lifecycle. Run a workflow locally, dispatch the heavy
step to a box you control.

- **Connection:** `cairn connect ssh` → a `compute.ssh` connection (host/user/port +
  `known_hosts`); the private key is held in the SecretStore. Key-based auth,
  host-key verified (never disabled).
- **Install:** `pip install 'cairndev[ssh]'` (asyncssh).
- **Foundation for** the NATS remote-worker channel (events-up/control-down), which
  reuses the JetStream substrate.

## The box lifecycle: `compute/acquire`, `compute/release` (ADR-0151 D6)

Start and stop the EC2 instance a `compute.ssh` connection names
(`cairn connect ssh --name gpu --set instance_id=i-… --set region=<aws-region>`),
under governance. Both call `cairn.core.compute_lifecycle`, the module
`cairn compute status|sweep` uses.

- `compute/acquire` — **`risk: paid_compute`**. Writes the `cairn:managed` lease tag
  (plus `cairn:release-after` when `max_hours > 0`), then starts the instance and
  waits (bounded) for its SSH port. Put an approval gate in front of it.
- `compute/release` — **`risk: external_write`, never approval-gated**. Stops the
  instance; an instance already stopped is a success. Declare it `finalizer: true`:

```yaml
  - id: acquire
    task: compute/acquire
    depends_on: [approve-run]
    with: {connection: $input.compute_connection, max_hours: 13}
  # … the legs that use the box …
  - id: release
    task: compute/release
    finalizer: true
    with: {connection: $input.compute_connection}   # from $input, so it resolves even if acquire never ran
```

Design, configuration and failure modes: [DESIGN.md](DESIGN.md).
