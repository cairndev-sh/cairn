# audit_store — design

- **Distribution:** `obs-operator-audit-store` (`cairn/operators/builtins/audit_store/`)
- **Capabilities provided:** `audit.sink`
- **Status:** proven (shipped; the substrate default for `audit.sink`)
- **Decisions:** ADR-0028 (auditing is a bound capability, not core), #86, #1233, #1970

## 1. Purpose

Give cairn a **tamper-evident record of every governed act** — which model was
called, which tool ran, which action was requested and executed, which gate a
human decided — using nothing but the standard library, so that auditing is
available in every deployment including an air-gapped one.

It deliberately does NOT: index or query (the console reads the file and
filters in `cairn/core/console/real.py`), retain or rotate (a deployment's job),
encrypt, or ship rows anywhere. Richer sinks are separate packs bound in place
of this one; core never changes.

## 2. Architecture

An append-only JSONL file with a sha256 hash chain over canonicalised rows.

```
audit_emit(action, ...)            # cairn/core/obs/audit.py — the seam
  -> get_audit_sink()              # resolves the profile's audit.sink binding, caches
    -> AuditStoreOp.record(event)  # THIS pack, service path
      -> _HashChainLog.append()    # hash = sha256(prev_hash + canonical(payload))
        -> asyncio.to_thread(...)  # blocking append off the event loop (#86)
```

- `canonical(payload)` is `json.dumps(sort_keys=True, separators=(",", ":"))`, so
  the hash is stable across processes and Python versions.
- Genesis `prev_hash` is 64 zeros. On construction the head is **recovered from
  the last row of the existing file**, so the chain continues unbroken across
  restarts; a corrupt row is skipped rather than fatal, and an unreadable file
  starts a fresh chain rather than refusing to boot.
- Writes are serialised by an `asyncio.Lock`, so concurrent seams cannot
  interleave and break the chain. The file write itself runs in a thread so an
  audit never stalls the loop it is auditing.
- **Two entry paths, one chain.** `record(event)` is the *service* path called
  by the substrate; `run(inp, ctx)` is the *node* path, so a template can write
  an explicit audit row as a step. The node path stamps `workspace_id` from the
  active workspace and fills `ts` when blank (#1233 — node-authored rows landed
  with an empty timestamp and the console rendered "—").

State lives entirely in the file at `CAIRN_AUDIT_LOG`, including the chain head:
every append takes an exclusive file lock (`flock`; a `.lock` sidecar where there is
no `fcntl`), reads the last row's hash from the end of the file, appends and
`fsync`s. Any number of instances in any number of processes (the cached service
sink, the per-call instance of a `task: audit/store` leg, a CLI run beside the
server) therefore extend one chain. A file system that refuses `flock` is logged
and the append proceeds unlocked.

## 3. Operators

| Operator | Capability | Risk | Mock | Input -> Output | Reports (stages · series · scalars) |
|---|---|---|---|---|---|
| `audit/store` | `audit.sink` | read_only | yes | `AuditWriteInput` -> `AuditWriteOutput` | `—` (`ReportSpec.NONE`: one append, nothing to report) |

`AuditWriteInput`: `action` (dotted verb, required), `outcome` (`ok` \| `deny` \|
`error`), `actor`, `target`, `run_id`, `ts` (filled by the sink when blank),
`metadata`. `AuditWriteOutput`: `ok`, `hash` (the chain hash of the row just
appended — a caller can quote it as a receipt).

The risk class is `read_only` even though the operator writes a file: it writes
only cairn's own append-only log, never a customer system. Nothing it does is
externally visible, and gating it would gate every governed act.

## 4. Configuration

| Setting | Where | Scope | Default | What it does |
|---|---|---|---|---|
| `CAIRN_AUDIT_LOG` | env | deployment | `audit.jsonl` (CWD) | Path of the chain file |
| `"audit.sink"` | profile `[profile.bindings]` | deployment | unbound in `local-dev` | Binds this operator as the sink; unbound = `NullAuditSink`, no rows |

This pack declares **no `config_schema`**, so the ADR-0068/0070 governance-scope
trap does not apply to it. Nothing here is per-run configurable: a run cannot
choose where the audit goes, which is the point.

Two operational traps, both found in production:

- **The default path is inside the container.** With `CAIRN_AUDIT_LOG` unset the
  chain is written to the process's CWD (`/app/audit.jsonl` in the prod image)
  and is lost on every rebuild. Point it at a persisted volume
  (`/root/.cairn/audit.jsonl` in the compose deployment). Found 2026-09-03 (#1970).
- **Binding without the pack loaded yields silence.** See §6.

No connections, no secrets.

## 5. Governance

This pack is not gated — it is what gates are recorded *in*. Everything it
writes is the audit trail itself: the hash chain is what an approver, an
auditor or a replay reads to establish what happened. A deployment can swap the
binding for a stricter sink (OTel, immudb, SIEM) without a core change; it
cannot make cairn write audit rows somewhere the profile did not name.

Rows are workspace-stamped (`workspace_id`) at both entry paths, so the
console's audit view is scoped by tenancy (#288, ADR-0051). A row written
before that field existed reads as `default`.

## 6. Failure modes

| Symptom | Cause | What the operator returns | Fail closed? |
|---|---|---|---|
| No audit rows at all, no error | `audit.sink` unbound in the active profile | n/a — `audit_emit` no-ops on `NullAuditSink` | No, by design (ADR-0028) |
| No audit rows, "audit binding 'audit/store' unresolved" in the log | The binding resolved **before the operator packs were loaded**, so the registry was empty and the null sink was cached for the process lifetime | n/a | No — fixed 2026-09-03 (#1970): the resolver now loads the packs once and retries before caching |
| Rows exist but vanish after a deploy | `CAIRN_AUDIT_LOG` unset, chain written inside the container | n/a | No — set the path (§4) |
| A row's hash does not verify | The file was edited or concatenated | n/a — verification is the reader's | The chain is tamper-**evident**, not tamper-proof. Rows removed from the END leave a chain that still verifies; quote a row's `hash` (the receipt) and pass it to `cairn lineage --expect-head` to see them |
| A corrupt line mid-file | Partial write, or a hand edit | Skipped when the head is read; the chain continues from the last parseable row | No, deliberately: one bad line must not stop auditing |
| An emitting seam raises | Any sink error | Logged and dropped by `audit_emit` | No — auditing never breaks the operation being audited |

The last row is the design's sharpest edge: auditing is **fail-safe, not
fail-closed**. A deployment that needs "no audit, no action" must express that
as a policy at the seam, not expect it from this pack.

## 7. Testing

Pack-local tests live in `cairn/operators/builtins/audit_store/tests/`
(CLAUDE.md rule 5). They cover the chain property directly: append N rows,
verify every `prev_hash` links and every `hash` recomputes; restart against an
existing file and confirm the head is recovered; a corrupt row is skipped.

Engine-side coverage of the binding and the seams is in
`tests/core/test_audit.py` and `tests/core/test_secure_profile.py`;
`tests/core/test_showcase_seed.py` verifies the whole chain end to end over
seeded rows. Nothing here needs a live service or a GPU.

From a worktree sharing the main venv:

```
env -u CAIRN_STORAGE PYTHONPATH=$PWD ../../../.venv/bin/python -m pytest -q tests/core/test_audit.py
```

## 8. History

| Date | Change | PR |
|---|---|---|
| 2026-09-03 | Design record written (first pack under CLAUDE.md rule 8) | #1997 |
| 2026-09-03 | Sink resolution loads operator packs once and retries before caching the null sink | #1985-era, issue #1970 |
| earlier | `ts` filled on the node path when blank | #1233 |
| earlier | Blocking append moved off the event loop | #86 |
