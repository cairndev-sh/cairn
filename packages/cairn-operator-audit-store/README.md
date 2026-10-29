# cairn-operator-audit-store

Zero-dependency **hash-chained append-only** audit sink — the **builtin default**
provider for the substrate `audit.sink` service (ADR-0028). Storage lives here,
in the operator pack; core holds only the `AuditSink` interface, the
`AuditEvent` shape, and the emission seams (`cairn/core/obs/audit.py`).

Each row is one `AuditEvent` plus `prev_hash` + `hash`, where
`hash = sha256(prev_hash + canonical(event))`. Altering or dropping any row
breaks every later link — the log is tamper-evident with stdlib `hashlib`
alone. The chain head is read from the last row of the file under a file
lock on every append, so it continues unbroken across restarts and across any
number of writers (instances or processes) sharing the log. Rows removed from
the end do not break the chain; compare a receipt with `cairn lineage --expect-head`. Rows land in `audit.jsonl` (override with
`CAIRN_AUDIT_LOG`), reusing the `events.jsonl` file convention — no new store.

Seams that emit today: the LLM gateway (`llm.complete`), the MCP boundary
(`tool.call`), and the durable action handler (`action.requested` /
`action.executed`).

**Optional (ADR-0028).** Auditing is off unless a profile binds it:

```toml
# in a secure / prod profile
[profile.bindings]
"audit.sink" = "audit/store"
```

`local-dev.toml` leaves it unbound. Richer sinks ship later as
`operator-audit-otel` (Apache-2.0) / `-immudb` / `-siem` — swap the binding,
no core change.
