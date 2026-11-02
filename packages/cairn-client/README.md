# cairn-client

The typed Python client for the cairn control-plane API (#1194, ADR-0085 D7).

Every method maps 1:1 onto a route in the committed contract
(`console/openapi.json`) — a test fails if the client and the spec drift.
It imports **nothing** from `cairn.core` (CI-enforced): this package is the
boundary that lets the CLI's user commands (#1195) and the ask agent (#1196)
work against a cairn deployment without engine internals.

```python
from cairn_client import CairnClient

c = CairnClient("http://localhost:9090")
c.login("dev", "dev")                       # cairn setup's seeded login
c.healthz()
c.launch_pipeline("training/e2e-finetune", {"dataset": "…"})
c.advance_step(pid, "promote-model", approver="me")
```

Auth: pass `token=` directly, call `login()`, or neither (an auth-disabled
dev engine accepts bare requests). The generic `request()` door covers any
route a typed method doesn't wrap yet.
