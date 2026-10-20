"""The hello-cairn feature-proof matrix (ADR-0127 D1, hello-cairn WP7).

storage × runtime × bus (all eight cells, ``tests/e2e/conftest.py``) × the example pack's four
pipelines, each cell a real ``cairn-server`` driven as a user would: ``POST /start``, every
gate decided through ``cairn remote advance`` (the first cell) or the approve door (the
others) — both record the approver (#2613) — the terminal state read back, then the
records: one Run per leg, the gate verdicts and approvers, each leg's own output, and the
feature receipts (``_receipts.py``) the report is built from.

A pipeline is one :class:`Row`. A row that cannot run here (the ``[train]`` extra, the proof
sshd) is SKIPPED with that reason — never silently absent — and a feature the example cannot
exercise yet is recorded NOT PROVED with its reason, so the report shows the whole proof,
including what is still owed. The declared triggers are armed and fired in ``test_triggers.py``.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Optional

import pytest

from tests.e2e.conftest import (
    HELLO,
    SSH,
    SSH_KEY,
    SSH_USER,
    Api,
    Cell,
    Server,
    cell_params,
    cli,
    dbos_workflow,
    tcp_reachable,
    train_available,
)
from tests.e2e.hello._receipts import record
from tests.e2e.model_stub import ANSWER, ModelStub

pytestmark = pytest.mark.e2e

FIRST_CELL: Cell = cell_params()[0].values[0]


@dataclass
class Ctx:
    cell: Cell
    srv: Server
    api: Api
    pid: str
    rec: dict
    stub: ModelStub
    chat_calls: int           # model calls the stub answered during this cell
    decided_by: str = "api"   # how the gates were decided: api | cli
    paused_leftovers: dict = field(default_factory=dict)   # see _one_run_per_leg

    def step(self, sid: str) -> dict:
        return self.rec["step_state"][sid]

    def out(self, sid: str) -> dict:
        return self.api.output(self.step(sid)["run_id"])

    def receipt(self, feature: str, evidence: str, *, proved: bool = True) -> None:
        record(feature, self.cell.id, self.rec.get("pipeline_name") or "?", evidence, proved=proved)


@dataclass(frozen=True)
class Row:
    """One pipeline of the example pack: how to drive it and what its record must show."""

    name: str
    gates: tuple[str, ...]                      # steps that wait for a person, in order
    check: Callable[[Ctx], None]                # the record assertions + receipts
    inputs: dict = field(default_factory=dict)
    needs: Callable[[], Optional[str]] = lambda: None   # a skip reason, or None
    marks: tuple = ()
    server_env: Callable[[], dict] = dict       # what the ENGINE needs beyond the cell
    prepare: Callable[[Server], None] = lambda srv: None   # set-up against the running server

    @property
    def ref(self) -> str:
        return f"hello-cairn/{self.name}"

    def require(self) -> None:
        yaml = HELLO / "pipelines" / f"{self.name}.yaml"
        assert yaml.is_file(), f"{yaml} is gone: the matrix proves the example's four pipelines"
        why = self.needs()
        if why:
            pytest.skip(why)


# ── the record checks per pipeline ─────────────────────────────────────────

def _one_run_per_leg(ctx: Ctx, legs: tuple[str, ...], *, operator_gated: tuple[str, ...] = ()) -> dict[str, str]:
    """Every executable leg became exactly one Run, each ``done``; returns step → run id.

    ``operator_gated``: legs whose operator declares ``requires_approval``. Approving one
    today re-submits the leg as a NEW Run and leaves the Run that paused for the approval
    at ``awaiting_approval`` for good (``pipeline_bridge.advance_step`` → ``_submit_step``)
    — a core gap this harness does not paper over: such a leftover is tolerated ONLY in
    exactly that shape, and :func:`_operator_gate_receipt` records it NOT PROVED with the
    run ids. Any other extra Run fails the cell."""
    ids = {}
    for sid in legs:
        st = ctx.step(sid)
        assert st["status"] == "done", (sid, st)
        assert st["run_id"], f"leg {sid} has no run"
        run = ctx.api.run(st["run_id"])
        assert run["status"] == "done", (sid, run.get("status"), run.get("error"))
        ids[sid] = st["run_id"]
    assert len(set(ids.values())) == len(legs), f"legs share a run: {ids}"
    # nothing else of this pipeline's ran: every run that names the pipeline is one of these
    mine = [r for r in ctx.api.runs() if r.get("bundle_pipeline_id") == ctx.pid]
    extra = [r for r in mine if r["id"] not in set(ids.values())]
    left = [r for r in extra if r.get("step_id") in operator_gated and r["status"] == "awaiting_approval"]
    assert extra == left, [(r["id"], r.get("step_id"), r["status"]) for r in mine]
    ctx.paused_leftovers = {r["step_id"]: r["id"] for r in left}
    return ids


def _operator_gate_receipt(ctx: Ctx, legs: tuple[str, ...]) -> None:
    """``gate.operator``: an operator-declared gate decided on the leg's ONE Run."""
    left = {sid: rid for sid, rid in ctx.paused_leftovers.items() if sid in legs}
    if left:
        ctx.receipt("gate.operator", "core gap: approving re-submitted the leg as a new Run and left the "
                    f"paused Run awaiting_approval ({', '.join(f'{k}: {v}' for k, v in sorted(left.items()))})",
                    proved=False)
    else:
        ctx.receipt("gate.operator", f"{', '.join(legs)} decided on the leg's one Run, no paused Run left")


def _decided(ctx: Ctx, *gates: str) -> str:
    """Each gate approved, and its approver recorded on BOTH decision paths (CLI and the
    approve door, #2613)."""
    for gate in gates:
        st = ctx.step(gate)
        assert st["status"] == "done", (gate, st)
        assert st.get("approver") == "proof", (gate, ctx.decided_by, st)
    return ", ".join(f"{g} approver={ctx.step(g)['approver']!r}" for g in gates) + f" via {ctx.decided_by}"


def _files_under(root: Path, store: str) -> list[Path]:
    return sorted(p for p in root.rglob("*") if p.is_file() and store in p.parts)


def _audit_rows(srv: Server) -> list[dict]:
    if not srv.audit_log.is_file():
        return []
    return [json.loads(line) for line in srv.audit_log.read_text(encoding="utf-8").splitlines() if line.strip()]


def _cell_receipts(ctx: Ctx) -> None:
    if ctx.cell.storage == "postgres":
        ctx.receipt("storage.postgres", f"CAIRN_STORAGE=postgres (database {ctx.srv.dsn.rsplit('/', 1)[-1]})")
    if ctx.cell.bus == "postgres":
        ctx.receipt("bus.postgres", "CAIRN_BUS=postgres: every leg's terminal reached the bridge over it")


def check_profile_and_publish(ctx: Ctx) -> None:
    ids = _one_run_per_leg(ctx, ("profile", "redact", "engine-check", "stage", "promote", "publish", "audit"),
                           operator_gated=("promote",))
    ctx.receipt("pipeline.data", f"pipeline {ctx.pid} done; {len(ids)} legs, one run each")
    # the data gate resolved from the profile's output
    data_ok = ctx.step("data-ok")
    assert data_ok["status"] == "done" and data_ok.get("verdict") == "data", data_ok
    profile = ctx.out("profile")
    assert profile.get("verdict") == "pass" and profile.get("record_count", 0) >= 10, profile
    ctx.receipt("gate.data", f"data-ok verdict='data' on profile verdict={profile['verdict']!r} "
                f"record_count={profile['record_count']}")
    # the person gate and the operator-declared gate, each decided and attributed
    assert ctx.step("review").get("verdict") == "approved", ctx.step("review")
    ctx.receipt("gate.person", _decided(ctx, "review", "promote"))
    _operator_gate_receipt(ctx, ("promote",))
    redact = ctx.out("redact")
    assert redact.get("ok") and "span_count" in redact, redact
    ctx.receipt("redaction", f"redact/text leg: span_count={redact['span_count']} redacted={redact.get('redacted')}")
    # the flow leg ran on the builtin engine (its persisted handle names it)
    handle = ctx.step("engine-check").get("handle") or {}
    assert handle.get("engine") == "builtin" and str(handle.get("job_id", "")).startswith("builtin"), handle
    ctx.receipt("flow.builtin-engine", f"engine-check handle={handle['job_id']}")
    # the artifact: staged once, published once, under the server's artifact root
    staged = _files_under(ctx.srv.artifacts, "hello-staging")
    published = _files_under(ctx.srv.artifacts, "hello-published")
    assert len(staged) == 1 and len(published) == 1, (staged, published)
    assert published[0].read_text(encoding="utf-8").strip(), "the published summary is empty"
    ctx.receipt("artifact.store", f"{published[0].relative_to(ctx.srv.artifacts)} written once")
    promote = ctx.out("promote")
    flow_artifacts = (ctx.out("engine-check").get("findings") or {}).get("artifact_ids") or []
    assert promote.get("promoted") is True and promote.get("artifact_id") == flow_artifacts[0], (promote, flow_artifacts)
    ctx.receipt("artifact.promote", f"tag {promote['tag']!r} → the flow leg's artifact {promote['artifact_id']}")
    # the audit leg's hash-chained row is in the sink at CAIRN_AUDIT_LOG
    audit = ctx.out("audit")
    rows = _audit_rows(ctx.srv)
    assert audit.get("ok") and audit.get("hash") and any(audit["hash"] in json.dumps(r) for r in rows), (audit, rows[-1:])
    ctx.receipt("audit", f"{len(rows)} row(s) at CAIRN_AUDIT_LOG; the audit leg's hash {audit['hash'][:12]}… among them")
    # the budget is on the record
    budget = (ctx.rec.get("governance") or {}).get("budget") or {}
    assert float(budget.get("usd", 0)) == 1.0, ctx.rec.get("governance")
    ctx.receipt("budget", f"governance.budget.usd={budget['usd']}")
    _hooks_receipt(ctx)
    _cell_receipts(ctx)


def _hooks_receipt(ctx: Ctx) -> None:
    """The hello-receipts listener (a `cairn.run_hooks` entry point of the cairn-pack-hello
    wheel) writes a line for every settled leg on the engine, whatever the pipeline. It is
    proved on profile-and-publish, which runs in every cell (CI included); nightly-ops
    needs the proof sshd, which CI cannot run."""
    hooks = ctx.srv.home / ".cairn" / "hello-cairn" / "receipts.jsonl"
    if hooks.is_file():
        lines = [line for line in hooks.read_text(encoding="utf-8").splitlines() if ctx.pid in line]
        assert lines, f"no hook receipt names {ctx.pid}"
        ctx.receipt("hooks", f"{len(lines)} hello-receipts lines for this pipeline at $CAIRN_HOME/hello-cairn/receipts.jsonl")
    else:
        ctx.receipt("hooks", "the hello-receipts listener is an entry point of the cairn-pack-hello wheel; "
                    "a dev pack (CAIRN_PACK_PATH) declares none — install the `hello` extra", proved=False)


def check_ask_the_docs(ctx: Ctx) -> None:   # assistant (template) → length → review
    ids = _one_run_per_leg(ctx, ("assistant", "length"))
    ctx.receipt("pipeline.agentic", f"pipeline {ctx.pid} done; assistant run {ids['assistant']}")
    ctx.receipt("gate.person", _decided(ctx, "review"))
    found = ctx.out("assistant").get("findings") or {}
    # the model call: build_chat_model → the OpenAI client → the stub, and its answer back
    assert ctx.chat_calls >= 1 and str(found.get("answer", "")).startswith(ANSWER), (ctx.chat_calls, found.get("answer"))
    ctx.receipt("llm", f"{ctx.chat_calls} chat completion(s) through build_chat_model to the keyless "
                "stub model (tests/e2e/model_stub.py, #2607); the answer is on the run")
    hits = found.get("hits") or []
    assert found.get("search_ok") and hits, {k: found.get(k) for k in ("search_ok", "search_error")}
    ctx.receipt("rag", f"rag/search over the pack's `docs` corpus (indexed at boot) returned {len(hits)} hits; "
                f"reranker={found.get('reranker')!r}")
    stats = found.get("answer_stats") or {}
    assert found.get("measure_ok") and stats.get("words") == len(found["answer"].split()), (stats, found.get("measure_error"))
    ctx.receipt("mcp", f"mcp/call hello-calc:word_count over stdio → words={stats['words']}")
    assert found.get("remembered") is True and found.get("memory_id"), found.get("memory_error")
    ctx.receipt("memory", f"memory/write to hello_answers → id {found['memory_id']}")
    length = ctx.out("length")
    assert length.get("crossed") is False and float(length.get("value", -1)) == float(stats["words"]), length
    ctx.receipt("monitor", f"monitor/threshold: value={length['value']} threshold={length['threshold']} crossed=False")
    if ctx.cell.runtime == "dbos":
        # the template leg ran as a DBOS workflow: no in-process owner (ADR-0125 D1), and the
        # runtime's own system database holds the workflow, its id the run id (#38)
        run = ctx.api.run(ids["assistant"])
        assert not run.get("owner"), run.get("owner")
        wf = dbos_workflow(ctx.srv, ids["assistant"])
        assert wf == ("SUCCESS", "cairn.run_template"), wf
        ctx.receipt("runtime.dbos", f"template leg {ids['assistant']} ran as DBOS workflow "
                    f"{wf[1]} → {wf[0]} (CAIRN_DBOS_DSN); no in-process owner")
    _cell_receipts(ctx)


def check_tiny_finetune(ctx: Ctx) -> None:  # profile → train (ml flow) → eval → loss → promote ⏸ → review ⏸ → card
    ids = _one_run_per_leg(ctx, ("profile", "train", "eval", "loss", "promote", "card"), operator_gated=("promote",))
    ctx.receipt("pipeline.ml", f"pipeline {ctx.pid} done")
    ctx.receipt("gate.person", _decided(ctx, "promote", "review"))
    _operator_gate_receipt(ctx, ("promote",))
    ctx.receipt("train", f"train.lora flow leg done (run {ids['train']}, model {ctx.rec['inputs'].get('base_model')}, "
                f"held-out eval_loss={ctx.out('eval').get('eval_loss')})")
    ctx.receipt("worker.pull", "no hello-cairn leg is routed to the pull lane (an ML target `worker:<pool>` "
                "on a remote-job leg) and no cell starts `cairn worker --pull`; only the worker's claim, "
                "its token check and an empty queue are tested", proved=False)
    _cell_receipts(ctx)


def check_nightly_ops(ctx: Ctx) -> None:    # job (template over SSH) → summarise (flow) → propose ⏸ → audit
    _one_run_per_leg(ctx, ("job", "summarise", "propose", "audit"), operator_gated=("propose",))
    ctx.receipt("pipeline.flow", f"pipeline {ctx.pid} done")
    ctx.receipt("gate.person", _decided(ctx, "propose"))
    _operator_gate_receipt(ctx, ("propose",))
    found = ctx.out("job").get("findings") or {}
    assert found.get("secret_present") is True, found
    ctx.receipt("secrets.env", "secrets.store → secrets/env: HELLO_REPORT_TOKEN present (the value never enters the run)")
    ctx.receipt("secrets.infisical", "no cell runs the Infisical variant (profiles/hello-infisical.toml): the "
                "proof starts no Infisical server and seeds no machine identity (scripts/oss/proof_seed.py); "
                "the env variant ran", proved=False)
    report = found.get("report") or {}
    assert str(found.get("job_exit_code")) == "0" and isinstance(report.get("lines"), int), \
        {k: found.get(k) for k in ("job_status", "job_exit_code", "job_error", "report")}
    ctx.receipt("ssh", f"remote/exec on compute.ssh `hello-ssh` (the proof sshd): host={found.get('job_host')!r}, "
                f"metrics.json read back lines={report['lines']}")
    _cell_receipts(ctx)


# ── nightly-ops: the proof sshd as the `hello-ssh` compute.ssh connection ───

def _nightly_needs() -> Optional[str]:
    if not tcp_reachable(SSH):
        return f"the proof sshd at {SSH} does not answer (docker compose --profile proof; scripts/oss/feature_proof.sh)"
    if not Path(SSH_KEY).is_file():
        return f"no proof ssh key at {SSH_KEY} (scripts/oss/proof_seed.py)"
    if shutil.which("ssh-keyscan") is None:
        return "ssh-keyscan is not on PATH (the known_hosts entry for the proof sshd)"
    try:
        import asyncssh  # noqa: F401, PLC0415
    except ModuleNotFoundError:
        return "asyncssh is not installed (the `ssh` extra; `hello` pulls it)"
    return None


def _nightly_env() -> dict:
    """The engine's environment: the job's secret (secrets/env) and the private key the
    connection's PRIVATE_KEY reference names. Neither is ever printed."""
    return {"HELLO_REPORT_TOKEN": "proof-report-token", "HELLO_SSH_KEY": Path(SSH_KEY).read_text()}


def _nightly_prepare(srv: Server) -> None:
    """Register the proof sshd as ``hello-ssh`` the way the README does: ``cairn connect add
    ssh`` against the engine's store, the host key pinned in a known_hosts file."""
    host, _, port = SSH.rpartition(":")
    scan = subprocess.run(["ssh-keyscan", "-p", port, host], capture_output=True, text=True, timeout=30)
    assert scan.stdout.strip(), f"ssh-keyscan found no host key at {SSH}: {scan.stderr}"
    known = srv.home / "known_hosts"
    known.write_text(scan.stdout)
    out = cli(srv, "connect", "add", "ssh", "--name", "hello-ssh", "--set", f"host={host}", "--set", f"port={port}",
              "--set", f"user={SSH_USER}", "--set", f"known_hosts={known}", "--secret", "PRIVATE_KEY=HELLO_SSH_KEY")
    assert out.returncode == 0, out.stdout + out.stderr


ROWS: tuple[Row, ...] = (
    Row("profile-and-publish", gates=("review", "promote"), check=check_profile_and_publish),
    Row("ask-the-docs", gates=("review",), check=check_ask_the_docs),
    Row("tiny-finetune", gates=("promote", "review"), check=check_tiny_finetune,
        needs=train_available, marks=(pytest.mark.train,),
        inputs={"base_model": os.environ.get("HELLO_TRAIN_MODEL", "sshleifer/tiny-gpt2")}),
    Row("nightly-ops", gates=("propose",), check=check_nightly_ops, needs=_nightly_needs,
        marks=(pytest.mark.services,), server_env=_nightly_env, prepare=_nightly_prepare),
)


def row_params() -> list:
    return [pytest.param(r, id=r.name, marks=list(r.marks)) for r in ROWS]


# ── the cells ──────────────────────────────────────────────────────────────

@pytest.mark.parametrize("row", row_params())
@pytest.mark.parametrize("cell", cell_params())
def test_pipeline(cell: Cell, row: Row, server_factory, model_stub) -> None:
    row.require()
    srv = server_factory(cell, row.name, extra_env=row.server_env())
    row.prepare(srv)
    api = srv.api()
    chat_before = model_stub.calls["/chat/completions"]
    pid = api.start(row.ref, row.inputs)
    decided_by = "cli" if cell == FIRST_CELL else "api"
    for gate in row.gates:
        api.wait_step(pid, gate, "awaiting_approval", timeout=180)
        if decided_by == "cli":
            # the first cell decides every gate the way a terminal user does
            out = cli(srv, "remote", "advance", pid, gate, "--approver", "proof")
            assert out.returncode == 0, out.stdout + out.stderr
        else:
            api.approve(pid, gate, approver="proof", rationale="feature proof")
    rec = api.wait_terminal(pid, timeout=300)
    assert rec["status"] == "done", (rec["status"], rec.get("trigger", {}).get("failure_reason"))
    row.check(Ctx(cell, srv, api, pid, rec, model_stub, model_stub.calls["/chat/completions"] - chat_before,
                  decided_by))


def test_the_cli_lists_and_explains_the_example(server_factory) -> None:
    """The CLI surface a new user meets first, against the running server's packs."""
    srv = server_factory(FIRST_CELL, "cli")
    ls = cli(srv, "pipeline", "ls")
    assert ls.returncode == 0 and "hello-cairn" in ls.stdout, ls.stdout + ls.stderr
    for row in ROWS:
        out = cli(srv, "pipeline", "explain", row.ref)
        assert out.returncode == 0, out.stdout + out.stderr
