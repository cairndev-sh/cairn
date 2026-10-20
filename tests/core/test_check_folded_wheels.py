"""``scripts/check_folded_wheels.py`` (ADR-0137 D7) REFUSES what it exists to refuse.

The gate builds five wheels and installs three into a clean venv, which no unit test does. Its
checks are plain functions over plain data, so each is proved here on the accepted shape and on
one tampered input per refusal: a sixth wheel, a missing operator, an operator from another
distribution, a changed contract digest, a contract package hosted elsewhere, a fourth cairn
distribution installed, and an in-process run that did not finish or ran something the engine
wheel does not carry.
"""
from __future__ import annotations

import copy
import importlib.util
import json
import tomllib

import pytest

from tests import _layout

_spec = importlib.util.spec_from_file_location("check_folded_wheels",
                                               _layout.REPO / "scripts" / "check_folded_wheels.py")
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)

SNAPSHOT = json.loads(gate.SNAPSHOT.read_text())
CONTRACTS = [d["name"] for d in SNAPSHOT["distributions"]]
HOSTS = {name: {"cairndev"} for name in CONTRACTS}
OPERATORS = {name: "cairndev" for name in tomllib.loads(
    (_layout.REPO / "pyproject.toml").read_text())["project"]["entry-points"]["cairn.operators"]}
PREFIX = "/venv"


def test_the_fixtures_are_the_accepted_shape():
    assert len(CONTRACTS) == gate.CORE_CONTRACTS and len(OPERATORS) == gate.BUILTIN_OPERATORS
    assert gate.contract_problems(CONTRACTS, HOSTS, SNAPSHOT, SNAPSHOT) == []
    assert gate.operator_problems(OPERATORS) == []
    assert gate.wheel_problems(gate.WHEELS) == []
    assert gate.installed_problems(["cairn-pack-hello", "cairndev", "cairn-client"]) == []


# ── D7.2 the wheels ──────────────────────────────────────────────────────────────────────────

def test_a_sixth_wheel_is_refused():
    (problem,) = gate.wheel_problems(gate.WHEELS | {"cairn_operator_mcp"})
    assert "extra: ['cairn_operator_mcp']" in problem


def test_a_missing_wheel_is_refused():
    (problem,) = gate.wheel_problems(gate.WHEELS - {"cairn_client"})
    assert "missing: ['cairn_client']" in problem


# ── D7.3 the installed set ───────────────────────────────────────────────────────────────────

def test_a_fourth_cairn_distribution_installed_is_refused():
    assert gate.installed_problems([*gate.BASE, "cairn-contracts-audit"])
    assert gate.installed_problems(["cairndev", "cairn-client"])


# ── D7.4 contracts and operators ─────────────────────────────────────────────────────────────

def test_a_changed_contract_digest_is_refused():
    tampered = copy.deepcopy(SNAPSHOT)
    tampered["definitions"][0]["digest"] = "0" * 64
    (problem,) = gate.contract_problems(CONTRACTS, HOSTS, tampered, SNAPSHOT)
    assert "differ from the pre-fold snapshot" in problem
    assert tampered["definitions"][0]["id"] in problem


def test_a_contract_package_hosted_by_another_distribution_is_refused():
    hosts = HOSTS | {CONTRACTS[0]: {"cairndev", CONTRACTS[0]}}
    (problem,) = gate.contract_problems(CONTRACTS, hosts, SNAPSHOT, SNAPSHOT)
    assert f"not hosted by cairndev alone: {{'{CONTRACTS[0]}'" in problem


def test_a_missing_contract_package_is_refused():
    assert any("12 contract packages resolved" in p
               for p in gate.contract_problems(CONTRACTS[1:], HOSTS, SNAPSHOT, SNAPSHOT))


def test_a_missing_operator_is_refused():
    missing = dict(OPERATORS)
    missing.pop("data/profile")
    (problem,) = gate.operator_problems(missing)
    assert problem.startswith("21 cairn.operators entry points, not 22")


def test_a_second_distribution_declaring_a_builtin_name_is_refused():
    """Duplicates are kept, not collapsed: a shadowing declaration of `data/profile` by another
    distribution is refused even though the distinct names still number 22."""
    pairs = list(OPERATORS.items()) + [("data/profile", "shadow-pack")]
    problems = gate.operator_problems(pairs)
    assert any(p.startswith("cairn.operators names declared more than once: {'data/profile'") for p in problems)
    assert any("shadow-pack" in p for p in problems)


def test_an_operator_from_another_distribution_is_refused():
    (problem,) = gate.operator_problems(OPERATORS | {"mcp/call": "cairn-operator-mcp"})
    assert problem == "operators not from cairndev: {'mcp/call': 'cairn-operator-mcp'}"


# ── D7.4 the in-process run ──────────────────────────────────────────────────────────────────

_TASKS = {"profile": "data/profile", "redact": "redact/text", "stage": "storage/put",
          "promote": "artifact/promote", "publish": "storage/put", "audit": "audit/store"}
_CAPS = {"data/profile": ("contract://data.profile/1.0.0", "cairn-contracts-data"),
         "redact/text": ("contract://redact.text/1.0.0", "cairn-contracts-redact"),
         "storage/put": ("contract://storage.put/1.0.0", "cairn-contracts-storage"),
         "artifact/promote": ("contract://artifact.promote/1.0.0", "cairn-contracts-artifact"),
         "audit/store": ("contract://audit.sink/1.0.0", "cairn-contracts-audit")}


def _leg(sid: str, kind: str, ref: str | None) -> dict:
    task = kind == "task"
    return {"id": sid, "kind": kind, "status": "done", "run_id": f"run-{sid}" if kind != "gate" else None,
            "template_ref": ref, "run_status": "done" if kind != "gate" else None,
            "op_file": f"{PREFIX}/lib/python3.13/site-packages/operator_x/{sid}.py" if task else None}


def _run() -> dict:
    """The shape ``_probe_run`` reports for the run the gate makes."""
    legs = [_leg("profile", "task", "task:data/profile"), _leg("data-ok", "gate", None),
            _leg("redact", "task", "task:redact/text"),
            _leg("engine-check", "flow", "flow:hello-cairn/engine-check"),
            _leg("stage", "task", "task:storage/put"), _leg("review", "gate", None),
            *(_leg(s, "task", f"task:{_TASKS[s]}") for s in ("promote", "publish", "audit"))]
    return {"status": "done", "legs": legs, "prefix": PREFIX, "approved": ["review", "promote"],
            "resolution": copy.deepcopy(SNAPSHOT),
            "invocations": [{"path": sid, "operator_ref": op, "capability_ref": _CAPS[op][0],
                             "distributions": [_CAPS[op][1]]} for sid, op in _TASKS.items()]}


def test_a_run_that_did_not_approve_exactly_the_person_gates_is_refused():
    """A fold regression that dropped promote's operator-declared gate still ends `done`; the gate
    must notice that only `review` was approved."""
    run = _run() | {"approved": ["review"]}
    problems = gate.run_problems(run, OPERATORS, CONTRACTS, SNAPSHOT)
    assert any(p.startswith("the run approved ['review'], not exactly the person gates") for p in problems)


def _problems(run: dict) -> list[str]:
    return gate.run_problems(run, OPERATORS, CONTRACTS, SNAPSHOT)


def test_the_run_the_gate_makes_passes():
    assert _problems(_run()) == []


def _paused(r):
    r["status"] = "paused"
    r["legs"][5]["status"] = "awaiting_approval"


def _foreign_op(r):
    r["legs"][0]["template_ref"], r["invocations"][0]["operator_ref"] = "task:mcp/call", "mcp/call"


def _op_outside_venv(r):
    r["legs"][2]["op_file"] = "/checkout/packages/cairn-operator-redact-regex/x.py"


def _no_run_record(r):
    r["legs"][4]["run_id"] = None


def _no_invocation(r):
    r["invocations"] = r["invocations"][1:]


def _capability_elsewhere(r):
    r["invocations"][3]["distributions"] = ["cairn-contracts-platform-only"]


def _closure_drift(r):
    r["resolution"]["distributions"][0]["version"] = "9.9.9"


@pytest.mark.parametrize(("tamper", "says"), [
    (_paused, "ended 'paused', not 'done'"),
    (_foreign_op, "which cairndev does not declare (declared by 'cairn-operator-mcp')"),
    (_op_outside_venv, "not from the venv"),
    (_no_run_record, "has no Run naming its operator"),
    (_no_invocation, "admitted with no contract invocation"),
    (_capability_elsewhere, "not by a contract package cairndev hosts"),
    (_closure_drift, "record's contract closure differs from the pre-fold snapshot"),
])
def test_a_run_that_does_not_hold_is_refused(tamper, says):
    run = _run()
    tamper(run)
    problems = gate.run_problems(run, OPERATORS | {"mcp/call": "cairn-operator-mcp"}, CONTRACTS, SNAPSHOT)
    assert any(says in p for p in problems), problems
