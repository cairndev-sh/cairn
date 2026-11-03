"""Generic architecture boundaries, machine-checked (docs/architecture/17-issue-workflow.md).

The rules are the root ``CLAUDE.md`` §3 dependency directions, read from the tree with
``ast`` (never grep): each import statement in ``cairn/`` is an edge ``(file, module,
top-level | lazy)``. A rule lists the forbidden edges and an ALLOWLIST of the sites that
already violate it at the time the rule was written — the recorded debt in
``docs/architecture/ARCHITECTURE-HEALTH.md`` "Boundary violations (actual)". The allowlist
may only SHRINK: a new file, a new module, or a lazy import promoted to module top fails
the rule. Removing debt does not require touching this file; adding debt does, and that is
the review conversation the rule exists to force.

Every rule is also exercised on synthetic source (the ``*_can_fail`` tests) so the gate is
shown to be able to fail — a green that measures nothing is worse than no test
(``tests/CLAUDE.md``).

The three rules tagged ADR-0120 (core/cli → cairn.packs, core/cli/services → operator
pack, lower layers → cairn.core.triggers) carry the open-source boundary (ADR-0120 D8,
#2424): their allowlists name the issue that removes each site and fail when a site is
gone. The non-import half of that boundary (operator names as strings, hard-coded refs,
the public router set) is ``test_public_core_boundary.py``.

Complements, does not replace: ``test_engine_registry.py::test_core_never_imports_an_operator_pack``,
``test_cairn_client_contract.py::test_client_imports_nothing_from_cairn_core``,
``test_mcp_server_layering.py``, ``test_leg_executor.py::test_a_data_leg_never_imports_langgraph``.
"""
from __future__ import annotations

import ast
from importlib.util import resolve_name
import re
from pathlib import Path

import pytest

from tests import _layout

REPO = _layout.REPO

# --------------------------------------------------------------------------- edge model
Edge = tuple[str, str, str]  # (relative file, imported module, "top" | "lazy")


def _edges_of_source(src: str, rel: str) -> list[Edge]:
    tree = ast.parse(src)
    out: list[Edge] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.append((rel, alias.name, "top" if node.col_offset == 0 else "lazy"))
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if node.level:
                parts = list(Path(rel).parent.parts)
                # Operator distributions install the package beneath src/, not
                # the repository's enclosing cairn/operators directories.
                if "src" in parts:
                    parts = parts[parts.index("src") + 1:]
                module = resolve_name("." * node.level + module, ".".join(parts))
            kind = "top" if node.col_offset == 0 else "lazy"
            if node.module:
                out.append((rel, module, kind))
            # `from cairn import packs` and `from .. import packs` must not
            # hide the forbidden dependency behind the allowed parent package.
            for alias in node.names:
                if alias.name != "*":
                    out.append((rel, f"{module}.{alias.name}", kind))
    return out


def _tree_edges() -> list[Edge]:
    edges: list[Edge] = []
    # the engine, and the operator packs that live beside it rather than in it (public tree)
    roots = [_layout.ENGINE, *(d for d in _layout.operator_packs() if not d.is_relative_to(_layout.ENGINE))]
    for path in sorted(p for root in roots for p in root.rglob("*.py")):
        rel = _layout.rel(path)
        if "/tests/" in rel or "/worktrees/" in rel:
            continue
        edges.extend(_edges_of_source(path.read_text(), rel))
    return edges


def _pack_of(rel: str) -> str | None:
    for part in Path(rel).parts:
        if part.startswith("operator_"):
            return part
    return None


def _builtin_modules() -> frozenset[str]:
    """The import names of the builtin operator packs — they all ship with the public core."""
    return frozenset(p.name for d in _layout.operator_packs("builtins") for p in (d / "src").glob("operator_*") if p.is_dir())


def _in_this_tree(rel: str) -> bool:
    """An allowlisted site in the platform's trees can be proven present only where they are."""
    return _layout.PRIVATE_TREES or not rel.startswith(("cairn/operators/official/", "cairn/packs/"))


# --------------------------------------------------------------------------- rules
R_PACKS = "core/cli → cairn.packs (root CLAUDE.md §3; ADR-0120 D3/D8: the public core never imports a domain pack)"
R_OPERATOR = "core/cli/services → operator pack (core never imports an operator pack; ADR-0120 D8)"
R_TRIGGERS = "lower layers (cairn/core outside triggers) → cairn.core.triggers (no new imports of the HTTP layer; ADR-0120 D6)"
R_PLATFORM = "public core → cairn_platform (ADR-0120 D3: the public core never imports the platform; owner decision 2026-10-12, no allowlist)"
R_PACK_CORE = "operator pack → cairn.* beyond the operator contract (ADR-0120 D7: cairn.core.operator, plus cairn.core.llm / cairn.core.eval for LLM and eval operators; #2453)"

#: #2453 step 1 — the exact inventory of what operator packs import beyond the operator
#: contract, one entry per (file, module), each with its PROPOSED category for the
#: category (owner decision 2026-10-12: all four approved). (A) re-exports landed in
#: `cairn.core.operator` and (D) the dataflow contract joined the allowed roots, so only
#: (B) context injection (a follow-up) and (C) private dependencies (they leave with the
#: platform, #2427) remain. The rule below fails on a new edge; the only-shrinks test fails
#: on a stale entry.
PACK_CORE_INVENTORY: dict[Edge, str] = {
    # (B) a context injection (what the runtime hands the operator)
    # (C) a private dependency the pack must stop using, or that moves private with it
    ("cairn/operators/official/dataset_checks/src/operator_dataset_checks/dataflow_stages.py", "cairn.core.backend_selection", "lazy"): "C",
    ("cairn/operators/official/ingest/src/operator_ingest/_api.py", "cairn.core.connection_probe", "lazy"): "C",
    ("cairn/operators/official/eval/src/operator_eval/operators/gate.py", "cairn.core.judge_agreement", "top"): "C",
    ("cairn/operators/official/serving/src/operator_serving/operators/serve_lifecycle.py", "cairn.core.ssh_exec", "top"): "C",
    ("cairn/operators/builtins/data_profile/src/operator_data_profile/operators/profile.py", "cairn.services.data", "top"): "C",
    ("cairn/operators/builtins/data_profile/src/operator_data_profile/schemas/profile.py", "cairn.services.data", "top"): "C",
    ("cairn/operators/official/data_expectations/src/operator_data_expectations/dataflow_stages.py", "cairn.services.data", "lazy"): "C",
    ("cairn/operators/official/data_expectations/src/operator_data_expectations/operators/expectations.py", "cairn.services.data", "top"): "C",
    ("cairn/operators/official/data_expectations/src/operator_data_expectations/schemas/expectations.py", "cairn.services.data", "top"): "C",
    ("cairn/operators/builtins/mcp/src/operator_mcp/operators/call.py", "cairn.services.mcp.client", "top"): "C",
    ("cairn/operators/official/notify_slack/src/operator_notify_slack/operators/slack.py", "cairn.services.mcp.client", "lazy"): "C",
    ("cairn/operators/builtins/memory_local/src/operator_memory_local/operators/search.py", "cairn.services.rag.embeddings", "top"): "C",
    ("cairn/operators/builtins/memory_local/src/operator_memory_local/operators/write.py", "cairn.services.rag.embeddings", "lazy"): "C",
    ("cairn/operators/builtins/rag_local/src/operator_rag_local/operators/fetch.py", "cairn.services.rag.embeddings", "top"): "C",
    ("cairn/operators/builtins/rag_local/src/operator_rag_local/operators/search.py", "cairn.services.rag.embeddings", "top"): "C",
    ("cairn/operators/builtins/memory_local/src/operator_memory_local/operators/search.py", "cairn.services.rag.stores", "top"): "C",
    ("cairn/operators/builtins/rag_local/src/operator_rag_local/operators/search.py", "cairn.services.rag.stores", "top"): "C",
    ("cairn/operators/builtins/memory_local/src/operator_memory_local/operators/search.py", "cairn.services.rag.stores.base", "top"): "C",
    ("cairn/operators/builtins/rag_local/src/operator_rag_local/operators/search.py", "cairn.services.rag.stores.base", "top"): "C",
}
# Each rule: name, predicate(rel, module) -> bool (True = the edge is forbidden),
# allowlist of (rel, module-prefix, kind) sites recorded as debt.
RULES: dict[str, tuple] = {
    R_PACKS: (
        lambda rel, mod: rel.startswith(("cairn/core/", "cairn/cli/")) and mod.startswith("cairn.packs"),
        set(),  # #2414 — core never imports a domain pack
    ),
    R_OPERATOR: (
        lambda rel, mod: rel.startswith(("cairn/core/", "cairn/cli/", "cairn/services/"))
        and mod.split(".")[0].startswith("operator_"),
        set(),
    ),
    R_TRIGGERS: (
        lambda rel, mod: rel.startswith("cairn/core/") and not rel.startswith("cairn/core/triggers/")
        and mod.startswith("cairn.core.triggers"),
        {("cairn/core/app_client.py", "cairn.core.triggers.main", "lazy"),
         ("cairn/core/app_client.py", "cairn.core.triggers.server", "lazy"),
         },
    ),
    "anything outside cairn/cli → cairn.cli": (
        lambda rel, mod: not rel.startswith("cairn/cli/") and mod.startswith("cairn.cli"),
        set(),  # the Ask REPL's `cairn.cli.commands.run` edge left with it (ADR-0134 D6)
    ),
    "services → cairn.packs (services never import packs)": (
        lambda rel, mod: rel.startswith("cairn/services/") and mod.startswith("cairn.packs"),
        set(),
    ),
    "operator pack → another operator pack (no private pack-to-pack coupling)": (
        lambda rel, mod: rel.startswith("cairn/operators/")
        and mod.split(".")[0].startswith("operator_")
        and mod.split(".")[0] != _pack_of(rel)
        # builtin → builtin ships together in the public core: not a private coupling
        # (#2427: the dataflow SSH engine → operator_remote, both builtins). The optional
        # tier (ADR-0134 D1) is installed only through an extra: no exemption either way.
        and not (rel.startswith("cairn/operators/builtins/") and mod.split(".")[0] in _builtin_modules()),
        set(),
    ),
    "operator pack → cairn.core.{dispatch,engine,triggers} internals": (
        lambda rel, mod: rel.startswith("cairn/operators/")
        and any(mod.startswith(f"cairn.core.{x}") for x in ("dispatch", "engine", "triggers")),
        # #2455 (ADR-0120 D7): monitor/dispatch launches through OperationContext.launch;
        # no operator pack reaches dispatch, engine or triggers.
        set(),
    ),
    R_PLATFORM: (
        lambda rel, mod: rel.startswith(("cairn/core/", "cairn/cli/", "cairn/services/", "cairn/operators/builtins/",
                                         "cairn/operators/optional/"))  # the optional tier is public too (ADR-0134 D1)
        and (mod == "cairn_platform" or mod.startswith("cairn_platform.")),
        set(),  # never: the platform depends on the core, not the reverse
    ),
    R_PACK_CORE: (
        lambda rel, mod: rel.startswith("cairn/operators/") and mod.startswith("cairn.")
        and mod not in ("cairn", "cairn.core", "cairn.services")  # the bare package edge; its names are the real targets
        # the contract roots: the operator package, llm/eval for those operators, and the
        # dataflow contract for engine/stage providers (owner decision 2026-10-12, #2453 D)
        and not (mod in ("cairn.core.operator", "cairn.core.llm", "cairn.core.eval", "cairn.core.dataflow")
                 or mod.startswith(("cairn.core.operator.", "cairn.core.llm.", "cairn.core.eval.", "cairn.core.dataflow."))),
        set(PACK_CORE_INVENTORY),
    ),
    # Ask's engine wiring (it was cairn/ask/) is the platform's since ADR-0134 D6: none of it
    # may come back into the engine tree under that name.
    "client/sdk/ask → cairn.core (surfaces import nothing from core)": (
        lambda rel, mod: any(rel.startswith(f"cairn/{d}/") for d in ("client", "ask", "sdk")) and mod.startswith("cairn.core"),
        set(),
    ),
}


def _allowed(edge: Edge, allow: set[tuple[str, str, str]]) -> bool:
    rel, mod, kind = edge
    for a_rel, a_mod, a_kind in allow:
        if rel == a_rel and (mod == a_mod or mod.startswith(a_mod + ".")) and (kind == a_kind or (a_kind == "top" and kind == "lazy")):
            return True
    return False


def _violations(edges: list[Edge], rule: str) -> list[Edge]:
    pred, allow = RULES[rule]
    return sorted(e for e in edges if pred(e[0], e[1]) and not _allowed(e, allow))


@pytest.fixture(scope="module")
def tree_edges() -> list[Edge]:
    return _tree_edges()


@pytest.mark.parametrize("rule", list(RULES))
def test_boundary_rule_holds_on_the_tree(tree_edges, rule):
    bad = _violations(tree_edges, rule)
    assert not bad, (
        f"{rule}\nnew boundary violation(s) — a recorded-debt allowlist may only shrink:\n"
        + "\n".join(f"  {rel}: {kind} import of {mod}" for rel, mod, kind in bad)
    )


@pytest.mark.parametrize("rule, rel, src", [
    (R_PACKS, "cairn/core/new_module.py", "from cairn.packs.training import x\n"),
    (R_OPERATOR, "cairn/core/dispatch/leg_executor.py", "def f():\n    import operator_training.flow_stages\n"),
    (R_TRIGGERS, "cairn/core/storage/new_store.py", "def f():\n    from cairn.core.triggers.store import S\n"),
    (R_TRIGGERS, "cairn/core/engine/pipeline.py", "from cairn.core.triggers.declared import D\n"),  # promoted lazy → top
    (R_PACK_CORE, "cairn/operators/official/x/src/operator_x/op.py", "from cairn.core.storage import default_store\n"),
    (R_PLATFORM, "cairn/core/dispatch/dispatcher.py", "def f():\n    from cairn_platform.hooks import score\n"),
    (R_PLATFORM, "cairn/cli/commands/pilot.py", "import cairn_platform.pilot_ledger\n"),
    (R_PACK_CORE, "cairn/operators/builtins/y/src/operator_y/op.py", "def f():\n    from cairn.core import workspace\n"),  # the bare-package form cannot hide it
    (R_PACKS, "cairn/cli/commands/new.py", "def f():\n    from cairn.packs.training import rules\n"),
    (R_OPERATOR, "cairn/services/rag/new.py", "def f():\n    from operator_eval.operators import delta\n"),
    (R_TRIGGERS, "cairn/core/dispatch/scheduler.py", "def f():\n    from cairn.core.triggers.inbox import get_trigger_inbox\n"),
    (R_TRIGGERS, "cairn/core/new_loose_module.py", "def f():\n    from cairn.core.triggers.auth import hash_password\n"),
    ("anything outside cairn/cli → cairn.cli", "cairn/core/dispatch/start.py", "from cairn.cli.commands import run\n"),
    ("services → cairn.packs (services never import packs)", "cairn/services/llm/factory.py", "def f():\n    from cairn.packs.training import rules\n"),
    ("operator pack → another operator pack (no private pack-to-pack coupling)", "cairn/operators/official/training/src/operator_training/train_entry.py", "def f():\n    from operator_eval.operators import delta\n"),
    ("operator pack → another operator pack (no private pack-to-pack coupling)", "cairn/operators/builtins/dataflow/src/operator_dataflow/engines.py", "def f():\n    from operator_training.flow_stages import load\n"),  # builtin → official
    ("operator pack → another operator pack (no private pack-to-pack coupling)", "cairn/operators/official/serving/src/operator_serving/x.py", "from operator_remote.operators.exec import RemoteExecOp\n"),  # official → builtin
    # ADR-0134 D1: an optional pack imports no other operator pack, and no builtin imports one
    ("operator pack → another operator pack (no private pack-to-pack coupling)", "cairn/operators/optional/dataflow_x/src/operator_dataflow_x/engine.py", "from operator_remote.operators.exec import RemoteExecOp\n"),  # optional → builtin
    ("operator pack → another operator pack (no private pack-to-pack coupling)", "cairn/operators/builtins/dataflow/src/operator_dataflow/engines.py", "def f():\n    import operator_dataflow_x.engine\n"),  # builtin → optional
    (R_PLATFORM, "cairn/operators/optional/dataflow_x/src/operator_dataflow_x/engine.py", "def f():\n    from cairn_platform.hooks import score\n"),
    ("operator pack → cairn.core.{dispatch,engine,triggers} internals", "cairn/operators/official/eval/src/operator_eval/operators/gate.py", "from cairn.core.dispatch.dispatcher import RunDispatcher\n"),
    ("client/sdk/ask → cairn.core (surfaces import nothing from core)", "cairn/client/api.py", "from cairn.core.storage import runs\n"),
])
def test_boundary_rule_can_fail(rule, rel, src):
    """Synthetic offending source: the rule must flag it (a gate that cannot fail is not a gate)."""
    assert _violations(_edges_of_source(src, rel), rule), f"{rule} did not flag {rel}: {src!r}"


@pytest.mark.parametrize("src", [
    "from ..packs.training import rules\n",
    "from .. import packs\n",
    "def f():\n    from ..packs import training\n",
])
def test_relative_import_cannot_bypass_core_pack_boundary(src):
    rule = R_PACKS
    assert _violations(_edges_of_source(src, "cairn/core/new.py"), rule)


def test_relative_import_within_owner_is_allowed():
    edges = _edges_of_source("from . import protocols\n", "cairn/core/storage/new.py")
    assert edges == [("cairn/core/storage/new.py", "cairn.core.storage.protocols", "top")]
    assert not any(_violations(edges, rule) for rule in RULES)


def test_operator_src_layout_relative_import_resolves_to_installed_package():
    rel = "cairn/operators/official/training/src/operator_training/operators/new.py"
    edges = _edges_of_source("from ..schemas import train\n", rel)
    assert (rel, "operator_training.schemas", "top") in edges
    assert not any(_violations(edges, rule) for rule in RULES)


def test_allowlisted_debt_is_still_present_or_reported(tree_edges, capsys):
    """Debt that disappears is progress; say so, so the allowlist can shrink in the next PR."""
    gone = []
    for rule, (pred, allow) in RULES.items():
        for a in allow:
            if not any(_allowed(e, {a}) and pred(e[0], e[1]) for e in tree_edges):
                gone.append((rule, a))
    if gone:
        print("\nrecorded debt no longer present — shrink the allowlist:\n" + "\n".join(f"  {r}: {a}" for r, a in gone))


# --------------------------------------------------------------------------- ADR-0120 debt names its issue
# The open-source boundary rules (ADR-0120 D8, #2424) are stricter than the print-only
# report above: every allowlisted site names the Phase 1 issue that removes it, and a
# site that no longer violates FAILS until its entry is deleted — the list only shrinks.
ADR0120_DEBT: dict[str, dict[tuple[str, str, str], str]] = {
    R_PACKS: {},  # closed by #2414
    R_OPERATOR: {},  # closed by #2415
    R_PACK_CORE: {e: "#2453" for e in PACK_CORE_INVENTORY},
    R_TRIGGERS: {
        # #2416 leaves one owner decision: app_client.py builds the HTTP app in-process
        # (a surface helper — it moves with the surface, #2427).
        ("cairn/core/app_client.py", "cairn.core.triggers.main", "lazy"): "#2416",
        ("cairn/core/app_client.py", "cairn.core.triggers.server", "lazy"): "#2416",
    },
}


@pytest.mark.parametrize("rule", list(ADR0120_DEBT))
def test_adr0120_debt_names_its_issue(rule):
    """ADR-0120 D8: each allowlisted site of an open-source boundary rule names the issue that removes it."""
    _, allow = RULES[rule]
    tagged = ADR0120_DEBT[rule]
    assert set(tagged) == allow, (
        f"{rule}: allowlist and issue map disagree — untagged {sorted(allow - set(tagged))}, "
        f"stale tags {sorted(set(tagged) - allow)}")
    assert all(ISSUE_RE.fullmatch(i) for i in tagged.values())


@pytest.mark.parametrize("rule", list(ADR0120_DEBT))
def test_adr0120_debt_only_shrinks(tree_edges, rule):
    """ADR-0120 D8: an allowlisted site that no longer violates must leave the allowlist (the list only shrinks)."""
    pred, allow = RULES[rule]
    gone = sorted(a for a in allow if _in_this_tree(a[0])
                  and not any(_allowed(e, {a}) and pred(e[0], e[1]) for e in tree_edges))
    assert not gone, f"{rule}: recorded debt is gone — delete these entries (and their issue tags):\n" + "\n".join(
        f"  {a} ({ADR0120_DEBT[rule][a]})" for a in gone)


# --------------------------------------------------------------------------- strict xfail needs an issue
XFAIL_RE = re.compile(r"xfail\(")
ISSUE_RE = re.compile(r"#\d{2,6}")


def _strict_xfails_without_issue(src: str, rel: str) -> list[str]:
    """A strict expected failure is a promise tied to an issue (ROADMAP option A): the
    reason must name the issue so the xfail cannot outlive the fix unnoticed."""
    found: list[str] = []
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        name = getattr(func, "attr", None) or getattr(func, "id", None)
        if name != "xfail":
            continue
        kw = {k.arg: k.value for k in node.keywords}
        strict = kw.get("strict")
        if not (isinstance(strict, ast.Constant) and strict.value is True):
            continue
        reason = kw.get("reason")
        text = reason.value if isinstance(reason, ast.Constant) and isinstance(reason.value, str) else ""
        if not ISSUE_RE.search(text):
            found.append(f"{rel}:{node.lineno}")
    return found


def test_strict_xfail_names_an_issue():
    offenders: list[str] = []
    packs = sorted(p for d in _layout.operator_packs() for p in d.rglob("tests/test_*.py"))
    for path in sorted((REPO / "tests").rglob("test_*.py")) + packs:
        rel = _layout.rel(path)
        if "/worktrees/" in rel:
            continue
        src = path.read_text()
        if XFAIL_RE.search(src):
            offenders.extend(_strict_xfails_without_issue(src, rel))
    assert not offenders, "strict xfail without an issue reference in reason=: " + ", ".join(offenders)


def test_strict_xfail_rule_can_fail():
    bad = "import pytest\n@pytest.mark.xfail(strict=True, reason='flaky')\ndef test_x():\n    assert 0\n"
    good = "import pytest\n@pytest.mark.xfail(strict=True, reason='#2171 readiness leg')\ndef test_x():\n    assert 0\n"
    lenient = "import pytest\n@pytest.mark.xfail(reason='no issue needed when not strict')\ndef test_x():\n    assert 0\n"
    assert _strict_xfails_without_issue(bad, "t.py") == ["t.py:2"]
    assert _strict_xfails_without_issue(good, "t.py") == []
    assert _strict_xfails_without_issue(lenient, "t.py") == []
