"""``scripts/check_public_set.py`` (ADR-0120 D1, ADR-0134 D1, ADR-0137): what a public-set venv
may hold, judged on a synthetic installed set against this tree's ``pyproject.toml`` files."""
from __future__ import annotations

import importlib.util

import pytest

from tests import _layout

_spec = importlib.util.spec_from_file_location("check_public_set", _layout.REPO / "scripts" / "check_public_set.py")
check = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check)

BASE = [("cairndev", "0.1.2"), ("cairn-client", "0.1.2"), ("httpx", "0.28.1")]


def test_the_folded_set_is_read_from_the_tree_not_a_list():
    folded = check.folded_set()
    assert "cairn-operator-mcp" in folded and "cairn-contracts-audit" in folded
    assert len(folded) == 27
    # what still builds on its own is never folded
    assert not folded & {"cairn-client", "cairn-operator-dataflow-metaflow", "cairn-operator-dataflow-skypilot"}


def test_the_base_install_passes():
    assert check.private_installed(installed=BASE) == []


@pytest.mark.parametrize("folded", ["cairn-operator-mcp", "cairn_contracts_audit", "cairn-operator-dataflow"])
def test_a_folded_distribution_installed_on_its_own_is_refused(folded):
    found = check.private_installed(installed=[*BASE, (folded, "0.1.0")])
    assert len(found) == 1 and "folded into cairndev (ADR-0137): uninstall it" in found[0], found


def test_a_selected_extra_admits_the_optional_engines_and_the_example_pack_only():
    optional = [("cairn-operator-dataflow-metaflow", "0.1.2"), ("cairn-pack-hello", "0.2.0")]
    assert check.private_installed(installed=[*BASE, *optional]) == [
        "cairn-operator-dataflow-metaflow 0.1.2", "cairn-pack-hello 0.2.0"]
    assert check.private_installed(["metaflow", "hello"], installed=[*BASE, *optional]) == []
    # an extra never admits a folded pack, asyncssh or a name the tree does not build
    extra = [("asyncssh", "2.23.1"), ("cairn-operator-remote", "0.1.0"), ("cairn-pack-other", "1.0")]
    assert [f.split()[0] for f in check.private_installed(["ssh", "hello"], installed=[*BASE, *extra])] == [
        "asyncssh", "cairn-operator-remote", "cairn-pack-other"]


def test_an_unknown_extra_is_an_error():
    with pytest.raises(SystemExit, match="declares no extra nope"):
        check.private_installed(["nope"], installed=BASE)
