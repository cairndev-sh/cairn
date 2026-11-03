"""The packs a CLI test drives the CLI against (#2428).

The public tree ships one domain pack, ``examples/hello-cairn``; the platform's domain and
official operator packs are not there. A CLI test that needs a pipeline, a flow or a kernel
in use therefore takes it from the example pack, or from a scratch pack it writes, and never
from a platform pack. What can only be pinned on the platform's packs lives in
``tests/packs/test_cli_on_shipped_packs.py``.

Both fixtures load packs through ``CAIRN_PACK_PATH`` (the documented dev loop, #1545) into a
fresh registry and restore the session's registry afterwards.
"""
from __future__ import annotations

import os
import textwrap
from pathlib import Path

import pytest

from tests import _layout

EXAMPLE_PACK = _layout.REPO / "examples" / "hello-cairn"

#: An agentic flow that compiles: the dev loop (`cairn run --mock`, `--interactive`,
#: `--dry-run`) compiles agentic templates only, and the example pack ships a data flow.
#: One node, a python function the core registers (an empty sequence fails to compile);
#: the tests stub the flow's body, so it never runs.
SCRATCH_FLOW = textwrap.dedent("""\
    ---
    name: echo
    version: 0.1.0
    description: A scratch agentic flow for the CLI tests.
    flow:
      type: state_machine
      root:
        type: sequence
        children:
          - type: python_node
            function: confidence_scorer
      nodes: {}
    tools:
      required: []
    ---

    # echo
    """)


def _load(pack_path: str | None) -> None:
    from cairn.core import pack as pack_api
    from cairn.core.pack import loader

    if pack_path is None:
        os.environ.pop(loader.DEV_PACK_PATH_ENV, None)
    else:
        os.environ[loader.DEV_PACK_PATH_ENV] = pack_path
    pack_api.clear()
    loader._WARNED_COMPAT.clear()
    loader.load_all_packs(strict=False)


@pytest.fixture
def dev_packs():
    """``dev_packs(dir, …)`` registers the pack dirs (or dirs of packs) given."""
    from cairn.core.pack import loader

    before = os.environ.get(loader.DEV_PACK_PATH_ENV)
    yield lambda *dirs: _load(os.pathsep.join(str(d) for d in dirs))
    _load(before)


@pytest.fixture
def example_pack(dev_packs) -> Path:
    dev_packs(EXAMPLE_PACK)
    return EXAMPLE_PACK


@pytest.fixture
def scratch_pack(tmp_path):
    """``scratch_pack(directory, name, flow=True)`` writes a pack and returns its dir.

    A directory spelled ``scratch_dev`` for a pack named ``scratch-dev`` is the shape the
    platform's hyphenated packs have, which is what the ref joins must survive (#2126)."""
    def make(directory: str, name: str, *, flow: bool = True) -> Path:
        d = tmp_path / "packs" / directory
        d.mkdir(parents=True)
        (d / "pack.toml").write_text(
            f'[pack]\nname = "{name}"\nversion = "0.1.0"\nkind = "domain"\n')
        (d / "__init__.py").write_text(
            "from cairn.core.pack import Pack\n\npack = Pack.from_manifest(__file__)\n")
        if flow:
            (d / "flows").mkdir()
            (d / "flows" / "echo.md").write_text(SCRATCH_FLOW)
        return d
    return make


@pytest.fixture(autouse=True)
def _cairn_profile_is_restored():
    """`cairn run` sets a dev profile for its own process, and CliRunner runs it in this one: without
    this, whichever test runs next on the worker reads the wrong backends."""
    before = os.environ.get("CAIRN_PROFILE")
    yield
    if before is None:
        os.environ.pop("CAIRN_PROFILE", None)
    else:
        os.environ["CAIRN_PROFILE"] = before
