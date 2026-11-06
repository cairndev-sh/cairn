"""Tests for bounded-memory subprocess spawning in the pull worker.

Verifies that large continuous output does not cause unbounded memory use
in ``_spawn`` and that exit status and the diagnostic tail are preserved.
"""
from __future__ import annotations

import sys

import pytest

from cairn.core.pull_worker import _ERROR_TAIL, _spawn


@pytest.mark.asyncio
async def test_spawn_bounds_large_output_and_preserves_tail():
    """A verbose process that prints tens of megabytes must return a short tail
    that ends with the final printed line, and a zero exit code."""
    # ~20 MB of output. Each line is small so the final line is unambiguous.
    script = (
        "import sys\n"
        "for i in range(2_000_000):\n"
        "    print(f'line {i}', flush=True)\n"
        "print('FINAL_MARKER', flush=True)\n"
    )
    code, tail = await _spawn([sys.executable, "-c", script])
    assert code == 0
    assert len(tail) <= _ERROR_TAIL
    assert tail.endswith("FINAL_MARKER")
    # The tail must contain some of the recent numbered lines, not just the marker.
    assert "line " in tail


@pytest.mark.asyncio
async def test_spawn_preserves_nonzero_exit_code():
    """A failing process still returns its non-zero exit code along with the tail."""
    script = (
        "import sys\n"
        "print('before failure', flush=True)\n"
        "sys.exit(42)\n"
    )
    code, tail = await _spawn([sys.executable, "-c", script])
    assert code == 42
    assert "before failure" in tail
    assert len(tail) <= _ERROR_TAIL
