"""hello-budget — the budget cell's domain pack (tests/e2e/hello/test_budget_cap.py). Data only."""
from __future__ import annotations

from cairn.core.pack import Pack

pack = Pack.from_manifest(__file__)

__all__ = ["pack"]
