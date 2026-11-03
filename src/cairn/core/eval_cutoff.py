"""Dated eval rows and a run's EVAL CUTOFF (#2033, ADR-0102 D1/D3).

LiveCodeBench dates every item so a run can declare a cutoff and be measured
only against material that existed before the model did (research C). Cairn's
registry had no notion of WHEN a row is from, so a mined eval set kept absorbing
signal captured after training — and every re-measurement of an old model
silently used newer rows than the last one, which is how a "regression" appears
that is really a changed yardstick.

This module is the vocabulary and the filter, in one place, because three
callers ask the same question and must get the same answer:

* the **registry** stamps ``dated_at`` on a version (``cairn.core.storage.artifacts``);
* the **holdout door** (:mod:`cairn.core.holdout`) skips versions and rows dated
  after the run's cutoff before it compares anything;
* the **scorecard** (:mod:`cairn_platform.judge_scorecard`) excludes them from the
  agreement figure and SAYS how many it excluded.

Two rules that look small and are not:

**A date-only cutoff is inclusive of its own day.** ``2026-06-01`` keeps a row
stamped ``2026-06-01T23:59``: a human who writes a date means the day, and a
cutoff that silently dropped most of it would be a bug nobody could see from
the number.

**An undated row is KEPT, and counted separately.** Every row written before
this field exists is undated; excluding them would empty every eval set that
predates #2033, and treating them as "excluded" in the count would report a
number that means nothing. They are reported as ``undated`` so a caller can
tell "nothing was after the cutoff" from "nothing carries a date at all".
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Iterable, Optional, Sequence

#: The input key a run declares its eval cutoff under. One name, because the
#: door and the scorecard both go looking for it and a second spelling would be
#: a cutoff that silently did not apply.
EVAL_CUTOFF_KEY = "eval_cutoff"

#: The field every dated row and every dated registry version carries.
DATED_AT_KEY = "dated_at"

#: What the cutoff and ``dated_at`` grammar accepts, in words, for every refusal to quote.
ACCEPTED_FORM = ("an ISO-8601 date or timestamp "
                 "(e.g. '2026-06-01' or '2026-06-01T10:00:00+00:00')")


class CutoffError(ValueError):
    """A date the cutoff filter refuses. A ``ValueError`` so the API turns it
    into the 422 it already turns a bad role into, never a 500."""


def _parse(value: Any, *, what: str) -> tuple[datetime, bool]:
    """``(instant, date_only)`` for an ISO-8601 date or timestamp.

    ``date_only`` travels with the instant because the two are compared
    differently: a date-only CUTOFF covers its whole day (see the module
    docstring), while a date-only ROW is the start of its day.
    """
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=timezone.utc)), False
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc), True
    text = str(value).strip()
    if not text:
        raise CutoffError(f"{what} is empty")
    normalized = text[:-1] + "+00:00" if text.endswith("Z") else text
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        raise CutoffError(f"{what} {value!r} is not {ACCEPTED_FORM}") from None
    date_only = len(text) == 10 and text.count("-") == 2
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed, date_only


def normalize_cutoff(value: Optional[Any]) -> Optional[str]:
    """Validate a declared eval cutoff, or ``None`` when none was declared.

    Returns the caller's own spelling back once it parses, so the number the
    scorecard prints is the one the human typed — a cutoff re-rendered as a
    timestamp reads like the platform changed it.
    """
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    _parse(value, what="eval cutoff")
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value).strip()


def normalize_dated_at(value: Optional[Any]) -> Optional[str]:
    """Validate a ``dated_at``, or ``None``. Same grammar as the cutoff."""
    if value is None:
        return None
    if isinstance(value, str) and not value.strip():
        return None
    _parse(value, what="dated_at")
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value).strip()


def row_dated_at(row: Any) -> Optional[str]:
    """The ``dated_at`` a row carries, or ``None``.

    Only that key. Deliberately NOT falling back to ``created_at`` or a
    timestamp found elsewhere in the row: "when this example is from" and "when
    this record was written" are different facts, and a cutoff that quietly
    compared the second would exclude rows nobody dated.
    """
    if not isinstance(row, dict):
        return None
    value = row.get(DATED_AT_KEY)
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    return str(value).strip()


def _cutoff_instant(cutoff: Any) -> datetime:
    """The first instant that is AFTER the cutoff — a date-only cutoff covers
    its own whole day, so the boundary is the start of the next one."""
    instant, date_only = _parse(cutoff, what="eval cutoff")
    if not date_only:
        return instant
    try:
        return instant + timedelta(days=1)
    except OverflowError:  # 9999-12-31 has no next day, and nothing can be after it
        return datetime.max.replace(tzinfo=timezone.utc)


def after_cutoff(dated_at: Optional[Any], cutoff: Optional[Any]) -> bool:
    """Is ``dated_at`` strictly after ``cutoff``? Undated is never after."""
    if cutoff is None or dated_at is None:
        return False
    stamp = normalize_dated_at(dated_at)
    if stamp is None:
        return False
    return _parse(stamp, what="dated_at")[0] >= _cutoff_instant(cutoff)


def filter_rows(
    rows: Sequence[dict] | Iterable[dict], cutoff: Optional[Any],
) -> tuple[list[dict], int, int]:
    """``(kept, excluded, undated)`` for ``rows`` under ``cutoff``.

    ``excluded`` is the number the scorecard prints. ``undated`` is the honesty
    counter: a caller can tell "nothing was after the cutoff" from "nothing here
    carries a date at all", which are very different states to be in when you
    are about to quote a measurement.
    """
    materialized = list(rows)
    if cutoff is None:
        return materialized, 0, sum(1 for r in materialized if row_dated_at(r) is None)
    boundary = _cutoff_instant(cutoff)
    kept: list[dict] = []
    excluded = undated = 0
    for row in materialized:
        stamp = row_dated_at(row)
        if stamp is None:
            undated += 1
            kept.append(row)
            continue
        if _parse(stamp, what="dated_at")[0] >= boundary:
            excluded += 1
            continue
        kept.append(row)
    return kept, excluded, undated


def version_after_cutoff(meta: Optional[dict], cutoff: Optional[Any]) -> bool:
    """Is a registry version dated after the cutoff?

    The door's coarse pass: a whole version stamped after the cutoff is out of
    the comparison without reading a byte of it. A version with no ``dated_at``
    is never excluded — see the module docstring.
    """
    if not meta or cutoff is None:
        return False
    return after_cutoff(meta.get(DATED_AT_KEY), cutoff)


def cutoff_in(values: Any) -> Optional[str]:
    """The eval cutoff a run declared, found anywhere in its inputs.

    Mirrors :func:`cairn.core.holdout.dataset_refs_in`: the launch door does not
    know a pipeline's input SHAPE, so it walks the structure looking for the one
    key rather than requiring every pipeline to hoist it to the top level. The
    first value found wins and a malformed one raises — a cutoff that silently
    did not apply is the failure this whole module exists to prevent.
    """
    stack = [values]
    while stack:
        node = stack.pop(0)
        if isinstance(node, dict):
            if EVAL_CUTOFF_KEY in node:
                found = normalize_cutoff(node[EVAL_CUTOFF_KEY])
                if found is not None:
                    return found
            stack.extend(node.values())
        elif isinstance(node, (list, tuple)):
            stack.extend(node)
    return None


def describe_exclusion(excluded: int, cutoff: Optional[Any]) -> str:
    """The sentence a scorecard prints, or ``""`` when there is nothing to say."""
    if cutoff is None or not excluded:
        return ""
    return (f"{excluded} row{'' if excluded == 1 else 's'} excluded by the eval cutoff "
            f"{cutoff}")


__all__ = [
    "ACCEPTED_FORM",
    "DATED_AT_KEY",
    "EVAL_CUTOFF_KEY",
    "CutoffError",
    "after_cutoff",
    "cutoff_in",
    "describe_exclusion",
    "filter_rows",
    "normalize_cutoff",
    "normalize_dated_at",
    "row_dated_at",
    "version_after_cutoff",
]
