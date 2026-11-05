"""Turn coverage.py's JSON report into the badge the README reads (#329).

    coverage json -o coverage.json
    python scripts/coverage_badge.py coverage.json site/badge.json

The output is a shields.io endpoint file. It reports the number and never fails on it: there is
no threshold here, because a threshold moved only to get a run through says nothing.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path


def colour(percent: float) -> str:
    """Shades only: the badge informs, and no colour means a job failed."""
    return "16a34a" if percent >= 80 else "ca8a04" if percent >= 60 else "dc2626"


def badge(report: dict) -> dict:
    percent = float(report["totals"]["percent_covered"])
    return {"schemaVersion": 1, "label": "coverage", "message": f"{percent:.0f}%", "color": colour(percent)}


def main(argv: list[str]) -> int:
    source, target = Path(argv[1]), Path(argv[2])
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(badge(json.loads(source.read_text(encoding="utf-8")))) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
