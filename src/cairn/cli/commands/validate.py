"""``cairn validate`` — offline artifact validation (no LLM, no backend).

Thin CLI wrapper over :mod:`cairn.core.validate`. Walks the supplied paths
(files or directories), runs the appropriate validator per artifact type, and
renders a per-file report. Exit code is non-zero if any artifact errors — or,
with ``--strict``, if any artifact warns. Wire into CI to gate publishes.

The two entry points (``cmd`` for Typer, ``main(argv)`` for tests +
``python -m``) share one render path. Logic stays in core/validate.py; this
file only does argument handling and output formatting.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Optional

import typer

from cairn.core.operator.reports import STRICT_ENV  # ADR-0103
from cairn.core.validate import (
    ValidationReport,
    expand_paths,
    validate_path,
)


def _format(report: ValidationReport, *, verbose: bool) -> str:
    mark = "✓" if report.ok else "✗"
    head = f"{mark} {report.path}  [{report.artifact_type}]"
    lines = [head]
    for issue in report.issues:
        sym = "ERROR" if issue.level == "error" else "warn "
        lines.append(f"    {sym}: {issue.message}")
    return "\n".join(lines)


#: The compile refusal for a capability the active profile does not bind (ADR-0111 D10).
_UNBOUND = "has no [profile.bindings] entry"


def _pack_profiles(files: list[Path]) -> list[Path]:
    """The profiles shipped by the packs that hold ``files`` (``<pack>/profiles/*.toml``)."""
    found: list[Path] = []
    for f in files:
        for parent in f.resolve().parents:
            if (parent / "pack.toml").is_file():
                for prof in sorted((parent / "profiles").glob("*.toml")):
                    if prof not in found:
                        found.append(prof)
                break
    return found


def _unbound_hint(reports: list[ValidationReport]) -> str:
    """When a capability is unbound, say which profile was active and which to use.

    A pack validates under the profile it runs with: the default dev profile binds
    only the builtins every pack shares, so a pack whose flow needs another capability
    (hello-cairn's nightly-job needs ``secrets.store``) ships a profile that binds it."""
    failing = [Path(r.path) for r in reports
               if any(i.level == "error" and _UNBOUND in i.message for i in r.issues)]
    if not failing:
        return ""
    from cairn.core.settings import getenv  # noqa: PLC0415

    active = getenv("CAIRN_PROFILE", "") or "local-dev (the default; set CAIRN_PROFILE to use another)"
    lines = [f"\nhint: a capability has no binding in the active profile ({active}). "
             "Validate under the profile the pack runs with:",
             "  CAIRN_PROFILE=<profile.toml> cairn validate …"]
    shipped = _pack_profiles(failing)
    if shipped:
        lines.append("  profiles shipped with this pack: " + ", ".join(str(p) for p in shipped))
    return "\n".join(lines)


def _run(
    paths: list[str],
    *,
    strict: bool,
    template_paths: Optional[list[str]],
    verbose: bool,
) -> int:
    """Shared implementation. Returns the exit code."""
    tpaths = [Path(p) for p in template_paths] if template_paths else None
    files = expand_paths([Path(p) for p in paths])
    if not files:
        print("no artifacts found", file=sys.stderr)
        return 1

    reports = [validate_path(f, template_search_paths=tpaths) for f in files]
    for r in reports:
        print(_format(r, verbose=verbose))

    n_err = sum(1 for r in reports if not r.ok)
    n_warn = sum(len(r.warnings) for r in reports)
    print(
        f"\n{len(reports)} artifact(s): {len(reports) - n_err} ok, "
        f"{n_err} failed, {n_warn} warning(s)"
    )
    hint = _unbound_hint(reports)
    if hint:
        print(hint)
    failed = n_err > 0 or (strict and n_warn > 0)
    return 1 if failed else 0


# ─── entry points ───────────────────────────────────────────────────


def cmd(
    paths: list[str] = typer.Argument(
        ..., help="Files or directories to validate.",
    ),
    strict: bool = typer.Option(
        False, "--strict",
        help="Treat warnings as failures (exit non-zero).",
    ),
    template_path: Optional[list[str]] = typer.Option(
        None, "--template-path",
        help="Extra directory to resolve skill template refs from (repeatable).",
    ),
    verbose: bool = typer.Option(
        False, "-v", "--verbose",
    ),
) -> None:
    """Validate templates / skills / policies offline (no LLM, no backend)."""
    from cairn.profiles import default_dev_profile  # noqa: PLC0415 — #2241 D: local compiles get local-dev

    default_dev_profile()
    # ADR-0103: anything that runs an operator under `cairn validate` reports
    # STRICTLY — an undeclared stage/series/scalar raises here, never in a job.
    os.environ.setdefault(STRICT_ENV, "1")
    rc = _run(
        paths=paths,
        strict=strict,
        template_paths=template_path,
        verbose=verbose,
    )
    raise typer.Exit(code=rc)


def main(argv: Optional[list[str]] = None) -> int:
    """argv-style entry point. Kept for backward-compatible test use.

    The Typer command (``cmd``) is the primary entry; this just wraps
    argparse for ``python -m cairn.cli.commands.validate`` and the test
    suite, both of which pre-date Typer.
    """
    from cairn.profiles import default_dev_profile  # noqa: PLC0415 — #2241 D, as cmd

    default_dev_profile()
    parser = argparse.ArgumentParser(
        prog="cairn validate",
        description="Validate templates / skills / policies offline (no LLM, no backend).",
    )
    parser.add_argument("paths", nargs="+", help="Files or directories to validate.")
    parser.add_argument(
        "--strict", action="store_true",
        help="Treat warnings as failures (exit non-zero).",
    )
    parser.add_argument(
        "--template-path", action="append", default=None, dest="template_paths",
        help="Extra directory to resolve skill template refs from (repeatable).",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    return _run(
        paths=args.paths,
        strict=args.strict,
        template_paths=args.template_paths,
        verbose=args.verbose,
    )


if __name__ == "__main__":
    raise SystemExit(main())
