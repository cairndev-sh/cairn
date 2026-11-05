"""Suggestion actuation surface (#488, ADR-0060 §D5 / ADR-0012 line).

How an ACCEPTED suggestion becomes a reviewable artifact — always stopping at
"here is the proposed change + evidence — approve?", never auto-applying (the
ADR-0012 autonomy line: diagnosis automated, actuation human-gated).

**Adaptive binding** (the decision): actuation follows where the pack lives.

  - **GitHub linked** → a GitHub **issue** for a diff-less suggestion (Phase 1:
    routed intent + evidence, no concrete change yet), upgrading to a **PR**
    once an optimizer executor (#468/#469) fills a real diff. For code-managed /
    dogfood deployments.
  - **not linked** → a **database-based issue**: the suggestion row itself,
    queued in the suggestion ledger for the console review→approve→new-version
    flow (ADR-0049 DB-managed customers). The apply→version step is deferred
    (ADR-0060 OD#6).

``CAIRN_SUGGEST_ACTUATOR`` selects: ``github`` | ``db`` | ``auto`` | ``off``
(default ``off`` — a deployment turns actuation on explicitly). ``auto`` picks
GitHub when a github.com remote + authed ``gh`` are present, else the DB issue.
"""
from __future__ import annotations
from cairn.core.settings import getenv

import logging
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional, Protocol, runtime_checkable

from cairn.core.storage.suggestions import Suggestion

_log = logging.getLogger("cairn.eval.actuation")


@dataclass
class ActuationResult:
    """The outcome of actuating a suggestion — a reviewable artifact, never an
    applied change. ``url`` is the issue/PR link when a binding produced one."""

    kind: str            # "github-issue" | "github-pr" | "db-issue" | "noop"
    ok: bool
    detail: str
    url: Optional[str] = None
    # The planned artifact (mode, title, body, and branch/file for a PR) — always
    # populated, even on dry-run, so callers/tests can inspect what WOULD happen.
    plan: Optional[dict[str, Any]] = None


@runtime_checkable
class SuggestionActuator(Protocol):
    kind: str
    async def actuate(self, suggestion: Suggestion) -> ActuationResult: ...


def proposal_markdown(s: Suggestion) -> str:
    """The human-reviewable proposal — description + evidence + routing. Serves
    as the GitHub issue/PR body AND the DB issue's rendered detail. Public (#297)."""
    ev = s.evidence or {}
    cluster = ev.get("cluster", {})
    lines = [
        f"**Suggestion `{s.suggestion_id}`** — category `{s.category}` → target `{s.target}`",
        f"- confidence: {s.confidence}",
        f"- cluster fingerprint: `{s.cluster_fingerprint}`",
        "",
        "## Diagnosis",
        s.description or s.title,
        "",
        "## Evidence",
        f"- failure_type: `{cluster.get('failure_type','')}`",
        f"- signature: `{cluster.get('signature','')}` at `{cluster.get('locus','') or 'workflow'}`",
        f"- affected runs ({cluster.get('size', 0)}): {', '.join(cluster.get('member_run_ids', []) or []) or '—'}",
    ]
    if ev.get("untrusted_metric"):
        lines += ["", "> ⚠️ Metric not yet proven honest (#458) — treat this suggestion as advisory."]
    if s.proposed_diff:
        lines += ["", "## Proposed change", "```diff", s.proposed_diff, "```"]
    else:
        lines += ["", "_No concrete diff yet — this is the routed intent + evidence "
                  "(optimizer executors #468/#469 fill the diff, which upgrades this to a PR)._"]
    return "\n".join(lines) + "\n"


_proposal_markdown = proposal_markdown  # the earlier private name; same function


def _plan(s: Suggestion) -> dict[str, Any]:
    """Build the actuation plan. ``mode`` is ``pr`` when the suggestion carries a
    concrete diff, else ``issue`` (routed intent + evidence only)."""
    has_diff = bool(s.proposed_diff)
    plan: dict[str, Any] = {
        "mode": "pr" if has_diff else "issue",
        "title": f"[suggestion] {s.title}",
        "body": proposal_markdown(s),
    }
    if has_diff:  # a PR needs a branch + a file to carry the change
        plan["branch"] = f"suggest/{s.suggestion_id}"
        plan["file"] = f"docs/suggestions/{s.suggestion_id}.md"
    return plan


def _github_linked(repo_dir: str = ".") -> bool:
    """True when ``repo_dir`` has a github.com remote AND ``gh`` is authed —
    best-effort, quiet (any failure → not linked)."""
    try:
        remote = subprocess.run(
            ["git", "remote", "get-url", "origin"], cwd=repo_dir,
            capture_output=True, text=True, timeout=5,
        )
        if remote.returncode != 0 or "github.com" not in remote.stdout:
            return False
        auth = subprocess.run(["gh", "auth", "status"], capture_output=True, text=True, timeout=5)
        return auth.returncode == 0
    except Exception:  # noqa: BLE001 — gh/git missing or slow → treat as not linked
        return False


class GitHubActuator:
    """Opens a GitHub **issue** for a diff-less suggestion, a **PR** once a diff
    exists. ``dry_run`` builds the plan without touching git/gh (the testable
    path)."""

    kind = "github"

    def __init__(self, *, repo_dir: str = ".", dry_run: bool = False) -> None:
        self._repo = Path(repo_dir)
        self._dry_run = dry_run

    async def actuate(self, suggestion: Suggestion) -> ActuationResult:
        plan = _plan(suggestion)
        result_kind = f"github-{plan['mode']}"
        if self._dry_run:
            return ActuationResult(kind=result_kind, ok=True, detail="dry-run — nothing opened", plan=plan)
        try:
            if plan["mode"] == "issue":
                url = subprocess.run(
                    ["gh", "issue", "create", "--title", plan["title"], "--body", plan["body"]],
                    cwd=self._repo, capture_output=True, text=True, check=True,
                ).stdout.strip()
                return ActuationResult(kind=result_kind, ok=True, detail="issue opened", url=url, plan=plan)
            # PR path — a concrete diff exists; carry it on a branch.
            path = self._repo / plan["file"]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(plan["body"], encoding="utf-8")
            self._git("checkout", "-b", plan["branch"])
            self._git("add", plan["file"])
            self._git("commit", "-m", plan["title"])
            self._git("push", "-u", "origin", plan["branch"])
            url = subprocess.run(
                ["gh", "pr", "create", "--title", plan["title"], "--body", plan["body"]],
                cwd=self._repo, capture_output=True, text=True, check=True,
            ).stdout.strip()
            return ActuationResult(kind=result_kind, ok=True, detail="PR opened", url=url, plan=plan)
        except Exception as e:  # noqa: BLE001 — surface, don't crash the CLI
            _log.warning("GitHub actuation failed for %s: %s", suggestion.suggestion_id, e)
            return ActuationResult(kind=result_kind, ok=False, detail=f"actuation failed: {e}", plan=plan)

    def _git(self, *args: str) -> None:
        subprocess.run(["git", *args], cwd=self._repo, capture_output=True, text=True, check=True)


class GitHubApiActuator:
    """Same contract as :class:`GitHubActuator`, over the GitHub REST API with
    ``httpx`` — for a deployment whose engine container has no ``git``/``gh``
    (prod's does not, #1969). Needs ``GITHUB_TOKEN`` (or ``GH_TOKEN``) with
    ``contents`` + ``pull_requests`` write on ``CAIRN_SUGGEST_REPO`` (``owner/repo``);
    ``CAIRN_SUGGEST_BASE`` names the base branch (default ``main``).

    A diff-less suggestion opens an **issue**; a suggestion with a diff writes
    ``docs/suggestions/<id>.md`` on branch ``suggest/<id>`` and opens a **PR**.
    The change is never applied to the pack: the PR IS the human gate."""

    kind = "github-api"

    def __init__(self, *, repo: str, token: str, base: str = "main", dry_run: bool = False,
                 api_url: str = "https://api.github.com") -> None:
        self._repo, self._token, self._base, self._dry_run, self._api = repo, token, base, dry_run, api_url.rstrip("/")

    def _headers(self) -> dict[str, str]:
        return {"authorization": f"Bearer {self._token}", "accept": "application/vnd.github+json",
                "x-github-api-version": "2022-11-28", "user-agent": "cairn-suggest"}

    async def actuate(self, suggestion: Suggestion) -> ActuationResult:
        plan = _plan(suggestion)
        result_kind = f"github-{plan['mode']}"
        if self._dry_run:
            return ActuationResult(kind=result_kind, ok=True, detail="dry-run — nothing opened", plan=plan)
        try:
            import base64

            import httpx  # optional at import time; required to actuate
        except ImportError as e:  # pragma: no cover
            return ActuationResult(kind=result_kind, ok=False, detail=f"httpx not installed: {e}", plan=plan)
        r = self._repo
        try:
            async with httpx.AsyncClient(base_url=self._api, headers=self._headers(), timeout=30) as c:
                if plan["mode"] == "issue":
                    res = await c.post(f"/repos/{r}/issues", json={"title": plan["title"], "body": plan["body"]})
                    res.raise_for_status()
                    return ActuationResult(kind=result_kind, ok=True, detail="issue opened", url=res.json().get("html_url"), plan=plan)
                head = await c.get(f"/repos/{r}/git/ref/heads/{self._base}")
                head.raise_for_status()
                sha = head.json()["object"]["sha"]
                ref = await c.post(f"/repos/{r}/git/refs", json={"ref": f"refs/heads/{plan['branch']}", "sha": sha})
                if ref.status_code not in (201, 422):  # 422 = branch exists (a re-accept); reuse it
                    ref.raise_for_status()
                body = {"message": plan["title"], "content": base64.b64encode(plan["body"].encode()).decode(), "branch": plan["branch"]}
                existing = await c.get(f"/repos/{r}/contents/{plan['file']}", params={"ref": plan["branch"]})
                if existing.status_code == 200:
                    body["sha"] = existing.json()["sha"]
                put = await c.put(f"/repos/{r}/contents/{plan['file']}", json=body)
                put.raise_for_status()
                pr = await c.post(f"/repos/{r}/pulls", json={"title": plan["title"], "body": plan["body"], "head": plan["branch"], "base": self._base})
                if pr.status_code == 422:  # a PR for this branch already exists: find it
                    open_prs = await c.get(f"/repos/{r}/pulls", params={"head": f"{r.split('/')[0]}:{plan['branch']}", "state": "open"})
                    open_prs.raise_for_status()
                    items = open_prs.json()
                    if items:
                        return ActuationResult(kind=result_kind, ok=True, detail="PR already open", url=items[0].get("html_url"), plan=plan)
                pr.raise_for_status()
                return ActuationResult(kind=result_kind, ok=True, detail="PR opened", url=pr.json().get("html_url"), plan=plan)
        except Exception as e:  # noqa: BLE001 — surface, never crash the accept
            _log.warning("GitHub API actuation failed for %s: %s", suggestion.suggestion_id, e)
            return ActuationResult(kind=result_kind, ok=False, detail=f"actuation failed: {e}", plan=plan)


class DbIssueActuator:
    """The database-based issue: the accepted suggestion row itself, queued in
    the suggestion ledger for the console review→approve→new-version flow
    (ADR-0049). No git — for deployments that don't keep packs in GitHub. The
    apply→bump-installed-workflow-version step is the deferred follow-on
    (ADR-0060 Open Decision #6)."""

    kind = "db"

    async def actuate(self, suggestion: Suggestion) -> ActuationResult:
        return ActuationResult(
            kind="db-issue",
            ok=True,
            detail=(
                f"recorded as a database issue (suggestion {suggestion.suggestion_id}) "
                "for console review — apply→new-version deferred (ADR-0060 OD#6)"
            ),
            plan=_plan(suggestion),
        )


def get_actuator(*, dry_run: bool = False, repo_dir: str = ".") -> Optional[SuggestionActuator]:
    """The configured actuator, or ``None`` when actuation is off (the default).
    ``CAIRN_SUGGEST_ACTUATOR`` = ``github`` | ``db`` | ``auto`` | ``off``. ``auto``
    picks GitHub when linked (github.com remote + authed ``gh``), else the DB
    issue — "GitHub if linked, otherwise a database-based issue"."""
    spec = getenv("CAIRN_SUGGEST_ACTUATOR", "off").strip().lower()
    # #1969 — a token + repo means the REST binding, which needs no git/gh in
    # the container. Wins for `github` and `auto`; `db`/`off` are untouched.
    repo = getenv("CAIRN_SUGGEST_REPO", "").strip()
    token = (os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "").strip()
    if spec in ("github", "auto") and repo and token:
        return GitHubApiActuator(repo=repo, token=token, base=getenv("CAIRN_SUGGEST_BASE", "main").strip() or "main", dry_run=dry_run)
    if spec in ("github", "git-pr", "gitpr", "pr", "issue"):
        return GitHubActuator(dry_run=dry_run, repo_dir=repo_dir)
    if spec in ("db", "console", "database"):
        return DbIssueActuator()
    if spec in ("auto", "on"):
        return (GitHubActuator(dry_run=dry_run, repo_dir=repo_dir)
                if _github_linked(repo_dir) else DbIssueActuator())
    return None


__all__ = [
    "ActuationResult",
    "DbIssueActuator",
    "GitHubActuator",
    "SuggestionActuator",
    "get_actuator",
    "proposal_markdown",
]
