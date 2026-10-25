"""BYO corpus resolution + role composition (#516/#517)."""
from __future__ import annotations

import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test-dummy")

import shutil
import subprocess
from pathlib import Path

import pytest

from cairn.core.storage.knowledge_corpora import (
    InMemoryWorkspaceCorpusStore, WorkspaceCorpus,
)
from cairn.services.rag.byo import (
    is_git_source, resolve_corpus_source, workspace_corpus_targets,
)


def test_is_git_source():
    assert is_git_source("git+https://github.com/x/y")
    assert is_git_source("https://github.com/x/y.git")
    assert not is_git_source("file:///tmp/x")
    assert not is_git_source("s3://b/p")


def test_resolve_file_uri(tmp_path):
    (tmp_path / "a.md").write_text("hi")
    assert resolve_corpus_source(f"file://{tmp_path}", allow_local=True) == str(tmp_path)


def test_resolve_s3_not_wired_yet():
    with pytest.raises(NotImplementedError):
        resolve_corpus_source("s3://b/p")


def test_resolve_git_clones_the_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "runbook.md").write_text("# db failover")
    env = {**os.environ, "GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@t",
           "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@t"}
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True, env=env)
    subprocess.run(["git", "add", "."], cwd=repo, check=True, env=env)
    subprocess.run(["git", "commit", "-q", "-m", "x"], cwd=repo, check=True, env=env)
    dest = resolve_corpus_source(f"git+file://{repo}", allow_local=True)  # a deployment caller
    try:
        assert Path(dest, "runbook.md").is_file()  # cloned the docs
    finally:
        shutil.rmtree(dest, ignore_errors=True)


def test_workspace_corpus_targets_carry_role_and_collection():
    c = WorkspaceCorpus(workspace_id="prod", name="runbooks",
                        source_uri="git+https://x/y", role="incident")
    [t] = workspace_corpus_targets([c])
    assert t.role == "incident" and t.name == "runbooks"
    # collection is embedder-scoped (#512) + slugified (#535), in the corpus's own
    # workspace (ADR-0062): ws_<workspace>__rag_<name>__<fp>
    assert t.collection.startswith("ws_prod__rag_runbooks__") and t.corpus_id == "ws:prod/runbooks"


@pytest.mark.asyncio
async def test_list_roles_unions_byo_corpora(monkeypatch):
    import cairn.core.storage.knowledge_corpora as kc
    store = InMemoryWorkspaceCorpusStore()
    await store.add(WorkspaceCorpus(workspace_id="default", name="myrunbooks",
                                    source_uri="git+https://x/y", role="incident"))
    monkeypatch.setattr(kc, "_STORE", store)

    from cairn.services.rag.roles import list_roles
    roles = list_roles([])  # no packs → BYO only
    assert "incident" in roles
    assert any(t.name == "myrunbooks" for t in roles["incident"])
