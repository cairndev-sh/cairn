"""``cairn pipeline show`` no longer says flow legs cannot run (0.1.2 verification,
bug 12): it ended with "that executor lands in #1127", but flow legs run (the
feature proof runs profile-and-publish's flow legs on every cell)."""
from __future__ import annotations

from typer.testing import CliRunner

from cairn.cli.commands import pipeline


def test_show_a_pipeline_with_flow_legs_has_no_stale_note(example_pack):
    res = CliRunner().invoke(pipeline.app, ["show", "hello-cairn/profile-and-publish"])
    assert res.exit_code == 0, res.output
    assert "[flow]" in res.output  # the pipeline does have flow legs
    assert "engine adapter" in res.output
    assert "#1127" not in res.output and "lands in" not in res.output
