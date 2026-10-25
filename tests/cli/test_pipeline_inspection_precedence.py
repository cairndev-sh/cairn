"""Inspection follows the declared-pipeline precedence of ADR-0106 (#113).

Scratch packs are loaded through the public dev-pack path; resolution and the
show/plan/explain projections are real. Nothing launches a pipeline or model.
"""
from __future__ import annotations

import json
import re

import pytest
import typer
from typer.testing import CliRunner

from cairn.cli.commands import pipeline, validate
from cairn.core.dispatch.start import resolve_start_ref

runner = CliRunner()
validate_app = typer.Typer()
validate_app.command('validate')(validate.cmd)
validate_app.command('_noop')(lambda: None)


def _wrapper(pack):
    directory = pack / 'pipelines'
    directory.mkdir()
    (directory / 'echo.yaml').write_text(
        'name: echo\nkind: pipeline\ndescription: The declared inspection wrapper.\n'
        'steps:\n  - id: inspect\n    template: echo\n', encoding='utf-8')


@pytest.fixture
def shared_pack(scratch_pack, dev_packs):
    pack = scratch_pack('inspection_pack', 'inspection-pack')
    _wrapper(pack)
    dev_packs(pack)
    return pack


@pytest.mark.parametrize('command', [('show',), ('show', '--plan'), ('explain',), ('explain', '--json')])
@pytest.mark.parametrize('ref', [
    'inspection-pack/echo', 'inspection_pack/echo',
    'pipeline:inspection-pack/echo', 'flow:inspection-pack/echo',
    ' pipeline:inspection-pack/echo ',
])
def test_shared_ref_inspects_the_pipeline_that_run_resolves(shared_pack, command, ref):
    kind, canonical = resolve_start_ref(ref)
    assert kind == 'pipeline'
    result = runner.invoke(pipeline.app, [command[0], ref, *command[1:]])
    assert result.exit_code == 0, result.output or repr(result.exception)
    if command == ('explain', '--json'):
        document = json.loads(result.output)
        assert document['ref'] == f'pipeline:{canonical}'
        assert document['steps'][0]['id'] == 'inspect'
    elif command == ('show',):
        assert canonical in result.output
        assert 'The declared inspection wrapper.' in result.output
        assert 'inspect [template]' in result.output
    else:
        assert 'inspect' in result.output


@pytest.mark.parametrize('command', ['show', 'explain'])
@pytest.mark.parametrize('ref', ['inspection-pack/missing', 'pipeline:inspection-pack/missing'])
def test_unknown_ref_remains_an_exit_two(shared_pack, command, ref):
    result = runner.invoke(pipeline.app, [command, ref])
    assert result.exit_code == 2
    assert 'unknown pipeline' in result.output


@pytest.mark.parametrize('command', ['show', 'explain'])
def test_a_file_path_is_not_treated_as_an_installed_pipeline(shared_pack, command):
    result = runner.invoke(pipeline.app, [command, str(shared_pack / 'pipelines' / 'echo.yaml')])
    assert result.exit_code == 2
    assert 'unknown pipeline' in result.output


@pytest.mark.parametrize('command', ['show', 'explain'])
def test_a_flow_without_a_declared_wrapper_is_not_synthesized(scratch_pack, dev_packs, command):
    dev_packs(scratch_pack('unwrapped', 'unwrapped'))
    result = runner.invoke(pipeline.app, [command, 'unwrapped/echo'])
    assert result.exit_code == 2
    assert 'unknown pipeline' in result.output


@pytest.mark.parametrize('command', ['show', 'explain'])
def test_two_pipeline_definitions_at_a_canonical_address_are_still_refused(
        scratch_pack, dev_packs, command):
    first = scratch_pack('first_pack', 'clash-a_b')
    second = scratch_pack('second_pack', 'clash_a-b')
    _wrapper(first)
    _wrapper(second)
    dev_packs(first, second)
    result = runner.invoke(pipeline.app, [command, 'clash-a-b/echo'])
    assert result.exit_code != 0
    assert 'names 2 installed pipelines' in result.output + str(result.exception)


def test_run_argument_help_describes_only_declared_pipelines():
    result = runner.invoke(pipeline.app, ['run', '--help'], env={'COLUMNS': '240'})
    assert result.exit_code == 0
    plain = re.sub(r'[│╭╮╰╯─]', '', result.output)
    plain = ' '.join(plain.split()).lower()
    assert 'which runs as its one-leg pipeline' not in plain
    assert 'bare flow refs are refused' in plain


@pytest.mark.parametrize('kind', ['data', 'ml'])
@pytest.mark.parametrize('stage', ['noop', 'not-a-cairn-stage'])
def test_data_and_ml_yaml_use_existing_flow_validation(tmp_path, kind, stage):
    """This part of #113 already works on current main; keep it as a control."""
    source = tmp_path / f'{kind}.yaml'
    source.write_text(f'name: offline\nkind: {kind}\nstages:\n  - stage: {stage}\n', encoding='utf-8')
    result = runner.invoke(validate_app, ['validate', str(source)])
    assert result.exit_code == (0 if stage == 'noop' else 1), result.output
    assert '[flow]' in result.output
    assert 'applies_to' not in result.output and 'rules' not in result.output
    if stage != 'noop':
        assert 'unknown' in result.output.lower() and stage in result.output
