"""Metadata-only contract descriptor inspection; no provider execution."""
from dataclasses import FrozenInstanceError
from importlib.metadata import PathDistribution
import socket
import json

import yaml
from jsonschema import Draft202012Validator
from cairn.core.reporting import ReportSpec
from cairn.core.pack import contracts

import pytest

from cairn.core.pack.contracts import ContractDiscoveryError, discover_contracts


def wheel_layout(tmp_path, name="example-contracts", revision="1.0.0", identifier="example.input"):
    root = tmp_path / name
    assets = root / "cairn_contracts" / name
    assets.mkdir(parents=True)
    info = root / f"{name.replace('-', '_')}-1.0.0.dist-info"
    info.mkdir()
    (info / "METADATA").write_text(f"Metadata-Version: 2.1\nName: {name}\nVersion: 1.0.0\n")
    (assets / "input.json").write_text('{}')
    manifest = assets / "contracts.toml"
    manifest.write_text(f'''format = "cairn.contracts/1"
[distribution]
name = "{name}"
version = "1.0.0"
[[definitions]]
id = "{identifier}"
revision = "{revision}"
kind = "schema"
path = "input.json"
''')
    (info / "RECORD").write_text(f"cairn_contracts/{name}/contracts.toml,,\ncairn_contracts/{name}/input.json,,\n")
    return PathDistribution(info), manifest


def inspect(dist):
    return discover_contracts(distributions=[dist])


def test_descriptor_without_imports_or_network(tmp_path, monkeypatch):
    dist, _ = wheel_layout(tmp_path)
    def forbidden(*args, **kwargs):
        pytest.fail("inspection attempted provider loading or networking")
    monkeypatch.setattr("importlib.metadata.EntryPoint.load", forbidden)
    monkeypatch.setattr(socket, "socket", forbidden)
    result = inspect(dist)
    assert result[0].name == "example-contracts"
    assert result[0].definitions[0].id == "example.input"
    assert result[0].definitions[0].path.read_text() == '{}'
    with pytest.raises(FrozenInstanceError):
        result[0].name = "mutated"
    assert isinstance(result, tuple)
    assert isinstance(result[0].definitions, tuple)


def test_discovery_enumerates_metadata(tmp_path, monkeypatch):
    dist, _ = wheel_layout(tmp_path)
    monkeypatch.setattr("cairn.core.pack.contracts.installed_distributions", lambda: [dist])
    assert discover_contracts() == inspect(dist)


@pytest.mark.parametrize("old,new", [
    ('format = "cairn.contracts/1"', 'format = "wrong"'),
    ('format = "cairn.contracts/1"', 'extra = true\nformat = "cairn.contracts/1"'),
    ('kind = "schema"', 'kind = "python"'),
    ('kind = "schema"', 'kind = "schema"\nextra = true'),
    ('id = "example.input"', 'id = "NoNamespace"'),
    ('revision = "1.0.0"', 'revision = "1"'),
    ('revision = "1.0.0"', 'revision = "1.0.0-beta"'),
    ('revision = "1.0.0"', 'revision = 1'),
    ('name = "example-contracts"', 'name = "different"'),
    ('version = "1.0.0"', 'version = "2.0.0"'),
    ('version = "1.0.0"', 'version = "1.0.0"\nextra = true'),
    ('path = "input.json"', ''),
])
def test_malformed_descriptor_rejected(tmp_path, old, new):
    dist, manifest = wheel_layout(tmp_path)
    manifest.write_text(manifest.read_text().replace(old, new))
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


@pytest.mark.parametrize("path", ["/tmp/input.json", "../input.json", "sub/../input.json", "a\\\\b", "missing.json", "./input.json"])
def test_unsafe_or_unlisted_path_rejected(tmp_path, path):
    dist, manifest = wheel_layout(tmp_path)
    manifest.write_text(manifest.read_text().replace('"input.json"', f'"{path}"'))
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


@pytest.mark.parametrize("target", ["input.json", "contracts.toml"])
def test_symlink_escape_rejected(tmp_path, target):
    dist, manifest = wheel_layout(tmp_path)
    original = manifest.parent / target
    outside = tmp_path / "outside"
    outside.write_text(original.read_text())
    original.unlink()
    original.symlink_to(outside)
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


def test_unlisted_existing_asset_rejected(tmp_path):
    dist, manifest = wheel_layout(tmp_path)
    record = dist._path / "RECORD"
    record.write_text(record.read_text().splitlines()[0] + '\n')
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


def test_listed_missing_asset_rejected(tmp_path):
    dist, manifest = wheel_layout(tmp_path)
    (manifest.parent / "input.json").unlink()
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


def test_duplicate_definition_rejected(tmp_path):
    dist, manifest = wheel_layout(tmp_path)
    manifest.write_text(manifest.read_text() + '\n[[definitions]]\nid="example.input"\nrevision="1.0.0"\nkind="schema"\npath="input.json"\n')
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


def test_distinct_revisions_same_owner_allowed(tmp_path):
    dist, manifest = wheel_layout(tmp_path)
    manifest.write_text(manifest.read_text() + '\n[[definitions]]\nid="example.input"\nrevision="2.0.0"\nkind="schema"\npath="input.json"\n')
    assert len(inspect(dist)[0].definitions) == 2


def test_conflicting_owners_rejected_even_with_different_revisions(tmp_path):
    first, _ = wheel_layout(tmp_path, "first-contracts")
    second, _ = wheel_layout(tmp_path, "second-contracts", revision="2.0.0")
    with pytest.raises(ContractDiscoveryError):
        discover_contracts(distributions=[first, second])


def test_result_order_deterministic(tmp_path):
    first, _ = wheel_layout(tmp_path, "first-contracts", identifier="first.input")
    second, _ = wheel_layout(tmp_path, "second-contracts", identifier="second.input")
    assert discover_contracts(distributions=[first, second]) == discover_contracts(distributions=[second, first])


def test_non_contract_distribution_ignored(tmp_path):
    info = tmp_path / "legacy.dist-info"
    info.mkdir()
    (info / "METADATA").write_text('Name: legacy\nVersion: 1.0\n')
    assert inspect(PathDistribution(info)) == ()


def test_declared_layout_without_descriptor_rejected(tmp_path):
    dist, manifest = wheel_layout(tmp_path)
    manifest.unlink()
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


def test_dependencies_are_declarations_not_install_requests(tmp_path):
    dist, manifest = wheel_layout(tmp_path)
    manifest.write_text(manifest.read_text() + '\n[[dependencies]]\ndistribution="other-contracts"\nversion="1.0.0"\n')
    assert inspect(dist)[0].dependencies == (("other-contracts", "1.0.0"),)


def test_invalid_toml_rejected(tmp_path):
    dist, manifest = wheel_layout(tmp_path)
    manifest.write_text('format = [')
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


def test_duplicate_installed_distribution_rejected(tmp_path):
    dist, _ = wheel_layout(tmp_path)
    with pytest.raises(ContractDiscoveryError):
        discover_contracts(distributions=[dist, dist])


@pytest.mark.parametrize('declaration', [
    '[[dependencies]]\ndistribution="other"\nversion="*"',
    '[[dependencies]]\ndistribution="other"\nversion="1.0.0"\nextra=true',
    '[[dependencies]]\ndistribution="other"\nversion="1.0.0"\n'
    '[[dependencies]]\ndistribution="other"\nversion="1.0.0"',
])
def test_invalid_dependency_rejected(tmp_path, declaration):
    dist, manifest = wheel_layout(tmp_path)
    manifest.write_text(manifest.read_text() + '\n' + declaration)
    with pytest.raises(ContractDiscoveryError):
        inspect(dist)


def test_definition_body_is_not_parsed(tmp_path):
    dist, manifest = wheel_layout(tmp_path)
    (manifest.parent / 'input.json').write_bytes(b'not a schema\xff')
    assert inspect(dist)[0].definitions[0].kind == 'schema'


# B1 readers are internal: success does not imply reference/dependency closure.
def read_body(tmp_path, body, kind='schema', suffix='.json'):
    path = tmp_path / ('body' + suffix)
    path.write_text(body)
    definition = contracts.ContractDefinition('example.input', '1.0.0', kind, path)
    return contracts._read_definition(definition)


@pytest.mark.parametrize('body,suffix', [
    ('{"type":"string","type":"number"}', '.json'),
    ('{"const":NaN}', '.json'), ('{"const":1e999}', '.json'),
    ('type: string\ntype: number', '.yaml'),
    ('const: !!python/object:os.system {}', '.yaml'),
    ('const: !!binary aGVsbG8=', '.yaml'), ('const: 2026-01-01', '.yaml'),
    ('const: .inf', '.yaml'), ('const: &a [*a]', '.yaml'),
    ('const: {1: value}', '.yaml'),
])
def test_reader_rejects_non_json_or_ambiguous_data(tmp_path, body, suffix):
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, body, suffix=suffix)


def test_reader_preserves_bytes_and_legacy_yaml_loader(tmp_path):
    raw = 'type: object\nproperties: {format: {type: string}}\n'
    contents, value = read_body(tmp_path, raw, suffix='.yaml')
    assert contents == raw.encode()
    assert value['properties']['format'] == {'type': 'string'}
    assert yaml.safe_load('a: 1\na: 2') == {'a': 2}


@pytest.mark.parametrize('keyword', ['format', 'pattern', '$dynamicRef', 'contentEncoding', 'unknown'])
def test_schema_unknown_keywords_refused(tmp_path, keyword):
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, json.dumps({'properties': {'a': {keyword: 'x'}}}))


BAD = {'format': 'x'}


@pytest.mark.parametrize('schema', [
    {'allOf': [BAD]}, {'anyOf': [BAD]}, {'oneOf': [BAD]}, {'prefixItems': [BAD]},
    {'not': BAD}, {'if': BAD}, {'then': BAD}, {'else': BAD}, {'items': BAD},
    {'additionalProperties': BAD}, {'$defs': {'d': BAD}},
])
def test_every_subschema_container_is_walked(tmp_path, schema):
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, json.dumps(schema))


@pytest.mark.parametrize('value', ['!!map [[a, b]]', '!!set {a: null}', '!!bool maybe',
                                   "!!int ''", "!!float ''", '!!timestamp x'])
def test_malformed_tagged_yaml_is_a_typed_refusal(tmp_path, value):
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, f'const: {value}\n', suffix='.yaml')


def test_unsupported_extension_refused(tmp_path):
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, '{}', suffix='.txt')


@pytest.mark.parametrize('schema', [
    {'$id': 'contract://other.input/1.0.0'}, {'$schema': 'https://example.test/dialect'},
    {'type': 'invalid'}, {'required': 'name'}, {'items': []},
    {'properties': {'a': {'$id': 'https://example.test/override'}}},
])
def test_schema_bad_identity_or_shape_refused(tmp_path, schema):
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, json.dumps(schema))


def test_annotations_are_not_schema_positions(tmp_path):
    schema = {'default': {'format': 'anything', '$ref': 'https://example.test'},
              'examples': [{'unknown': True}], 'const': {'pattern': 'text'},
              '$defs': {'format': {'type': 'string'}}}
    _, parsed = read_body(tmp_path, json.dumps(schema))
    assert parsed == schema


@pytest.mark.parametrize('schema,good,bad', [
    ({'type': 'integer'}, 1, 'x'), ({'enum': [1, 2]}, 1, 3), ({'const': 2}, 2, 1),
    ({'type': 'object', 'properties': {'a': {'type': 'integer'}}, 'required': ['a'],
      'additionalProperties': False}, {'a': 1}, {'a': 1, 'b': 2}),
    ({'minProperties': 1, 'maxProperties': 2}, {'a': 1}, {}),
    ({'items': {'type': 'integer'}}, [1], ['a']),
    ({'prefixItems': [{'type': 'string'}], 'items': False}, ['a'], ['a', 1]),
    ({'minItems': 1, 'maxItems': 2, 'uniqueItems': True}, [1, 2], [1, 1]),
    ({'minimum': 1, 'maximum': 3, 'multipleOf': 1}, 2, 2.5),
    ({'exclusiveMinimum': 1, 'exclusiveMaximum': 3}, 2, 3),
    ({'minLength': 1, 'maxLength': 2}, 'a', 'abc'),
    ({'allOf': [{'type': 'integer'}, {'minimum': 1}]}, 2, 0),
    ({'anyOf': [{'const': 1}, {'const': 2}]}, 1, 3),
    ({'oneOf': [{'type': 'number'}, {'type': 'integer'}]}, 1.5, 1),
    ({'not': {'type': 'string'}}, 1, 'x'),
    ({'if': {'type': 'string'}, 'then': {'minLength': 2}, 'else': {'const': 1}}, 'ab', 'a'),
    ({'$defs': {'a/b': {'type': 'integer'}}, '$ref': '#/$defs/a~1b'}, 1, 'x'),
    ({'allOf': [{'properties': {'a': {'type': 'integer'}}}], 'additionalProperties': False}, {}, {'a': 1}),
])
def test_supported_schema_constructs_use_standard_validator(tmp_path, schema, good, bad):
    _, parsed = read_body(tmp_path, json.dumps(schema))
    validator = Draft202012Validator(parsed)
    assert validator.is_valid(good)
    assert not validator.is_valid(bad)


@pytest.mark.parametrize('value', [True, False])
def test_boolean_schema(tmp_path, value):
    assert read_body(tmp_path, json.dumps(value))[1] is value


def test_schema_annotations_and_identity(tmp_path):
    schema = {'$schema': 'https://json-schema.org/draft/2020-12/schema',
              '$id': 'contract://example.input/1.0.0', 'title': 'Example',
              'description': 'Example input', '$comment': 'Not executable',
              'deprecated': False, 'readOnly': True, 'writeOnly': False}
    assert read_body(tmp_path, json.dumps(schema))[1] == schema


def capability_body():
    return {'format': 'cairn.capability/1', 'id': 'example.input', 'revision': '1.0.0',
            'summary': 'Example', 'input': 'contract://example.data/1.0.0',
            'configuration': 'contract://example.options/1.0.0',
            'output': 'contract://example.result/1.0.0'}


def test_capability_reader_with_optional_fields(tmp_path):
    body = capability_body()
    body.update(report='contract://example.report/1.0.0', obligations=[
        {'name': 'identity', 'description': 'Keep identity', 'evidence': 'docs/identity.md'}])
    assert read_body(tmp_path, json.dumps(body), 'capability')[1] == body


@pytest.mark.parametrize('key,value', [('format', 'wrong'), ('id', 'other.input'),
    ('revision', '2.0.0'), ('summary', ''), ('input', 2), ('unexpected', True),
    ('obligations', {}), ('obligations', [{'name': 'identity'}]),
    ('obligations', [{'name': 'x', 'description': 'x', 'evidence': '../escape.md'}])])
def test_capability_closed_shape_refused(tmp_path, key, value):
    body = capability_body()
    body[key] = value
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, json.dumps(body), 'capability')


def report_body():
    return {'format': 'cairn.report/1', 'id': 'example.input', 'revision': '1.0.0',
            'spec': {'stages': ['train'], 'series': [{'name': 'loss', 'unit': ''}],
                     'scalars': ['steps']}}


def test_report_reuses_existing_serialization(tmp_path):
    body = report_body()
    body['semantics'] = {'loss': {'description': 'Objective', 'unit': '',
        'applicability': 'During training', 'comparison_context': 'Same objective'}}
    _, parsed = read_body(tmp_path, json.dumps(body), 'report')
    assert ReportSpec.from_manifest(parsed['spec']).to_manifest() == body['spec']
    assert read_body(tmp_path, json.dumps({**body, 'spec': {}, 'semantics': {}}), 'report')[1]['spec'] == {}


@pytest.mark.parametrize('spec', [[], {'unknown': []}, {'stages': 'train'},
    {'scalars': [2]}, {'scalars': ['Bad Name']}, {'stages': ['x', 'x']},
    {'series': ['loss']}, {'series': [{'name': 'loss'}]},
    {'series': [{'name': 'loss', 'unit': '', 'required': True}]}])
def test_report_spec_strict_before_permissive_parser(tmp_path, spec):
    body = report_body()
    body['spec'] = spec
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, json.dumps(body), 'report')


@pytest.mark.parametrize('semantics', [[], {'missing': {}}, {'loss': {'unknown': 'x'}},
    {'loss': {'description': 1, 'unit': '', 'applicability': 'x', 'comparison_context': 'x'}}])
def test_report_semantics_refused(tmp_path, semantics):
    body = report_body()
    body['semantics'] = semantics
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, json.dumps(body), 'report')


def test_reader_does_not_resolve_references(tmp_path, monkeypatch):
    monkeypatch.setattr(socket, 'socket', lambda *a, **k: pytest.fail('network attempted'))
    # B2 must reject this reference; B1 validates only local document shape.
    schema = {'$ref': 'https://example.test/schema'}
    assert read_body(tmp_path, json.dumps(schema))[1] == schema


@pytest.mark.parametrize('schema,bad', [
    ({'maxProperties': 0}, {'a': 1}), ({'minItems': 1}, []), ({'maxItems': 0}, [1]),
    ({'minimum': 1}, 0), ({'maximum': 1}, 2), ({'exclusiveMinimum': 1}, 1),
    ({'minLength': 1}, ''), ({'if': False, 'else': {'const': 1}}, 2),
])
def test_schema_individual_bounds_reject(tmp_path, schema, bad):
    _, parsed = read_body(tmp_path, json.dumps(schema))
    assert not Draft202012Validator(parsed).is_valid(bad)


@pytest.mark.parametrize('kind,body', [('capability', {}), ('report', {}),
    ('report', {'format': 'cairn.report/1', 'id': 'example.input', 'revision': '1.0.0',
                'spec': {}, 'extra': True}),
    ('report', {'format': 'cairn.capability/1', 'id': 'example.input', 'revision': '1.0.0', 'spec': {}}),
    ('report', {'format': 'cairn.report/1', 'id': 'other.input', 'revision': '1.0.0', 'spec': {}})])
def test_incomplete_or_unknown_definition_fields_reject(tmp_path, kind, body):
    with pytest.raises(ContractDiscoveryError):
        read_body(tmp_path, json.dumps(body), kind)


# ── B2: offline closure, data-only inventory and digests ─────────────────────
import base64
import hashlib
import os
import shutil
import subprocess
import sys
import zipfile
from importlib.metadata import distributions as installed_distributions
from pathlib import Path

from cairn.core.pack import registry as pack_registry
from cairn.core.pack.contracts import resolve_contracts

SCHEMA_URI = 'https://json-schema.org/draft/2020-12/schema'


def installed(tmp_path, name, version='1.0.0', *, definitions=(), files=(), requires=(),
              dependencies=(), metadata_files=()):
    """One installed-layout distribution: ``definitions`` are (id, revision, kind, path,
    body); ``files`` extra (path, bytes) inside the contract dir; ``metadata_files``
    (name, text) inside dist-info. Returns the PathDistribution."""
    root = tmp_path / f'{name}-{version}'
    assets = root / 'cairn_contracts' / name
    assets.mkdir(parents=True, exist_ok=True)
    info = root / f"{name.replace('-', '_')}-{version}.dist-info"
    info.mkdir(exist_ok=True)
    requires_text = ''.join(f'Requires-Dist: {r}\n' for r in requires)
    (info / 'METADATA').write_text(f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n{requires_text}')
    for filename, text in metadata_files:
        (info / filename).write_text(text)
    inventory = [f'cairn_contracts/{name}/contracts.toml']
    for identifier, revision, kind, path, body in definitions:
        (assets / path).parent.mkdir(parents=True, exist_ok=True)
        (assets / path).write_bytes(body if isinstance(body, bytes) else json.dumps(body).encode())
        inventory.append(f'cairn_contracts/{name}/{path}')
    for path, data in files:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_bytes(data)
        inventory.append(path)
    (assets / 'contracts.toml').write_bytes(descriptor(name, version, [d[:4] for d in definitions], dependencies))
    (info / 'RECORD').write_text(''.join(f'{p},,\n' for p in inventory) + f'{info.name}/METADATA,,\n')
    return PathDistribution(info)


def alpha_definitions():
    schema = {'$schema': SCHEMA_URI, '$id': 'contract://alpha.data/1.0.0', 'type': 'object',
              '$defs': {'a/b~c': {'type': 'integer'}}, 'properties': {'n': {'$ref': '#/$defs/a~1b~0c'}}}
    report = {'format': 'cairn.report/1', 'id': 'alpha.report', 'revision': '1.0.0',
              'spec': {'series': [{'name': 'loss', 'unit': ''}]}}
    return [('alpha.data', '1.0.0', 'schema', 'data.json', schema),
            ('alpha.report', '1.0.0', 'report', 'report.json', report)]


def alpha(tmp_path, **changes):
    spec = dict(definitions=alpha_definitions())
    spec.update(changes)
    return installed(tmp_path, 'alpha-contracts', **spec)


BETA_EVIDENCE = ('cairn_contracts/beta-contracts/docs/identity.md', b'# identity\n')


def beta_definitions(capability_changes=None, schema_changes=None):
    schema = {'$schema': SCHEMA_URI, '$id': 'contract://beta.result/1.0.0',
              'allOf': [{'$ref': 'contract://alpha.data/1.0.0'}, {'$ref': 'contract://alpha.data/1.0.0#/$defs/a~1b~0c'}]}
    schema.update(schema_changes or {})
    capability = {'format': 'cairn.capability/1', 'id': 'beta.run', 'revision': '1.0.0', 'summary': 'Run',
                  'input': 'contract://alpha.data/1.0.0', 'configuration': 'contract://beta.result/1.0.0',
                  'output': 'contract://beta.result/1.0.0', 'report': 'contract://alpha.report/1.0.0',
                  'obligations': [{'name': 'identity', 'description': 'Keep it', 'evidence': 'docs/identity.md'}]}
    capability.update(capability_changes or {})
    return [('beta.result', '1.0.0', 'schema', 'result.json', schema),
            ('beta.run', '1.0.0', 'capability', 'run.json', capability)]


def beta(tmp_path, *, capability_changes=None, schema_changes=None, **changes):
    spec = dict(definitions=beta_definitions(capability_changes, schema_changes), files=[BETA_EVIDENCE],
                requires=['alpha-contracts==1.0.0'], dependencies=[('alpha-contracts', '1.0.0')])
    spec.update(changes)
    return installed(tmp_path, 'beta-contracts', **spec)


def resolve(*dists):
    return resolve_contracts(distributions=list(dists))


def test_closure_resolves_cross_distribution_references_evidence_and_digests(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail('closure attempted provider loading or networking')
    monkeypatch.setattr('importlib.metadata.EntryPoint.load', forbidden)
    monkeypatch.setattr(socket, 'socket', forbidden)
    a, b = alpha(tmp_path), beta(tmp_path)
    result = resolve(b, a)
    assert [d.name for d in result] == ['alpha-contracts', 'beta-contracts']
    assert result[1].closure == ('alpha-contracts',) and result[0].closure == ()
    run = {d.id: d for d in result[1].definitions}['beta.run']
    assert run.references == ('contract://alpha.data/1.0.0', 'contract://alpha.report/1.0.0', 'contract://beta.result/1.0.0')
    assert run.evidence == (('docs/identity.md', hashlib.sha256(b'# identity\n').hexdigest()),)
    assert run.digest == hashlib.sha256(run.path.read_bytes()).hexdigest()
    assert result[1].digest == hashlib.sha256(result[1].descriptor.read_bytes()).hexdigest()
    assert {d.id: d.references for d in result[0].definitions} == {
        'alpha.data': ('contract://alpha.data/1.0.0',), 'alpha.report': ()}
    with pytest.raises(FrozenInstanceError):
        run.digest = 'x'
    assert resolve(a, b) == result, 'enumeration order must not change the result'


def test_empty_installation_and_facade(tmp_path, monkeypatch):
    assert resolve() == ()
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [])
    assert pack_registry.all_contract_definitions() == ()
    a, b = alpha(tmp_path), beta(tmp_path)
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [b, a])
    assert [d.id for d in pack_registry.all_contract_definitions()] == ['alpha.data', 'alpha.report', 'beta.result', 'beta.run']
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [b])
    with pytest.raises(ContractDiscoveryError):
        pack_registry.all_contract_definitions()


@pytest.mark.parametrize('changes', [
    dict(requires=[]),                                        # descriptor declares, wheel does not
    dict(requires=['alpha-contracts>=1.0.0']),                # not an exact pin
    dict(requires=['alpha-contracts==1.0.0', 'requests==2.0']),  # runtime dependency
    dict(requires=['alpha-contracts==1.0.0; python_version>"3"']),
    dict(requires=['alpha-contracts[x]==1.0.0']),
    dict(requires=['alpha-contracts==1.0.1'], dependencies=[('alpha-contracts', '1.0.1')]),  # wrong version
    dict(requires=['alpha-contracts==1.0.0', 'gamma-contracts==1.0.0'],
         dependencies=[('alpha-contracts', '1.0.0'), ('gamma-contracts', '1.0.0')]),  # missing
])
def test_dependency_declaration_mismatches_refuse(tmp_path, changes):
    with pytest.raises(ContractDiscoveryError):
        resolve(alpha(tmp_path), beta(tmp_path, **changes))


def test_missing_dependency_and_cycle_refuse(tmp_path):
    with pytest.raises(ContractDiscoveryError):
        resolve(beta(tmp_path))
    cyclic = alpha(tmp_path, requires=['beta-contracts==1.0.0'], dependencies=[('beta-contracts', '1.0.0')])
    with pytest.raises(ContractDiscoveryError, match='cycle'):
        resolve(cyclic, beta(tmp_path))


@pytest.mark.parametrize('ref', [
    'contract://alpha.data/1.0.0#/nope', 'contract://alpha.data/1.0.0#anchor',
    'contract://alpha.data/1.0.0#/$defs', 'contract://alpha.data/1.0.0#/type',
    'contract://alpha.report/1.0.0', 'contract://beta.run/1.0.0', 'contract://alpha.data/1.0.1',
    'contract://alpha.data/1', 'https://example.com/schema.json', 'file:///etc/passwd',
    'data.json', '../alpha-contracts/data.json', '#/missing', '#/allOf',
    'contract://alpha.data/1.0.0#/type/0', 'contract://alpha.data/1.0.0#/%24defs',
])
def test_unsupported_or_unresolvable_references_refuse(tmp_path, ref):
    with pytest.raises(ContractDiscoveryError):
        resolve(alpha(tmp_path), beta(tmp_path, schema_changes={'allOf': [{'$ref': ref}]}))


@pytest.mark.parametrize('field,ref', [
    ('input', 'contract://alpha.report/1.0.0'), ('report', 'contract://alpha.data/1.0.0'),
    ('output', 'contract://alpha.data/1.0.0#/$defs/a~1b~0c'), ('input', '#/x'),
    ('configuration', 'contract://beta.run/1.0.0'), ('input', 'contract://gamma.data/1.0.0'),
])
def test_capability_references_must_be_exact_and_of_the_right_kind(tmp_path, field, ref):
    with pytest.raises(ContractDiscoveryError):
        resolve(alpha(tmp_path), beta(tmp_path, capability_changes={field: ref}))


def test_undeclared_cross_distribution_reference_refuses(tmp_path):
    gamma = installed(tmp_path, 'gamma-contracts', definitions=[
        ('gamma.data', '1.0.0', 'schema', 'g.json', {'$ref': 'contract://alpha.data/1.0.0'})])
    with pytest.raises(ContractDiscoveryError, match='undeclared'):
        resolve(alpha(tmp_path), gamma)
    transitive = installed(tmp_path, 'gamma-contracts', requires=['beta-contracts==1.0.0'],
                           dependencies=[('beta-contracts', '1.0.0')], definitions=[
        ('gamma.data', '1.0.0', 'schema', 'g.json', {'$ref': 'contract://alpha.data/1.0.0'})])
    with pytest.raises(ContractDiscoveryError, match='undeclared'):
        resolve(alpha(tmp_path), beta(tmp_path), transitive)


@pytest.mark.parametrize('schema', [
    {'$ref': '#'}, {'allOf': [{'$ref': ''}]},
    {'$defs': {'x': {'$ref': '#/$defs/y'}, 'y': {'$ref': '#/$defs/x'}}, 'allOf': [{'$ref': '#/$defs/x'}]},
    {'enum': [True], 'allOf': [{'$ref': '#/enum/0'}]},           # boolean DATA is not a schema
    {'deprecated': False, 'allOf': [{'$ref': '#/deprecated'}]},
    {'minimum': 1, 'allOf': [{'$ref': '#/minimum/0'}]},          # pointer through a scalar
    {'$defs': {f'd{i}': {'$ref': f'#/$defs/d{i + 1}'} for i in range(3000)} | {'d3000': {}}, '$ref': '#/$defs/d0'},
])
def test_non_terminating_or_malformed_reference_structures_refuse(tmp_path, schema):
    with pytest.raises(ContractDiscoveryError):
        resolve(alpha(tmp_path, definitions=[('alpha.data', '1.0.0', 'schema', 'data.json', schema)]))


def test_boolean_subschemas_are_schema_positions(tmp_path):
    schema = {'additionalProperties': False, 'prefixItems': [True],
              'properties': {'a': {'$ref': '#/additionalProperties'}, 'b': {'$ref': '#/prefixItems/0'}}}
    assert resolve(alpha(tmp_path, definitions=[('alpha.data', '1.0.0', 'schema', 'data.json', schema)]))


def test_recursive_structure_is_not_a_cycle(tmp_path):
    tree = {'type': 'object', 'properties': {'children': {'type': 'array', 'items': {'$ref': '#'}}}}
    result = resolve(alpha(tmp_path, definitions=[('alpha.data', '1.0.0', 'schema', 'data.json', tree)]))
    assert result[0].definitions[0].references == ('contract://alpha.data/1.0.0',)


def test_digests_follow_bytes_not_meaning(tmp_path):
    before = resolve(alpha(tmp_path), beta(tmp_path))
    b = beta(tmp_path)
    (b.locate_file('cairn_contracts/beta-contracts/docs/identity.md')).write_bytes(b'# identity \n')
    (b.locate_file('cairn_contracts/beta-contracts/result.json')).write_bytes(
        b.locate_file('cairn_contracts/beta-contracts/result.json').read_bytes() + b'\n')
    after = resolve(alpha(tmp_path), b)
    changed = {d.id: d for d in after[1].definitions}
    unchanged = {d.id: d for d in before[1].definitions}
    assert changed['beta.result'].digest != unchanged['beta.result'].digest
    assert changed['beta.run'].digest == unchanged['beta.run'].digest
    assert changed['beta.run'].evidence != unchanged['beta.run'].evidence
    assert after[0] == before[0]
    assert after[1].digest == before[1].digest


@pytest.mark.parametrize('changes', [
    dict(files=[('provider.py', b'print(1)')]),
    dict(files=[('cairn_contracts/beta-contracts/helper.py', b'')]),
    dict(files=[('cairn_contracts/beta-contracts/lib.so', b'')]),
    dict(files=[('__editable__.beta.pth', b'/src')]),
    dict(files=[('beta_contracts/data.json', b'{}')]),
    dict(metadata_files=[('entry_points.txt', '[cairn.operators]\nx = provider:Pack\n')]),
    dict(metadata_files=[('direct_url.json', '{"url": "file:///src", "dir_info": {"editable": true}}')]),
    dict(metadata_files=[('direct_url.json', '[]')]), dict(metadata_files=[('direct_url.json', '{"dir_info": []}')]),
])
def test_distribution_that_is_not_data_only_refuses(tmp_path, changes):
    files = changes.pop('files', []) + [('cairn_contracts/beta-contracts/docs/identity.md', b'# identity\n')]
    with pytest.raises(ContractDiscoveryError):
        resolve(alpha(tmp_path), beta(tmp_path, files=files, **changes))


def test_declared_docs_and_installer_metadata_are_permitted(tmp_path):
    b = beta(tmp_path, files=[('cairn_contracts/beta-contracts/docs/identity.md', b'# identity\n'),
                              ('cairn_contracts/beta-contracts/README.md', b'docs')],
             metadata_files=[('INSTALLER', 'uv\n'), ('REQUESTED', ''),
                             ('direct_url.json', '{"url": "file:///w.whl", "archive_info": {}}')])
    assert resolve(alpha(tmp_path), b)[1].name == 'beta-contracts'


@pytest.mark.parametrize('evidence', ['docs/missing.md', '../identity.md'])
def test_evidence_must_be_an_inventoried_asset(tmp_path, evidence):
    changes = {'obligations': [{'name': 'identity', 'description': 'Keep it', 'evidence': evidence}]}
    with pytest.raises(ContractDiscoveryError):
        resolve(alpha(tmp_path), beta(tmp_path, capability_changes=changes))


def test_yaml_aliases_refused_even_when_acyclic(tmp_path):
    with pytest.raises(ContractDiscoveryError, match='alias'):
        read_body(tmp_path, 'const: [&a 1, *a]', suffix='.yaml')


# ── real wheels, real installer, isolated subprocess ──────────────────────────

def build_wheel(out_dir, name, version, files, requires=()):
    """A PEP 427 wheel written here, so the test owns every byte of the artifact."""
    dn = name.replace('-', '_')
    info = f'{dn}-{version}.dist-info'
    entries = dict(files)
    entries[f'{info}/METADATA'] = (f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n'
                                   + ''.join(f'Requires-Dist: {r}\n' for r in requires)).encode()
    entries[f'{info}/WHEEL'] = b'Wheel-Version: 1.0\nGenerator: test\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
    record = ''
    for path, data in entries.items():
        digest = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b'=').decode()
        record += f'{path},sha256={digest},{len(data)}\n'
    entries[f'{info}/RECORD'] = (record + f'{info}/RECORD,,\n').encode()
    wheel = Path(out_dir) / f'{dn}-{version}-py3-none-any.whl'
    with zipfile.ZipFile(wheel, 'w', zipfile.ZIP_DEFLATED) as z:
        for path, data in entries.items():
            z.writestr(path, data)
    return wheel


def descriptor(name, version, definitions, dependencies=()):
    text = f'format = "cairn.contracts/1"\n[distribution]\nname = "{name}"\nversion = "{version}"\n'
    for dep, ver in dependencies:
        text += f'[[dependencies]]\ndistribution = "{dep}"\nversion = "{ver}"\n'
    for identifier, revision, kind, path in definitions:
        text += f'[[definitions]]\nid = "{identifier}"\nrevision = "{revision}"\nkind = "{kind}"\npath = "{path}"\n'
    return text.encode()


def contract_wheels(out_dir):
    a = 'cairn_contracts/alpha-contracts/'
    b = 'cairn_contracts/beta-contracts/'
    alpha_schema = json.dumps({'$id': 'contract://alpha.data/1.0.0', 'type': 'object'}).encode()
    beta_schema = json.dumps({'$id': 'contract://beta.result/1.0.0', 'allOf': [{'$ref': 'contract://alpha.data/1.0.0'}]}).encode()
    for version in ('1.0.0', '1.0.1'):
        build_wheel(out_dir, 'alpha-contracts', version, {
            a + 'contracts.toml': descriptor('alpha-contracts', version, [('alpha.data', '1.0.0', 'schema', 'data.json')]),
            a + 'data.json': alpha_schema})
    build_wheel(out_dir, 'beta-contracts', '1.0.0', {
        b + 'contracts.toml': descriptor('beta-contracts', '1.0.0', [('beta.result', '1.0.0', 'schema', 'result.json')],
                                         [('alpha-contracts', '1.0.0')]),
        b + 'result.json': beta_schema}, requires=['alpha-contracts==1.0.0'])
    build_wheel(out_dir, 'evil-contracts', '1.0.0', {
        'cairn_contracts/evil-contracts/contracts.toml': descriptor('evil-contracts', '1.0.0', [('evil.data', '1.0.0', 'schema', 'data.json')]),
        'cairn_contracts/evil-contracts/data.json': b'{}',
        '../escape.txt': b'x', 'evil_provider.py': b'raise SystemExit("provider executed")'})


INSPECT = '''
import json, socket, sys, builtins, importlib.metadata
from importlib.metadata import distributions
sys.path.insert(0, sys.argv[1])  # the import trap below is reachable only if the target is importable
def forbidden(*a, **k): raise SystemExit("inspection reached a provider or the network")
socket.socket = forbidden
importlib.metadata.EntryPoint.load = forbidden
real_import = builtins.__import__
def guarded(name, *a, **k):
    if name.startswith("evil_provider"): forbidden()
    return real_import(name, *a, **k)
builtins.__import__ = guarded
from cairn.core.pack.contracts import ContractDiscoveryError, resolve_contracts
try:
    result = resolve_contracts(distributions=list(distributions(path=[sys.argv[1]])))
    print(json.dumps({"ok": [[d.name, d.version, [x.id for x in d.definitions], list(d.closure)] for d in result]}))
except ContractDiscoveryError as exc:
    print(json.dumps({"refused": str(exc)}))
'''


def uv_install(wheels, target, *args):
    cmd = ['uv', 'pip', 'install', '--offline', '--no-index', '--find-links', str(wheels),
           '--target', str(target), '--python', sys.executable, *args]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def inspect_installed(target):
    env = {**os.environ, 'PYTHONPATH': str(Path(contracts.__file__).parents[3])}
    run = subprocess.run([sys.executable, '-c', INSPECT, str(target)], capture_output=True, text=True, env=env)
    assert run.returncode == 0, run.stderr
    return json.loads(run.stdout)


@pytest.mark.parametrize('requested,expected', [
    (['beta-contracts==1.0.0'], {'ok': [['alpha-contracts', '1.0.0', ['alpha.data'], []],
                                        ['beta-contracts', '1.0.0', ['beta.result'], ['alpha-contracts']]]}),
    (['--no-deps', 'beta-contracts==1.0.0'], 'not installed at that version'),               # dependency missing
    (['--no-deps', 'beta-contracts==1.0.0', 'alpha-contracts==1.0.1'], 'not installed at that version'),
    (['evil-contracts==1.0.0'], 'not data-only'),                                            # executable payload
    ([], {'ok': []}),
])
def test_real_wheels_installed_by_the_package_manager(tmp_path, requested, expected):
    """Real artifacts through the real installer, inspected in a fresh interpreter whose
    only installed contract distributions are the ones in the target directory."""
    if shutil.which('uv') is None:
        if os.environ.get('CI'):
            pytest.fail('uv is required for the installed-wheel proof in CI')
        pytest.skip('uv is not on PATH; the installed-wheel proof needs the package manager')
    wheels, target = tmp_path / 'wheels', tmp_path / 'site'
    wheels.mkdir()
    target.mkdir()
    contract_wheels(wheels)
    if requested:
        uv_install(wheels, target, *requested)
    result = inspect_installed(target)
    if isinstance(expected, str):
        assert expected in result.get('refused', ''), result
    else:
        assert result == expected
    assert not (target / 'escape.txt').exists() and not (tmp_path / 'escape.txt').exists()


# ── C: the resolution snapshot facade (names, versions, digests; never paths) ──

def test_resolution_snapshot_is_names_versions_and_digests_only(tmp_path, monkeypatch):
    a, b = alpha(tmp_path), beta(tmp_path)
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [b, a])
    snap = pack_registry.contract_resolution_snapshot()
    assert snap['format'] == 'cairn.resolution/1'
    assert [d['name'] for d in snap['distributions']] == ['alpha-contracts', 'beta-contracts']
    assert [(d['distribution'], d['id']) for d in snap['definitions']] == [
        ('alpha-contracts', 'alpha.data'), ('alpha-contracts', 'alpha.report'),
        ('beta-contracts', 'beta.result'), ('beta-contracts', 'beta.run')]
    data = a.locate_file('cairn_contracts/alpha-contracts/data.json').read_bytes()
    assert snap['definitions'][0]['digest'] == hashlib.sha256(data).hexdigest()
    assert str(tmp_path) not in json.dumps(snap) and 'path' not in json.dumps(snap)
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [b])
    with pytest.raises(ContractDiscoveryError):           # one bad distribution: no partial snapshot
        pack_registry.contract_resolution_snapshot()
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [])
    assert pack_registry.contract_resolution_snapshot() == {
        'format': 'cairn.resolution/1', 'distributions': [], 'definitions': []}


# ── #2241 B: implements grammar + the two invocation facades ─────────────────

@pytest.mark.parametrize('refs,ok', [
    (None, True), ([], True), (['contract://a.b/1.0.0', 'contract://a.b/1.1.0'], True),
    (['contract://a.b/1.0.0', 'contract://a.c/1.0.0'], False), (['contract://a.b/1'], False), (['contract://a/1.0.0'], False),
    ('contract://a.b/1.0.0', False), (['contract://a.b/1.0.0'] * 2, False), ([1], False)])
def test_validate_implements_is_the_pack_owners_grammar(refs, ok):
    if ok:
        assert contracts.validate_implements(refs) == tuple(refs or ())
    else:
        with pytest.raises(ValueError):
            contracts.validate_implements(refs)


def test_invocation_definitions_and_contract_facades(tmp_path, monkeypatch):
    a, b = alpha(tmp_path), beta(tmp_path)
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [b, a])
    data = pack_registry.invocation_definitions('contract://beta.run/1.0.0')
    assert [(d['kind'], d['id']) for d in data['definitions']] == [
        ('capability', 'beta.run'), ('schema', 'alpha.data'), ('schema', 'beta.result'), ('schema', 'beta.result'), ('report', 'alpha.report')]
    assert data['report_spec'] == {'series': [{'name': 'loss', 'unit': ''}]}
    assert all(len(d['digest']) == 64 and d['distribution'] for d in data['definitions'])
    recorded = [(f"contract://{d['id']}/{d['revision']}", d['digest']) for d in data['definitions']]
    closure = [(f"contract://{x.id}/{x.revision}", x.digest) for dist in resolve(b, a) for x in dist.definitions]
    env = pack_registry.invocation_contract(recorded, closure)
    assert env['capability_ref'] == 'contract://beta.run/1.0.0'
    assert env['refs'] == {'input': 'contract://alpha.data/1.0.0', 'configuration': 'contract://beta.result/1.0.0',
                           'output': 'contract://beta.result/1.0.0'}
    assert set(env['schemas']) == {'contract://alpha.data/1.0.0', 'contract://beta.result/1.0.0'}   # schemas only, no paths
    assert str(tmp_path) not in json.dumps(env)
    for bad in ('contract://alpha.data/1.0.0', 'contract://nope.x/1.0.0'):            # not a capability / unknown
        with pytest.raises(ContractDiscoveryError):
            pack_registry.invocation_definitions(bad)
    drifted = [(uri, 'f' * 64) for uri, _ in recorded]                                # installation changed since admission
    with pytest.raises(ContractDiscoveryError, match='differs from the recorded'):
        pack_registry.invocation_contract(drifted, closure)
    transitive = [(uri, 'f' * 64 if uri == 'contract://alpha.data/1.0.0' else d) for uri, d in closure]
    with pytest.raises(ContractDiscoveryError, match='differs from the recorded'):      # a $ref target drifted
        pack_registry.invocation_contract([r for r in recorded if 'alpha.data' not in r[0]], transitive)


# ── ADR-0137 D3: the engine distribution may host several contract packages ───

ENGINE_SCRIPTS = '[console_scripts]\ncairn = cairn.cli:main\n[cairn.operators]\necho = operator_echo:Pack\n'
EDITABLE = '{"url": "file:///src/cairn", "dir_info": {"editable": true}}'


def host(tmp_path, packages, *, name='cairndev', version='0.1.0', files=(), metadata_files=(), requires=(),
         unlisted=()):
    """An installed-layout engine distribution: its own code (``cairn/__init__.py``) plus
    ``cairn_contracts/<package>/`` for each ``(package, version, definitions, dependencies)``,
    definitions as in :func:`installed`. ``files`` are extra (path, bytes) under the root;
    ``unlisted`` paths are written but left out of ``RECORD``."""
    root = tmp_path / f'{name}-{version}'
    info = root / f"{name.replace('-', '_')}-{version}.dist-info"
    info.mkdir(parents=True, exist_ok=True)
    requires_text = ''.join(f'Requires-Dist: {r}\n' for r in requires)
    (info / 'METADATA').write_text(f'Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n{requires_text}')
    for filename, text in metadata_files:
        (info / filename).write_text(text)
    written = [('cairn/__init__.py', b'')]
    for package, package_version, definitions, dependencies in packages:
        for _, _, _, path, body in definitions:
            written.append((f'cairn_contracts/{package}/{path}', body if isinstance(body, bytes) else json.dumps(body).encode()))
        written.append((f'cairn_contracts/{package}/contracts.toml',
                        descriptor(package, package_version, [d[:4] for d in definitions], dependencies)))
    for path, data in [*written, *files]:
        (root / path).parent.mkdir(parents=True, exist_ok=True)
        (root / path).write_bytes(data)
    inventory = [path for path, _ in [*written, *files] if path not in unlisted]
    (info / 'RECORD').write_text(''.join(f'{p},,\n' for p in inventory) + f'{info.name}/METADATA,,\n')
    return PathDistribution(info)


def hosted_alpha(version='1.0.0'):
    return ('alpha-contracts', version, alpha_definitions(), ())


def hosted_beta(version='1.0.0', alpha_version='1.0.0'):
    return ('beta-contracts', version, beta_definitions(), [('alpha-contracts', alpha_version)])


def engine(tmp_path, **changes):
    """``cairndev`` hosting alpha and beta exactly as the standalone fixtures ship them."""
    spec = dict(packages=[hosted_alpha(), hosted_beta()], files=[BETA_EVIDENCE])
    spec.update(changes)
    return host(tmp_path, spec.pop('packages'), **spec)


def test_one_engine_host_gives_one_record_per_package_at_the_descriptor_version(tmp_path):
    dist = engine(tmp_path, packages=[hosted_alpha('1.1.0'), hosted_beta('1.0.0', alpha_version='1.1.0')])
    assert [(d.name, d.version) for d in discover_contracts(distributions=[dist])] == [
        ('alpha-contracts', '1.1.0'), ('beta-contracts', '1.0.0')]
    result = resolve(dist)
    assert [(d.name, d.version, d.closure) for d in result] == [
        ('alpha-contracts', '1.1.0', ()), ('beta-contracts', '1.0.0', ('alpha-contracts',))]
    assert {x.version for x in result[0].definitions} == {'1.1.0'}       # never the host's 0.1.0


@pytest.mark.parametrize('changes', [
    dict(metadata_files=[('direct_url.json', EDITABLE)]),                                     # editable, inventoried copies
    dict(metadata_files=[('entry_points.txt', ENGINE_SCRIPTS)], requires=['requests>=2', 'pyyaml; extra == "x"']),
])
def test_an_editable_engine_host_or_one_with_entry_points_and_loose_requirements_resolves(tmp_path, changes):
    assert [d.name for d in resolve(engine(tmp_path, **changes))] == ['alpha-contracts', 'beta-contracts']


def test_a_hosted_package_depends_on_a_hosted_package_at_its_exact_version(tmp_path):
    result = resolve(engine(tmp_path))
    run = {d.id: d for d in result[1].definitions}['beta.run']
    assert result[1].dependencies == (('alpha-contracts', '1.0.0'),) and result[1].closure == ('alpha-contracts',)
    assert 'contract://alpha.data/1.0.0' in run.references


def test_recorded_identity_is_the_same_standalone_and_hosted(tmp_path, monkeypatch):
    a, b = alpha(tmp_path / 'standalone'), beta(tmp_path / 'standalone')
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [b, a])
    standalone = pack_registry.contract_resolution_snapshot()
    data = pack_registry.invocation_definitions('contract://beta.run/1.0.0')
    recorded = [(f"contract://{d['id']}/{d['revision']}", d['digest']) for d in data['definitions']]
    dist = engine(tmp_path / 'hosted')
    monkeypatch.setattr(contracts, 'installed_distributions', lambda: [dist])
    assert pack_registry.contract_resolution_snapshot() == standalone
    closure = [(f"contract://{x.id}/{x.revision}", x.digest) for d in resolve(dist) for x in d.definitions]
    assert pack_registry.invocation_contract(recorded, closure)['capability_ref'] == 'contract://beta.run/1.0.0'


def test_a_second_engine_named_distribution_never_hosts(tmp_path):
    """Owner decision 2a: one engine is the only host, so a shadowed second ``cairndev`` dist-info
    (another sys.path entry) is refused rather than trusted to host packages of its own."""
    with pytest.raises(ContractDiscoveryError, match='more than one cairndev distribution'):
        resolve(host(tmp_path, [hosted_alpha()]), host(tmp_path, [hosted_alpha()], version='0.2.0'))


def test_a_second_engine_hosting_other_packages_is_refused_too(tmp_path):
    """PR1 review N1: the refusal is the engine count, not the duplicate-package rule — two
    ``cairndev`` dist-infos hosting DISJOINT packages are refused as well."""
    first = host(tmp_path, [hosted_alpha()])
    second = host(tmp_path, [hosted_beta()], version='0.2.0')
    with pytest.raises(ContractDiscoveryError, match='more than one cairndev distribution'):
        resolve(first, second)


@pytest.mark.parametrize('stale_first', [True, False])
def test_a_stale_engine_without_contracts_still_counts(tmp_path, stale_first):
    """PR1 review N1b: a second ``cairndev`` dist-info that carries no ``cairn_contracts/`` (an
    older engine on another sys.path entry) is counted before its inventory is read, so it
    cannot leave which engine hosts the packages ambiguous — in either order."""
    hosting = engine(tmp_path)
    stale = host(tmp_path, [], version='0.0.9')
    order = (stale, hosting) if stale_first else (hosting, stale)
    with pytest.raises(ContractDiscoveryError, match='more than one cairndev distribution'):
        resolve(*order)


def test_a_distribution_without_a_name_and_without_contracts_is_skipped(tmp_path):
    """Counting engines reads every distribution's name; one with broken metadata that carries
    no contracts must stay skipped, as before."""
    info = tmp_path / 'broken-1.0.dist-info'
    info.mkdir()
    (info / 'METADATA').write_text('Metadata-Version: 2.1\nVersion: 1.0\n')
    (info / 'RECORD').write_text('')
    assert [d.name for d in resolve(PathDistribution(info), engine(tmp_path))] == [
        'alpha-contracts', 'beta-contracts']


def test_a_non_engine_distribution_with_two_descriptors_refuses(tmp_path):
    with pytest.raises(ContractDiscoveryError, match='exactly one descriptor'):
        resolve(engine(tmp_path, name='other-engine'))


def _rename_descriptor(dist, package, new):
    manifest = Path(dist.locate_file(f'cairn_contracts/{package}/contracts.toml'))
    manifest.write_text(manifest.read_text().replace(f'name = "{package}"', f'name = "{new}"'))
    return dist


def _escape(dist, tmp_path):
    data = Path(dist.locate_file('cairn_contracts/alpha-contracts/data.json'))
    outside = tmp_path / 'outside.json'
    outside.write_bytes(data.read_bytes())
    data.unlink()
    data.symlink_to(outside)
    return dist


@pytest.mark.parametrize('build,reason', [
    (lambda t: _rename_descriptor(engine(t), 'alpha-contracts', 'gamma-contracts'), 'name mismatch'),
    (lambda t: engine(t, files=[BETA_EVIDENCE, ('cairn_contracts/alpha-contracts/helper.py', b'')]),
     'hosted contract package is not data-only'),
    (lambda t: engine(t, files=[BETA_EVIDENCE, ('cairn_contracts/alpha-contracts/hook.pth', b'/src')]),
     'hosted contract package is not data-only'),
    (lambda t: engine(t, unlisted=['cairn_contracts/alpha-contracts/contracts.toml']), 'sits in no contract package'),
    (lambda t: engine(t, unlisted=['cairn_contracts/beta-contracts/run.json']), 'absent from distribution inventory'),
    (lambda t: _escape(engine(t), t), 'escapes'),
    (lambda t: engine(t, files=[BETA_EVIDENCE, ('cairn_contracts/x.json', b'{}')]), 'sits in no contract package'),
    (lambda t: engine(t, packages=[hosted_alpha(), ('gamma-contracts', '1.0.0', alpha_definitions()[:1], ())], files=[]),
     'conflicting contract identity owners'),
    (lambda t: engine(t, packages=[hosted_beta()]), 'not installed at that version'),                       # missing
    (lambda t: engine(t, packages=[hosted_alpha('1.0.1'), hosted_beta()]), 'not installed at that version'),
    (lambda t: engine(t, packages=[(*hosted_alpha()[:3], [('beta-contracts', '1.0.0')]), hosted_beta()]), 'cycle'),
])
def test_a_hosted_package_that_breaks_a_kept_rule_refuses(tmp_path, build, reason):
    with pytest.raises(ContractDiscoveryError, match=reason):
        resolve(build(tmp_path))


def test_a_package_installed_standalone_and_hosted_refuses_naming_both_hosts(tmp_path):
    with pytest.raises(ContractDiscoveryError) as refused:
        resolve(alpha(tmp_path), host(tmp_path, [hosted_alpha()]))
    message = str(refused.value)
    assert 'contract package alpha-contracts is installed twice, by alpha-contracts and by cairndev' in message
    assert 'uninstall the standalone one' in message
