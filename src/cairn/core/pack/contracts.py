"""Inspect installed contract descriptors without loading pack entry points.

``discover_contracts`` is descriptor inspection only. ``resolve_contracts`` (#2240 B2) adds
the offline closure: data-only inventory, exact dependency pins, reference resolution,
evidence inventory and SHA-256 digests. Neither executes a provider or touches the network.
"""
from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from importlib.metadata import Distribution, distributions as installed_distributions
from pathlib import Path, PurePosixPath
import re
import json
import math
import tomllib
from typing import Iterable

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name
import yaml
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError
from referencing import Registry, Resource
from referencing.exceptions import Unresolvable
from referencing.jsonschema import DRAFT202012

from cairn.core.reporting import ReportSpec

_ID = re.compile(r"[a-z][a-z0-9_]*(?:\.[a-z][a-z0-9_]*)+")
_REVISION = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)")
_REFERENCE = re.compile(rf'contract://({_ID.pattern})/({_REVISION.pattern})')


class ContractDiscoveryError(ValueError):
    """A declared contract distribution has an invalid descriptor or inventory."""


@dataclass(frozen=True)
class ContractDefinition:
    id: str
    revision: str
    kind: str
    path: Path


@dataclass(frozen=True)
class ContractDistribution:
    name: str
    version: str
    descriptor: Path
    definitions: tuple[ContractDefinition, ...]
    dependencies: tuple[tuple[str, str], ...]


def _table(value: object, required: set[str], optional: set[str] | None = None) -> dict:
    if not isinstance(value, dict) or not required <= value.keys():
        raise ContractDiscoveryError("missing or malformed descriptor fields")
    if value.keys() - required - (optional or set()):
        raise ContractDiscoveryError("unknown descriptor fields")
    return value


def _string(value: object) -> str:
    if not isinstance(value, str) or not value:
        raise ContractDiscoveryError("descriptor values must be nonempty strings")
    return value


def _revision(value: object) -> str:
    value = _string(value)
    if not _REVISION.fullmatch(value):
        raise ContractDiscoveryError("contract revision must be exact major.minor.patch")
    return value


def _relative(value: str) -> PurePosixPath:
    if "\\" in value or any(part in ("", ".", "..") for part in value.split("/")):
        raise ContractDiscoveryError("unsafe contract asset path")
    path = PurePosixPath(value)
    if path.is_absolute():
        raise ContractDiscoveryError("absolute contract asset path")
    return path


def _asset(dist: Distribution, inventory: set[str], root: Path, relative: str) -> Path:
    path = _relative(relative)
    if relative not in inventory:
        raise ContractDiscoveryError("contract asset is absent from distribution inventory")
    located = Path(dist.locate_file(str(path)))
    resolved = located.resolve()
    if not resolved.is_relative_to(root) or not resolved.is_file():
        raise ContractDiscoveryError("contract asset is missing or escapes its distribution root")
    return resolved


def _inspect(dist: Distribution, inventory: set[str], descriptor: str, hosted: bool) -> ContractDistribution:
    """One contract package. Standalone: the package is the distribution (its name and
    installed version). Hosted by the engine (ADR-0137 D3): the package is its directory,
    named by its own descriptor, at the descriptor's version, whatever the host's version."""
    if hosted:
        name = PurePosixPath(descriptor).parts[1]
        if name != canonicalize_name(name, validate=True):
            raise ContractDiscoveryError("hosted contract directory is not a canonical package name")
    else:
        name = canonicalize_name(_string(dist.metadata.get("Name")), validate=True)
    expected = f"cairn_contracts/{name}/contracts.toml"
    if descriptor != expected:
        raise ContractDiscoveryError("contract layout does not match distribution identity")
    # Check containment against the installed distribution root before following asset links.
    install_root = Path(dist.locate_file("")).resolve()
    asset_root = Path(dist.locate_file(f"cairn_contracts/{name}")).resolve()
    if not asset_root.is_relative_to(install_root):
        raise ContractDiscoveryError("contract directory escapes installed distribution")
    manifest = _asset(dist, inventory, asset_root, expected)
    raw = _table(tomllib.loads(manifest.read_text(encoding="utf-8")),
                 {"format", "distribution", "definitions"}, {"dependencies"})
    if raw["format"] != "cairn.contracts/1":
        raise ContractDiscoveryError("unsupported contract descriptor format")
    identity = _table(raw["distribution"], {"name", "version"})
    if canonicalize_name(_string(identity["name"]), validate=True) != name:
        raise ContractDiscoveryError("descriptor distribution name mismatch")
    version = _string(identity["version"])
    if not hosted and version != dist.version:
        raise ContractDiscoveryError("descriptor distribution version mismatch")
    definitions = _definitions(dist, inventory, asset_root, name, raw["definitions"])
    dependencies = _dependencies(raw.get("dependencies", []))
    return ContractDistribution(name, version, manifest, definitions, dependencies)


def _definitions(dist: Distribution, inventory: set[str], asset_root: Path,
                 name: str, definitions: object) -> tuple[ContractDefinition, ...]:
    if not isinstance(definitions, list) or not definitions:
        raise ContractDiscoveryError("definitions must be a nonempty array")
    found = {}
    for item in definitions:
        item = _table(item, {"id", "revision", "kind", "path"})
        identifier = _string(item["id"])
        if not _ID.fullmatch(identifier):
            raise ContractDiscoveryError("invalid namespaced contract identity")
        revision = _revision(item["revision"])
        kind = _string(item["kind"])
        if kind not in {"capability", "stage", "schema", "report"}:
            raise ContractDiscoveryError("unsupported definition kind")
        relative = _relative(_string(item["path"]))
        path = _asset(dist, inventory, asset_root, f"cairn_contracts/{name}/{relative}")
        key = (identifier, revision)
        if key in found:
            raise ContractDiscoveryError("duplicate contract identity/revision")
        found[key] = ContractDefinition(identifier, revision, kind, path)
    return tuple(found[k] for k in sorted(found))


def _dependencies(dependencies: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(dependencies, list):
        raise ContractDiscoveryError("dependencies must be an array")
    required = {}
    for item in dependencies:
        item = _table(item, {"distribution", "version"})
        dependency = canonicalize_name(_string(item["distribution"]), validate=True)
        if dependency in required:
            raise ContractDiscoveryError("duplicate dependency declaration")
        required[dependency] = _revision(item["version"])
    return tuple(sorted(required.items()))


def _descriptors(inventory: set[str]) -> list[str]:
    """Every ``cairn_contracts/<package>/contracts.toml`` the inventory lists. A descriptor at
    any other depth, or a file under ``cairn_contracts/`` in no package directory, refuses."""
    declared = sorted(p for p in inventory if p.startswith("cairn_contracts/"))
    descriptors = [p for p in declared if PurePosixPath(p).name == "contracts.toml"]
    if any(len(PurePosixPath(p).parts) != 3 for p in descriptors):
        raise ContractDiscoveryError("a contract descriptor must sit at cairn_contracts/<package>/contracts.toml")
    packages = {PurePosixPath(p).parts[1] for p in descriptors}
    if any(len(PurePosixPath(p).parts) < 3 or PurePosixPath(p).parts[1] not in packages for p in declared):
        raise ContractDiscoveryError("a file under cairn_contracts/ sits in no contract package directory")
    return descriptors


def _discover(distributions: Iterable[Distribution] | None
              ) -> list[tuple[Distribution, set[str], ContractDistribution, bool]]:
    """``(dist, inventory, package, hosted)`` by package name. Only the engine distribution
    hosts packages it is not named after, any number of them (ADR-0137 D3); every other
    contract distribution is standalone: one descriptor, named after it (ADR-0111)."""
    from cairn.core.pack.loader import ENGINE_DISTRIBUTION  # noqa: PLC0415 — keep contracts off the loader's import path
    results = {}
    owners = {}
    engine_hosts = 0
    for dist in installed_distributions() if distributions is None else distributions:
        try:
            raw_name = dist.metadata.get("Name")
            hosted = isinstance(raw_name, str) and canonicalize_name(raw_name) == ENGINE_DISTRIBUTION
            if hosted:
                # One engine is the only host (owner decision 2a): a second dist-info named after it
                # (a shadowed install on another sys.path entry) never hosts packages of its own.
                # Counted before its inventory is read: a stale engine that carries no contracts
                # still makes "which engine hosts them" ambiguous (PR1 review N1b).
                engine_hosts += 1
                if engine_hosts > 1:
                    raise ContractDiscoveryError(
                        f"more than one {ENGINE_DISTRIBUTION} distribution is installed: remove the stale one")
            inventory = {str(path) for path in (dist.files or ())}
            descriptors = _descriptors(inventory)
            if not descriptors:
                continue
            host = _string(raw_name)
            if not hosted and len(descriptors) != 1:
                raise ContractDiscoveryError("contract distribution must inventory exactly one descriptor")
            for descriptor in descriptors:
                item = _inspect(dist, inventory, descriptor, hosted)
                if item.name in results:
                    other = results[item.name][0].metadata.get("Name")
                    raise ContractDiscoveryError(
                        f"contract package {item.name} is installed twice, by {other} and by {host}: "
                        "uninstall the standalone one (ADR-0137)")
                for definition in item.definitions:
                    if definition.id in owners and owners[definition.id] != item.name:
                        raise ContractDiscoveryError("conflicting contract identity owners")
                    owners[definition.id] = item.name
                results[item.name] = (dist, inventory, item, hosted)
        except (OSError, ValueError, RuntimeError) as exc:
            if isinstance(exc, ContractDiscoveryError):
                raise
            raise ContractDiscoveryError("cannot inspect contract distribution metadata or assets") from exc
    return [results[name] for name in sorted(results)]


def discover_contracts(*, distributions: Iterable[Distribution] | None = None) -> tuple[ContractDistribution, ...]:
    """Read descriptors from installed metadata; never load entry points or definition bodies.

    Missing dependency packages and schema/reference closure are intentionally not checked
    here (``resolve_contracts`` is). The returned inventory must not be treated as executable
    or conformant.
    """
    return tuple(item for _, _, item, _ in _discover(distributions))


_SCHEMA_MAPS = {'properties', '$defs'}
_SCHEMA_LISTS = {'allOf', 'anyOf', 'oneOf', 'prefixItems'}
_SCHEMA_SINGLE = {'additionalProperties', 'items', 'not', 'if', 'then', 'else'}
_SCHEMA_KEYWORDS = _SCHEMA_MAPS | _SCHEMA_LISTS | _SCHEMA_SINGLE | {
    '$schema', '$id', '$ref', 'type', 'enum', 'const', 'required',
    'minProperties', 'maxProperties', 'minItems', 'maxItems', 'uniqueItems',
    'minimum', 'maximum', 'exclusiveMinimum', 'exclusiveMaximum', 'multipleOf',
    'minLength', 'maxLength', 'title', 'description', 'default', 'examples',
    'deprecated', 'readOnly', 'writeOnly', '$comment',
}


def _unique_pairs(pairs: Iterable[tuple[object, object]]) -> dict:
    result = {}
    for key, value in pairs:
        if not isinstance(key, str) or key in result:
            raise ContractDiscoveryError('duplicate or non-string document key')
        result[key] = value
    return result


class _ContractYamlLoader(yaml.SafeLoader):
    """Local constructors; never change the legacy/global YAML loader."""

    def compose_node(self, parent: yaml.Node | None, index: object) -> yaml.Node:
        # Aliases are not JSON data, and acyclic ones amplify (a few lines of
        # nested anchors expand to ~1e9 node visits); refuse at compose time.
        if self.check_event(yaml.AliasEvent):
            raise ContractDiscoveryError('YAML aliases are not contract data')
        return super().compose_node(parent, index)

    def construct_mapping(self, node: yaml.MappingNode, deep: bool = False) -> dict:
        if not isinstance(node, yaml.MappingNode):
            raise ContractDiscoveryError('YAML mapping tag on a non-mapping value')
        return _unique_pairs((self.construct_object(k, deep=deep),
                              self.construct_object(v, deep=deep)) for k, v in node.value)


def _json_data(value: object, ancestors: frozenset[int] = frozenset()) -> None:
    if isinstance(value, (dict, list)):
        if id(value) in ancestors:
            raise ContractDiscoveryError('cyclic document data')
        children = value.values() if isinstance(value, dict) else value
        for child in children:
            _json_data(child, ancestors | {id(value)})
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ContractDiscoveryError('non-finite document number')
    elif value is not None and not isinstance(value, (str, int, bool)):
        raise ContractDiscoveryError('document contains non-JSON data')


def _subschemas(schema: dict, key: str, value: object) -> list[tuple[object, str, object]]:
    """``(container, token, child)`` for the subschemas one keyword holds."""
    if key in _SCHEMA_MAPS:
        return [(value, token, child) for token, child in value.items()]
    if key in _SCHEMA_LISTS:
        return [(value, str(index), child) for index, child in enumerate(value)]
    if key in _SCHEMA_SINGLE:
        return [(schema, key, value)]
    return []


def _schema_positions(schema: object, slots: set[tuple[int, str]] | None = None) -> Iterable[dict]:
    """Every subschema mapping, depth first. ``slots`` collects ``(id(container), token)`` of
    each subschema slot so a boolean subschema (not identity-trackable) can be recognised."""
    if isinstance(schema, bool):
        return
    if not isinstance(schema, dict) or schema.keys() - _SCHEMA_KEYWORDS:
        raise ContractDiscoveryError('unsupported schema keyword or shape')
    yield schema
    for key, value in schema.items():
        for container, token, child in _subschemas(schema, key, value):
            if slots is not None:
                slots.add((id(container), token))
            yield from _schema_positions(child, slots)


def _validate_schema(value: object, uri: str) -> None:
    Draft202012Validator.check_schema(value)
    for position in _schema_positions(value):
        if '$schema' in position and position['$schema'] != Draft202012Validator.META_SCHEMA['$id']:
            raise ContractDiscoveryError('unsupported schema dialect')
        if '$id' in position and position['$id'] != uri:
            raise ContractDiscoveryError('schema identity differs from descriptor')


def _validate_capability(value: dict) -> None:
    _table(value, {'format', 'id', 'revision', 'summary', 'input', 'configuration', 'output'},
           {'report', 'obligations'})
    for field in ('summary', 'input', 'configuration', 'output'):
        _string(value[field])
    if 'report' in value:
        _string(value['report'])
    obligations = value.get('obligations', [])
    if not isinstance(obligations, list):
        raise ContractDiscoveryError('obligations must be an array')
    for obligation in obligations:
        _table(obligation, {'name', 'description', 'evidence'})
        for field in obligation:
            _string(obligation[field])
        _relative(obligation['evidence'])


def _validate_stage(value: dict) -> None:
    """ADR-0118 M1a — ``cairn.stage/1``: a capability without ``input`` (rows are not
    contracted). ``report``/``obligations`` stay optional, read like a capability's."""
    _table(value, {'format', 'id', 'revision', 'summary', 'configuration', 'output'}, {'report', 'obligations'})
    _validate_capability({**value, 'input': value['configuration']})


#: ADR-0118 D2 — the stat keys the RUNNER adds to a stage's raw stats (sysmetrics
#: ``sysmetrics.py`` Sampler.stop, capture ``capture.py`` capture_stage, the map merge and
#: ``kernel_version`` in ``cairn/core/dataflow/runner.py``). A stage contract may not
#: declare them, so one open output schema answers the same on raw and merged stats.
RESERVED_STAGE_STATS = frozenset(
    {'kernel_version', 'items_total', 'items_done', 'sysmetrics_samples',
     'artifacts_count', 'artifacts_bytes', 'artifacts_index', 'artifacts_skipped'}
    | {f'{k}_{agg}' for k in ('cpu_pct', 'rss_mb', 'gpu_pct', 'vram_mb') for agg in ('mean', 'max')})


def _strings(value: object) -> list[str]:
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise ContractDiscoveryError('report names must be an array of strings')
    return value


def _report_spec(value: object) -> ReportSpec:
    spec = _table(value, set(), {'stages', 'series', 'scalars'})
    for field in ('stages', 'scalars'):
        _strings(spec.get(field, []))
    series = spec.get('series', [])
    if not isinstance(series, list):
        raise ContractDiscoveryError('report series must be an array')
    for item in series:
        _table(item, {'name', 'unit'})
        _string(item['name'])
        if not isinstance(item['unit'], str):
            raise ContractDiscoveryError('report unit must be a string')
    return ReportSpec.from_manifest(spec)


def _validate_report(value: dict) -> None:
    _table(value, {'format', 'id', 'revision', 'spec'}, {'semantics'})
    spec = _report_spec(value['spec'])
    semantics = value.get('semantics', {})
    if not isinstance(semantics, dict):
        raise ContractDiscoveryError('report semantics must be a mapping')
    for name, annotation in semantics.items():
        if name not in spec.series_names | set(spec.scalars):
            raise ContractDiscoveryError('semantics names an undeclared metric')
        _table(annotation, {'description', 'unit', 'applicability', 'comparison_context'})
        if any(not isinstance(text, str) for text in annotation.values()):
            raise ContractDiscoveryError('report semantic annotations must be strings')


def _read_definition(definition: ContractDefinition) -> tuple[bytes, object]:
    """Read/validate one body, preserving bytes. Internal; does NOT validate closure.

    Caller owns inventory/containment checks. B2 must validate all references and evidence
    before exposing a resolved definition. Returned parsed data is local to this call.
    """
    try:
        contents = definition.path.read_bytes()
        if definition.path.suffix == '.json':
            value = json.loads(contents, object_pairs_hook=_unique_pairs)
        elif definition.path.suffix in {'.yaml', '.yml'}:
            try:
                value = yaml.load(contents, Loader=_ContractYamlLoader)
            except (TypeError, KeyError, IndexError, AttributeError) as exc:
                # PyYAML's own constructors raise these for explicitly tagged
                # values they cannot build (`!!bool maybe`, `!!int ''`).
                raise ContractDiscoveryError('malformed tagged YAML value') from exc
        else:
            raise ContractDiscoveryError('unsupported definition file extension')
        _json_data(value)
        uri = f'contract://{definition.id}/{definition.revision}'
        if definition.kind == 'schema':
            _validate_schema(value, uri)
        else:
            _definition_record(definition, value)
        return contents, value
    except (OSError, ValueError, yaml.YAMLError, SchemaError, RecursionError) as exc:
        if isinstance(exc, ContractDiscoveryError):
            raise
        raise ContractDiscoveryError(f'invalid {definition.kind} body: {definition.id}') from exc


def _definition_record(definition: ContractDefinition, value: object) -> None:
    if not isinstance(value, dict):
        raise ContractDiscoveryError('definition body must be a mapping')
    if (value.get('id'), value.get('revision')) != (definition.id, definition.revision):
        raise ContractDiscoveryError('definition identity differs from descriptor')
    if value.get('format') != f'cairn.{definition.kind}/1':
        raise ContractDiscoveryError('unsupported definition format')
    readers = {'capability': _validate_capability, 'stage': _validate_stage, 'report': _validate_report}
    if definition.kind not in readers:
        raise ContractDiscoveryError('unsupported definition kind')
    readers[definition.kind](value)



def validate_implements(refs: object) -> tuple[str, ...]:
    """#2241 B — an operator's ``implements`` declaration, closed: a nonempty list of
    exact ``contract://<id>/<revision>`` references naming ONE capability ID (ADR-0111
    D3). ``None``/empty means legacy and returns ``()``. Shared by the manifest parser
    and ``register_operator`` so both owners refuse the same strings."""
    if not refs:
        return ()
    if not isinstance(refs, (list, tuple)) or any(not isinstance(r, str) for r in refs):
        raise ValueError("implements must be a list of contract:// references")
    matches = [_REFERENCE.fullmatch(r) for r in refs]
    if any(m is None for m in matches):
        raise ValueError(f"implements entries must be exact contract://<id>/<major.minor.patch>: {list(refs)!r}")
    if len({m.group(1) for m in matches}) != 1 or len(set(refs)) != len(refs):
        raise ValueError(f"implements must name one capability ID with distinct revisions: {list(refs)!r}")
    return tuple(refs)


# ── B2: offline closure, data-only inventory and digests ──────────────────────

_IN_PLACE = {'allOf', 'anyOf', 'oneOf', 'not', 'if', 'then', 'else'}
_EXECUTABLE = {'.py', '.pyc', '.pyo', '.pyw', '.so', '.pyd', '.dll', '.dylib', '.exe',
               '.sh', '.bat', '.cmd', '.ps1', '.pth'}


@dataclass(frozen=True)
class ResolvedDefinition:
    """One closed definition. ``path`` is a local lookup detail, not identity."""
    id: str
    revision: str
    kind: str
    distribution: str
    version: str
    digest: str
    references: tuple[str, ...]
    evidence: tuple[tuple[str, str], ...]
    path: Path


@dataclass(frozen=True)
class ResolvedDistribution:
    name: str
    version: str
    descriptor: Path
    digest: str
    dependencies: tuple[tuple[str, str], ...]
    closure: tuple[str, ...]
    definitions: tuple[ResolvedDefinition, ...]


@dataclass(frozen=True)
class _Body:
    definition: ContractDefinition
    distribution: str
    contents: bytes
    value: object


def _digest(contents: bytes) -> str:
    return sha256(contents).hexdigest()


def _data_only(dist: Distribution, inventory: set[str], name: str) -> None:
    if list(dist.entry_points):
        raise ContractDiscoveryError('contract distribution declares entry points')
    direct = json.loads(dist.read_text('direct_url.json') or '{}')
    if not isinstance(direct, dict) or not isinstance(direct.get('dir_info', {}), dict):
        raise ContractDiscoveryError('malformed direct_url.json')
    if direct.get('dir_info', {}).get('editable'):
        raise ContractDiscoveryError('editable contract installation')
    for path in inventory:
        parts = PurePosixPath(path)
        metadata = len(parts.parts) == 2 and parts.parts[0].endswith('.dist-info')
        if parts.suffix.lower() in _EXECUTABLE or not (metadata or path.startswith(f'cairn_contracts/{name}/')):
            raise ContractDiscoveryError('contract distribution is not data-only')


def _hosted_data_only(inventory: set[str], name: str) -> None:
    """A package the engine hosts is data-only per package (ADR-0137 D3): no executable or
    ``.pth`` under its directory. The host's code, entry points, editability and requirements
    are the engine's and are not judged here."""
    if any(PurePosixPath(p).suffix.lower() in _EXECUTABLE for p in inventory if p.startswith(f'cairn_contracts/{name}/')):
        raise ContractDiscoveryError('hosted contract package is not data-only')


def _runtime_requirements(dist: Distribution) -> tuple[tuple[str, str], ...]:
    found = {}
    for text in dist.metadata.get_all('Requires-Dist') or ():
        try:
            requirement = Requirement(text)
        except InvalidRequirement as exc:
            raise ContractDiscoveryError('malformed installed requirement') from exc
        pins = [(spec.operator, spec.version) for spec in requirement.specifier]
        if requirement.extras or requirement.marker or requirement.url or len(pins) != 1 or pins[0][0] != '==':
            raise ContractDiscoveryError('contract dependencies must be exact pins')
        name = canonicalize_name(requirement.name)
        if name in found:
            raise ContractDiscoveryError('duplicate installed requirement')
        found[name] = pins[0][1]
    return tuple(sorted(found.items()))


def _closure(name: str, graph: dict[str, list[str]], chain: tuple[str, ...] = ()) -> tuple[str, ...]:
    if name in chain:
        raise ContractDiscoveryError('contract dependency cycle')
    reached: set[str] = set()
    for dependency in graph[name]:
        reached.add(dependency)
        reached.update(_closure(dependency, graph, chain + (name,)))
    return tuple(sorted(reached))


def _outgoing(body: _Body) -> list[tuple[object, str, str]]:
    """``(position, reference, required target kind)`` for every reference a body makes."""
    value, kind = body.value, body.definition.kind
    if kind == 'schema':
        return [(position, position['$ref'], 'schema') for position in _schema_positions(value) if '$ref' in position]
    if kind in ('capability', 'stage'):
        parts = ('input', 'configuration', 'output') if kind == 'capability' else ('configuration', 'output')
        pairs = [(value, value[field], 'schema') for field in parts]
        return pairs + ([(value, value['report'], 'report')] if 'report' in value else [])
    return []


def _target(reference: str, source: _Body, kind: str, bodies: dict[str, _Body], allowed: set[str]) -> tuple[str, str]:
    """Validate one reference; return ``(target uri, fragment)``. Only a schema body may
    use same-document references or JSON Pointer fragments."""
    base, _, fragment = reference.partition('#')
    from_schema = source.definition.kind == 'schema'
    if base == '' and from_schema:
        base = f'contract://{source.definition.id}/{source.definition.revision}'
    if not _REFERENCE.fullmatch(base) or base not in bodies:
        raise ContractDiscoveryError('reference must name an installed contract://<id>/<revision>')
    body = bodies[base]
    if body.definition.kind != kind or (fragment and not (from_schema and fragment.startswith('/'))):
        raise ContractDiscoveryError('reference targets the wrong kind, an anchor or a non-schema document')
    if body.distribution not in allowed:
        raise ContractDiscoveryError('reference crosses into an undeclared contract distribution')
    return base, fragment


def _pointer_target(registry: Registry, uri: str, fragment: str, bodies: dict[str, _Body]) -> object:
    resolver = registry.resolver(base_uri=uri)
    target = resolver.lookup(f'#{fragment}').contents
    slots: set[tuple[int, str]] = set()
    positions = {id(position) for position in _schema_positions(bodies[uri].value, slots)}
    parent, _, token = fragment.rpartition('/')
    container = resolver.lookup(f'#{parent}').contents
    if (id(container), token.replace('~1', '/').replace('~0', '~')) not in slots if isinstance(target, bool) else id(target) not in positions:
        raise ContractDiscoveryError('reference fragment is not a schema position')
    return target


def _in_place(position: dict, targets: dict[int, object]) -> Iterable[object]:
    """Subschemas applied to the SAME instance location: a cycle through these never
    terminates. ``properties``/``items`` descend into the instance and may recurse."""
    for key in _IN_PLACE & position.keys():
        yield from position[key] if key in _SCHEMA_LISTS else (position[key],)
    if '$ref' in position:
        yield targets[id(position)]


def _check_cycles(position: object, targets: dict[int, object], state: dict[int, bool]) -> None:
    if isinstance(position, bool) or state.get(id(position)) is True:
        return
    if state.get(id(position)) is False:
        raise ContractDiscoveryError('reference cycle')
    state[id(position)] = False
    for child in _in_place(position, targets):
        _check_cycles(child, targets, state)
    state[id(position)] = True


def _resolve_references(body: _Body, bodies: dict[str, _Body], registry: Registry,
                        allowed: set[str], targets: dict[int, object]) -> tuple[str, ...]:
    resolved = set()
    for position, reference, kind in _outgoing(body):
        uri, fragment = _target(reference, body, kind, bodies, allowed)
        if body.definition.kind == 'schema':
            targets[id(position)] = _pointer_target(registry, uri, fragment, bodies) if fragment else bodies[uri].value
        resolved.add(uri)
    return tuple(sorted(resolved))


def _evidence(dist: Distribution, inventory: set[str], root: Path, name: str, value: object) -> tuple[tuple[str, str], ...]:
    found = {}
    for obligation in (value.get('obligations', []) if isinstance(value, dict) else []):
        relative = obligation['evidence']
        found[relative] = _digest(_asset(dist, inventory, root, f'cairn_contracts/{name}/{relative}').read_bytes())
    return tuple(sorted(found.items()))


def _read_bodies(entries: list[tuple[Distribution, set[str], ContractDistribution, bool]]) -> dict[str, _Body]:
    bodies = {}
    for _, _, item, _ in entries:
        for definition in item.definitions:
            contents, value = _read_definition(definition)
            bodies[f'contract://{definition.id}/{definition.revision}'] = _Body(definition, item.name, contents, value)
    return bodies


def _check_dependencies(entries: list[tuple[Distribution, set[str], ContractDistribution, bool]]
                        ) -> dict[str, tuple[str, ...]]:
    items = {item.name: item for _, _, item, _ in entries}
    graph = {}
    for dist, inventory, item, hosted in entries:
        if hosted:
            _hosted_data_only(inventory, item.name)
        else:
            _data_only(dist, inventory, item.name)
            if _runtime_requirements(dist) != item.dependencies:
                raise ContractDiscoveryError('descriptor dependencies differ from installed requirements')
        for dependency, version in item.dependencies:
            if dependency not in items or items[dependency].version != version:
                raise ContractDiscoveryError('declared contract dependency is not installed at that version')
        graph[item.name] = [dependency for dependency, _ in item.dependencies]
    return {name: _closure(name, graph) for name in graph}


def _resolve_all_references(entries: list[tuple[Distribution, set[str], ContractDistribution, bool]],
                            bodies: dict[str, _Body]) -> dict[str, tuple[str, ...]]:
    registry = Registry().with_resources(
        (uri, Resource.from_contents(body.value, default_specification=DRAFT202012))
        for uri, body in bodies.items() if body.definition.kind == 'schema')
    targets: dict[int, object] = {}
    references = {}
    for _, _, item, _ in entries:
        allowed = {item.name, *(dependency for dependency, _ in item.dependencies)}
        for definition in item.definitions:
            uri = f'contract://{definition.id}/{definition.revision}'
            references[uri] = _resolve_references(bodies[uri], bodies, registry, allowed, targets)
    state: dict[int, bool] = {}
    for body in bodies.values():
        if body.definition.kind == 'schema':
            for position in _schema_positions(body.value):
                _check_cycles(position, targets, state)
        elif body.definition.kind == 'stage':
            _check_stage_output(bodies[body.value['output']].value, targets)
    return references


def _check_stage_output(schema: object, targets: dict[int, object]) -> None:
    """ADR-0118 D2 — a stage's output schema stays open and declares no reserved stat key,
    at its root and every subschema applied to the same instance (``allOf``/``$ref``/…).
    Open means nothing bounds the keys the runner adds: ``additionalProperties`` is absent
    or ``true``, and no ``maxProperties``, ``unevaluatedProperties`` or ``propertyNames``
    (the last two are outside the schema subset today; refused here all the same)."""
    seen: set[int] = set()
    pending = [schema]
    while pending:
        position = pending.pop()
        if isinstance(position, bool) or id(position) in seen:
            continue
        seen.add(id(position))
        closing = sorted(k for k in ('maxProperties', 'unevaluatedProperties', 'propertyNames') if k in position)
        if position.get('additionalProperties', True) is not True:
            closing.insert(0, 'additionalProperties')
        if closing:
            raise ContractDiscoveryError(f'a stage output schema must stay open (ADR-0118 D2): {closing} '
                                         'would bound the keys the runner adds')
        declared = set(position.get('properties', {})) | set(position.get('required', []))
        if declared & RESERVED_STAGE_STATS:
            raise ContractDiscoveryError('a stage output schema may not declare the runner\'s reserved stat keys '
                                         f'{sorted(declared & RESERVED_STAGE_STATS)}')
        pending.extend(_in_place(position, targets))


def _resolve_distribution(dist: Distribution, inventory: set[str], item: ContractDistribution, closure: tuple[str, ...],
                          bodies: dict[str, _Body], references: dict[str, tuple[str, ...]]) -> ResolvedDistribution:
    definitions = []
    for definition in item.definitions:
        uri = f'contract://{definition.id}/{definition.revision}'
        evidence = _evidence(dist, inventory, item.descriptor.parent, item.name, bodies[uri].value)
        definitions.append(ResolvedDefinition(definition.id, definition.revision, definition.kind, item.name, item.version,
                                              _digest(bodies[uri].contents), references[uri], evidence, definition.path))
    return ResolvedDistribution(item.name, item.version, item.descriptor, _digest(item.descriptor.read_bytes()),
                                item.dependencies, closure, tuple(definitions))


#: The last successful resolution of the INSTALLED set: (installed key, file stamps, result,
#: the bodies read for it by URI).
_INSTALLED: tuple[tuple, tuple, tuple[ResolvedDistribution, ...], dict[str, tuple[ResolvedDefinition, object]]] | None = None


def _installed_key() -> tuple:
    """Every installed distribution's metadata directory and the stamp of its ``RECORD``: an
    install, upgrade, removal or reinstall of any distribution changes it, without parsing
    any metadata."""
    out = []
    for d in installed_distributions():
        path = getattr(d, "_path", None)
        if path is None:
            out.append((str(d.metadata.get("Name") or ""), str(d.version or ""), None, None))
            continue
        try:
            st = (Path(path) / "RECORD").stat()
            out.append((str(path), "", st.st_mtime_ns, st.st_size))
        except OSError:
            out.append((str(path), "", None, None))
    return tuple(sorted(out, key=repr))


def _stamps(resolved: tuple[ResolvedDistribution, ...]) -> tuple:
    """Size and mtime of every file a resolved contract distribution ships: an in-place
    edit of an installed definition changes it. A vanished file is a stamp too."""
    out = []
    for distribution in resolved:
        for path in sorted(distribution.descriptor.parent.rglob("*")):
            try:
                st = path.stat()
                out.append((str(path), st.st_mtime_ns, st.st_size))
            except OSError:
                out.append((str(path), None, None))
    return tuple(out)


def _installed() -> tuple[tuple[ResolvedDistribution, ...], dict[str, tuple[ResolvedDefinition, object]]]:
    """The installed set's resolution and the bodies read for it, kept for the process
    (maintainer 2026-10-11, "1a"). A full resolution lists every file of every installed
    distribution and validates every body, about 2 s, and the launch door, every leg and
    every opted-in stage asked for it again. Reused only while every distribution's metadata
    directory and ``RECORD`` stamp and every contract file's size and mtime are unchanged; a
    refusal is never kept; a result whose files changed while it was being read is not kept
    either (its digests are re-checked against the files before keeping)."""
    global _INSTALLED
    key = _installed_key()
    kept = _INSTALLED
    if kept is not None and kept[0] == key and kept[1] == _stamps(kept[2]):
        return kept[2], kept[3]
    resolved, bodies = _resolve_contracts(None)
    stamps = _stamps(resolved)
    if all(_digest(d.path.read_bytes()) == d.digest for x in resolved for d in x.definitions):
        _INSTALLED = (key, stamps, resolved, bodies)
    return resolved, bodies


def installed_contract_bodies() -> dict[str, tuple[ResolvedDefinition, object]]:
    """Every installed definition by URI with its parsed body, from the same read that was
    digested. The bodies are shared: a caller that hands one on copies it first."""
    return _installed()[1]


def resolve_contracts(*, distributions: Iterable[Distribution] | None = None) -> tuple[ResolvedDistribution, ...]:
    """Discover, then close every installed contract distribution offline.

    Refuses (never partially) a distribution that is not data-only, whose installed
    requirements differ from its descriptor, whose dependencies are missing, mispinned or
    cyclic, or whose bodies/references/evidence do not resolve exactly. Returns sorted
    frozen records with SHA-256 digests; nothing is executed or fetched. The installed set's
    result is kept for the process (:func:`_installed`); an explicit ``distributions``
    always resolves afresh.
    """
    if distributions is not None:
        return _resolve_contracts(distributions)[0]
    return _installed()[0]


def _resolve_contracts(distributions: Iterable[Distribution] | None
                       ) -> tuple[tuple[ResolvedDistribution, ...], dict[str, tuple[ResolvedDefinition, object]]]:
    try:
        entries = _discover(distributions)
        closures = _check_dependencies(entries)
        bodies = _read_bodies(entries)
        references = _resolve_all_references(entries, bodies)
        resolved = tuple(_resolve_distribution(dist, inventory, item, closures[item.name], bodies, references)
                         for dist, inventory, item, _ in entries)
        return resolved, {f"contract://{d.id}/{d.revision}": (d, bodies[f"contract://{d.id}/{d.revision}"].value)
                          for x in resolved for d in x.definitions}
    except (OSError, ValueError, TypeError, AttributeError, RecursionError, Unresolvable) as exc:
        # TypeError: a pointer walked through a scalar; RecursionError: a pathologically
        # deep reference or dependency chain. Both are refusals, never crashes.
        if isinstance(exc, ContractDiscoveryError):
            raise
        raise ContractDiscoveryError('cannot resolve installed contract distributions') from exc
