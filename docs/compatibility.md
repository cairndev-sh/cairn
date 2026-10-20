# Compatibility and versioning policy

Cairn is pre-1.0 (`0.1.3`). The policy is strict about *what* is public and honest about
*how* it may change; independently installable packs depend on it.

## What is public
The **locked surfaces** (root `CLAUDE.md` §5): `Operation`, `Pack`/`pack.toml`, `PipelineDef`,
`DataflowSpec`, `Run`, `console/openapi.json`; plus the entry-point group names, the
storage schemas of persisted records (`PipelineRecord`, `RunEvent`, `ArtifactRecord`,
`dataset_versions`), the `cairn` CLI verbs and exit codes, and the event topics. Each family
has an owner and a doc in `docs/contracts/README.md`. Everything else under `cairn.core`
is internal: packs may import `cairn.core.operator` (and `cairn.core.llm` / `cairn.core.eval`
for those operator kinds) and nothing else.

## Change classes
| Class | Meaning | Allowed how |
|---|---|---|
| **Additive** | a new optional field/key/type/topic/route; a new kernel, operator, engine; a new column with a default | any PR; CHANGELOG entry for packs |
| **Deprecating** | an old name kept working with a warning | one MINOR of warning before removal; documented in the CHANGELOG and the contract doc |
| **Breaking** | removing/renaming/retyping a public field, route, verb, exit code, topic or entry-point group; changing identity semantics (content hash, `template_ref`, `seq`); moving ownership between boundaries | an ADR (`docs/decisions/`, append-only) **and** a migration note; MINOR bump pre-1.0, MAJOR after; API breaks also need the `api-break` label and the console change in the same PR |

## Specific rules
- **Schemas / storage.** Migrations are additive (`add_missing_columns`); rows written
  before a field existed must still hydrate. A destructive migration needs an ADR and a
  backup step in the migration note.
- **Artifacts.** `id` = content hash, immutable; `parents` may grow; kinds are an enum
  that only grows. A version never moves (`dataset:<name>@N` is permanent).
- **Operators.** `input_schema`/`output_schema` are public: adding an optional field is
  additive; removing or retyping is breaking → manifest `version` bump + CHANGELOG. The
  manifest and the class must agree (drift tests).
- **Packs.** A pack must install and pass its tests against the public core surface alone;
  no private pack-to-pack imports (boundary tests). Entry-point group names are stable.
- **Engines.** An engine is whatever passes the conformance suite; the suite may gain
  cases (engines must follow) but never drop a guarantee without an ADR.
- **API.** oasdiff judges: additions pass; breaking changes fail without `api-break`.
- **CLI.** Exit codes in use: 0 success · 1 failure · 2 usage/refusal · 3 policy/door
  refusal · 130 interrupted. Changing a verb's meaning is breaking.
- **Events.** New types/topics are additive; renames are breaking.

## Release discipline
`/release` classifies every merged change by the table above and blocks on a breaking
change without an ADR or a stale contract doc / support-matrix row. Deprecations and
breaks are listed under **Breaking** and **Migration** in the release notes.
