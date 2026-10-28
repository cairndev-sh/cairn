# ADR-0111 — Independent contracts, capability-only domain composition and single-capability operators

- **Status:** ACCEPTED with conditions (maintainer, 2026-10-06; P1 amended by ADR-0137 for `cairndev` only). This line read "PROPOSED — maintainer review pending" until 2026-10-26, when it was corrected to match the decision log (`README.md`); the owner confirmed the acceptance that day. The decision text is unchanged.
- **Date:** 2026-09-24
- **Owner:** Cairn maintainer
- **Related epic:** #2230, capability contract review. Related execution work: #2182 and #1488; conformance: #2238. No new issue filed by this record.
- **Evidence baseline:** working tree at `3f67d348`, including existing uncommitted changes.
- **Proposed amendments:** ADR-0020 pack assets/bindings and operator metadata; ADR-0026 packaging/discovery extension. Preserves its pack/profile/environment ownership split.
- **Relationship to ADR-0108:** replaces its proposed pack-owned contract-carrier direction with independently distributable contracts if accepted. ADR-0108 is itself PROPOSED; neither proposal is current authority. Its conformance and governance goals remain applicable.
- **Preserves:** ADR-0068/0070 configuration precedence and authority, ADR-0084 execution boundaries/compiler, ADR-0103 operator reporting, ADR-0106 declared-pipeline launch. ADR-0110 native export remains a separate proposal.

## Context

Multiple domain packs can require the same semantic capability, and multiple operators
can implement it. The definition must therefore be discoverable without installing a
particular consuming domain pack or loading a provider implementation. Keeping shared
meaning inside one workflow or repeating it across operators creates dependency and drift.

The desired separation is:

1. **Domain:** declarative composition, workflow interfaces, defaults and policy intent.
2. **Contracts:** independent, versioned definitions of capabilities and their boundaries.
3. **Operators:** concrete implementations conforming to those definitions.

The gold training exercise is evidence for design gaps, not a working implementation.
See [scope review](../architecture-fix/gold-training-review/SCOPE-AND-REVIEW.md). Its
copied flows/operators still use legacy identities and schemas; proposed manifests do
not establish executable compatibility.

### Current evidence

| Classification | Source | Finding |
|---|---|---|
| OBSERVED | `cairn/core/operator/protocol.py:261`, `:304` | Operation owns input/output/config/report declarations and allows a list of capabilities. Exactly-one capability is a proposed restriction. |
| OBSERVED | `cairn/core/pack/composition.py:179` | Composition validation and profile binding checks already exist; extend this boundary rather than adding a parallel resolver. |
| OBSERVED | `cairn/core/operator/config.py:114` | Configuration resolution already exists; semantic separation must not introduce competing precedence. |
| OBSERVED | `cairn/core/operator/executor.py:113` | Executor selection already exists and is distinct from capability semantics. |
| OBSERVED | `cairn/packs/training/operations/run-train.yaml` | Current domain operation directly names train/stub. Capability-only authoring requires migration. |
| DOCUMENTED | ADR-0020 | Domain packs are declarative; local schemas and operation bindings are prescribed by the older locked layout. |
| DOCUMENTED | ADR-0106 D2 | A pipeline can inherit its component's interface rather than duplicate it. |
| DOCUMENTATION / IMPLEMENTATION CONFLICT | ADR-0020 vs `docs/architecture/06-packs.md` | Literal zero-Python rule versus minimal domain-pack discovery __init__.py. This proposal excludes business code and requires a migration decision for discovery glue. |

## Decision (proposed)

### D1 — Three independently owned components

| Owner | Contains | Excludes |
|---|---|---|
| Contract distribution | Capability definitions; shared data, configuration and result schemas; semantic report definitions; compatibility rules; documentation and conformance examples | Workflows, executable validators/handlers, provider SDKs, secrets, datasets/model weights, storage or runtime implementations |
| Domain pack | Declared pipelines, flows, invocation mappings, workflow-specific schemas, policies, presets, examples and docs | Concrete operator/provider selection, business implementation code, credentials, authoritative copies of shared definitions |
| Operator distribution | One or more operators, implementation-specific schemas/options, adapters, diagnostics, tests and dependencies | Domain workflow ordering, redefinition of shared semantics, private dispatch/HTTP dependencies |

Independent ownership does not imply one repository or Python package per capability.
A contract distribution may publish several related capabilities and reusable schemas.
Keep distributions cohesive and avoid a mandatory global contract megabundle.

Core owns generic loading, validation and existing enforcement interfaces. Domain meaning
does not become a Core enum. Contract dependencies must be data-only and acyclic; no
contract imports a domain or implementation. Shared artifact/model identity references
existing platform contracts; training must not invent another identity system.

### D2 — Domain executable dependencies are capability-only

A domain may require many capabilities. Every authored business-action invocation
references a known capability and compatible revision, never a concrete operator,
Python handler, provider SDK or infrastructure image. This applies to MCP actions too:
the domain requests a declared capability with validated tool arguments; provider/tool
permissions remain enforced by the existing MCP/operator boundaries.

“Capability-only” does not prohibit references to local flows, shared schemas, logical
resources or policy declarations. Conditions, gates and composition remain declarative
workflow constructs; they are not converted into arbitrary operators merely for uniformity.

Business execution remains in implementations reached through existing machinery.
Agentic prompt/model nodes and data/ML stage kernels currently have distinct interfaces.
Map their authored requirements to explicit supported contracts through reviewed lowering
or adapters; do not claim all current legs are Operation subclasses or replace the
compiler/dataflow engine. An unhandled node/stage is a migration blocker, not permission
to bypass the new rule. No new runtime is introduced.

Domain packs contain no Python business logic, shell scripts or Dockerfiles. Existing
minimal discovery glue is a named legacy exception until data-only discovery is agreed;
do not claim literal zero-code distribution before that change is qualified.

### D3 — One semantic capability per operator

An operator implementation has exactly one primary semantic capability identity.
It may declare support for multiple compatible revisions of that identity. An operator
distribution can contain many operators and share internal implementation helpers.

Unrelated capabilities use separate operator declarations, even when backed by one SDK.
Provider metadata, execution features and tags are not additional capability identities.
Invoking dependency capabilities does not mean an operator implements them; any such
invocations must retain the existing scoped context and enforcement path.

This restriction changes the current multi-capability metadata contract. Inventory actual
multi-capability registrations before migration. Treat legacy aliases explicitly, with
deprecation/compatibility rules; never silently remove them or duplicate implementation
code to satisfy metadata shape. The first implementation plan must name affected callers.

### D4 — Capability definitions describe enforceable promises

A capability declares identity/revision, meaning, inputs/outputs, semantic configuration,
preconditions, required observations, failure/checkpoint obligations and compatibility.
It references reusable schemas rather than embedding independent copies everywhere.

Guarantees must state scope and conditions. A seed is not a bitwise reproducibility
guarantee. Schema-valid data is not necessarily suitable or authorized data. A digest-shaped
string does not prove artifact integrity. Define which owner produces and checks evidence.

Executable validation stays in existing generic validators or explicitly selected
implementations. Capability files contain neither executable plugins nor unrestricted
expressions. A custom content check is a declared validation dependency, not hidden code
inside a contract. Contracts constrain behavior but do not grant authorization.

### D5 — Operators may extend without redefining shared semantics

Operators may own provider-response schemas, intermediate structures, implementation
configuration and diagnostic metrics. They adapt these to the shared boundary.

Required shared inputs must be accepted within the declared support envelope; outputs
must satisfy the contract. Supported parameter combinations are explicit and checked
before effects. An operator cannot silently reinterpret a shared field, omit a required
result, or require an undeclared secret/option. An implementation supporting only LoRA
must refuse QLoRA rather than ignoring the request.

Implementation-specific options exposed to profiles are public implementation contracts,
not private merely because they are outside the capability. Changes require compatibility
management. Internal data models need not be shared unless another boundary relies on them.

### D6 — Configuration separates meaning from authority

| Configuration | Definition owner | Value/authority owner |
|---|---|---|
| Portable semantic options | Contract distribution | Permitted domain defaults and request inputs |
| Implementation options | Operator | Existing profile/configuration layers |
| Execution placement/resources | Existing engine/executor contracts | Profile/environment |
| Governance controls | Existing governance contracts | Authorized environment policy; domain may add permitted restrictions |
| Credentials/connections | Existing secret/connection interfaces | Environment secret management |

Keep ADR-0068/0070's resolver, precedence and governance/operational distinction.
Semantic versus implementation is an additional ownership dimension, not a replacement
authority hierarchy. The implementation plan must define conflict handling and a legacy
flat-input adapter; avoid ambiguous copies of a knob in input, config and extra.

Shared defaults live in one place; a domain override is explicit. Record effective
configuration and provenance with secrets removed. No provider selection or credentials
may be smuggled through semantic config or a generic extra escape hatch.

### D7 — Reporting contracts extend the current reporting mechanism

Shared definitions describe metric name, type, unit, measurement scope, aggregation,
required/optional/conditional availability and comparison context. Operators map their
measurements to those semantics and may declare additional diagnostics.

Preserve ReportSpec, Reporter, existing events and manifest/class agreement (ADR-0103).
Choose generation or verified composition from shared definitions, never two unchecked
authoritative declarations. Workflow narratives are domain outputs, not metric contracts.

Best-effort telemetry is not proof for a mandatory gate. Required evaluation outcomes
must travel through validated, durable result/artifact paths. Missing measurement is
not zero, and loss values are not comparable without compatible objective/dataset context.

### D8 — Domain schemas are needed only for new composition interfaces

Every public flow/pipeline interface must be declared, but a separate local schema file
is not universally required. Reuse a shared contract when identical; use supported
inheritance for an unchanged wrapper. Author a local schema when composition exposes
a new input/output shape, referencing shared definitions for reusable fields.

Example: read → deduplicate → redact → write may expose a dataset artifact plus combined
dedup/redaction accounting. No individual operator owns that assembled interface.
Conversely, a one-capability wrapper should not copy its input/output schemas.

This amends ADR-0020's mandatory local schema-file layout if accepted. Test references
and schema composition, including closed-schema behavior; do not assume allOf extends
an additionalProperties=false schema. Domain-owned API schemas still need versioning.

### D9 — Operations are simple declarative invocation mappings

A named operation contains a capability reference, permitted input/configuration mappings,
optional semantic defaults and result projection. It has no concrete operator/provider,
runtime selection, arbitrary code, retry scheduler or independent policy evaluator.
Flow/pipeline owns branching and orchestration. Existing governance references may be
retained only in the owning approved grammar; mapping cannot relax environment authority.

Illustrative syntax, **not current grammar**:

```yaml
capability: dataset.deduplicate@1
inputs:
  dataset: $input.dataset
config:
  key_columns: $input.deduplication_keys
outputs:
  dataset: $result.dataset
  removed_count: $result.duplicates_removed
```

Use Cairn's existing expression machinery. Define mapping context and schema checking
precisely before implementation. Static validation checks known references/structure;
runtime validates actual values and postconditions. Complex transforms belong in operators.

A separate file is justified by reuse or nontrivial adaptation, not mandatory ceremony.
If inline capability invocation is approved, it lowers to the same canonical invocation
representation as a named operation. Do not maintain two execution paths. Existing
operation-node files remain until their migration is explicitly supported.

### D10 — Profiles bind implementations; resolution is deterministic

Reuse the profile binding concept:

```toml
# Illustrative names using the existing binding-table concept.
[profile.bindings]
"dataset.deduplicate" = "dataset/deduplicate-duckdb"
```

Resolve the capability and schemas independently of providers, then choose a profile-bound
implementation compatible with the requested revision, semantic options and execution
policy. Missing or ambiguous bindings fail explicitly; no arbitrary provider fallback.
Keep provider defaults distinct from identity resolution and current-policy admission.

Separate identities: distribution version, contract revision, operator version and artifact
digest. Pin resolved definitions, implementation and effective configuration in existing
plan/run provenance. Recovery must not silently select a different implementation.

### D11 — Independent discovery without a new service

Contract distributions must be inspectable when no consumer domain or provider SDK is
installed. Declare every published identity/revision and external contract dependency.
Reject conflicting definitions and unavailable required revisions. Resolve packaged
references offline; schema inspection must not fetch arbitrary network URLs or execute
provider imports. Constrain paths to declared installed assets and reject cycles.

Reuse installed distribution metadata, existing pack loading/composition and JSON
Schema/Pydantic mechanisms where sufficient. No new registry service, database, singleton
or entry-point group is authorized by this ADR. The concrete distribution manifest,
index/discovery extension, trust/provenance rules and version-resolution syntax require
an exact reviewed schema delta. Do not silently invent a currently supported pack kind.

## Proposed physical structure

These are responsibility locations, not approved new loader conventions. Optional
directories should exist only when used. File formats: TOML for existing pack manifests;
JSON Schema in JSON/YAML for schemas; declarative YAML for proposed capability/report
metadata. Contract-distribution manifest syntax remains a decision before coding.

```text
gold-training-contracts/
  manifest                         # proposed distribution descriptor
  capabilities/training.sft.adapter.yaml
  data/supervised-example.schema.json
  configuration/sft-options.schema.json
  results/training-result.schema.json
  reports/training.yaml
  examples/{valid,invalid}/
  docs/{semantics,compatibility}.md

gold-training-domain/
  pack.toml
  pipelines/supervised-finetune.yaml
  flows/{readiness.yaml,sft-train.yaml,training-report.md}
  operations/run-training.yaml      # when adaptation/reuse needs a named mapping
  schemas/FinetuneReport.schema.json # only composition-specific shape
  policies/candidate-acceptance.yaml
  presets/small-experiment.yaml
  examples/
  docs/

gold-training-operator/
  pyproject.toml
  pack.toml
  src/operator_gold_training/
    operators/{local,stub}.py
    schemas/local_options.py
    reporting.py
    adapters/sft.py                 # optional; inline small adapters instead
  tests/
  docs/
```

The gold fixture currently has different names and only a subset. Do not mistake this
target tree for generated/installed files. Shared model/artifact definitions belong to
their existing generic owner, not automatically to gold-training-contracts. A stub is a
test double and cannot satisfy a real-training/artifact guarantee for production.

## Dependency and execution graphs

```text
                     independent contracts
                      ↑                 ↑
                  requires          implements
                      │                 │
               domain packs       operator distributions
                      │                 ↑
                      └── profile binds ┘

Invocation:
declared pipeline → domain composition / capability mapping
  → contract + binding + configuration validation
  → existing admission / governed invocation / executor or engine
  → selected implementation → validated results + existing provenance
```

Metadata checks with sufficient static inputs happen before side effects. Runtime checks
cover dynamic inputs, authorization and produced outputs. There is no new launch door.

## Verification and acceptance

Upstream reuse: this proposal needs neither a new LangGraph primitive nor a new execution
engine. Existing schema tooling, pack composition, profile resolver, operator invocation,
reporting and artifact storage remain the substrate. New work is declaration/distribution
and conformance wiring. No effort estimate is justified before the selected closure and
exact grammar delta are reviewed.

- Discover contracts with no domain/operator implementation installed.
- Two independent domain fixture packs consume the same contract without copying it.
- Two conforming implementation fixtures can be profile-selected under the same semantic
  request; unsupported options are refused. Test doubles do not establish real ML proof.
- Conflicting IDs, missing revisions, cyclic/unresolved refs and network schema resolution
  fail clearly; loading contracts performs no provider imports.
- Reject concrete operator/provider fields in opted-in domain invocation syntax.
- Reject unrelated multiple capability identities on an opted-in operator; preserve explicit
  legacy compatibility until migration completes.
- Named/inline invocations, if both exposed, lower identically and reach the same gates.
- Invalid inputs, denied policy, unauthorized overrides and incompatible bindings produce
  zero protected effects. Output violations cannot count as successful conformance.
- Required report/result checks include conditional availability and measurement context;
  missing metrics never fabricate passing quality evidence.
- Verify shared schema/model generation or drift checks; preserve ADR-0103 manifest parity.
- Retry/adoption keeps contract/operator pins; test old-row hydration where records change.
- Preserve managed template/task paths and bridge/Temporal behavior where affected; pair
  SQLite/Postgres migrations only when persistence changes require them.
- Inject a broken mapping/removed gate and prove an independent oracle rejects it. A
  generated schema and generated test agreeing is insufficient evidence.

Existing evidence homes include operator/config/report tests, composition tests and
`tests/core/test_architecture_boundaries.py`. New independent discovery and semantic
substitution proofs are missing. Required FAIL/UNKNOWN blocks readiness. Record workload
sizes and validation costs before claiming scalability; schema validation does not inspect
all dataset rows in the control plane.

## Slices and migration

1. **Approve grammar and ownership:** exact identities, distribution format/discovery,
   profile revision matching, one-capability migration and configuration adapters.
2. **Contract discovery fixtures:** independently load data-only definitions and prove
   refusal cases using existing owners. No production domain migration yet.
3. **One invocation path:** review one coherent fixture composition, adapt selected
   implementations and converge on existing governed invocation. #2182 and remaining
   #1488 behavior stay scoped work, not a blanket rewrite.
4. **Domain-facing proof:** reusable operation mappings, referenced/inherited schemas,
   and actual policy/preset examples; prove two consumers and interchangeable providers.
5. **Reporting/config compatibility:** one authoritative semantic definition with checked
   implementation mappings and existing runtime mechanisms.
6. **Incremental adoption:** mark legacy definitions structurally validated but not
   semantically certified; migrate and deprecate explicitly. No silent signature changes.

Retain legacy readers/adapters until accepted migration criteria pass. Pin in-flight
work to its old resolution; do not dual-execute or rebind to roll back. A rollback restores
the prior compatible package/configuration for new admissions, not a disabled gate or
weakened contract. No allowlist expansion to hide new coupling.

## Architecture review

- **Boundaries:** pack discovery/composition, operator invocation, engine mapping, governance,
  reporting and provenance. Consumer contracts move to independent data definitions;
  implementation and enforcement owners do not move merely because files move.
- **Contract impact:** HIGH: Pack/manifest, Operation capability metadata, reference grammar,
  optional invocation syntax and recorded pins. API/OpenAPI changes are conditional on
  exposing new surfaces, not automatically in scope.
- **Invariants:** declared pipeline launch, per-leg identity, operator/flow gates, budget,
  redaction, audit and artifact registration remain required. Runtime proof is not supplied
  by this documentation change. Keep the ADR-0106 development exception explicitly scoped.
- **Forbidden dependencies:** no Core-to-domain/provider imports, operator-to-operator
  imports, SDK-to-Core imports or contract-to-consumer dependencies introduced.
- **Risks:** HIGH — unchecked aliases/config defaults, governance bypass during lowering,
  semantic drift between Python/JSON Schema and unstable version resolution. MEDIUM —
  unnecessary packaging fragmentation or duplicated adapter layers.
- **Recommendation:** PROCEED WITH CONDITIONS as a proposal; acceptance and exact grammar
  review precede production implementation. Existing ADRs remain authoritative meanwhile.

## Consequences and alternatives

Benefits: independently discoverable shared meaning; provider substitution through profiles;
smaller domain mappings; fewer duplicated schemas; explicit extension boundaries. Costs:
version/discovery rules, compatibility migration and ongoing conformance tests. This is a
modularity improvement, not evidence of throughput, ML quality or production readiness.

Alternatives considered:

1. Keep contracts inside consumer domain packs: smallest immediate change, but forces
   independent consumers/providers to depend on a particular domain or duplicate meaning.
2. Keep only operator-owned schemas: preserves current structure, but provides no independent
   shared semantic authority for multiple implementations.
3. Add a contracts service/new runtime: unnecessary for installed metadata validation and
   outside scope; reuse current loaders and execution machinery.
4. Keep multi-capability operators unrestricted: avoids migration but weakens the requested
   single-operation boundary. Prefer shared helpers and distinct operator declarations.

## Open decisions before implementation

Exact contract manifest format and install discovery; namespaced IDs/revision policy;
alias migration; schema-to-Pydantic source authority; allowed inline operation grammar;
stage/agentic-node capability lowering; snapshot/pin storage compatibility; and the first
coherent non-production fixture composition. No directories, executable formats or support
claims become production contracts merely because this proposal illustrates them.

## Production design proposal — 2026-10-05 (#2230)

**PROPOSED, not accepted or implemented.** The maintainer authorized preparation of this
concrete design, not production changes. This dated addendum resolves earlier open
questions for review; D1–D11 remain the intended ownership model. Earlier examples and
historical observations remain intact. Acceptance must explicitly name this addendum.

### P1. Distribution and import-free discovery

Use an independently installable Python wheel containing declarative assets. No new
entry-point group, service, database or singleton. Contract discovery belongs to the
existing `cairn/core/pack` owner but does not invoke `load_all_packs`: the current loader
calls `ep.load()` (`loader.py:612`), which executes imports.

The proposed wheel layout is `cairn_contracts/<normalized-distribution-name>/contracts.toml`
plus files beneath that directory. Normalize distribution names using the existing
`packaging` library. Enumerate installed distribution file metadata and use its file
location API; do not import a package to locate resources. The path must be listed as
owned by that distribution. Exactly one descriptor is permitted per distribution.
A contract-only wheel needs no Python module, namespace initializer or entry point.

Version 1 supports installed wheels with a complete file inventory. Editable/source-tree
contract distributions are explicitly unsupported initially; development uses a locally
built wheel in an isolated environment. Existing domain/operator editable installs remain
unchanged. Do not add a new development-path environment variable.

```toml
format = "cairn.contracts/1"

[distribution]
name = "cairn-training-contracts"
version = "1.0.0"

[[definitions]]
id = "training.sft.adapter"
revision = "1.0.0"
kind = "capability"
path = "capabilities/training.sft.adapter.yaml"

[[definitions]]
id = "training.sft.options"
revision = "1.0.0"
kind = "schema"
path = "configuration/sft-options.schema.json"

[[dependencies]]
distribution = "cairn-artifact-contracts"
version = "1.0.0"
```

Closed descriptor: only the illustrated top-level keys; `definitions` is nonempty and
`dependencies` may be omitted. Definition kinds are `capability`, `schema`, `report`.
All shown record fields are required; unknown fields reject. This excerpt needs the
remaining referenced definitions in a real wheel; it is not an installable example.
Distribution name/version must match installed metadata. Dependencies must also appear
as exact requirements in wheel metadata; descriptor requirements cannot install anything.
Extra runtime dependencies and executable assets are not permitted in contract-only wheels.
No installation, provider discovery or network requests occur during inspection.

### P2. References, revisions and provenance

IDs use lowercase dot-separated segments matching `[a-z][a-z0-9_]*`, with at least two
segments. One distribution owns each ID across all installed revisions. Revisions are
three numeric components `major.minor.patch`, without aliases, ranges or prereleases in
version 1. References use `contract://training.sft.options/1.0.0`; schema fragments may
append a JSON Pointer. Major-only gold references such as `@1` are fixture syntax only.

A package release may contain several exact revisions of the same ID. A revision is
immutable: any content change requires a new revision. Breaking semantic changes require
a new major revision; other changes still require explicit consumer/provider adoption.
No automatic "latest", range negotiation or major-version substitution at runtime.
Multiple installed distributions claiming an ID reject even if bytes match. Duplicate
ID/revision records in one descriptor also reject. Multiple providers implementing one
capability are valid and are a separate matter from definition ownership.

Compute SHA-256 over descriptor bytes and each declared asset's bytes; retain the sorted
transitive definition closure, including dependency distribution name/version and digests.
Whitespace changes consequently change content identity. Hashes prove byte identity, not
publisher trust or semantic compatibility. Installation authorization stays with existing
deployment/package controls; this proposal does not provide signing or sandbox Python
package installation. Inspection must validate its own paths even for installed packages.

Reject absolute paths, traversal, symlinks escaping the distribution asset root, files
missing from the owning distribution inventory, duplicate YAML keys and non-data YAML
tags. Resolve only declared installed definitions. Permit same-document JSON Pointer
references and exact `contract://` references; reject file/HTTP references, relative
cross-file references, unresolved fragments and reference/dependency cycles, including
recursive schemas in version 1. Do not fetch JSON Schema dialect URLs: use a locally
supported dialect. Unknown dialects or validation vocabulary refuse inspection.

### P3. Capability and reporting grammar

Shared schemas use JSON Schema Draft 2020-12, parsed with existing JSON/YAML tooling.
Validate required formats explicitly; an annotation-only `format` must not silently
become a guarantee. Implementation must inventory the installed validator and its
supported keywords before promising full dialect coverage; no custom schema engine.

```yaml
format: cairn.capability/1
id: training.sft.adapter
revision: 1.0.0
summary: Produce an adapter candidate from supervised examples.
input: contract://training.sft.input/1.0.0
configuration: contract://training.sft.options/1.0.0
output: contract://training.sft.result/1.0.0
report: contract://training.sft.report/1.0.0
obligations:
  - name: candidate_identity
    description: Return a registered immutable artifact identity.
    evidence: examples/candidate-identity.md
```

All fields except `report` and `obligations` are required; unknown fields reject. Schema
references must resolve to `schema`, report references to `report`. Obligations have
exactly `name`, `description`, `evidence`; evidence is a packaged documentation path.
They are review/conformance obligations, not executable policy or proof of enforcement.
Each must map to a rejecting test before an operator is certified. Do not confuse schema
validation of an artifact reference with validation of remote dataset/model contents.

A report asset has `format: cairn.report/1`, `id`, `revision`, and `spec`. `spec` uses the
existing ADR-0103 `ReportSpec` serialized fields; no second reporting vocabulary or event
transport. Optional `semantics` maps existing series/scalar names to `description`, `unit`,
`applicability` and `comparison_context` strings. These annotations describe meaning;
requiredness remains in the existing reporting contract. If that cannot express a needed
conditional guarantee, reject that guarantee for v1 and propose a separate reviewed delta.
No universally required eval loss; unlike objectives or datasets are not comparable merely
because metric names match. Operator diagnostics may extend, never redefine, shared names.

### P4. Authoring and operator conformance delta

For opted-in domain packs, propose `contract_mode = "v1"` under `[pack]`, plus a
`[contracts]` table mapping local aliases to exact capability/schema/report references.
Legacy manifests omit both. Aliases are authoring conveniences, expanded to exact refs
before planning; they never become stored identity or installation-time global aliases.

Named operation files retain existing mapping/expression behavior. For v1 replace the
concrete `operator` and its implementation `version` selector with one `capability`
reference, and add optional `configuration` mapping for semantic configuration. `input`
continues to map runtime inputs. All other existing fields retain their current meaning
unless explicitly prohibited by v1 validation. Authoring-schema review must enumerate the
existing fields and reject any that permit provider overrides; no catch-all extra fields.
Inline capability syntax is deferred: flows/templates initially reference named operations.
Pipeline leg ordering and kinds remain unchanged; no generic graph IR is introduced.

An opted-in operator manifest adds `implements = ["contract://training.sft.adapter/1.0.0"]`
inside its existing operator declaration. Nonempty exact revision list; every entry must
have the same capability ID. Runtime class metadata must agree with the manifest. Existing
`capabilities` labels survive only as explicitly mapped legacy aliases; they do not satisfy
new semantic validation. A multi-semantic operator needs separate declarations sharing
implementation helpers, not duplicated business logic.

Profile binding keys remain capability IDs and values retain existing operator-ref syntax.
A selected operator must implement the exact required revision; unsupported combinations
refuse rather than select another provider. Provider options stay in the existing
implementation-configuration path. No provider configuration in domain invocation mappings.
Resolve semantic defaults and then existing authorized overrides through the current
configuration owner; do not create a competing merge order. Governance fields cannot be
weakened by capability defaults, domain presets or per-run values. #2241 must publish a
field-level legacy mapping and preview/runtime equivalence tests before migrating callers.

JSON Schema owns shared shape; Pydantic validates/adapts the implementation boundary.
Do not claim schema equivalence from matching examples. Test shared rejection cases and
explicit model/schema drift; validators cannot impose undisclosed stronger requirements.
Unsupported configurations must be declared and rejected during compatibility validation.
Data content checks remain governed operators; Core does not scan dataset rows.

### P5. Lowering, effects and immutable recorded resolution

The existing pack composition owner resolves declarations, dependencies and bindings.
Existing engine/dispatch owners lower selected operations into their existing carriers.
Task, template and data/ml paths must reach their established governance and execution
owners; stage/engine adapters require #2241 classification, not forced Operation wrappers.
This design adds no execution engine, executor, launch door or policy evaluator.

Propose one optional `contract_resolution` field on existing pipeline plan/record carriers:
`format: cairn.resolution/1` plus `invocations`, keyed by stable leg/node/stage path. Each
entry records `capability_ref`, `definitions` (exact refs, distribution identities, hashes),
`operator_ref`, `operator_version`, and `profile_digest`. Store effective non-secret
configuration using existing parameter/provenance mechanisms, not a second payload store.
Secrets remain scoped references; never hash raw secrets into public provenance.

Record resolution before effects. Each leg references its parent snapshot and invocation
path rather than rediscovering bindings. Retry/resume compares available definitions and
implementation pins to the snapshot: mismatch or unavailable revision refuses with an
actionable error. New admissions can use a newly reviewed package/profile; in-flight work
cannot silently switch. Existing records without the field remain legacy-compatible and
must not be labelled contract-certified. Exact Python/storage field mapping and API
exposure belong to #2240's reviewed compatibility slice, with SQLite/Postgres hydration
and bridge/Temporal serialization tests; no new tables are proposed here.

### P6. Authority amendments, gold crosswalk and delivery

Acceptance would amend only these surfaces; historical records are not rewritten:

| Existing authority | Proposed amendment / preserved rule |
|---|---|
| ADR-0020 | Add v1 declaration fields and optional referenced interfaces; local schemas only for distinct composition shape. Existing manifests remain supported. |
| ADR-0026 | Add metadata-only installed contract-wheel discovery inside the pack owner; preserve domain/operator install semantics and environment ownership. |
| Training FREEZE-v1 capability vocabulary | Independent distributions own shared definitions instead of a consuming pack; preserve consumes/produces, governance, retry/checkpoint and provenance obligations. |
| ADR-0103 | Reuse ReportSpec, reporter and event ownership; semantic annotations do not replace transport or grant runtime guarantees. |
| ADR-0084 / ADR-0106 | Preserve compiler, runtime ownership, launch door, per-leg runs and explicit local-development exception. |
| ADR-0108 proposal | Supersede its proposed pack-owned definition carrier only; retain conformance goals. |
| ADR-0068/0070 mechanisms | Preserve current configuration precedence; their Proposed status is not upgraded by this design. |

Gold revision 2 supersedes the earlier copied-file exercise described in the historical
Context section. Its `manifest.proposed.yaml`, `gold-review/v2`, major-only refs and
injected backends remain illustrative. P1–P5 are the production candidate, not a claim that
gold is installable. Generic artifact identity remains owned by existing storage; shared
artifact-reference schemas should be supplied by a cohesive core contract distribution,
not redefined by every training/data pack. Dataset row schemas remain domain semantics.

| Decision/proof | Owner / rejecting evidence |
|---|---|
| Accept exact design and amendments | #2230, maintainer decision naming this addendum |
| Metadata-only discovery and recorded identity | #2240: installed wheel closure; no provider imports/network; malformed refs, duplicate owners, cycles and unsafe paths reject |
| Governed conformance and compatibility | #2241: bad inputs/options/outputs refuse; denied effects never reach providers; retries/cancel/reporting preserve owner semantics |
| Operator migration | #2242–#2245: every current inventory row accounted for, package-sized changes |
| Shared consumption and substitution | #2246 with #2241: two independent consumers and two genuinely conforming providers; negative stub is not a provider |
| Completeness enforcement | #2247: omitted operators and manifest/class drift fail; legacy domains continue working |
| Training-specific semantic composition | #2193/#2186: consume shared definitions; no parallel contract carrier |

### P7. Acceptance limits and next review

This design makes no throughput, model-quality or recovery claim. Installed closure and
negative tests are necessary but do not prove real backend behavior. Benchmark metadata
resolution against a declared package/reference count before claiming scalability; choose
limits from measured deployment needs, not invented quotas. Remote/paid tests require
separate authorization. Native export (#2238), profile scope (#2248), and workflow
qualification (#2249) remain separate delivery owners.

Before implementation approval, reviewers must assess: feasibility of the wheel inventory
layout using the existing build toolchain; ReportSpec serialization/dialect coverage;
exact closed operation-field crosswalk; and plan/record compatibility mapping. These are
explicit review conditions, not permission for implementation to invent missing grammar.
If any condition requires another public contract change, revise this proposal first.

### P8. Explicit v1 operation-field crosswalk

Observed `OperationBinding` (`cairn/core/operator/binding.py:136`) has eleven fields,
although its comments call the schema ten-field. This is a documentation/implementation
conflict; the candidate below enumerates names rather than relying on that count.

| Existing field | Proposed v1 disposition |
|---|---|
| `operator`, `version` | Forbidden; replaced by required exact `capability` reference (or pack-local alias). |
| `input`, `output` | Retained mappings; closed shared schemas validate resolved inputs/results. |
| `resources` | Forbidden in v1 operations; contract requirements and deployment selection own this boundary. |
| `secrets` | Forbidden in v1 operations; selected implementations declare scoped requirements. |
| `artifacts` | Forbidden in v1 operations; output contract identifies artifact references and existing control-plane registration owns persistence. |
| `budget`, `approval`, `policy` | Retained with current closed sub-schemas, subject to existing authority; cannot weaken environment/implementation enforcement. |
| `execution` | Forbidden in v1 operations; cannot choose profile, mode or compute provider from a domain. |
| new `configuration` | Optional semantic mapping validated against the capability schema; no implementation options. |

P8 supplies the field enumeration requested in P4/P7; its compatibility impact still
requires maintainer review. Legacy operation files retain their existing fields unchanged.

### P9. Identity consistency and evidence assets

Descriptor and capability/report asset IDs and revisions must agree exactly; mismatch
rejects. A schema's `$id`, when present, must equal its descriptor-derived contract URI.
Referenced obligation evidence files must belong to the same distribution inventory,
satisfy P2 path restrictions, and be included with their byte digests in the immutable
closure. Documentation digests establish provenance, not that the obligation was proved.

## Design verification and refinement — 2026-10-06

**PROPOSED refinement; not maintainer acceptance.** This addendum takes precedence over
P3/P5 where it narrows the candidate design. Evidence is local feasibility, not shipped
contract discovery or runtime conformance.

1. **Packaging feasibility confirmed narrowly:** cached Hatchling built a data-only wheel
   using `bypass-selection = true` and `force-include` mapping assets to
   `cairn_contracts/<normalized-distribution-name>`. The wheel contained no Python source;
   extracting it into a temporary installed-layout directory allowed `importlib.metadata`
   to enumerate its distribution, RECORD and descriptor without importing a provider.
   Full package-manager installation, dependency closure and malicious-wheel rejection
   remain #2240 acceptance tests. Do not mistake a wheel extraction probe for those tests.
2. **Reuse existing schema tooling:** local jsonschema 4.26.0 and referencing 0.37.0 resolved
   an exact `contract://` resource in an in-memory registry; valid integer passed and
   string failed. No resolver retrieval callback/network is needed. Implementation must
   validate the proposed bounded vocabulary, duplicate keys and paths before handing
   schemas to the validator; passing one schema is not full dialect qualification.
3. **ReportSpec correction:** `cairn/core/reporting.py:55` declares stages/series/scalars,
   and `:105`/`:117` serialize/parse them. It has no required-emission guarantee. Therefore
   P3's phrase about existing requiredness must not be read as required metric delivery.
   For v1, required quality evidence belongs in validated typed output results; telemetry
   remains the existing reporting mechanism. A new guaranteed-emission protocol is deferred.
   Strictly validate report documents before `from_manifest`, which is not itself a closed
   document parser. Tested nonempty manifest roundtrip succeeded.
4. **One resolution snapshot, not two:** add proposed optional `contract_resolution` to
   `PipelinePlan` (`engine/bundle_pipeline.py:107`), which already resides in
   `PipelineRecord.plan` (`storage/pipeline_store.py:86`). Do not also duplicate it on the
   record. Absence reads as legacy, not certified. Both pipeline stores serialize the same
   model JSON (`pipeline_store.py:206,360`); no new SQL column/table is proposed for this
   field. Actual persistence/hydration and bridge/Temporal propagation tests remain required.
   Pins still need provider code/package identity where operator version alone is mutable.
5. **Provider metadata:** extend the existing operator manifest/declaration only; inspect
   compatibility before invocation. Profile capability-ID bindings do not replace existing
   legacy pack-disambiguation keys. Preserve both meanings explicitly during migration.

The review packet and profile inventory live in
[foundation implementation design review](../architecture-fix/foundation-implementation-design-review.md).


## Maintainer acceptance — 2026-10-06

**ACCEPTED WITH EXPLICIT REMAINING DESIGN CONDITIONS.** The maintainer replied "accepted"
to the concrete contracts/profile/invocation review and its stated outstanding work.
This dated status supersedes earlier PROPOSED labels for the bounded decisions below;
historical proposal text is retained rather than rewritten. This is design acceptance,
not implementation completion, runtime qualification, commit or deployment authorization.

Accepted review: [foundation implementation design review](../architecture-fix/foundation-implementation-design-review.md).
Remaining preimplementation planning: supported schema vocabulary/format policy;
profile-field-to-runtime-consumer mapping; individual stage/interface classification;
and resolution propagation through invocation and recovery. Acceptance does not fill
these gaps by assumption. Final file-level plans still follow impact/approval rules;
new contract deltas outside this accepted scope require a further scoped decision.

Other proposed ADRs and F3–F6/specialized designs are not accepted by this response.

Accepted scope: independent data-only contract distributions and metadata-only discovery;
exact offline references; shared schema tooling; existing ReportSpec for telemetry with
required quality evidence in typed results; one optional resolution snapshot in
PipelinePlan persisted through PipelineRecord.plan; capability-only selected domains;
single-capability operator implementations and legacy compatibility through existing owners.
The 2026-10-06 refinements govern over earlier P3/P5 alternatives. The pack-owned carrier
alternative in proposed ADR-0108 is superseded for this scope, not its conformance goals.
The authority crosswalk applies only to these accepted changes; locked source records and
training freeze history remain intact. Required-executor policy placement remains part of
the outstanding detailed invocation/profile design, not an implicitly accepted new field.


## Maintainer acceptance of detailed mappings — 2026-10-06

The maintainer explicitly stated "accepted the design" after reviewing the detailed
mapping update in [foundation implementation design review](../architecture-fix/foundation-implementation-design-review.md).
Acceptance now includes the specified bounded JSON Schema vocabulary (including format
exclusion), profile consumer/ownership mapping, stage/engine interface classification,
and proposed execution/recovery propagation direction. This supersedes earlier pending
acceptance statements for those specified decisions; historical text remains intact.

Exact additive DataflowSpec/OperationContext/activity fields, unresolved gateway/embedder
handoffs and per-kernel helper-versus-invocation choices were not fully specified in that
packet. Resolve them in the affected issue's scoped plan; do not treat this acceptance as
a choice of unspecified API fields or an architecture waiver. Existing issue impact gates
continue to apply to additional contract deltas. Preserve accepted scope without asking
for approval of the same decisions again.

This records design acceptance only. Installed closure, consumer equivalence, lifecycle,
serialization and recovery evidence remain required. No issue is closed and no production
implementation, commit, push or deployment is claimed or authorized by this record.
