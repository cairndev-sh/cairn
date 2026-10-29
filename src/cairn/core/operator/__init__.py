"""Operator package — §24.19 + §24.20 substrate.

Public surface:

  - :class:`Operation` — generic base class operator authors subclass.
  - :class:`OperationContext` — runtime context handed to ``Operation.run``.
  - :class:`OperatorRegistry` — name → Operation-class lookup, mirroring
    the existing :class:`ExecutorRegistry` shape (verified by the
    /verify-upstream check before this module was added).
  - :func:`register_operator` — class decorator that attaches metadata
    and registers the class with :data:`default_registry`.
  - Store protocols (:class:`ArtifactStore`, :class:`SecretStore`,
    :class:`MetricsLogger`, :class:`CostLedger`) and their Null
    implementations.

  - The contract's re-exports (ADR-0120 D7, #2453): ``resolve_connection``,
    ``as_ssh_params``, ``ArtifactRegistry``, ``build_artifact_store``,
    ``parse_dataset_ref``, ``current_workspace``, ``workspace_collection``,
    ``RedactSpan``, ``RedactionResult``, ``AuditEvent``, ``audit_emit``, the audit chain rule
    (``GENESIS_HASH``, ``canonical``, ``chain_hash``, ``verify_chain``; ADR-0156 D3),
    ``NotifyEvent``, ``NotifyResult``, ``get_settings``, ``getenv``, ``selected_backend``
    (ADR-0134 D3b), and the compute lifecycle ``compute/acquire`` / ``compute/release``
    call (ADR-0151 D6: ``load_box``, ``ec2_client``, ``describe_state``, ``stamp_lease``,
    ``start_instance``, ``stop_instance``, ``wait_port_open``, ``iso``, ``BILLING_STATES``,
    ``STOPPED_STATES``) — so an operator pack imports only this package.

Entry-points discovery (the ``cairn.operators`` group) lives in
:mod:`cairn.core.operator.loader` and is owned by task OP2 (#181).
This module defines the in-process side only.
"""

from __future__ import annotations

from cairn.core.operator.reports import (  # ADR-0103 — the reporting contract
    ReportSpec,
    Reporter,
    Series,
    UndeclaredReportError,
)
from cairn.core.operator.protocol import (
    ArtifactStore,
    CostLedger,
    MetricsLogger,
    NullArtifactStore,
    NullCostLedger,
    NullMetricsLogger,
    NullSecretStore,
    Operation,
    OperationContext,
    ResourceSpec,
    SecretStore,
)
from cairn.core.operator.binding import (
    ApprovalField,
    ArtifactsField,
    BindingError,
    BudgetField,
    ExecutionField,
    OperationBinding,
    PolicyField,
    ResourcesField,
    load_binding,
    load_bindings_from_dir,
)
from cairn.core.operator.binding_validate import (
    OperationBindingError,
    validate_operation_binding,
)
from cairn.core.operator.guard import (
    AMBIENT_CONTEXT_ATTRS,
    CapabilityNotDeclared,
    GuardedContext,
    guard_context,
)
from cairn.core.operator.loader import (
    ENTRY_POINT_GROUP,
    ensure_loaded,
    load_all_operators,
)
from cairn.core.operator.executor import (
    ExecutorPolicy,
    UnsupportedExecutorError,
    declared_executors,
    implemented_executors,
    is_implemented,
    pick_executor,
)
from cairn.core.operator.runtime import (
    BindingEvalError,
    invoke_operator,
    invoke_operator_inprocess,
    mock_mode,
    mock_mode_active,
    resolve_is_mocked,
    resolve_simple_binding,
    sandbox_isolated_active,
    sandbox_mode,
    sandbox_mode_active,
)
from cairn.core.operator.testing import run_operator_for_test
from cairn.core.operator.registry import (
    DuplicateOperatorError,
    OperatorRegistry,
    UnknownOperatorError,
    default_registry,
    register_operator,
)

# ADR-0120 D7 / #2453 (owner decision 2026-10-12, group A) — the public operator
# contract also carries these deployment-resolved helpers and event/record types, so an
# operator pack imports ONLY `cairn.core.operator`. Resolved lazily (PEP 562): the source
# modules import this package themselves, so a top-level re-import would cycle.
_CONTRACT_REEXPORTS: dict[str, tuple[str, str]] = {
    # connections — the deployment's resolved connection for a binding / profile key
    "resolve_connection": ("cairn.core.connections", "resolve_connection"),
    "as_ssh_params": ("cairn.core.connections", "as_ssh_params"),
    # the artifact REGISTRY (datasets, versions, promotion) — not ctx.artifacts' put/get store
    "ArtifactRegistry": ("cairn.core.storage.artifacts", "ArtifactStore"),
    "build_artifact_store": ("cairn.core.storage.artifacts", "build_artifact_store"),
    "parse_dataset_ref": ("cairn.core.storage.artifacts", "parse_dataset_ref"),
    # workspace identity
    "current_workspace": ("cairn.core.workspace", "current_workspace"),
    "workspace_collection": ("cairn.core.workspace", "workspace_collection"),
    # observability record types + the audit emitter (chokepoint: cairn.core.obs.audit)
    "RedactSpan": ("cairn.core.obs.redact", "RedactSpan"),
    "RedactionResult": ("cairn.core.obs.redact", "RedactionResult"),
    "AuditEvent": ("cairn.core.obs.audit", "AuditEvent"),
    "audit_emit": ("cairn.core.obs.audit", "audit_emit"),
    # ADR-0156 D3 — the audit hash-chain rule, one definition for the writer and every reader
    "GENESIS_HASH": ("cairn.core.obs.audit", "GENESIS_HASH"),
    "canonical": ("cairn.core.obs.audit", "canonical"),
    "chain_hash": ("cairn.core.obs.audit", "chain_hash"),
    "verify_chain": ("cairn.core.obs.audit", "verify_chain"),
    # notification event / result types
    "NotifyEvent": ("cairn.core.notifier", "NotifyEvent"),
    "NotifyResult": ("cairn.core.notifier", "NotifyResult"),
    # provider settings (memory / rag providers)
    "get_settings": ("cairn.core.provider_settings", "get_settings"),
    # ADR-0122 D2 (#2429) — the one lookup for a cairn variable: CAIRN_<NAME>, else OBS_<NAME>
    "getenv": ("cairn.core.settings", "getenv"),
    # ADR-0134 D3b (#2678) — the one backend-selection plane (profile → env → default) an
    # engine pack reads its configuration through, instead of importing backend_selection
    "selected_backend": ("cairn.core.backend_selection", "selected"),
    # ADR-0151 D6 (#201) — the compute lifecycle compute/acquire and compute/release call:
    # turning a `compute.ssh` box on and off stays ONE module (with `cairn compute sweep`),
    # and the operator pack reaches it through this contract, never by importing it
    "BILLING_STATES": ("cairn.core.compute_lifecycle", "BILLING_STATES"),
    "STOPPED_STATES": ("cairn.core.compute_lifecycle", "STOPPED_STATES"),
    "describe_state": ("cairn.core.compute_lifecycle", "describe_state"),
    "ec2_client": ("cairn.core.compute_lifecycle", "ec2_client"),
    "iso": ("cairn.core.compute_lifecycle", "iso"),
    "load_box": ("cairn.core.compute_lifecycle", "load_box"),
    "stamp_lease": ("cairn.core.compute_lifecycle", "stamp_lease"),
    "start_instance": ("cairn.core.compute_lifecycle", "start_instance"),
    "stop_instance": ("cairn.core.compute_lifecycle", "stop_instance"),
    "wait_port_open": ("cairn.core.compute_lifecycle", "wait_port_open"),
}


def __getattr__(name: str):
    try:
        module, attr = _CONTRACT_REEXPORTS[name]
    except KeyError:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}") from None
    import importlib  # noqa: PLC0415 — resolved on first use

    return getattr(importlib.import_module(module), attr)


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_CONTRACT_REEXPORTS))


__all__ = [
    # ADR-0120 D7 / #2453 — the contract's re-exports (lazy, see _CONTRACT_REEXPORTS)
    *sorted(_CONTRACT_REEXPORTS),
    # Protocol surfaces
    "Operation",
    "OperationContext",
    "ResourceSpec",
    # ADR-0103 — the reporting contract
    "ReportSpec",
    "Reporter",
    "Series",
    "UndeclaredReportError",
    # Store protocols + null impls
    "ArtifactStore",
    "SecretStore",
    "MetricsLogger",
    "CostLedger",
    "NullArtifactStore",
    "NullSecretStore",
    "NullMetricsLogger",
    "NullCostLedger",
    # Capability guard for lifted @step ops (#1254, ADR-0087 P2/D5)
    "AMBIENT_CONTEXT_ATTRS",
    "CapabilityNotDeclared",
    "GuardedContext",
    "guard_context",
    # Registry
    "OperatorRegistry",
    "default_registry",
    "register_operator",
    "UnknownOperatorError",
    "DuplicateOperatorError",
    # Loader
    "ENTRY_POINT_GROUP",
    "load_all_operators",
    "ensure_loaded",
    # Binding (OP3)
    "OperationBinding",
    "ResourcesField",
    "ArtifactsField",
    "BudgetField",
    "ApprovalField",
    "PolicyField",
    "ExecutionField",
    "BindingError",
    "load_binding",
    "load_bindings_from_dir",
    # Compile-time binding validation (SCH4)
    "OperationBindingError",
    "validate_operation_binding",
    # Runtime (OP3 minimal — OP4 expands)
    "BindingEvalError",
    "resolve_simple_binding",
    "invoke_operator_inprocess",
    "invoke_operator",
    "mock_mode",
    "mock_mode_active",
    "sandbox_mode",
    "sandbox_mode_active",
    "sandbox_isolated_active",
    "resolve_is_mocked",
    # Operator test harness (#1544) — drives the REAL invoke path
    "run_operator_for_test",
    # §Phase-2.7 executor dispatcher
    "ExecutorPolicy",
    "UnsupportedExecutorError",
    "pick_executor",
    "declared_executors",
    "is_implemented",
    "implemented_executors",
]
