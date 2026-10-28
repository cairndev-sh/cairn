"""Inprocess operation invocation runtime (OP4 #183 — full §24.20.6 grammar).

Bridges three things:

  1. :class:`OperationBinding` (§24.20.5) — what the author declared
  2. :class:`Operation` subclass + :class:`OperationContext` (§24.20.12/13)
  3. The graph state dict (LangGraph ``IncidentMachineState`` for now)

This module ships the inprocess invocation path. It implements the
bounded §24.20.6 binding grammar in full:

  - ``$state.<dotted.path>``                    — read graph state
  - ``$input.<dotted.path>``                    — read template input
  - ``$nodes.<name>.output.<dotted.path>``      — read prior node output
  - ``$artifacts.<name>``                       — read artifact handle
  - ``$env.<NAME>``                             — read deploy-time env var
  - ``$default(<binding>, <literal>)``          — fallback to literal
  - Literals (anything not a ``$``-prefixed string)

There is **no Jinja, no Python expressions, no method calls, no string
concat**. Forbidden forms surface as :class:`BindingEvalError` here and
as file:line errors in the doctor (OP7 #186 — not yet wired).

OP6 #185 replaces :func:`invoke_operator_inprocess` with profile-driven
routing that dispatches image-bound operators to external execution.
"""

from __future__ import annotations
from cairn.core.settings import getenv

import asyncio
import contextlib
import inspect
import logging
import os
import tempfile
from contextvars import ContextVar
from pathlib import Path
from typing import TYPE_CHECKING, Any, Type

from cairn.core.engine.expr import (  # the ONE expression language (#1118)
    ExprError as BindingEvalError,
    Scopes as _ExprScopes,
    resolve_value as _resolve_value,
)
from cairn.core.obs.telemetry import span  # per-operator trace span (#242)
from cairn.core.operator.binding import OperationBinding
from cairn.core.operator.protocol import Operation, OperationContext
from cairn.core.workspace import scoped_workspace  # tenancy scope for remote jobs (#239)

if TYPE_CHECKING:
    from cairn.core.operator.effective import ContractEnvelope

_log = logging.getLogger("cairn.operator.runtime")


# ─── mock mode (CLI `cairn run --mock`, §24.20.14) ───────────────────
#
# A run-scoped flag, carried in a ContextVar so it reaches every operation
# node without threading a parameter through the whole compile/run path. The
# CLI wraps a run in `with mock_mode():`; invoke_operator defaults is_mocked to
# this when the caller doesn't pass it explicitly, so operators that declare
# supports_mock take their `.mock()` path with no side effects.
_MOCK_MODE: ContextVar[bool] = ContextVar("cairn_mock_mode", default=False)

# ─── sandbox mode (Playground, ADR-0073 #836) ────────────────────────
#
# Distinct from full mock mode. A Playground *sandbox* run wants REAL metrics
# on a capped sample, so it must NOT stub readers/analysis — only write-class
# ops (s3 put, notify, registry, serve, train) get their `.mock()` path. The
# split is per-operator: an op runs LIVE in the sandbox iff it declares
# `side_effect_free = True`; everything else with `supports_mock` is stubbed
# (safe default — an unmarked op is treated as write-class). Full `--mock`
# (`mock_mode`) still stubs every `supports_mock` op regardless of this flag.
_SANDBOX_MODE: ContextVar[bool] = ContextVar("cairn_sandbox_mode", default=False)

# Whether the sandbox run's environment is the ISOLATED playground env (scratch
# backends + TTL). Only then may `sandbox_safe` cheap writes (artifact/report to
# the scratch bucket) run LIVE — in any other env they're stubbed, so a sandbox
# run can never write to prod infra (#844).
_SANDBOX_ISOLATED: ContextVar[bool] = ContextVar("cairn_sandbox_isolated", default=False)


def mock_mode_active() -> bool:
    return _MOCK_MODE.get()


def sandbox_mode_active() -> bool:
    return _SANDBOX_MODE.get()


def sandbox_isolated_active() -> bool:
    return _SANDBOX_ISOLATED.get()


@contextlib.contextmanager
def mock_mode(enabled: bool = True):
    """Activate operator mock mode for the duration of the block."""
    token = _MOCK_MODE.set(enabled)
    try:
        yield
    finally:
        _MOCK_MODE.reset(token)


@contextlib.contextmanager
def sandbox_mode(enabled: bool = True, isolated: bool = False):
    """Activate Playground sandbox mode for the block.

    ``isolated`` marks that the run's environment is the playground env (scratch
    backends + TTL), which lets ``sandbox_safe`` cheap writes run LIVE (#844).
    """
    token = _SANDBOX_MODE.set(enabled)
    iso_token = _SANDBOX_ISOLATED.set(enabled and isolated)
    try:
        yield
    finally:
        _SANDBOX_MODE.reset(token)
        _SANDBOX_ISOLATED.reset(iso_token)


def resolve_is_mocked(operator_cls: "Type[Operation]") -> bool:
    """Per-operator mock decision from the run-scoped mode ContextVars.

    - full ``mock_mode`` (``cairn run --mock``) → stub every operator (the
      ``.mock()`` path is only *taken* if the op declares ``supports_mock``);
    - Playground ``sandbox_mode`` (#836) → stub write-class ops, but run LIVE:
        * ``side_effect_free`` reads/analysis (env-independent, real metrics), and
        * ``sandbox_safe`` cheap writes IFF the env is the isolated playground
          env (real report to the scratch bucket; never prod) — #844;
    - neither → live.
    """
    if _MOCK_MODE.get():
        return True
    if _SANDBOX_MODE.get():
        if getattr(operator_cls, "side_effect_free", False):
            return False
        if getattr(operator_cls, "sandbox_safe", False) and _SANDBOX_ISOLATED.get():
            return False
        return True
    return False


# ─── §24.20.6 binding resolver (#1118: delegates to the ONE language) ─────
#
# The hand-rolled grammar that lived here (walk + $default splitter + scope
# dispatch, ~150 lines) moved to cairn.core.engine.expr — the single resolver
# every dialect now shares. This module keeps only the public names call sites
# and packs already depend on. BindingEvalError IS ExprError so existing
# except-clauses and tests keep working unchanged.


def resolve_simple_binding(
    expr: Any,
    *,
    state: dict[str, Any],
    template_input: dict[str, Any] | None = None,
    nodes: dict[str, Any] | None = None,
    artifacts: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
    use_os_env: bool = True,
) -> Any:
    """Evaluate ONE binding expression under the §24.20.6 grammar.

    Forms — see module docstring. Anything else fails LOUD; the doctor
    is the second line of defense.

    :param state: graph state dict (mandatory).
    :param template_input: optional ``$input.*`` source. If omitted,
        falls back to ``state["template_input"]``.
    :param nodes: optional ``$nodes.*`` source. If omitted, falls back
        to ``state["nodes"]``.
    :param artifacts: optional ``$artifacts.*`` source. If omitted,
        falls back to ``state["artifacts"]``.
    :param env: optional ``$env.*`` source (preferred over ``os.environ``).
    :param use_os_env: if ``True`` (default), fall back to ``os.environ``
        when ``env`` doesn't contain the requested name. Set ``False``
        in unit tests to force explicit env handling.

    :raises BindingEvalError: form is unsupported / path missing / literal
        unparseable.
    """
    return _resolve_value(
        expr,
        _ExprScopes(
            state=state,
            input=template_input,
            nodes=nodes,
            artifacts=artifacts,
            env=env,
            use_os_env=use_os_env,
        ),
        source="binding",
    )


# ─── Output-target writer (nested) ───────────────────────────────────────


def _apply_nested_patch(
    patch: dict[str, Any], dotted_key: str, value: Any
) -> None:
    """Set ``patch[a][b][c] = value`` for a ``$state.a.b.c`` target.

    Builds intermediate dicts as needed. The patch is then merged into
    LangGraph state by the runtime — shallow merge means top-level keys
    in the patch replace the corresponding state keys wholesale.
    Authors who need deep-merge semantics declare a reducer on the state
    schema (LangGraph mechanism, outside operator scope).
    """
    parts = dotted_key.split(".")
    node: dict[str, Any] = patch
    for part in parts[:-1]:
        existing = node.get(part)
        if not isinstance(existing, dict):
            existing = {}
            node[part] = existing
        node = existing
    node[parts[-1]] = value


def _apply_output_binding(
    op_name: str,
    binding: OperationBinding,
    out_dict: dict[str, Any],
) -> dict[str, Any]:
    """Map an operator's output dict into a ``$state.*`` patch.

    ``binding.output`` maps each operator-output field to a ``$state.<path>``
    target. Shared by the in-process and mcp_tool executors — both produce
    an ``out_dict`` (one from the Operation's return model, one from the
    validated MCP result) and lower it the same way."""
    state_patch: dict[str, Any] = {}
    for op_field, target_expr in binding.output.items():
        if not isinstance(target_expr, str) or not target_expr.startswith("$state."):
            raise BindingEvalError(
                f"operator {op_name!r} output {op_field!r}: target must be "
                f"$state.<path>, got {target_expr!r}"
            )
        target_key = target_expr[len("$state."):]
        if not target_key:
            raise BindingEvalError(
                f"operator {op_name!r} output {op_field!r}: target "
                f"{target_expr!r} is missing the state key"
            )
        if op_field not in out_dict:
            raise BindingEvalError(
                f"operator {op_name!r} output {op_field!r}: operator "
                f"output has no field {op_field!r} (available: "
                f"{sorted(out_dict.keys())})"
            )
        _apply_nested_patch(state_patch, target_key, out_dict[op_field])
    return state_patch


# ─── Inprocess invocation ─────────────────────────────────────────────────


async def _run_account() -> str:
    """The run's account: the owner of the run's workspace (the dispatch layer
    scopes :func:`~cairn.core.workspace.current_workspace` from the run record)."""
    from cairn.core.connections import run_account  # noqa: PLC0415 — lazy: keep the connections stack off the import path

    return await run_account()


async def _scoped_secrets(secret_keys: list[str] | None, *, is_mocked: bool,
                          account_id: str | None) -> Any:
    """``ctx.secrets`` for one invocation, in the run's account.

    #532 — stored connections reach an operator's DECLARED secrets. For the
    deployment (``default``) they are bridged into ``os.environ`` as before (an
    operator that reads ``os.environ`` — ``train/kaggle`` — or the env-backed
    SecretStore sees them). A tenant account's values are never written to the
    process environment, which every run shares: they back this invocation's
    ``ctx.secrets`` alone, and the deployment's bound SecretStore is not consulted."""
    from cairn.core.operator.secrets import build_scoped_secrets  # noqa: PLC0415
    from cairn.core.secrets_store import scoped_account  # noqa: PLC0415 — lazy: crypto off the import path

    if not secret_keys:
        return build_scoped_secrets([])
    acct = account_id if account_id is not None else await _run_account()
    values: dict[str, str] | None = None
    if not is_mocked:
        from cairn.core import connections  # noqa: PLC0415 — lazy, as above

        if scoped_account(acct) is None:
            await connections.inject_declared_secrets(secret_keys, account_id=acct)
        else:
            values = await connections.declared_secret_values(secret_keys, account_id=acct)
    return build_scoped_secrets(secret_keys, account_id=acct, values=values)


async def resolve_declared_compute(binding, *, op_name: str, is_mocked: bool,
                                   state, template_input, nodes, artifacts, env,
                                   account_id: str | None = None):
    """Resolve a declared ``compute_target`` to ``(connection, name)``, or
    ``(None, None)`` when none is declared.

    The name resolves in ``account_id`` (when omitted, the current run's account):
    a tenant run reaches only its own connections, the deployment only its own.

    Shared by the in-process and remote-job paths (#1718). It used to live only
    on the remote_job branch, gated on ``driver_kind == "remote_job"`` — but the
    real constraint is whether the driver can hand the operator a live object,
    and the docstring there says so: *"docker marshals I/O via files, so skip it
    there"*. ``in_process`` threads ``ctx`` straight into ``op.run(inp, ctx)``,
    so excluding it was incidental, not deliberate.

    The cost of that accident: ``serve/vllm`` declares
    ``executors = ['remote_job', 'in_process']`` but implements no submit/poll,
    so ``is_remote_job_operator`` is False and it ALWAYS dispatches in_process —
    where ``ctx.compute`` was never populated. Every production dispatch raised
    "serve/vllm needs a compute.ssh connection" before touching SSH: an operator
    that was complete and unreachable.
    """
    if is_mocked or not (binding.execution and binding.execution.compute_target):
        return None, None
    target: Any = binding.execution.compute_target
    if isinstance(target, str) and target.startswith("$"):
        target = resolve_simple_binding(
            {"_ct": target}, state=state, template_input=template_input,
            nodes=nodes, artifacts=artifacts, env=env,
        )["_ct"]
    if not target:
        return None, None
    # lazy: keep the connections/secret stack off the operator import path
    from cairn.core.connections import resolve_connection  # noqa: PLC0415
    conn = await resolve_connection(str(target), account_id=account_id)
    if conn is None:
        raise BindingEvalError(
            f"operator {op_name!r} compute_target {target!r} did not resolve to an "
            f"enabled connection — add it with `cairn connect`"
        )
    return conn, str(target)


async def invoke_operator_inprocess(
    binding: OperationBinding,
    operator_cls: Type[Operation],
    *,
    state: dict[str, Any],
    template_input: dict[str, Any] | None = None,
    nodes: dict[str, Any] | None = None,
    artifacts: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
    pipeline_id: str,
    node_name: str,
    is_mocked: bool = False,
    contract: "ContractEnvelope | None" = None,
) -> dict[str, Any]:
    """Run an operator inprocess and return a state-patch dict.

    Steps:

      1. Resolve ``binding.input`` via :func:`resolve_simple_binding`
         under the full §24.20.6 grammar.
      2. Validate the resolved dict against ``operator_cls.input_schema``
         (Pydantic, fail-loud).
      3. Build an :class:`OperationContext` (Null stores by default).
      4. Call ``op.run(inp, ctx)`` (or ``op.mock(...)`` if ``is_mocked``
         and the operator declares ``supports_mock``). The protocol
         declares ``run`` as sync but operators are free to override
         with ``async def run`` — if the call returns a coroutine, it's
         awaited here.
      5. Apply ``binding.output`` mapping. Targets MUST be ``$state.<path>``
         (flat or nested). Nested paths build the patch tree
         (``{"a": {"b": value}}``); LangGraph's reducer policy decides
         whether the merge replaces or deep-merges.
      6. Return the partial state-update dict for LangGraph to merge.
    """
    op_name = getattr(operator_cls, "name", operator_cls.__name__)

    # ── 1. Resolve inputs ────────────────────────────────────────────
    try:
        resolved_input = resolve_simple_binding(
            binding.input,
            state=state,
            template_input=template_input,
            nodes=nodes,
            artifacts=artifacts,
            env=env,
        )
    except BindingEvalError as e:
        raise BindingEvalError(
            f"operator {op_name!r} binding input resolution failed: {e}"
        ) from e

    ti = template_input if template_input is not None else state.get("template_input", {})
    # #1718 — an in_process operator that DECLARES a compute_target gets it,
    # through the existing ctx_overrides seam. `serve/vllm` is the case that
    # exposed the gap: it declares executors ['remote_job','in_process'] but
    # implements no submit/poll, so it always dispatches in_process — where
    # ctx.compute was never populated and every dispatch raised before SSH.
    _compute_conn, _ = await resolve_declared_compute(
        binding, op_name=op_name, is_mocked=bool(is_mocked), state=state,
        template_input=template_input, nodes=nodes, artifacts=artifacts, env=env,
    )
    out_dict = await invoke_operator_core(
        operator_cls,
        resolved_input,
        pipeline_id=pipeline_id,
        node_name=node_name,
        profile_config=state.get("operator_config"),
        workflow_config=state.get("workflow_config"),  # ADR-0070 — saved per-workflow layer
        run_overrides=(ti or {}).get("config_overrides"),
        secret_keys=binding.secrets,
        is_mocked=is_mocked,
        ctx_overrides={"compute": _compute_conn} if _compute_conn is not None else None,
        contract=contract,
    )

    # ── 5. Apply output binding ──────────────────────────────────────
    # binding.output maps operator-output-field → $state.<path>
    # e.g.   output: {logs: $state.logs}  means: state.logs = out.logs
    return _apply_output_binding(op_name, binding, out_dict)


#: ADR-0120 D7 (#2455) — the launch handle every operator invoked in the current
#: context receives as ``OperationContext.launch`` (``None`` outside a governed run).
_LAUNCH: ContextVar[Any] = ContextVar("cairn_launch_handle", default=None)


@contextlib.contextmanager
def launch_scope(handle: Any):
    """Bind ``handle`` as ``OperationContext.launch`` for every operator invoked in
    the block (ADR-0120 D7, #2455). The dispatch layer enters it around a template
    run and its resume, the way ``node_sink_scope`` carries the node sink: the
    contextvar propagates across ``await``, so the compiler's operation node needs
    no dispatch import. A pipeline task leg passes the handle explicitly through
    ``ctx_overrides``; an explicit override always wins over the scope."""
    token = _LAUNCH.set(handle)
    try:
        yield
    finally:
        _LAUNCH.reset(token)


async def invoke_operator_core(
    operator_cls: Type[Operation],
    resolved_input: dict[str, Any],
    *,
    pipeline_id: str,
    node_name: str,
    profile_config: dict[str, Any] | None = None,
    workflow_config: dict[str, Any] | None = None,
    run_overrides: dict[str, Any] | None = None,
    secret_keys: list[str] | None = None,
    is_mocked: bool = False,
    run_sync_in_thread: bool = False,
    ctx_overrides: dict[str, Any] | None = None,
    contract: "ContractEnvelope | None" = None,
    account_id: str | None = None,
) -> dict[str, Any]:
    """Everything AFTER binding resolution — the ONE invoke path (#1542).

    ``account_id`` is the run's account (when omitted, derived from the run
    context: :func:`cairn.core.connections.run_account`). Declared secrets are
    resolved in it — see :func:`_scoped_secrets`.

    #2241 B — ``contract`` (an opted-in operator's recorded shared contract, resolved
    by the caller) is checked three times here: identity against the running class
    (D5), input/configuration before the call (inside ``effective_parameters``),
    output after it (D3: a non-conforming result fails the run even though the
    effect happened). ``None`` = legacy, byte-identical to before.

    Config layering + the governance-strip, input validation, secret injection
    and scoping, context build, mock/shard/run. Extracted so the template
    operation node (:func:`invoke_operator_inprocess`) and the pipeline
    ``task:`` leg (``PipelineBridge._invoke_task_op``) run the SAME effective
    contract: before this, the task-leg path skipped config layering
    (ADR-0068), the #905 governance-strip, secret injection (#532), and
    sharding (#962) — the same operator had a weaker contract depending on
    which grammar invoked it, and an env-locked guardrail only actually
    governed template runs.

    ``run_sync_in_thread`` preserves each caller's blocking behavior: the
    bridge runs sync ops in a worker thread (a long op must not block its
    event loop); the template path keeps its historical inline call.

    ``ctx_overrides`` sets attributes on the built :class:`OperationContext`
    just before the call — the same late binding
    :func:`invoke_operator_job` already does for ``ctx.compute`` (#121). It is
    how :func:`cairn.core.operator.testing.run_operator_for_test` supplies a
    compute connection / artifact store without hand-rolling a context and
    skipping everything above (#1544). Only existing context attributes may be
    set; an unknown name fails LOUD rather than silently attaching a field the
    operator will never read.
    """
    op_name = getattr(operator_cls, "name", operator_cls.__name__)

    # ── config layering + the #905 strip + validation (ADR-0068/0070) ─
    # The ONE computation, shared with the launch preview (#2023): see
    # cairn.core.operator.effective. Nothing about the effective inputs or
    # config is worked out twice, so "what will run" and "what ran" cannot
    # disagree.
    from cairn.core.operator.effective import (  # noqa: PLC0415 — avoid import cycle (protocol→runtime)
        EffectiveParametersError,
        effective_parameters,
    )
    if contract is not None:
        contract.check_identity(operator_cls)

    try:
        eff = effective_parameters(
            operator_cls, resolved_input,
            profile_config=profile_config,
            workflow_config=workflow_config,
            run_overrides=run_overrides,
            contract=contract,
        )
    except EffectiveParametersError as e:
        raise BindingEvalError(str(e)) from e
    if eff.governance_dropped:
        _log.warning(
            "op %s: ignored run-supplied governance knob(s) %s — env-owned, "
            "set them via the profile [profile.config] layer (#905/ADR-0070)",
            op_name, eff.governance_dropped,
        )
    inp = eff.validated

    # ── build context ────────────────────────────────────────────────
    # Secrets are scoped to the declared `secrets:` keys (§24.20.13), backed
    # by the profile-bound `secrets.store` provider (NullSecretStore when
    # nothing is declared or no provider is bound).
    ctx = OperationContext(
        pipeline_id=pipeline_id,
        node_name=node_name,
        is_mocked=is_mocked,
        secrets=await _scoped_secrets(secret_keys, is_mocked=is_mocked, account_id=account_id),
        logger=_log,
    )
    _attach_reporter(ctx, operator_cls, run_id=pipeline_id)  # ADR-0103 — before overrides, so a test sink wins
    for attr, value in (ctx_overrides or {}).items():
        if not hasattr(ctx, attr):
            raise BindingEvalError(
                f"operator {op_name!r}: ctx_overrides names {attr!r}, which is not an "
                f"OperationContext attribute (§24.20.13 bounds the context surface)"
            )
        setattr(ctx, attr, value)
    if ctx.launch is None:
        ctx.launch = _LAUNCH.get()  # ADR-0120 D7 (#2455): the dispatch layer's scoped handle, if any

    # ── ADR-0103 D3: the RUNTIME records what this step runs with ────
    # Effective inputs + config after every default, with each config
    # field's scope. No operator writes this; none can forget it. This is
    # what preview, clone-from-run, diff and the Metrics tab's Parameters read.
    _record_parameters(ctx, eff)

    # ── invoke ───────────────────────────────────────────────────────
    op = operator_cls()
    try:
        out = await _run_operator(op, op_name, inp, ctx, operator_cls, is_mocked, run_sync_in_thread)
    finally:
        await ctx.release_handles()  # #2453 (B): store handles the context opened for this run

    out_dict = out.model_dump() if hasattr(out, "model_dump") else dict(out or {})
    _conform_output(contract, op_name, out if hasattr(out, "model_dump") else out_dict)
    return out_dict


async def _run_operator(op, op_name, inp, ctx, operator_cls, is_mocked, run_sync_in_thread):
    """Step 4 of :func:`invoke_operator_core`: the mock / sharded / threaded / inline call."""
    if is_mocked and getattr(operator_cls, "supports_mock", False):
        out = op.mock(inp, ctx)
    else:
        # #962 — a Shardable dataset operator over the row threshold runs as a
        # sharded, budgeted, nice'd process-pool job instead of a single inline
        # (event-loop-blocking) call. Below threshold, or non-shardable, it
        # takes the normal path with zero overhead. run_sharded is blocking
        # (spawns children), so it runs in a thread — the event loop stays
        # free to service other runs + the console (the core #962 fix).
        from cairn.core.operator.sharded import run_sharded, should_shard
        do_shard, total = should_shard(op, inp)
        if do_shard:
            _log.info("operator %s: sharded batch path (%d rows)", op_name, total)
            out = await asyncio.get_event_loop().run_in_executor(
                None, run_sharded, op, inp, total
            )
        elif run_sync_in_thread:
            out = await asyncio.to_thread(op.run, inp, ctx)
        else:
            out = op.run(inp, ctx)
    if inspect.iscoroutine(out):
        out = await out
    return out


def _bytes_as_text(value: Any) -> Any:
    """Each ``bytes`` value as the string the JSON projection carries: its UTF-8 text when it
    decodes (what Pydantic's JSON mode gives, and what the shared schemas' ``string`` says), its
    base64 text when it does not (#2564)."""
    if isinstance(value, (bytes, bytearray)):
        try:
            return bytes(value).decode("utf-8")
        except UnicodeDecodeError:
            import base64  # noqa: PLC0415 — only a non-UTF-8 output pays for it

            return base64.b64encode(bytes(value)).decode("ascii")
    if isinstance(value, dict):
        return {k: _bytes_as_text(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_bytes_as_text(v) for v in value]
    return value


def _json_projection(out: Any) -> Any:
    """The JSON form of a MODEL output, as the shared output schema judges it (datetime →
    string, bytes → string, …) — the ONE projection the conformance check sees (#2564). Any
    other output (a plain dict, a durable job's recorded result) is judged as it is.

    Pydantic's JSON mode decodes ``bytes`` as UTF-8 and raises on anything else, so a contracted
    ``storage/get`` with ``decode=False`` on binary content failed a leg that worked without a
    contract. Such bytes project as base64 text instead; a JSON schema has no bytes type, and
    the check only needs the string. The projection is for the check alone: the operator's own
    output (``model_dump()``) is returned unchanged, bytes and all."""
    if not hasattr(out, "model_dump"):  # a model, as the seam has always told one apart
        return out
    try:
        return out.model_dump(mode="json")
    except UnicodeDecodeError:
        from pydantic_core import to_jsonable_python  # noqa: PLC0415 — the non-UTF-8 path only

        return to_jsonable_python(_bytes_as_text(out.model_dump()))


def _conform_output(contract: "ContractEnvelope | None", op_name: str, out: Any) -> None:
    """#2241 B (D3) — a non-conforming result fails the run even though the effect
    happened; evidence stays where the operator put it, nothing is rolled back.
    ``out`` is the output model (judged in its JSON projection) or a dict (judged as it is)."""
    if contract is None:
        return
    problems = contract.problems("output", _json_projection(out))
    if problems:
        from cairn.core.operator.effective import ContractConformanceError  # noqa: PLC0415 — cycle, as above
        raise ContractConformanceError(
            f"operator {op_name!r} output does not conform to {contract.capability_ref}: " + "; ".join(problems))


# ─── §24.3 mcp_tool invocation ───────────────────────────────────────


async def invoke_operator_mcp_tool(
    binding: OperationBinding,
    operator_cls: Type[Operation],
    *,
    state: dict[str, Any],
    template_input: dict[str, Any] | None = None,
    nodes: dict[str, Any] | None = None,
    artifacts: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
    pipeline_id: str,
    node_name: str,
    is_mocked: bool = False,
) -> dict[str, Any]:
    """Run an mcp_tool-executed operator: dispatch a manifest-declared MCP
    tool through the substrate MCP client instead of the operator's Python.

    The operator's ``run()`` is never called — the operator is a *typed
    wrapper* around one MCP tool. The flow mirrors the in-process path
    except the invocation step:

      1. Resolve ``binding.input`` (full §24.20.6 grammar).
      2. Validate against ``operator_cls.input_schema`` — the resolved dict
         is the tool's ``args``.
      3. Look up the operator's ``[operator.<n>].mcp`` server/tool from the
         registry metadata.
      4. Call ``McpClient.call_tool(server, tool, args)`` (the same
         substrate client ``mcp/call`` uses).
      5. Validate the MCP result against ``operator_cls.output_schema``.
      6. Apply ``binding.output`` mapping → state patch.

    ``is_mocked`` short-circuits the network call entirely and returns an
    empty state patch: a generic echo can't be schema-shaped to the
    operator's ``output_schema``, so offline runs simply skip the call and
    the output binding (no state write). Operators that need a meaningful
    offline result should ship an in_process ``mock()`` and declare both
    executors.
    """
    op_name = getattr(operator_cls, "name", operator_cls.__name__)

    # ── 1. Resolve inputs ────────────────────────────────────────────
    try:
        resolved_input = resolve_simple_binding(
            binding.input,
            state=state,
            template_input=template_input,
            nodes=nodes,
            artifacts=artifacts,
            env=env,
        )
    except BindingEvalError as e:
        raise BindingEvalError(
            f"operator {op_name!r} binding input resolution failed: {e}"
        ) from e

    # ── 2. Validate input → becomes the tool args ────────────────────
    try:
        inp = operator_cls.input_schema.model_validate(resolved_input)
    except Exception as e:
        raise BindingEvalError(
            f"operator {op_name!r} input failed Pydantic validation: {e}"
        ) from e
    args = inp.model_dump() if hasattr(inp, "model_dump") else dict(resolved_input)

    # ── 3. Resolve the MCP server/tool from manifest metadata ────────
    from cairn.core.operator.registry import default_registry

    md = default_registry.get_metadata(op_name)
    server = getattr(md, "mcp_server", None) if md else None
    tool = getattr(md, "mcp_tool", None) if md else None
    if not (server and tool):
        raise BindingEvalError(
            f"operator {op_name!r} is dispatched via mcp_tool but has no "
            f"[operator.{op_name!r}.mcp] server/tool in its manifest — the "
            f"dispatcher has no MCP tool to call. (Manifest validation should "
            f"have caught this; the operator may have been registered without "
            f"its pack.toml.)"
        )

    # ── 4. Invoke through the substrate MCP client ───────────────────
    if is_mocked:
        _log.info(
            "operator %r mcp_tool mocked — skipping %s:%s call and output binding",
            op_name, server, tool,
        )
        return {}

    from cairn.services.mcp.client import get_mcp_client

    result = await get_mcp_client().call_tool(server, tool, args)

    # ── 5. Validate the MCP result against output_schema ─────────────
    try:
        out = operator_cls.output_schema.model_validate(result)
    except Exception as e:
        raise BindingEvalError(
            f"operator {op_name!r} mcp_tool result failed output_schema "
            f"validation (tool {server}:{tool}): {e}"
        ) from e
    out_dict = out.model_dump() if hasattr(out, "model_dump") else dict(out)

    # ── 6. Apply output binding ──────────────────────────────────────
    return _apply_output_binding(op_name, binding, out_dict)


# ─── §Phase-5 job executors (docker / remote_job) ────────────────────


async def invoke_operator_job(
    binding: OperationBinding,
    operator_cls: Type[Operation],
    *,
    driver_kind: str,
    state: dict[str, Any],
    template_input: dict[str, Any] | None = None,
    nodes: dict[str, Any] | None = None,
    artifacts: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
    pipeline_id: str,
    node_name: str,
    is_mocked: bool = False,
    poll_interval: float = 2.0,
    max_polls: int = 0,
    contract: "ContractEnvelope | None" = None,
    dataflow_contracts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run an operator through the container/remote job lifecycle (issue #7).

    Same input-resolution + output-mapping as :func:`invoke_operator_inprocess`;
    the only difference is step 4 — instead of ``op.run()``, the work is
    dispatched to a driver (``remote_job``: the operator's submit/poll;
    ``docker``: its declared image) and polled to completion. Mock mode bypasses
    the job entirely (``op.mock``), so tests/`--mock` never touch a daemon/provider.
    """
    from cairn.core.operator.jobs import (
        DockerJobDriver,
        RemoteJobDriver,
        run_job_lifecycle,
    )
    op_name = getattr(operator_cls, "name", operator_cls.__name__)

    try:
        resolved_input = resolve_simple_binding(
            binding.input, state=state, template_input=template_input,
            nodes=nodes, artifacts=artifacts, env=env,
        )
    except BindingEvalError as e:
        raise BindingEvalError(f"operator {op_name!r} binding input resolution failed: {e}") from e
    if contract is not None:
        # #2345 — the same checks the in_process seam runs, BEFORE anything is submitted:
        # identity, then the shared input + configuration (this path layers no config,
        # so the configuration is the operator's own defaults).
        from cairn.core.operator.effective import contract_input_problems  # noqa: PLC0415 — cycle, as above

        contract.check_identity(operator_cls)
        problems = contract_input_problems(contract, operator_cls, resolved_input, {})
        if problems:
            from cairn.core.operator.effective import ContractConformanceError  # noqa: PLC0415
            raise ContractConformanceError(
                f"operator {op_name!r} does not conform to {contract.capability_ref}: " + "; ".join(problems))
    try:
        inp = operator_cls.input_schema.model_validate(resolved_input)
    except Exception as e:
        raise BindingEvalError(f"operator {op_name!r} input failed Pydantic validation: {e}") from e

    # #532 — declared secrets before the remote-job operator submits (train/kaggle
    # etc. read os.environ / ~/.kaggle for a deployment run), in the run's account.
    ctx = OperationContext(
        pipeline_id=pipeline_id, node_name=node_name, is_mocked=is_mocked,
        secrets=await _scoped_secrets(binding.secrets, is_mocked=is_mocked, account_id=None),
        logger=_log,
        dataflow_contracts=dataflow_contracts,   # ADR-0118 D4 — the flow door's admitted stage contracts
    )
    _attach_reporter(ctx, operator_cls, run_id=pipeline_id)  # ADR-0103
    # The job path does no config layering (the operator submits to a box), so
    # its parameters block is the validated input alone — recorded all the same.
    _record_parameters(ctx, _job_parameters(operator_cls, inp))
    op = operator_cls()
    # The RESOLVED connection name (e.g. "ec2b") — captured so the durable record
    # can store it for the control-loop poll, NOT the raw "$input.compute_target"
    # binding expression (which resolve_connection can't look up). #536 e2e fix.
    resolved_compute_target: Any = None

    # #121: resolve a declared compute_target into ctx.compute so a remote-job
    # operator's submit/poll can reach the chosen backend (RunPod, Vast.ai, …).
    # Resolved like secrets — a deployment-resolved handle — supporting a literal
    # connection name or a $-binding into state/input. Only the remote_job driver
    # threads ctx to the operator; docker marshals I/O via files, so skip it there.
    # docker is still excluded: it marshals I/O through files and cannot receive
    # a live connection object. Shared with the in_process path (#1718).
    if driver_kind == "remote_job":
        _conn, resolved_compute_target = await resolve_declared_compute(
            binding, op_name=op_name, is_mocked=bool(is_mocked),
            state=state, template_input=template_input, nodes=nodes,
            artifacts=artifacts, env=env,
        )
        if _conn is not None:
            ctx.compute = _conn

    if is_mocked and getattr(operator_cls, "supports_mock", False):
        result: Any = op.mock(inp, ctx)
        if inspect.iscoroutine(result):
            result = await result
    else:
        if driver_kind == "remote_job" and _remote_job_durable():
            # Durable ingress (ADR-0038, #127): register the job + pause the run
            # (interrupt); a separate control worker drives it and resumes via
            # RunDispatcher. Default off → the inline poll below (unchanged).
            return await _invoke_remote_job_durable(
                op, inp, ctx, binding, op_name,
                run_id=str(pipeline_id), node_name=node_name, resolved_input=resolved_input,
                compute_target=resolved_compute_target, contract=contract,
            )
        if driver_kind == "remote_job":
            # ADR-0082 P0 (#1072): pin dataset_hash + image + resolved spec on
            # the run BEFORE dispatch (inline path — the driver submits inside
            # run_job_lifecycle, so the image digest comes from the compute
            # connection's config here). Best-effort by contract.
            from cairn.core.operator.pins import record_train_pins  # noqa: PLC0415 — keep httpx off the operator import path
            _pin_image = ""
            try:
                _compute = getattr(ctx, "compute", None)
                _pin_image = str((getattr(_compute, "config", None) or {}).get("image", "") or "")
            except Exception:  # noqa: BLE001 — image is an optional pin input
                _pin_image = ""
            await record_train_pins(
                str(pipeline_id), node_name, operator=op_name,
                resolved_input=resolved_input, image=_pin_image,
            )
            driver: Any = RemoteJobDriver(op, inp, ctx)
        elif driver_kind == "docker":
            io_dir = tempfile.mkdtemp(prefix="cairn-job-")
            (Path(io_dir) / "in.json").write_text(inp.model_dump_json(), encoding="utf-8")
            driver = DockerJobDriver(
                image=getattr(operator_cls, "image"),
                io_dir=io_dir,
                entrypoint=getattr(operator_cls, "container_entrypoint", None),
            )
        else:
            raise ValueError(f"unknown driver_kind {driver_kind!r}")
        # ADR-0082 D8a (#1076): the inline poll loop bridges stage progress in
        # the poll metrics into `stage` run events (the durable path does the
        # same in job_control.drive_once) — one timeline, either path.
        from cairn.core.operator.stage_events import emit_stage_event  # noqa: PLC0415 — keep the storage stack off the operator import path
        _stage_watermark: list[Any] = [None]

        async def _on_running(handle: Any, status: Any) -> None:
            _stage_watermark[0] = await emit_stage_event(
                str(pipeline_id), node_name=node_name,
                job_id=str(getattr(handle, "job_id", "") or ""),
                metrics=dict(getattr(status, "metrics", None) or {}),
                last_done=_stage_watermark[0],
            )

        result = await run_job_lifecycle(
            driver, poll_interval=poll_interval, max_polls=max_polls,
            on_running=_on_running,
        )

    # Output: a model instance (remote op poll) → model_dump; a raw dict
    # (docker out.json) → validate against output_schema first.
    if hasattr(result, "model_dump"):
        out_dict = result.model_dump()
        _conform_output(contract, op_name, result)   # #2345
    else:
        model = operator_cls.output_schema.model_validate(result or {})
        out_dict = model.model_dump()
        _conform_output(contract, op_name, model)    # #2345
    return _apply_output_binding(op_name, binding, out_dict)


def _remote_job_durable() -> bool:
    """Opt-in (ADR-0038, #127): submit + pause + worker-driven, instead of the
    inline poll loop. Off by default → no behavior change."""
    return getenv("CAIRN_REMOTE_JOB_DURABLE", "").strip().lower() in ("1", "true", "yes", "on")


def _worker_pool(target: Any) -> "str | None":
    """The pool name when ``target`` names a pull-worker pool, else ``None``.

    ``worker:gpu`` -> ``"gpu"``; ``worker:`` -> ``""`` (any worker); a
    connection name -> ``None`` (push it, as before). The prefix is
    :data:`cairn.core.pull_worker_door.WORKER_TARGET_PREFIX`, imported lazily so
    this module keeps its import surface (the door reads the job store).
    """
    if not isinstance(target, str) or not target:
        return None
    from cairn.core.pull_worker_door import WORKER_TARGET_PREFIX  # noqa: PLC0415 — cycle: door reads the store

    if not target.startswith(WORKER_TARGET_PREFIX):
        return None
    return target[len(WORKER_TARGET_PREFIX):].strip()


async def _invoke_remote_job_durable(
    op: Any, inp: Any, ctx: Any, binding: OperationBinding, op_name: str,
    *, run_id: str, node_name: str, resolved_input: dict[str, Any],
    compute_target: Any = None, contract: "ContractEnvelope | None" = None,
) -> dict[str, Any]:
    """Durable remote-job step (ADR-0038, #127): idempotently submit + register
    the job, then pause the run via ``interrupt()`` until the control worker
    resumes it. Idempotent because LangGraph **re-runs the node from the top** on
    resume — so we must check the store BEFORE re-submitting (submit is
    side-effecting). On the resume re-run, the terminal record returns/raises."""
    from cairn.core.operator.job_store import (
        CANCELLED, DONE, FAILED, PULL_BACKEND, RemoteJobRecord, get_remote_job_store,
    )
    from cairn.core.operator.jobs import JobCancelled, JobFailed

    # #564 — the durable record id is keyed on run_id (``run_id:node``); the control
    # loop resumes by that same run_id. An empty/sentinel run_id (e.g. the compiler's
    # ``"p?"`` fallback, or an unthreaded id under Temporal) collides across runs onto
    # ``:node``/``p?:node`` — one run's failed record then short-circuits the next.
    # #563 fixed the Temporal-side injection; this guard makes a still-missing id fail
    # LOUD at submit time instead of silently corrupting the store. run_id is REQUIRED.
    if not run_id or run_id in ("p?", "None"):
        raise BindingEvalError(
            f"remote-job operator {op_name!r} has no run identity (run_id={run_id!r}); "
            "the durable record + control-loop resume key on it. This is a runtime "
            "state-threading bug (run_id must reach the node) — see #564."
        )

    store = get_remote_job_store()
    rid = f"{run_id}:{node_name}"
    rec = await store.get(rid)
    if rec is not None and rec.status == DONE:
        # #2345 — the resumed node still holds its recorded contract: judge the result
        _conform_output(contract, op_name, rec.result or {})
        return _apply_output_binding(op_name, binding, rec.result or {})
    if rec is not None and rec.status == FAILED:
        raise JobFailed(rec.error or f"remote job {rid} failed")
    if rec is not None and rec.status == CANCELLED:
        # ADR-0151 D9 — a cancelled job is terminal: end the node, never re-pause on it.
        raise JobCancelled(rec.error or f"remote job {rid} was cancelled")
    if rec is None:
        target = (compute_target if compute_target is not None
                  else (binding.execution.compute_target if binding.execution else None))
        # ADR-0104 D2 (#565/#2085) — the PULL lane. A ``worker:<pool>`` target is
        # not a machine the control plane pushes into; it is "whatever machine
        # claims this". So the record is REGISTERED and nothing is dispatched:
        # `op.submit` is the push, and calling it here would send the work to a
        # provider AND leave a job on the queue. The run then pauses on the same
        # interrupt as a pushed job, and the worker's heartbeat drives the resume
        # through the very same control loop (`build_default_poll`).
        # The job is the paused run's (#239): its workspace is the one in scope (the
        # run's, scoped from its record). With auth on and nothing in scope this
        # refuses before anything is registered or dispatched, rather than
        # stamping the process default.
        workspace_id = scoped_workspace()
        pool = _worker_pool(target)
        pinned_image = ""
        if pool is not None:
            pinned_image = getenv("CAIRN_TRAIN_IMAGE", "").strip()
            await store.register(
                RemoteJobRecord(
                    run_id=run_id, node_name=node_name,
                    workspace_id=workspace_id,
                    provider=getattr(op, "name", op_name),
                    backend=PULL_BACKEND, compute_target=target,
                    input=dict(resolved_input),
                    # The pool IS the capability a worker offers (`--capabilities
                    # pool=<name>`); an unnamed pool (`worker:`) takes any worker.
                    requires=({"pool": pool} if pool else {}),
                    # Empty is legal: `cairn worker --pull --image …` supplies it,
                    # and the worker refuses a job with neither, naming both fixes.
                    image=getenv("CAIRN_TRAIN_IMAGE", "").strip(),
                )
            )
        else:
            handle = await op.submit(inp, ctx)  # dispatch — guarded by the get() above
            pinned_image = (str(handle.meta.get("image", "") or "")
                            if isinstance(handle.meta, dict) else "")
            await store.register(
                RemoteJobRecord(
                    run_id=run_id, node_name=node_name,
                    workspace_id=workspace_id,  # inherit the paused run's workspace (#239)
                    provider=getattr(op, "name", op_name),
                    job_id=handle.job_id, backend=handle.backend, job_meta=dict(handle.meta),
                    compute_target=target,
                    input=dict(resolved_input),
                )
            )
        # ADR-0082 P0 (#1072): pin dataset_hash + image(+digest) + resolved
        # spec on the run at SUBMIT time — inside the rec-is-None branch, so
        # the resume re-run never re-pins. Best-effort by contract.
        from cairn.core.operator.pins import record_train_pins  # noqa: PLC0415 — keep httpx off the operator import path
        # `pinned_image` is the pushed handle's image, or — on the pull lane —
        # the image the claiming worker will run. Either way it is what executed.
        await record_train_pins(
            run_id, node_name, operator=op_name,
            resolved_input=resolved_input, image=pinned_image,
        )
    # Active → pause. On resume the node re-runs from the top and hits the
    # terminal-record branch above; nothing side-effecting runs before interrupt.
    from langgraph.types import interrupt
    # ADR-0065: ``kind`` lets the runtimes surface this as a RUNNING remote job, not
    # a human-approval gate. ``remote_job`` kept for interrupt_kind back-compat.
    interrupt({"kind": "remote_job", "remote_job": True, "run_id": run_id, "node": node_name})
    return _apply_output_binding(op_name, binding, {})  # safety — not reached on the first pass


# ─── §Phase-2.7 executor dispatcher ──────────────────────────────────


async def invoke_operator(
    binding: OperationBinding,
    operator_cls: Type[Operation],
    *,
    state: dict[str, Any],
    template_input: dict[str, Any] | None = None,
    nodes: dict[str, Any] | None = None,
    artifacts: dict[str, Any] | None = None,
    env: dict[str, str] | None = None,
    pipeline_id: str,
    node_name: str,
    is_mocked: bool | None = None,
    executor_policy: "ExecutorPolicy | None" = None,
    contract: "ContractEnvelope | None" = None,
    dataflow_contracts: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run an operator through the picked executor.

    ``is_mocked`` defaults to the run-scoped mock-mode flag
    (:func:`mock_mode_active`, set by ``cairn run --mock``) when the caller
    doesn't pass it explicitly — so the compiled graph's operation nodes honor
    mock mode without threading the flag through compile.

    This is the public entry point that callers (the engine compiler,
    test fixtures) should use. It consults
    :func:`cairn.core.operator.executor.pick_executor` to decide
    *which* executor to dispatch through, then forwards to the matching
    implementation. For Phase 2.7 only ``in_process`` lands here; the
    other six locked-enum executors are stubbed and surface as
    :class:`UnsupportedExecutorError`.

    All keyword arguments are passed through to
    :func:`invoke_operator_inprocess` unchanged — the dispatcher does
    not transform the call shape, only the implementation it routes to.

    Migration from :func:`invoke_operator_inprocess`: callers that
    explicitly want in-process invocation (e.g. tests that bypass the
    picker for setup/teardown isolation) can still call the lower-level
    function directly. New call sites should use ``invoke_operator()``.
    """
    # Imported here to avoid the executor module pulling in the runtime
    # module at module load (the executor module imports Operation +
    # registry, which both load fine eagerly; runtime imports the
    # executor module lazily so the dispatcher stays a thin overlay).
    from cairn.core.operator.executor import (
        UnsupportedExecutorError,
        pick_executor,
    )

    if is_mocked is None:
        # Per-operator: full mock stubs all; sandbox stubs only write-class
        # ops (side-effect-free reads/analysis run LIVE for real metrics, #836).
        is_mocked = resolve_is_mocked(operator_cls)

    # ── Approval chokepoint (ADR-0034, P1) ───────────────────────────
    # The compiled operation-node fn catches the NeedsApprovalError, pauses via
    # LangGraph interrupt(), and re-invokes with the approval recorded.
    #
    # The check itself lives in `enforce_gate` because this is not the only
    # dispatch path: the pipeline bridge's `task:` legs call it too. It was an
    # inline block here until #1402, when a second path reproduced everything
    # around it and silently skipped it.
    from cairn.core.approval.enforce import enforce_gate  # noqa: PLC0415 - cycle: approval → operator

    enforce_gate(
        operator_cls, binding,
        state=state, node_name=node_name,
        run_id=str(pipeline_id), is_mocked=bool(is_mocked),
    )

    executor = pick_executor(operator_cls, policy=executor_policy)
    op_label = getattr(operator_cls, "name", None) or operator_cls.__name__

    # Per-operator span (#242) — one span per operator call, nested under the
    # run span via OTel current-span context. ``run_id`` == ``pipeline_id``.
    with span("cairn.operator", {
        "operator": op_label, "node": node_name, "executor": executor,
        "mocked": bool(is_mocked), "run_id": str(pipeline_id),
    }):
        if executor == "in_process":
            return await invoke_operator_inprocess(
                binding,
                operator_cls,
                state=state,
                template_input=template_input,
                nodes=nodes,
                artifacts=artifacts,
                env=env,
                pipeline_id=pipeline_id,
                node_name=node_name,
                contract=contract,
                is_mocked=is_mocked,
            )

        if executor == "mcp_tool":
            return await invoke_operator_mcp_tool(
                binding,
                operator_cls,
                state=state,
                template_input=template_input,
                nodes=nodes,
                artifacts=artifacts,
                env=env,
                pipeline_id=pipeline_id,
                node_name=node_name,
                is_mocked=is_mocked,
            )

        # Job executors (issue #7): remote_job needs the operator to implement the
        # submit/poll protocol; docker needs a declared image. If the operator
        # declared the executor but hasn't adopted it, degrade to in-process — its
        # run() still does the work. Purely additive: in_process operators unchanged.
        if executor in ("remote_job", "docker"):
            from cairn.core.operator.jobs import is_remote_job_operator

            usable = (
                (executor == "remote_job" and is_remote_job_operator(operator_cls))
                or (executor == "docker" and getattr(operator_cls, "image", None))
            )
            if usable:
                return await invoke_operator_job(
                    binding, operator_cls, driver_kind=executor,
                    state=state, template_input=template_input, nodes=nodes,
                    artifacts=artifacts, env=env, pipeline_id=pipeline_id,
                    node_name=node_name, is_mocked=is_mocked, contract=contract,
                    dataflow_contracts=dataflow_contracts,
                )
            return await invoke_operator_inprocess(
                binding, operator_cls,
                state=state, template_input=template_input, nodes=nodes,
                artifacts=artifacts, env=env, pipeline_id=pipeline_id,
                node_name=node_name,
                contract=contract, is_mocked=is_mocked,
            )

        # Should never hit this: pick_executor returns only implemented
        # names, and the only implemented name today is in_process. The
        # branch is here so when a future PR adds a new executor to
        # _IMPLEMENTED_EXECUTORS without updating this dispatch table, the
        # error names the gap loudly.
        raise UnsupportedExecutorError(
            f"executor {executor!r} is in the implemented set but no "
            f"dispatch branch in invoke_operator() handles it — "
            f"cairn/core/operator/runtime.py needs an implementation."
        )


__all__ = [
    "BindingEvalError",
    "resolve_simple_binding",
    "invoke_operator_inprocess",
    "invoke_operator_mcp_tool",
    "invoke_operator_job",
    "invoke_operator",
]


# Late type annotation — keeps the dataclass import out of the module's
# top-level circular dance.
from cairn.core.operator.executor import ExecutorPolicy  # noqa: E402


# ─── ADR-0103 (#2035): the reporting contract, wired by the runtime ─────────

def _attach_reporter(ctx: OperationContext, operator_cls: Type[Operation], *, run_id: str) -> None:
    """Give the operator its typed reporter, bound to this run's event store.

    Strict (raise on an undeclared name) only when ``CAIRN_REPORT_STRICT`` is
    set — tests and ``cairn validate``. A running job drops-with-a-warning.
    Never raises: a reporter that cannot be built falls back to the null one.
    """
    from cairn.core.operator.reports import (  # noqa: PLC0415 — keep reports importable without the runtime
        Reporter, ReportSpec, StorageSink, strict_reports_enabled,
    )
    try:
        spec = getattr(operator_cls, "reports", None) or ReportSpec.NONE
        ctx.report = Reporter(
            spec=spec, node=ctx.node_name, sink=StorageSink.for_run(run_id),
            strict=strict_reports_enabled(),
        )
    except Exception:  # noqa: BLE001 — reporting must never fail the operator
        _log.warning("could not attach a reporter for %s; using the null reporter", ctx.node_name, exc_info=True)
        ctx.report = Reporter.null()


def _job_parameters(operator_cls: Type[Operation], inp: Any) -> Any:
    """The parameters block for the remote-job path, which does no config layering.

    Built through the same dataclass the in-process path uses, so the two
    records have one shape and one producer.
    """
    from cairn.core.operator.effective import EffectiveParameters, redact  # noqa: PLC0415 — cycle: protocol → runtime
    try:
        inputs = inp.model_dump(mode="json") if hasattr(inp, "model_dump") else dict(inp or {})
    except Exception:  # noqa: BLE001 — a non-serialisable input is recorded by repr, not skipped
        inputs = {"_repr": repr(inp)[:2000]}
    return EffectiveParameters(
        operator=getattr(operator_cls, "name", operator_cls.__name__),
        version=str(getattr(operator_cls, "version", "") or ""),
        risk=str(getattr(operator_cls, "risk", "") or ""),
        inputs=redact(inputs), validated=inp,
    )


def _record_parameters(ctx: OperationContext, eff: Any) -> None:
    """Write the step's ``parameters`` run event through the reporter's sink.

    Routed through the same sink as the operator's own reports so a test
    sink captures it and production appends it to the run store. Never raises.
    The payload is :meth:`EffectiveParameters.as_payload` — the same body the
    launch preview returns per step (#2023), so a preview and the record it
    predicts cannot drift.
    """
    from cairn.core.operator.reports import PARAMETERS_EVENT_TYPE  # noqa: PLC0415 — same reason as above
    try:
        ctx.report._emit(PARAMETERS_EVENT_TYPE, eff.as_payload(mocked=bool(ctx.is_mocked)))  # noqa: SLF001 — the runtime is the one legitimate writer
    except Exception:  # noqa: BLE001 — never fail the operator for its own record
        _log.warning("parameters record for %s failed (run unaffected)", ctx.node_name, exc_info=True)
