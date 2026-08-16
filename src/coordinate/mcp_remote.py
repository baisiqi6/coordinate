"""Loopback remote MCP transport/auth adapter (R3-I1).

This module is the product-owned boundary between the official MCP SDK
streamable HTTP app and Coordinate's identity/scope model. It deliberately
does NOT:

- open a connection on the event loop thread; every domain call runs through
  the existing facades, which create/close their own short-lived SQLite
  connections per call;
- re-implement MCP protocol parsing, JSON-RPC validation or tool schemas (the
  SDK owns those; tools stay registered once in ``mcp_server.py``);
- write SQL, call the CLI, shell, SSH or subprocesses, or hold process-level
  mutable business state;
- create or migrate the database (the connection factory must be built with
  ``connect(db_path, must_exist=True)`` by the CLI layer).

Security model (all fail closed):

- Authentication is a digest-only server-local policy file (``load_mcp_auth_policy``):
  strict regular-file/mode checks, 64-lowercase-hex digests only, unknown
  fields, duplicate client ids, duplicate digests and illegal scopes are
  startup errors. Presented tokens are compared timing-safely; missing, bad
  and unknown tokens produce one identical static 401.
- ``RemoteMCPApp`` is the outermost ASGI stack: authentication, then bounded
  concurrency (default 16, exhausted capacity fails closed with a static 503
  instead of queueing unbounded waiters), then a bounded access log, then the
  SDK app. The authenticated principal is stored in a request-local
  ``ContextVar`` before any SDK dispatch and reset afterwards; the SDK's
  middleware and the sync tool bodies (SDK worker threads inherit the copied
  context) read the same value, so identity can never come from model
  arguments, ``_meta`` or client name.
- The SDK ``ServerMiddleware`` appended here performs per-principal
  authorization: ``tools/list`` is filtered deletion-style on
  ``ListToolsResult.tools``; ``tools/call`` re-checks tool, workspace,
  platform, channel-binding and job-projection scope before the result leaves
  the boundary. Cross-workspace job reads and missing jobs are byte-identical
  ``not_found`` envelopes. ``runtime_agent_list`` is a global read opened only
  by an explicit tool grant.
- Access logs carry request id, derived client id, MCP method/tool name,
  status and latency only; authorization headers, bodies, prompts, tool
  results, DB paths and exception text never appear.
"""

from __future__ import annotations

import asyncio
import contextvars
import json
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .agent_interface import AgentInterface
from .mcp_server import MCP_TOOL_NAMES, TOOL_CHANNEL_CREATE, TYPED_INPUT_TOOLS
from .policy_common import (
    AuthPolicyError,
    constant_time_digest_equals,
    digest_token,
    is_valid_token_digest,
    read_strict_policy_file,
)
from .runtime_interface import MESSAGE_JOB_NOT_FOUND, RuntimeInterface

logger = logging.getLogger("coordinate.mcp_remote")

# R3 contract limits (the concurrency profile deliberately reuses the R2A
# validated value; any change must be recorded in the R3 plan).
DEFAULT_MAX_CONCURRENT = 16
DEFAULT_MAX_REQUEST_BODY_SIZE = 4 * 1024 * 1024  # 4 MiB

# Platforms governed by the shared channel-binding authority in db.py.
_CHANNEL_BOUND_PLATFORMS = frozenset({"discord", "kook"})

# Tools whose first authorization step is the top-level ``workspace_id``
# argument; ``runtime_job_get`` is checked on the job projection instead and
# ``runtime_agent_list`` is the explicit-grant global read. The six R5B
# tools register a single typed ``input`` model (see ``TYPED_INPUT_TOOLS``);
# the middleware unwraps it so the workspace check sees the same field the
# schema advertises.
_WORKSPACE_SCOPED_TOOLS = frozenset(
    {
        "coordinate.operator_pending",
        "coordinate.workspace_audit",
        "coordinate.runtime_request_submit",
        "coordinate.task_create_record",
        "coordinate.completion_prepare",
        "coordinate.completion_preflight",
        "coordinate.completion_claim",
        "coordinate.completion_apply",
        "coordinate.completion_consume",
        TOOL_CHANNEL_CREATE,
    }
)

def _typed_input_args(name: str, args: dict[str, Any]) -> dict[str, Any] | None:
    """Unwrap the single typed ``input`` argument of an R5B tool.

    Returns the inner argument dict when the wire shape is exactly
    ``{"input": <object>}``, else None (the caller must fail closed). The
    tool set is the single shared definition from ``mcp_server`` — the
    remote unwrap never re-handwrites the schema.
    """
    if name not in TYPED_INPUT_TOOLS:
        return None
    if not isinstance(args, dict) or set(args) != {"input"}:
        return None
    inner = args.get("input")
    if not isinstance(inner, dict):
        return None
    return inner

# Static wire bodies for transport-level failures. Identical for every
# missing/unknown/bad credential; nothing request-derived is ever echoed.
_UNAUTHORIZED_BODY: dict[str, Any] = {
    "error": {"code": "unauthorized", "message": "invalid credentials"},
}
_UNAVAILABLE_BODY: dict[str, Any] = {
    "error": {"code": "unavailable", "message": "server busy"},
}
_INTERNAL_BODY: dict[str, Any] = {
    "error": {"code": "internal", "message": "internal error"},
}

_FORBIDDEN_ENVELOPE: dict[str, Any] = {
    "ok": False,
    "data": None,
    "error": {"code": "forbidden", "message": "forbidden"},
}

_request_principal: contextvars.ContextVar["McpPrincipal | None"] = (
    contextvars.ContextVar("coordinate_mcp_remote_principal", default=None)
)


def get_request_principal() -> "McpPrincipal | None":
    """Return the authenticated principal of the current request, else None.

    Readable from the ASGI auth middleware, the SDK ``ServerMiddleware`` and
    sync tool bodies (anyio copies the context into worker threads); never
    settable from wire content.
    """
    return _request_principal.get()


@dataclass(frozen=True)
class McpPrincipal:
    """One authenticated remote MCP client derived strictly from the policy."""

    client_id: str
    token_sha256: str
    workspace_ids: frozenset[str] = field(default_factory=frozenset)
    platforms: frozenset[str] = field(default_factory=frozenset)
    tools: frozenset[str] = field(default_factory=frozenset)

    @property
    def actor(self) -> str:
        """Fixed derived actor; callers can never override it."""
        return f"mcp-remote:{self.client_id}"


class McpAuthPolicy:
    """Loaded, validated digest allowlist (digests only, no plaintext)."""

    def __init__(self, clients: dict[str, McpPrincipal]) -> None:
        self._by_digest = clients

    def authenticate(self, token: str | None) -> McpPrincipal | None:
        """Return the principal for a valid token, else None.

        Timing-safe digest comparison; missing/unknown/bad all return None so
        the caller can emit one identical 401.
        """
        if not isinstance(token, str) or not token:
            return None
        supplied = digest_token(token)
        for expected, principal in self._by_digest.items():
            if constant_time_digest_equals(expected, supplied):
                return principal
        return None


def _require_keys(
    mapping: dict[str, Any], allowed: frozenset[str], label: str
) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise AuthPolicyError(f"unknown {label} field(s): {sorted(unknown)[0]}")


def _scope_list(
    value: Any, label: str, client_id: str, *, allow_empty: bool
) -> frozenset[str]:
    if not isinstance(value, list):
        raise AuthPolicyError(
            f"client {client_id!r} requires a {label} list"
        )
    if not allow_empty and not value:
        raise AuthPolicyError(
            f"client {client_id!r} requires a non-empty {label} list"
        )
    items: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise AuthPolicyError(
                f"client {client_id!r} {label} entries must be non-blank strings"
            )
        items.add(item)
    return frozenset(items)


def load_mcp_auth_policy(path: str) -> McpAuthPolicy:
    """Load and strictly validate the server-local remote MCP policy file.

    Fail-closed rules: non-regular or group/world-writable files, invalid
    JSON, unknown/missing fields, duplicate client ids, duplicate digests,
    plaintext (non-digest) tokens and unknown tool names all raise
    ``AuthPolicyError`` so the server fails before binding. Workspace and
    platform lists may be empty (fail closed at call time); the tools list
    must be non-empty and drawn from the eleven registered tools.
    """
    raw = read_strict_policy_file(path)
    try:
        document = json.loads(raw)
    except ValueError as exc:
        raise AuthPolicyError("mcp auth policy is not valid JSON") from exc
    if not isinstance(document, dict):
        raise AuthPolicyError("mcp auth policy root must be an object")
    _require_keys(
        document, frozenset({"version", "clients"}), "mcp auth policy root"
    )
    if document.get("version") != 1:
        raise AuthPolicyError("mcp auth policy version must be 1")
    clients = document.get("clients")
    if not isinstance(clients, list) or not clients:
        raise AuthPolicyError("mcp auth policy clients must be a non-empty list")

    parsed: dict[str, McpPrincipal] = {}
    seen_client_ids: set[str] = set()
    seen_digests: set[str] = set()
    for index, entry in enumerate(clients):
        if not isinstance(entry, dict):
            raise AuthPolicyError(f"clients[{index}] must be an object")
        _require_keys(
            entry,
            frozenset(
                {
                    "client_id",
                    "token_sha256",
                    "workspace_ids",
                    "platforms",
                    "tools",
                }
            ),
            f"clients[{index}]",
        )
        client_id = entry.get("client_id")
        if not isinstance(client_id, str) or not client_id.strip():
            raise AuthPolicyError(
                f"clients[{index}] client_id must be a non-blank string"
            )
        if client_id in seen_client_ids:
            raise AuthPolicyError(f"duplicate client_id: {client_id!r}")
        seen_client_ids.add(client_id)
        digest = entry.get("token_sha256")
        if not isinstance(digest, str) or not is_valid_token_digest(digest):
            raise AuthPolicyError(
                f"clients[{index}] token_sha256 must be 64 lowercase hex chars"
            )
        if digest in seen_digests:
            raise AuthPolicyError(
                f"duplicate token_sha256 for client {client_id!r}"
            )
        seen_digests.add(digest)
        workspace_ids = _scope_list(
            entry.get("workspace_ids"),
            "workspace_ids",
            client_id,
            allow_empty=True,
        )
        platforms = _scope_list(
            entry.get("platforms"), "platforms", client_id, allow_empty=True
        )
        tools = _scope_list(
            entry.get("tools"), "tools", client_id, allow_empty=False
        )
        unknown_tools = tools - MCP_TOOL_NAMES
        if unknown_tools:
            raise AuthPolicyError(
                f"client {client_id!r} grants unknown tool(s): "
                f"{sorted(unknown_tools)[0]}"
            )
        parsed[digest] = McpPrincipal(
            client_id=client_id,
            token_sha256=digest,
            workspace_ids=workspace_ids,
            platforms=platforms,
            tools=tools,
        )
    return McpAuthPolicy(clients=parsed)


def make_interface_provider(
    connection_factory: Callable[[], Any],
) -> Callable[[], AgentInterface]:
    """Build the request-scoped facade provider for ``build_mcp_server``.

    Each tool invocation resolves the current authenticated principal (never
    wire content) and constructs an ``AgentInterface`` with the derived actor
    and the shared must-exist connection factory.
    """

    def _provider() -> AgentInterface:
        principal = get_request_principal()
        if principal is None:
            raise RuntimeError(
                "remote MCP tool invoked without an authenticated principal"
            )
        return AgentInterface(
            connection_factory=connection_factory, actor=principal.actor
        )

    return _provider


def _rpc_identity(raw: bytes) -> tuple[str | None, str | None]:
    """Extract JSON-RPC method/tool name for the access log, or (None, None).

    Parses only the method and the tool name; the body itself is never logged
    or stored.
    """
    if not raw:
        return None, None
    try:
        document = json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None, None
    if not isinstance(document, dict) or not isinstance(
        document.get("method"), str
    ):
        return None, None
    method = document["method"]
    tool = None
    if method == "tools/call":
        params = document.get("params")
        if isinstance(params, dict) and isinstance(params.get("name"), str):
            tool = params["name"]
    return method, tool


_LOG_FIELD_LIMIT = 64


def _safe_log_field(value: Any) -> str:
    """Render one access-log field as a bounded, single-line, printable token.

    Request-derived method/tool names and policy-derived client ids must never
    inject a newline or control character into the log stream: unprintable
    characters are replaced, the value is trimmed and bounded, and empty/None
    render as ``-``. The body itself is never logged.
    """
    if value is None:
        return "-"
    sanitized = "".join(ch if ch.isprintable() else "?" for ch in str(value)).strip()
    if not sanitized:
        return "-"
    if len(sanitized) > _LOG_FIELD_LIMIT:
        sanitized = sanitized[: _LOG_FIELD_LIMIT]
    return sanitized


def _filter_list_tools_wire_result(
    result: Any, allowed: frozenset[str]
) -> dict[str, Any]:
    """Deletion-style filter over the serialized ``tools/list`` wire dict.

    ``call_next`` yields the serialized wire form (not the ``ListToolsResult``
    model). A result without a list-valued ``tools`` key is never forwarded
    unfiltered: the middleware fails closed with an empty tool list.
    """
    if isinstance(result, dict) and isinstance(result.get("tools"), list):
        filtered = [
            tool
            for tool in result["tools"]
            if isinstance(tool, dict) and tool.get("name") in allowed
        ]
        return {**result, "tools": filtered}
    return {"tools": []}


def _checked_job_get_wire_result(
    result: Any, workspace_ids: frozenset[str], not_found_result: Any
) -> Any:
    """Authorize a job-get result before it leaves the boundary.

    Cross-workspace and missing jobs share one ``not_found`` envelope, so the
    caller cannot tell whether the job exists. Any unexpected shape (non-dict
    result, missing/malformed ``structuredContent`` envelope, non-dict data,
    absent workspace id) fails closed to the same ``not_found`` instead of
    forwarding potentially cross-workspace content. Domain error envelopes
    pass through unchanged.
    """
    if not isinstance(result, dict) or not isinstance(
        result.get("structuredContent"), dict
    ):
        return not_found_result
    envelope = result["structuredContent"]
    if not envelope.get("ok"):
        return result
    data = envelope.get("data")
    workspace_id = data.get("workspace_id") if isinstance(data, dict) else None
    if not isinstance(workspace_id, str) or workspace_id not in workspace_ids:
        return not_found_result
    return result


class RemoteMCPApp:
    """Outermost ASGI stack: auth -> bounded concurrency -> access log.

    A plain ASGI callable so it can wrap the SDK's Starlette app without any
    framework coupling. Non-HTTP scopes (lifespan) pass through untouched.
    """

    def __init__(
        self,
        sdk_app: Any,
        *,
        policy: McpAuthPolicy,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
        max_request_body_size: int = DEFAULT_MAX_REQUEST_BODY_SIZE,
    ) -> None:
        self._app = sdk_app
        self._policy = policy
        self._max_body = max_request_body_size
        self._capacity = max_concurrent
        self._active = 0
        self._guard = asyncio.Lock()

    async def _acquire(self) -> bool:
        """Claim one dispatch slot; False when capacity is exhausted."""
        async with self._guard:
            if self._active >= self._capacity:
                return False
            self._active += 1
            return True

    async def _release(self) -> None:
        async with self._guard:
            self._active -= 1

    async def __call__(
        self, scope: dict[str, Any], receive: Any, send: Any
    ) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return
        request_id = secrets.token_hex(8)
        start = time.monotonic()
        principal = None
        method: str | None = None
        tool: str | None = None
        status = 500
        try:
            principal = self._authenticate(scope)
            if principal is None:
                status = await self._send_static(
                    send, 401, _UNAUTHORIZED_BODY, request_id
                )
                return
            if not await self._acquire():
                status = await self._send_static(
                    send, 503, _UNAVAILABLE_BODY, request_id
                )
                return
            token = _request_principal.set(principal)
            try:
                status, method, tool = await self._dispatch(
                    scope, receive, send, request_id
                )
            finally:
                _request_principal.reset(token)
                await self._release()
        except Exception as exc:
            # Bounded 500: log only the exception type and request id; never
            # the raw exception text, body, prompt, result or DB path.
            logger.error(
                "unhandled request error type=%s request_id=%s",
                type(exc).__name__,
                request_id,
            )
            try:
                status = await self._send_static(
                    send, 500, _INTERNAL_BODY, request_id
                )
            except Exception:
                # The response may already have started; nothing safe to send.
                status = 500
        finally:
            self._log_access(request_id, principal, method, tool, status, start)

    def _authenticate(self, scope: dict[str, Any]) -> McpPrincipal | None:
        token: str | None = None
        for key, value in scope.get("headers", []):
            if key.lower() == b"authorization":
                parts = value.decode("latin-1").split(None, 1)
                if len(parts) == 2 and parts[0].lower() == "bearer":
                    candidate = parts[1].strip()
                    token = candidate or None
                else:
                    token = None
                break
        return self._policy.authenticate(token)

    async def _dispatch(
        self,
        scope: dict[str, Any],
        receive: Any,
        send: Any,
        request_id: str,
    ) -> tuple[int, str | None, str | None]:
        captured: list[bytes] = []
        captured_bytes = 0

        async def _receive() -> dict[str, Any]:
            nonlocal captured_bytes
            message = await receive()
            if message["type"] == "http.request":
                chunk = message.get("body") or b""
                if captured_bytes < self._max_body:
                    take = chunk[: self._max_body - captured_bytes]
                    captured.append(take)
                    captured_bytes += len(take)
            return message

        holder: dict[str, int] = {"status": 500}

        async def _send(message: dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                holder["status"] = message["status"]
                headers = list(message.get("headers", []))
                headers.append(
                    (b"x-coordinate-request-id", request_id.encode("ascii"))
                )
                message = {**message, "headers": headers}
            await send(message)

        await self._app(scope, _receive, _send)
        method, tool = _rpc_identity(b"".join(captured))
        return holder["status"], method, tool

    @staticmethod
    async def _send_static(
        send: Any, status: int, body: dict[str, Any], request_id: str
    ) -> int:
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"x-coordinate-request-id", request_id.encode("ascii")),
                ],
            }
        )
        await send({"type": "http.response.body", "body": payload})
        return status

    @staticmethod
    def _log_access(
        request_id: str,
        principal: McpPrincipal | None,
        method: str | None,
        tool: str | None,
        status: int,
        start: float,
    ) -> None:
        """Bounded access log: request id, client id, method/tool, status,
        latency only. Raw paths, bodies, prompts, results, tokens, DB paths
        and exception text never appear; every request-derived or
        policy-derived field is rendered as a bounded single-line token."""
        client = _safe_log_field(
            principal.client_id if principal is not None else None
        )
        logger.info(
            "request id=%s client=%s method=%s tool=%s status=%s latency_ms=%.1f",
            request_id,
            client,
            _safe_log_field(method),
            _safe_log_field(tool),
            status,
            (time.monotonic() - start) * 1000.0,
        )


def build_remote_mcp_app(
    server: Any,
    *,
    policy: McpAuthPolicy,
    connection_factory: Callable[[], Any],
    allowed_hosts: list[str],
    allowed_origins: list[str],
    streamable_http_path: str = "/mcp",
    max_concurrent: int = DEFAULT_MAX_CONCURRENT,
    max_request_body_size: int = DEFAULT_MAX_REQUEST_BODY_SIZE,
) -> RemoteMCPApp:
    """Build the full remote MCP ASGI stack around a built MCPServer.

    Appends the per-principal authorization ``ServerMiddleware`` (public SDK
    seam; ``tools/list`` filters the serialized wire ``tools`` list
    deletion-style and ``tools/call`` re-authorizes) and constructs the SDK
    streamable HTTP app with the stateless 2026-07-28 profile, no event
    store, an explicit body limit and explicit transport-security allowlists.
    """
    from mcp.server.transport_security import TransportSecuritySettings
    from mcp.types import CallToolResult, TextContent

    runtime = RuntimeInterface(
        connection_factory=connection_factory, actor="mcp-remote"
    )

    def _tool_error_result(code: str, message: str) -> CallToolResult:
        envelope: dict[str, Any] = {
            "ok": False,
            "data": None,
            "error": {"code": code, "message": message},
        }
        return CallToolResult(
            content=[
                TextContent(
                    type="text",
                    text=json.dumps(envelope, ensure_ascii=False),
                )
            ],
            structured_content=envelope,
            is_error=True,
        )

    def _forbidden_result() -> CallToolResult:
        return _tool_error_result("forbidden", "forbidden")

    async def _channel_scope_denied(
        principal: McpPrincipal,
        workspace_id: str,
        origin: Any,
        reply: Any,
    ) -> CallToolResult | None:
        """Reuse the channel-binding authority for discord/kook destinations.

        Only an explicit ``ok=True, data.bound=False`` resolution counts as
        "no active binding" and passes through to the domain (the delivery
        authority governs unbound destinations). Query failures and malformed
        envelopes fail closed to a static forbidden; an active binding that
        points to a different workspace is forbidden too. Nothing about the
        binding, workspace or error is ever echoed.
        """
        for value in (origin, reply):
            if not isinstance(value, dict):
                continue
            platform = value.get("platform")
            destination = value.get("destination")
            if not isinstance(platform, str) or platform not in _CHANNEL_BOUND_PLATFORMS:
                continue
            if not isinstance(destination, str) or not destination:
                continue
            envelope = await asyncio.to_thread(
                runtime.resolve_channel_workspace,
                platform=platform,
                channel_id=destination,
            )
            if not isinstance(envelope, dict) or not envelope.get("ok"):
                # Query failure or malformed envelope: never treat as unbound.
                return _forbidden_result()
            data = envelope.get("data")
            if not isinstance(data, dict):
                return _forbidden_result()
            if data.get("bound") is False:
                # Explicit unbound: the delivery authority governs it.
                continue
            if data.get("bound") is True:
                binding = data.get("binding")
                if (
                    not isinstance(binding, dict)
                    or binding.get("workspace_id") != workspace_id
                ):
                    return _forbidden_result()
                continue
            # Unknown/missing bound state: fail closed.
            return _forbidden_result()
        return None

    async def _submit_denied(
        principal: McpPrincipal, args: dict[str, Any]
    ) -> CallToolResult | None:
        workspace_id = args.get("workspace_id")
        if not isinstance(workspace_id, str) or workspace_id not in principal.workspace_ids:
            return _forbidden_result()
        origin = args.get("origin")
        reply = args.get("reply")
        if (
            isinstance(origin, dict)
            and isinstance(origin.get("platform"), str)
            and origin["platform"] not in principal.platforms
        ):
            return _forbidden_result()
        if isinstance(reply, dict) and isinstance(reply.get("platform"), str):
            reply_platform = reply["platform"]
            if reply_platform != "none" and reply_platform not in principal.platforms:
                return _forbidden_result()
        return await _channel_scope_denied(
            principal, workspace_id, origin, reply
        )

    not_found_wire = _tool_error_result(
        "not_found", MESSAGE_JOB_NOT_FOUND
    ).model_dump(by_alias=True, mode="json", exclude_none=True)

    async def _authorize(ctx: Any, call_next: Callable[[Any], Any]) -> Any:
        principal = get_request_principal()
        if principal is None:
            # Unreachable behind the ASGI auth middleware; fail closed anyway.
            if ctx.method == "tools/list":
                return {"tools": []}
            if ctx.method == "tools/call":
                return _forbidden_result()
            return await call_next(ctx)
        if ctx.method == "tools/list":
            return _filter_list_tools_wire_result(
                await call_next(ctx), principal.tools
            )
        if ctx.method == "tools/call":
            params = ctx.params if isinstance(ctx.params, dict) else {}
            name = params.get("name")
            if not isinstance(name, str) or name not in principal.tools:
                return _forbidden_result()
            raw_args = params.get("arguments")
            args = raw_args if isinstance(raw_args, dict) else {}
            if name in TYPED_INPUT_TOOLS:
                args = _typed_input_args(name, args)
                if args is None:
                    return _forbidden_result()
            if name in _WORKSPACE_SCOPED_TOOLS:
                workspace_id = args.get("workspace_id")
                if not isinstance(workspace_id, str) or workspace_id not in principal.workspace_ids:
                    return _forbidden_result()
            if name == TOOL_CHANNEL_CREATE and "discord" not in principal.platforms:
                return _forbidden_result()
            if name == "coordinate.runtime_request_submit":
                denied = await _submit_denied(principal, args)
                if denied is not None:
                    return denied
            result = await call_next(ctx)
            if name == "coordinate.runtime_job_get":
                return _checked_job_get_wire_result(
                    result, principal.workspace_ids, not_found_wire
                )
            return result
        return await call_next(ctx)

    # C2 ordering: the per-principal auth/scope middleware must run BEFORE
    # the shared R5B call-shape gate (registered inside build_mcp_server) so
    # an unauthorized caller probing a malformed shape gets the static
    # forbidden instead of a shape error. Middleware runs outermost-first,
    # so insert at index 0.
    server.middleware.insert(0, _authorize)

    sdk_app = server.streamable_http_app(
        streamable_http_path=streamable_http_path,
        stateless_http=True,
        event_store=None,
        max_request_body_size=max_request_body_size,
        transport_security=TransportSecuritySettings(
            enable_dns_rebinding_protection=True,
            allowed_hosts=list(allowed_hosts),
            allowed_origins=list(allowed_origins),
        ),
    )
    return RemoteMCPApp(
        sdk_app,
        policy=policy,
        max_concurrent=max_concurrent,
        max_request_body_size=max_request_body_size,
    )
