"""Loopback runtime HTTP data-plane adapter (R2A).

This module owns the server-local auth policy and the versioned HTTP surface
over the bounded ``RuntimeInterface``. It deliberately does NOT:

- open a connection on the event loop thread; every domain call runs inside a
  worker thread through ``asyncio.to_thread`` and the facade creates/closes its
  own short-lived SQLite connection per call;
- re-implement domain validation or transaction boundaries;
- call the CLI, shell, SSH or subprocesses;
- expose any route outside the R2A endpoint set, and never binds outside
  loopback (the CLI layer rejects non-loopback hosts before this module runs).

Auth is a server-local JSON policy file (digests only, never plaintext
tokens). Missing/unknown/bad credentials all yield the same static 401 body;
role/scope mismatches yield a static 403. The principal (client id, role,
platform/workspace scope, agent identity) comes exclusively from the policy;
request bodies can never set actor, agent or scope fields.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .policy_common import (
    AuthPolicyError,
    _TOKEN_DIGEST_RE,  # re-exported for callers that imported it from R2A
    constant_time_digest_equals,
    digest_token,
    read_strict_policy_file,
)
from .runtime_interface import (
    MESSAGE_UNAVAILABLE,
    RuntimeInterface,
)
from .runtime_contract import build_runtime_contract

logger = logging.getLogger("coordinate.runtime_http")

# R2A contract limits.
MAX_BODY_BYTES = 1024 * 1024  # 1 MiB
DEFAULT_MAX_CONCURRENT = 16
DRAIN_TIMEOUT_SECONDS = 30.0

_ROLES = frozenset({"bridge", "agentd"})
LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})

# Static wire bodies for transport-level failures (identical for every
# missing/unknown/bad credential; no client enumeration).
_UNAUTHORIZED_ENVELOPE: dict[str, Any] = {
    "ok": False,
    "data": None,
    "error": {"code": "unauthorized", "message": "invalid credentials"},
}
_FORBIDDEN_ENVELOPE: dict[str, Any] = {
    "ok": False,
    "data": None,
    "error": {"code": "forbidden", "message": "forbidden"},
}
_BAD_BODY_ENVELOPE: dict[str, Any] = {
    "ok": False,
    "data": None,
    "error": {"code": "invalid_request", "message": "invalid request body"},
}
_TOO_LARGE_ENVELOPE: dict[str, Any] = {
    "ok": False,
    "data": None,
    "error": {"code": "payload_too_large", "message": "request body too large"},
}
_INTERNAL_ENVELOPE: dict[str, Any] = {
    "ok": False,
    "data": None,
    "error": {"code": "internal", "message": "internal error"},
}


def _protocol_envelope(status: int) -> dict[str, Any]:
    """Structured envelope for aiohttp protocol responses (404/405/413/5xx).

    Only the plan-defined error codes are used: not_found, invalid_request,
    payload_too_large, internal. The message is static and never echoes the
    raw path; the HTTP status is preserved by the caller.
    """
    if status == 404:
        return {
            "ok": False,
            "data": None,
            "error": {"code": "not_found", "message": "not found"},
        }
    if status == 413:
        return {
            "ok": False,
            "data": None,
            "error": {"code": "payload_too_large", "message": "request body too large"},
        }
    if 500 <= status < 600:
        return dict(_INTERNAL_ENVELOPE)
    return {
        "ok": False,
        "data": None,
        "error": {"code": "invalid_request", "message": "invalid request"},
    }

_STATUS_FOR_CODE = {
    "invalid_request": 400,
    "not_found": 404,
    "conflict": 409,
    "unavailable": 503,
    "internal": 500,
}


@dataclass(frozen=True)
class ClientCredential:
    """One authenticated client derived strictly from the server-local policy."""

    client_id: str
    role: str
    token_digest: str
    platforms: frozenset[str] = field(default_factory=frozenset)
    workspace_ids: frozenset[str] = field(default_factory=frozenset)
    agent_id: str | None = None

    @property
    def actor(self) -> str:
        return f"runtime-http:{self.client_id}"


class AuthPolicy:
    """Loaded, validated credential allowlist (digests only)."""

    def __init__(self, clients: dict[str, ClientCredential]) -> None:
        self._clients = clients

    def authenticate(self, client_id: str, token: str) -> ClientCredential | None:
        """Return the principal for a valid credential pair, else None.

        Timing-safe digest comparison; missing/unknown/bad all return None so
        the caller can emit one identical 401.
        """
        credential = self._clients.get(client_id)
        if credential is None or not isinstance(token, str) or not token:
            return None
        supplied = digest_token(token)
        if not constant_time_digest_equals(credential.token_digest, supplied):
            return None
        return credential

    def __contains__(self, client_id: str) -> bool:
        return client_id in self._clients


def _require_keys(mapping: dict[str, Any], allowed: frozenset[str], label: str) -> None:
    unknown = set(mapping) - allowed
    if unknown:
        raise AuthPolicyError(f"unknown {label} field(s): {sorted(unknown)[0]}")


def load_auth_policy(path: str | os.PathLike[str]) -> AuthPolicy:
    """Load and strictly validate the server-local auth policy file.

    Fail-closed rules: unknown/missing fields, duplicate clients, invalid
    role/digest/identity shapes, group/world-writable mode and missing files
    all raise ``AuthPolicyError``. Plaintext tokens are never accepted.
    """
    raw = read_strict_policy_file(path)
    try:
        document = json.loads(raw)
    except ValueError as exc:
        raise AuthPolicyError("auth policy is not valid JSON") from exc
    if not isinstance(document, dict):
        raise AuthPolicyError("auth policy root must be an object")
    _require_keys(document, frozenset({"version", "clients"}), "policy root")
    if document.get("version") != 1:
        raise AuthPolicyError("auth policy version must be 1")
    clients = document.get("clients")
    if not isinstance(clients, list) or not clients:
        raise AuthPolicyError("auth policy clients must be a non-empty list")

    parsed: dict[str, ClientCredential] = {}
    agent_owners: dict[str, str] = {}
    for index, entry in enumerate(clients):
        if not isinstance(entry, dict):
            raise AuthPolicyError(f"clients[{index}] must be an object")
        _require_keys(
            entry,
            frozenset(
                {"client_id", "role", "token_sha256", "platforms", "workspace_ids", "agent_id"}
            ),
            f"clients[{index}]",
        )
        client_id = entry.get("client_id")
        if not isinstance(client_id, str) or not client_id.strip():
            raise AuthPolicyError(f"clients[{index}] client_id must be a non-blank string")
        if client_id in parsed:
            raise AuthPolicyError(f"duplicate client_id: {client_id!r}")
        role = entry.get("role")
        if role not in _ROLES:
            raise AuthPolicyError(f"clients[{index}] role must be 'bridge' or 'agentd'")
        digest = entry.get("token_sha256")
        if not isinstance(digest, str) or not _TOKEN_DIGEST_RE.match(digest):
            raise AuthPolicyError(f"clients[{index}] token_sha256 must be 64 lowercase hex chars")

        platforms: frozenset[str] = frozenset()
        workspace_ids: frozenset[str] = frozenset()
        agent_id: str | None = None
        if role == "bridge":
            if "agent_id" in entry:
                raise AuthPolicyError(f"bridge client {client_id!r} must not set agent_id")
            platforms = _require_string_list(entry.get("platforms"), "platforms", client_id)
            workspace_ids = _require_string_list(
                entry.get("workspace_ids"), "workspace_ids", client_id
            )
        else:  # agentd
            if "platforms" in entry or "workspace_ids" in entry:
                raise AuthPolicyError(
                    f"agentd client {client_id!r} must not set platforms or workspace_ids"
                )
            agent_id = entry.get("agent_id")
            if not isinstance(agent_id, str) or not agent_id.strip():
                raise AuthPolicyError(
                    f"agentd client {client_id!r} requires a non-blank agent_id"
                )
            if agent_id in agent_owners:
                raise AuthPolicyError(
                    f"agent_id {agent_id!r} is claimed by more than one agentd client"
                )
            agent_owners[agent_id] = client_id

        parsed[client_id] = ClientCredential(
            client_id=client_id,
            role=role,
            token_digest=digest,
            platforms=platforms,
            workspace_ids=workspace_ids,
            agent_id=agent_id,
        )
    return AuthPolicy(clients=parsed)


def _require_string_list(
    value: Any, label: str, client_id: str
) -> frozenset[str]:
    if not isinstance(value, list) or not value:
        raise AuthPolicyError(
            f"bridge client {client_id!r} requires a non-empty {label} list"
        )
    items: set[str] = set()
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise AuthPolicyError(
                f"bridge client {client_id!r} {label} entries must be non-blank strings"
            )
        items.add(item)
    return frozenset(items)


class _HttpError(Exception):
    """Transport-level failure rendered as a static envelope."""

    def __init__(self, status: int, envelope: dict[str, Any]) -> None:
        super().__init__(envelope["error"]["code"])
        self.status = status
        self.envelope = envelope


class RuntimeHttpServer:
    """aiohttp application factory + request lifecycle (R2A)."""

    def __init__(
        self,
        *,
        interface: RuntimeInterface,
        policy: AuthPolicy,
        max_concurrent: int = DEFAULT_MAX_CONCURRENT,
        max_body_bytes: int = MAX_BODY_BYTES,
    ) -> None:
        self._interface = interface
        self._policy = policy
        self._max_concurrent = max_concurrent
        self._max_body_bytes = max_body_bytes
        self._semaphore = asyncio.Semaphore(max_concurrent)

    # -- app construction ------------------------------------------------

    def build_app(self) -> Any:
        """Build the aiohttp application (imported lazily; optional extra)."""
        from aiohttp import web

        app = web.Application(middlewares=[self._auth_middleware])
        app.router.add_get("/healthz", self._handle_healthz)
        app.router.add_get("/readyz", self._handle_readyz)
        app.router.add_get("/v1/runtime/contract", self._handle_runtime_contract)
        app.router.add_get(
            "/v1/channel-bindings/{platform}/{channel_id}",
            self._handle_channel_binding,
        )
        app.router.add_post("/v1/requests", self._handle_submit)
        app.router.add_get(
            "/v1/workspaces/{workspace_id}/jobs/{job_id}", self._handle_job_get
        )
        app.router.add_post("/v1/jobs/claim", self._handle_claim)
        app.router.add_post("/v1/jobs/{job_id}/progress", self._handle_progress)
        app.router.add_post("/v1/jobs/{job_id}/report", self._handle_report)
        app.router.add_post("/v1/jobs/{job_id}/lease/renew", self._handle_renew)
        app.router.add_get(
            "/v1/agents/{agent_id}/reconcile", self._handle_reconcile_agent
        )
        return app

    async def start(self, host: str, port: int) -> tuple[Any, Any]:
        """Start the listener; return (runner, site) for the caller to drain.

        ``access_log=None`` disables aiohttp's default raw access log; the only
        request log is the bounded route-template log from this module.
        """
        from aiohttp import web

        runner = web.AppRunner(self.build_app(), access_log=None)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        await site.start()
        return runner, site

    # -- auth -------------------------------------------------------------

    @property
    def _auth_middleware(self) -> Any:
        from aiohttp import web

        @web.middleware
        async def _authenticate(request: Any, handler: Callable[..., Any]) -> Any:
            request["request_id"] = secrets.token_hex(8)
            start = time.monotonic()
            principal = None
            try:
                if request.path.startswith("/v1/"):
                    principal = self._authenticate_request(request)
                    request["principal"] = principal
                response = await handler(request)
            except _HttpError as exc:
                response = web.json_response(exc.envelope, status=exc.status)
            except web.HTTPException as exc:
                # Protocol responses (404 unmatched, 405 wrong method, 413,
                # 5xx) keep their HTTP status but render the structured
                # envelope with the plan-defined error codes, the request-id
                # header and the bounded access log tail. The static message
                # never echoes the raw path.
                response = web.json_response(
                    _protocol_envelope(exc.status), status=exc.status
                )
            except Exception as exc:
                # Bounded 500: log only the exception type and request id;
                # never the raw exception text, path, body, token, prompt,
                # result or DB path.
                logger.error(
                    "unhandled request error type=%s request_id=%s",
                    type(exc).__name__,
                    request["request_id"],
                )
                response = web.json_response(dict(_INTERNAL_ENVELOPE), status=500)
            response.headers["X-Coordinate-Request-Id"] = request["request_id"]
            self._log_access(request, principal, response.status, start)
            return response

        return _authenticate

    def _log_access(
        self,
        request: Any,
        principal: ClientCredential | None,
        status: int,
        start: float,
    ) -> None:
        """Bounded access log: request id, client id, role, method, route
        template, status and latency only. Raw path identifiers, bodies,
        prompts, results, tokens and exception text never appear."""
        resource = getattr(request.match_info.route, "resource", None)
        route = getattr(resource, "canonical", "-") if resource is not None else "-"
        client = principal.client_id if principal is not None else "-"
        role = principal.role if principal is not None else "-"
        logger.info(
            "request id=%s client=%s role=%s method=%s route=%s status=%s latency_ms=%.1f",
            request["request_id"],
            client,
            role,
            request.method,
            route,
            status,
            (time.monotonic() - start) * 1000.0,
        )

    def _authenticate_request(self, request: Any) -> ClientCredential:
        client_id = request.headers.get("X-Coordinate-Client-ID", "")
        authorization = request.headers.get("Authorization", "")
        token = ""
        if authorization.startswith("Bearer "):
            token = authorization[len("Bearer "):]
        principal = self._policy.authenticate(client_id, token)
        if principal is None:
            raise _HttpError(401, _UNAUTHORIZED_ENVELOPE)
        return principal

    @staticmethod
    def _require_role(principal: ClientCredential, role: str) -> None:
        if principal.role != role:
            raise _HttpError(403, _FORBIDDEN_ENVELOPE)

    # -- request helpers ----------------------------------------------------

    async def _read_json_object(self, request: Any) -> dict[str, Any]:
        content_type = request.headers.get("Content-Type", "")
        if content_type.split(";", 1)[0].strip().lower() != "application/json":
            raise _HttpError(400, _BAD_BODY_ENVELOPE)
        if request.content_length is not None and request.content_length > self._max_body_bytes:
            raise _HttpError(413, _TOO_LARGE_ENVELOPE)
        raw = await request.content.read(self._max_body_bytes + 1)
        if len(raw) > self._max_body_bytes:
            raise _HttpError(413, _TOO_LARGE_ENVELOPE)
        if not raw:
            raise _HttpError(400, _BAD_BODY_ENVELOPE)
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError):
            raise _HttpError(400, _BAD_BODY_ENVELOPE)
        if not isinstance(payload, dict):
            raise _HttpError(400, _BAD_BODY_ENVELOPE)
        return payload

    @staticmethod
    def _allow_fields(payload: dict[str, Any], allowed: frozenset[str]) -> None:
        unknown = set(payload) - allowed
        if unknown:
            raise _HttpError(400, _BAD_BODY_ENVELOPE)

    async def _call(self, principal: ClientCredential, fn: Callable[[], dict[str, Any]]) -> Any:
        """Run one domain call under the bounded semaphore in a worker thread."""
        interface = RuntimeInterface(
            connection_factory=self._interface.connection_factory,
            actor=principal.actor,
        )
        async with self._semaphore:
            return await asyncio.to_thread(fn, interface)

    async def _binding_authorizes_workspace(
        self,
        principal: ClientCredential,
        *,
        platform: Any,
        channel_id: Any,
        workspace_id: str,
    ) -> bool:
        """Authorize a dynamic bridge scope from the canonical channel binding.

        Static ``workspace_ids`` remain valid for non-channel/bootstrap traffic.
        A bridge may additionally act for a workspace reached through one of
        its permitted platforms, but only while the exact channel is bound to
        that workspace in Coordinate. Lookup errors and malformed identities
        fail closed without exposing the bound workspace.
        """
        if (
            not isinstance(platform, str)
            or platform not in principal.platforms
            or not isinstance(channel_id, str)
            or not channel_id.strip()
        ):
            return False
        envelope = await self._call(
            principal,
            lambda interface: interface.resolve_channel_workspace(
                platform=platform, channel_id=channel_id
            ),
        )
        if not envelope.get("ok"):
            error = envelope.get("error")
            if isinstance(error, dict) and error.get("code") == "unavailable":
                raise _HttpError(503, envelope)
            return False
        data = envelope.get("data")
        if not isinstance(data, dict) or data.get("bound") is not True:
            return False
        binding = data.get("binding")
        return (
            isinstance(binding, dict)
            and binding.get("platform") == platform
            and binding.get("channel_id") == channel_id
            and binding.get("workspace_id") == workspace_id
        )

    # -- public endpoints ----------------------------------------------------

    async def _handle_healthz(self, request: Any) -> Any:
        from aiohttp import web

        return web.json_response(
            {"ok": True, "data": {"status": "ok"}, "error": None},
            status=200,
        )

    async def _handle_readyz(self, request: Any) -> Any:
        from aiohttp import web

        ready = await asyncio.to_thread(self._interface.readiness)
        if not ready:
            return web.json_response(
                {
                    "ok": False,
                    "data": None,
                    "error": {"code": "unavailable", "message": MESSAGE_UNAVAILABLE},
                },
                status=503,
            )
        return web.json_response(
            {"ok": True, "data": {"status": "ready"}, "error": None},
            status=200,
        )

    async def _handle_runtime_contract(self, request: Any) -> Any:
        return self._render(
            request,
            {"ok": True, "data": build_runtime_contract(transport="http"), "error": None},
        )

    # -- bridge endpoints -----------------------------------------------------

    async def _handle_channel_binding(self, request: Any) -> Any:
        principal = request["principal"]
        self._require_role(principal, "bridge")
        platform = request.match_info["platform"]
        channel_id = request.match_info["channel_id"]
        if platform not in principal.platforms:
            raise _HttpError(403, _FORBIDDEN_ENVELOPE)
        envelope = await self._call(
            principal,
            lambda interface: interface.resolve_channel_workspace(
                platform=platform, channel_id=channel_id
            ),
        )
        # The binding itself is the dynamic workspace authority for a bridge
        # on an allowed platform.  This avoids a second workspace list that
        # would have to be manually synchronized after channel provisioning.
        return self._render(request, envelope)

    async def _handle_submit(self, request: Any) -> Any:
        principal = request["principal"]
        self._require_role(principal, "bridge")
        payload = await self._read_json_object(request)
        self._allow_fields(
            payload,
            frozenset(
                {
                    "workspace_id",
                    "prompt",
                    "origin",
                    "reply",
                    "task_id",
                    "target_agent",
                    "routing_request",
                    "worktree_path",
                    "idempotency_key",
                }
            ),
        )
        # Missing/malformed identity fields are contract errors (400); a
        # well-formed value outside the bridge scope is authorization (403).
        workspace_id = payload.get("workspace_id")
        if not isinstance(workspace_id, str) or not workspace_id.strip():
            raise _HttpError(400, _BAD_BODY_ENVELOPE)
        origin = payload.get("origin")
        if not isinstance(origin, dict) or not isinstance(origin.get("platform"), str):
            raise _HttpError(400, _BAD_BODY_ENVELOPE)
        if origin["platform"] not in principal.platforms:
            raise _HttpError(403, _FORBIDDEN_ENVELOPE)
        reply = payload.get("reply")
        if not isinstance(reply, dict) or not isinstance(reply.get("platform"), str):
            raise _HttpError(400, _BAD_BODY_ENVELOPE)
        # Reply platform must stay inside the bridge scope, or use the
        # audit/no-delivery sentinel ``none`` (agentd-mode replies).
        if (
            reply["platform"] not in principal.platforms
            and reply["platform"] != "none"
        ):
            raise _HttpError(403, _FORBIDDEN_ENVELOPE)
        # Every channel-bearing request is authorized by the live binding,
        # including workspaces also present in the static bootstrap scope.
        # Otherwise the static list would become a fail-open bypass for a
        # forged or cross-workspace origin.
        if not await self._binding_authorizes_workspace(
            principal,
            platform=origin.get("platform"),
            channel_id=origin.get("destination"),
            workspace_id=workspace_id,
        ):
            raise _HttpError(403, _FORBIDDEN_ENVELOPE)
        if reply["platform"] != "none" and not await self._binding_authorizes_workspace(
            principal,
            platform=reply.get("platform"),
            channel_id=reply.get("destination"),
            workspace_id=workspace_id,
        ):
            raise _HttpError(403, _FORBIDDEN_ENVELOPE)
        envelope = await self._call(
            principal,
            lambda interface: interface.submit_request(
                workspace_id=workspace_id,
                prompt=payload.get("prompt"),
                origin=origin,
                reply=reply,
                task_id=payload.get("task_id"),
                target_agent=payload.get("target_agent"),
                routing_request=payload.get("routing_request"),
                worktree_path=payload.get("worktree_path"),
                idempotency_key=payload.get("idempotency_key"),
            ),
        )
        return self._render(request, envelope)

    async def _handle_job_get(self, request: Any) -> Any:
        principal = request["principal"]
        self._require_role(principal, "bridge")
        workspace_id = request.match_info["workspace_id"]
        job_id = request.match_info["job_id"]
        envelope = await self._call(
            principal,
            lambda interface: interface.get_job(job_id, workspace_id=workspace_id),
        )
        if workspace_id not in principal.workspace_ids:
            if not envelope.get("ok"):
                raise _HttpError(403, _FORBIDDEN_ENVELOPE)
            data = envelope.get("data")
            payload = data.get("payload") if isinstance(data, dict) else None
            origin = payload.get("origin") if isinstance(payload, dict) else None
            # This is a live capability check, not proof of which transport
            # originally created the job: the current binding grants this
            # bridge access only to jobs targeting its currently bound channel.
            if not isinstance(origin, dict) or not await self._binding_authorizes_workspace(
                principal,
                platform=origin.get("platform"),
                channel_id=origin.get("destination"),
                workspace_id=workspace_id,
            ):
                raise _HttpError(403, _FORBIDDEN_ENVELOPE)
        return self._render(request, envelope)

    # -- agentd endpoints ------------------------------------------------------

    async def _handle_claim(self, request: Any) -> Any:
        principal = request["principal"]
        self._require_role(principal, "agentd")
        payload = await self._read_json_object(request)
        self._allow_fields(payload, frozenset({"ttl_seconds", "reap_mode", "reap_reason", "claim_request_id"}))
        envelope = await self._call(
            principal,
            lambda interface: interface.claim_job(
                agent_id=principal.agent_id,
                ttl_seconds=payload.get("ttl_seconds"),
                reap_mode=payload.get("reap_mode"),
                reap_reason=payload.get("reap_reason"),
                claim_request_id=payload.get("claim_request_id"),
            ),
        )
        return self._render(request, envelope)

    async def _handle_progress(self, request: Any) -> Any:
        principal = request["principal"]
        self._require_role(principal, "agentd")
        job_id = request.match_info["job_id"]
        payload = await self._read_json_object(request)
        self._allow_fields(
            payload,
            frozenset({"stage", "summary", "session_id", "attempt_token", "lease_id"}),
        )
        envelope = await self._call(
            principal,
            lambda interface: interface.record_job_progress(
                job_id=job_id,
                agent_id=principal.agent_id,
                stage=payload.get("stage"),
                summary=payload.get("summary"),
                session_id=payload.get("session_id"),
                attempt_token=payload.get("attempt_token"),
                lease_id=payload.get("lease_id"),
            ),
        )
        return self._render(request, envelope)

    async def _handle_report(self, request: Any) -> Any:
        principal = request["principal"]
        self._require_role(principal, "agentd")
        job_id = request.match_info["job_id"]
        payload = await self._read_json_object(request)
        self._allow_fields(
            payload,
            frozenset({"status", "result", "attempt_token", "lease_id"}),
        )
        envelope = await self._call(
            principal,
            lambda interface: interface.report_job_result(
                job_id=job_id,
                agent_id=principal.agent_id,
                status=payload.get("status"),
                result=payload.get("result"),
                attempt_token=payload.get("attempt_token"),
                lease_id=payload.get("lease_id"),
            ),
        )
        return self._render(request, envelope)

    async def _handle_renew(self, request: Any) -> Any:
        principal = request["principal"]
        self._require_role(principal, "agentd")
        job_id = request.match_info["job_id"]
        payload = await self._read_json_object(request)
        self._allow_fields(
            payload,
            frozenset({"lease_id", "attempt_token", "ttl_seconds"}),
        )
        envelope = await self._call(
            principal,
            lambda interface: interface.renew_managed_lease(
                lease_id=payload.get("lease_id"),
                job_id=job_id,
                attempt_token=payload.get("attempt_token"),
                agent_id=principal.agent_id,
                ttl_seconds=payload.get("ttl_seconds"),
            ),
        )
        return self._render(request, envelope)

    async def _handle_reconcile_agent(self, request: Any) -> Any:
        principal = request["principal"]
        self._require_role(principal, "agentd")
        agent_id = request.match_info["agent_id"]
        # The URL identity is merely a selector; authority comes from the
        # authenticated policy principal and may never be substituted by a
        # caller to inspect another agent's leases.
        if agent_id != principal.agent_id:
            raise _HttpError(403, _FORBIDDEN_ENVELOPE)
        envelope = await self._call(
            principal,
            lambda interface: interface.reconcile_agent(agent_id=principal.agent_id),
        )
        return self._render(request, envelope)

    # -- rendering -------------------------------------------------------------

    def _render(self, request: Any, envelope: dict[str, Any]) -> Any:
        from aiohttp import web

        if envelope["ok"]:
            status = 200
        else:
            status = _STATUS_FOR_CODE.get(envelope["error"]["code"], 500)
        return web.json_response(envelope, status=status)


async def serve_forever(
    server: RuntimeHttpServer,
    host: str,
    port: int,
    *,
    drain_timeout: float = DRAIN_TIMEOUT_SECONDS,
) -> int:
    """Run the listener until SIGTERM/SIGINT, then drain in-flight calls.

    The signal handler only stops accepting new requests; domain calls already
    in worker threads run to their existing transaction boundary (never
    cancelled mid-transaction). Cleanup is bounded by ``drain_timeout``.
    """
    import signal

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _request_stop() -> None:
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:  # pragma: no cover - non-POSIX
            pass

    runner, site = await server.start(host, port)
    logger.info(
        "runtime-http listening host=%s port=%s max_concurrent=%s",
        host,
        port,
        DEFAULT_MAX_CONCURRENT,
    )
    try:
        await stop.wait()
    finally:
        await site.stop()
        try:
            await asyncio.wait_for(runner.cleanup(), timeout=drain_timeout)
        except asyncio.TimeoutError:
            logger.error("runtime-http drain timed out after %ss", drain_timeout)
    logger.info("runtime-http stopped")
    return 0
