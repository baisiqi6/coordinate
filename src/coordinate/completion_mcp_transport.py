"""Completion-only private Remote MCP transport (R5B).

``mark-done-files`` uses this transport to drive the receipt control plane
over Remote MCP instead of the SSH ``--event-cli-path`` wrapper. It is
deliberately NOT a general MCP client:

- exactly three fixed typed calls (``completion_preflight`` /
  ``completion_claim`` / ``completion_apply``), no ``call(tool, payload)``,
  shell, Git, deploy, recovery or arbitrary header surface;
- the bearer token is read only from the caller-named environment variable;
  it never appears in argv, logs, results, exception messages or receipts;
- the endpoint URL is parsed at the transport boundary: any valid
  ``https://`` URL with a host is accepted, ``http://`` only for loopback
  hosts (local test/development), and everything else (plaintext
  non-loopback HTTP, other schemes, userinfo, missing host) is rejected
  with a static error BEFORE the token is read or any network connection
  is attempted;
- every failure raises ``CompletionReceiptError`` with a static machine
  readable reason so the CLI orchestrator fails closed BEFORE any canonical
  file write; only a bounded server reason code is preserved across the
  wire — server-provided error text is never adopted.

The MCP SDK is an optional dependency: constructing the transport is free,
but any call without the ``mcp`` extra fails closed with
``mcp_transport_unavailable`` instead of an import traceback.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
from typing import Any
from urllib.parse import urlsplit

from .completion import CompletionReceiptError
from .mcp_server import (
    TOOL_COMPLETION_APPLY,
    TOOL_COMPLETION_CLAIM,
    TOOL_COMPLETION_PREFLIGHT,
)

__all__ = ["CompletionMCPTransport"]


# C1: the endpoint boundary.  https anywhere; http only for loopback hosts
# (localhost, 127.0.0.0/8, ::1) for local test/development.
_ALLOWED_SCHEMES = frozenset({"https", "http"})
_LOOPBACK_HOSTS = frozenset({"localhost", "127.0.0.1", "::1"})

_URL_REJECTED_MESSAGE = "invalid Remote MCP endpoint URL"
_URL_REJECTED_REASON = "invalid_url"


def _is_loopback_host(hostname: str) -> bool:
    if hostname in _LOOPBACK_HOSTS:
        return True
    if hostname.startswith("127."):
        octets = hostname.split(".")
        return (
            len(octets) == 4
            and all(octet.isdigit() and 0 <= int(octet) <= 255 for octet in octets)
        )
    return False


def _validate_endpoint_url(url: str) -> str:
    """Validate an endpoint URL at the transport boundary (C1).

    Accepts any valid ``https://`` URL with a host, plus ``http://`` only
    for loopback hosts. Rejects other schemes, missing hosts and userinfo
    with the static reason ``invalid_url`` — before any token read or
    network connection.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        raise CompletionReceiptError(
            _URL_REJECTED_MESSAGE, reason=_URL_REJECTED_REASON,
        ) from None
    scheme = parts.scheme
    hostname = parts.hostname
    if scheme not in _ALLOWED_SCHEMES:
        raise CompletionReceiptError(
            _URL_REJECTED_MESSAGE, reason=_URL_REJECTED_REASON,
        ) from None
    if not hostname:
        raise CompletionReceiptError(
            _URL_REJECTED_MESSAGE, reason=_URL_REJECTED_REASON,
        ) from None
    if parts.username is not None or parts.password is not None:
        raise CompletionReceiptError(
            _URL_REJECTED_MESSAGE, reason=_URL_REJECTED_REASON,
        ) from None
    if scheme == "http" and not _is_loopback_host(hostname):
        # Plaintext HTTP must never carry the bearer token off-loopback.
        raise CompletionReceiptError(
            _URL_REJECTED_MESSAGE, reason=_URL_REJECTED_REASON,
        ) from None
    return url


# C3: local static error text per bounded server reason code.  The server's
# error.message is never adopted; only a bounded lowercase reason code is
# kept, and unknown codes degrade to one static default.
_SAFE_REASON_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_ERROR_REASON_MESSAGES: dict[str, str] = {
    "unauthorized": "remote MCP authentication failed",
    "forbidden": "remote MCP refused the request",
    "invalid_request": "remote MCP rejected the arguments",
    "not_found": "remote resource not found",
    "unknown_receipt": "receipt not found on the remote control plane",
    "workspace_mismatch": "receipt workspace does not match the caller",
    "task_mismatch": "receipt task does not match the caller",
    "actor_mismatch": "receipt actor does not match the caller",
    "expired": "receipt expired",
    "before_fingerprint_mismatch": "receipt before fingerprint mismatch",
    "after_fingerprint_mismatch": "receipt after fingerprint mismatch",
    "fingerprint_mismatch": "receipt fingerprint mismatch",
    "not_claimed": "receipt is not claimed",
    "not_applied": "receipt is not applied",
    "already_consumed": "receipt already consumed",
    "gate_not_passed": "completion gate not passed",
    "deployed_not_done": "deployed harness not done",
    "deployed_harness_unavailable": "deployed harness unavailable",
    "deployed_task_missing": "deployed task missing",
}
_DEFAULT_ERROR_MESSAGE = "remote MCP call refused"


def _safe_reason(code: Any) -> str:
    """Return *code* when it is a bounded lowercase identifier, else the
    static ``transport_refused`` fallback (a hostile endpoint cannot forge
    an arbitrary reason through the wire)."""
    if isinstance(code, str) and _SAFE_REASON_RE.fullmatch(code):
        return code
    return "transport_refused"


class CompletionMCPTransport:
    """Narrow completion receipt client for one Remote MCP endpoint.

    ``url`` is the streamable-HTTP endpoint (``https://…/mcp``);
    ``token_env`` names the environment variable holding the bearer token.
    """

    # Completion-only surface: exactly the three control-plane tools, drawn
    # from the single server-side name authority (C2) — no generic call.
    FIXED_TOOLS: tuple[str, ...] = (
        TOOL_COMPLETION_PREFLIGHT,
        TOOL_COMPLETION_CLAIM,
        TOOL_COMPLETION_APPLY,
    )

    def __init__(self, *, url: str, token_env: str) -> None:
        # C1: parse/validate at the boundary, before the token is read or any
        # network connection is attempted.
        self._url = _validate_endpoint_url(url)
        self._token_env = token_env

    # -- the only three operations the transport may perform ----------------

    def preflight(
        self, *, receipt_id: str, workspace_id: str
    ) -> dict[str, Any]:
        """Read-only receipt preflight; returns the authoritative binding."""
        return self._invoke(
            TOOL_COMPLETION_PREFLIGHT,
            {"receipt_id": receipt_id, "workspace_id": workspace_id},
        )

    def claim(
        self,
        *,
        receipt_id: str,
        workspace_id: str,
        task_id: str,
        before_fingerprint: str,
        expected_after_fingerprint: str,
    ) -> dict[str, Any]:
        """Reserve the receipt (authorized -> claimed) BEFORE the local write."""
        return self._invoke(
            TOOL_COMPLETION_CLAIM,
            {
                "receipt_id": receipt_id,
                "workspace_id": workspace_id,
                "task_id": task_id,
                "before_fingerprint": before_fingerprint,
                "expected_after_fingerprint": expected_after_fingerprint,
            },
        )

    def apply(
        self,
        *,
        receipt_id: str,
        workspace_id: str,
        task_id: str,
        after_fingerprint: str,
    ) -> dict[str, Any]:
        """Acknowledge the receipt (claimed -> applied) AFTER the local write."""
        return self._invoke(
            TOOL_COMPLETION_APPLY,
            {
                "receipt_id": receipt_id,
                "workspace_id": workspace_id,
                "task_id": task_id,
                "after_fingerprint": after_fingerprint,
            },
        )

    # -- internals ----------------------------------------------------------

    def _token(self) -> str:
        token = os.environ.get(self._token_env)
        if not token:
            raise CompletionReceiptError(
                f"Remote MCP token environment variable {self._token_env!r} "
                "is not set",
                reason="token_missing",
            )
        return token

    def _invoke(self, tool_name: str, arguments: dict[str, Any]) -> dict[str, Any]:
        if tool_name not in self.FIXED_TOOLS:  # defensive; no generic path
            raise CompletionReceiptError(
                "transport refuses an unlisted tool",
                reason="transport_refused",
            )
        try:
            import mcp  # noqa: F401
        except ImportError:
            raise CompletionReceiptError(
                "Remote MCP transport requires the optional 'mcp' extra "
                "(mcp>=2,<3)",
                reason="mcp_transport_unavailable",
            ) from None
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        token = self._token()

        async def _run() -> dict[str, Any]:
            import httpx2

            async with httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {token}"}
            ) as http_client:
                async with streamable_http_client(
                    self._url, http_client=http_client
                ) as (read_stream, write_stream):
                    async with ClientSession(read_stream, write_stream) as session:
                        result = await session.call_tool(
                            tool_name, {"input": arguments}
                        )
            return _translate_result(tool_name, result)

        try:
            return asyncio.run(_run())
        except CompletionReceiptError:
            raise
        except Exception:
            # Never echo connection/protocol exception text: the reason is a
            # static code and the message is static.
            raise CompletionReceiptError(
                "remote MCP call failed before a valid response",
                reason="transport_failed",
            ) from None


def _translate_result(tool_name: str, result: Any) -> dict[str, Any]:
    """Extract the inner result dict from an MCP CallToolResult.

    Domain failure envelopes raise ``CompletionReceiptError`` with the
    server's reason code preserved. Any other shape (missing envelope,
    malformed ok, missing data) fails closed with ``invalid_response``.
    """
    envelope = getattr(result, "structured_content", None)
    if not isinstance(envelope, dict):
        envelope = None
        for content in getattr(result, "content", None) or ():
            text = getattr(content, "text", None)
            if not isinstance(text, str):
                continue
            try:
                parsed = json.loads(text)
            except ValueError:
                continue
            if isinstance(parsed, dict) and isinstance(parsed.get("ok"), bool):
                envelope = parsed
                break
    if not isinstance(envelope, dict) or not isinstance(
        envelope.get("ok"), bool
    ):
        raise CompletionReceiptError(
            "remote MCP response is not a valid envelope",
            reason="invalid_response",
        )
    if envelope["ok"] is False:
        error = envelope.get("error")
        code = error.get("code") if isinstance(error, dict) else None
        # C3: keep only the bounded reason code; the exception message is
        # always local static text, so a misconfigured or hostile endpoint
        # cannot inject its own error text into the CLI output.
        reason = _safe_reason(code)
        raise CompletionReceiptError(
            _ERROR_REASON_MESSAGES.get(reason, _DEFAULT_ERROR_MESSAGE),
            reason=reason,
        )
    data = envelope.get("data")
    if not isinstance(data, dict):
        raise CompletionReceiptError(
            "remote MCP success response is missing a data object",
            reason="invalid_response",
        )
    return data
