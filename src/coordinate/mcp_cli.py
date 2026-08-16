"""MCP command registrar and serve handlers (R1 stdio, R3 streamable HTTP).

``coordinate mcp serve`` keeps stdio as the default transport and adds the R3
loopback-only streamable HTTP profile. The MCP SDK is imported lazily inside
the handlers: a base install without the ``mcp`` extra keeps ``import
coordinate``, ``coordinate --help`` and every other CLI command working, and
starting the MCP server without the extra returns an actionable install hint
instead of an import traceback.

The remote profile fails closed before binding: loopback-only host, valid
port/path, digest-only policy file, exact (never wildcard) Host/Origin
allowlists, an existing ready Coordinate DB (never created), and only then a
listener.
"""

from __future__ import annotations

import argparse
import sys

INSTALL_HINT = (
    "error: MCP support requires the optional 'mcp' extra (mcp>=2,<3). "
    "Install with: pip install 'coordinate[mcp]'"
)

DEFAULT_PORT = 8766
DEFAULT_PATH = "/mcp"


def _is_valid_path(value: str) -> bool:
    """Fail-closed URL path check: absolute, no query/fragment/controls."""
    if not value.startswith("/") or len(value) > 200:
        return False
    if any(ch in value for ch in ("?", "#")):
        return False
    allowed = frozenset(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~/"
    )
    return all(ch in allowed for ch in value)


def _validate_remote_args(args: argparse.Namespace) -> str | None:
    """Return a fail-closed error message for invalid remote flags, else None."""
    from .runtime_http import LOOPBACK_HOSTS

    host = getattr(args, "host", "127.0.0.1")
    if host not in LOOPBACK_HOSTS:
        return (
            "mcp streamable-http only binds loopback (127.0.0.1, ::1 or "
            f"localhost); refusing non-loopback host {host!r}"
        )
    port = getattr(args, "port", DEFAULT_PORT)
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        return "--port must be an integer between 1 and 65535"
    path = getattr(args, "path", DEFAULT_PATH)
    if not isinstance(path, str) or not _is_valid_path(path):
        return "--path must be an absolute URL path (e.g. /mcp) without query or fragment"
    auth_file = getattr(args, "auth_file", None)
    if not auth_file:
        return "mcp streamable-http requires --auth-file"
    allowed_host = getattr(args, "allowed_host", None) or []
    if not allowed_host:
        return "mcp streamable-http requires at least one --allowed-host"
    for label, entries in (
        ("--allowed-host", allowed_host),
        ("--allowed-origin", getattr(args, "allowed_origin", None) or []),
    ):
        for entry in entries:
            if not isinstance(entry, str) or not entry.strip() or any(
                ch.isspace() for ch in entry
            ):
                return f"{label} entries must be non-blank values without whitespace"
            if "*" in entry:
                return f"{label} must be exact values; wildcard {entry!r} is refused"
    return None


def register_mcp_command(subcommands) -> None:
    """Register the ``mcp`` parser in its canonical position."""
    mcp = subcommands.add_parser(
        "mcp", help="MCP agent interface (stdio / streamable HTTP)"
    )
    mcp_subcommands = mcp.add_subparsers(dest="mcp_command")

    serve = mcp_subcommands.add_parser(
        "serve",
        help="Serve the MCP agent interface (R1: stdio default; R3: streamable HTTP)",
    )
    serve.add_argument(
        "--transport",
        choices=["stdio", "streamable-http"],
        default="stdio",
        help="Transport to serve (default: stdio; streamable-http is the R3 remote profile)",
    )
    serve.add_argument(
        "--host",
        default="127.0.0.1",
        help="Loopback bind address (127.0.0.1, ::1 or localhost only)",
    )
    serve.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help="Loopback TCP port (default: 8766)",
    )
    serve.add_argument(
        "--path",
        default=DEFAULT_PATH,
        help="Streamable HTTP endpoint path (default: /mcp)",
    )
    serve.add_argument(
        "--auth-file",
        metavar="PATH",
        help="Server-local MCP client policy JSON (digests only; must not be group/world writable)",
    )
    serve.add_argument(
        "--allowed-host",
        action="append",
        metavar="HOST",
        help="Exact Host header value accepted by the endpoint, including port (repeatable; never a wildcard)",
    )
    serve.add_argument(
        "--allowed-origin",
        action="append",
        metavar="ORIGIN",
        help="Exact Origin header value accepted (repeatable; never a wildcard)",
    )
    serve.add_argument(
        "--actor",
        default="mcp",
        help="Fixed actor identity recorded for stdio tool mutations; remote identity is derived per principal",
    )
    serve.set_defaults(handler=handle_mcp_serve)


def _serve_streamable_http(args: argparse.Namespace) -> int:
    """Serve the R3 streamable HTTP profile; every check fails before binding."""
    error = _validate_remote_args(args)
    if error is not None:
        print(f"error: {error}", file=sys.stderr)
        return 1

    # Policy loads before the optional-dependency probe so a bad policy is
    # reported on any install, not masked by a missing extra.
    from .mcp_remote import (
        build_remote_mcp_app,
        load_mcp_auth_policy,
        make_interface_provider,
    )
    from .policy_common import AuthPolicyError

    try:
        policy = load_mcp_auth_policy(args.auth_file)
    except AuthPolicyError as exc:
        print(f"error: invalid mcp-remote auth policy: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"error: cannot read mcp-remote auth policy: {exc}", file=sys.stderr)
        return 1

    # Probe only the optional MCP dependency. Coordinate's own imports below
    # stay outside the try so a real import bug surfaces as a normal traceback
    # instead of being misreported as a missing extra.
    try:
        import mcp  # noqa: F401
    except ImportError:
        print(INSTALL_HINT, file=sys.stderr)
        return 1

    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    from .runtime_interface import RuntimeInterface, RuntimeInterfaceConfig

    # Existing-only DB: must_exist=True never creates the file, and the
    # readiness probe fails closed on a missing or schema-less database.
    runtime = RuntimeInterface.from_config(
        RuntimeInterfaceConfig(db_path=args.db, actor="mcp-remote")
    )
    if not runtime.readiness():
        print(
            "error: coordinate database is missing or not ready (remote MCP "
            "requires an existing DB with events/jobs/workspaces tables)",
            file=sys.stderr,
        )
        return 1

    from .mcp_server import build_mcp_server

    server = build_mcp_server(
        interface_provider=make_interface_provider(runtime.connection_factory)
    )
    app = build_remote_mcp_app(
        server,
        policy=policy,
        connection_factory=runtime.connection_factory,
        allowed_hosts=getattr(args, "allowed_host", []) or [],
        allowed_origins=getattr(args, "allowed_origin", []) or [],
        streamable_http_path=getattr(args, "path", DEFAULT_PATH),
    )
    import uvicorn

    # uvicorn's own access log is disabled; the only request log is the
    # bounded middleware log from coordinate.mcp_remote.
    uvicorn.run(
        app,
        host=getattr(args, "host", "127.0.0.1"),
        port=getattr(args, "port", DEFAULT_PORT),
        log_config=None,
        access_log=False,
    )
    return 0


def handle_mcp_serve(args: argparse.Namespace) -> int:
    """Serve the MCP agent interface (stdio default; streamable HTTP remote)."""
    transport = getattr(args, "transport", "stdio")
    if transport == "streamable-http":
        return _serve_streamable_http(args)
    if transport != "stdio":
        print(
            f"error: unsupported transport {transport!r} "
            "(supported: stdio, streamable-http)",
            file=sys.stderr,
        )
        return 1
    # Probe only the optional MCP dependency. Coordinate's own imports below
    # stay outside the try so a real import bug surfaces as a normal traceback
    # instead of being misreported as a missing extra.
    try:
        import mcp  # noqa: F401
    except ImportError:
        print(INSTALL_HINT, file=sys.stderr)
        return 1

    from .agent_interface import AgentInterface, AgentInterfaceConfig
    from .mcp_server import build_mcp_server

    interface = AgentInterface.from_config(
        AgentInterfaceConfig(db_path=args.db, actor=args.actor)
    )
    server = build_mcp_server(interface)
    # stdio_server diverts stdout to protocol framing; SDK logs and our own
    # diagnostics go to stderr only.
    server.run(transport="stdio")
    return 0
