"""Runtime HTTP CLI registrar and serve handler (R2A).

``coordinate runtime-http serve`` is the only startup surface of the loopback
runtime HTTP core. aiohttp is imported lazily inside the handler: a base
install without the ``runtime-http`` extra keeps ``import coordinate``,
``coordinate --help`` and every other CLI command working, and starting the
server without the extra returns an actionable install hint instead of an
import traceback.
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from .runtime_http import (
    AuthPolicyError,
    LOOPBACK_HOSTS,
    RuntimeHttpServer,
    load_auth_policy,
    serve_forever,
)

INSTALL_HINT = (
    "error: runtime HTTP requires the optional 'runtime-http' extra "
    "(aiohttp>=3.9). Install with: pip install 'coordinate[runtime-http]'"
)

DEFAULT_PORT = 8765


def register_runtime_http_command(subcommands) -> None:
    """Register the ``runtime-http`` parser in its canonical position."""
    runtime_http = subcommands.add_parser(
        "runtime-http", help="Loopback runtime HTTP data plane (R2A)"
    )
    runtime_http_subcommands = runtime_http.add_subparsers(dest="runtime_http_command")

    serve = runtime_http_subcommands.add_parser(
        "serve",
        help="Serve the runtime HTTP core over loopback (R2A)",
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
        help="Loopback TCP port (default: 8765)",
    )
    serve.add_argument(
        "--auth-file",
        required=True,
        metavar="PATH",
        help="Server-local auth policy JSON (digests only; must not be group/world writable)",
    )
    serve.set_defaults(handler=handle_runtime_http_serve)


def handle_runtime_http_serve(args: argparse.Namespace) -> int:
    """Serve the runtime HTTP core (R2A)."""
    host = getattr(args, "host", "127.0.0.1")
    if host not in LOOPBACK_HOSTS:
        print(
            "error: runtime-http only binds loopback (127.0.0.1, ::1 or localhost); "
            f"refusing non-loopback host {host!r}",
            file=sys.stderr,
        )
        return 1
    port = getattr(args, "port", DEFAULT_PORT)
    if not isinstance(port, int) or isinstance(port, bool) or not 1 <= port <= 65535:
        print("error: --port must be an integer between 1 and 65535", file=sys.stderr)
        return 1

    # Probe only the optional aiohttp dependency. Coordinate's own imports
    # below stay outside the try so a real import bug surfaces as a normal
    # traceback instead of being misreported as a missing extra.
    try:
        import aiohttp  # noqa: F401
    except ImportError:
        print(INSTALL_HINT, file=sys.stderr)
        return 1

    try:
        policy = load_auth_policy(args.auth_file)
    except AuthPolicyError as exc:
        print(f"error: invalid runtime-http auth policy: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"error: cannot read runtime-http auth policy: {exc}", file=sys.stderr)
        return 1

    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    from .runtime_interface import RuntimeInterface, RuntimeInterfaceConfig

    interface = RuntimeInterface.from_config(
        RuntimeInterfaceConfig(db_path=args.db, actor="runtime-http")
    )
    server = RuntimeHttpServer(interface=interface, policy=policy)
    return asyncio.run(serve_forever(server, host, port))
