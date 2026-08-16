#!/usr/bin/env python3
"""只读检查 Coordinate Remote MCP 的网络、认证、协议与 tool grant。"""

from __future__ import annotations

import argparse
import json
import os
import stat
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


PROTOCOL_VERSION = "2026-07-28"


class HttpStatusError(RuntimeError):
    def __init__(self, status: int) -> None:
        super().__init__(f"Remote MCP HTTP {status}")
        self.status = status


def _emit(ok: bool, classification: str, **fields: Any) -> None:
    payload = {"ok": ok, "classification": classification, **fields}
    print(json.dumps(payload, ensure_ascii=False, sort_keys=True))


def _safe_url(value: str) -> str:
    parsed = urllib.parse.urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("URL 必须是完整的 http(s) Remote MCP endpoint")
    if parsed.username or parsed.password or parsed.fragment:
        raise ValueError("URL 不得包含 userinfo 或 fragment")
    if parsed.scheme == "http" and parsed.hostname not in {
        "127.0.0.1",
        "::1",
        "localhost",
    }:
        raise ValueError("非 loopback Remote MCP 必须使用 HTTPS")
    return value


def _read_token(args: argparse.Namespace) -> str:
    if args.token_env:
        token = os.environ.get(args.token_env, "")
        source = f"environment variable {args.token_env}"
    else:
        if os.name == "nt":
            raise ValueError(
                "Windows 不支持 --token-file preflight；请由 ACL 受限 launcher "
                "用 --token-env 仅向当前进程注入"
            )
        path = Path(args.token_file)
        if not path.is_file():
            raise ValueError("token file 不存在或不是 regular file")
        if os.name != "nt":
            mode = stat.S_IMODE(path.stat().st_mode)
            if mode & 0o077:
                raise ValueError("token file 必须拒绝 group/other 读取（建议 mode 0600）")
        token = path.read_text(encoding="utf-8").strip()
        source = "token file"
    if not token or any(ch.isspace() for ch in token):
        raise ValueError(f"{source} 为空或包含 whitespace")
    return token


def _message_from_body(body: bytes, content_type: str) -> dict[str, Any]:
    text = body.decode("utf-8", errors="replace")
    if "text/event-stream" in content_type.lower():
        candidates = [
            line[5:].strip()
            for line in text.splitlines()
            if line.startswith("data:") and line[5:].strip()
        ]
        if not candidates:
            raise ValueError("SSE response 没有 data JSON")
        text = candidates[-1]
    value = json.loads(text)
    if not isinstance(value, dict):
        raise ValueError("response 不是 JSON object")
    return value


class Remote:
    def __init__(self, url: str, token: str, timeout: float) -> None:
        self.url = url
        self.token = token
        self.timeout = timeout

    def post(self, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
        method = payload.get("method")
        if not isinstance(method, str) or not method:
            raise ValueError("JSON-RPC payload 缺少 method")
        headers = {
            "Accept": "application/json, text/event-stream",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
            "MCP-Protocol-Version": PROTOCOL_VERSION,
            "MCP-Method": method,
            "User-Agent": "coordinate-operator-mcp-preflight/1",
        }
        params = payload.get("params")
        if (
            method == "tools/call"
            and isinstance(params, dict)
            and isinstance(params.get("name"), str)
        ):
            headers["MCP-Name"] = params["name"]
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                status = response.status
                body = response.read(1024 * 1024)
                content_type = response.headers.get("Content-Type", "")
        except urllib.error.HTTPError as exc:
            status = exc.code
            body = exc.read(1024 * 1024)
            content_type = exc.headers.get("Content-Type", "") if exc.headers else ""
        if status in {401, 403}:
            return status, {}
        if status < 200 or status >= 300:
            return status, {}
        if not body:
            return status, {}
        return status, _message_from_body(body, content_type)


def _meta() -> dict[str, Any]:
    return {
        "io.modelcontextprotocol/protocolVersion": PROTOCOL_VERSION,
        "io.modelcontextprotocol/clientCapabilities": {},
        "io.modelcontextprotocol/clientInfo": {
            "name": "coordinate-operator-preflight",
            "version": "1",
        },
    }


def _rpc_error(message: dict[str, Any]) -> dict[str, Any] | None:
    error = message.get("error")
    return error if isinstance(error, dict) else None


def _modern_discover(remote: Remote) -> tuple[str, list[str]]:
    status, discover = remote.post(
        {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "server/discover",
            "params": {"_meta": _meta()},
        }
    )
    if status == 401:
        raise PermissionError("401")
    if status == 403:
        raise PermissionError("403")
    if status < 200 or status >= 300:
        raise HttpStatusError(status)
    error = _rpc_error(discover)
    if error or not isinstance(discover.get("result"), dict):
        raise RuntimeError("server/discover 返回 JSON-RPC error 或缺少 result")
    supported = discover["result"].get("supportedVersions", [])
    if PROTOCOL_VERSION not in supported:
        raise RuntimeError("server/discover 未声明当前 protocol version")
    return PROTOCOL_VERSION, _tools_list(remote)


def _tools_list(remote: Remote) -> list[str]:
    status, message = remote.post(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/list",
            "params": {"_meta": _meta()},
        }
    )
    if status == 401:
        raise PermissionError("401")
    if status == 403:
        raise PermissionError("403")
    if status < 200 or status >= 300:
        raise HttpStatusError(status)
    result = message.get("result")
    if _rpc_error(message) or not isinstance(result, dict):
        raise RuntimeError("tools/list 返回 JSON-RPC error 或缺少 result")
    # Coordinate 当前 registry 是单页 complete result；出现 cursor 时先升级
    # 此脚本，不能静默把第一页宣称为完整 tool grant。
    if result.get("nextCursor"):
        raise RuntimeError("tools/list 返回 cursor；当前 preflight 不支持分页")
    tools = result.get("tools")
    if not isinstance(tools, list):
        raise RuntimeError("tools/list result 缺少 tools array")
    names: list[str] = []
    for tool in tools:
        if not isinstance(tool, dict) or not isinstance(tool.get("name"), str):
            raise RuntimeError("tools/list 含 malformed tool entry")
        names.append(tool["name"])
    return sorted(set(names))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--token-env")
    source.add_argument("--token-file")
    parser.add_argument("--expected-tool", action="append", default=[])
    parser.add_argument("--timeout", type=float, default=10.0)
    args = parser.parse_args(argv)
    try:
        url = _safe_url(args.url)
        if args.timeout <= 0 or args.timeout > 60:
            raise ValueError("timeout 必须大于 0 且不超过 60 秒")
        token = _read_token(args)
    except ValueError as exc:
        _emit(False, "config", message=str(exc))
        return 2

    try:
        version, tools = _modern_discover(Remote(url, token, args.timeout))
    except PermissionError as exc:
        status = int(str(exc))
        _emit(False, "authentication" if status == 401 else "forbidden", http_status=status)
        return 11 if status == 401 else 12
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        _emit(False, "network", message=type(exc).__name__)
        return 10
    except HttpStatusError as exc:
        _emit(False, "protocol", http_status=exc.status)
        return 13
    except (RuntimeError, ValueError, json.JSONDecodeError) as exc:
        _emit(False, "protocol", message=str(exc))
        return 13

    missing = sorted(set(args.expected_tool) - set(tools))
    if missing:
        _emit(
            False,
            "tool_scope",
            protocol_version=version,
            tools=tools,
            missing_expected_tools=missing,
        )
        return 14
    _emit(True, "ready", protocol_version=version, tools=tools)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
