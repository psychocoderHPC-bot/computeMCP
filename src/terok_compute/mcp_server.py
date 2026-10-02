# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Compute MCP server running inside a Terok container.

It speaks only to the authenticated gateway over HTTP; it has no SSH
credentials and never learns the host's SSH configuration.  Uses the official
MCP Python SDK (v2 ``MCPServer``) with stdio transport.
"""

from __future__ import annotations

import asyncio
import base64
import json as _json
import os
import sys
from typing import Any, IO

import aiohttp
from mcp.server.mcpserver import MCPServer

DEFAULT_TIMEOUT = 60.0
EXEC_TIMEOUT_PADDING = 30.0


class GatewayClient:
    def __init__(self, base_url: str, token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.token = token
        self._session: aiohttp.ClientSession | None = None
        self._lock = asyncio.Lock()

    async def session(self) -> aiohttp.ClientSession:
        async with self._lock:
            if self._session is None or self._session.closed:
                self._session = aiohttp.ClientSession(
                    headers={"Authorization": f"Bearer {self.token}"},
                    timeout=aiohttp.ClientTimeout(total=None, sock_connect=15),
                )
            return self._session

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def request(
        self,
        method: str,
        path: str,
        *,
        json: Any = None,
        params: dict | None = None,
        timeout: float | None = None,
    ) -> Any:
        session = await self.session()
        try:
            async with session.request(
                method,
                f"{self.base_url}{path}",
                json=json,
                params=params,
                timeout=aiohttp.ClientTimeout(total=timeout) if timeout else None,
            ) as response:
                text = await response.text()
                if response.status >= 400:
                    raise RuntimeError(
                        f"gateway {method} {path} failed with {response.status}: {text[:500]}"
                    )
                if not text:
                    return {}
                return _json.loads(text)
        except aiohttp.ClientError as exc:
            raise RuntimeError(f"gateway request failed: {exc}") from exc

    async def upload(
        self,
        target: str,
        remote_path: str,
        data: "IO[bytes]",
        length: int,
        *,
        append: bool = False,
        parents: bool = False,
    ) -> Any:
        """Stream a local file object to the gateway's upload endpoint."""
        params = {"target": target, "path": remote_path}
        if append:
            params["append"] = "true"
        if parents:
            params["parents"] = "true"

        async def body():
            # aiohttp streams an async-iterable request body chunk-by-chunk, so
            # neither the MCP nor the gateway buffers the whole file.
            while True:
                chunk = data.read(262144)
                if not chunk:
                    break
                yield chunk

        session = await self.session()
        async with session.put(
            f"{self.base_url}/v1/files/upload",
            params=params,
            data=body(),
        ) as response:
            text = await response.text()
            if response.status >= 400:
                raise RuntimeError(
                    f"gateway upload failed with {response.status}: {text[:500]}"
                )
        return _json.loads(text) if text else {}

    async def download(self, target: str, remote_path: str, sink: "IO[bytes]") -> dict:
        """Stream a remote file from the gateway into a local file object."""
        params = {"target": target, "path": remote_path, "encoding": "stream"}
        session = await self.session()
        async with session.get(
            f"{self.base_url}/v1/files/read", params=params
        ) as response:
            if response.status >= 400:
                text = await response.text()
                raise RuntimeError(
                    f"gateway download failed with {response.status}: {text[:500]}"
                )
            written = 0
            async for chunk in response.content.iter_chunked(262144):
                sink.write(chunk)
                written += len(chunk)
            sink.flush()
        return {"path": remote_path, "written": written}


def _client_from_env() -> GatewayClient:
    url = os.environ.get("TEROK_COMPUTE_GATEWAY")
    token = os.environ.get("TEROK_COMPUTE_TOKEN")
    if not url:
        raise SystemExit("TEROK_COMPUTE_GATEWAY is not set")
    if not token:
        raise SystemExit("TEROK_COMPUTE_TOKEN is not set")
    return GatewayClient(url, token)


def build_server(client: GatewayClient) -> MCPServer:
    mcp = MCPServer(
        "terok-compute",
        instructions=(
            "Access to isolated remote development containers. "
            "Call compute_targets FIRST to discover machines; use only target "
            "names it returns. All operations run inside the remote development "
            "container, never on the gateway or compute host. Use compute_exec "
            "for short commands and persistent compute_session_* tools for "
            "builds, tests, debuggers or other long-running commands. Multiple "
            "sessions on the same target run concurrently, so use a second "
            "session to inspect a running build."
        ),
    )

    @mcp.tool()
    async def compute_targets() -> dict:
        """List remote compute targets available to this Terok task.

        Call this before any other compute tool and only use the returned
        target names.
        """
        return await client.request("GET", "/v1/targets")

    @mcp.tool()
    async def compute_status(target: str) -> dict:
        """Get state, active route and client count for a compute target."""
        return await client.request("GET", f"/v1/targets/{target}")

    @mcp.tool()
    async def compute_exec(
        target: str,
        command: str,
        cwd: str | None = None,
        timeout: float | None = None,
    ) -> dict:
        """Run a non-interactive command inside a remote development container.

        Returns exit_status, stdout and stderr. Use persistent sessions for
        commands expected to run longer than a few seconds.
        """
        payload = {"target": target, "command": command, "cwd": cwd}
        if timeout is not None:
            payload["timeout"] = timeout
        return await client.request(
            "POST",
            "/v1/exec",
            json=payload,
            timeout=(timeout + EXEC_TIMEOUT_PADDING) if timeout else DEFAULT_TIMEOUT * 10,
        )

    @mcp.tool()
    async def compute_session_create(
        target: str,
        cwd: str | None = None,
        columns: int = 160,
        rows: int = 50,
    ) -> dict:
        """Create an independent persistent PTY session on a target.

        Use for builds, tests, debuggers and interactive programs. Returns a
        session_id used by the other compute_session_* tools.
        """
        return await client.request(
            "POST",
            "/v1/sessions",
            json={"target": target, "cwd": cwd, "columns": columns, "rows": rows},
        )

    @mcp.tool()
    async def compute_session_write(session_id: str, data: str) -> dict:
        """Send input (including a trailing newline) to a PTY session."""
        return await client.request(
            "POST", f"/v1/sessions/{session_id}/write", json={"data": data}
        )

    @mcp.tool()
    async def compute_session_read(session_id: str, max_bytes: int = 0) -> dict:
        """Read and clear buffered output from a PTY session.

        `max_bytes=0` drains everything currently buffered. `exit_status` is
        set once the remote process has exited.
        """
        return await client.request(
            "POST", f"/v1/sessions/{session_id}/read", json={"max_bytes": max_bytes}
        )

    @mcp.tool()
    async def compute_session_resize(session_id: str, columns: int, rows: int) -> dict:
        """Resize a PTY session's terminal."""
        return await client.request(
            "POST",
            f"/v1/sessions/{session_id}/resize",
            json={"columns": columns, "rows": rows},
        )

    @mcp.tool()
    async def compute_session_close(session_id: str) -> dict:
        """Close a PTY session (terminates the remote process)."""
        return await client.request("DELETE", f"/v1/sessions/{session_id}")

    @mcp.tool()
    async def compute_sessions(target: str | None = None) -> dict:
        """List sessions owned by this client, optionally filtered by target."""
        params = {"target": target} if target else None
        return await client.request("GET", "/v1/sessions", params=params)

    @mcp.tool()
    async def compute_file_read(target: str, path: str) -> dict:
        """Read a file inside a remote development container (UTF-8, lossy).

        Paths are inside the container, never on the gateway host.
        """
        return await client.request(
            "GET", "/v1/files/read", params={"target": target, "path": path}
        )

    @mcp.tool()
    async def compute_file_read_base64(target: str, path: str) -> dict:
        """Read a binary file inside a remote container as base64."""
        return await client.request(
            "GET",
            "/v1/files/read",
            params={"target": target, "path": path, "encoding": "base64"},
        )

    @mcp.tool()
    async def compute_file_write(
        target: str, path: str, content: str, encoding: str = "utf-8"
    ) -> dict:
        """Write text (or base64 when encoding='base64') to a file in a container."""
        return await client.request(
            "PUT",
            "/v1/files/write",
            params={"target": target},
            json={"path": path, "content": content, "encoding": encoding},
        )

    @mcp.tool()
    async def compute_file_list(target: str, path: str) -> dict:
        """List a directory inside a remote development container."""
        return await client.request(
            "GET", "/v1/files/list", params={"target": target, "path": path}
        )

    @mcp.tool()
    async def compute_file_stat(target: str, path: str) -> dict:
        """Stat a path inside a remote development container."""
        return await client.request(
            "GET", "/v1/files/stat", params={"target": target, "path": path}
        )

    @mcp.tool()
    async def compute_file_mkdir(target: str, path: str) -> dict:
        """Create a directory (and parents) inside a remote container."""
        return await client.request(
            "POST", "/v1/files/mkdir", params={"target": target}, json={"path": path}
        )

    @mcp.tool()
    async def compute_file_remove(target: str, path: str) -> dict:
        """Remove a file or directory inside a remote container."""
        return await client.request(
            "POST", "/v1/files/remove", params={"target": target}, json={"path": path}
        )

    @mcp.tool()
    async def compute_file_rename(
        target: str, source: str, destination: str
    ) -> dict:
        """Rename/move a path inside a remote container."""
        return await client.request(
            "POST",
            "/v1/files/rename",
            params={"target": target},
            json={"source": source, "destination": destination},
        )

    @mcp.tool()
    async def compute_file_upload(
        target: str,
        local_path: str,
        remote_path: str,
        append: bool = False,
        parents: bool = False,
    ) -> dict:
        """Upload a file that already exists in this Terok container to a path
        inside a remote development container.

        The file is streamed (never base64-encoded into the response), so it is
        suitable for large binaries and build artifacts. `local_path` is read
        from this container; `remote_path` is written in the remote container.
        """
        if not os.path.isfile(local_path):
            raise ValueError(f"local file not found: {local_path}")
        length = os.path.getsize(local_path)
        with open(local_path, "rb") as handle:
            return await client.upload(
                target, remote_path, handle, length, append=append, parents=parents
            )

    @mcp.tool()
    async def compute_file_download(
        target: str, remote_path: str, local_path: str
    ) -> dict:
        """Download a file from a remote development container into this Terok
        container, streaming it to `local_path`.

        Use this for large files and build artifacts; it does not place file
        bytes in the tool response.
        """
        os.makedirs(os.path.dirname(os.path.abspath(local_path)), exist_ok=True)
        with open(local_path, "wb") as handle:
            return await client.download(target, remote_path, handle)

    return mcp


def main() -> None:
    client = _client_from_env()
    mcp = build_server(client)
    try:
        mcp.run("stdio")
    finally:
        asyncio.run(client.close())


if __name__ == "__main__":
    main()
