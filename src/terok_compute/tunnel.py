# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""SSH tunnel management with route failover.

Each tunnel is a plain ``ssh -N -L ...`` subprocess launched with
``asyncio.create_subprocess_exec`` (never ``shell=True``).  Configuration
strings are never concatenated into a shell command.

In ``direct`` transport no tunnel is started; the target is reached directly at
``remote_host:remote_port``.  This is useful when the gateway already runs next
to the container or in tests, and keeps the same state machine for both modes.
"""

from __future__ import annotations

import asyncio
import logging
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone

from .config import SSHConfig, TargetConfig, TransportConfig

log = logging.getLogger("terok_compute.tunnel")


class TunnelError(RuntimeError):
    pass


@dataclass
class Tunnel:
    """A managed local-to-remote forwarding for one target."""

    target: TargetConfig
    route: str | None
    local_port: int
    process: asyncio.subprocess.Process | None = None

    @property
    def is_direct(self) -> bool:
        return self.target.transport.kind == "direct"

    async def stop(self) -> None:
        if self.process is None:
            return
        if self.process.returncode is None:
            try:
                self.process.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(self.process.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    self.process.kill()
                except ProcessLookupError:
                    pass
                await self.process.wait()
        self.process = None


def _probe_once(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=0.75):
            return True
    except OSError:
        return False


async def probe(host: str, port: int, timeout: float = 8.0) -> bool:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if await loop.run_in_executor(None, _probe_once, host, port):
            return True
        if loop.time() >= deadline:
            return False
        await asyncio.sleep(0.2)


def allocate_loopback_port(ssh: SSHConfig, reserved: set[int]) -> int:
    """Allocate a loopback port from the configured range, avoiding ``reserved``."""

    for port in range(ssh.internal_port_min, ssh.internal_port_max + 1):
        if port in reserved:
            continue
        sock = socket.socket()
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind(("127.0.0.1", port))
        except OSError:
            continue
        finally:
            sock.close()
        reserved.add(port)
        return port
    raise TunnelError("no free internal port in configured range")


class TunnelManager:
    """Owns one :class:`Tunnel` per target and handles failover."""

    def __init__(self, ssh: SSHConfig) -> None:
        self.ssh = ssh
        self._reserved: set[int] = set()

    async def open_for_route(
        self,
        target: TargetConfig,
        route: str,
        local_port: int,
    ) -> Tunnel:
        transport = target.transport
        if transport.kind == "direct":
            if not await probe(transport.remote_host, transport.remote_port):
                raise TunnelError(
                    f"direct endpoint {transport.remote_host}:{transport.remote_port} unreachable"
                )
            return Tunnel(target=target, route=None, local_port=transport.remote_port)

        argv = ["ssh"]
        if self.ssh.config:
            argv += ["-F", self.ssh.config]
        argv += [
            "-N",
            "-o", "BatchMode=yes",
            "-o", "ExitOnForwardFailure=yes",
            "-o", f"ConnectTimeout={int(self.ssh.connect_timeout)}",
            "-o", f"ServerAliveInterval={self.ssh.server_alive_interval}",
            "-o", f"ServerAliveCountMax={self.ssh.server_alive_count_max}",
            "-L",
            f"127.0.0.1:{local_port}:{transport.remote_host}:{transport.remote_port}",
            route,
        ]
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        tunnel = Tunnel(target=target, route=route, local_port=local_port, process=process)

        if not await probe("127.0.0.1", local_port):
            stderr = await _drain_stderr(process)
            await tunnel.stop()
            raise TunnelError(
                f"route {route!r} did not establish forwarding"
                + (f": {stderr}" if stderr else "")
            )
        return tunnel

    async def connect(
        self,
        target: TargetConfig,
        on_route: callable | None = None,
    ) -> Tunnel:
        """Try each configured route in order; return the first working tunnel."""

        transport = target.transport
        if transport.kind == "direct":
            return await self.open_for_route(target, "direct", transport.remote_port)

        last_error: Exception | None = None
        for route in transport.ssh_targets:
            local_port = allocate_loopback_port(self.ssh, self._reserved)
            try:
                tunnel = await self.open_for_route(target, route, local_port)
                log.info("target %s connected via route %s on 127.0.0.1:%d",
                         target.name, route, local_port)
                return tunnel
            except TunnelError as exc:
                last_error = exc
                self._reserved.discard(local_port)
                log.warning("target %s route %s failed: %s", target.name, route, exc)
                if on_route is not None:
                    on_route(route, exc)
        raise TunnelError(
            f"no working route for target {target.name!r}: {last_error}"
        )

    def release(self, tunnel: Tunnel) -> None:
        if tunnel.route is not None:
            self._reserved.discard(tunnel.local_port)

    @property
    def reserved(self) -> frozenset[int]:
        return frozenset(self._reserved)


async def _drain_stderr(process: asyncio.subprocess.Process) -> str:
    if process.stderr is None:
        return ""
    try:
        data = await asyncio.wait_for(process.stderr.read(), timeout=2)
    except (asyncio.TimeoutError, Exception):  # noqa: BLE001
        return ""
    return data.decode(errors="replace").strip()


def utcnow() -> datetime:
    return datetime.now(timezone.utc)
