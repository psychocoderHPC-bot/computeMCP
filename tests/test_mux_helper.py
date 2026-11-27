# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""End-to-end tests for the stdlib login-node multiplexer helper.

Each test spawns ``computemcp-mux.py`` as a real subprocess, hands it one end of
a Unix socketpair through ``--fd``, and talks to it with the gateway-side
:class:`compute_mcp.mux.Multiplexer`.  Fake loopback echo servers stand in for
the per-job relays.  This proves wire compatibility between the two
implementations, not just the gateway side against itself.
"""

import asyncio
import importlib.util
import json
import pathlib
import socket
import subprocess
import sys

import pytest

from compute_mcp.mux import Multiplexer, OpenError

HELPER = (
    pathlib.Path(__file__).resolve().parents[1]
    / "src"
    / "compute_mcp"
    / "bundles"
    / "computemcp-slurm"
    / "computemcp-mux.py"
)
TIMEOUT = 10.0


def load_helper_module():
    spec = importlib.util.spec_from_file_location(
        "computemcp_mux_helper", str(HELPER)
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class RelayServer:
    """Tiny loopback echo server that identifies itself with a label."""

    def __init__(self, label):
        self.label = label
        self._server = None
        self.port = None
        self._writers = set()

    async def start(self):
        self._server = await asyncio.start_server(
            self._handle, "127.0.0.1", 0
        )
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def _handle(self, reader, writer):
        self._writers.add(writer)
        try:
            writer.write(self.label)
            await writer.drain()
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except (OSError, asyncio.CancelledError):
            pass
        finally:
            self._writers.discard(writer)
            writer.close()

    async def stop(self):
        if self._server is not None:
            self._server.close()
            # Python 3.12 ``wait_closed`` waits for live connections, so close
            # them explicitly before waiting.
            for writer in list(self._writers):
                writer.close()
            await self._server.wait_closed()


async def spawn_helper(registry_path):
    parent, child = socket.socketpair()
    child_fd = child.fileno()
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(HELPER),
        "--fd",
        str(child_fd),
        "--registry",
        str(registry_path),
        pass_fds=(child_fd,),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    child.close()
    parent.setblocking(False)
    reader, writer = await asyncio.open_connection(sock=parent)
    mux = Multiplexer(reader, writer, initiator=True)
    await mux.start()
    return proc, mux


async def stop_helper(proc, mux):
    await mux.close()
    try:
        await asyncio.wait_for(proc.wait(), TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()


def write_registry(tmp_path, mapping):
    path = tmp_path / "registry.json"
    path.write_text(json.dumps(mapping), encoding="utf-8")
    return path


async def read_exactly(stream, size):
    received = bytearray()
    while len(received) < size:
        chunk = await asyncio.wait_for(
            stream.read(size - len(received)), TIMEOUT
        )
        if not chunk:
            break
        received += chunk
    return bytes(received)


async def test_open_routes_to_the_right_relay_and_streams_are_isolated(tmp_path):
    relay_a = await RelayServer(b"A:").start()
    relay_b = await RelayServer(b"B:").start()
    registry = write_registry(
        tmp_path,
        {
            "job-a": "127.0.0.1:%d" % relay_a.port,
            "job-b": {"host": "127.0.0.1", "port": relay_b.port},
        },
    )
    proc, mux = await spawn_helper(registry)
    try:
        stream_a = await mux.open_stream("job-a", timeout=TIMEOUT)
        assert await read_exactly(stream_a, 2) == b"A:"

        stream_b = await mux.open_stream("job-b", timeout=TIMEOUT)
        assert await read_exactly(stream_b, 2) == b"B:"

        # A write on one stream must never surface on the other.
        await stream_a.write(b"only-a")
        assert await read_exactly(stream_a, 6) == b"only-a"
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(stream_b.read(1), 0.2)

        await stream_b.write(b"only-b")
        assert await read_exactly(stream_b, 6) == b"only-b"
    finally:
        await stop_helper(proc, mux)
        await relay_a.stop()
        await relay_b.stop()


async def test_eof_propagates_through_the_helper(tmp_path):
    relay = await RelayServer(b"R:").start()
    registry = write_registry(tmp_path, {"job-eof": "127.0.0.1:%d" % relay.port})
    proc, mux = await spawn_helper(registry)
    try:
        stream = await mux.open_stream("job-eof", timeout=TIMEOUT)
        assert await read_exactly(stream, 2) == b"R:"
        await stream.write(b"ping")
        assert await read_exactly(stream, 4) == b"ping"

        await stream.close()  # half-close; the relay then closes its side
        assert await asyncio.wait_for(stream.read(16), TIMEOUT) == b""
    finally:
        await stop_helper(proc, mux)
        await relay.stop()


async def test_unknown_job_returns_open_error(tmp_path):
    registry = write_registry(tmp_path, {})
    proc, mux = await spawn_helper(registry)
    try:
        with pytest.raises(OpenError):
            await mux.open_stream("missing-job", timeout=TIMEOUT)
        # The helper and connection survive a rejected OPEN.
        live = await RelayServer(b"L:").start()
        registry.write_text(
            json.dumps({"job-live": "127.0.0.1:%d" % live.port}),
            encoding="utf-8",
        )
        stream = await mux.open_stream("job-live", timeout=TIMEOUT)
        assert await read_exactly(stream, 2) == b"L:"
        await live.stop()
    finally:
        await stop_helper(proc, mux)


async def test_helper_exits_without_deadlock_when_the_gateway_closes(tmp_path):
    relay = await RelayServer(b"D:").start()
    registry = write_registry(tmp_path, {"job-deadlock": "127.0.0.1:%d" % relay.port})
    proc, mux = await spawn_helper(registry)
    stream = await mux.open_stream("job-deadlock", timeout=TIMEOUT)
    assert await read_exactly(stream, 2) == b"D:"

    await mux.close()
    try:
        assert await asyncio.wait_for(proc.wait(), TIMEOUT) is not None
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
        await relay.stop()


def test_version_and_help_paths_do_not_need_a_registry():
    version = subprocess.run(
        [sys.executable, str(HELPER), "--version"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert version.returncode == 0
    assert "protocol 1" in version.stdout

    helped = subprocess.run(
        [sys.executable, str(HELPER), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert helped.returncode == 0
    assert "--registry" in helped.stdout


def test_old_python_check_errors_cleanly(monkeypatch, capsys):
    module = load_helper_module()
    monkeypatch.setattr(module, "MIN_PYTHON", (99, 0))
    with pytest.raises(SystemExit) as excinfo:
        module.check_python((3, 8, 0))
    assert excinfo.value.code == 2
    err = capsys.readouterr().err
    assert "Python >=" in err
    assert "99.0" in err
