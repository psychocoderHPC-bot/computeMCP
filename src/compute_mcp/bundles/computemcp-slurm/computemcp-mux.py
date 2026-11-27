#!/usr/bin/env python3
"""Login-node multiplexer helper for the computeMCP single-channel proxy.

The gateway runs this file as a user process on the login node over one binary
SSH session channel (stdin/stdout, no PTY).  It speaks the frame protocol
implemented by ``compute_mcp.mux`` on the gateway side and, for every ``OPEN``
control message, dials the local job relay registered for that ``job_id``.  It
then pumps opaque bytes in both directions with credit flow control, directional
EOF and reset.

This helper must run on a login node, so it is deliberately standard-library
only and compatible with Python 3.8.  Usage::

    computemcp-mux.py --registry /path/to/registry.json
    computemcp-mux.py --fd 3 --registry /path/to/registry.json

The registry is a JSON object mapping a computeMCP ``job_id`` to a relay
endpoint.  An endpoint may be ``"127.0.0.1:PORT"`` or
``{"host": "127.0.0.1", "port": PORT}``.  Hosts must be loopback; the helper
never dials an arbitrary destination and never shells out.
"""

import argparse
import asyncio
import contextlib
import ipaddress
import json
import os
import socket
import struct
import sys

MIN_PYTHON = (3, 8)
PROTOCOL_VERSION = 1
MIN_PROTOCOL_VERSION = 1

HEADER = struct.Struct("!BBHII")
HEADER_SIZE = HEADER.size

VERSION = 0x01
OPEN = 0x02
OPEN_OK = 0x03
OPEN_ERR = 0x04
CREDIT = 0x05
EOF = 0x06
RESET = 0x07
PING = 0x08
PONG = 0x09
DATA = 0x10

MAX_DATA_FRAME = 65536
MAX_CONTROL_PAYLOAD = 1024 * 1024
ABSOLUTE_MAX_PAYLOAD = 16 * 1024 * 1024
RECV_WINDOW = 256 * 1024
CREDIT_THRESHOLD = 64 * 1024
REGISTRY_MAX_BYTES = 4 * 1024 * 1024
CONNECT_TIMEOUT = 10.0


class ProtocolError(Exception):
    """The gateway violated the wire protocol."""


def check_python(version_info):
    """Abort with a clear message when the interpreter is too old."""
    major = version_info[0]
    minor = version_info[1]
    if (major, minor) < MIN_PYTHON:
        sys.stderr.write(
            "computemcp-mux: Python >= %d.%d is required, found %d.%d. "
            "Load a suitable Python module on the login node before launching "
            "the helper.\n"
            % (MIN_PYTHON[0], MIN_PYTHON[1], major, minor)
        )
        raise SystemExit(2)


def _require_loopback(host):
    if host == "localhost":
        return
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        raise ValueError(
            "registry endpoint host must be a loopback IP literal or localhost"
        )
    if not address.is_loopback:
        raise ValueError("registry endpoint host is not loopback: %s" % host)


def parse_endpoint(value):
    """Return ``(host, port)`` for a registry endpoint, rejecting non-loopback."""
    port_text = None
    if isinstance(value, str):
        host, sep, port_text = value.rpartition(":")
        if not sep:
            raise ValueError("endpoint must be 'host:port'")
    elif isinstance(value, dict):
        host = value.get("host", "127.0.0.1")
        if "port" not in value:
            raise ValueError("endpoint object has no port")
        port_text = str(value["port"])
    elif isinstance(value, (list, tuple)) and len(value) == 2:
        host = value[0]
        port_text = str(value[1])
    else:
        raise ValueError("unsupported endpoint value")
    if not host:
        host = "127.0.0.1"
    if not isinstance(host, str):
        raise ValueError("endpoint host must be a string")
    try:
        port = int(port_text)
    except (TypeError, ValueError):
        raise ValueError("endpoint port is not an integer")
    if not 1 <= port <= 65535:
        raise ValueError("endpoint port out of range")
    _require_loopback(host)
    return host, port


def load_registry(path):
    """Read and validate the job_id -> relay endpoint registry."""
    if not path:
        raise ValueError("no registry path configured")
    with open(path, "rb") as handle:
        raw = handle.read(REGISTRY_MAX_BYTES + 1)
    if len(raw) > REGISTRY_MAX_BYTES:
        raise ValueError("registry file is too large")
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError("registry is not valid JSON: %s" % exc)
    if not isinstance(data, dict):
        raise ValueError("registry must be a JSON object")
    registry = {}
    for job_id, endpoint in data.items():
        if not isinstance(job_id, str) or not job_id:
            continue
        try:
            registry[job_id] = parse_endpoint(endpoint)
        except ValueError:
            continue
    return registry


def encode_frame(frame_type, stream_id, payload=b""):
    return HEADER.pack(
        PROTOCOL_VERSION, frame_type, 0, stream_id, len(payload)
    ) + payload


async def read_exactly(reader, count):
    readexactly = getattr(reader, "readexactly", None)
    if readexactly is not None:
        try:
            return await readexactly(count)
        except asyncio.IncompleteReadError:
            return None
    chunks = []
    remaining = count
    while remaining:
        chunk = await reader.read(remaining)
        if not chunk:
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _parse_json(payload):
    try:
        body = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ProtocolError("invalid control payload: %s" % exc)
    if not isinstance(body, dict):
        raise ProtocolError("control payload is not a JSON object")
    return body


def _short(text, limit=200):
    text = " ".join(str(text).split())
    if len(text) > limit:
        text = text[: limit - 3] + "..."
    return text


class HelperStream:
    """One opened job relay plus the pump tasks that bridge it to the gateway."""

    def __init__(self, helper, stream_id, relay_reader, relay_writer, job_id):
        self.helper = helper
        self.stream_id = stream_id
        self.relay_reader = relay_reader
        self.relay_writer = relay_writer
        self.job_id = job_id

        self._in_queue = []
        self._in_bytes = 0
        self._in_wake = asyncio.Event()
        self._recv_credit = RECV_WINDOW
        self._consumed_since_credit = 0

        self._send_credit = 0
        self._send_wake = asyncio.Event()

        self._eof_from_gateway = False
        self._relay_write_closed = False
        self._eof_to_gateway_sent = False
        self._closed = False
        self._tasks = []

    def start(self):
        self._tasks = [
            asyncio.ensure_future(self._relay_reader()),
            asyncio.ensure_future(self._relay_writer()),
        ]

    # -- gateway -> helper ----------------------------------------------

    def feed_data(self, payload):
        if self._closed:
            raise ProtocolError("DATA on a closed stream")
        if self._eof_from_gateway:
            raise ProtocolError("DATA after gateway EOF")
        if len(payload) > self._recv_credit:
            raise ProtocolError(
                "peer sent %d bytes with %d credit"
                % (len(payload), self._recv_credit)
            )
        self._in_queue.append(payload)
        self._in_bytes += len(payload)
        self._recv_credit -= len(payload)
        if self._in_bytes > RECV_WINDOW:
            raise ProtocolError("receive buffer exceeded its window")
        self._in_wake.set()

    def add_credit(self, credit):
        self._send_credit += credit
        self._send_wake.set()

    def gateway_eof(self):
        self._eof_from_gateway = True
        self._in_wake.set()

    def gateway_reset(self):
        self._finalize()

    # -- pumps -----------------------------------------------------------

    async def _grant_credit(self, amount):
        if amount <= 0:
            return
        self._consumed_since_credit += amount
        if self._consumed_since_credit >= CREDIT_THRESHOLD:
            grant = self._consumed_since_credit
            self._consumed_since_credit = 0
            await self.helper.send_credit(self.stream_id, grant)

    async def _relay_reader(self):
        try:
            while not self._closed:
                while self._send_credit <= 0 and not self._closed:
                    self._send_wake.clear()
                    if self._send_credit > 0 or self._closed:
                        break
                    await self._send_wake.wait()
                if self._closed:
                    return
                take = min(MAX_DATA_FRAME, self._send_credit)
                try:
                    data = await self.relay_reader.read(take)
                except (OSError, asyncio.IncompleteReadError):
                    data = b""
                if not data:
                    await self._send_eof()
                    return
                self._send_credit -= len(data)
                await self.helper.send_data(self.stream_id, data)
        except asyncio.CancelledError:
            raise
        except ProtocolError:
            raise
        except Exception as exc:  # noqa: BLE001 - reported on the wire
            self._relay_failed("relay read failed: %s" % _short(exc))

    async def _relay_writer(self):
        try:
            while not self._closed:
                if not self._in_queue:
                    if self._eof_from_gateway:
                        await self._half_close_write()
                        return
                    self._in_wake.clear()
                    if self._in_queue or self._closed or self._eof_from_gateway:
                        continue
                    await self._in_wake.wait()
                    continue
                chunk = self._in_queue.pop(0)
                self._in_bytes -= len(chunk)
                try:
                    self.relay_writer.write(chunk)
                    drain = getattr(self.relay_writer, "drain", None)
                    if drain is not None:
                        await drain()
                except (OSError, RuntimeError) as exc:
                    self._relay_failed("relay write failed: %s" % _short(exc))
                    return
                await self._grant_credit(len(chunk))
        except asyncio.CancelledError:
            raise
        except ProtocolError:
            raise
        except Exception as exc:  # noqa: BLE001 - reported on the wire
            self._relay_failed("relay writer failed: %s" % _short(exc))

    async def _half_close_write(self):
        if self._relay_write_closed:
            return
        self._relay_write_closed = True
        try:
            write_eof = getattr(self.relay_writer, "write_eof", None)
            if write_eof is not None:
                write_eof()
            else:
                sock = self.relay_writer.get_extra_info("socket")
                if sock is not None:
                    sock.shutdown(socket.SHUT_WR)
        except (OSError, RuntimeError, AttributeError):
            pass
        self._maybe_cleanup()

    async def _send_eof(self):
        if self._eof_to_gateway_sent or self._closed:
            return
        self._eof_to_gateway_sent = True
        with contextlib.suppress(OSError):
            await self.helper.send_frame(EOF, self.stream_id, b"")
        self._maybe_cleanup()

    def _maybe_cleanup(self):
        if (
            self._eof_from_gateway
            and self._relay_write_closed
            and self._eof_to_gateway_sent
        ):
            self._finalize()

    def _relay_failed(self, reason):
        if self._closed:
            return
        task = asyncio.ensure_future(
            self.helper.send_reset(self.stream_id, reason)
        )
        self.helper.track(task)
        self._finalize()

    def _finalize(self):
        if self._closed:
            return
        self._closed = True
        self._in_queue = []
        self._in_bytes = 0
        self._in_wake.set()
        self._send_wake.set()
        with contextlib.suppress(Exception):
            self.relay_writer.close()
        current = asyncio.current_task()
        for task in self._tasks:
            if task is not current and not task.done():
                task.cancel()
        self.helper.remove(self.stream_id)

    def close(self):
        self._finalize()


class Helper:
    def __init__(self, reader, writer, registry_path):
        self.reader = reader
        self.writer = writer
        self.registry_path = registry_path
        self._write_lock = asyncio.Lock()
        self._streams = {}
        self._pending = set()
        self._tasks = set()
        self._closed = False
        self._negotiated = None

    # -- output ----------------------------------------------------------

    async def send_frame(self, frame_type, stream_id, payload=b""):
        data = encode_frame(frame_type, stream_id, payload)
        async with self._write_lock:
            if self._closed:
                return
            self.writer.write(data)
            drain = getattr(self.writer, "drain", None)
            if drain is not None:
                await drain()

    async def send_control(self, frame_type, kind, **fields):
        body = {"type": kind}
        body.update(fields)
        payload = json.dumps(body, separators=(",", ":")).encode("utf-8")
        await self.send_frame(frame_type, 0, payload)

    async def send_data(self, stream_id, data):
        await self.send_frame(DATA, stream_id, data)

    async def send_credit(self, stream_id, amount):
        if amount > 0:
            await self.send_control(
                CREDIT, "CREDIT", stream_id=stream_id, credit=amount
            )

    async def send_reset(self, stream_id, reason):
        await self.send_control(RESET, "RESET", reason=_short(reason))

    def track(self, task):
        self._tasks.add(task)
        try:
            task.add_done_callback(self._tasks.discard)
        except AttributeError:  # pragma: no cover - very old loop objects
            pass

    def remove(self, stream_id):
        self._streams.pop(stream_id, None)

    # -- lifecycle -------------------------------------------------------

    async def run(self):
        await self.send_control(
            VERSION,
            "VERSION",
            version=PROTOCOL_VERSION,
            min_version=MIN_PROTOCOL_VERSION,
        )
        while not self._closed:
            frame = await self._read_frame()
            if frame is None:
                break
            await self._dispatch(frame)

    async def _read_frame(self):
        header = await read_exactly(self.reader, HEADER_SIZE)
        if header is None:
            return None
        version, frame_type, flags, stream_id, length = HEADER.unpack(header)
        if length > ABSOLUTE_MAX_PAYLOAD:
            raise ProtocolError(
                "declared payload %d exceeds absolute limit" % length
            )
        if frame_type == DATA:
            limit = MAX_DATA_FRAME
        else:
            limit = MAX_CONTROL_PAYLOAD
        payload = b""
        if length:
            payload = await read_exactly(self.reader, length)
            if payload is None:
                return None
        if length > limit:
            if frame_type == DATA:
                with contextlib.suppress(Exception):
                    await self.send_reset(
                        stream_id, "oversize DATA frame %d > %d" % (length, limit)
                    )
                return (version, frame_type, flags, stream_id, b"")
            raise ProtocolError("oversize control frame %d > %d" % (length, limit))
        return (version, frame_type, flags, stream_id, payload)

    async def _dispatch(self, frame):
        version, frame_type, _flags, stream_id, payload = frame
        if self._negotiated is None:
            if frame_type != VERSION:
                raise ProtocolError("first frame must be VERSION")
            body = _parse_json(payload)
            try:
                peer_version = int(body.get("version", 0))
                peer_min = int(body.get("min_version", 1))
            except (TypeError, ValueError):
                raise ProtocolError("VERSION version fields are not integers")
            if peer_min > PROTOCOL_VERSION or peer_version < MIN_PROTOCOL_VERSION:
                raise ProtocolError(
                    "version mismatch: peer %d/min %d, local %d/min %d"
                    % (
                        peer_version,
                        peer_min,
                        PROTOCOL_VERSION,
                        MIN_PROTOCOL_VERSION,
                    )
                )
            self._negotiated = min(peer_version, PROTOCOL_VERSION)
            return
        if version != PROTOCOL_VERSION:
            raise ProtocolError(
                "frame version %d != negotiated %d"
                % (version, self._negotiated)
            )

        if frame_type == DATA:
            await self._handle_data(stream_id, payload)
        elif frame_type == OPEN:
            await self._handle_open(payload)
        elif frame_type == CREDIT:
            await self._handle_credit(payload)
        elif frame_type == EOF:
            self._handle_eof(stream_id)
        elif frame_type == RESET:
            self._handle_reset(stream_id, payload)
        elif frame_type == PING:
            await self.send_control(PONG, "PONG")
        elif frame_type == PONG:
            return
        elif frame_type == VERSION:
            raise ProtocolError("duplicate VERSION frame")
        elif frame_type in (OPEN_OK, OPEN_ERR):
            raise ProtocolError("helper does not open streams")
        else:
            raise ProtocolError("unknown frame type %d" % frame_type)

    async def _handle_data(self, stream_id, payload):
        if stream_id == 0:
            raise ProtocolError("DATA on the control stream")
        stream = self._streams.get(stream_id)
        if stream is None:
            await self.send_reset(stream_id, "unknown stream")
            return
        try:
            stream.feed_data(payload)
        except ProtocolError as exc:
            await self.send_reset(stream_id, str(exc))
            stream.gateway_reset()

    async def _handle_open(self, payload):
        body = _parse_json(payload)
        try:
            stream_id = int(body["stream_id"])
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("OPEN without a valid stream_id")
        job_id = body.get("job_id")
        if job_id is not None and not isinstance(job_id, str):
            raise ProtocolError("OPEN job_id must be a string")
        if stream_id <= 0 or stream_id > 0xFFFFFFFF:
            raise ProtocolError("OPEN stream_id out of range")
        if stream_id in self._streams or stream_id in self._pending:
            await self.send_control(
                OPEN_ERR,
                "OPEN_ERR",
                stream_id=stream_id,
                error="stream already open",
            )
            return
        self._pending.add(stream_id)
        task = asyncio.ensure_future(self._open_stream(stream_id, job_id))
        self.track(task)

    async def _open_stream(self, stream_id, job_id):
        relay_reader = None
        relay_writer = None
        try:
            registry = load_registry(self.registry_path)
            endpoint = registry.get(job_id)
            if endpoint is None:
                raise LookupError("job %r is not in the relay registry" % job_id)
            relay_reader, relay_writer = await asyncio.wait_for(
                asyncio.open_connection(endpoint[0], endpoint[1]),
                CONNECT_TIMEOUT,
            )
        except Exception as exc:  # noqa: BLE001 - reported to the gateway
            if relay_writer is not None:
                with contextlib.suppress(Exception):
                    relay_writer.close()
            await self.send_control(
                OPEN_ERR,
                "OPEN_ERR",
                stream_id=stream_id,
                error=_short(exc),
            )
            return
        finally:
            self._pending.discard(stream_id)
        if self._closed:
            with contextlib.suppress(Exception):
                relay_writer.close()
            return
        stream = HelperStream(
            self, stream_id, relay_reader, relay_writer, job_id
        )
        self._streams[stream_id] = stream
        await self.send_control(OPEN_OK, "OPEN_OK", stream_id=stream_id)
        await self.send_credit(stream_id, RECV_WINDOW)
        stream.start()

    async def _handle_credit(self, payload):
        body = _parse_json(payload)
        try:
            stream_id = int(body["stream_id"])
            credit = int(body["credit"])
        except (KeyError, TypeError, ValueError):
            raise ProtocolError("CREDIT without stream_id/credit")
        if credit <= 0:
            raise ProtocolError("CREDIT must be positive")
        stream = self._streams.get(stream_id)
        if stream is None:
            await self.send_reset(stream_id, "CREDIT for unknown stream")
            return
        stream.add_credit(credit)

    def _handle_eof(self, stream_id):
        if stream_id == 0:
            raise ProtocolError("EOF on the control stream")
        stream = self._streams.get(stream_id)
        if stream is None:
            return
        stream.gateway_eof()

    def _handle_reset(self, stream_id, payload):
        if stream_id == 0:
            raise ProtocolError("RESET on the control stream")
        stream = self._streams.get(stream_id)
        if stream is not None:
            stream.gateway_reset()

    # -- teardown --------------------------------------------------------

    async def close(self):
        if self._closed:
            return
        self._closed = True
        stream_tasks = []
        for stream in list(self._streams.values()):
            stream_tasks.extend(stream._tasks)
            stream.close()
        self._streams.clear()
        tasks = [
            task
            for task in list(self._tasks) + stream_tasks
            if not task.done() and task is not asyncio.current_task()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), 2.0
                )
        close = getattr(self.writer, "close", None)
        if close is not None:
            with contextlib.suppress(Exception):
                close()
        wait_closed = getattr(self.writer, "wait_closed", None)
        if wait_closed is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(wait_closed(), 2.0)


async def _control_streams(args):
    if args.fd is not None:
        sock = socket.socket(
            socket.AF_UNIX, socket.SOCK_STREAM, fileno=args.fd
        )
        sock.setblocking(False)
        return await asyncio.open_connection(sock=sock)
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    transport, writer_protocol = await loop.connect_write_pipe(
        asyncio.streams.FlowControlMixin, sys.stdout.buffer
    )
    writer = asyncio.StreamWriter(transport, writer_protocol, reader, loop)
    return reader, writer


async def _amain(args):
    reader, writer = await _control_streams(args)
    helper = Helper(reader, writer, args.registry)
    try:
        await helper.run()
    finally:
        await helper.close()


def build_parser():
    parser = argparse.ArgumentParser(
        description=(
            "computeMCP login-node multiplexer helper. It reads framed control "
            "messages on stdin (or --fd) and serves OPEN requests by dialing "
            "registered loopback job relays."
        )
    )
    parser.add_argument(
        "--registry",
        default=os.environ.get("COMPUTEMCP_MUX_REGISTRY"),
        help="path to the job_id -> relay endpoint registry (JSON)",
    )
    parser.add_argument(
        "--fd",
        type=int,
        default=None,
        help="use this connected socket file descriptor instead of stdin/stdout",
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="print the protocol version and exit",
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.version:
        print("computemcp-mux protocol %d" % PROTOCOL_VERSION)
        return 0
    check_python(tuple(sys.version_info[:2]))
    if not args.registry:
        sys.stderr.write(
            "computemcp-mux: --registry or COMPUTEMCP_MUX_REGISTRY is required\n"
        )
        return 2
    try:
        asyncio.run(_amain(args))
    except (BrokenPipeError, ConnectionResetError):
        return 0
    except KeyboardInterrupt:
        return 130
    except ProtocolError as exc:
        sys.stderr.write("computemcp-mux: protocol error: %s\n" % exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
