# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Bounded stream multiplexer used by the single-channel Slurm proxy.

The gateway and the login-node helper speak one framed protocol over a binary
pipe (the outer SSH channel).  This module implements the gateway-side codec
and multiplexer; ``bundles/computemcp-slurm/computemcp-mux.py`` implements the
same wire format for the remote helper with the Python standard library only.

Wire format
-----------

Every frame starts with a 12-byte big-endian header::

    version  u8    protocol version of the sender
    type     u8    frame type (see :class:`FrameType`)
    flags    u16   reserved, currently always 0
    stream   u32   stream id; 0 is the control stream
    length   u32   payload length in bytes

``DATA`` frames carry raw bytes on a non-zero stream.  ``OPEN``, ``OPEN_OK``,
``OPEN_ERR``, ``CREDIT``, ``PING``, ``PONG`` and ``VERSION`` travel on stream
zero with a small UTF-8 JSON body.  ``EOF`` and ``RESET`` address a specific
stream in the header; ``EOF`` has an empty payload and ``RESET`` has an
optional JSON reason.

Design decisions that the design document left open (see section 6):

* Stream ids are namespaced by parity so both peers may open streams without a
  numeric collision.  The initiator (gateway) allocates odd ids, the acceptor
  (helper) allocates even ids.  This module only allocates ids, it never
  rejects a peer id on parity grounds.
* Flow control is credit based per stream.  When a stream opens, each side
  sends one ``CREDIT`` granting the peer its receive window.  A receiver
  returns credit only after the application consumes buffered bytes, which
  bounds the receive buffer to ``recv_window``.
* ``EOF`` is ordered *after* queued ``DATA`` for that stream.  It is therefore
  queued as a per-stream marker instead of the priority control queue, so a
  half-close never overtakes data.  ``RESET`` is terminal and uses the
  priority queue.
* A header-level framing defect (bad version, unknown type, impossible
  length) fails the whole connection, but only after a ``RESET`` on stream 0
  is written and callbacks are invoked.  A per-stream defect (credit
  violation, oversize data frame, unknown stream) resets that stream and
  leaves the connection usable.

The module is transport agnostic: it accepts any object that offers
``read``/``readexactly`` for reading and ``write``/``drain``/``close`` for
writing.  AsyncSSH streams and ``asyncio.StreamReader``/``StreamWriter``
pairs both satisfy this.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import struct
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum

log = logging.getLogger("compute_mcp.mux")

#: ``!BBHII`` -> 12 bytes in network byte order.
HEADER_STRUCT = struct.Struct("!BBHII")
HEADER_SIZE = HEADER_STRUCT.size

PROTOCOL_VERSION = 1
MIN_PROTOCOL_VERSION = 1

#: Largest ``DATA`` payload this implementation accepts or emits.
MAX_DATA_FRAME = 65536
#: Largest control (JSON) payload accepted on a control frame.
MAX_CONTROL_PAYLOAD = 1024 * 1024
#: Hard ceiling before any payload is read.  A declared length above this is a
#: fatal framing error; it must never be used to allocate a buffer.
ABSOLUTE_MAX_PAYLOAD = 16 * 1024 * 1024

CONTROL_STREAM_ID = 0

DEFAULT_RECV_WINDOW = 256 * 1024
DEFAULT_SEND_BUFFER = 256 * 1024
DEFAULT_CREDIT_THRESHOLD = 64 * 1024
DEFAULT_HANDSHAKE_TIMEOUT = 15.0
DEFAULT_OPEN_TIMEOUT = 30.0

#: Upper bound on outstanding send credit, to keep counters from overflowing.
MAX_SEND_CREDIT = 1 << 31


class FrameType(IntEnum):
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


class MuxError(Exception):
    """Base class for multiplexer failures."""


class ProtocolError(MuxError):
    """A peer violated the wire protocol."""


class ConnectionClosed(MuxError):
    """The underlying transport is gone."""


class StreamReset(MuxError):
    """The remote side reset the stream."""


class StreamClosed(MuxError):
    """The local side already half-closed the stream."""


class OpenError(MuxError):
    """The remote side rejected ``OPEN``."""


@dataclass
class MuxStats:
    """Counters that tests and operators can inspect without touching state."""

    frames_read: int = 0
    frames_written: int = 0
    bytes_read: int = 0
    bytes_written: int = 0
    protocol_errors: int = 0
    resets: int = 0
    peak_recv_buffer: int = 0
    open_streams: int = 0


@dataclass(frozen=True)
class Frame:
    version: int
    type: FrameType
    flags: int
    stream_id: int
    payload: bytes = b""

    @property
    def is_control(self) -> bool:
        return self.stream_id == CONTROL_STREAM_ID


def encode_header(
    version: int, frame_type: int, flags: int, stream_id: int, payload_length: int
) -> bytes:
    """Encode one 12-byte header, validating every field."""
    if not 0 <= version <= 0xFF:
        raise ProtocolError(f"version {version} out of range")
    if not 0 <= int(frame_type) <= 0xFF:
        raise ProtocolError(f"frame type {frame_type} out of range")
    if not 0 <= flags <= 0xFFFF:
        raise ProtocolError(f"flags {flags} out of range")
    if not 0 <= stream_id <= 0xFFFFFFFF:
        raise ProtocolError(f"stream id {stream_id} out of range")
    if not 0 <= payload_length <= 0xFFFFFFFF:
        raise ProtocolError(f"payload length {payload_length} out of range")
    return HEADER_STRUCT.pack(
        version, int(frame_type), flags, stream_id, payload_length
    )


def decode_header(data: bytes) -> tuple[int, int, int, int, int]:
    """Decode a header and return ``(version, type, flags, stream, length)``."""
    if len(data) != HEADER_SIZE:
        raise ProtocolError(f"short header: {len(data)} bytes")
    version, frame_type, flags, stream_id, payload_length = HEADER_STRUCT.unpack(
        data
    )
    return version, frame_type, flags, stream_id, payload_length


def encode_frame(
    version: int, frame_type: int, flags: int, stream_id: int, payload: bytes = b""
) -> bytes:
    return encode_header(version, frame_type, flags, stream_id, len(payload)) + payload


def _json_bytes(kind: str, **fields: object) -> bytes:
    body = {"type": kind}
    body.update(fields)
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def _parse_json(payload: bytes) -> dict:
    try:
        body = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtocolError(f"invalid control payload: {exc}") from exc
    if not isinstance(body, dict):
        raise ProtocolError("control payload is not a JSON object")
    return body


def _require_int(
    body: dict, key: str, *, minimum: int = 0, maximum: int = 0xFFFFFFFF
) -> int:
    value = body.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ProtocolError(f"control field {key!r} must be an integer")
    if not minimum <= value <= maximum:
        raise ProtocolError(
            f"control field {key!r}={value} outside [{minimum}, {maximum}]"
        )
    return value


#: Per-stream marker that orders ``EOF`` after queued data.
_EOF_SENTINEL = object()
#: Sentinel pushed onto the inbound queue when the connection closes.
_CLOSED = object()


class Stream:
    """One bidirectional byte stream carried by a :class:`Multiplexer`.

    ``read`` returns ``b""`` after the remote half-closes.  ``write`` returns
    once the bytes are accepted into the bounded send buffer; it blocks while
    the buffer or the peer's credit is exhausted.  ``close`` performs a
    half-close (sends ``EOF``); ``reset`` aborts both directions.
    """

    def __init__(
        self,
        mux: "Multiplexer",
        stream_id: int,
        *,
        job_id: str | None = None,
        outbound: bool = False,
    ) -> None:
        self._mux = mux
        self.stream_id = stream_id
        self.job_id = job_id
        self._outbound = outbound
        self._state = "opening" if outbound else "open"

        self._recv = bytearray()
        self._recv_credit = mux._recv_window
        self._consumed_since_credit = 0
        self._eof_received = False
        self._eof_sent = False
        self._local_closed = False
        self._reset = False
        self._reset_reason: str | None = None
        self._finalized = False

        self._send_queue: deque[object] = deque()
        self._send_bytes = 0
        self._send_credit = 0

        self._wake = asyncio.Event()
        self._space = asyncio.Event()
        self._space.set()
        self._open_future: asyncio.Future | None = None

    # -- introspection ---------------------------------------------------

    @property
    def opened(self) -> bool:
        return self._state == "open"

    def at_eof(self) -> bool:
        return self._eof_received and not self._recv

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return (
            f"Stream(id={self.stream_id}, state={self._state}, "
            f"recv={len(self._recv)}, send={self._send_bytes})"
        )

    # -- public API ------------------------------------------------------

    async def read(self, n: int = -1) -> bytes:
        """Read up to ``n`` bytes; ``b""`` means the remote half-closed."""
        while True:
            if self._recv:
                if n is None or n < 0:
                    data = bytes(self._recv)
                    self._recv.clear()
                else:
                    data = bytes(self._recv[:n])
                    del self._recv[:n]
                self._after_consume(len(data))
                return data
            if self._reset:
                raise StreamReset(self._reset_reason or "stream reset")
            if self._eof_received:
                return b""
            if self._mux._closed_event.is_set():
                raise ConnectionClosed("connection closed")
            self._wake.clear()
            # Re-check after clearing to close the set/clear race without an
            # intervening await (single-threaded event loop).
            if (
                self._recv
                or self._reset
                or self._eof_received
                or self._mux._closed_event.is_set()
            ):
                continue
            await self._wake.wait()

    async def write(self, data: bytes) -> None:
        """Queue ``data`` for sending, respecting credit and a bounded buffer."""
        if isinstance(data, memoryview):
            data = data.tobytes()
        elif isinstance(data, bytearray):
            data = bytes(data)
        elif not isinstance(data, bytes):
            raise TypeError("write() requires bytes")
        if self._local_closed:
            raise StreamClosed("write on a half-closed stream")
        mux = self._mux
        offset = 0
        total = len(data)
        while offset < total:
            while True:
                if self._reset:
                    raise StreamReset(self._reset_reason or "stream reset")
                if self._local_closed:
                    raise StreamClosed("stream closed during write")
                if mux._closed_event.is_set():
                    raise ConnectionClosed("connection closed")
                chunk_len = min(mux._max_data_frame, total - offset)
                if (
                    self._send_bytes == 0
                    or self._send_bytes + chunk_len <= mux._send_buffer_limit
                ):
                    break
                self._space.clear()
                if (
                    self._send_bytes == 0
                    or self._send_bytes + chunk_len <= mux._send_buffer_limit
                ):
                    continue
                await self._space.wait()
            chunk = data[offset : offset + chunk_len]
            self._send_queue.append(chunk)
            self._send_bytes += len(chunk)
            offset += chunk_len
            mux._send_ready.set()

    async def close(self) -> None:
        """Half-close: queued data is flushed, then ``EOF`` is sent."""
        if self._local_closed:
            return
        self._local_closed = True
        self._send_queue.append(_EOF_SENTINEL)
        self._space.set()
        self._wake.set()
        self._mux._send_ready.set()
        self._mux._maybe_finalize(self)

    async def reset(self, reason: str = "reset") -> None:
        """Abort the stream in both directions."""
        if self._reset:
            return
        self._mux._send_reset(self.stream_id, reason)
        self._receive_reset("local reset: " + reason)

    # -- internal --------------------------------------------------------

    def _after_consume(self, count: int) -> None:
        if count <= 0:
            return
        self._recv_credit += count
        self._consumed_since_credit += count
        if (
            not self._reset
            and self._consumed_since_credit >= self._mux._credit_threshold
        ):
            grant = self._consumed_since_credit
            self._consumed_since_credit = 0
            self._mux._enqueue_credit(self.stream_id, grant)

    def _feed_data(self, payload: bytes) -> None:
        if self._reset:
            raise ProtocolError("DATA on a reset stream")
        if self._eof_received:
            raise ProtocolError("DATA after peer EOF")
        if len(payload) > self._recv_credit:
            raise ProtocolError(
                f"peer sent {len(payload)} bytes with {self._recv_credit} credit"
            )
        self._recv.extend(payload)
        self._recv_credit -= len(payload)
        if len(self._recv) > self._mux._recv_window:
            raise ProtocolError("receive buffer exceeded its window")
        self._mux.stats.peak_recv_buffer = max(
            self._mux.stats.peak_recv_buffer, len(self._recv)
        )
        self._wake.set()

    def _receive_eof(self) -> None:
        if self._eof_received or self._reset:
            return
        self._eof_received = True
        self._wake.set()
        self._mux._maybe_finalize(self)

    def _receive_reset(self, reason: str) -> None:
        if self._reset:
            return
        self._reset = True
        self._reset_reason = reason
        self._send_queue.clear()
        self._send_bytes = 0
        self._space.set()
        self._wake.set()
        self._mux._maybe_finalize(self, force=True)


class Multiplexer:
    """Frame codec, fair scheduler and stream registry for one transport.

    Construct it with a ``reader`` and a ``writer`` pair, then ``await
    start()``.  The handshake is symmetric: both sides send one ``VERSION``
    frame and accept the peer's.  Set ``initiator=False`` on the side that
    only accepts streams (the helper) so locally allocated ids are even.
    """

    def __init__(
        self,
        reader: object,
        writer: object,
        *,
        version: int = PROTOCOL_VERSION,
        min_version: int = MIN_PROTOCOL_VERSION,
        max_data_frame: int = MAX_DATA_FRAME,
        max_control_payload: int = MAX_CONTROL_PAYLOAD,
        recv_window: int = DEFAULT_RECV_WINDOW,
        send_buffer_limit: int = DEFAULT_SEND_BUFFER,
        credit_threshold: int = DEFAULT_CREDIT_THRESHOLD,
        handshake: bool = True,
        handshake_timeout: float = DEFAULT_HANDSHAKE_TIMEOUT,
        initiator: bool = True,
        on_frame_read=None,
        on_frame_written=None,
        on_protocol_error=None,
    ) -> None:
        if not 1 <= version <= 0xFF:
            raise ValueError("version must fit in one byte")
        if not 1 <= min_version <= version:
            raise ValueError("min_version must be in [1, version]")
        if not 1 <= max_data_frame <= ABSOLUTE_MAX_PAYLOAD:
            raise ValueError("max_data_frame out of range")
        if not 1 <= max_control_payload <= ABSOLUTE_MAX_PAYLOAD:
            raise ValueError("max_control_payload out of range")
        if recv_window < 1:
            raise ValueError("recv_window must be positive")
        if credit_threshold < 1 or credit_threshold > recv_window:
            raise ValueError("credit_threshold must be in [1, recv_window]")
        if send_buffer_limit < max_data_frame:
            send_buffer_limit = max_data_frame

        self._reader = reader
        self._writer = writer
        self._version = version
        self._min_version = min_version
        self._max_data_frame = max_data_frame
        self._max_control_payload = max_control_payload
        self._recv_window = recv_window
        self._send_buffer_limit = send_buffer_limit
        self._credit_threshold = credit_threshold
        self._handshake_enabled = handshake
        self._handshake_timeout = handshake_timeout
        self._initiator = initiator
        self._on_frame_read = on_frame_read
        self._on_frame_written = on_frame_written
        self._on_protocol_error = on_protocol_error

        self._streams: dict[int, Stream] = {}
        self._incoming: asyncio.Queue = asyncio.Queue()
        self._control_queue: deque[bytes] = deque()
        self._send_ready = asyncio.Event()
        self._closed_event = asyncio.Event()
        self._write_lock = asyncio.Lock()

        self._reader_task: asyncio.Task | None = None
        self._writer_task: asyncio.Task | None = None
        self._handshake_future: asyncio.Future | None = None
        self._closed_sentinel_sent = False

        self._negotiated_version: int | None = None
        self._handshake_done = False
        self._closing = False
        self._next_stream_id = 1 if initiator else 2
        self._rr_index = 0

        self.stats = MuxStats()

    # -- lifecycle -------------------------------------------------------

    @property
    def negotiated_version(self) -> int | None:
        return self._negotiated_version

    @property
    def closed(self) -> bool:
        return self._closed_event.is_set()

    async def start(self) -> int:
        """Start the tasks and complete the version handshake."""
        if self._reader_task is not None:
            return self._negotiated_version or self._version
        loop = asyncio.get_running_loop()
        self._handshake_future = loop.create_future()
        self._reader_task = loop.create_task(self._reader_loop())
        self._writer_task = loop.create_task(self._writer_loop())
        if self._handshake_enabled:
            self._enqueue_control(
                FrameType.VERSION,
                payload=_json_bytes(
                    "VERSION", version=self._version, min_version=self._min_version
                ),
            )
            try:
                await asyncio.wait_for(
                    asyncio.shield(self._handshake_future), self._handshake_timeout
                )
            except asyncio.TimeoutError as exc:
                await self._fail_connection("version handshake timed out")
                self._discard_handshake_exception()
                raise ProtocolError("version handshake timed out") from exc
            except ConnectionClosed as exc:
                self._discard_handshake_exception()
                raise ProtocolError(f"handshake failed: {exc}") from exc
        else:
            self._negotiated_version = self._version
            self._handshake_done = True
            if not self._handshake_future.done():
                self._handshake_future.set_result(self._version)
        return self._negotiated_version or self._version

    def _discard_handshake_exception(self) -> None:
        future = self._handshake_future
        if future is not None and future.done() and not future.cancelled():
            with contextlib.suppress(Exception):
                future.exception()

    async def wait_closed(self) -> None:
        await self._closed_event.wait()

    async def close(self) -> None:
        """Shut the connection down, resetting every live stream."""
        if self._closed_event.is_set():
            return
        self._closing = True
        for stream in list(self._streams.values()):
            stream._receive_reset("multiplexer closed")
        self._streams.clear()
        self.stats.open_streams = 0
        await self._close_transport("multiplexer closed")

    # -- stream API ------------------------------------------------------

    def _allocate_stream_id(self) -> int:
        candidate = self._next_stream_id
        for _ in range(0x80000000):
            if candidate == 0:
                candidate = 2 if self._initiator else 1
            if candidate not in self._streams:
                self._next_stream_id = (candidate + 2) & 0xFFFFFFFF
                if self._next_stream_id == 0:
                    self._next_stream_id = 2 if self._initiator else 1
                return candidate
            candidate = (candidate + 2) & 0xFFFFFFFF
        raise MuxError("stream id space exhausted")

    async def open_stream(
        self,
        job_id: str | None = None,
        *,
        stream_id: int | None = None,
        timeout: float | None = DEFAULT_OPEN_TIMEOUT,
    ) -> Stream:
        """Open a stream and wait for ``OPEN_OK`` from the peer."""
        if not self._handshake_done:
            raise MuxError("open_stream() before handshake")
        if self._closing:
            raise ConnectionClosed("multiplexer closing")
        if stream_id is None:
            stream_id = self._allocate_stream_id()
        else:
            if not 1 <= stream_id <= 0xFFFFFFFF:
                raise ValueError("stream_id out of range")
            if stream_id in self._streams:
                raise ValueError(f"stream id {stream_id} already in use")

        stream = Stream(self, stream_id, job_id=job_id, outbound=True)
        self._streams[stream_id] = stream
        self.stats.open_streams = len(self._streams)
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        stream._open_future = future
        fields = {"stream_id": stream_id}
        if job_id is not None:
            fields["job_id"] = job_id
        self._enqueue_control(FrameType.OPEN, payload=_json_bytes("OPEN", **fields))
        try:
            await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            self._streams.pop(stream_id, None)
            stream._receive_reset("open timed out")
            self._send_reset(stream_id, "open timed out")
            raise
        except BaseException:
            self._streams.pop(stream_id, None)
            stream._receive_reset("open failed")
            raise
        stream._state = "open"
        stream._recv_credit = self._recv_window
        self._enqueue_credit(stream_id, self._recv_window)
        return stream

    async def accept(self) -> Stream:
        """Return the next peer-opened stream, waiting if necessary."""
        while True:
            if not self._incoming.empty():
                item = self._incoming.get_nowait()
                if item is _CLOSED:
                    self._incoming.put_nowait(_CLOSED)
                    raise ConnectionClosed("connection closed")
                return item
            if self._closed_event.is_set():
                raise ConnectionClosed("connection closed")
            get_task = asyncio.ensure_future(self._incoming.get())
            close_task = asyncio.ensure_future(self._closed_event.wait())
            try:
                done, _ = await asyncio.wait(
                    {get_task, close_task}, return_when=asyncio.FIRST_COMPLETED
                )
            finally:
                for task in (get_task, close_task):
                    if not task.done():
                        task.cancel()
                close_task.cancel()
                if close_task.done() and not close_task.cancelled():
                    with contextlib.suppress(Exception):
                        close_task.exception()
            if close_task.done() and not close_task.cancelled():
                get_task.cancel()
                raise ConnectionClosed("connection closed")
            item = get_task.result()
            if item is _CLOSED:
                self._incoming.put_nowait(_CLOSED)
                raise ConnectionClosed("connection closed")
            return item

    async def ping(self, timeout: float = DEFAULT_HANDSHAKE_TIMEOUT) -> None:
        """Round-trip a ``PING`` and wait for ``PONG``."""
        loop = asyncio.get_running_loop()
        future: asyncio.Future = loop.create_future()
        self._ping_future = future
        self._enqueue_control(FrameType.PING, payload=_json_bytes("PING"))
        try:
            await asyncio.wait_for(future, timeout)
        finally:
            self._ping_future = None

    # -- framing helpers -------------------------------------------------

    def _outgoing_version(self) -> int:
        value = self._negotiated_version or self._version
        return value & 0xFF

    def _enqueue_control(
        self,
        frame_type: FrameType,
        payload: bytes = b"",
        *,
        stream_id: int = CONTROL_STREAM_ID,
        flags: int = 0,
    ) -> None:
        version = self._outgoing_version()
        self._control_queue.append(
            encode_frame(version, frame_type, flags, stream_id, payload)
        )
        self._send_ready.set()

    def _enqueue_credit(self, stream_id: int, amount: int) -> None:
        if amount <= 0:
            return
        self._enqueue_control(
            FrameType.CREDIT,
            payload=_json_bytes("CREDIT", stream_id=stream_id, credit=amount),
        )

    def _send_reset(self, stream_id: int, reason: str) -> None:
        self.stats.resets += 1
        self._enqueue_control(
            FrameType.RESET,
            payload=_json_bytes("RESET", reason=reason),
            stream_id=stream_id,
        )

    def _maybe_finalize(self, stream: Stream, *, force: bool = False) -> None:
        if stream._finalized:
            return
        if force or (stream._eof_received and stream._eof_sent):
            stream._finalized = True
            self._streams.pop(stream.stream_id, None)
            self.stats.open_streams = len(self._streams)

    async def _write_raw(self, frame_bytes: bytes, frame: Frame | None = None) -> None:
        async with self._write_lock:
            self._writer.write(frame_bytes)
            self.stats.frames_written += 1
            self.stats.bytes_written += len(frame_bytes)
            drain = getattr(self._writer, "drain", None)
            if drain is not None:
                await drain()
            if frame is not None and self._on_frame_written is not None:
                self._on_frame_written(frame)

    async def _read_exactly(self, count: int) -> bytes | None:
        try:
            readexactly = getattr(self._reader, "readexactly", None)
            if readexactly is not None:
                return await readexactly(count)
            chunks: list[bytes] = []
            remaining = count
            while remaining:
                chunk = await self._reader.read(remaining)
                if not chunk:
                    return None
                chunks.append(chunk)
                remaining -= len(chunk)
            return b"".join(chunks)
        except (asyncio.IncompleteReadError, EOFError):
            return None

    # -- reader ----------------------------------------------------------

    async def _reader_loop(self) -> None:
        try:
            while not self._closing:
                header = await self._read_exactly(HEADER_SIZE)
                if header is None:
                    await self._on_connection_lost("peer closed the connection")
                    return
                self.stats.bytes_read += HEADER_SIZE
                version, raw_type, flags, stream_id, length = decode_header(header)
                self.stats.frames_read += 1
                try:
                    frame_type = FrameType(raw_type)
                except ValueError:
                    await self._fail_connection(f"unknown frame type {raw_type}")
                    return
                if length > ABSOLUTE_MAX_PAYLOAD:
                    await self._fail_connection(
                        f"declared payload {length} exceeds absolute limit"
                    )
                    return
                payload = b""
                if length:
                    payload = await self._read_exactly(length)
                    if payload is None:
                        await self._on_connection_lost(
                            "peer closed mid-payload"
                        )
                        return
                    self.stats.bytes_read += length
                frame = Frame(version, frame_type, flags, stream_id, payload)
                if self._on_frame_read is not None:
                    self._on_frame_read(frame)
                limit = (
                    self._max_data_frame
                    if frame_type is FrameType.DATA
                    else self._max_control_payload
                )
                if length > limit:
                    self.stats.protocol_errors += 1
                    await self._stream_protocol_error(
                        stream_id,
                        f"oversize {frame_type.name} payload {length} > {limit}",
                    )
                    continue
                await self._dispatch(frame)
        except asyncio.CancelledError:
            raise
        except (ConnectionClosed, asyncio.IncompleteReadError, OSError) as exc:
            await self._on_connection_lost(str(exc) or exc.__class__.__name__)
        except Exception as exc:  # noqa: BLE001 - never die silently
            log.exception("multiplexer reader crashed")
            self.stats.protocol_errors += 1
            await self._fail_connection(f"reader crashed: {exc!r}")

    async def _dispatch(self, frame: Frame) -> None:
        if frame.type is FrameType.VERSION:
            await self._handle_version(frame)
            return
        if not self._handshake_done:
            await self._fail_connection("frame received before version handshake")
            return
        if frame.version != self._outgoing_version():
            await self._fail_connection(
                f"frame version {frame.version} != negotiated "
                f"{self._outgoing_version()}"
            )
            return
        try:
            if frame.type is FrameType.DATA:
                await self._handle_data(frame)
            elif frame.type is FrameType.OPEN:
                await self._handle_open(frame)
            elif frame.type is FrameType.OPEN_OK:
                self._handle_open_ok(frame)
            elif frame.type is FrameType.OPEN_ERR:
                self._handle_open_err(frame)
            elif frame.type is FrameType.CREDIT:
                self._handle_credit(frame)
            elif frame.type is FrameType.EOF:
                self._handle_eof(frame)
            elif frame.type is FrameType.RESET:
                self._handle_reset(frame)
            elif frame.type is FrameType.PING:
                self._enqueue_control(
                    FrameType.PONG,
                    payload=_json_bytes("PONG"),
                )
            elif frame.type is FrameType.PONG:
                future = getattr(self, "_ping_future", None)
                if future is not None and not future.done():
                    future.set_result(None)
            else:  # pragma: no cover - exhaustive over the enum
                await self._fail_connection(f"unhandled frame type {frame.type}")
        except ProtocolError as exc:
            self.stats.protocol_errors += 1
            await self._stream_protocol_error(frame.stream_id, str(exc))

    async def _handle_version(self, frame: Frame) -> None:
        if frame.stream_id != CONTROL_STREAM_ID:
            await self._fail_connection("VERSION on a non-control stream")
            return
        if self._handshake_done:
            await self._fail_connection("duplicate VERSION frame")
            return
        body = _parse_json(frame.payload)
        peer_version = _require_int(body, "version", minimum=1, maximum=0xFF)
        peer_min = _require_int(body, "min_version", minimum=1, maximum=0xFF)
        if peer_min > self._version or peer_version < self._min_version:
            message = (
                f"version mismatch: peer {peer_version}/min {peer_min}, "
                f"local {self._version}/min {self._min_version}"
            )
            if (
                self._handshake_future is not None
                and not self._handshake_future.done()
            ):
                self._handshake_future.set_exception(ProtocolError(message))
            await self._fail_connection(message)
            return
        negotiated = min(peer_version, self._version)
        self._negotiated_version = negotiated
        self._handshake_done = True
        if self._handshake_future is not None and not self._handshake_future.done():
            self._handshake_future.set_result(negotiated)

    async def _handle_data(self, frame: Frame) -> None:
        if frame.stream_id == CONTROL_STREAM_ID:
            raise ProtocolError("DATA on the control stream")
        stream = self._streams.get(frame.stream_id)
        if stream is None:
            self._send_reset(frame.stream_id, "unknown stream")
            return
        stream._feed_data(frame.payload)

    async def _handle_open(self, frame: Frame) -> None:
        if frame.stream_id != CONTROL_STREAM_ID:
            raise ProtocolError("OPEN on a non-control stream")
        body = _parse_json(frame.payload)
        stream_id = _require_int(body, "stream_id", minimum=1)
        job_id = body.get("job_id")
        if job_id is not None and not isinstance(job_id, str):
            raise ProtocolError("OPEN job_id must be a string")
        if stream_id in self._streams:
            self._enqueue_control(
                FrameType.OPEN_ERR,
                payload=_json_bytes(
                    "OPEN_ERR", stream_id=stream_id, error="stream already open"
                ),
            )
            return
        stream = Stream(self, stream_id, job_id=job_id, outbound=False)
        self._streams[stream_id] = stream
        self.stats.open_streams = len(self._streams)
        self._enqueue_control(
            FrameType.OPEN_OK,
            payload=_json_bytes("OPEN_OK", stream_id=stream_id),
        )
        stream._recv_credit = self._recv_window
        self._enqueue_credit(stream_id, self._recv_window)
        self._incoming.put_nowait(stream)

    def _handle_open_ok(self, frame: Frame) -> None:
        if frame.stream_id != CONTROL_STREAM_ID:
            raise ProtocolError("OPEN_OK on a non-control stream")
        body = _parse_json(frame.payload)
        stream_id = _require_int(body, "stream_id", minimum=1)
        stream = self._streams.get(stream_id)
        if stream is None or not stream._outbound:
            # The opener may have abandoned this stream (for example after its
            # own timeout); reset the referenced id instead of failing all
            # other streams.
            self._send_reset(stream_id, "OPEN_OK for unknown stream")
            return
        future = stream._open_future
        if future is not None and not future.done():
            future.set_result(None)

    def _handle_open_err(self, frame: Frame) -> None:
        if frame.stream_id != CONTROL_STREAM_ID:
            raise ProtocolError("OPEN_ERR on a non-control stream")
        body = _parse_json(frame.payload)
        stream_id = _require_int(body, "stream_id", minimum=1)
        reason = body.get("error", "open rejected")
        stream = self._streams.pop(stream_id, None)
        self.stats.open_streams = len(self._streams)
        if stream is None:
            return
        stream._receive_reset(f"open rejected: {reason}")
        future = stream._open_future
        if future is not None and not future.done():
            future.set_exception(OpenError(str(reason)))

    def _handle_credit(self, frame: Frame) -> None:
        if frame.stream_id != CONTROL_STREAM_ID:
            raise ProtocolError("CREDIT on a non-control stream")
        body = _parse_json(frame.payload)
        stream_id = _require_int(body, "stream_id", minimum=1)
        credit = _require_int(body, "credit", minimum=1)
        stream = self._streams.get(stream_id)
        if stream is None:
            self._send_reset(stream_id, "CREDIT for unknown stream")
            return
        if stream._send_credit + credit > MAX_SEND_CREDIT:
            raise ProtocolError("send credit overflow")
        stream._send_credit += credit
        self._send_ready.set()

    def _handle_eof(self, frame: Frame) -> None:
        if frame.stream_id == CONTROL_STREAM_ID:
            raise ProtocolError("EOF on the control stream")
        stream = self._streams.get(frame.stream_id)
        if stream is None:
            self._send_reset(frame.stream_id, "EOF for unknown stream")
            return
        stream._receive_eof()

    def _handle_reset(self, frame: Frame) -> None:
        if frame.stream_id == CONTROL_STREAM_ID:
            raise ProtocolError("RESET on the control stream")
        reason = "peer reset"
        if frame.payload:
            with contextlib.suppress(ProtocolError):
                reason = str(_parse_json(frame.payload).get("reason", reason))
        stream = self._streams.get(frame.stream_id)
        if stream is None:
            return
        stream._receive_reset(reason)

    # -- error handling --------------------------------------------------

    async def _stream_protocol_error(self, stream_id: int, message: str) -> None:
        log.warning("stream protocol error on %s: %s", stream_id, message)
        if self._on_protocol_error is not None:
            with contextlib.suppress(Exception):
                self._on_protocol_error(stream_id, message)
        if stream_id == CONTROL_STREAM_ID:
            await self._fail_connection(message)
            return
        stream = self._streams.get(stream_id)
        if stream is not None:
            stream._receive_reset("protocol error: " + message)
        self._send_reset(stream_id, "protocol error: " + message)

    async def _fail_connection(self, message: str) -> None:
        if self._closing:
            return
        self._closing = True
        self.stats.protocol_errors += 1
        log.error("multiplexer protocol failure: %s", message)
        if self._on_protocol_error is not None:
            with contextlib.suppress(Exception):
                self._on_protocol_error(CONTROL_STREAM_ID, message)
        payload = _json_bytes("RESET", reason=message)
        frame = encode_frame(
            self._outgoing_version(),
            FrameType.RESET,
            0,
            CONTROL_STREAM_ID,
            payload,
        )
        try:
            await asyncio.wait_for(self._write_raw(frame), 2.0)
        except (asyncio.TimeoutError, OSError, ConnectionClosed):
            pass
        except Exception:  # noqa: BLE001 - best-effort diagnostic frame
            log.debug("could not emit protocol error frame", exc_info=True)
        await self._close_transport(message)

    async def _on_connection_lost(self, message: str) -> None:
        if self._closed_event.is_set():
            return
        self._closing = True
        await self._close_transport(message)

    async def _close_transport(self, message: str) -> None:
        self._closing = True
        if not self._closed_event.is_set():
            self._closed_event.set()
        if not self._closed_sentinel_sent:
            self._closed_sentinel_sent = True
            self._incoming.put_nowait(_CLOSED)
        if self._handshake_future is not None and not self._handshake_future.done():
            self._handshake_future.set_exception(ConnectionClosed(message))
        for stream in list(self._streams.values()):
            stream._receive_reset(message)
        self._streams.clear()
        self.stats.open_streams = 0
        current = asyncio.current_task()
        tasks = [
            task
            for task in (self._reader_task, self._writer_task)
            if task is not None and task is not current and not task.done()
        ]
        for task in tasks:
            task.cancel()
        if tasks:
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), 2.0
                )
        close = getattr(self._writer, "close", None)
        if close is not None:
            with contextlib.suppress(Exception):
                close()
        wait_closed = getattr(self._writer, "wait_closed", None)
        if wait_closed is not None:
            with contextlib.suppress(Exception):
                await asyncio.wait_for(wait_closed(), 2.0)

    # -- writer ----------------------------------------------------------

    def _has_sendable(self) -> bool:
        if self._control_queue:
            return True
        for stream in self._streams.values():
            if not stream._send_queue:
                continue
            if stream._send_queue[0] is _EOF_SENTINEL:
                return True
            if stream._send_credit > 0:
                return True
        return False

    async def _writer_loop(self) -> None:
        try:
            while not self._closing:
                wrote = await self._writer_pass()
                if wrote:
                    await asyncio.sleep(0)
                    continue
                self._send_ready.clear()
                if self._has_sendable():
                    continue
                await self._send_ready.wait()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - surfaced through close
            log.exception("multiplexer writer crashed")
            await self._on_connection_lost("writer crashed")

    async def _writer_pass(self) -> bool:
        wrote = False
        while self._control_queue:
            frame_bytes = self._control_queue.popleft()
            version, raw_type, flags, stream_id, length = decode_header(
                frame_bytes[:HEADER_SIZE]
            )
            frame = Frame(
                version,
                FrameType(raw_type),
                flags,
                stream_id,
                frame_bytes[HEADER_SIZE:],
            )
            await self._write_raw(frame_bytes, frame)
            wrote = True

        streams = list(self._streams.values())
        if not streams:
            return wrote
        count = len(streams)
        start = self._rr_index % count
        for offset in range(count):
            stream = streams[(start + offset) % count]
            if stream._finalized or not stream._send_queue:
                continue
            front = stream._send_queue[0]
            if front is _EOF_SENTINEL:
                stream._send_queue.popleft()
                stream._eof_sent = True
                frame_bytes = encode_frame(
                    self._outgoing_version(),
                    FrameType.EOF,
                    0,
                    stream.stream_id,
                    b"",
                )
                await self._write_raw(
                    frame_bytes,
                    Frame(
                        self._outgoing_version(),
                        FrameType.EOF,
                        0,
                        stream.stream_id,
                        b"",
                    ),
                )
                wrote = True
                self._maybe_finalize(stream)
                continue
            if stream._send_credit <= 0:
                continue
            take = min(
                len(front), stream._send_credit, self._max_data_frame
            )
            payload = front[:take]
            if take == len(front):
                stream._send_queue.popleft()
            else:
                stream._send_queue[0] = front[take:]
            stream._send_credit -= take
            stream._send_bytes -= take
            stream._space.set()
            frame_bytes = encode_frame(
                self._outgoing_version(),
                FrameType.DATA,
                0,
                stream.stream_id,
                payload,
            )
            await self._write_raw(
                frame_bytes,
                Frame(
                    self._outgoing_version(), FrameType.DATA, 0, stream.stream_id, payload
                ),
            )
            wrote = True
        self._rr_index = (start + 1) % count
        return wrote
