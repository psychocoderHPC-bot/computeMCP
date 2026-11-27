# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Wire-level and behavioural tests for ``compute_mcp.mux``.

The tests use ``socket.socketpair`` so no network is involved.  A raw peer that
speaks the frame format directly proves the codec against fragmented and
coalesced input and against deliberate protocol violations; a second
``Multiplexer`` covers the normal stream and flow-control behaviour.
"""

import asyncio
import json
import random
import socket

import pytest

from compute_mcp.mux import (
    HEADER_SIZE,
    HEADER_STRUCT,
    ConnectionClosed,
    Frame,
    FrameType,
    Multiplexer,
    ProtocolError,
    StreamClosed,
    StreamReset,
)

HANDSHAKE_TIMEOUT = 5.0


class RawPeer:
    """Minimal frame peer used to exercise the codec and error paths."""

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    async def send_frame(
        self,
        frame_type,
        stream_id,
        payload=b"",
        *,
        version=1,
        flags=0,
        length_override=None,
    ):
        length = len(payload) if length_override is None else length_override
        header = HEADER_STRUCT.pack(
            version, int(frame_type), flags, stream_id, length
        )
        self.writer.write(header + payload)
        await self.writer.drain()

    async def send_fragmented(self, frame_type, stream_id, payload, piece=1):
        length = len(payload)
        header = HEADER_STRUCT.pack(1, int(frame_type), 0, stream_id, length)
        for chunk in _pieces(header + payload, piece):
            self.writer.write(chunk)
            await self.writer.drain()

    async def send_version(self, version=1, min_version=1):
        body = json.dumps(
            {"type": "VERSION", "version": version, "min_version": min_version},
            separators=(",", ":"),
        ).encode()
        await self.send_frame(FrameType.VERSION, 0, body, version=version)

    async def recv_frame(self):
        header = await self.reader.readexactly(HEADER_SIZE)
        version, frame_type, flags, stream_id, length = HEADER_STRUCT.unpack(
            header
        )
        payload = await self.reader.readexactly(length) if length else b""
        return Frame(version, FrameType(frame_type), flags, stream_id, payload)

    async def recv_until(self, frame_type, stream_id=None, timeout=HANDSHAKE_TIMEOUT):
        async def _inner():
            while True:
                frame = await self.recv_frame()
                if frame.type is frame_type and (
                    stream_id is None or frame.stream_id == stream_id
                ):
                    return frame

        return await asyncio.wait_for(_inner(), timeout)

    def close(self):
        self.writer.close()


def _pieces(data, size):
    for start in range(0, len(data), size):
        yield data[start : start + size]


async def _socket_reader_writer(sock):
    sock.setblocking(False)
    return await asyncio.open_connection(sock=sock)


async def make_pair(**kwargs):
    left, right = socket.socketpair()
    left_reader, left_writer = await _socket_reader_writer(left)
    right_reader, right_writer = await _socket_reader_writer(right)
    mux_a = Multiplexer(left_reader, left_writer, initiator=True, **kwargs)
    mux_b = Multiplexer(right_reader, right_writer, initiator=False, **kwargs)
    await asyncio.gather(mux_a.start(), mux_b.start())
    return mux_a, mux_b


async def make_raw_pair(**kwargs):
    left, right = socket.socketpair()
    mux_reader, mux_writer = await _socket_reader_writer(left)
    peer_reader, peer_writer = await _socket_reader_writer(right)
    mux = Multiplexer(mux_reader, mux_writer, initiator=True, **kwargs)
    peer = RawPeer(peer_reader, peer_writer)
    start = asyncio.ensure_future(mux.start())
    await peer.send_version()
    version_frame = await peer.recv_frame()
    assert version_frame.type is FrameType.VERSION
    await asyncio.wait_for(start, HANDSHAKE_TIMEOUT)
    return mux, peer


async def close_all(*muxes):
    await asyncio.gather(*(mux.close() for mux in muxes), return_exceptions=True)


async def echo_accept(mux):
    while True:
        try:
            stream = await mux.accept()
        except ConnectionClosed:
            return
        asyncio.ensure_future(_echo(stream))


async def _echo(stream):
    try:
        while True:
            data = await stream.read(65536)
            if not data:
                break
            await stream.write(data)
        await stream.close()
    except (StreamReset, StreamClosed, ConnectionClosed):
        pass


class FragmentingWriter:
    """Split every write into small pieces to exercise partial frames."""

    def __init__(self, inner, rng, max_piece=7):
        self._inner = inner
        self._rng = rng
        self._max_piece = max_piece

    def write(self, data):
        offset = 0
        while offset < len(data):
            step = self._rng.randint(1, min(self._max_piece, len(data) - offset))
            self._inner.write(data[offset : offset + step])
            offset += step

    async def drain(self):
        await self._inner.drain()

    def close(self):
        self._inner.close()

    def __getattr__(self, name):
        return getattr(self._inner, name)


class FragmentingReader:
    """Expose only ``read`` with a random small limit; no ``readexactly``."""

    def __init__(self, inner, rng, max_piece=7):
        self._inner = inner
        self._rng = rng
        self._max_piece = max_piece

    readexactly = None

    async def read(self, n=-1):
        if n is not None and n >= 0:
            limit = min(n, self._rng.randint(1, self._max_piece))
        else:
            limit = self._max_piece
        return await self._inner.read(limit)

    def __getattr__(self, name):
        return getattr(self._inner, name)


async def test_fragmented_header_and_coalesced_frames():
    mux, peer = await make_raw_pair()
    try:
        open_body = json.dumps(
            {"type": "OPEN", "stream_id": 7, "job_id": "job-a"},
            separators=(",", ":"),
        ).encode()
        # One byte per write forces the reader to reassemble a fragmented frame.
        await peer.send_fragmented(FrameType.OPEN, 0, open_body, piece=1)
        await peer.recv_until(FrameType.OPEN_OK, 0)
        await peer.recv_until(FrameType.CREDIT, 0)

        stream = await asyncio.wait_for(mux.accept(), HANDSHAKE_TIMEOUT)
        assert stream.stream_id == 7
        assert stream.job_id == "job-a"

        first = b"alpha\x00\x00binary"
        second = b"\x00\x00second\x00"
        # Both frames arrive in a single TCP write (coalesced).
        frames = (
            HEADER_STRUCT.pack(1, int(FrameType.DATA), 0, 7, len(first))
            + first
            + HEADER_STRUCT.pack(1, int(FrameType.DATA), 0, 7, len(second))
            + second
        )
        peer.writer.write(frames)
        await peer.writer.drain()

        assert await asyncio.wait_for(
            stream.read(len(first)), HANDSHAKE_TIMEOUT
        ) == first
        assert await asyncio.wait_for(
            stream.read(len(second)), HANDSHAKE_TIMEOUT
        ) == second
    finally:
        peer.close()
        await close_all(mux)


async def test_binary_zeros_roundtrip():
    mux_a, mux_b = await make_pair()
    server = asyncio.ensure_future(echo_accept(mux_b))
    try:
        stream = await mux_a.open_stream("job-zeros")
        payload = bytes(range(256)) + b"\x00" * 300 + b"\xff\x00\xff"
        await stream.write(payload)
        assert await asyncio.wait_for(stream.read(len(payload)), HANDSHAKE_TIMEOUT) == payload
    finally:
        server.cancel()
        await close_all(mux_a, mux_b)


async def test_oversize_data_frame_resets_only_its_stream():
    mux, peer = await make_raw_pair(max_data_frame=64)
    try:
        open_body = json.dumps(
            {"type": "OPEN", "stream_id": 9, "job_id": "job-oversize"},
            separators=(",", ":"),
        ).encode()
        await peer.send_frame(FrameType.OPEN, 0, open_body)
        await peer.recv_until(FrameType.OPEN_OK, 0)
        await peer.recv_until(FrameType.CREDIT, 0)
        await asyncio.wait_for(mux.accept(), HANDSHAKE_TIMEOUT)

        # Declared 100 bytes with a 64-byte limit: the stream is reset, but the
        # connection stays usable.
        await peer.send_frame(FrameType.DATA, 9, b"x" * 100)
        reset = await peer.recv_until(FrameType.RESET, 9)
        assert b"oversize" in reset.payload

        await peer.send_frame(
            FrameType.PING,
            0,
            json.dumps({"type": "PING"}).encode(),
        )
        pong = await peer.recv_until(FrameType.PONG, 0)
        assert json.loads(pong.payload)["type"] == "PONG"
    finally:
        peer.close()
        await close_all(mux)


async def test_unknown_stream_id_is_reset_and_connection_survives():
    mux, peer = await make_raw_pair()
    try:
        await peer.send_frame(FrameType.DATA, 777, b"orphan")
        reset = await peer.recv_until(FrameType.RESET, 777)
        assert b"unknown stream" in reset.payload

        await peer.send_frame(FrameType.PING, 0, b'{"type":"PING"}')
        await peer.recv_until(FrameType.PONG, 0)
    finally:
        peer.close()
        await close_all(mux)


async def test_bad_frame_version_is_fatal_after_emitting_reset():
    mux, peer = await make_raw_pair()
    try:
        # Handshake is done, so a frame with a different header version must
        # fail the connection, but only after a diagnostic RESET.
        await peer.send_frame(FrameType.DATA, 3, b"data", version=9)
        reset = await peer.recv_until(FrameType.RESET, 0)
        assert b"version" in reset.payload
        await asyncio.wait_for(mux.wait_closed(), HANDSHAKE_TIMEOUT)
    finally:
        peer.close()
        await close_all(mux)


async def test_version_mismatch_fails_handshake():
    left, right = socket.socketpair()
    mux_reader, mux_writer = await _socket_reader_writer(left)
    peer_reader, peer_writer = await _socket_reader_writer(right)
    mux = Multiplexer(mux_reader, mux_writer, initiator=True)
    peer = RawPeer(peer_reader, peer_writer)
    try:
        start = asyncio.ensure_future(mux.start())
        await peer.send_version(version=2, min_version=2)
        with pytest.raises(ProtocolError):
            await asyncio.wait_for(start, HANDSHAKE_TIMEOUT)
    finally:
        peer.close()
        await close_all(mux)


async def test_credit_exhaustion_then_refill():
    mux_a, mux_b = await make_pair(
        recv_window=8,
        send_buffer_limit=8,
        max_data_frame=8,
        credit_threshold=4,
    )
    release = asyncio.Event()

    async def slow_echo():
        peer_stream = await mux_b.accept()
        await release.wait()
        while True:
            data = await peer_stream.read(65536)
            if not data:
                break
            await peer_stream.write(data)

    server = asyncio.ensure_future(slow_echo())
    try:
        stream = await mux_a.open_stream("job-credit")
        await asyncio.sleep(0.05)  # let the peer's initial CREDIT arrive
        await stream.write(b"aaaaaaaa")
        await asyncio.sleep(0.05)  # first chunk consumes all peer credit
        await stream.write(b"bbbbbbbb")  # buffered, no credit left
        blocked = asyncio.ensure_future(stream.write(b"cccccccc"))
        await asyncio.sleep(0.1)
        assert not blocked.done()

        # Releasing the reader refunds credit and the blocked write completes.
        release.set()
        await asyncio.wait_for(blocked, HANDSHAKE_TIMEOUT)

        received = bytearray()
        while len(received) < 24:
            chunk = await asyncio.wait_for(
                stream.read(24 - len(received)), HANDSHAKE_TIMEOUT
            )
            if not chunk:
                break
            received += chunk
        assert bytes(received) == b"aaaaaaaa" + b"bbbbbbbb" + b"cccccccc"
    finally:
        server.cancel()
        await close_all(mux_a, mux_b)


async def test_half_close_preserves_the_reverse_direction():
    mux_a, mux_b = await make_pair()

    async def server():
        stream = await mux_b.accept()
        received = b""
        while True:
            chunk = await stream.read(65536)
            if not chunk:
                break
            received += chunk
        await stream.write(b"reply:" + received)
        await stream.close()

    task = asyncio.ensure_future(server())
    try:
        stream = await mux_a.open_stream("job-half")
        await stream.write(b"hello")
        await stream.close()  # half-close: we may still read the reply
        assert await asyncio.wait_for(stream.read(64), HANDSHAKE_TIMEOUT) == b"reply:hello"
        assert await asyncio.wait_for(stream.read(64), HANDSHAKE_TIMEOUT) == b""
    finally:
        task.cancel()
        await close_all(mux_a, mux_b)


async def test_reset_propagates_to_the_peer():
    mux_a, mux_b = await make_pair()

    async def server():
        return await mux_b.accept()

    task = asyncio.ensure_future(server())
    try:
        stream = await mux_a.open_stream("job-reset")
        peer_stream = await asyncio.wait_for(task, HANDSHAKE_TIMEOUT)
        await stream.reset("boom")
        with pytest.raises(StreamReset):
            await asyncio.wait_for(peer_stream.read(16), HANDSHAKE_TIMEOUT)
        with pytest.raises(StreamReset):
            await stream.write(b"after-reset")
    finally:
        task.cancel()
        await close_all(mux_a, mux_b)


async def test_receive_buffer_stays_bounded():
    mux_a, mux_b = await make_pair(
        recv_window=32,
        send_buffer_limit=8,
        max_data_frame=8,
        credit_threshold=16,
    )
    accepted = asyncio.ensure_future(mux_b.accept())
    try:
        stream = await mux_a.open_stream("job-bounded")
        await asyncio.wait_for(accepted, HANDSHAKE_TIMEOUT)
        # Nobody reads on B, so A blocks once credit and its buffer are spent.
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(stream.write(b"z" * 1000), 0.15)
        await asyncio.sleep(0.1)
        assert mux_b.stats.peak_recv_buffer <= 32
        peer_stream = accepted.result()
        assert len(peer_stream._recv) <= 32
    finally:
        await close_all(mux_a, mux_b)


async def test_fair_round_robin_across_streams():
    written = []
    mux_a, mux_b = await make_pair(
        recv_window=1_000_000,
        send_buffer_limit=1_000_000,
        max_data_frame=1000,
        credit_threshold=64 * 1024,
        on_frame_written=lambda frame: written.append(frame)
        if frame.type is FrameType.DATA
        else None,
    )
    accepted = []

    async def accept_two():
        for _ in range(2):
            accepted.append(await mux_b.accept())

    task = asyncio.ensure_future(accept_two())
    try:
        first = await mux_a.open_stream("job-fair-a")
        second = await mux_a.open_stream("job-fair-b")
        await asyncio.wait_for(task, HANDSHAKE_TIMEOUT)
        payload = b"p" * 5000
        # Both writes complete without yielding, so both queues are full before
        # the writer runs; a fair scheduler must interleave the two streams.
        await first.write(payload)
        await second.write(payload)
        deadline = asyncio.get_running_loop().time() + HANDSHAKE_TIMEOUT
        while len(written) < 8 and asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(0.01)
        sids = [frame.stream_id for frame in written if frame.type is FrameType.DATA]
        assert first.stream_id in sids[:3]
        assert second.stream_id in sids[:3]
        assert sids[:4].count(first.stream_id) == 2
        assert sids[:4].count(second.stream_id) == 2
    finally:
        task.cancel()
        await close_all(mux_a, mux_b)


async def test_randomized_chunking_and_fragmentation_fuzz():
    for seed in (0, 1, 2):
        rng = random.Random(seed)
        left, right = socket.socketpair()
        left_reader, left_writer = await _socket_reader_writer(left)
        right_reader, right_writer = await _socket_reader_writer(right)
        mux_a = Multiplexer(
            FragmentingReader(left_reader, rng),
            FragmentingWriter(left_writer, rng),
            initiator=True,
            max_data_frame=64,
            recv_window=256,
            send_buffer_limit=256,
            credit_threshold=64,
        )
        mux_b = Multiplexer(
            FragmentingReader(right_reader, rng),
            FragmentingWriter(right_writer, rng),
            initiator=False,
            max_data_frame=64,
            recv_window=256,
            send_buffer_limit=256,
            credit_threshold=64,
        )
        await asyncio.gather(mux_a.start(), mux_b.start())
        server = asyncio.ensure_future(echo_accept(mux_b))
        try:
            payload = bytes(rng.randrange(256) for _ in range(3000))
            stream = await mux_a.open_stream("job-fuzz-%d" % seed)
            received = bytearray()

            async def reader():
                # Read concurrently: the echo cannot drain if this side only
                # writes, and the resulting full-duplex stall is not a mux bug.
                while len(received) < len(payload):
                    chunk = await stream.read(rng.randint(1, 300))
                    if not chunk:
                        break
                    received.extend(chunk)

            reader_task = asyncio.ensure_future(reader())
            offset = 0
            while offset < len(payload):
                step = rng.randint(1, 200)
                await stream.write(payload[offset : offset + step])
                offset += step
            await asyncio.wait_for(reader_task, HANDSHAKE_TIMEOUT)
            assert bytes(received) == payload
        finally:
            server.cancel()
            await close_all(mux_a, mux_b)


async def test_write_after_close_raises():
    mux_a, mux_b = await make_pair()

    async def server():
        return await mux_b.accept()

    task = asyncio.ensure_future(server())
    try:
        stream = await mux_a.open_stream("job-write-closed")
        await asyncio.wait_for(task, HANDSHAKE_TIMEOUT)
        await stream.close()
        with pytest.raises(StreamClosed):
            await stream.write(b"too late")
    finally:
        task.cancel()
        await close_all(mux_a, mux_b)
