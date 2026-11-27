#!/usr/bin/env python3
# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Micro-benchmark the mux framing against a raw socketpair.

This isolates the cost of the length-prefixed frame codec and its writer/reader
tasks from storage, SSH ciphers and cluster queuing.  Both paths use the same
``socket.socketpair()`` pair and the same echo pattern:

* raw   - bytes go into the socketpair and come back unchanged;
* mux   - the same bytes travel as DATA frames through two Multiplexers.

It reports round-trip throughput (MiB/s) and ping-pong p50/p95 latency.

A comparison with the *legacy* single-container transport is deliberately not
attempted here: it needs a real login node, a real Slurm allocation, the same
SSH cipher and the same storage path (design §10).  Run this script on the
gateway host and on a compute target to compare CPU/substrate effects.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import socket
import time
from dataclasses import asdict, dataclass

from compute_mcp.mux import ConnectionClosed, Multiplexer, StreamClosed, StreamReset

MIB = 1024 * 1024


@dataclass
class Result:
    name: str
    bytes: int
    seconds: float
    mib_per_second: float
    latency_p50_us: float
    latency_p95_us: float


async def _open_pair():
    left, right = socket.socketpair()
    left.setblocking(False)
    right.setblocking(False)
    left_reader, left_writer = await asyncio.open_connection(sock=left)
    right_reader, right_writer = await asyncio.open_connection(sock=right)
    return (left_reader, left_writer), (right_reader, right_writer)


async def _raw_echo(reader, writer):
    try:
        while True:
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        writer.close()


async def _mux_echo(mux):
    while True:
        try:
            stream = await mux.accept()
        except ConnectionClosed:
            return
        asyncio.create_task(_mux_echo_stream(stream))


async def _mux_echo_stream(stream):
    try:
        while True:
            data = await stream.read(65536)
            if not data:
                break
            await stream.write(data)
        await stream.close()
    except (StreamReset, StreamClosed, ConnectionClosed):
        pass


async def _raw_roundtrip(size: int, chunk: int) -> float:
    (client_reader, client_writer), (peer_reader, peer_writer) = await _open_pair()
    peer = asyncio.create_task(_raw_echo(peer_reader, peer_writer))
    payload = b"\xa5" * chunk

    async def reader():
        remaining = size
        while remaining:
            data = await client_reader.read(min(chunk, remaining))
            if not data:
                raise RuntimeError("raw peer closed early")
            remaining -= len(data)

    async def writer():
        remaining = size
        while remaining:
            block = payload if remaining >= chunk else payload[:remaining]
            client_writer.write(block)
            await client_writer.drain()
            remaining -= len(block)

    start = time.perf_counter()
    await asyncio.gather(reader(), writer())
    elapsed = time.perf_counter() - start
    client_writer.close()
    peer_writer.close()
    with _suppress():
        await client_writer.wait_closed()
    peer.cancel()
    with _suppress():
        await peer
    return elapsed


async def _mux_roundtrip(size: int, chunk: int) -> float:
    (a_reader, a_writer), (b_reader, b_writer) = await _open_pair()
    mux_a = Multiplexer(a_reader, a_writer, initiator=True)
    mux_b = Multiplexer(b_reader, b_writer, initiator=False)
    await asyncio.gather(mux_a.start(), mux_b.start())
    echo = asyncio.create_task(_mux_echo(mux_b))
    stream = await mux_a.open_stream("bench")
    payload = b"\x5a" * chunk

    async def reader():
        remaining = size
        while remaining:
            data = await stream.read(min(chunk, remaining))
            if not data:
                raise RuntimeError("mux peer closed early")
            remaining -= len(data)

    async def writer():
        remaining = size
        while remaining:
            block = payload if remaining >= chunk else payload[:remaining]
            await stream.write(block)
            remaining -= len(block)

    start = time.perf_counter()
    await asyncio.gather(reader(), writer())
    elapsed = time.perf_counter() - start
    await stream.close()
    await mux_a.close()
    await mux_b.close()
    echo.cancel()
    with _suppress():
        await echo
    return elapsed


async def _raw_latency(count: int, payload: bytes) -> list[float]:
    (client_reader, client_writer), (peer_reader, peer_writer) = await _open_pair()
    peer = asyncio.create_task(_raw_echo(peer_reader, peer_writer))
    samples = []
    for _ in range(count):
        start = time.perf_counter()
        client_writer.write(payload)
        await client_writer.drain()
        remaining = len(payload)
        while remaining:
            data = await client_reader.read(remaining)
            remaining -= len(data)
        samples.append(time.perf_counter() - start)
    client_writer.close()
    peer.cancel()
    with _suppress():
        await peer
    return samples


async def _mux_latency(count: int, payload: bytes) -> list[float]:
    (a_reader, a_writer), (b_reader, b_writer) = await _open_pair()
    mux_a = Multiplexer(a_reader, a_writer, initiator=True)
    mux_b = Multiplexer(b_reader, b_writer, initiator=False)
    await asyncio.gather(mux_a.start(), mux_b.start())
    echo = asyncio.create_task(_mux_echo(mux_b))
    stream = await mux_a.open_stream("latency")
    samples = []
    for _ in range(count):
        start = time.perf_counter()
        await stream.write(payload)
        remaining = len(payload)
        while remaining:
            data = await stream.read(remaining)
            remaining -= len(data)
        samples.append(time.perf_counter() - start)
    await stream.close()
    await mux_a.close()
    await mux_b.close()
    echo.cancel()
    with _suppress():
        await echo
    return samples


def _percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    if not ordered:
        return 0.0
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return ordered[index] * 1e6


class _suppress:
    def __enter__(self):
        return self

    def __exit__(self, *_exc):
        return True


async def _measure(name, roundtrip, latency, size, chunk, count, latency_size):
    elapsed = await roundtrip(size, chunk)
    samples = await latency(count, b"\x42" * latency_size)
    return Result(
        name=name,
        bytes=size,
        seconds=elapsed,
        mib_per_second=(size / MIB) / elapsed if elapsed else float("inf"),
        latency_p50_us=_percentile(samples, 0.50),
        latency_p95_us=_percentile(samples, 0.95),
    )


async def run(args) -> dict:
    size = args.size_mib * MIB
    raw = await _measure(
        "raw-socketpair", _raw_roundtrip, _raw_latency,
        size, args.chunk, args.latency_count, args.latency_size,
    )
    mux = await _measure(
        "mux-framing", _mux_roundtrip, _mux_latency,
        size, args.chunk, args.latency_count, args.latency_size,
    )
    report = {
        "config": {
            "size_mib": args.size_mib,
            "chunk_bytes": args.chunk,
            "latency_count": args.latency_count,
            "latency_payload_bytes": args.latency_size,
        },
        "results": [asdict(raw), asdict(mux)],
        "mux_overhead_ratio": (
            raw.mib_per_second / mux.mib_per_second if mux.mib_per_second else 0.0
        ),
        "note": (
            "socketpair micro-benchmark only; a legacy-transport comparison "
            "requires a real login node, Slurm allocation, SSH cipher and "
            "storage path"
        ),
    }
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--size-mib", type=int, default=64)
    parser.add_argument("--chunk", type=int, default=65536)
    parser.add_argument("--latency-count", type=int, default=2000)
    parser.add_argument("--latency-size", type=int, default=64)
    parser.add_argument("--json", type=str, default=None,
                        help="write the JSON report to this path")
    args = parser.parse_args(argv)
    report = asyncio.run(run(args))
    for result in report["results"]:
        print(
            f"{result['name']:16s} {result['mib_per_second']:8.1f} MiB/s  "
            f"p50 {result['latency_p50_us']:7.1f} us  "
            f"p95 {result['latency_p95_us']:7.1f} us"
        )
    print(f"mux/raw throughput ratio: {report['mux_overhead_ratio']:.3f}")
    if args.json:
        with open(args.json, "w", encoding="utf-8") as handle:
            json.dump(report, handle, indent=2)
            handle.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
