#!/usr/bin/env python3
"""Loopback TCP relay for computemcp-provision.sh using Slurm job steps.

The relay is started on the login node by computemcp-provision.sh.  It binds a
loopback port and, for every accepted connection, opens a one-task ``srun``
step inside the tracked allocation that runs this same file with ``--connect``
on the compute node.  That step dials the container's node-local SSH port and
pipes bytes back to the login node, so the gateway never needs a direct route
to the compute node.

The node-local container port and CPU binding are read from the environment
(``COMPUTEMCP_CONTAINER_PORT`` and ``COMPUTEMCP_CPU_BIND``) that the
provisioning helper exports before launching the relay.  Standard library only.
"""
import os
import signal
import socket
import subprocess
import sys
import threading


def pump(source, destination):
    try:
        while True:
            data = getattr(source, 'read1', source.read)(65536)
            if not data:
                break
            pending = memoryview(data)
            while pending:
                written = destination.write(pending)
                if not written:
                    return
                pending = pending[written:]
            destination.flush()
    except (OSError, ValueError):
        pass


if sys.argv[1] == '--check':
    try:
        with socket.create_connection(('127.0.0.1', int(sys.argv[2])), timeout=10) as conn:
            with conn.makefile('rb') as reader:
                banner = reader.readline(256)
            if not banner.startswith(b'SSH-2.0-'):
                raise ValueError('Endpoint did not return an SSH banner')
    except (OSError, ValueError) as error:
        print('Endpoint readiness check failed: ' + str(error), file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if sys.argv[1] == '--connect':
    container_port = int(sys.argv[2])
    with socket.create_connection(('127.0.0.1', container_port), timeout=10) as conn:
        conn.settimeout(None)
        reader = conn.makefile('rb', buffering=0)
        writer = conn.makefile('wb', buffering=0)
        threading.Thread(target=pump, args=(sys.stdin.buffer, writer), daemon=True).start()
        pump(reader, sys.stdout.buffer)
    sys.exit(0)

port, jobid, node, ready = sys.argv[1:]
container_port = int(os.environ['COMPUTEMCP_CONTAINER_PORT'])
cpu_bind = os.environ['COMPUTEMCP_CPU_BIND']
children = set()
lock = threading.Lock()


def terminate(*_):
    with lock:
        for child in children:
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
    os._exit(0)


signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)


def serve(conn):
    child = None
    try:
        with conn:
            with lock:
                child = subprocess.Popen(
                    ['srun', '--jobid=' + jobid, '--overlap', '--nodes=1',
                     '--ntasks=1', '--cpus-per-task=1', '--cpu-bind=' + cpu_bind,
                     '--nodelist=' + node, '--unbuffered', 'python3',
                     os.path.abspath(__file__), '--connect', str(container_port)],
                    stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=0,
                    start_new_session=True)
                children.add(child)
            reader = conn.makefile('rb', buffering=0)
            writer = conn.makefile('wb', buffering=0)

            def upstream():
                pump(reader, child.stdin)
                child.stdin.close()

            threading.Thread(target=upstream, daemon=True).start()
            pump(child.stdout, writer)
            conn.shutdown(socket.SHUT_RDWR)
    except OSError as error:
        print(error, file=sys.stderr)
    finally:
        if child is not None:
            try:
                os.killpg(child.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            child.wait()
            with lock:
                children.discard(child)


with socket.socket() as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # Port 0 asks the kernel for an ephemeral port.  The helper runs on the
    # login node and delegates the choice to this process, so two targets that
    # share a login node never collide on a fixed default.  Read the concrete
    # port back and publish it in the ready file so the helper can dial it.
    listener.bind(('127.0.0.1', int(port)))
    listener.listen(32)
    bound_port = listener.getsockname()[1]
    # Ready file: "<pid> <port>\n".  Existing readers only check for its
    # existence/size, so the extra field is backward compatible.  Written
    # before the accept loop so the helper never sees a missing file.
    with open(ready, 'w') as status:
        status.write(str(os.getpid()) + ' ' + str(bound_port) + '\n')
    while True:
        conn, _ = listener.accept()
        threading.Thread(target=serve, args=(conn,), daemon=True).start()
