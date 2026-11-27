#!/usr/bin/env python3
"""Loopback TCP relay for computemcp-provision.sh using Slurm job steps.

The relay is started on the login node by computemcp-provision.sh.  It binds a
loopback port and, for every accepted connection, opens a one-task ``srun``
step inside the tracked allocation that runs this same file with ``--connect``
on the compute node.  That step dials the container's node-local SSH port and
pipes bytes back to the login node, so the gateway never needs a direct route
to the compute node.

Each job relays through its own explicit ready/endpoint file, so several relays
can run concurrently on one login node without a global registry.  The CLI is::

    computemcp-relay.py [--job-id ID] [--ready-file FILE] PORT SLURM_JOB_ID NODE [READY]

``--ready-file`` is an alias for the historical positional READY argument.  The
ready file is replaced atomically (``<pid> <port>\\n``) so a reader never sees a
partial value and concurrent starts for different jobs cannot clobber each
other's file.  Port ``0`` asks the kernel for an ephemeral port.

The node-local container port and CPU binding are read from the environment
(``COMPUTEMCP_CONTAINER_PORT`` and ``COMPUTEMCP_CPU_BIND``) that the
provisioning helper exports before launching the relay.  Standard library only.
"""
import os
import signal
import socket
import subprocess
import sys
import tempfile
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


def write_ready(path, pid, port):
    """Atomically publish ``<pid> <port>`` for this job's relay."""
    directory = os.path.dirname(os.path.abspath(path)) or '.'
    os.makedirs(directory, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix='.relay-ready-', dir=directory)
    try:
        with os.fdopen(fd, 'w') as status:
            status.write(str(pid) + ' ' + str(port) + '\n')
            status.flush()
            os.fsync(status.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def parse_server_argv(argv):
    """Parse the legacy positional form plus optional explicit flags."""
    job_id = None
    ready = None
    positional = []
    index = 0
    while index < len(argv):
        arg = argv[index]
        if arg == '--job-id':
            index += 1
            if index >= len(argv):
                raise SystemExit('--job-id requires a value')
            job_id = argv[index]
        elif arg.startswith('--job-id='):
            job_id = arg.split('=', 1)[1]
        elif arg in ('--ready-file', '--endpoint-file'):
            index += 1
            if index >= len(argv):
                raise SystemExit(arg + ' requires a value')
            ready = argv[index]
        elif arg.startswith('--ready-file='):
            ready = arg.split('=', 1)[1]
        elif arg.startswith('--endpoint-file='):
            ready = arg.split('=', 1)[1]
        elif arg in ('-h', '--help'):
            raise SystemExit(__doc__)
        elif arg.startswith('--'):
            raise SystemExit('unknown option: ' + arg)
        else:
            positional.append(arg)
        index += 1
    if len(positional) == 4:
        port, jobid, node, positional_ready = positional
        ready = ready or positional_ready
    elif len(positional) == 3:
        port, jobid, node = positional
    else:
        raise SystemExit(
            'usage: computemcp-relay.py [--job-id ID] [--ready-file FILE] '
            'PORT SLURM_JOB_ID NODE [READY]'
        )
    if not ready:
        raise SystemExit('a ready/endpoint file is required')
    return job_id, port, jobid, node, ready


USAGE = ('usage: computemcp-relay.py [--job-id ID] [--ready-file FILE] '
         'PORT SLURM_JOB_ID NODE [READY]')

# Guard the mode dispatch so an empty/partial argv prints usage instead of
# raising IndexError and dumping a traceback.
if len(sys.argv) < 2:
    raise SystemExit(USAGE)

if sys.argv[1] == '--check':
    if len(sys.argv) < 3:
        raise SystemExit(USAGE)
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
    if len(sys.argv) < 3:
        raise SystemExit(USAGE)
    container_port = int(sys.argv[2])
    with socket.create_connection(('127.0.0.1', container_port), timeout=10) as conn:
        conn.settimeout(None)
        reader = conn.makefile('rb', buffering=0)
        writer = conn.makefile('wb', buffering=0)
        threading.Thread(target=pump, args=(sys.stdin.buffer, writer), daemon=True).start()
        pump(reader, sys.stdout.buffer)
    sys.exit(0)

job_id, port, jobid, node, ready = parse_server_argv(sys.argv[1:])
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
            argv = ['srun', '--jobid=' + jobid, '--overlap', '--nodes=1',
                    '--ntasks=1', '--cpus-per-task=1', '--cpu-bind=' + cpu_bind,
                    '--nodelist=' + node, '--unbuffered', 'python3',
                    os.path.abspath(__file__), '--connect', str(container_port)]
            with lock:
                child = subprocess.Popen(
                    argv,
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
    # port back and publish it in this job's ready file.
    listener.bind(('127.0.0.1', int(port)))
    listener.listen(32)
    bound_port = listener.getsockname()[1]
    # Ready file: "<pid> <port>\n".  Written atomically and per job.
    write_ready(ready, os.getpid(), bound_port)
    if job_id:
        print('computemcp relay for job %s on port %d' % (job_id, bound_port),
              file=sys.stderr)
    while True:
        conn, _ = listener.accept()
        threading.Thread(target=serve, args=(conn,), daemon=True).start()
