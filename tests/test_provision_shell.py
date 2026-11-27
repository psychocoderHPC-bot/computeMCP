# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""End-to-end regression tests for the Slurm bundle shell helpers.

The tests run the REAL shipped scripts from
``src/compute_mcp/bundles/computemcp-slurm/`` through bash, against a stub
PATH that emulates a minimal Slurm cluster (``sbatch``/``squeue``/``scancel``/
``srun``), a stateful ``docker`` client and a loopback SSH-banner server.

The tests are hermetic *per test*, not per suite: the mode the provisioning
helper picks is computed FROM THE STATE IT READS (``STATE/mode``), not from
PATH alone.  Previously tracked jobs therefore take the path the mode file
specifies, even if the PATH shifted.  To keep the direct tests deterministic,
each of them starts from a fresh (absent) ``STATE`` so the helper's mode
stays ``direct`` even if an earlier test in the suite ran in Slurm mode.

Covered behavior:

- Slurm mode (sbatch + srun on PATH): ``sbatch`` is invoked, the jobid is
  tracked in the state directory and the endpoint is the forward/relay port.
- Direct mode (no sbatch/srun on PATH): no Slurm tool is ever called; the
  container is built and started on the node, the endpoint is the container's
  own loopback port, and a second provision reuses the running container
  (no rebuild, no restart) -- the "setup if needed / start if not running /
  connect" contract.
- The ``COMPUTEMCP_PROVISION_ENV`` hook: each line runs in the provisioning
  shell (login node), and, on the Slurm path, the per-job settings file lets
  ``computemcp-job.sh`` re-apply the same lines in the job step (compute
  node): a login-node ``module load`` does not propagate into the allocation.
- A failing hook line aborts provisioning before any build or start.

Everything runs in a fresh ``tmp_path``; the only network access is loopback
banner servers on ephemeral ports.  Slurm-mode tests spawn one background
relay/job process that is torn down in a ``finally`` block; subprocess
run timeouts (180 s) bound the worst case.  Every test asserts which PATH
variant it used, so a mis-set PATH fails loudly rather than silently picking
the wrong provisioning mode.
"""

from __future__ import annotations

import ast
import contextlib
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

_TEST_DIR = Path(__file__).resolve().parent
if str(_TEST_DIR) not in sys.path:
    sys.path.insert(0, str(_TEST_DIR))

from fakes.conftest import fake_slurm_ctl, fake_slurm_env  # noqa: E402,F401

REPO = Path(__file__).resolve().parent.parent
BUNDLE_DIR = REPO / "src" / "compute_mcp" / "bundles" / "computemcp-slurm"
PROVISION = BUNDLE_DIR / "computemcp-provision.sh"
JOB = BUNDLE_DIR / "computemcp-job.sh"
CONTAINER = BUNDLE_DIR / "computemcp-container.sh"
FAKE_SLURM = REPO / "tests" / "fakes" / "fake_slurm.py"

pytestmark = pytest.mark.skipif(
    shutil.which("bash") is None, reason="bash is required for the shell tests"
)


# --- helpers ---------------------------------------------------------------


def _free_port() -> int:
    sock = socket.socket()
    try:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]
    finally:
        sock.close()


def _wait_port(port: int, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return True
        except OSError:
            time.sleep(0.05)
    return False


# Per-test roots used by :func:`_cleanup`.  Parallel pytest workers in other
# checkouts may run the same shell helpers; a global ``pkill`` would kill their
# processes (and they ours).  Cleanup is therefore scoped to the test roots
# registered here instead of matching a bare script name.
_TEST_ROOTS: set[str] = set()


def _register_root(path) -> None:
    _TEST_ROOTS.add(str(path))


def _scoped_pids(patterns) -> list[int]:
    roots = [root for root in _TEST_ROOTS if root]
    if not roots:
        return []
    pids = []
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(
                "utf-8", "replace"
            )
            cwd = os.readlink(entry / "cwd")
        except (OSError, ValueError):
            continue
        if not any(name in cmdline for name in patterns):
            continue
        if any(root in cmdline or cwd.startswith(root) for root in roots):
            pids.append(int(entry.name))
    return pids


def _cleanup() -> None:
    """Stop the background helpers *this test* may still be holding."""
    for pid in _scoped_pids(
        (
            "t8_banner_server.py",
            "t8_connect_bridge.py",
            "computemcp-relay.py",
            "computemcp-job.sh",
        )
    ):
        with contextlib.suppress(OSError, ValueError):
            subprocess.run(["kill", str(pid)], check=False)


def _docker_counts(log: Path) -> dict:
    if not log.exists():
        return {"build": 0, "create": 0, "start": 0}
    text = log.read_text(encoding="utf-8", errors="replace")
    return {
        "build": text.count("docker build --build-arg"),
        "create": text.count("\ndocker create"),
        "start": text.count("docker start"),
        # "docker stop <name>" as logged by the stub; the distinct trailing
        # space keeps it from counting "docker start".
        "stop": sum(
            1
            for line in text.splitlines()
            if line.startswith("docker stop ") or line.startswith("docker stop\t")
        ),
    }


def _published_port(container_port: int) -> int:
    """A host port distinct from ``container_port`` for the docker stub.

    Real ``--publish 127.0.0.1::<port>`` makes Docker pick an arbitrary
    ephemeral host port, so the host endpoint must never be assumed equal to
    the container port.  The test reserves a free port and pins it through
    ``DOCKER_STUB_PUBLISHED_PORT``; the docker stub's own default is a fixed
    40000+ mapping (``published_port`` in the stub).
    """
    for _ in range(50):
        port = _free_port()
        if port != container_port:
            return port
    raise AssertionError("could not find a free published port")


def _shell_quote(text: str) -> str:
    """Render a value the way bash ``printf %q`` does for the settings file."""
    return "'" + text.replace("'", "'\\''") + "'"


def _embedded_python_blocks(script: Path) -> list[tuple[str, str]]:
    """Extract ``python3 ... <<'TAG' ... TAG`` heredocs from a shell script.

    Only blocks whose opening command mentions ``python`` are returned, and the
    interpreter reads the body verbatim, so parsing the captured body is the
    same source the shipped script executes.
    """
    text = script.read_text(encoding="utf-8")
    blocks: list[tuple[str, str]] = []
    for match in re.finditer(r"<<'([A-Za-z_][A-Za-z0-9_]*)'\n", text):
        tag = match.group(1)
        line_start = text.rfind("\n", 0, match.start()) + 1
        header = text[line_start:match.start()]
        if "python" not in header:
            continue
        end = text.find("\n" + tag + "\n", match.end())
        if end == -1:
            continue
        blocks.append((tag, text[match.end():end]))
    return blocks


# --- stub scripts ----------------------------------------------------------


BANNER_SERVER = '''#!/usr/bin/env python3
"""Tiny TCP server that sends an SSH banner on every connection.

Every accepted connection gets a single banner line; that is all the
container's ``wait_for_banner`` needs to be satisfied.
"""
import socket
import sys
import threading

port = int(sys.argv[1])


def serve(sock):
    try:
        with sock:
            sock.sendall(b"SSH-2.0-computemcp-test\\r\\n")
            sock.settimeout(2)
            while True:
                if not sock.recv(4096):
                    break
    except OSError:
        pass


with socket.socket() as listener:
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", port))
    listener.listen(16)
    sys.stderr.write("banner on %d\\n" % port)
    sys.stderr.flush()
    while True:
        conn, _ = listener.accept()
        threading.Thread(target=serve, args=(conn,), daemon=True).start()
'''

CONNECT_BRIDGE = '''#!/usr/bin/env python3
"""Bridge stdin/stdout to 127.0.0.1:<port> (the srun --connect relay step)."""
import socket
import sys
import threading

port = int(sys.argv[1])
with socket.create_connection(("127.0.0.1", port), timeout=10) as conn:
    conn.settimeout(None)
    reader = conn.makefile("rb", buffering=0)
    writer = conn.makefile("wb", buffering=0)

    def upstream():
        try:
            while True:
                data = sys.stdin.buffer.read(65536)
                if not data:
                    break
                writer.write(data)
                writer.flush()
        except (OSError, ValueError):
            pass
        finally:
            try:
                conn.shutdown(socket.SHUT_WR)
            except OSError:
                pass

    threading.Thread(target=upstream, daemon=True).start()
    try:
        while True:
            data = reader.read(65536)
            if not data:
                break
            sys.stdout.buffer.write(data)
            sys.stdout.buffer.flush()
    except (OSError, ValueError):
        pass
'''

DOCKER_STUB = r'''#!/usr/bin/env bash
# Minimal stateful docker stub for the computeMCP shell tests.
# Invocations are logged; the state directory tracks images and containers so
# repeated provisions are distinguishable from first-time builds.  "start"
# spawns the real loopback banner server so the provisioning helper's
# wait_for_banner succeeds against a live TCP endpoint.
set -euo pipefail
STATE_DIR="${DOCKER_STUB_STATE:?}"
LOG="${DOCKER_STUB_LOG:-/dev/null}"
BANNER_BIN="$BANNER_SERVER"
mkdir -p "$STATE_DIR"
printf 'docker %s\n' "$*" >> "$LOG"

CMD="${1:-}"
shift || true

img_marker() { printf '%s/image-%s' "$STATE_DIR" "$(printf '%s' "$1" | tr '/:' '__')"; }
ctr_dir() { printf '%s/ctr-%s' "$STATE_DIR" "$1"; }
# Docker maps the container port to an EPHEMERAL host port for
# ``--publish 127.0.0.1::<container-port>``; model that here.  Tests that
# exercise the distinct published endpoint pin it with
# DOCKER_STUB_PUBLISHED_PORT.  Without a pin the stub keeps the historical
# behavior (published == container port), so Slurm-mode tests that rely on a
# container-port banner are unchanged.
published_port() {
    if [ -n "${DOCKER_STUB_PUBLISHED_PORT:-}" ]; then
        printf '%s' "$DOCKER_STUB_PUBLISHED_PORT"
        return
    fi
    printf '%s' "${1:-${COMPUTEMCP_CONTAINER_PORT:-2222}}"
}

case "$CMD" in
    info) exit 0 ;;
    image)
        [ "${1:-}" = inspect ] || exit 1
        [ -f "$(img_marker "${2:-}")" ] && exit 0 || exit 1
        ;;
    build)
        TAG=""
        CONTEXT=""
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --tag) TAG="$2"; shift 2 ;;
                --build-arg) shift 2 ;;
                *) CONTEXT="$1"; shift ;;
            esac
        done
        # Optional hook: capture the generated Dockerfile so a test can assert
        # on the resolved base image (DOCKER_STUB_DOCKERFILE_LOG).
        if [ -n "${DOCKER_STUB_DOCKERFILE_LOG:-}" ] && [ -f "$CONTEXT/Dockerfile" ]; then
            cat "$CONTEXT/Dockerfile" >> "$DOCKER_STUB_DOCKERFILE_LOG"
        fi
        touch "$(img_marker "$TAG")"
        exit 0
        ;;
    container)
        SUB="${1:-}"; shift || true
        case "$SUB" in
            inspect)
                FORMAT=""
                NAME=""
                while [ "$#" -gt 0 ]; do
                    case "$1" in
                        --format) FORMAT="$2"; shift 2 ;;
                        *) NAME="$1"; shift ;;
                    esac
                done
                [ -d "$(ctr_dir "$NAME")" ] || exit 1
                case "$FORMAT" in
                    *owner-uid*) cat "$(ctr_dir "$NAME")/owner" ;;
                    *org.computemcp.system*) cat "$(ctr_dir "$NAME")/system" ;;
                    *State.Running*)
                        if [ -f "$(ctr_dir "$NAME")/running" ]; then echo true; else echo false; fi
                        ;;
                    *) echo "$NAME" ;;
                esac
                exit 0
                ;;
            port)
                NAME="$1"
                [ -f "$(ctr_dir "$NAME")/running" ] || exit 1
                # Docker assigns an ephemeral HOST port for
                # ``--publish 127.0.0.1::<container-port>``; the real mapping is
                # not the container port.
                printf '127.0.0.1:%s\n' "$(published_port)"
                exit 0
                ;;
            *) exit 0 ;;
        esac
        ;;
    create)
        NAME=""
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --name) NAME="$2"; shift 2 ;;
                --label) shift 2 ;;
                --publish|--mount|--env|--memory|--cpus|--device|--security-opt|--group-add|--restart) shift 2 ;;
                *) shift ;;
            esac
        done
        mkdir -p "$(ctr_dir "$NAME")"
        echo "$(id -u)" > "$(ctr_dir "$NAME")/owner"
        echo "${COMPUTEMCP_SYSTEM:-computemcp}" > "$(ctr_dir "$NAME")/system"
        exit 0
        ;;
    cp) exit 0 ;;
    start)
        NAME="$1"
        PORT="${COMPUTEMCP_CONTAINER_PORT:-2222}"
        PUBLISHED="$(published_port "$PORT")"
        DIR="$(ctr_dir "$NAME")"
        mkdir -p "$DIR"
        if [ ! -f "$DIR/running" ]; then
            # The container SSH endpoint is reachable inside on the container
            # port and on the host through the published port; the stub serves
            # the banner on both so either check succeeds.
            nohup python3 "$BANNER_BIN" "$PORT" >"$DIR/banner.out" 2>&1 </dev/null 9>&- &
            echo "$!" > "$DIR/banner.pid"
            nohup python3 "$BANNER_BIN" "$PUBLISHED" >"$DIR/banner-published.out" 2>&1 </dev/null 9>&- &
            echo "$!" > "$DIR/banner-published.pid"
            touch "$DIR/running"
        fi
        exit 0
        ;;
    stop)
        NAME="$1"
        DIR="$(ctr_dir "$NAME")"
        for PIDFILE in "$DIR/banner.pid" "$DIR/banner-published.pid"; do
            if [ -f "$PIDFILE" ]; then
                kill "$(cat "$PIDFILE")" 2>/dev/null || true
            fi
        done
        rm -f "$DIR/running"
        exit 0
        ;;
    exec) exit 0 ;;
    logs) exit 0 ;;
    *) exit 0 ;;
esac
'''

SBATCH = '''#!/usr/bin/env bash
# sbatch stub: log argv, then emulate the scheduler starting the batch script.
# Slurm passes the batch script path and the per-job settings file as the last
# two positional arguments, so it runs that script in the background with a
# fake job id and reports the id plus a test cluster name.  With
# SLURM_STRIP_JOB_ENV=1 it launches the batch script with HOME and every
# COMPUTEMCP_* variable removed, modelling a restrictive ``--export`` policy;
# the positional settings path survives, so the batch script must re-source it.
set -euo pipefail
LOG="${SLURM_STUB_LOG:-/dev/null}"
printf 'sbatch %s\\n' "$*" >> "$LOG"

ARGS=("$@")
N=${#ARGS[@]}
SETTINGS="${ARGS[N-1]}"
JOB_SCRIPT="${ARGS[N-2]}"
export SLURM_JOB_ID=4242
LAUNCH=(bash)
if [ "${SLURM_STRIP_JOB_ENV:-}" = 1 ]; then
    LAUNCH=(env -u HOME
        -u COMPUTEMCP_SYSTEM -u COMPUTEMCP_STORAGE_ROOT -u COMPUTEMCP_STATE_DIR
        -u COMPUTEMCP_BUNDLE_DIR -u COMPUTEMCP_SANDBOX_DIR -u COMPUTEMCP_HOST_HOME
        -u COMPUTEMCP_CONTAINER_RUNTIME -u COMPUTEMCP_IMAGE -u COMPUTEMCP_GPU_VENDORS
        -u COMPUTEMCP_BUILD_LOCATION -u COMPUTEMCP_CPUS_PER_NODE
        -u COMPUTEMCP_GPUS_PER_NODE -u COMPUTEMCP_MEMORY_PER_NODE_MIB
        -u COMPUTEMCP_CONTAINER_PORT -u COMPUTEMCP_SSH_WAIT_SECONDS -u COMPUTEMCP_SSH_USER
        -u COMPUTEMCP_SSH_PUBLIC_KEY -u COMPUTEMCP_CPU_BIND -u COMPUTEMCP_SRUN_ARGS
        -u COMPUTEMCP_PROVISION_ENV
        bash)
fi
if [ -n "${SLURM_JOB_OUT:-}" ]; then
    nohup "${LAUNCH[@]}" "$JOB_SCRIPT" "$SETTINGS" >"$SLURM_JOB_OUT" 2>&1 </dev/null 9>&- &
    echo "$!" > "$SLURM_JOB_OUT.pid"
fi
printf '4242;testcluster\\n'
'''

SQUEUE = '''#!/usr/bin/env bash
set -euo pipefail
JOBID_FILE="${SLURM_JOBID_FILE:-}"
JOBID="$(cat "$JOBID_FILE" 2>/dev/null || true)"
if [ -n "$JOBID" ]; then
    printf '%s|RUNNING\\n' "$JOBID"
fi
'''

SCANCEL = '''#!/usr/bin/env bash
set -euo pipefail
printf 'scancel %s\\n' "$*" >> "${SLURM_STUB_LOG:-/dev/null}"
exit 0
'''

SRUN = '''#!/usr/bin/env bash
# srun stub.
#   * If the step ends in ``--connect <port>`` (the relay's per-connection
#     step), bridge stdin/stdout to 127.0.0.1:<port>.
#   * Otherwise run the requested command exactly as an srun job step would
#     and return when it exits (the allocation is kept alive by job.sh itself).
set -euo pipefail
LOG="${SLURM_STUB_LOG:-/dev/null}"
printf 'srun %s\\n' "$*" >> "$LOG"
env | grep '^COMPUTEMCP_' | sort >> "$LOG" || true

ARGS=("$@")
N=${#ARGS[@]}
if [ "$N" -ge 2 ] && [ "${ARGS[N-2]}" = "--connect" ]; then
    exec python3 "$CONNECT_BRIDGE" "${ARGS[N-1]}"
fi

exec "$@"
'''


def _write_stub(path: Path, content: str, *, executable: bool = True) -> None:
    path.write_text(content, encoding="utf-8")
    if executable:
        path.chmod(0o755)


# --- stub PATH management --------------------------------------------------


class _StubPath:
    """A stub PATH prefix.

    ``bin`` is the directory holding the stub executables; ``shell_path`` is
    the final PATH string pytest exposes to the helper (the host tools env is
    set in ``_base_env``).
    """

    def __init__(self, bin: Path, shell_path: str):
        self.bin = bin
        self.shell_path = shell_path


class _Stubs:
    """Executable stub PATHs for the shell tests: one with Slurm, one without.

    Both share the same ``banner_server.py``/``connect_bridge.py`` helper and
    the same ``docker`` stub.  The forcing stub directory is a single temp
    directory - the host PATH is appended AFTER it in ``_base_env`` so that
    real system tools (``bash``, ``awk``, ``python3``, ``install``, ...)
    resolve, while ``sbatch``/``srun`` resolve to the fixtures (or are
    absent in the no-Slurm case).  This matches the production deployment
    where the gateway's route is a set of PATH entries: a stub first, the
    rest of the system after it.
    """

    def __init__(self, root: Path):
        self.root = root
        base = root / "stubbin"
        base.mkdir()
        self.banner = base / "t8_banner_server.py"
        self.bridge = base / "t8_connect_bridge.py"
        _write_stub(self.banner, BANNER_SERVER, executable=False)
        _write_stub(self.bridge, CONNECT_BRIDGE, executable=False)
        _write_stub(base / "docker", DOCKER_STUB)
        _write_stub(base / "squeue", SQUEUE)
        _write_stub(base / "scancel", SCANCEL)
        _write_stub(base / "sbatch", SBATCH)
        _write_stub(base / "srun", SRUN)

        slurm_path = str(base) + os.pathsep + os.environ.get("PATH", "")
        self.slurm = _StubPath(base, slurm_path)

        # Direct PATH: stub tools WITHOUT sbatch/srun, then system.
        # bash's PATH lookup (command -v) consults directories in order, so
        # the absence of sbatch/srun in the stub dir means the lookup
        # continues to the system dirs.  The host sandbox does NOT ship
        # slurm tools, so this is a true hermetic "no slurm" PATH.  We
        # still assert the absence before each direct-mode run as a guard
        # against a site installing a real cluster stack.
        direct_base = root / "stubbin-direct"
        direct_base.mkdir()
        for path in base.iterdir():
            if path.name in ("sbatch", "srun"):
                continue
            link = direct_base / path.name
            if not link.exists():
                link.symlink_to(path)
        direct_path = str(direct_base) + os.pathsep + os.environ.get("PATH", "")
        # Sanity: the direct PATH must not carry the slurm tools anywhere.
        for entry in direct_path.split(os.pathsep):
            if not entry:
                continue
            for name in ("sbatch", "srun"):
                if (Path(entry) / name).exists():
                    pytest.fail(
                        f"direct PATH carries {name!r} at {entry}: the test "
                        f"environment is not hermetic"
                    )
        self.direct = _StubPath(direct_base, direct_path)


_stubs_instance: _Stubs | None = None


@pytest.fixture
def stubs(tmp_path: Path) -> _Stubs:
    global _stubs_instance
    _stubs_instance = _Stubs(tmp_path)
    return _stubs_instance


def stubs_banner_path() -> Path:
    """Path of the banner server (public helper for housekeeping)."""
    assert _stubs_instance is not None
    return _stubs_instance.banner


class _Housekeeping:
    """Track background helper processes the tests spawn so teardown is safe.

    The docker stub spawns a banner server per started container.
    The slurm-mode provision spawns a relay process and a job.sh background
    child.  ``_cleanup`` kills them all by name; this blunt sweep also
    covers cross-test leaks (a banner from a previous run).
    """

    def __init__(self) -> None:
        self._procs: list[subprocess.Popen] = []

    def start_banner(self, port: int) -> subprocess.Popen:
        proc = subprocess.Popen(
            ["python3", str(stubs_banner_path()), str(port)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        self._procs.append(proc)
        return proc

    def stop(self) -> None:
        for proc in self._procs:
            if proc.poll() is None:
                with contextlib.suppress(OSError):
                    os.killpg(proc.pid, 15)
                with contextlib.suppress(Exception):
                    proc.wait(timeout=5)
        self._procs.clear()
        _cleanup()


@pytest.fixture
def housekeeping():
    handle = _Housekeeping()
    yield handle
    handle.stop()


def _base_env(system: str, storage_root: Path, port: int) -> dict:
    """The gateway-style COMPUTEMCP_* environment of one provisioning run.

    The caller's PATH layout decides which mode the helper picks:
    ``stubs.slurm.shell_path`` puts the slurm stubs first;
    ``stubs.direct.shell_path`` puts only the non-slurm stubs first.  Either
    way the remainder of the host PATH is appended (not prepended) so a
    stray ``sbatch`` in /usr/bin would NOT be picked up before the stubs -
    matching the "stub first, system after" production layout.
    """
    _register_root(storage_root)
    _register_root(storage_root.parent)
    env = dict(os.environ)
    env.update(
        {
            "COMPUTEMCP_SYSTEM": system,
            "COMPUTEMCP_STORAGE_ROOT": str(storage_root),
            "COMPUTEMCP_CONTAINER_RUNTIME": "docker",
            "COMPUTEMCP_IMAGE": "docker://ubuntu:22.04",
            "COMPUTEMCP_CONTAINER_PORT": str(port),
            "COMPUTEMCP_FORWARD_PORT": str(port + 1),
            "COMPUTEMCP_SSH_WAIT_SECONDS": "30",
            "COMPUTEMCP_WAIT_SECONDS": "120",
            "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
            "COMPUTEMCP_NODES": "1",
            # Pin the runtime name so the existing assertions stay readable.
            # The helper defaults to ``computemcp-<uid>-<system>``; tests that
            # exercise the default derivation pop this key first.
            "COMPUTEMCP_CONTAINER_NAME": f"computemcp-{system}",
            # Both the direct ``docker start`` path (via the docker stub) and
            # the slurm ``srun --connect`` path (via the srun stub) need this.
            "BANNER_SERVER": str(stubs_banner_path()),
            "CONNECT_BRIDGE": str(stubs_banner_path().with_name("t8_connect_bridge.py")),
        }
    )
    return env


# --- tests -----------------------------------------------------------------


def test_slurm_mode_submits_job_and_reports_relay_endpoint(tmp_path, stubs):
    """Slurm present: sbatch is invoked and the relay port is the endpoint.

    The stub scheduler background-launches the batch script (like a fast
    scheduler would), which starts the container inside the allocation; the
    login-node relay then serves the configured FORWARD port.  The script
    must report exactly that login-side port: the client connects to the
    relay, which srun-bridges the bytes into the allocation.
    """
    container_port = _free_port()
    forward_port = _free_port()
    workdir = tmp_path / "slurm"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "casetest" / "state" / "jobs" / "default"

    env = _base_env("casetest", storage, container_port)
    env["COMPUTEMCP_FORWARD_PORT"] = str(forward_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["SLURM_JOB_OUT"] = str(workdir / "slurm-job.out")
    env["SLURM_JOBID_FILE"] = str(state_dir / "jobid")
    env["PATH"] = stubs.slurm.shell_path

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )
    job_pid_file = Path(env["SLURM_JOB_OUT"] + ".pid")

    def teardown():
        if job_pid_file.exists():
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(job_pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        assert result.returncode == 0, (
            f"provision exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        endpoint_line = next(
            line for line in result.stdout.splitlines()
            if line.strip().startswith("ENDPOINT")
        )
        assert (
            endpoint_line.strip() == f"ENDPOINT 127.0.0.1:{forward_port}"
        ), endpoint_line
        # The scheduler was actually invoked and the jobid was persisted.
        assert "sbatch --parsable" in (workdir / "slurm.log").read_text()
        assert (state_dir / "jobid").read_text().strip() == "4242"
        assert (state_dir / "mode").read_text().strip() == "slurm"
        # Per-job settings file (written before sbatch by design) transports
        # the whole container configuration plus the provision hook.
        settings = state_dir / "srun-casetest.settings"
        assert settings.exists()
        assert "COMPUTEMCP_PROVISION_ENV" in settings.read_text()
        # The compute node may run the job step with HOME stripped, so the
        # settings file must carry the already-resolved ABSOLUTE storage root.
        # Read it back through bash's own parser to avoid depending on ``%q``
        # quoting details.
        storage_line = next(
            line for line in settings.read_text().splitlines()
            if line.startswith("export COMPUTEMCP_STORAGE_ROOT=")
        )
        resolved = subprocess.run(
            ["bash", "-c", f"{storage_line}; printf '%s' \"$COMPUTEMCP_STORAGE_ROOT\""],
            capture_output=True,
            text=True,
        )
        assert resolved.returncode == 0, resolved.stderr
        assert resolved.stdout == str(storage), resolved.stdout
        assert resolved.stdout.startswith("/"), resolved.stdout
        # The relay must be listening on the reported forward port so that
        # the "provisioned endpoint" exposed to the gateway is real.
        assert _wait_port(forward_port, timeout=15), (
            f"relay not listening on {forward_port}\nstderr:\n{result.stderr}"
        )
    finally:
        teardown()


def test_slurm_job_start_resources_settings_when_env_is_stripped(tmp_path, stubs):
    """The direct container start works under a restrictive export policy.

    A JURECA submission with ``--export=SLURM_SUBMIT_DIR=...`` (no ``ALL``)
    strips the batch script's runtime environment, so ``computemcp-job.sh``
    starts without the ``COMPUTEMCP_*`` values the gateway exported and
    ``computemcp-container.sh`` would abort with "Neither
    COMPUTEMCP_STORAGE_ROOT nor HOME".  The settings file path is passed as a
    positional argument, which Slurm delivers even under ``--export=NONE``, and
    job.sh re-sources it in its own shell before the DIRECT container start, so
    the restrictive export never reaches the start.  The sbatch stub runs the
    batch script with HOME and every ``COMPUTEMCP_*`` variable removed; the
    container must still start.  Note there is no srun step involved: the start
    runs directly on the batch node, whose lifetime keeps the instance alive.
    """
    container_port = _free_port()
    forward_port = _free_port()
    workdir = tmp_path / "slurm-strip"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "striptest" / "state" / "jobs" / "default"

    env = _base_env("striptest", storage, container_port)
    env["COMPUTEMCP_FORWARD_PORT"] = str(forward_port)
    env["SLURM_STRIP_JOB_ENV"] = "1"
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["SLURM_JOB_OUT"] = str(workdir / "slurm-job.out")
    env["SLURM_JOBID_FILE"] = str(state_dir / "jobid")
    env["PATH"] = stubs.slurm.shell_path

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )
    job_pid_file = Path(env["SLURM_JOB_OUT"] + ".pid")

    def teardown():
        if job_pid_file.exists():
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(job_pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        settings = state_dir / "srun-striptest.settings"
        assert settings.exists()
        assert settings.read_text(), settings
        assert result.returncode == 0, (
            f"provision exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        endpoint_line = next(
            line for line in result.stdout.splitlines()
            if line.strip().startswith("ENDPOINT")
        )
        assert (
            endpoint_line.strip() == f"ENDPOINT 127.0.0.1:{forward_port}"
        ), endpoint_line
        # The start must NOT have fallen back to the HOME-less failure path:
        # job.sh re-sourced the settings file in its own shell.
        job_out = (workdir / "slurm-job.out").read_text()
        combined = result.stderr + job_out
        assert "Neither COMPUTEMCP_STORAGE_ROOT nor HOME" not in combined, combined
        # It reached the container runtime: the docker stub started the container.
        docker_log = workdir / "docker.log"
        assert docker_log.exists()
        assert "\ndocker start " in docker_log.read_text(), docker_log.read_text()
    finally:
        teardown()


def test_direct_mode_builds_once_starts_once_and_reuses(tmp_path, stubs):
    """No Slurm: direct mode on this node, idempotent on repeat runs.

    Regression core of the feature: first provision must build
    (``docker build --build-arg`` exactly once), create the container and
    start it (``docker start`` exactly once, which in the stub brings up a
    real loopback banner server) and emit the container's PUBLISHED host port.
    Docker publishes an ephemeral host port (the stub maps the container port
    to a distinct 40000+ port), so the endpoint must be the mapping, not the
    container port.  The second provision against the same storage root must
    NOT build or start again: the container is running, so it is reused and
    the same endpoint line is emitted.  This is the "setup if needed / start
    if not running / connect" contract of direct_provision.
    """
    container_port = _free_port()
    workdir = tmp_path / "direct"
    workdir.mkdir()
    storage = workdir / "storage"

    env = _base_env("directtest", storage, container_port)
    published_port = _published_port(container_port)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path

    docker_log = workdir / "docker.log"
    slurm_log = workdir / "slurm.log"

    first = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )

    def teardown():
        _cleanup()

    try:
        assert published_port != container_port, (
            "test must exercise a host port distinct from the container port"
        )
        assert first.returncode == 0, (
            f"first provision exited {first.returncode}\n"
            f"stdout:\n{first.stdout}\nstderr:\n{first.stderr}"
        )
        # Direct mode emits Docker's published host port, never the container port.
        assert (
            first.stdout.splitlines()[-1].strip()
            == f"ENDPOINT 127.0.0.1:{published_port}"
        ), first.stdout
        # Direct mode must never touch the scheduler.  The slurm log is
        # empty (sbatch/srun were not resolved; the stubs are not on PATH
        # and the host does not carry the real tools).
        assert not slurm_log.exists() or not slurm_log.read_text().strip(), (
            f"direct mode invoked a Slurm tool:\n{slurm_log.read_text()}"
        )
        # First time: exactly one build, one create, one start.
        assert _docker_counts(docker_log) == {
            "build": 1, "create": 1, "start": 1, "stop": 0,
        }, _docker_counts(docker_log)
        # The published host port must really serve the SSH endpoint;
        # that is what the gateway will forward to.
        assert _wait_port(published_port, timeout=15)

        second = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert second.returncode == 0, second.stderr
        # Same endpoint line, printed again (reuse, not re-provision).
        assert (
            second.stdout.splitlines()[-1].strip()
            == f"ENDPOINT 127.0.0.1:{published_port}"
        ), second.stdout
        # The build/start/reuse invariant: NOTHING rebuilt or restarted on
        # the second run while the container stayed alive.
        assert _docker_counts(docker_log) == {
            "build": 1, "create": 1, "start": 1, "stop": 0,
        }, _docker_counts(docker_log)
        assert "Reusing running container" in second.stderr
        # The tracked mode is direct and status reports a live endpoint.
        assert (
            storage / "directtest" / "state" / "mode"
        ).read_text().strip() == "direct"
        status = subprocess.run(
            ["bash", str(PROVISION), "status"],
            capture_output=True,
            text=True,
            env=env,
            cwd=workdir,
        )
        assert status.returncode == 0
        assert f"endpoint: 127.0.0.1:{published_port}" in status.stdout
        assert "state: running" in status.stdout
    finally:
        teardown()


def test_default_container_name_includes_uid(tmp_path, stubs):
    """Without an override the runtime name embeds the remote uid.

    Docker is daemon-global, so ``computemcp-$SYSTEM`` collides for two users
    on the same node.  The default derivation is
    ``computemcp-<id -u>-<system>``; this runs the real provisioner without
    ``COMPUTEMCP_CONTAINER_NAME`` and asserts the docker stub trace carries the
    uid-qualified name, and that its image tag derives from the same name.
    """
    container_port = _free_port()
    workdir = tmp_path / "defaultname"
    workdir.mkdir()
    storage = workdir / "storage"
    published_port = _published_port(container_port)
    expected = f"computemcp-{os.getuid()}-defaultname"

    env = _base_env("defaultname", storage, container_port)
    # Exercise the default derivation, not the pinned test override.
    env.pop("COMPUTEMCP_CONTAINER_NAME", None)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path

    try:
        result = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert result.returncode == 0, (
            f"provision exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        log = (workdir / "docker.log").read_text(encoding="utf-8", errors="replace")
        # The container was created/started and the image built under the
        # uid-qualified name; the image tag is the lowercased NAME.
        assert f"--name {expected}" in log, log
        assert "docker build --build-arg" in log
        assert f"--tag {expected}:latest" in log, log
        assert f"docker start {expected}" in log, log
        # The stub records the state under a directory named after the container.
        assert (workdir / "dockerstate" / f"ctr-{expected}").is_dir()
    finally:
        _cleanup()


def test_container_name_override_is_used_and_validated(tmp_path, stubs):
    """The optional override is honored and still validated.

    ``COMPUTEMCP_CONTAINER_NAME`` lets an operator pin a name.  A legal value
    is used verbatim in the docker trace; an illegal value (a ``/`` or an empty
    segment) exits 2 with a clear message before any docker command runs.
    """
    container_port = _free_port()
    workdir = tmp_path / "override"
    workdir.mkdir()
    storage = workdir / "storage"
    published_port = _published_port(container_port)

    env = _base_env("overridetest", storage, container_port)
    env["COMPUTEMCP_CONTAINER_NAME"] = "computemcp-custom"
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path

    try:
        result = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert result.returncode == 0, result.stderr
        log = (workdir / "docker.log").read_text(encoding="utf-8", errors="replace")
        assert "--name computemcp-custom" in log, log

        # An illegal name is rejected before touching docker.
        bad_workdir = tmp_path / "override-bad"
        bad_workdir.mkdir()
        bad_env = _base_env("overridetest", bad_workdir / "storage", _free_port())
        bad_env["COMPUTEMCP_CONTAINER_NAME"] = "computemcp/evil"
        bad_env["DOCKER_STUB_LOG"] = str(bad_workdir / "docker.log")
        bad_env["DOCKER_STUB_STATE"] = str(bad_workdir / "dockerstate")
        bad_env["PATH"] = stubs.direct.shell_path
        bad = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=60,
            env=bad_env,
            cwd=bad_workdir,
        )
        assert bad.returncode == 2, (bad.returncode, bad.stderr)
        assert "Invalid container name" in bad.stderr, bad.stderr
        bad_log = bad_workdir / "docker.log"
        assert not bad_log.exists() or not bad_log.read_text().strip(), (
            "an invalid override still invoked docker"
        )
    finally:
        _cleanup()


def test_direct_mode_status_reports_published_endpoint(tmp_path, stubs):
    """The ``status`` action reports Docker's published host port.

    It must read the recorded ``container.endpoint`` (the real mapping), not
    hardcode the container port, because Docker publishes an ephemeral host
    port for ``--publish 127.0.0.1::<container-port>``.
    """
    container_port = _free_port()
    workdir = tmp_path / "status"
    workdir.mkdir()
    storage = workdir / "storage"
    published_port = _published_port(container_port)

    env = _base_env("statustest", storage, container_port)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path

    def teardown():
        _cleanup()

    try:
        provision = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert provision.returncode == 0, provision.stderr
        # The endpoint file holds the real published mapping.
        endpoint_file = storage / "statustest" / "state" / "container.endpoint"
        assert endpoint_file.read_text().strip() == (
            f"127.0.0.1:{published_port}"
        ), endpoint_file.read_text()
        status = subprocess.run(
            ["bash", str(PROVISION), "status"],
            capture_output=True,
            text=True,
            env=env,
            cwd=workdir,
        )
        assert status.returncode == 0, status.stderr
        assert f"endpoint: 127.0.0.1:{published_port}" in status.stdout, status.stdout
        assert f"endpoint: 127.0.0.1:{container_port}" not in status.stdout
        assert "state: running" in status.stdout
    finally:
        teardown()


def test_direct_mode_live_port_beats_stale_cached_endpoint(tmp_path, stubs):
    """A changed published port heals even with a stale ``container.endpoint``.

    Regression for the daemon-restart staleness bug: the cached endpoint used
    to be returned whenever it was valid-format, BEFORE the live runtime query,
    so a Docker daemon restart that re-published the container on a new
    ephemeral host port left the gateway forwarding to the dead port on every
    ``status``/``target-refresh``.  The live ``docker container port`` query is
    now authoritative; a stale file must neither be emitted nor survive the
    call, and the reported endpoint must match the current published port.
    """
    container_port = _free_port()
    workdir = tmp_path / "stale"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "staletest" / "state"
    old_port = _published_port(container_port)
    new_port = _published_port(container_port)
    assert old_port != new_port and new_port != container_port

    env = _base_env("staletest", storage, container_port)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(old_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path

    def teardown():
        _cleanup()

    try:
        first = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert first.returncode == 0, first.stderr
        assert (
            first.stdout.splitlines()[-1].strip()
            == f"ENDPOINT 127.0.0.1:{old_port}"
        ), first.stdout

        # Simulate a daemon restart that re-publishes on a new host port: the
        # stub reports the new mapping while the cached file still holds the
        # dead one.
        endpoint_file = state_dir / "container.endpoint"
        endpoint_file.write_text(f"127.0.0.1:{old_port}\n", encoding="utf-8")
        env["DOCKER_STUB_PUBLISHED_PORT"] = str(new_port)

        second = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert second.returncode == 0, second.stderr
        assert (
            second.stdout.splitlines()[-1].strip()
            == f"ENDPOINT 127.0.0.1:{new_port}"
        ), second.stdout
        # The stale file was refreshed to the live mapping.
        assert endpoint_file.read_text().strip() == f"127.0.0.1:{new_port}"

        status = subprocess.run(
            ["bash", str(PROVISION), "status"],
            capture_output=True,
            text=True,
            env=env,
            cwd=workdir,
        )
        assert status.returncode == 0, status.stderr
        assert f"endpoint: 127.0.0.1:{new_port}" in status.stdout, status.stdout
        assert f"endpoint: 127.0.0.1:{old_port}" not in status.stdout
    finally:
        teardown()


def test_direct_stop_without_container_runtime_env(tmp_path, stubs):
    """``stop`` with only COMPUTEMCP_STATE_DIR must still stop the container.

    Regression: the gateway invoked ``stop`` with no COMPUTEMCP_* environment,
    so the helper exited 2 with "must be apptainer or docker" and the remote
    container was never stopped.  With the runtime recorded in
    ``$STATE/container.runtime`` at provision time, ``stop`` derives it and
    stops the container without any runtime variable.
    """
    container_port = _free_port()
    workdir = tmp_path / "stopenv"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "stopruntime" / "state"
    published_port = _published_port(container_port)

    provision_env = _base_env("stopruntime", storage, container_port)
    provision_env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    provision_env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    provision_env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    provision_env["PATH"] = stubs.direct.shell_path

    def teardown():
        _cleanup()

    try:
        provision = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=provision_env,
            cwd=workdir,
        )
        assert provision.returncode == 0, provision.stderr
        assert (state_dir / "container.runtime").read_text().strip() == "docker"

        # The gateway-style stop environment: state dir only, no COMPUTEMCP_*
        # runtime/port/system.  PATH still reaches the docker stub.
        stop_env = {
            "PATH": stubs.direct.shell_path,
            "HOME": os.environ.get("HOME", str(workdir)),
            "COMPUTEMCP_STATE_DIR": str(state_dir),
            # The provisioner that created ``computemcp-stopruntime`` used this
            # pinned override; the env-less stop must resolve the same name.
            "COMPUTEMCP_CONTAINER_NAME": "computemcp-stopruntime",
            "BANNER_SERVER": str(stubs_banner_path()),
            "DOCKER_STUB_LOG": str(workdir / "docker-stop.log"),
            "DOCKER_STUB_STATE": str(workdir / "dockerstate"),
        }
        stop = subprocess.run(
            ["bash", str(PROVISION), "stop"],
            capture_output=True,
            text=True,
            timeout=60,
            env=stop_env,
            cwd=workdir,
        )
        assert "must be apptainer or docker" not in stop.stderr, stop.stderr
        assert stop.returncode == 0, (
            f"stop exited {stop.returncode}\nstdout:{stop.stdout}\nstderr:{stop.stderr}"
        )
        # The container was actually stopped through the docker runtime.
        stop_log = workdir / "docker-stop.log"
        assert stop_log.exists(), "docker stub did not run for stop"
        assert "docker stop computemcp-stopruntime" in stop_log.read_text()
        # And it no longer serves the endpoint.
        assert not _wait_port(published_port, timeout=3)
    finally:
        teardown()


def test_direct_mode_hook_marker_applied_on_login(tmp_path, stubs):
    """COMPUTEMCP_PROVISION_ENV lines run in the login-node shell.

    One hook line creates a marker file in the provisioning CWD: the helper
    must apply the line before the container steps (so the marker exists)
    and still emit the endpoint (a successful hook does not break the path).
    This covers the login-node half of the hook contract.
    """
    container_port = _free_port()
    workdir = tmp_path / "hookok"
    workdir.mkdir()
    storage = workdir / "storage"
    marker = workdir / "hook-login-marker"

    env = _base_env("hookok", storage, container_port)
    published_port = _published_port(container_port)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path
    env["COMPUTEMCP_PROVISION_ENV"] = f"touch {marker}"

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )

    def teardown():
        _cleanup()

    try:
        assert result.returncode == 0, (
            f"provision exited {result.returncode}\n{result.stderr}"
        )
        assert marker.exists(), "login-node hook line did not run"
        assert (
            result.stdout.splitlines()[-1].strip()
            == f"ENDPOINT 127.0.0.1:{published_port}"
        ), result.stdout
    finally:
        teardown()


def test_direct_mode_failing_hook_aborts_before_container(tmp_path, stubs):
    """A failing COMPUTEMCP_PROVISION_ENV line must abort the provision.

    The helper runs every hook line in the provisioning shell under
    ``set -euo pipefail``; a failing line (``false``) therefore exits before
    the container is ever built or started and no state is written.  This is
    the guard against a misconfigured ``module load`` leaving a
    half-provisioned target behind.
    """
    container_port = _free_port()
    workdir = tmp_path / "hookfail"
    workdir.mkdir()
    storage = workdir / "storage"

    env = _base_env("hookfail", storage, container_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.direct.shell_path
    env["COMPUTEMCP_PROVISION_ENV"] = "false"

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=workdir,
    )
    assert result.returncode != 0, (
        f"expected non-zero, got {result.returncode}\n{result.stdout}"
    )
    # The hook aborted before any container runtime use.
    docker_log = workdir / "docker.log"
    assert not docker_log.exists() or not docker_log.read_text().strip()
    # No state (no jobid; no mode file) was written either.
    state_dir = storage / "hookfail" / "state"
    assert not (state_dir / "jobid").exists()
    assert not (state_dir / "mode").exists()


def test_job_sh_reapplies_hook_from_settings_file(tmp_path, stubs, housekeeping):
    """computemcp-job.sh re-applies hook lines in the job step.

    A login-node ``module load`` does not propagate into the allocation, so
    the per-job settings file transports ``COMPUTEMCP_PROVISION_ENV`` (via
    ``%q``) and the batch script re-runs each line in its own shell before
    the container starts.  This runs the REAL job.sh with a settings file
    whose two hook lines create a marker and append a count: both must be
    applied in the job shell while job.sh starts the container directly on
    the batch node (banner already running) and keeps the allocation alive
    until this test tears it down.
    """
    container_port = _free_port()
    workdir = tmp_path / "jobhook"
    workdir.mkdir()
    jobdir = workdir / "jobcheck"
    jobdir.mkdir()
    state_dir = jobdir / "state"
    storage = workdir / "storage"

    env = _base_env("jobhook", storage, container_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["PATH"] = stubs.slurm.shell_path

    hook_marker = workdir / "job-hook-marker"
    hook_count = workdir / "job-hook-count"
    lines = (f"touch {hook_marker}", f"echo applied >> {hook_count}")
    env["COMPUTEMCP_PROVISION_ENV"] = "\n".join(lines)

    # Pre-seed everything the stub "start" path needs so the job's container
    # start succeeds without an image build: an existing, labeled container
    # plus a live banner server on the container port.
    dockerstate = workdir / "dockerstate"
    ctr_dir = dockerstate / "ctr-computemcp-jobhook"
    ctr_dir.mkdir(parents=True)
    (ctr_dir / "running").touch()
    (ctr_dir / "owner").write_text(str(os.getuid()), encoding="utf-8")
    (ctr_dir / "system").write_text("jobhook", encoding="utf-8")
    housekeeping.start_banner(container_port)
    assert _wait_port(container_port, timeout=15)

    settings = jobdir / "settings"
    settings.write_text(
        "\n".join(
            [
                "export COMPUTEMCP_SYSTEM=jobhook",
                f"export COMPUTEMCP_STATE_DIR={state_dir}",
                f"export COMPUTEMCP_SANDBOX_DIR={jobdir / 'sandbox'}",
                f"export COMPUTEMCP_HOST_HOME={jobdir / 'home'}",
                "export COMPUTEMCP_CONTAINER_RUNTIME=docker",
                "export COMPUTEMCP_IMAGE=docker://ubuntu:22.04",
                "export COMPUTEMCP_GPU_VENDORS=''",
                "export COMPUTEMCP_CPUS_PER_NODE=''",
                "export COMPUTEMCP_GPUS_PER_NODE=''",
                "export COMPUTEMCP_MEMORY_PER_NODE_MIB=''",
                f"export COMPUTEMCP_CONTAINER_PORT={container_port}",
                "export COMPUTEMCP_SSH_WAIT_SECONDS=15",
                "export COMPUTEMCP_SSH_USER=agent",
                "export COMPUTEMCP_SSH_PUBLIC_KEY='ssh-ed25519 AAAATEST fixture@test'",
                "export COMPUTEMCP_CPU_BIND=none",
                "export COMPUTEMCP_SRUN_ARGS=''",
                "export COMPUTEMCP_PROVISION_ENV="
                + _shell_quote(env["COMPUTEMCP_PROVISION_ENV"]),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    # Validate the round-trip before launch: sourcing the settings file must
    # reproduce the original hook byte-for-byte (spaces, newlines, parens).
    check = subprocess.run(
        ["bash", "-c",
         f"source {settings}; printf '%s' \"$COMPUTEMCP_PROVISION_ENV\""],
        capture_output=True,
        text=True,
        env=env,
    )
    assert check.returncode == 0, check.stderr
    assert check.stdout == env["COMPUTEMCP_PROVISION_ENV"]

    job_env = dict(env)
    job_env["SLURM_JOB_ID"] = "7777"
    stderr_path = workdir / "job.sh.stderr"
    job = subprocess.Popen(
        ["bash", str(JOB), str(settings)],
        env=job_env,
        cwd=jobdir,
        stdout=subprocess.DEVNULL,
        stderr=stderr_path.open("wb"),
        start_new_session=True,
    )
    try:
        # The job keeps the allocation alive forever; wait for both hook
        # lines to land in the job shell.  If the job exits on its own
        # still check the marker.
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            if hook_marker.exists() and hook_count.exists():
                break
            if job.poll() is not None:
                break
            time.sleep(0.1)
        assert hook_marker.exists(), (
            f"job.sh did not apply the compute-node hook line; "
            f"job_exited={job.poll() is not None}\n"
            f"job stderr:\n"
            f"{stderr_path.read_text(encoding='utf-8', errors='replace')}"
        )
        counts = hook_count.read_text(encoding="utf-8", errors="replace").splitlines()
        assert counts == ["applied"], counts
        # The start ran directly on the batch node: the docker stub saw the
        # container start path run (it would not appear if the start had been
        # wrapped in an srun step that exits).
        docker_log = (workdir / "docker.log").read_text(
            encoding="utf-8", errors="replace"
        )
        assert "docker container inspect computemcp-jobhook" in docker_log, docker_log
    finally:
        if job.poll() is None:
            with contextlib.suppress(OSError):
                os.killpg(job.pid, 15)
            with contextlib.suppress(Exception):
                job.wait(timeout=10)
        housekeeping.stop()


def test_slurm_mode_settings_roundtrip_preserves_hook(tmp_path, stubs):
    """The per-job settings file transports the hook byte-for-byte.

    The slurm path (covered in the sbatch test) writes
    ``srun-<system>.settings`` BEFORE submitting the job so the batch script
    can source it.  This test runs the real provision in slurm mode with a
    multi-line hook (spaces + parentheses + an embedded newline boundary) and
    asserts the settings file round-trips the value through bash ``source``
    exactly, which is the transport contract between login and compute node.
    """
    container_port = _free_port()
    forward_port = _free_port()
    workdir = tmp_path / "slurmhk"
    workdir.mkdir()
    storage = workdir / "storage"

    env = _base_env("slurmhk", storage, container_port)
    env["COMPUTEMCP_FORWARD_PORT"] = str(forward_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    state_dir_slurm = storage / "slurmhk" / "state" / "jobs" / "default"
    env["SLURM_JOBID_FILE"] = str(state_dir_slurm / "jobid")
    env["SLURM_JOB_OUT"] = str(workdir / "slurm-job.out")
    env["PATH"] = stubs.slurm.shell_path
    # Deliberately spicy values: newline delimiter, spaces, $ and (;
    # %q must protect every character for the transport to be safe.
    marker_a = workdir / "hook-a (v1)"
    marker_b = workdir / "hook-b $x"
    # Hook lines are user-authored shell (that is what makes `module load x`
    # and `source ...` work), so literal paths with spaces/parentheses must be
    # quoted by the author.  Single-quote each path; the value still carries
    # `(`, spaces, `$`, `;` and a newline, which is what the %q transport must
    # protect when the settings file is sourced on the compute node.
    hook = (
        f"touch '{marker_a}'\n"
        f"printf '%s\\n' 'x ($y) ;' > '{marker_b}'"
    )
    env["COMPUTEMCP_PROVISION_ENV"] = hook

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )
    job_pid_file = Path(env["SLURM_JOB_OUT"] + ".pid")

    def teardown():
        if job_pid_file.exists():
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(job_pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        assert result.returncode == 0, (
            f"provision exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        settings = (storage / "slurmhk" / "state" / "jobs" / "default") / "srun-slurmhk.settings"
        assert settings.exists()
        check = subprocess.run(
            ["bash", "-c",
             f"source {settings}; printf '%s' \"$COMPUTEMCP_PROVISION_ENV\""],
            capture_output=True,
            text=True,
        )
        assert check.returncode == 0, check.stderr
        assert check.stdout == hook, (
            f"round-trip mismatch:\nexpected {hook!r}\ngot      {check.stdout!r}"
        )
        # The login shell ALSO ran both lines (apply_provision_env), so
        # marker-a exists; marker-b holds the literal 'x ($y) ;' line.
        assert marker_a.exists(), "line 1 of the hook did not run on the login node"
        assert (
            marker_b.read_text(encoding="utf-8", errors="replace")
            == "x ($y) ;\n"
        ), marker_b.read_text(encoding="utf-8", errors="replace")
    finally:
        teardown()


# --- Build location: login vs compute node ---------------------------------


def _provision_slurm_arrays(tmp_path):
    """Write an argv-recording ``apptainer`` stub and return its bin dir.

    The login-node runtime check (``command -v apptainer``) needs a stub so the
    test does not depend on Apptainer being installed; the recorded argv lets a
    test prove whether the login node attempted a build.
    """
    root = tmp_path / "stublog"
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    stub = bin_dir / "apptainer"
    stub.write_text(
        f'#!/usr/bin/env bash\nprintf \'apptainer %s\\n\' "$*" >> {root}/apptainer.log\nexit 0\n',
        encoding="utf-8",
    )
    stub.chmod(0o755)
    return root, bin_dir


def _base_apptainer_env(system, storage_root, port, container_name):
    env = {
        "PATH": os.environ.get("PATH", ""),
        "COMPUTEMCP_SYSTEM": system,
        "COMPUTEMCP_STORAGE_ROOT": str(storage_root),
        "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
        "COMPUTEMCP_IMAGE": "docker://ubuntu:24.04",
        "COMPUTEMCP_CONTAINER_NAME": container_name,
        "COMPUTEMCP_CONTAINER_PORT": str(port),
        "COMPUTEMCP_FORWARD_PORT": str(port + 1),
        "COMPUTEMCP_SSH_WAIT_SECONDS": "15",
        "COMPUTEMCP_WAIT_SECONDS": "60",
        "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
        "COMPUTEMCP_NODES": "1",
    }
    return env


def test_provision_compute_build_skips_login_build(tmp_path, stubs):
    """build-location = compute: the login node must NOT build the sandbox.

    The architecture-mismatched partition case: the login node is x86-64 but the
    compute node is aarch64, so a login-node ``apptainer build`` would produce a
    sandbox that fails with exec format error.  With no sandbox present, the
    login-node provision must instead print the informational message, carry
    ``COMPUTEMCP_BUILD_LOCATION=compute`` into the per-job settings, and submit
    the allocation so the batch job builds it on the compute node.
    """
    container_port = _free_port()
    forward_port = _free_port()
    workdir = tmp_path / "computebuild"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "archtest" / "state" / "jobs" / "default"

    env = _base_apptainer_env(
        "archtest", storage, container_port, "computemcp-archtest"
    )
    env["COMPUTEMCP_FORWARD_PORT"] = str(forward_port)
    env["COMPUTEMCP_BUILD_LOCATION"] = "compute"
    env["COMPUTEMCP_WAIT_SECONDS"] = "3"
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    # No SLURM_JOB_OUT: the sbatch stub does not launch the batch job, so only
    # the login-node phase runs.  Any apptainer build here would be the bug this
    # test guards against; the job-owned build is covered separately.
    env["SLURM_JOBID_FILE"] = str(state_dir / "jobid")
    _root, app_bin = _provision_slurm_arrays(tmp_path)
    env["PATH"] = str(app_bin) + os.pathsep + stubs.slurm.shell_path

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )

    try:
        # The login node skipped the build: no sandbox tree and no build call.
        assert not (storage / "archtest" / "sandbox").exists()
        assert "will be built on the compute node" in result.stderr, result.stderr
        app_log = _root / "apptainer.log"
        assert not app_log.exists() or "build" not in app_log.read_text(
            encoding="utf-8"
        ), app_log.read_text(encoding="utf-8")
        # The submission still happened and the settings carry the flag.
        settings = state_dir / "srun-archtest.settings"
        assert settings.exists()
        assert "export COMPUTEMCP_BUILD_LOCATION=compute" in settings.read_text()
        assert "sbatch --parsable" in (workdir / "slurm.log").read_text()
    finally:
        _cleanup()


def test_provision_login_build_unchanged_by_default(tmp_path, stubs):
    """The default build-location = login keeps the existing build behavior.

    A missing Apptainer sandbox is built on the login node before submission;
    the stub records the login-node build call.
    """
    container_port = _free_port()
    forward_port = _free_port()
    workdir = tmp_path / "loginbuild"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "loginbuild" / "state" / "jobs" / "default"

    env = _base_apptainer_env(
        "loginbuild", storage, container_port, "computemcp-loginbuild"
    )
    env["COMPUTEMCP_FORWARD_PORT"] = str(forward_port)
    env["COMPUTEMCP_WAIT_SECONDS"] = "3"
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    # No SLURM_JOB_OUT: only the login-node phase runs; the stub apptainer
    # records the login-node build that this default must perform.
    env["SLURM_JOBID_FILE"] = str(state_dir / "jobid")
    _root, app_bin = _provision_slurm_arrays(tmp_path)
    env["PATH"] = str(app_bin) + os.pathsep + stubs.slurm.shell_path

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )

    try:
        assert "building it on the login node" in result.stderr, result.stderr
        app_log = (_root / "apptainer.log").read_text(encoding="utf-8")
        assert "build" in app_log, app_log
    finally:
        _cleanup()


def test_provision_rejects_invalid_build_location(tmp_path, stubs):
    """An unknown build-location is rejected with exit 2 before any build."""
    container_port = _free_port()
    workdir = tmp_path / "badbuild"
    workdir.mkdir()
    storage = workdir / "storage"
    env = _base_apptainer_env(
        "badbuild", storage, container_port, "computemcp-badbuild"
    )
    env["COMPUTEMCP_BUILD_LOCATION"] = "head"
    env["PATH"] = stubs.slurm.shell_path

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=workdir,
    )
    assert result.returncode == 2, (result.returncode, result.stderr)
    assert "Invalid COMPUTEMCP_BUILD_LOCATION" in result.stderr, result.stderr
    assert not (storage / "badbuild" / "sandbox").exists()


def test_provision_compute_without_slurm_falls_back_to_login(tmp_path, stubs):
    """Direct mode + build-location = compute warns and builds on the node.

    There is no allocation to build on, so the helper falls back to the default
    login-node build and still provisions successfully in direct mode.
    """
    container_port = _free_port()
    workdir = tmp_path / "directcompute"
    workdir.mkdir()
    storage = workdir / "storage"
    published_port = _published_port(container_port)

    env = _base_env("directcompute", storage, container_port)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published_port)
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["COMPUTEMCP_BUILD_LOCATION"] = "compute"
    env["PATH"] = stubs.direct.shell_path

    try:
        result = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert result.returncode == 0, (result.returncode, result.stderr)
        assert "requires Slurm" in result.stderr, result.stderr
        # It fell back to a normal direct-mode build on this node.
        assert _docker_counts(workdir / "docker.log")["build"] == 1
    finally:
        _cleanup()


def test_job_sh_builds_sandbox_on_compute_node(tmp_path, stubs):
    """build-location = compute: job.sh builds+configures before start.

    The login node skipped the build, so the batch script (running on the first
    allocated compute node) must build and configure the sandbox there before
    starting the container.  The stub apptainer log proves the build ran on the
    compute node.
    """
    APPTH = tmp_path / "apptainer-stub"
    APPTH.mkdir()
    call_log = tmp_path / "apptainer.log"
    app = APPTH / "apptainer"
    app.write_text(
        "#!/usr/bin/env bash\n"
        f'printf \'apptainer %s\\n\' "$*" >> {call_log}\n'
        "exit 0\n",
        encoding="utf-8",
    )
    app.chmod(0o755)

    workdir = tmp_path / "jobbuild"
    workdir.mkdir()
    state = workdir / "state"
    state.mkdir()
    sandbox = workdir / "sandbox"
    bundle = BUNDLE_DIR
    settings = workdir / "settings"
    settings.write_text(
        "\n".join(
            [
                "export COMPUTEMCP_SYSTEM=archtest",
                f"export COMPUTEMCP_STATE_DIR={state}",
                f"export COMPUTEMCP_SANDBOX_DIR={sandbox}",
                f"export COMPUTEMCP_HOST_HOME={workdir / 'home'}",
                "export COMPUTEMCP_CONTAINER_RUNTIME=apptainer",
                "export COMPUTEMCP_IMAGE=docker://ubuntu:24.04",
                "export COMPUTEMCP_BUILD_LOCATION=compute",
                "export COMPUTEMCP_SSH_PUBLIC_KEY='ssh-ed25519 AAAATEST fixture@test'",
                "export COMPUTEMCP_SSH_USER=ubuntu",
                "export COMPUTEMCP_SSH_WAIT_SECONDS=2",
                f"export COMPUTEMCP_CONTAINER_PORT={_free_port()}",
                f"export COMPUTEMCP_BUNDLE_DIR={bundle}",
                "export COMPUTEMCP_SRUN_ARGS=''",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["SLURM_JOB_ID"] = "9876"
    env["PATH"] = (
        str(APPTH) + os.pathsep + stubs.slurm.shell_path
    )
    result = subprocess.run(
        ["bash", str(JOB), str(settings)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=workdir,
    )
    calls = call_log.read_text(encoding="utf-8") if call_log.exists() else ""
    # The batch script reached the compute-node container build path: the
    # apptainer stub saw the build command.  The stub build makes no sandbox, so
    # the single build command is the proof that the job owned the build (a real
    # build produces the sandbox and its built-in configure installs the key).
    assert "build" in calls, (calls, result.stderr, result.stdout)


def test_job_sh_login_build_location_does_not_build(tmp_path, stubs):
    """build-location = login: job.sh never builds on the compute node.

    The default keeps the existing behavior exactly: the sandbox was built on
    the login node, so the batch script only starts the container (the start
    runs DIRECTLY on the batch node; no apptainer build call is made).  The
    sandbox is absent here, so start stops at container.sh's "Missing sandbox"
    gate -- proving the direct invocation reached the container script without
    an srun step.
    """
    APPTH = tmp_path / "apptainer-stub"
    APPTH.mkdir()
    call_log = tmp_path / "apptainer.log"
    app = APPTH / "apptainer"
    app.write_text(
        "#!/usr/bin/env bash\n"
        f'printf \'apptainer %s\\n\' "$*" >> {call_log}\n'
        "exit 0\n",
        encoding="utf-8",
    )
    app.chmod(0o755)

    workdir = tmp_path / "joblogin"
    workdir.mkdir()
    state = workdir / "state"
    state.mkdir()
    settings = workdir / "settings"
    settings.write_text(
        "\n".join(
            [
                "export COMPUTEMCP_SYSTEM=plain",
                f"export COMPUTEMCP_STATE_DIR={state}",
                f"export COMPUTEMCP_SANDBOX_DIR={workdir / 'sandbox'}",
                "export COMPUTEMCP_CONTAINER_RUNTIME=apptainer",
                "export COMPUTEMCP_IMAGE=docker://ubuntu:24.04",
                "export COMPUTEMCP_BUILD_LOCATION=login",
                "export COMPUTEMCP_SSH_WAIT_SECONDS=2",
                f"export COMPUTEMCP_CONTAINER_PORT={_free_port()}",
                f"export COMPUTEMCP_BUNDLE_DIR={BUNDLE_DIR}",
                "export COMPUTEMCP_SRUN_ARGS=''",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["SLURM_JOB_ID"] = "9877"
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["PATH"] = str(APPTH) + os.pathsep + stubs.slurm.shell_path
    result = subprocess.run(
        ["bash", str(JOB), str(settings)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=workdir,
    )
    calls = call_log.read_text(encoding="utf-8") if call_log.exists() else ""
    assert "build" not in calls, (calls, result.stderr)
    # The container start ran directly on this node: container.sh's own
    # missing-sandbox gate fired, and no srun step carried the start.
    assert "Missing sandbox; use build first." in result.stderr, result.stderr
    slurm_log = workdir / "slurm.log"
    slurm_text = slurm_log.read_text(encoding="utf-8") if slurm_log.exists() else ""
    assert "start" not in slurm_text, slurm_text


def test_batch_job_starts_container_directly_not_via_srun(tmp_path, stubs):
    """Regression: the Slurm batch path must start the instance WITHOUT srun.

    Under fakeroot/root-mapped Apptainer (no user namespaces) the instance's
    lifetime is tied to the ``srun`` job step: when the step command exits the
    scheduler tears down the step cgroup and kills the instance, so
    ``instance start`` reports success but the instance is gone moments later
    ("no instance found").  The batch process itself keeps the allocation
    alive, so job.sh must call ``computemcp-container.sh start`` directly.
    A stub ``srun`` and a stub ``apptainer`` both log their invocations; the
    test asserts the apptainer ``instance start`` happened and that no srun
    call carried the container start.
    """
    APPTH = tmp_path / "apptainer-stub"
    APPTH.mkdir()
    app_log = tmp_path / "apptainer.log"
    app = APPTH / "apptainer"
    app.write_text(
        "#!/usr/bin/env bash\n"
        f'printf \'apptainer %s\\n\' "$*" >> {app_log}\n'
        "exit 0\n",
        encoding="utf-8",
    )
    app.chmod(0o755)

    # srun stub records every call.  The relay's --connect steps never run in
    # this test (the relay runs on the login node), so any srun entry carrying
    # the container script would prove the start was wrapped.
    srun_dir = tmp_path / "srun-stub"
    srun_dir.mkdir()
    srun_log = srun_dir / "srun.log"
    _write_stub(
        srun_dir / "srun",
        "#!/usr/bin/env bash\n"
        f"printf 'srun %s\\\\n' \"$*\" >> {srun_log}\n"
        "exit 0\n",
    )
    # The start path probes the container port with ``ss`` when available;
    # stub it to report the port free so the run reaches ``instance start``.
    _write_stub(srun_dir / "ss", "#!/bin/sh\nexit 0\n")

    workdir = tmp_path / "directstart"
    workdir.mkdir()
    state = workdir / "state"
    state.mkdir()
    port = _free_port()
    sandbox = _make_fake_sandbox(workdir)
    result, sandbox, host_home = _run_apptainer_configure(
        tmp_path, "ubuntu", port, sandbox=sandbox
    )
    assert result.returncode == 0, result.stderr

    settings = workdir / "settings"
    settings.write_text(
        "\n".join(
            [
                "export COMPUTEMCP_SYSTEM=directstart",
                f"export COMPUTEMCP_STATE_DIR={state}",
                f"export COMPUTEMCP_SANDBOX_DIR={sandbox}",
                f"export COMPUTEMCP_HOST_HOME={host_home}",
                "export COMPUTEMCP_CONTAINER_RUNTIME=apptainer",
                "export COMPUTEMCP_IMAGE=docker://ubuntu:24.04",
                "export COMPUTEMCP_BUILD_LOCATION=login",
                "export COMPUTEMCP_SSH_PUBLIC_KEY='ssh-ed25519 AAAATEST fixture@test'",
                "export COMPUTEMCP_SSH_USER=ubuntu",
                "export COMPUTEMCP_SSH_WAIT_SECONDS=2",
                f"export COMPUTEMCP_CONTAINER_PORT={port}",
                f"export COMPUTEMCP_BUNDLE_DIR={BUNDLE_DIR}",
                "export COMPUTEMCP_SRUN_ARGS='--cpu-bind=none'",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["SLURM_JOB_ID"] = "9879"
    env["PATH"] = (
        str(srun_dir) + os.pathsep
        + str(APPTH) + os.pathsep
        + os.environ.get("PATH", "")
    )
    result = subprocess.run(
        ["bash", str(JOB), str(settings)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=workdir,
    )
    # The direct start reached ``apptainer instance start``; the stub serves no
    # banner, so the run then stops at the container script's banner wait.
    app_calls = app_log.read_text(encoding="utf-8") if app_log.exists() else ""
    assert "instance start" in app_calls, (app_calls, result.stderr, result.stdout)
    # Regression: the start did NOT go through an srun step.  A future change
    # that wraps the start in srun (the container-lifetime bug) would make the
    # srun stub log the container script path here.
    srun_text = srun_log.read_text(encoding="utf-8") if srun_log.exists() else ""
    assert "computemcp-container.sh" not in srun_text, (
        f"the container start went through srun:\n{srun_text}\n"
        f"{result.stdout}\n{result.stderr}"
    )


# --- Apptainer account provisioning ---------------------------------------

_SANDBOX_PASSWD = (
    "root:x:0:0:root:/root:/bin/bash\n"
    "daemon:x:1:1:daemon:/usr/sbin:/usr/sbin/nologin\n"
    "ubuntu:x:1000:1000:Ubuntu:/home/ubuntu:/bin/bash\n"
)


def _make_fake_sandbox(root: Path) -> Path:
    """Build the minimal Apptainer sandbox tree ``apptainer_configure`` needs.

    The real sandbox is created by ``apptainer build``; for the hermetic test
    only the executable sentinels, the host key and an Ubuntu account are
    needed, so the test does not require Apptainer on the host.
    """
    sandbox = root / "sandbox"
    for rel in (
        "usr/bin/fakeroot",
        "usr/sbin/dropbear",
        "usr/lib/openssh/sftp-server",
    ):
        path = sandbox / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
        path.chmod(0o755)
    hostkey = sandbox / "etc/dropbear/dropbear_ed25519_host_key"
    hostkey.parent.mkdir(parents=True, exist_ok=True)
    hostkey.write_text("not-a-real-key\n", encoding="utf-8")
    (sandbox / "etc/passwd").write_text(_SANDBOX_PASSWD, encoding="utf-8")
    (sandbox / "etc/shadow").write_text(
        "root:*:19000:0:99999:7:::\nubuntu:*:19000:0:99999:7:::\n",
        encoding="utf-8",
    )
    (sandbox / "etc/shells").write_text("/bin/sh\n/bin/bash\n", encoding="utf-8")
    return sandbox


def _run_apptainer_configure(
    tmp_path: Path,
    ssh_user: str | None,
    port: int,
    *,
    sandbox: Path | None = None,
) -> tuple[subprocess.CompletedProcess, Path, Path]:
    """Run the real ``apptainer_configure`` against a fake sandbox.

    A stub ``apptainer`` satisfies the helper's ``command -v apptainer`` check;
    the configuration itself only edits ordinary sandbox files with host
    ``python3``.  Returns (result, sandbox, host_home).  Pass ``sandbox`` to
    reuse an already-configured tree (idempotency tests) instead of building a
    pristine one.
    """
    root = tmp_path / "apptainer"
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    apptainer_stub = bin_dir / "apptainer"
    apptainer_stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    apptainer_stub.chmod(0o755)
    if sandbox is None:
        sandbox = _make_fake_sandbox(root)
    host_home = root / "hosthome"
    env = dict(os.environ)
    env.update(
        {
            "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
            "COMPUTEMCP_SYSTEM": "aptest",
            "COMPUTEMCP_SANDBOX_DIR": str(sandbox),
            "COMPUTEMCP_HOST_HOME": str(host_home),
            "COMPUTEMCP_CONTAINER_PORT": str(port),
            "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
            "COMPUTEMCP_STORAGE_ROOT": str(root / "storage"),
        }
    )
    if ssh_user is not None:
        env["COMPUTEMCP_SSH_USER"] = ssh_user
    else:
        env.pop("COMPUTEMCP_SSH_USER", None)
    env["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")
    result = subprocess.run(
        ["bash", str(CONTAINER), "configure"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=root,
    )
    return result, sandbox, host_home


def _passwd_account(sandbox: Path) -> dict:
    rows = {}
    for line in (sandbox / "etc/passwd").read_text(encoding="utf-8").splitlines():
        fields = line.split(":")
        rows[fields[0]] = fields
    return rows


@pytest.mark.parametrize("ssh_user", ["agent", "dev", None])
def test_apptainer_configure_renames_account_to_ssh_user(tmp_path, ssh_user):
    """Apptainer exposes the gateway's login account, not hardcoded ``ubuntu``.

    Regression: ``apptainer_configure`` used to keep the sandbox ``ubuntu``
    account and ignore ``COMPUTEMCP_SSH_USER``, so a default target
    (``container_user = "ubuntu"``) connected but could not exec (502).  The
    sandbox account is renamed to the resolved login name (default ``ubuntu``)
    and every downstream reference follows it.
    """
    expected = ssh_user or "ubuntu"
    result, sandbox, host_home = _run_apptainer_configure(
        tmp_path, ssh_user, _free_port()
    )
    assert result.returncode == 0, (
        f"configure exited {result.returncode}\n"
        f"stdout:{result.stdout}\nstderr:{result.stderr}"
    )
    accounts = _passwd_account(sandbox)
    assert expected in accounts, accounts
    if expected != "ubuntu":
        assert "ubuntu" not in accounts, f"unexpected sandbox account retained: {accounts}"
    line = accounts[expected]
    # UID/GID (and the passwd field count) are preserved from the base image.
    assert line[2] == "1000" and line[3] == "1000", line
    assert line[5] == f"/home/{expected}", line
    assert line[6] == f"/usr/local/bin/computemcp-{expected}-shell", line
    assert line[1] == "*", line

    shell_path = sandbox / f"usr/local/bin/computemcp-{expected}-shell"
    assert shell_path.is_file(), shell_path
    assert os.access(shell_path, os.X_OK)
    shell_text = shell_path.read_text(encoding="utf-8")
    assert f"HOME=/home/{expected}" in shell_text
    assert f"USER={expected}" in shell_text
    assert f"LOGNAME={expected}" in shell_text
    assert (
        (sandbox / "etc/shells")
        .read_text(encoding="utf-8")
        .splitlines()
        .count(f"/usr/local/bin/computemcp-{expected}-shell")
        == 1
    )

    startscript = (sandbox / ".singularity.d/startscript").read_text(encoding="utf-8")
    assert f"/home/{expected}/.ssh/authorized_keys" in startscript
    assert "CONTAINER_PORT" not in startscript  # substituted, not left literal

    auth = host_home / ".ssh/authorized_keys"
    assert auth.read_text(encoding="utf-8") == (
        "ssh-ed25519 AAAATEST fixture@test\n"
    )
    # The original account files are preserved for recovery.
    backup = sandbox / "etc/passwd.computemcp-backup"
    assert "ubuntu:x:1000" in backup.read_text(encoding="utf-8")


def test_apptainer_configure_rejects_invalid_ssh_user(tmp_path):
    result, _, _ = _run_apptainer_configure(tmp_path, "root", _free_port())
    assert result.returncode == 2, result
    assert "Invalid COMPUTEMCP_SSH_USER" in result.stderr, result.stderr


def test_apptainer_configure_is_idempotent(tmp_path):
    """A second configure of an already-renamed sandbox must succeed.

    Regression: every ``ensure_container`` calls ``configure``; after the first
    run the ``ubuntu`` account no longer exists (it was renamed to the login
    user), so the old "Expected exactly one existing ubuntu account" check made
    every subsequent connect abort.  The second run must reconfigure the
    requested account in place and leave exactly one account behind.
    """
    port = _free_port()
    result, sandbox, _ = _run_apptainer_configure(tmp_path, "agent", port)
    assert result.returncode == 0, (
        f"first configure exited {result.returncode}\n"
        f"stdout:{result.stdout}\nstderr:{result.stderr}"
    )
    original_backup = (sandbox / "etc/passwd.computemcp-backup").read_bytes()

    second, sandbox2, _ = _run_apptainer_configure(
        tmp_path, "agent", port, sandbox=sandbox
    )
    assert second.returncode == 0, (
        f"second configure exited {second.returncode}\n"
        f"stdout:{second.stdout}\nstderr:{second.stderr}"
    )
    assert sandbox2 == sandbox
    accounts = _passwd_account(sandbox)
    assert "agent" in accounts, accounts
    assert "ubuntu" not in accounts, accounts
    names = [line.split(":")[0] for line in
             (sandbox / "etc/passwd").read_text(encoding="utf-8").splitlines()]
    assert names.count("agent") == 1, names
    # The original base-image passwd must not be overwritten on a re-run.
    assert (
        sandbox / "etc/passwd.computemcp-backup"
    ).read_bytes() == original_backup


def test_apptainer_configure_ubuntu_is_idempotent(tmp_path):
    """When the resolved login user is ``ubuntu`` the rename is a no-op.

    The base image already ships an ``ubuntu`` account; configuring for
    ``COMPUTEMCP_SSH_USER=ubuntu`` must accept that single account as the
    target, reconfigure it in place, and succeed again on a second configure
    without creating a duplicate or aborting.
    """
    port = _free_port()
    result, sandbox, _ = _run_apptainer_configure(tmp_path, "ubuntu", port)
    assert result.returncode == 0, (
        f"first configure exited {result.returncode}\n"
        f"stdout:{result.stdout}\nstderr:{result.stderr}"
    )
    original_backup = (sandbox / "etc/passwd.computemcp-backup").read_bytes()
    accounts = _passwd_account(sandbox)
    assert accounts["ubuntu"][5] == "/home/ubuntu", accounts
    assert accounts["ubuntu"][6] == "/usr/local/bin/computemcp-ubuntu-shell", accounts

    second, sandbox2, _ = _run_apptainer_configure(
        tmp_path, "ubuntu", port, sandbox=sandbox
    )
    assert second.returncode == 0, (
        f"second configure exited {second.returncode}\n"
        f"stdout:{second.stdout}\nstderr:{second.stderr}"
    )
    assert sandbox2 == sandbox
    accounts = _passwd_account(sandbox)
    assert "ubuntu" in accounts, accounts
    names = [line.split(":")[0] for line in
             (sandbox / "etc/passwd").read_text(encoding="utf-8").splitlines()]
    assert names.count("ubuntu") == 1, names
    # The original base-image passwd must not be overwritten on a re-run.
    assert (
        sandbox / "etc/passwd.computemcp-backup"
    ).read_bytes() == original_backup


def _make_previously_managed_sandbox(
    root: Path, managed_name: str = "agent"
) -> Path:
    """Build a sandbox as if an earlier configure renamed ``ubuntu``.

    Mirrors a sandbox configured when the container user was ``managed_name``:
    the base ``ubuntu`` account is gone and the managed account carries the
    computeMCP shell path ``/usr/local/bin/computemcp-<name>-shell``.
    """
    sandbox = _make_fake_sandbox(root)
    passwd = sandbox / "etc/passwd"
    text = passwd.read_text(encoding="utf-8")
    text = text.replace(
        "ubuntu:x:1000:1000:Ubuntu:/home/ubuntu:/bin/bash",
        f"{managed_name}:x:1000:1000:Ubuntu:/home/{managed_name}:"
        f"/usr/local/bin/computemcp-{managed_name}-shell",
    )
    passwd.write_text(text, encoding="utf-8")
    shadow = sandbox / "etc/shadow"
    shadow.write_text(
        "root:*:19000:0:99999:7:::\n"
        f"{managed_name}:*:19000:0:99999:7:::\n",
        encoding="utf-8",
    )
    return sandbox


def test_apptainer_configure_reconfigures_managed_account_to_new_user(tmp_path):
    """A change of the configured container user must not need a rebuild.

    Regression: a sandbox built and configured while the container user was
    ``agent`` has no ``ubuntu`` account, so reconnecting with the new default
    ``ubuntu`` aborted ("Expected exactly one 'ubuntu' or 'ubuntu' account").
    The previously-managed account is detected by its computeMCP shell path and
    renamed in place, keeping the UID/GID and leaving the base-image backup.
    """
    root = tmp_path / "apptainer"
    sandbox = _make_previously_managed_sandbox(root, "agent")
    before = (sandbox / "etc/passwd").read_bytes()
    result, sandbox, _ = _run_apptainer_configure(
        tmp_path, "ubuntu", _free_port(), sandbox=sandbox
    )
    assert result.returncode == 0, (
        f"configure exited {result.returncode}\n"
        f"stdout:{result.stdout}\nstderr:{result.stderr}"
    )
    accounts = _passwd_account(sandbox)
    assert "ubuntu" in accounts, accounts
    assert "agent" not in accounts, accounts
    line = accounts["ubuntu"]
    assert line[2] == "1000" and line[3] == "1000", line
    assert line[5] == "/home/ubuntu", line
    assert line[6] == "/usr/local/bin/computemcp-ubuntu-shell", line
    backup = sandbox / "etc/passwd.computemcp-backup"
    assert backup.read_bytes() == before, "backup must be the pre-configure passwd"


def test_apptainer_configure_reconfigures_managed_account_to_explicit_user(tmp_path):
    """A previously-managed sandbox can be moved to any explicit user in place."""
    root = tmp_path / "apptainer"
    sandbox = _make_previously_managed_sandbox(root, "agent")
    before = (sandbox / "etc/passwd").read_bytes()
    result, sandbox, _ = _run_apptainer_configure(
        tmp_path, "dev", _free_port(), sandbox=sandbox
    )
    assert result.returncode == 0, (
        f"configure exited {result.returncode}\n"
        f"stdout:{result.stdout}\nstderr:{result.stderr}"
    )
    accounts = _passwd_account(sandbox)
    assert "dev" in accounts, accounts
    assert "agent" not in accounts, accounts
    line = accounts["dev"]
    assert line[2] == "1000", line
    assert line[6] == "/usr/local/bin/computemcp-dev-shell", line
    assert (sandbox / "etc/passwd.computemcp-backup").read_bytes() == before
    # A second run with the same user is idempotent (candidate source 2).
    original_backup = (sandbox / "etc/passwd.computemcp-backup").read_bytes()
    second, sandbox2, _ = _run_apptainer_configure(
        tmp_path, "dev", _free_port(), sandbox=sandbox
    )
    assert second.returncode == 0, (
        f"second configure exited {second.returncode}\n"
        f"stdout:{second.stdout}\nstderr:{second.stderr}"
    )
    assert sandbox2 == sandbox
    assert "dev" in _passwd_account(sandbox)
    assert (sandbox / "etc/passwd.computemcp-backup").read_bytes() == original_backup


def test_apptainer_configure_rejects_ambiguous_managed_accounts(tmp_path):
    """Two computeMCP-managed accounts are ambiguous and must be rejected."""
    root = tmp_path / "apptainer"
    sandbox = _make_previously_managed_sandbox(root, "agent")
    with (sandbox / "etc/passwd").open("a", encoding="utf-8") as stream:
        stream.write("dev:x:1001:1001:Dev:/home/dev:/usr/local/bin/computemcp-dev-shell\n")
    result, _, _ = _run_apptainer_configure(
        tmp_path, "ubuntu", _free_port(), sandbox=sandbox
    )
    assert result.returncode != 0, result
    assert "Expected exactly one" in result.stderr, result.stderr
    assert "ubuntu" in result.stderr, result.stderr


def test_apptainer_start_after_rename_uses_new_shell_path(tmp_path):
    """``start`` checks the renamed account's shell, not the stale old file.

    A sandbox configured while the container user was ``agent`` carries a
    ``computemcp-agent-shell`` file; renaming the account to ``ubuntu`` adds
    ``computemcp-ubuntu-shell`` but leaves the old file in place.  The start
    gate must check ``computemcp-<SSH_USER>-shell`` only, so the stale file is
    harmless and start proceeds past the gate and the authorized_keys check to
    ``apptainer instance start`` (the stub only records its argv, so the run
    then stops at the SSH banner wait).
    """
    root = tmp_path / "apptainer"
    sandbox = _make_previously_managed_sandbox(root, "agent")
    stale = sandbox / "usr/local/bin/computemcp-agent-shell"
    stale.parent.mkdir(parents=True, exist_ok=True)
    stale.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    stale.chmod(0o755)  # a stale configured shell, exactly as a real build left it

    result, sandbox, host_home = _run_apptainer_configure(
        tmp_path, "ubuntu", _free_port(), sandbox=sandbox
    )
    assert result.returncode == 0, (
        f"configure exited {result.returncode}\n"
        f"stdout:{result.stdout}\nstderr:{result.stderr}"
    )
    accounts = _passwd_account(sandbox)
    assert "ubuntu" in accounts, accounts
    assert "agent" not in accounts, accounts
    shell = sandbox / "usr/local/bin/computemcp-ubuntu-shell"
    assert shell.is_file(), shell
    assert os.access(shell, os.X_OK)
    # The stale file is left in place by configure; start must ignore it.
    assert stale.is_file(), stale

    # Silently failing the stub is not enough: record its argv so the run can
    # be proven to have reached ``apptainer instance start``.
    stub = root / "bin" / "apptainer"
    calls = root / "apptainer-calls.log"
    stub.write_text(f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{calls}'\nexit 0\n", encoding="utf-8")

    env = dict(os.environ)
    env.update(
        {
            "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
            "COMPUTEMCP_SYSTEM": "apstart",
            "COMPUTEMCP_SANDBOX_DIR": str(sandbox),
            "COMPUTEMCP_HOST_HOME": str(host_home),
            "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
            "COMPUTEMCP_STORAGE_ROOT": str(root / "storage"),
            "COMPUTEMCP_SSH_USER": "ubuntu",
            "COMPUTEMCP_SSH_WAIT_SECONDS": "2",
        }
    )
    # The remote uid makes the instance name unique, but this node still serves
    # the loopback banner for the container port, so keep the free fixed port.
    port = _free_port()
    env["COMPUTEMCP_CONTAINER_PORT"] = str(port)
    env["PATH"] = str(root / "bin") + os.pathsep + env["PATH"]
    result = subprocess.run(
        ["bash", str(CONTAINER), "start"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=root,
    )
    # Not the missing-configure gate...
    assert "Run configure once before starting this existing sandbox." not in (
        result.stderr
    ), result.stderr
    # ...and not the authorized_keys gate either.
    assert "Missing authorized_keys" not in result.stderr, result.stderr
    # The recorded argv proves start passed both gates and started the instance.
    assert calls.is_file(), result.stderr
    calls_text = calls.read_text(encoding="utf-8")
    assert "instance start" in calls_text, calls_text
    assert f"--bind {host_home}:/home/ubuntu" in calls_text, calls_text
    # The stub serves no SSH banner, so the run stops at the banner wait.
    assert "Apptainer SSH endpoint did not become ready." in result.stderr, (
        result.returncode,
        result.stdout,
        result.stderr,
    )


def test_apptainer_configure_rejects_ambiguous_accounts(tmp_path):
    """Two ``ubuntu`` accounts are ambiguous and must be rejected clearly."""
    root = tmp_path / "apptainer"
    sandbox = _make_fake_sandbox(root)
    with (sandbox / "etc/passwd").open("a", encoding="utf-8") as stream:
        stream.write("ubuntu:x:1001:1001:Second:/home/ubuntu2:/bin/bash\n")
    result, _, _ = _run_apptainer_configure(
        tmp_path, "agent", _free_port(), sandbox=sandbox
    )
    assert result.returncode != 0, result
    assert "Expected exactly one" in result.stderr, result.stderr
    assert "agent" in result.stderr, result.stderr


def test_apptainer_configure_rejects_cross_name_ambiguity(tmp_path):
    """A pre-added account matching the login user plus the base ``ubuntu``
    account are cross-name-ambiguous and must be rejected cleanly.

    Regression: the idempotency fix (commit 3d1f52d) accepts the requested user
    account as a candidate so a re-run of an already-renamed sandbox succeeds.
    That same branch makes a *pristine* sandbox with both ``ubuntu`` (uid 1000)
    and a pre-existing ``agent`` (uid 1001) ambiguous: two candidates.  The
    configure must abort and, being a rejection, change no account (no rename,
    no uid/GID edit) so a failed run has zero side effects.
    """
    root = tmp_path / "apptainer"
    sandbox = _make_fake_sandbox(root)
    passwd = sandbox / "etc/passwd"
    # A "pre-added" account with the requested login name and a different uid,
    # so the code finds both the base ``ubuntu`` and a ``user`` candidate.
    with passwd.open("a", encoding="utf-8") as stream:
        stream.write("agent:x:1001:1001:Agent:/home/agent:/bin/bash\n")
    original = passwd.read_bytes()
    result, _, _ = _run_apptainer_configure(
        tmp_path, "agent", _free_port(), sandbox=sandbox
    )
    assert result.returncode != 0, result
    assert "Expected exactly one" in result.stderr, result.stderr
    accounts = _passwd_account(sandbox)
    assert "ubuntu" in accounts, accounts
    assert "agent" in accounts, accounts
    assert accounts["ubuntu"][2] == "1000"
    assert accounts["agent"][2] == "1001"
    # A rejected configure must be side-effect free: passwd left byte-identical.
    assert passwd.read_bytes() == original, "passwd was modified on failure"


def test_job_sh_relocated_resolves_bundle_dir_from_settings(tmp_path):
    """A scheduler-spooled job.sh finds the container script via settings.

    JURECA copies ``computemcp-job.sh`` into ``/var/spool/parastation/jobs/`` and
    runs it there, so ``BASH_SOURCE`` no longer points at the bundle.  The
    per-job settings file carries ``COMPUTEMCP_BUNDLE_DIR``; the relocated copy
    must use it and reach ``computemcp-container.sh start``.  A stub container
    script that records its argv stands in for the real one (the real start
    needs a configured sandbox); reaching it proves the sibling resolution.
    """
    spool = tmp_path / "spool"
    spool.mkdir()
    relocated = spool / "computemcp-job.sh"
    shutil.copy2(JOB, relocated)
    state = tmp_path / "state"
    state.mkdir()
    # A sibling container script that marks the unreachable line: if job.sh
    # resolves COMPUTEMCP_BUNDLE_DIR, it runs this and fails here rather than
    # writing a ready file, which the test detects via the marker.
    bundle = tmp_path / "spoolbundle"
    bundle.mkdir()
    container = bundle / "computemcp-container.sh"
    container.write_text(
        "#!/bin/sh\necho STUB-CONTAINER-REACHED >&2\nexit 1\n", encoding="utf-8"
    )
    container.chmod(0o755)
    # srun is deliberately NOT stubbed: if job.sh still wrapped the start in an
    # srun job step, the missing srun would abort the run before the marker.
    settings = tmp_path / "settings"
    settings.write_text(
        f"export COMPUTEMCP_STATE_DIR={_shell_quote(str(state))}\n"
        f"export COMPUTEMCP_BUNDLE_DIR={_shell_quote(str(bundle))}\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.pop("COMPUTEMCP_BUNDLE_DIR", None)
    env["SLURM_JOB_ID"] = "9001"
    result = subprocess.run(
        ["bash", str(relocated), str(settings)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=spool,
    )
    assert "Missing container script" not in result.stderr, result.stderr
    # Reaching the stub container script proves the sibling resolution succeeded
    # and that the start was invoked directly (no srun).
    assert "STUB-CONTAINER-REACHED" in result.stderr, result.stderr


def test_job_sh_relocated_without_bundle_dir_reports_missing_script(tmp_path):
    """A relocated copy with no ``COMPUTEMCP_BUNDLE_DIR`` fails clearly."""
    spool = tmp_path / "spool"
    spool.mkdir()
    relocated = spool / "computemcp-job.sh"
    shutil.copy2(JOB, relocated)
    state = tmp_path / "state"
    settings = tmp_path / "settings"
    settings.write_text(
        f"export COMPUTEMCP_STATE_DIR={_shell_quote(str(state))}\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.pop("COMPUTEMCP_BUNDLE_DIR", None)
    env["SLURM_JOB_ID"] = "9002"
    result = subprocess.run(
        ["bash", str(relocated), str(settings)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=spool,
    )
    assert result.returncode == 1, result
    assert "Missing container script" in result.stderr, result.stderr
    assert str(spool) in result.stderr, result.stderr


# --- HOME-less compute-node regression ------------------------------------
#
# Slurm can submit the batch job with ``--export=NONE`` (or a custom export
# policy), so the job step -- and thus computemcp-container.sh -- runs with
# HOME stripped.  The container script previously referenced a bare ``$HOME``
# in its STORAGE_ROOT fallback under ``set -u`` and crashed with
# "HOME: unbound variable" on the compute node.  The per-job settings file now
# carries the absolute login-node COMPUTEMCP_STORAGE_ROOT, and the script
# guards every HOME reference so it neither crashes nor needs HOME when the
# absolute value is present.


def _container_env_without_home(
    root: Path, system: str, *, storage_root: str | None
) -> dict:
    """A minimal container-script environment with HOME explicitly removed."""
    env = {
        "PATH": str(root / "bin") + os.pathsep + os.environ.get("PATH", ""),
        "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
        "COMPUTEMCP_SYSTEM": system,
        "COMPUTEMCP_SANDBOX_DIR": str(root / "sandbox"),
        "COMPUTEMCP_HOST_HOME": str(root / "hosthome"),
        "COMPUTEMCP_CONTAINER_PORT": str(_free_port()),
        "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
    }
    if storage_root is not None:
        env["COMPUTEMCP_STORAGE_ROOT"] = storage_root
    env.pop("HOME", None)  # the point of the test: Slurm stripped HOME
    return env


def _stub_apptainer(root: Path) -> None:
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    stub = bin_dir / "apptainer"
    stub.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    stub.chmod(0o755)


def test_container_configure_without_home_uses_absolute_storage_root(tmp_path):
    """``configure`` with HOME unset must not crash and use the absolute root.

    Regression for the JURECA compute-node failure
    ``computemcp-container.sh: line 27: HOME: unbound variable``.  With HOME
    removed but an absolute COMPUTEMCP_STORAGE_ROOT present (the normal
    compute-node case after the settings file carries it), the script must
    succeed, create its state directory under the absolute root, and never
    mention an unbound variable.
    """
    root = tmp_path / "nohome"
    root.mkdir()
    sandbox = _make_fake_sandbox(root)
    _stub_apptainer(root)
    storage = root / "abs-storage"

    env = _container_env_without_home(root, "nohome", storage_root=str(storage))
    env["COMPUTEMCP_SANDBOX_DIR"] = str(sandbox)
    result = subprocess.run(
        ["bash", str(CONTAINER), "configure"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=root,
    )
    assert result.returncode == 0, (
        f"configure exited {result.returncode}\n"
        f"stdout:{result.stdout}\nstderr:{result.stderr}"
    )
    assert "unbound variable" not in result.stderr, result.stderr
    # The absolute root was used verbatim; STATE lives below it, not under HOME.
    assert (storage / "nohome" / "state").is_dir(), sorted(storage.rglob("*"))
    assert ".local/share/computemcp" not in result.stderr


def test_container_without_home_and_without_storage_root_fails_clearly(tmp_path):
    """Neither storage root nor HOME: a clear error, not an unbound variable.

    The old fallback ``${COMPUTEMCP_STORAGE_ROOT:-$HOME/...}`` raised a raw
    ``HOME: unbound variable`` under ``set -u``.  The script must instead exit
    non-zero naming the missing input.
    """
    root = tmp_path / "nosettings"
    root.mkdir()
    _make_fake_sandbox(root)
    _stub_apptainer(root)

    env = _container_env_without_home(root, "nosettings", storage_root=None)
    for action in ("configure", "start"):
        result = subprocess.run(
            ["bash", str(CONTAINER), action],
            capture_output=True,
            text=True,
            timeout=60,
            env=env,
            cwd=root,
        )
        assert result.returncode != 0, (action, result.stdout)
        assert "unbound variable" not in result.stderr, result.stderr
        assert "COMPUTEMCP_STORAGE_ROOT" in result.stderr, result.stderr
        assert "HOME" in result.stderr, result.stderr


def test_provision_without_home_and_without_storage_root_fails_clearly(tmp_path):
    """``provision`` likewise rejects an ambiguous HOME-less configuration."""
    workdir = tmp_path / "provision-nohome"
    workdir.mkdir()
    env = {
        "PATH": os.environ.get("PATH", ""),
        "COMPUTEMCP_CONTAINER_RUNTIME": "docker",
        "COMPUTEMCP_IMAGE": "docker://ubuntu:22.04",
        "COMPUTEMCP_SYSTEM": "provisionnohome",
        "COMPUTEMCP_CONTAINER_PORT": str(_free_port()),
        "COMPUTEMCP_FORWARD_PORT": str(_free_port()),
    }
    env.pop("HOME", None)
    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=workdir,
    )
    assert result.returncode != 0, result.stdout
    assert "unbound variable" not in result.stderr, result.stderr
    assert "COMPUTEMCP_STORAGE_ROOT" in result.stderr, result.stderr
    assert "HOME" in result.stderr, result.stderr


# --- Apptainer sandbox architecture hardening ------------------------------
#
# JURECA's ``dc-gh`` partition is ARM/aarch64 (NVIDIA Grace).  A sandbox built
# on the x86-64 login node aborts inside Apptainer with an opaque
# ``exec format error``; the start path now reads the sandbox ELF architecture,
# compares it with the node, and reports the mismatch clearly.  The node arch
# is injectable through COMPUTEMCP_NODE_ARCH (test/override only) so the check
# is hermetic.


def _write_elf_stub(path: Path, e_machine: int, *, endianness: str = "little") -> None:
    """Write a minimal ELF header carrying ``e_machine``.

    ``endianness`` selects the ELF byte order (``EI_DATA``): ``"little"``
    (ELFDATA2LSB, the default, matching the original caller) or ``"big"``
    (ELFDATA2MSB).
    """
    if endianness not in ("little", "big"):
        raise ValueError(f"unsupported endianness: {endianness!r}")
    header = bytearray(20)
    header[0:4] = b"\x7fELF"
    header[4] = 2  # EI_CLASS = ELFCLASS64
    # e_type lives at offset 16; e_machine at offsets 18-19 (0x12), whose order
    # follows the ELF header byte order.
    low = e_machine & 0xFF
    high = (e_machine >> 8) & 0xFF
    if endianness == "little":
        header[5] = 1  # EI_DATA = ELFDATA2LSB
        header[18] = low
        header[19] = high
    else:
        header[5] = 2  # EI_DATA = ELFDATA2MSB
        header[18] = high
        header[19] = low
    header[16] = 3  # ET_DYN
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(bytes(header))


def _run_apptainer_start(
    tmp_path: Path,
    *,
    sandbox_sh_machine: int,
    node_arch: str,
    ssh_user: str = "ubuntu",
    endianness: str = "little",
) -> tuple[subprocess.CompletedProcess, Path]:
    """Configure a fake sandbox, give ``bin/sh`` an ELF machine, then ``start``.

    Returns (result, apptainer call log path).  The apptainer stub records its
    argv so a test can prove whether ``instance start`` was reached.
    """
    root = tmp_path / "apptainer"
    sandbox = _make_fake_sandbox(root)
    result, sandbox, host_home = _run_apptainer_configure(
        tmp_path, ssh_user, _free_port(), sandbox=sandbox
    )
    assert result.returncode == 0, result.stderr
    _write_elf_stub(sandbox / "bin/sh", sandbox_sh_machine, endianness=endianness)

    stub = root / "bin" / "apptainer"
    calls = root / "apptainer-calls.log"
    stub.write_text(
        f"#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{calls}'\nexit 0\n",
        encoding="utf-8",
    )

    env = dict(os.environ)
    env.update(
        {
            "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
            "COMPUTEMCP_SYSTEM": "archtest",
            "COMPUTEMCP_SANDBOX_DIR": str(sandbox),
            "COMPUTEMCP_HOST_HOME": str(host_home),
            "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
            "COMPUTEMCP_STORAGE_ROOT": str(root / "storage"),
            "COMPUTEMCP_SSH_USER": ssh_user,
            "COMPUTEMCP_SSH_WAIT_SECONDS": "2",
            "COMPUTEMCP_CONTAINER_PORT": str(_free_port()),
            "COMPUTEMCP_NODE_ARCH": node_arch,
        }
    )
    env["PATH"] = str(root / "bin") + os.pathsep + env["PATH"]
    result = subprocess.run(
        ["bash", str(CONTAINER), "start"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=root,
    )
    return result, calls


def test_apptainer_start_rejects_arch_mismatch(tmp_path):
    """A sandbox of another architecture fails with an actionable message.

    Case A: the sandbox ``/bin/sh`` is aarch64 (0xb7) while the node is x86_64,
    so start must exit non-zero, name both architectures and the sandbox path,
    and never reach ``apptainer instance start``.
    """
    result, calls = _run_apptainer_start(
        tmp_path, sandbox_sh_machine=0xB7, node_arch="x86_64"
    )
    assert result.returncode != 0, (result.returncode, result.stdout, result.stderr)
    assert "does not match" in result.stderr, result.stderr
    assert "aarch64" in result.stderr, result.stderr
    assert "x86_64" in result.stderr, result.stderr
    assert "bin/sh" in result.stderr, result.stderr
    assert "exec format error" in result.stderr, result.stderr
    # The opaque error must be prevented, not merely explained afterwards.
    calls_text = calls.read_text(encoding="utf-8") if calls.exists() else ""
    assert "instance start" not in calls_text, calls_text


def test_apptainer_start_proceeds_on_matching_arch(tmp_path):
    """Matching architectures pass the check and reach ``instance start``.

    Case B: an x86-64 sandbox (0x3e) on an x86_64 node must not emit the arch
    error; the stub start succeeds and the run then stops at the banner wait,
    exactly like the existing start tests.
    """
    result, calls = _run_apptainer_start(
        tmp_path, sandbox_sh_machine=0x3E, node_arch="x86_64"
    )
    assert "does not match" not in result.stderr, result.stderr
    assert calls.is_file(), result.stderr
    calls_text = calls.read_text(encoding="utf-8")
    assert "instance start" in calls_text, calls_text
    # The stub serves no SSH banner, so the run stops at the banner wait.
    assert "Apptainer SSH endpoint did not become ready." in result.stderr, (
        result.returncode,
        result.stdout,
        result.stderr,
    )


def test_apptainer_start_warns_on_unknown_arch(tmp_path):
    """An unclassifiable sandbox only warns and proceeds.

    Case C: an unknown e_machine (0x1234) cannot be compared, so the check must
    warn instead of blocking a setup that may well work.
    """
    result, calls = _run_apptainer_start(
        tmp_path, sandbox_sh_machine=0x1234, node_arch="x86_64"
    )
    assert "does not match" not in result.stderr, result.stderr
    assert "WARNING" in result.stderr, result.stderr
    calls_text = calls.read_text(encoding="utf-8") if calls.exists() else ""
    assert "instance start" in calls_text, calls_text


def test_apptainer_start_rejects_big_endian_arch_mismatch(tmp_path):
    """A big-endian sandbox ELF is decoded and a mismatch still fails.

    Case D: a binary whose ELF header declares big-endian (``EI_DATA = 2``)
    holds ``e_machine`` in most-significant-byte-first order (bytes 18,19),
    so aarch64 encodes as 0x00 0xb7.  The check must decode that byte order,
    recognize aarch64, and reject the sandbox on an x86_64 node with the
    actionable error instead of silently passing.
    """
    result, calls = _run_apptainer_start(
        tmp_path,
        sandbox_sh_machine=0xB7,
        node_arch="x86_64",
        endianness="big",
    )
    assert result.returncode != 0, (result.returncode, result.stdout, result.stderr)
    assert "does not match" in result.stderr, result.stderr
    assert "aarch64" in result.stderr, result.stderr
    assert "x86_64" in result.stderr, result.stderr
    assert "bin/sh" in result.stderr, result.stderr
    assert "exec format error" in result.stderr, result.stderr
    calls_text = calls.read_text(encoding="utf-8") if calls.exists() else ""
    assert "instance start" not in calls_text, calls_text


def test_batch_job_start_rejects_stale_login_arch_sandbox(tmp_path):
    """The joint build-location x arch-check path catches a stale sandbox.

    With ``build-location = compute`` the login node skips the build and the
    batch job runs ``computemcp-container.sh start`` on the first allocated
    node, so a STALE sandbox - built earlier on the arch-mismatched login node
    (here: an x86-64 login leaving an e_machine 0x3e /bin/sh) - must be
    rejected by the START path's architecture gate on the aarch64 compute node,
    with the actionable "does not match this node" error.  The batch script is
    run exactly as the scheduler would (SLURM_JOB_ID + settings file): it starts
    the container DIRECTLY on this node (no srun step), and a stub apptainer that
    records argv proves ``instance start`` was never reached.
    """
    APPTH = tmp_path / "apptainer-stub"
    APPTH.mkdir()
    call_log = tmp_path / "apptainer.log"
    app = APPTH / "apptainer"
    app.write_text(
        "#!/usr/bin/env bash\n"
        f'printf \'apptainer %s\\n\' "$*" >> {call_log}\n'
        "exit 0\n",
        encoding="utf-8",
    )
    app.chmod(0o755)
    # srun stub: the start must NOT go through it.  It only logs; if job.sh ever
    # sent the start here the log would carry the container script path.
    srun_dir = tmp_path / "srun-stub"
    srun_dir.mkdir()
    srun_log = srun_dir / "srun.log"
    _write_stub(
        srun_dir / "srun",
        "#!/usr/bin/env bash\n"
        f"printf 'srun %s\\\\n' \"$*\" >> {srun_log}\n"
        "exit 0\n",
    )

    workdir = tmp_path / "stalesandbox"
    workdir.mkdir()
    state = workdir / "state"
    state.mkdir()
    sandbox = _make_fake_sandbox(workdir)
    # The stale sandbox: its /bin/sh is the login-node x86-64 binary, left
    # over from an earlier build on the x86-64 login node.
    _write_elf_stub(sandbox / "bin/sh", 0x3E)
    # The login node still manages to configure the existing (stale) sandbox:
    # configure is architecture-independent and must not be blocked here.
    configure_result, _, _ = _run_apptainer_configure(
        workdir, "ubuntu", _free_port(), sandbox=sandbox
    )
    assert configure_result.returncode == 0, configure_result.stderr

    host_home = workdir / "hosthome"
    (host_home / ".ssh").mkdir(parents=True, exist_ok=True)
    (host_home / ".ssh" / "authorized_keys").write_text(
        "ssh-ed25519 AAAATEST fixture@test\n", encoding="utf-8"
    )

    settings = workdir / "settings"
    settings.write_text(
        "\n".join(
            [
                "export COMPUTEMCP_SYSTEM=stalearch",
                f"export COMPUTEMCP_STATE_DIR={state}",
                f"export COMPUTEMCP_SANDBOX_DIR={sandbox}",
                f"export COMPUTEMCP_HOST_HOME={host_home}",
                "export COMPUTEMCP_CONTAINER_RUNTIME=apptainer",
                "export COMPUTEMCP_IMAGE=docker://ubuntu:24.04",
                "export COMPUTEMCP_BUILD_LOCATION=compute",
                "export COMPUTEMCP_SSH_PUBLIC_KEY='ssh-ed25519 AAAATEST fixture@test'",
                "export COMPUTEMCP_SSH_USER=ubuntu",
                "export COMPUTEMCP_SSH_WAIT_SECONDS=2",
                f"export COMPUTEMCP_NODE_ARCH=aarch64",
                f"export COMPUTEMCP_CONTAINER_PORT={_free_port()}",
                f"export COMPUTEMCP_BUNDLE_DIR={BUNDLE_DIR}",
                "export COMPUTEMCP_SRUN_ARGS=''",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env["SLURM_JOB_ID"] = "9878"
    # The batch script runs as the scheduler would: srun/stage tools from the
    # stub dir, apptainer from the apptainer stub, everything else system.
    env["PATH"] = (
        str(srun_dir) + os.pathsep
        + str(APPTH) + os.pathsep
        + os.environ.get("PATH", "")
    )
    result = subprocess.run(
        ["bash", str(JOB), str(settings)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=workdir,
    )
    calls = call_log.read_text(encoding="utf-8") if call_log.exists() else ""
    # The batch script recognizes the STALE sandbox as present, so the
    # compute-node build path is not triggered (the job only builds a MISSING
    # sandbox): no ``apptainer build`` call was made.
    assert "build" not in calls, (calls, result.stderr)
    # The start ran directly on the batch node, not in an srun step: the srun
    # stub never saw the container script.  (The relay's --connect steps are
    # created by the login-side relay process, not by this batch script.)
    slurm_log = srun_log.read_text(encoding="utf-8") if srun_log.exists() else ""
    assert "computemcp-container.sh" not in slurm_log, (slurm_log, result.stderr)
    # start exited non-zero with the actionable message, naming both
    # architectures, and the job script has no apptainer call log at all
    # (the arch gate fires before the first apptainer instance call), and the
    # ``instance start`` argv was never recorded.
    assert result.returncode != 0, (result.returncode, result.stdout, result.stderr)
    assert "does not match" in result.stderr, result.stderr
    assert "aarch64" in result.stderr and "x86_64" in result.stderr, result.stderr
    assert "instance start" not in calls, (
        f"instance start was reached despite the arch gate:\n{calls}\n"
        f"{result.stdout}\n{result.stderr}"
    )


# --- COMPUTEMCP_IMAGE normalization ----------------------------------------
#
# A doubled ``docker://`` prefix (the wizard default retyped) made Apptainer
# treat ``docker://`` as the source transport host and fail with an opaque
# "lookup docker: no such host".  The helper now collapses repeated prefixes
# and, for Apptainer, accepts a bare registry reference.


def _run_apptainer_build(
    tmp_path: Path, image: str
) -> tuple[subprocess.CompletedProcess, Path]:
    """Run the real ``apptainer_build`` with an argv/def-capturing stub.

    The stub ``apptainer`` records its argv and copies the definition file
    (the last argument of ``apptainer build``) next to its log, so a test can
    assert on the generated ``From:`` line without Apptainer installed.
    """
    root = tmp_path / "appbuild"
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    def_capture = root / "definition"
    calls = root / "apptainer.log"
    stub = bin_dir / "apptainer"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        f"printf 'apptainer %s\\n' \"$*\" >> '{calls}'\n"
        "# The definition path is the final argv element of ``apptainer build``.\n"
        "for LAST in \"$@\"; do :; done\n"
        f"[ -f \"$LAST\" ] && cp -- \"$LAST\" '{def_capture}'\n"
        "exit 0\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)
    sandbox = root / "sandbox"
    env = dict(os.environ)
    env.update(
        {
            "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
            "COMPUTEMCP_SYSTEM": "imgtest",
            "COMPUTEMCP_STORAGE_ROOT": str(root / "storage"),
            "COMPUTEMCP_SANDBOX_DIR": str(sandbox),
            "COMPUTEMCP_HOST_HOME": str(root / "hosthome"),
            "COMPUTEMCP_CONTAINER_PORT": str(_free_port()),
            "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
            "COMPUTEMCP_IMAGE": image,
        }
    )
    env["PATH"] = str(bin_dir) + os.pathsep + env["PATH"]
    result = subprocess.run(
        ["bash", str(CONTAINER), "build"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=root,
    )
    return result, def_capture


def _definition_from(def_capture: Path) -> str:
    assert def_capture.is_file(), "the apptainer build did not receive a definition"
    return def_capture.read_text(encoding="utf-8")


def test_apptainer_build_collapses_doubled_docker_prefix(tmp_path):
    """A doubled ``docker://`` must not reach the definition's ``From:`` line."""
    result, def_capture = _run_apptainer_build(
        tmp_path, "docker://docker://nvcr.io/nvidia/cuda:13.4.2-devel-ubuntu24.04"
    )
    definition = _definition_from(def_capture)
    assert (
        "From: nvcr.io/nvidia/cuda:13.4.2-devel-ubuntu24.04\n" in definition
    ), definition
    assert "From: docker://" not in definition, definition
    assert "Bootstrap: docker\n" in definition, definition


def test_apptainer_build_prefixes_bare_registry_reference(tmp_path):
    """A bare registry reference is accepted and prefixed for Apptainer."""
    result, def_capture = _run_apptainer_build(
        tmp_path, "nvcr.io/nvidia/cuda:13.4.2-devel-ubuntu24.04"
    )
    definition = _definition_from(def_capture)
    assert (
        "From: nvcr.io/nvidia/cuda:13.4.2-devel-ubuntu24.04\n" in definition
    ), definition
    assert "From: docker://" not in definition, definition


def test_apptainer_build_leaves_single_docker_prefix_unchanged(tmp_path):
    """A normal single ``docker://`` ref is unchanged."""
    result, def_capture = _run_apptainer_build(tmp_path, "docker://ubuntu:24.04")
    definition = _definition_from(def_capture)
    assert "From: ubuntu:24.04\n" in definition, definition
    assert "From: docker://" not in definition, definition


def test_apptainer_build_leaves_unsupported_scheme_untouched(tmp_path):
    """Another scheme is not rewritten; Apptainer/Aptainer gets it verbatim."""
    result, _ = _run_apptainer_build(tmp_path, "oras://ghcr.io/org/img:tag")
    assert result.returncode == 2, (result.returncode, result.stderr)
    assert "must use docker://" in result.stderr, result.stderr


def test_docker_build_collapses_doubled_docker_prefix(tmp_path, stubs):
    """A doubled ``docker://`` must not leak ``docker://`` into ``FROM``."""
    container_port = _free_port()
    workdir = tmp_path / "dockerfile"
    workdir.mkdir()
    storage = workdir / "storage"
    dockerfile_log = workdir / "Dockerfile.log"

    env = _base_env("dockerfiletest", storage, container_port)
    env["COMPUTEMCP_IMAGE"] = "docker://docker://ubuntu:24.04"
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(_published_port(container_port))
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["DOCKER_STUB_DOCKERFILE_LOG"] = str(dockerfile_log)
    env["PATH"] = stubs.direct.shell_path

    try:
        result = subprocess.run(
            ["bash", str(PROVISION), "provision"],
            capture_output=True,
            text=True,
            timeout=180,
            env=env,
            cwd=workdir,
        )
        assert result.returncode == 0, (result.returncode, result.stderr)
        assert dockerfile_log.is_file(), "the docker build did not capture the Dockerfile"
        dockerfile = dockerfile_log.read_text(encoding="utf-8")
        # FROM is parameterized by BASE_IMAGE; the resolved value must be the
        # collapsed ref, and no transport prefix may survive into the build.
        assert "ARG BASE_IMAGE=ubuntu:24.04" in dockerfile, dockerfile
        assert "docker://" not in dockerfile, dockerfile
    finally:
        _cleanup()


# --- Dynamic relay-port allocation -----------------------------------------
#
# A fixed login-node relay port collides when two bundle targets share one login
# node (the second relay fails with EADDRINUSE).  The helper now asks the relay
# to bind an ephemeral port and learns the concrete port from its ready file,
# persisting it to ``state/relay.port`` so a reconnect reuses it.


def _endpoint_port(stdout: str) -> int:
    """Parse the port from the helper's ``ENDPOINT 127.0.0.1:<port>`` line."""
    for line in stdout.splitlines():
        if line.strip().startswith("ENDPOINT"):
            host_port = line.split()[-1]
            return int(host_port.rsplit(":", 1)[1])
    raise AssertionError(f"no ENDPOINT line in stdout:\n{stdout}")


def _slurm_env(tmp_path, stubs, system, container_port, forward_port=None):
    """Build a gateway-style Slurm environment with its own work/storage tree."""
    workdir = tmp_path / system
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / system / "state" / "jobs" / "default"
    env = _base_env(system, storage, container_port)
    # The default is dynamic allocation; only pin the override when asked.
    env.pop("COMPUTEMCP_FORWARD_PORT", None)
    if forward_port is not None:
        env["COMPUTEMCP_FORWARD_PORT"] = str(forward_port)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["SLURM_JOB_OUT"] = str(workdir / "slurm-job.out")
    env["SLURM_JOBID_FILE"] = str(state_dir / "jobid")
    env["PATH"] = stubs.slurm.shell_path
    return workdir, storage, state_dir, env


def _run_provision(env, workdir, *args):
    return subprocess.run(
        ["bash", str(PROVISION), *args],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )


def test_slurm_distinct_state_dirs_get_distinct_relay_ports(tmp_path, stubs):
    """Two targets on one login node must not collide on a fixed relay port.

    Each provision has its own state directory but runs against the same stubbed
    host, so the relays would share a login node in production.  The dynamic
    allocation must yield different ports and both relays must actually listen.
    """
    container_a, container_b = _free_port(), _free_port()
    wd_a, _storage_a, _state_a, env_a = _slurm_env(tmp_path, stubs, "uniqa", container_a)
    wd_b, _storage_b, _state_b, env_b = _slurm_env(tmp_path, stubs, "uniqb", container_b)

    def teardown():
        for wd in (wd_a, wd_b):
            pid_file = Path(wd / "slurm-job.out.pid")
            if pid_file.exists():
                with contextlib.suppress(OSError, ValueError):
                    os.kill(int(pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        result_a = _run_provision(env_a, wd_a, "provision")
        result_b = _run_provision(env_b, wd_b, "provision")
        assert result_a.returncode == 0, (result_a.returncode, result_a.stderr)
        assert result_b.returncode == 0, (result_b.returncode, result_b.stderr)
        port_a = _endpoint_port(result_a.stdout)
        port_b = _endpoint_port(result_b.stdout)
        assert port_a != port_b, (result_a.stdout, result_b.stdout)
        assert _wait_port(port_a, timeout=15), port_a
        assert _wait_port(port_b, timeout=15), port_b
    finally:
        teardown()


def test_slurm_repeated_provision_reuses_recorded_relay_port(tmp_path, stubs):
    """A reconnect against the same state reuses the port and keeps the relay."""
    container_port = _free_port()
    workdir, _storage, state_dir, env = _slurm_env(
        tmp_path, stubs, "reuseport", container_port
    )
    job_pid_file = Path(env["SLURM_JOB_OUT"] + ".pid")

    def teardown():
        if job_pid_file.exists():
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(job_pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        first = _run_provision(env, workdir, "provision")
        assert first.returncode == 0, (first.returncode, first.stderr)
        port = _endpoint_port(first.stdout)
        relay_pid = (state_dir / "relay.pid").read_text().strip()
        recorded = (state_dir / "relay.port").read_text().strip()
        assert recorded == str(port), (recorded, port)

        second = _run_provision(env, workdir, "provision")
        assert second.returncode == 0, (second.returncode, second.stderr)
        assert _endpoint_port(second.stdout) == port
        assert (state_dir / "relay.port").read_text().strip() == str(port)
        # The relay was reused, not restarted: the pid is unchanged.
        assert (state_dir / "relay.pid").read_text().strip() == relay_pid
        assert _wait_port(port, timeout=15), port
    finally:
        teardown()


def test_slurm_explicit_forward_port_override_is_honored(tmp_path, stubs):
    """COMPUTEMCP_FORWARD_PORT is used verbatim and reported in ENDPOINT."""
    container_port = _free_port()
    forward_port = _free_port()
    workdir, _storage, state_dir, env = _slurm_env(
        tmp_path, stubs, "overrideport", container_port, forward_port=forward_port
    )
    job_pid_file = Path(env["SLURM_JOB_OUT"] + ".pid")

    def teardown():
        if job_pid_file.exists():
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(job_pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        result = _run_provision(env, workdir, "provision")
        assert result.returncode == 0, (result.returncode, result.stderr)
        assert _endpoint_port(result.stdout) == forward_port
        assert (state_dir / "relay.port").read_text().strip() == str(forward_port)
    finally:
        teardown()


def test_slurm_ready_file_carries_pid_and_port_never_zero(tmp_path, stubs):
    """The ready file holds ``<pid> <port>`` and ENDPOINT is never ``:0``."""
    container_port = _free_port()
    workdir, _storage, state_dir, env = _slurm_env(
        tmp_path, stubs, "readyformat", container_port
    )
    job_pid_file = Path(env["SLURM_JOB_OUT"] + ".pid")

    def teardown():
        if job_pid_file.exists():
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(job_pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        result = _run_provision(env, workdir, "provision")
        assert result.returncode == 0, (result.returncode, result.stderr)
        fields = (state_dir / "relay.ready").read_text().split()
        assert len(fields) == 2, fields
        relay_pid, bound_port = int(fields[0]), int(fields[1])
        assert bound_port >= 1
        assert relay_pid > 0
        port = _endpoint_port(result.stdout)
        assert port == bound_port, (port, bound_port)
        assert ":0" not in result.stdout, result.stdout
        # The readiness check must work against the new ready-file format.
        check = subprocess.run(
            ["python3", str(BUNDLE_DIR / "computemcp-relay.py"), "--check", str(port)],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert check.returncode == 0, (check.returncode, check.stderr)
    finally:
        teardown()


# --- Multi-job stage tests (fake Slurm) ------------------------------------
#
# These tests reuse tests/fakes/fake_slurm.py (the durable JSON queue shared by
# the multi-job design tests) and drive the REAL provision helper through its
# explicit submit/observe/attach/cancel/detach stages.  Everything is scoped to
# a per-test tmp tree; no global pkill or fixed PID names are used.


def _fake_records(fake_slurm_env) -> list[dict]:
    try:
        text = fake_slurm_env.log_path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return []
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def _job_dir(storage: Path, system: str, job_id: str) -> Path:
    return storage / system / "state" / "jobs" / job_id


def _fake_env(
    tmp_path: Path,
    stubs: _Stubs,
    fake_slurm_env,
    system: str,
    *,
    job_id: str = "default",
    extra: dict | None = None,
):
    """A gateway-style environment whose scheduler is the fake Slurm harness."""
    workdir = tmp_path / system
    workdir.mkdir()
    storage = workdir / "storage"
    env = _base_env(system, storage, _free_port())
    # Derive a per-job instance name; co-located jobs must not share one.
    env.pop("COMPUTEMCP_CONTAINER_NAME", None)
    env.update(fake_slurm_env.environment())
    env["PATH"] = str(fake_slurm_env.bin_dir) + os.pathsep + stubs.direct.shell_path
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["COMPUTEMCP_JOB_ID"] = job_id
    env.pop("COMPUTEMCP_FORWARD_PORT", None)
    if extra:
        env.update({str(key): str(value) for key, value in extra.items()})
    return workdir, storage, env


def _bridged_fake_bin(tmp_path: Path, stubs: _Stubs, fake_slurm_env) -> Path:
    """Fake Slurm commands plus a real srun -> container-port bridge."""
    bin_dir = tmp_path / "bridged-bin"
    bin_dir.mkdir()
    for name in ("sbatch", "squeue", "scancel", "sinfo", "slurmctl"):
        (bin_dir / name).symlink_to(fake_slurm_env.module_path)
    _write_stub(bin_dir / "srun", SRUN)
    shutil.copy2(stubs.slurm.bin / "docker", bin_dir / "docker")
    return bin_dir


def test_submit_returns_fast_and_observe_reports_each_state(
    tmp_path, stubs, fake_slurm_env
):
    """submit returns on sbatch acceptance; observe tracks PENDING->READY.

    ``submit`` must not wait in the queue: the fake job stays PENDING and the
    call still returns.  ``observe`` then reports pending, starting once the
    scheduler starts the job, ready after the job publishes its node, and
    terminal after the scheduler marks it complete.
    """
    workdir, storage, env = _fake_env(
        tmp_path, stubs, fake_slurm_env, "stageflow", job_id="stage"
    )
    jobdir = _job_dir(storage, "stageflow", "stage")

    started = time.monotonic()
    submit = _run_provision(env, workdir, "submit")
    elapsed = time.monotonic() - started
    assert submit.returncode == 0, (submit.returncode, submit.stderr)
    assert elapsed < 30, elapsed
    assert "JOB_ID stage" in submit.stdout, submit.stdout
    assert "SLURM_JOB_ID" in submit.stdout, submit.stdout
    assert "STATE pending" in submit.stdout, submit.stdout
    # Fast return: no relay was started and the numeric id is persisted.
    assert not (jobdir / "relay.ready").exists()
    slurm_id = (jobdir / "jobid").read_text().strip()

    def observe() -> subprocess.CompletedProcess:
        return _run_provision(env, workdir, "observe", "stage")

    pending = observe()
    assert pending.returncode == 0, pending.stderr
    assert "STATE pending" in pending.stdout, pending.stdout

    ctl = fake_slurm_env.run("slurmctl", "start-job", slurm_id)
    assert ctl.returncode == 0, ctl.stderr
    starting = observe()
    assert "STATE starting" in starting.stdout, starting.stdout

    # The batch job publishes its node and the concrete container port.
    (jobdir / "ready").write_text("fake-node\n", encoding="utf-8")
    (jobdir / "container.port").write_text("23456\n", encoding="utf-8")
    ready = observe()
    assert "STATE ready" in ready.stdout, ready.stdout
    assert "NODE fake-node" in ready.stdout, ready.stdout
    assert "CONTAINER_PORT 23456" in ready.stdout, ready.stdout

    assert fake_slurm_env.run(
        "slurmctl", "set-state", slurm_id, "COMPLETED"
    ).returncode == 0
    terminal = observe()
    assert "STATE terminal" in terminal.stdout, terminal.stdout


def test_attach_starts_relay_only_after_ready(
    tmp_path, stubs, fake_slurm_env, housekeeping
):
    """attach waits for readiness, then starts the job's own relay."""
    workdir, storage, env = _fake_env(
        tmp_path, stubs, fake_slurm_env, "attachflow", job_id="att"
    )
    env["PATH"] = (
        str(_bridged_fake_bin(tmp_path, stubs, fake_slurm_env))
        + os.pathsep
        + stubs.direct.shell_path
    )
    jobdir = _job_dir(storage, "attachflow", "att")
    container_port = _free_port()
    env["COMPUTEMCP_CONTAINER_PORT"] = str(container_port)

    submit = _run_provision(env, workdir, "submit")
    assert submit.returncode == 0, submit.stderr
    slurm_id = (jobdir / "jobid").read_text().strip()
    assert fake_slurm_env.run("slurmctl", "start-job", slurm_id).returncode == 0
    (jobdir / "ready").write_text("fake-node\n", encoding="utf-8")
    (jobdir / "container.port").write_text(
        str(container_port) + "\n", encoding="utf-8"
    )
    housekeeping.start_banner(container_port)
    assert _wait_port(container_port, timeout=10)

    attach = _run_provision(env, workdir, "attach", "att")
    assert attach.returncode == 0, (attach.returncode, attach.stderr)
    endpoint_line = next(
        line for line in attach.stdout.splitlines() if line.strip().startswith("ENDPOINT")
    )
    port = int(endpoint_line.split()[1].rsplit(":", 1)[1])
    assert _wait_port(port, timeout=15)
    assert (jobdir / "relay.ready").exists()
    relay_pid = int((jobdir / "relay.ready").read_text().split()[0])

    try:
        detach = _run_provision(env, workdir, "detach", "att")
        assert detach.returncode == 0, detach.stderr
        # detach removes the relay files but leaves the job record intact.
        assert not (jobdir / "relay.ready").exists()
        assert (jobdir / "jobid").exists()
    finally:
        with contextlib.suppress(OSError, ValueError):
            os.kill(relay_pid, 15)


def test_detach_and_teardown_never_cancel_and_cancel_is_explicit(
    tmp_path, stubs, fake_slurm_env
):
    """Shutdown-style detach/teardown emit zero scancel; cancel emits one."""
    workdir, storage, env = _fake_env(
        tmp_path, stubs, fake_slurm_env, "cancelrule", job_id="creq"
    )
    jobdir = _job_dir(storage, "cancelrule", "creq")
    submit = _run_provision(env, workdir, "submit")
    assert submit.returncode == 0, submit.stderr
    slurm_id = (jobdir / "jobid").read_text().strip()

    assert _run_provision(env, workdir, "detach", "creq").returncode == 0
    assert _run_provision(env, workdir, "teardown", "creq").returncode == 0
    scancel_before = [r for r in _fake_records(fake_slurm_env) if r["command"] == "scancel"]
    assert scancel_before == []

    cancel = _run_provision(env, workdir, "cancel", "creq")
    assert cancel.returncode == 0, cancel.stderr
    scancel_after = [r for r in _fake_records(fake_slurm_env) if r["command"] == "scancel"]
    assert len(scancel_after) == 1, scancel_after
    assert scancel_after[0].get("cancelled") == [int(slurm_id)]
    # Repeated cancel is safe and does not scancel a terminal job again.
    assert _run_provision(env, workdir, "cancel", "creq").returncode == 0


def _install_sacct_stub(
    fake_slurm_env,
    env: dict,
    *,
    job_id: str | None = None,
    state: str = "",
    comment: str = "",
) -> None:
    """Put a deterministic ``sacct`` on the fake cluster PATH.

    The shipped code only shells out to ``sacct`` as a fallback, so the fake
    Slurm harness does not model it.  With ``job_id=None`` the stub prints
    nothing (a purged job); otherwise it prints one allocation row
    ``JobID|State|Comment`` in the format the helper requests.
    """
    stub = fake_slurm_env.bin_dir / "sacct"
    _write_stub(
        stub,
        "#!/usr/bin/env bash\n"
        'if [ -n "${FAKE_SACCT_JOB_ID:-}" ]; then\n'
        "  printf '%s|%s|%s\\n' "
        '"$FAKE_SACCT_JOB_ID" "$FAKE_SACCT_STATE" "$FAKE_SACCT_COMMENT"\n'
        "fi\n"
        "exit 0\n",
    )
    if job_id is not None:
        env["FAKE_SACCT_JOB_ID"] = str(job_id)
        env["FAKE_SACCT_STATE"] = state
        env["FAKE_SACCT_COMMENT"] = comment
    else:
        env.pop("FAKE_SACCT_JOB_ID", None)


def test_submit_emits_correlation_comment_and_recovers_lost_id(
    tmp_path, stubs, fake_slurm_env
):
    """submit persists and emits a --comment token; observe recovers a lost id.

    The audit's crash window is between sbatch acceptance and
    ``state/jobid`` persistence.  The token is written before sbatch, so
    removing the id file (the crash) still lets observe find the job through
    the scheduler comment.
    """
    workdir, storage, env = _fake_env(
        tmp_path, stubs, fake_slurm_env, "corrflow", job_id="corr"
    )
    jobdir = _job_dir(storage, "corrflow", "corr")

    submit = _run_provision(env, workdir, "submit", "corr")
    assert submit.returncode == 0, submit.stderr
    token = (jobdir / "correlation_token").read_text().strip()
    assert re.fullmatch(r"[A-Za-z0-9._:-]+", token), token
    assert f"CORRELATION_TOKEN {token}" in submit.stdout, submit.stdout
    slurm_id = (jobdir / "jobid").read_text().strip()

    sbatch_records = [
        record for record in _fake_records(fake_slurm_env)
        if record["command"] == "sbatch"
    ]
    assert len(sbatch_records) == 1, sbatch_records
    assert sbatch_records[0]["comment"] == token, sbatch_records[0]
    assert f"--comment={token}" in sbatch_records[0]["argv"], sbatch_records[0]

    assert fake_slurm_env.run("slurmctl", "start-job", slurm_id).returncode == 0
    # Simulate the crash: the numeric id was never persisted.
    (jobdir / "jobid").unlink()
    _install_sacct_stub(
        fake_slurm_env, env, job_id=slurm_id, state="RUNNING", comment=token
    )

    recovered = _run_provision(env, workdir, "observe", "corr")
    assert recovered.returncode == 0, recovered.stderr
    assert f"SLURM_JOB_ID {slurm_id}" in recovered.stdout, recovered.stdout
    assert "STATE starting" in recovered.stdout, recovered.stdout
    # Recovery must re-persist the id for the next call.
    assert (jobdir / "jobid").read_text().strip() == slurm_id


def test_attach_recovers_lost_id_then_reports_terminal(
    tmp_path, stubs, fake_slurm_env
):
    """attach also recovers by token and never starts a relay for a dead job."""
    workdir, storage, env = _fake_env(
        tmp_path, stubs, fake_slurm_env, "attachrec", job_id="arec"
    )
    jobdir = _job_dir(storage, "attachrec", "arec")
    submit = _run_provision(env, workdir, "submit", "arec")
    assert submit.returncode == 0, submit.stderr
    slurm_id = (jobdir / "jobid").read_text().strip()
    token = (jobdir / "correlation_token").read_text().strip()
    assert fake_slurm_env.run(
        "slurmctl", "set-state", slurm_id, "COMPLETED"
    ).returncode == 0
    (jobdir / "jobid").unlink()
    _install_sacct_stub(
        fake_slurm_env, env, job_id=slurm_id, state="COMPLETED", comment=token
    )

    attach = _run_provision(env, workdir, "attach", "arec")
    assert attach.returncode != 0, attach.stdout
    assert "terminal" in attach.stderr.lower(), attach.stderr
    assert (jobdir / "jobid").read_text().strip() == slurm_id
    assert not (jobdir / "relay.ready").exists()
    assert not (jobdir / "relay.pid").exists()


def test_observe_maps_every_terminal_state_and_purged_id(
    tmp_path, stubs, fake_slurm_env
):
    """Every real terminal state, plus a purged id, reports terminal.

    A terminal mapping that misses a state (PREEMPTED, a timeout) leaves the
    gateway quota slot stuck, so all of them are asserted explicitly.
    """
    workdir, storage, env = _fake_env(
        tmp_path, stubs, fake_slurm_env, "termflow", job_id="term"
    )
    jobdir = _job_dir(storage, "termflow", "term")
    submit = _run_provision(env, workdir, "submit", "term")
    assert submit.returncode == 0, submit.stderr
    slurm_id = (jobdir / "jobid").read_text().strip()

    for native in ("COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "NODE_FAIL"):
        assert fake_slurm_env.run(
            "slurmctl", "set-state", slurm_id, native
        ).returncode == 0, native
        observed = _run_provision(env, workdir, "observe", "term")
        assert observed.returncode == 0, (native, observed.stderr)
        assert "STATE terminal" in observed.stdout, (native, observed.stdout)

    # PREEMPTED is a real Slurm state the fake controller does not model, so
    # inject it into the queue directly.
    queue = json.loads(fake_slurm_env.queue_path.read_text(encoding="utf-8"))
    queue["jobs"][slurm_id]["state"] = "PREEMPTED"
    fake_slurm_env.queue_path.write_text(json.dumps(queue), encoding="utf-8")
    observed = _run_provision(env, workdir, "observe", "term")
    assert "STATE terminal" in observed.stdout, observed.stdout

    # Purged: neither squeue nor sacct knows the id any more.
    queue = json.loads(fake_slurm_env.queue_path.read_text(encoding="utf-8"))
    del queue["jobs"][slurm_id]
    fake_slurm_env.queue_path.write_text(json.dumps(queue), encoding="utf-8")
    _install_sacct_stub(fake_slurm_env, env)
    observed = _run_provision(env, workdir, "observe", "term")
    assert observed.returncode == 0, observed.stderr
    assert "STATE terminal" in observed.stdout, observed.stdout


def _apptainer_stub(bin_dir: Path, log: Path, banner: Path) -> None:
    _write_stub(
        bin_dir / "apptainer",
        r'''#!/usr/bin/env bash
set -euo pipefail
printf 'apptainer %s\n' "$*" >> "$APP_LOG"
if [ "$1" = instance ] && [ "$2" = start ]; then
    PORT="$(cat "$COMPUTEMCP_STATE_DIR/runtime/container.port")"
    nohup python3 "$T8_BANNER" "$PORT" \
        >"$COMPUTEMCP_STATE_DIR/runtime/banner.out" 2>&1 </dev/null 9>&- &
    echo "$!" > "$COMPUTEMCP_STATE_DIR/runtime/banner.pid"
fi
if [ "$1" = instance ] && [ "$2" = stop ]; then
    if [ -f "$COMPUTEMCP_STATE_DIR/runtime/banner.pid" ]; then
        kill "$(cat "$COMPUTEMCP_STATE_DIR/runtime/banner.pid")" 2>/dev/null || true
    fi
fi
exit 0
''',
    )
    _write_stub(bin_dir / "ss", "#!/bin/sh\nexit 0\n")
    (bin_dir / "apptainer.log.target").write_text(str(log), encoding="utf-8")


def _apptainer_base_env(tmp_path: Path, stubs: _Stubs, *, system: str) -> dict:
    root = tmp_path / "apptainer"
    sandbox = _make_fake_sandbox(root)
    configured, sandbox, host_home = _run_apptainer_configure(
        tmp_path, "ubuntu", _free_port(), sandbox=sandbox
    )
    assert configured.returncode == 0, configured.stderr
    bin_dir = tmp_path / "appbin"
    bin_dir.mkdir()
    log = tmp_path / "apptainer.log"
    _apptainer_stub(bin_dir, log, stubs_banner_path())
    env = dict(os.environ)
    env.update(
        {
            "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
            "COMPUTEMCP_SYSTEM": system,
            "COMPUTEMCP_STORAGE_ROOT": str(tmp_path / "storage"),
            "COMPUTEMCP_SANDBOX_DIR": str(sandbox),
            "COMPUTEMCP_HOST_HOME": str(host_home),
            "COMPUTEMCP_CONTAINER_PORT": "0",
            "COMPUTEMCP_SSH_WAIT_SECONDS": "10",
            "COMPUTEMCP_SSH_USER": "ubuntu",
            "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
            "COMPUTEMCP_ROOT_MODE": "fakeroot",
            "APP_LOG": str(log),
            "T8_BANNER": str(stubs_banner_path()),
            "PATH": str(bin_dir) + os.pathsep + os.environ.get("PATH", ""),
        }
    )
    return env, sandbox, host_home, log


def test_two_colocated_jobs_get_distinct_instance_ports_and_runtime_files(
    tmp_path, stubs
):
    """Same node: distinct instance names, ports and runtime files."""
    env, sandbox, host_home, log = _apptainer_base_env(
        tmp_path, stubs, system="coloc"
    )
    state_root = tmp_path / "storage" / "coloc" / "state"
    ports = {}
    banners = []
    for job_id in ("a", "b"):
        job_env = dict(env)
        job_env["COMPUTEMCP_JOB_ID"] = job_id
        job_state = state_root / "jobs" / job_id
        job_env["COMPUTEMCP_STATE_DIR"] = str(job_state)
        result = subprocess.run(
            ["bash", str(CONTAINER), "start"],
            capture_output=True,
            text=True,
            timeout=60,
            env=job_env,
            cwd=tmp_path,
        )
        assert result.returncode == 0, (job_id, result.returncode, result.stderr)
        ports[job_id] = int((job_state / "container.port").read_text().strip())
        runtime = job_state / "runtime"
        assert (runtime / "container.port").exists()
        pid_file = runtime / "banner.pid"
        if pid_file.exists():
            banners.append(int(pid_file.read_text().strip()))
    try:
        assert ports["a"] != ports["b"], ports
        text = log.read_text(encoding="utf-8")
        assert any(name.endswith("-coloc-a") for name in text.split()), text
        assert any(name.endswith("-coloc-b") for name in text.split()), text
        # The runtime directories are per job, not shared.
        assert (state_root / "jobs" / "a" / "runtime").is_dir()
        assert (state_root / "jobs" / "b" / "runtime").is_dir()
    finally:
        for pid in banners:
            with contextlib.suppress(OSError, ValueError):
                os.kill(pid, 15)


def test_fakeroot_and_clean_root_modes_differ_and_record_digest(tmp_path, stubs):
    """fakeroot uses the writable sandbox; clean uses a SIF + digest."""
    env, sandbox, host_home, log = _apptainer_base_env(
        tmp_path, stubs, system="rootmode"
    )
    state_root = tmp_path / "storage" / "rootmode" / "state"
    banners = []

    def start(job_id: str, extra: dict) -> Path:
        job_state = state_root / "jobs" / job_id
        job_env = dict(env)
        job_env.update({k: str(v) for k, v in extra.items()})
        job_env["COMPUTEMCP_JOB_ID"] = job_id
        job_env["COMPUTEMCP_STATE_DIR"] = str(job_state)
        result = subprocess.run(
            ["bash", str(CONTAINER), "start"],
            capture_output=True,
            text=True,
            timeout=60,
            env=job_env,
            cwd=tmp_path,
        )
        assert result.returncode == 0, (job_id, result.returncode, result.stderr)
        pid_file = job_state / "runtime" / "banner.pid"
        if pid_file.exists():
            banners.append(int(pid_file.read_text().strip()))
        return job_state

    try:
        fake_state = start("fake", {"COMPUTEMCP_ROOT_MODE": "fakeroot"})
        fake_line = [
            line for line in log.read_text(encoding="utf-8").splitlines()
            if "instance start" in line
        ][-1]
        assert "--fakeroot" in fake_line and "--writable" in fake_line, fake_line
        assert str(sandbox) in fake_line, fake_line

        image = tmp_path / "clean-base.sif"
        image.write_bytes(b"immutable clean base\n")
        clean_state = start(
            "clean",
            {
                "COMPUTEMCP_ROOT_MODE": "clean",
                "COMPUTEMCP_CLEAN_IMAGE": str(image),
            },
        )
        clean_line = [
            line for line in log.read_text(encoding="utf-8").splitlines()
            if "instance start" in line
        ][-1]
        assert "--writable-tmpfs" in clean_line, clean_line
        assert " --writable " not in clean_line, clean_line
        assert str(image) in clean_line, clean_line
        digest = (clean_state / "image.digest").read_text().strip()
        assert digest == hashlib.sha256(image.read_bytes()).hexdigest()
    finally:
        for pid in banners:
            with contextlib.suppress(OSError, ValueError):
                os.kill(pid, 15)


def test_home_selection_shared_private_and_clean_home(tmp_path, stubs, fake_slurm_env):
    """Homes: shared, per-client private, and clean_home beneath the private one."""
    home_base = tmp_path / "homes"
    shared = _fake_env(
        tmp_path,
        stubs,
        fake_slurm_env,
        "homeshared",
        job_id="s",
        extra={"COMPUTEMCP_HOME_BASE": str(home_base), "COMPUTEMCP_JOB_KIND": "shared"},
    )
    wd, storage, env = shared
    r = _run_provision(env, wd, "submit", "s")
    assert r.returncode == 0, r.stderr
    shared_home = (_job_dir(storage, "homeshared", "s") / "home").read_text().strip()
    assert shared_home == str((home_base / "shared").resolve())

    def private(job_id: str, clean_home: str):
        wd, storage, env = _fake_env(
            tmp_path,
            stubs,
            fake_slurm_env,
            f"home{job_id}",
            job_id=job_id,
            extra={
                "COMPUTEMCP_HOME_BASE": str(home_base),
                "COMPUTEMCP_JOB_KIND": "private",
                "COMPUTEMCP_CLIENT_ID": "agent-x",
                "COMPUTEMCP_CLEAN_HOME": clean_home,
            },
        )
        result = _run_provision(env, wd, "submit", job_id)
        assert result.returncode == 0, (job_id, result.returncode, result.stderr)
        return (
            _job_dir(storage, f"home{job_id}", job_id) / "home"
        ).read_text().strip()

    absolute = private("abs", "/benchmarks/run-42")
    relative = private("rel", "benchmarks/run-42")
    expected = str((home_base / "agents" / "agent-x" / "benchmarks" / "run-42").resolve())
    assert absolute == expected, absolute
    assert relative == expected, relative


def test_home_rejects_traversal_and_symlink_escape(tmp_path, stubs, fake_slurm_env):
    home_base = tmp_path / "homes"
    counter = {"n": 0}

    def run(clean_home: str):
        counter["n"] += 1
        wd, storage, env = _fake_env(
            tmp_path,
            stubs,
            fake_slurm_env,
            f"homebad{counter['n']}",
            job_id="bad",
            extra={
                "COMPUTEMCP_HOME_BASE": str(home_base),
                "COMPUTEMCP_JOB_KIND": "private",
                "COMPUTEMCP_CLIENT_ID": "agent-x",
                "COMPUTEMCP_CLEAN_HOME": clean_home,
            },
        )
        return _run_provision(env, wd, "submit", "bad")

    traversal = run("../escape")
    assert traversal.returncode == 2, (traversal.returncode, traversal.stderr)
    assert "'..'" in traversal.stderr or ".." in traversal.stderr, traversal.stderr

    outside = tmp_path / "outside"
    outside.mkdir()
    (home_base / "agents" / "agent-x").mkdir(parents=True)
    (home_base / "agents" / "agent-x" / "link").symlink_to(
        outside, target_is_directory=True
    )
    symlink = run("link")
    assert symlink.returncode == 2, (symlink.returncode, symlink.stderr)
    assert "symlink" in symlink.stderr, symlink.stderr

    # Rejections happen before any submission or container work.
    assert not [r for r in _fake_records(fake_slurm_env) if r["command"] == "sbatch"]


def test_submit_applies_provision_env_bootstrap(tmp_path, stubs, fake_slurm_env):
    """The site hook runs on the login node and is carried into the settings."""
    marker = tmp_path / "login-hook-marker"
    wd, storage, env = _fake_env(
        tmp_path,
        stubs,
        fake_slurm_env,
        "envboot",
        job_id="env",
        extra={"COMPUTEMCP_PROVISION_ENV": f"touch {marker}"},
    )
    result = _run_provision(env, wd, "submit", "env")
    assert result.returncode == 0, result.stderr
    assert marker.exists()
    settings = _job_dir(storage, "envboot", "env") / "srun-envboot.settings"
    assert settings.exists()
    assert "COMPUTEMCP_PROVISION_ENV" in settings.read_text()


# --- Portability and prerequisite regressions ------------------------------
#
# R1/R2 review findings on the bundle: Python 3.8 compatibility of the embedded
# helpers, the missing Apptainer account on bare base images, the Apptainer
# prerequisite probe, the Slurm+Docker published-port bug, stale re-submit
# state, the digest pin, and the relay argv guard.


def test_embedded_container_python_is_python38_compatible():
    """The embedded Python helpers must parse with the 3.8 grammar.

    ``computemcp-container.sh`` embeds Python that runs on the node's
    ``python3``.  A 3.9-only call (``Path.is_relative_to``) previously slipped
    in, so every block is parsed with ``feature_version=(3, 8)``.
    """
    blocks = _embedded_python_blocks(CONTAINER)
    tags = {tag for tag, _ in blocks}
    assert {"PY", "CONFIGURE", "PYPORT", "PYDIGEST"} <= tags, tags
    for tag, code in blocks:
        ast.parse(
            code,
            filename=f"{CONTAINER.name}:{tag}",
            feature_version=(3, 8),
        )


def test_relay_cli_rejects_missing_arguments():
    """No argv or a partial mode must print usage, never an IndexError."""
    relay = BUNDLE_DIR / "computemcp-relay.py"
    noargs = subprocess.run(
        [sys.executable, str(relay)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert noargs.returncode != 0, noargs.stdout
    assert "usage:" in (noargs.stdout + noargs.stderr).lower(), noargs.stderr
    assert "Traceback" not in noargs.stderr, noargs.stderr

    partial = subprocess.run(
        [sys.executable, str(relay), "--check"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert partial.returncode != 0, partial.stdout
    assert "usage:" in (partial.stdout + partial.stderr).lower(), partial.stderr
    assert "Traceback" not in partial.stderr, partial.stderr


def _apptainer_probe_env(tmp_path: Path, version: str) -> dict:
    root = tmp_path / "appprobe"
    bin_dir = root / "bin"
    bin_dir.mkdir(parents=True)
    stub = bin_dir / "apptainer"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        f"printf 'apptainer version {version}\\n'\n",
        encoding="utf-8",
    )
    stub.chmod(0o755)
    env = dict(os.environ)
    env.update(
        {
            "COMPUTEMCP_CONTAINER_RUNTIME": "apptainer",
            "COMPUTEMCP_SYSTEM": "probe",
            "COMPUTEMCP_STORAGE_ROOT": str(root / "storage"),
            "COMPUTEMCP_SANDBOX_DIR": str(root / "sandbox"),
            "COMPUTEMCP_HOST_HOME": str(root / "home"),
            "COMPUTEMCP_CONTAINER_PORT": str(_free_port()),
        }
    )
    env["PATH"] = str(bin_dir) + os.pathsep + os.environ.get("PATH", "")
    return env


def test_apptainer_prerequisites_reject_legacy_version(tmp_path):
    """A legacy Singularity (major 0) fails with an actionable message."""
    env = _apptainer_probe_env(tmp_path, "0.9.9")
    result = subprocess.run(
        ["bash", str(CONTAINER), "configure"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=tmp_path,
    )
    assert result.returncode != 0, (result.returncode, result.stderr)
    assert "1.x or newer is required" in result.stderr, result.stderr
    assert "Singularity" in result.stderr, result.stderr


def test_apptainer_prerequisites_accept_modern_version(tmp_path):
    """Apptainer 1.x passes the probe and reaches the normal configure path."""
    env = _apptainer_probe_env(tmp_path, "1.3.3")
    result = subprocess.run(
        ["bash", str(CONTAINER), "configure"],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=tmp_path,
    )
    # No sandbox exists, so configure stops there; the prerequisite check did
    # not reject the version.
    assert "1.x or newer is required" not in result.stderr, result.stderr
    assert "Missing sandbox" in result.stderr, result.stderr


def test_apptainer_configure_creates_missing_account(tmp_path):
    """A base image with only ``root`` gets a non-root account for the user.

    Some images (the NVIDIA CUDA bases) ship no ``ubuntu`` account.  Configure
    must create one with a free UID/GID instead of aborting.
    """
    root = tmp_path / "apptainer"
    sandbox = _make_fake_sandbox(root)
    (sandbox / "etc/passwd").write_text("root:x:0:0:root:/root:/bin/bash\n")
    (sandbox / "etc/shadow").write_text("root:*:19000:0:99999:7:::\n")
    result, sandbox, host_home = _run_apptainer_configure(
        tmp_path, "agent", _free_port(), sandbox=sandbox
    )
    assert result.returncode == 0, (result.stdout, result.stderr)
    accounts = _passwd_account(sandbox)
    assert "agent" in accounts, accounts
    line = accounts["agent"]
    assert int(line[2]) >= 1000, line
    assert line[2] == line[3], line
    assert line[5] == "/home/agent", line
    assert line[6] == "/usr/local/bin/computemcp-agent-shell", line

    groups = {}
    for group_line in (sandbox / "etc/group").read_text().splitlines():
        fields = group_line.split(":")
        groups[fields[0]] = fields
    assert "agent" in groups, groups
    assert groups["agent"][2] == line[3], groups
    shadow = (sandbox / "etc/shadow").read_text()
    assert any(row.startswith("agent:") for row in shadow.splitlines()), shadow
    assert (host_home / ".ssh/authorized_keys").is_file()


def test_clean_image_digest_pin_mismatch_fails(tmp_path, stubs):
    """An operator-pinned digest aborts on a different clean base."""
    env, _sandbox, _host_home, _log = _apptainer_base_env(
        tmp_path, stubs, system="digest"
    )
    image = tmp_path / "clean-base.sif"
    image.write_bytes(b"immutable clean base\n")
    digest = hashlib.sha256(image.read_bytes()).hexdigest()
    job_state = tmp_path / "storage" / "digest" / "state" / "jobs" / "pin"
    job_env = dict(env)
    job_env.update(
        {
            "COMPUTEMCP_ROOT_MODE": "clean",
            "COMPUTEMCP_CLEAN_IMAGE": str(image),
            "COMPUTEMCP_STATE_DIR": str(job_state),
            "COMPUTEMCP_JOB_ID": "pin",
        }
    )

    wrong = dict(job_env)
    wrong["COMPUTEMCP_IMAGE_DIGEST"] = "0" * 64
    rejected = subprocess.run(
        ["bash", str(CONTAINER), "configure"],
        capture_output=True,
        text=True,
        timeout=60,
        env=wrong,
        cwd=tmp_path,
    )
    assert rejected.returncode != 0, (rejected.returncode, rejected.stderr)
    assert "digest mismatch" in rejected.stderr, rejected.stderr

    pinned = dict(job_env)
    pinned["COMPUTEMCP_IMAGE_DIGEST"] = "sha256:" + digest
    accepted = subprocess.run(
        ["bash", str(CONTAINER), "configure"],
        capture_output=True,
        text=True,
        timeout=60,
        env=pinned,
        cwd=tmp_path,
    )
    assert accepted.returncode == 0, (accepted.returncode, accepted.stderr)
    assert (job_state / "image.digest").read_text().strip() == digest


def test_submit_clears_stale_runtime_state(tmp_path, stubs, fake_slurm_env):
    """A re-submit drops the previous allocation's readiness files."""
    workdir, storage, env = _fake_env(
        tmp_path, stubs, fake_slurm_env, "staleflow", job_id="stale"
    )
    jobdir = _job_dir(storage, "staleflow", "stale")
    jobdir.mkdir(parents=True, exist_ok=True)
    stale = {
        "ready": "dead-node\n",
        "container.port": "23456\n",
        "container.endpoint": "127.0.0.1:23456\n",
        "cancelled": "cancelled\n",
        "ready.ABC123": "dead-node\n",
    }
    for name, value in stale.items():
        (jobdir / name).write_text(value, encoding="utf-8")

    result = _run_provision(env, workdir, "submit", "stale")
    assert result.returncode == 0, (result.returncode, result.stderr)
    for name in stale:
        assert not (jobdir / name).exists(), name


def test_observe_and_attach_agree_on_non_terminal_states(
    tmp_path, stubs, fake_slurm_env
):
    """SUSPENDED/COMPLETING/STOPPED are non-terminal in observe AND attach.

    Regression: observe mapped them to ``starting`` while attach aborted with
    "terminal", so the two stages disagreed about the same allocation.
    """
    workdir, storage, env = _fake_env(
        tmp_path, stubs, fake_slurm_env, "agreeflow", job_id="agree"
    )
    jobdir = _job_dir(storage, "agreeflow", "agree")
    submit = _run_provision(env, workdir, "submit", "agree")
    assert submit.returncode == 0, submit.stderr
    slurm_id = (jobdir / "jobid").read_text().strip()

    attach_env = dict(env)
    attach_env["COMPUTEMCP_WAIT_SECONDS"] = "1"
    for native in ("SUSPENDED", "COMPLETING", "STOPPED"):
        queue = json.loads(fake_slurm_env.queue_path.read_text(encoding="utf-8"))
        queue["jobs"][slurm_id]["state"] = native
        fake_slurm_env.queue_path.write_text(json.dumps(queue), encoding="utf-8")

        observed = _run_provision(env, workdir, "observe", "agree")
        assert observed.returncode == 0, observed.stderr
        assert "STATE starting" in observed.stdout, (native, observed.stdout)

        attached = _run_provision(attach_env, workdir, "attach", "agree")
        assert attached.returncode != 0, (native, attached.stderr)
        combined = (attached.stdout + attached.stderr).lower()
        assert "timed out" in combined, (native, combined)
        assert "is terminal" not in combined, (native, combined)


def test_slurm_docker_port_zero_uses_published_host_port(tmp_path, stubs):
    """Slurm+Docker: port 0 resolves to the concrete published host port.

    Restriction: the request is 0 ("ephemeral"), so the readiness check and the
    relay must dial the host port Docker published, never 0 and never the
    in-container 2222.
    """
    published = _free_port()
    forward = _free_port()
    workdir = tmp_path / "portzero"
    workdir.mkdir()
    storage = workdir / "storage"
    state_dir = storage / "portzero" / "state" / "jobs" / "default"

    env = _base_env("portzero", storage, 0)
    env["COMPUTEMCP_CONTAINER_PORT"] = "0"
    env["COMPUTEMCP_FORWARD_PORT"] = str(forward)
    env["DOCKER_STUB_PUBLISHED_PORT"] = str(published)
    env["SLURM_STUB_LOG"] = str(workdir / "slurm.log")
    env["DOCKER_STUB_LOG"] = str(workdir / "docker.log")
    env["DOCKER_STUB_STATE"] = str(workdir / "dockerstate")
    env["SLURM_JOB_OUT"] = str(workdir / "slurm-job.out")
    env["SLURM_JOBID_FILE"] = str(state_dir / "jobid")
    env["PATH"] = stubs.slurm.shell_path

    result = subprocess.run(
        ["bash", str(PROVISION), "provision"],
        capture_output=True,
        text=True,
        timeout=180,
        env=env,
        cwd=workdir,
    )
    job_pid_file = Path(env["SLURM_JOB_OUT"] + ".pid")

    def teardown():
        if job_pid_file.exists():
            with contextlib.suppress(OSError, ValueError):
                os.kill(int(job_pid_file.read_text().strip()), 15)
        _cleanup()

    try:
        assert result.returncode == 0, (
            f"provision exited {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        assert (state_dir / "container.port").read_text().strip() == str(
            published
        )
        endpoint_line = next(
            line for line in result.stdout.splitlines()
            if line.strip().startswith("ENDPOINT")
        )
        assert endpoint_line.strip() == f"ENDPOINT 127.0.0.1:{forward}"
        slurm_log = (workdir / "slurm.log").read_text()
        assert f"--connect {published}" in slurm_log, slurm_log
        assert "--connect 0" not in slurm_log, slurm_log
        assert "--connect 2222" not in slurm_log, slurm_log
    finally:
        teardown()
