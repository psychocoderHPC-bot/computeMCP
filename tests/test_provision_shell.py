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

import contextlib
import os
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
BUNDLE_DIR = REPO / "src" / "compute_mcp" / "bundles" / "computemcp-slurm"
PROVISION = BUNDLE_DIR / "computemcp-provision.sh"
JOB = BUNDLE_DIR / "computemcp-job.sh"
CONTAINER = BUNDLE_DIR / "computemcp-container.sh"

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


def _kill_process(pattern: str) -> None:
    """Best-effort pkill for a leaked helper process."""
    subprocess.run(["pkill", "-f", pattern], check=False, stdout=subprocess.DEVNULL)


def _pkill_pids(pattern: str) -> None:
    procs = subprocess.run(
        ["pgrep", "-f", pattern], capture_output=True, text=True, check=False
    ).stdout.split()
    for pid in procs:
        with contextlib.suppress(OSError, ValueError):
            subprocess.run(["kill", pid], check=False)


def _cleanup() -> None:
    """Stop the background helpers the tests may still be holding."""
    _kill_process("banner_server.py")
    _pkill_pids("computemcp-relay.py")
    _pkill_pids("computemcp-job.sh")
    _pkill_pids("connect_bridge.py")


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
        while [ "$#" -gt 0 ]; do
            case "$1" in
                --tag) TAG="$2"; shift 2 ;;
                --build-arg) shift 2 ;;
                *) shift ;;
            esac
        done
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
# fake job id and reports the id plus a test cluster name.
set -euo pipefail
LOG="${SLURM_STUB_LOG:-/dev/null}"
printf 'sbatch %s\\n' "$*" >> "$LOG"

ARGS=("$@")
N=${#ARGS[@]}
SETTINGS="${ARGS[N-1]}"
JOB_SCRIPT="${ARGS[N-2]}"
export SLURM_JOB_ID=4242
if [ -n "${SLURM_JOB_OUT:-}" ]; then
    nohup bash "$JOB_SCRIPT" "$SETTINGS" >"$SLURM_JOB_OUT" 2>&1 </dev/null 9>&- &
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
        self.banner = base / "banner_server.py"
        self.bridge = base / "connect_bridge.py"
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
    env = dict(os.environ)
    env.update(
        {
            "COMPUTEMCP_SYSTEM": system,
            "COMPUTEMCP_STORAGE_ROOT": str(storage_root),
            "COMPUTEMCP_CONTAINER_RUNTIME": "docker",
            "COMPUTEMCP_IMAGE": "docker://ubuntu:22.04",
            "COMPUTEMCP_CONTAINER_PORT": str(port),
            "COMPUTEMCP_FORWARD_PORT": str(port + 1),
            "COMPUTEMCP_SSH_WAIT_SECONDS": "15",
            "COMPUTEMCP_WAIT_SECONDS": "60",
            "COMPUTEMCP_SSH_PUBLIC_KEY": "ssh-ed25519 AAAATEST fixture@test",
            "COMPUTEMCP_NODES": "1",
            # Pin the runtime name so the existing assertions stay readable.
            # The helper defaults to ``computemcp-<uid>-<system>``; tests that
            # exercise the default derivation pop this key first.
            "COMPUTEMCP_CONTAINER_NAME": f"computemcp-{system}",
            # Both the direct ``docker start`` path (via the docker stub) and
            # the slurm ``srun --connect`` path (via the srun stub) need this.
            "BANNER_SERVER": str(stubs_banner_path()),
            "CONNECT_BRIDGE": str(stubs_banner_path().with_name("connect_bridge.py")),
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
    state_dir = storage / "casetest" / "state"

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
        # The relay must be listening on the reported forward port so that
        # the "provisioned endpoint" exposed to the gateway is real.
        assert _wait_port(forward_port, timeout=15), (
            f"relay not listening on {forward_port}\nstderr:\n{result.stderr}"
        )
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
    the job step starts.  This runs the REAL job.sh with a settings file
    whose two hook lines create a marker and append a count: both must be
    applied in the job shell while the stub ``srun`` starts the container
    (banner already running) and keeps the allocation alive until this
    test tears it down.
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
        # (e.g. srun stub exits fast) still check the marker.
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
        # The container-step log shows the stub srun launched after the hook
        # (slurm log is non-empty because the stub records the invocation).
        slurm_log = (workdir / "slurm.log").read_text(encoding="utf-8", errors="replace")
        assert "srun" in slurm_log
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
    state_dir_slurm = storage / "slurmhk" / "state"
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
        settings = (storage / "slurmhk" / "state") / "srun-slurmhk.settings"
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
    (``container_user = "agent"``) connected but could not exec (502).  The
    sandbox account is now renamed to the resolved login name (default
    ``agent``) and every downstream reference follows it.
    """
    expected = ssh_user or "agent"
    result, sandbox, host_home = _run_apptainer_configure(
        tmp_path, ssh_user, _free_port()
    )
    assert result.returncode == 0, (
        f"configure exited {result.returncode}\n"
        f"stdout:{result.stdout}\nstderr:{result.stderr}"
    )
    accounts = _passwd_account(sandbox)
    assert expected in accounts, accounts
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


def test_job_sh_relocated_resolves_bundle_dir_from_settings(tmp_path):
    """A scheduler-spooled job.sh finds the container script via settings.

    JURECA copies ``computemcp-job.sh`` into ``/var/spool/parastation/jobs/`` and
    runs it there, so ``BASH_SOURCE`` no longer points at the bundle.  The
    per-job settings file carries ``COMPUTEMCP_BUNDLE_DIR``; the relocated copy
    must use it and get past the sibling check (reaching the job step).
    """
    spool = tmp_path / "spool"
    spool.mkdir()
    relocated = spool / "computemcp-job.sh"
    shutil.copy2(JOB, relocated)
    state = tmp_path / "state"
    stub_bin = tmp_path / "bin"
    stub_bin.mkdir()
    srun = stub_bin / "srun"
    srun.write_text(
        "#!/bin/sh\necho STUB-SRUN-REACHED >&2\nexit 1\n", encoding="utf-8"
    )
    srun.chmod(0o755)
    settings = tmp_path / "settings"
    settings.write_text(
        f"export COMPUTEMCP_STATE_DIR={_shell_quote(str(state))}\n"
        f"export COMPUTEMCP_BUNDLE_DIR={_shell_quote(str(BUNDLE_DIR))}\n",
        encoding="utf-8",
    )
    env = dict(os.environ)
    env.pop("COMPUTEMCP_BUNDLE_DIR", None)
    env["SLURM_JOB_ID"] = "9001"
    env["PATH"] = str(stub_bin) + os.pathsep + os.environ.get("PATH", "")
    result = subprocess.run(
        ["bash", str(relocated), str(settings)],
        capture_output=True,
        text=True,
        timeout=60,
        env=env,
        cwd=spool,
    )
    assert "Missing container script" not in result.stderr, result.stderr
    # Reaching the stub srun proves the sibling resolution succeeded.
    assert "STUB-SRUN-REACHED" in result.stderr, result.stderr


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
