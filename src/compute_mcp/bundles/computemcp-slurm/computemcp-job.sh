#!/usr/bin/env bash
# computeMCP Slurm batch job.
#
# Runs inside the allocation on the compute node.  It starts the container via
# computemcp-container.sh (on the first node) and then keeps the allocation
# alive.  The container configuration comes from the per-job settings file whose
# path the provisioning helper passes as the first positional argument (with
# COMPUTEMCP_SRUN_SETTINGS_FILE as an environment fallback).  The settings file
# also carries COMPUTEMCP_SRUN_ARGS for the relay's ``--connect`` job steps; the
# relay reads them via COMPUTEMCP_CPU_BIND from the environment the login node
# exported, so this batch script does not parse them.
set -euo pipefail
umask 077

: "${SLURM_JOB_ID:?Must run inside a Slurm allocation}"

# The provisioning helper passes the per-job settings file path as the first
# positional argument, which Slurm delivers verbatim even with --export=NONE.
# Source it FIRST: it exports the whole container configuration (STATE, port,
# banner timeout, runtime, ...) in THIS batch shell, so a restrictive export
# policy that strips the gateway's COMPUTEMCP_* variables cannot leave the
# direct container start below with defaults.
SETTINGS="${1:-${COMPUTEMCP_SRUN_SETTINGS_FILE:-}}"
if [ -n "$SETTINGS" ] && [ -r "$SETTINGS" ]; then
    # shellcheck disable=SC1090
    source "$SETTINGS"
fi

STATE="${COMPUTEMCP_STATE_DIR:?COMPUTEMCP_STATE_DIR is required}"
CONTAINER_PORT="${COMPUTEMCP_CONTAINER_PORT:-2222}"
SSH_WAIT_SECONDS="${COMPUTEMCP_SSH_WAIT_SECONDS:-120}"

# Resolve the bundle directory.  Slurm may copy this batch script into its spool
# directory (e.g. JURECA /var/spool/parastation/jobs), so ``BASH_SOURCE`` points
# at the spool copy where the sibling helpers do not exist.  The provisioning
# helper exports COMPUTEMCP_BUNDLE_DIR (inherited from the environment or, more
# reliably, carried by the sourced settings file) and that is authoritative.
if [ -n "${COMPUTEMCP_BUNDLE_DIR:-}" ]; then
    SCRIPT_DIR="$COMPUTEMCP_BUNDLE_DIR"
else
    SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
fi
CONTAINER_SCRIPT="$SCRIPT_DIR/computemcp-container.sh"
[ -r "$CONTAINER_SCRIPT" ] || {
    echo "Missing container script: $CONTAINER_SCRIPT (resolved from COMPUTEMCP_BUNDLE_DIR='${COMPUTEMCP_BUNDLE_DIR:-}')" >&2
    exit 1
}

# Apply the provision hook again on the compute node: a login-node ``module
# load`` / ``source`` does not propagate into the allocation.  The settings file
# carries COMPUTEMCP_PROVISION_ENV (newline-joined, no trailing newline), and we
# run each line in THIS shell before the job step starts.  Absent/empty is a
# strict no-op; set -euo pipefail aborts on any failing line.
if [ -n "${COMPUTEMCP_PROVISION_ENV:-}" ]; then
    while IFS= read -r LINE; do
        [ -n "$LINE" ] || continue
        eval "$LINE"
    done <<< "$COMPUTEMCP_PROVISION_ENV"
fi

mkdir -p "$STATE"
exec 8>"$STATE/job-run.lock"
flock -n 8 || { echo 'Another job is already using this state directory.' >&2; exit 1; }

READY="$STATE/ready-$SLURM_JOB_ID"
STARTED=0
cleanup() {
    rm -f "$READY"
    if [ "$STARTED" = 1 ]; then
        bash "$CONTAINER_SCRIPT" stop || true
    fi
}
trap cleanup EXIT
trap 'exit 0' TERM INT
rm -f "$READY"

# Compute-node build.  When the gateway configures ``build-location = compute``
# (an architecture-mismatched partition, e.g. an ARM partition whose login nodes
# are x86-64), the login node skipped the build and this batch script -- which
# runs ON the first allocated node -- builds and configures the sandbox here, so
# the sandbox matches the compute architecture.  build_location = login (the
# default) is a strict no-op: the existing container is started below.  The
# settings file already exported COMPUTEMCP_SSH_PUBLIC_KEY (or the provision
# helper's resolve_ssh_key fallback ran on the login node), so configure installs
# the key.  The sandbox path falls back the same way as the other scripts.
if [ "${COMPUTEMCP_BUILD_LOCATION:-login}" = compute ]; then
    BUILD_SANDBOX="${COMPUTEMCP_SANDBOX_DIR:-}"
    if [ -z "$BUILD_SANDBOX" ]; then
        BUILD_SANDBOX="${COMPUTEMCP_STORAGE_ROOT:?COMPUTEMCP_SANDBOX_DIR or COMPUTEMCP_STORAGE_ROOT is required}/${COMPUTEMCP_SYSTEM:?COMPUTEMCP_SYSTEM is required}/sandbox"
    fi
    if [ ! -d "$BUILD_SANDBOX" ]; then
        echo "Sandbox missing; building and configuring it on the compute node (build-location = compute)." >&2
        # ``build`` runs the runtime build and the immediate configure (the
        # container script's build action ends in configure for both runtimes),
        # so the SSH key from the settings file is installed here too.
        bash "$CONTAINER_SCRIPT" build
    fi
fi

# Start the container DIRECTLY on this batch node.  It must NOT run inside an
# ``srun`` job step: under fakeroot/root-mapped Apptainer the instance's
# lifetime is tied to the step, so when the step command exits the scheduler
# tears down the step cgroup and kills the instance ("no instance found" moments
# later, even though ``instance start`` reported success).  The batch process
# keeps the allocation alive, so the instance it starts here survives.  The
# settings file was already sourced above, so this shell owns the full
# COMPUTEMCP_* environment regardless of the user's --export policy.  SRUN_ARGS
# apply only to the relay's internal ``--connect`` steps, not to starting the
# instance, so they are not passed here.
bash "$CONTAINER_SCRIPT" start
STARTED=1

# Publish the node only after the container serves an SSH banner.
python3 - "$CONTAINER_PORT" "$SSH_WAIT_SECONDS" <<'PY'
import socket
import time
import sys

deadline = time.monotonic() + int(sys.argv[2])
while time.monotonic() < deadline:
    try:
        with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=2) as sock:
            sock.settimeout(2)
            with sock.makefile("rb") as stream:
                for _ in range(20):
                    line = stream.readline(4096)
                    if line.startswith(b"SSH-"):
                        raise SystemExit(0)
                    if not line:
                        break
    except OSError:
        pass
    time.sleep(1)
raise SystemExit(f"Container SSH server did not become ready on port {sys.argv[1]}")
PY

NODE="$(hostname -s)"
TEMP_READY="$(mktemp "$STATE/ready-$SLURM_JOB_ID.XXXXXX")"
printf '%s\n' "$NODE" > "$TEMP_READY"
mv -- "$TEMP_READY" "$READY"
echo "Container ready on $NODE, port $CONTAINER_PORT." >&2

# Keep the allocation alive; the EXIT trap stops the container on termination.
while :; do
    sleep 10 &
    wait "$!"
done
