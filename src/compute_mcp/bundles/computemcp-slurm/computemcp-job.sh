#!/usr/bin/env bash
# computeMCP Slurm batch job.
#
# Runs inside the allocation on the compute node.  It starts the container via
# computemcp-container.sh (on the first node) and then keeps the allocation
# alive.  The job-step arguments come from the per-job settings file whose path
# the provisioning helper passes as the first positional argument (with
# COMPUTEMCP_SRUN_SETTINGS_FILE as an environment fallback); they are the
# SRUN_ARGS the gateway rendered and are deliberately NOT copied from the
# sbatch stage.
set -euo pipefail
umask 077

: "${SLURM_JOB_ID:?Must run inside a Slurm allocation}"

STATE="${COMPUTEMCP_STATE_DIR:-}"
CONTAINER_PORT="${COMPUTEMCP_CONTAINER_PORT:-2222}"
SSH_WAIT_SECONDS="${COMPUTEMCP_SSH_WAIT_SECONDS:-120}"
SRUN_ARGS=()
# The provisioning helper passes the per-job settings file path as the first
# positional argument, which Slurm delivers verbatim even with --export=NONE.
# This is the SRUN_ARGS transport: it does not touch the user's export policy.
# Sourcing must happen before STATE is required, because with --export=NONE the
# settings file is the only carrier of the container configuration.
SETTINGS="${1:-${COMPUTEMCP_SRUN_SETTINGS_FILE:-}}"
if [ -n "$SETTINGS" ] && [ -r "$SETTINGS" ]; then
    # Source the per-job settings: it exports the whole container configuration
    # and COMPUTEMCP_SRUN_ARGS (one complete argument per line, no trailing
    # newline, exactly as the gateway rendered it).
    # shellcheck disable=SC1090
    source "$SETTINGS"
fi
STATE="${COMPUTEMCP_STATE_DIR:?COMPUTEMCP_STATE_DIR is required}"

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

if [ -n "${COMPUTEMCP_SRUN_ARGS:-}" ]; then
    mapfile -t SRUN_ARGS <<< "$COMPUTEMCP_SRUN_ARGS"
fi

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

# Launch the container step explicitly through srun.  The step carries only the
# rendered SRUN_ARGS; sbatch settings are never copied here.
srun "${SRUN_ARGS[@]}" bash "$CONTAINER_SCRIPT" start
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
