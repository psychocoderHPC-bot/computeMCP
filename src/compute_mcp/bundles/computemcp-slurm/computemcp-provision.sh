#!/usr/bin/env bash
# computeMCP Slurm provisioning entry point, run on the Slurm login node.
#
# Actions:
#   submit   - submit one allocation (sbatch --parsable), persist its native id
#              and return immediately; no queue wait and no relay.
#   observe  - report the current job state (pending/starting/ready/terminal)
#              without blocking and without starting anything.
#   attach   - wait/poll until the allocation publishes readiness, then start
#              (or reuse) this job's login-node relay and print ENDPOINT.
#   cancel   - explicit, authorized scancel of this job plus relay shutdown.
#   detach   - stop the relay only; never scancel.  Used by shutdown/teardown.
#   teardown - alias of detach that also leaves durable job state in place.
#   provision- legacy synchronous submit+attach (kept for the current gateway).
#   stop/close - direct targets stop their container; Slurm targets detach.
#   status   - print the tracked job state/endpoint.
#   shell    - interactive shell in the tracked allocation.
#
# The command may be given as ``computemcp-provision.sh <action> [job_id]``.
# ``job_id`` is the opaque computeMCP job identity; when omitted it comes from
# COMPUTEMCP_JOB_ID and otherwise defaults to the stable legacy id ``default``.
# Each job owns a state directory under ``<state-root>/jobs/<job_id>/`` so
# co-located jobs never share a jobid, relay port, PID or readiness file.
#
# Stages are strictly separate: SBATCH_ARGS request the allocation; the batch
# script sources the settings file for the container configuration and starts
# the container directly on the batch node; SRUN_ARGS are consumed by the
# relay's per-connection srun steps.  They are never copied between each other
# or merged.
#
# Settings-file transport: the helper writes a per-job settings file (the
# container configuration plus COMPUTEMCP_SRUN_ARGS) and passes its path to the
# batch script as a positional argument (``sbatch ... job.sh SETTINGS``).
# Slurm passes positional arguments to the batch script verbatim, so this
# survives ``--export=NONE`` and any custom export policy without the helper
# touching the user's ``--export``.
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_SCRIPT="$SCRIPT_DIR/computemcp-container.sh"
JOB_SCRIPT="$SCRIPT_DIR/computemcp-job.sh"
RELAY="$SCRIPT_DIR/computemcp-relay.py"
# The scheduler may copy computemcp-job.sh into its spool directory and run it
# there, so the batch script cannot rely on BASH_SOURCE to find its siblings.
# Export the real bundle directory and transport it through the per-job
# settings file (see write_settings) so the job finds computemcp-container.sh.
export COMPUTEMCP_BUNDLE_DIR="$SCRIPT_DIR"

ACTION="${1:-provision}"
shift || true
case "$ACTION" in
    submit|observe|attach|cancel|detach|teardown|provision|stop|close|shell|status) ;;
    *)
        echo "Usage: bash $0 [submit|observe|attach|cancel|detach|teardown|provision|stop|close|shell|status] [job_id]" >&2
        exit 2
        ;;
esac

# --- Job identity -----------------------------------------------------------
# The positional argument wins over the environment so a caller can address a
# specific job explicitly.  ``default`` keeps the pre-multi-job single-job path
# working (and is the only id the current gateway knows).
JOB_ID_ARG="${1:-}"
if [ -n "$JOB_ID_ARG" ]; then
    shift
fi
if [ -z "$JOB_ID_ARG" ] && [ -z "${COMPUTEMCP_JOB_ID:-}" ] &&
    [ -n "${COMPUTEMCP_STATE_DIR:-}" ] &&
    [ "$(basename -- "$(dirname -- "$COMPUTEMCP_STATE_DIR")")" = jobs ]; then
    # The caller addressed the job by its per-job state directory.
    JOB_ID_ARG="$(basename -- "$COMPUTEMCP_STATE_DIR")"
fi
JOB_ID="${JOB_ID_ARG:-${COMPUTEMCP_JOB_ID:-default}}"
if ! { [[ "$JOB_ID" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] &&
       [ "$JOB_ID" != . ] && [ "$JOB_ID" != .. ]; }; then
    echo "Invalid job id: $JOB_ID" >&2
    exit 2
fi
export COMPUTEMCP_JOB_ID="$JOB_ID"

# --- Gateway environment and fallbacks -------------------------------------
SYSTEM="${COMPUTEMCP_SYSTEM:-}"
if [ -n "${COMPUTEMCP_STATE_DIR:-}" ] && [ -z "$SYSTEM" ]; then
    # Derive the system name from "<storage>/<system>/state/jobs/<id>" or the
    # legacy "<storage>/<system>/state" so env-less stop/status keep working.
    STATE_PARENT="$(dirname -- "$COMPUTEMCP_STATE_DIR")"
    if [ "$(basename -- "$STATE_PARENT")" = jobs ]; then
        STATE_PARENT="$(dirname -- "$STATE_PARENT")"
    fi
    if [ "$(basename -- "$STATE_PARENT")" = state ]; then
        STATE_PARENT="$(dirname -- "$STATE_PARENT")"
    fi
    DERIVED_SYSTEM="$(basename -- "$STATE_PARENT")"
    if [ -n "$DERIVED_SYSTEM" ] && [ "$DERIVED_SYSTEM" != / ] &&
        [ "$DERIVED_SYSTEM" != . ] && [ "$DERIVED_SYSTEM" != .. ] &&
        [[ "$DERIVED_SYSTEM" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]]; then
        SYSTEM="$DERIVED_SYSTEM"
    fi
fi
SYSTEM="${SYSTEM:-computemcp}"
if ! { [[ "$SYSTEM" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] && [ "$SYSTEM" != . ] && [ "$SYSTEM" != .. ]; }; then
    echo "Invalid system name: $SYSTEM" >&2
    exit 2
fi

expand_home_prefix() {
    local VALUE="$1"
    if [[ "$VALUE" =~ ^\$HOME(/|$) ]]; then
        [ -n "${HOME:-}" ] || {
            echo 'A configured path starts with $HOME but HOME is unset; provide an absolute path.' >&2
            exit 2
        }
        VALUE="$HOME${VALUE#\$HOME}"
    elif [[ "$VALUE" =~ ^~(/|$) ]]; then
        [ -n "${HOME:-}" ] || {
            echo 'A configured path starts with ~ but HOME is unset; provide an absolute path.' >&2
            exit 2
        }
        VALUE="$HOME${VALUE#\~}"
    fi
    printf '%s\n' "$VALUE"
}

STORAGE_ROOT="${COMPUTEMCP_STORAGE_ROOT:-}"
STORAGE_ROOT="$(expand_home_prefix "$STORAGE_ROOT")"
if [ -z "$STORAGE_ROOT" ]; then
    if [ -n "${HOME:-}" ]; then
        STORAGE_ROOT="$HOME/.local/share/computemcp"
    elif [ -n "${COMPUTEMCP_STATE_DIR:-}" ]; then
        STATE_PARENT="$(dirname -- "$COMPUTEMCP_STATE_DIR")"
        if [ "$(basename -- "$STATE_PARENT")" = jobs ]; then
            STATE_PARENT="$(dirname -- "$STATE_PARENT")"
        fi
        if [ "$(basename -- "$STATE_PARENT")" = state ]; then
            STATE_PARENT="$(dirname -- "$STATE_PARENT")"
        fi
        STORAGE_ROOT="$(dirname -- "$STATE_PARENT")"
    else
        echo 'Neither COMPUTEMCP_STORAGE_ROOT nor HOME is set; provide an absolute COMPUTEMCP_STORAGE_ROOT.' >&2
        exit 2
    fi
fi
SYSTEM_DIR="$STORAGE_ROOT/$SYSTEM"
STATE_ROOT="${COMPUTEMCP_STATE_DIR:-$SYSTEM_DIR/state}"
# The state directory may already point at a per-job directory (an explicit
# caller) or at the target root.  Normalize to the target root so the per-job
# layout is stable.
if [ "$(basename -- "$(dirname -- "$STATE_ROOT")")" = jobs ]; then
    STATE_ROOT="$(dirname -- "$(dirname -- "$STATE_ROOT")")"
fi
JOB_STATE="$STATE_ROOT/jobs/$JOB_ID"

# --- Slurm detection --------------------------------------------------------
# SLURM mode keeps the allocation/srun/relay path; DIRECT mode (no Slurm on the
# box) runs the container on this login/remote node and exposes its loopback
# port.  A configured provision_command is chosen by the gateway before this
# bundle helper runs, so it still takes precedence.
HAVE_SLURM=0
if command -v sbatch >/dev/null 2>&1 && command -v srun >/dev/null 2>&1; then
    HAVE_SLURM=1
fi

# --- Runtime name -----------------------------------------------------------
# Docker is daemon-global and Apptainer instances are per-host, so the target
# name alone collides for two users on the same node.  The remote numeric uid
# is the only discriminator available on the remote host and is stable across
# login and compute nodes; include it.  A non-default job id is appended so two
# jobs on one compute node get distinct instance names.  Keep this derivation
# identical to computemcp-container.sh so both scripts agree byte-for-byte.
REMOTE_UID="$(id -u 2>/dev/null || true)"
job_name_suffix() {
    if [ "$JOB_ID" != default ]; then
        printf -- '-%s' "$JOB_ID"
    fi
}
if [ -n "${COMPUTEMCP_CONTAINER_NAME:-}" ]; then
    NAME="$COMPUTEMCP_CONTAINER_NAME"
elif [ -n "$REMOTE_UID" ]; then
    NAME="computemcp-$REMOTE_UID-$SYSTEM$(job_name_suffix)"
else
    NAME="computemcp-$SYSTEM$(job_name_suffix)"
fi
if ! { [[ "$NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] && [ "$NAME" != . ] && [ "$NAME" != .. ]; }; then
    echo "Invalid container name: $NAME" >&2
    exit 2
fi

SANDBOX="${COMPUTEMCP_SANDBOX_DIR:-$SYSTEM_DIR/sandbox}"
HOST_HOME="${COMPUTEMCP_HOST_HOME:-$SYSTEM_DIR/home}"
RUNTIME="${COMPUTEMCP_CONTAINER_RUNTIME:-}"
IMAGE="${COMPUTEMCP_IMAGE:-}"
GPU_VENDORS="${COMPUTEMCP_GPU_VENDORS:-}"
BUILD_LOCATION="${COMPUTEMCP_BUILD_LOCATION:-login}"
case "$BUILD_LOCATION" in
    login|compute) ;;
    compute-node) BUILD_LOCATION=compute ;;
    *)
        echo "Invalid COMPUTEMCP_BUILD_LOCATION: $BUILD_LOCATION (expected login or compute)." >&2
        exit 2
        ;;
esac

# --- Root mode (fakeroot vs clean) -----------------------------------------
ROOT_MODE="${COMPUTEMCP_ROOT_MODE:-fakeroot}"
case "$ROOT_MODE" in
    fakeroot|clean) ;;
    *)
        echo "Invalid COMPUTEMCP_ROOT_MODE: $ROOT_MODE (expected fakeroot or clean)." >&2
        exit 2
        ;;
esac
CLEAN_IMAGE="${COMPUTEMCP_CLEAN_IMAGE:-}"

CONTAINER_PORT="${COMPUTEMCP_CONTAINER_PORT:-}"
if [ "$HAVE_SLURM" != 1 ] && [ -z "$CONTAINER_PORT" ]; then
    CONTAINER_PORT=2222
fi
if [ -z "$CONTAINER_PORT" ]; then
    # Empty means "let the compute node pick a free ephemeral port"; each job
    # publishes the concrete port in its own state directory.
    CONTAINER_PORT=0
fi
SSH_WAIT_SECONDS="${COMPUTEMCP_SSH_WAIT_SECONDS:-120}"
FORWARD_PORT_OVERRIDE="${COMPUTEMCP_FORWARD_PORT:-}"
PORT=0
if [ -n "$FORWARD_PORT_OVERRIDE" ]; then
    if ! { [[ "$FORWARD_PORT_OVERRIDE" =~ ^[0-9]+$ ]] &&
           (( FORWARD_PORT_OVERRIDE >= 1 && FORWARD_PORT_OVERRIDE <= 65535 )); }; then
        echo "Invalid COMPUTEMCP_FORWARD_PORT: $FORWARD_PORT_OVERRIDE (expected an integer 1..65535)." >&2
        exit 2
    fi
    PORT="$FORWARD_PORT_OVERRIDE"
fi
WAIT_SECONDS="${COMPUTEMCP_WAIT_SECONDS:-900}"
NODES="${COMPUTEMCP_NODES:-1}"

# --- Pre-provision environment hook -----------------------------------------
# The gateway exports COMPUTEMCP_PROVISION_ENV as the configured ``module load``
# / ``source`` lines joined by newlines (no trailing newline).  Each line runs
# in THIS shell so the hook takes effect here; set -euo pipefail aborts on
# failure.  The same helper is applied inside computemcp-job.sh, because a
# login-node module load does not propagate to the compute node.
apply_provision_env() {
    [ -n "${COMPUTEMCP_PROVISION_ENV:-}" ] || return 0
    local LINE
    while IFS= read -r LINE; do
        [ -n "$LINE" ] || continue
        eval "$LINE"
    done <<< "$COMPUTEMCP_PROVISION_ENV"
}

# --- Home selection ---------------------------------------------------------
# Shared jobs use <home-base>/shared/; private jobs use
# <home-base>/agents/<client_id>/ with an optional clean_home resolved beneath
# that private home.  A client id, clean_home and clean_home path components
# are validated and symlink escapes are rejected.  The heavy lifting is in
# Python so the same check is safe against concurrent changes.
COMPUTEMCP_HOME_BASE="${COMPUTEMCP_HOME_BASE:-}"
JOB_KIND="${COMPUTEMCP_JOB_KIND:-shared}"
case "$JOB_KIND" in
    shared|private) ;;
    *)
        echo "Invalid COMPUTEMCP_JOB_KIND: $JOB_KIND (expected shared or private)." >&2
        exit 2
        ;;
esac
CLIENT_ID="${COMPUTEMCP_CLIENT_ID:-}"
CLEAN_HOME="${COMPUTEMCP_CLEAN_HOME:-}"

resolve_home() {
    local BASE="$1" KIND="$2" CLIENT="$3" CLEAN="$4"
    python3 - "$BASE" "$KIND" "$CLIENT" "$CLEAN" <<'PY'
import os
import re
import sys

base, kind, client, clean = sys.argv[1:5]
if kind not in ("shared", "private"):
    raise SystemExit("invalid job kind")
if kind == "private":
    if not client:
        raise SystemExit("COMPUTEMCP_CLIENT_ID is required for a private job")
    if client in (".", "..") or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]*", client):
        raise SystemExit("invalid COMPUTEMCP_CLIENT_ID")
if kind == "shared" and clean:
    raise SystemExit("COMPUTEMCP_CLEAN_HOME is only valid for private jobs")

base = os.path.abspath(base)
os.makedirs(base, exist_ok=True)
base = os.path.realpath(base)
if kind == "private":
    rel = ["agents", client]
else:
    rel = ["shared"]
if clean:
    value = clean
    if value.startswith("/"):
        value = value[1:]
    parts = [part for part in value.split("/") if part not in ("", ".")]
    if not parts:
        raise SystemExit("COMPUTEMCP_CLEAN_HOME must name a directory")
    if any(part == ".." for part in parts):
        raise SystemExit("COMPUTEMCP_CLEAN_HOME must not contain '..'")
    rel.extend(parts)

current = base
for part in rel:
    current = os.path.join(current, part)
    if os.path.islink(current):
        raise SystemExit("refusing a symlink in the home path: " + current)
    try:
        os.mkdir(current)
    except FileExistsError:
        if os.path.islink(current) or not os.path.isdir(current):
            raise SystemExit("home path component is not a directory: " + current)
resolved = os.path.realpath(current)
if os.path.commonpath([base, resolved]) != base:
    raise SystemExit("resolved home escapes the home base")
print(resolved)
PY
}

select_home() {
    local EXPLICIT_BASE="${COMPUTEMCP_HOME_BASE:-}"
    # Preserve the previous host-home behavior when neither the new home base
    # nor a private/clean-home selection was requested.  A direct target always
    # keeps its old default; a Slurm target honors an explicit host-home from an
    # older gateway and otherwise uses the new shared home.
    if [ -z "$EXPLICIT_BASE" ] && [ -z "${COMPUTEMCP_JOB_KIND:-}" ] &&
        [ -z "$CLIENT_ID" ] && [ -z "$CLEAN_HOME" ]; then
        if [ "$HAVE_SLURM" != 1 ]; then
            HOST_HOME="${COMPUTEMCP_HOST_HOME:-$SYSTEM_DIR/home}"
            return 0
        fi
        if [ -n "${COMPUTEMCP_HOST_HOME:-}" ]; then
            HOST_HOME="$COMPUTEMCP_HOST_HOME"
            return 0
        fi
    fi
    local BASE RESOLVED
    BASE="${EXPLICIT_BASE:-$SYSTEM_DIR/home}"
    BASE="$(expand_home_prefix "$BASE")"
    RESOLVED="$(resolve_home "$BASE" "$JOB_KIND" "$CLIENT_ID" "$CLEAN_HOME")" || exit 2
    HOST_HOME="$RESOLVED"
}

# Re-export the resolved layout so computemcp-container.sh uses the same paths
# on the login node and the compute node.
export COMPUTEMCP_SYSTEM="$SYSTEM"
export COMPUTEMCP_STATE_DIR="$JOB_STATE"
export COMPUTEMCP_SANDBOX_DIR="$SANDBOX"
export COMPUTEMCP_HOST_HOME="$HOST_HOME"
export COMPUTEMCP_CONTAINER_RUNTIME="$RUNTIME"
export COMPUTEMCP_IMAGE="$IMAGE"
export COMPUTEMCP_GPU_VENDORS="$GPU_VENDORS"
export COMPUTEMCP_BUILD_LOCATION="$BUILD_LOCATION"
export COMPUTEMCP_CONTAINER_PORT="$CONTAINER_PORT"
export COMPUTEMCP_SSH_WAIT_SECONDS="$SSH_WAIT_SECONDS"
export COMPUTEMCP_SSH_USER="${COMPUTEMCP_SSH_USER:-ubuntu}"
export COMPUTEMCP_CONTAINER_NAME="$NAME"
export COMPUTEMCP_ROOT_MODE="$ROOT_MODE"
export COMPUTEMCP_CLEAN_IMAGE="$CLEAN_IMAGE"
export COMPUTEMCP_JOB_KIND="$JOB_KIND"
export COMPUTEMCP_CLIENT_ID="$CLIENT_ID"
export COMPUTEMCP_CLEAN_HOME="$CLEAN_HOME"

# Resolve the SSH public key to install in the container.
resolve_ssh_key() {
    if [ -n "${COMPUTEMCP_SSH_PUBLIC_KEY:-}" ]; then
        return 0
    fi
    if [ -n "${COMPUTEMCP_SSH_PUBLIC_KEY_FILE:-}" ]; then
        [ -r "$COMPUTEMCP_SSH_PUBLIC_KEY_FILE" ] || {
            echo "Unreadable SSH public key file: $COMPUTEMCP_SSH_PUBLIC_KEY_FILE" >&2
            exit 1
        }
        COMPUTEMCP_SSH_PUBLIC_KEY="$(head -n 1 "$COMPUTEMCP_SSH_PUBLIC_KEY_FILE")"
        export COMPUTEMCP_SSH_PUBLIC_KEY
        return 0
    fi
    if [ -s "$HOST_HOME/.ssh/authorized_keys" ]; then
        COMPUTEMCP_SSH_PUBLIC_KEY="$(head -n 1 "$HOST_HOME/.ssh/authorized_keys")"
        export COMPUTEMCP_SSH_PUBLIC_KEY
        return 0
    fi
    echo 'No SSH public key available: set COMPUTEMCP_SSH_PUBLIC_KEY.' >&2
    exit 1
}

# --- Parse the two argv-style stages ---------------------------------------
SBATCH_ARGS=()
if [ -n "${COMPUTEMCP_SBATCH_ARGS:-}" ]; then
    mapfile -t SBATCH_ARGS <<< "$COMPUTEMCP_SBATCH_ARGS"
fi
SRUN_ARGS=()
if [ -n "${COMPUTEMCP_SRUN_ARGS:-}" ]; then
    mapfile -t SRUN_ARGS <<< "$COMPUTEMCP_SRUN_ARGS"
fi

CPU_BIND="none"
for (( IDX = 0; IDX < ${#SRUN_ARGS[@]}; IDX++ )); do
    ARG="${SRUN_ARGS[IDX]}"
    case "$ARG" in
        --cpu-bind=*) CPU_BIND="${ARG#--cpu-bind=}" ;;
        --cpu-bind) if (( IDX + 1 < ${#SRUN_ARGS[@]} )); then CPU_BIND="${SRUN_ARGS[IDX + 1]}"; fi ;;
    esac
done
export COMPUTEMCP_CPU_BIND="$CPU_BIND"

# Manual/legacy resource fallback.  It fills SBATCH_ARGS ONLY when the gateway
# provided no COMPUTEMCP_SBATCH_ARGS, so it cannot double-request resources.
if [ "${#SBATCH_ARGS[@]}" -eq 0 ]; then
    LEGACY_CPUS="${COMPUTEMCP_CPUS:-}"
    LEGACY_MEMORY="${COMPUTEMCP_MEMORY:-}"
    LEGACY_GPUS="${COMPUTEMCP_GPUS:-}"
    LEGACY_TIME="${COMPUTEMCP_TIME_LIMIT:-}"
    [ -z "${COMPUTEMCP_PARTITION:-}" ] || SBATCH_ARGS+=(--partition="$COMPUTEMCP_PARTITION")
    [ -z "${COMPUTEMCP_ACCOUNT:-}" ] || SBATCH_ARGS+=(--account="$COMPUTEMCP_ACCOUNT")
    [ -z "$LEGACY_CPUS" ] || SBATCH_ARGS+=(--cpus-per-task="$LEGACY_CPUS")
    [ -z "$LEGACY_GPUS" ] || SBATCH_ARGS+=(--gres="gpu:$LEGACY_GPUS")
    [ -z "$LEGACY_MEMORY" ] || SBATCH_ARGS+=(--mem="$LEGACY_MEMORY")
    [ -z "$LEGACY_TIME" ] || SBATCH_ARGS+=(--time="$LEGACY_TIME")
fi

# --- Validation -------------------------------------------------------------
[[ "$NODES" =~ ^[0-9]+$ ]] || { echo "Invalid node count: $NODES" >&2; exit 2; }
if (( NODES > 1 )); then
    echo 'multi-node not yet supported: requested COMPUTEMCP_NODES='"$NODES" >&2
    exit 2
fi
for VALUE in "$CONTAINER_PORT" "$SSH_WAIT_SECONDS" "$PORT" "$WAIT_SECONDS"; do
    [[ "$VALUE" =~ ^[0-9]+$ ]] || { echo "Expected integer: $VALUE" >&2; exit 2; }
done
# CONTAINER_PORT 0 means "pick a free ephemeral port on the compute node"; an
# explicit value must be a usable unprivileged port.
if (( CONTAINER_PORT != 0 )) && (( CONTAINER_PORT < 1024 || CONTAINER_PORT > 65535 )); then
    echo 'Invalid container port.' >&2
    exit 2
fi
(( PORT >= 0 && PORT <= 65535 && SSH_WAIT_SECONDS > 0 && WAIT_SECONDS > 0 )) || {
    echo 'Invalid timeout or port configuration.' >&2
    exit 2
}
if [ "$CONTAINER_PORT" != 0 ] && [ "$PORT" != 0 ] && [ "$PORT" = "$CONTAINER_PORT" ]; then
    echo 'Forward and container ports must differ.' >&2
    exit 2
fi
# Only the stages that build/start a container need the clean base; teardown
# and status must keep working without re-supplying it.
if [ "$ROOT_MODE" = clean ] && { [ "$ACTION" = submit ] || [ "$ACTION" = provision ]; }; then
    if [ "$RUNTIME" = docker ]; then
        echo 'clean root mode requires Apptainer (an immutable SIF), not Docker.' >&2
        exit 2
    fi
    [ -n "$CLEAN_IMAGE" ] || { echo 'COMPUTEMCP_CLEAN_IMAGE is required for clean root mode.' >&2; exit 2; }
    if [ "$BUILD_LOCATION" = compute ]; then
        echo 'clean root mode uses a prepared image; build-location = compute is not supported.' >&2
        exit 2
    fi
fi

# The relay and batch script are only needed in Slurm mode; direct mode neither
# submits an allocation nor starts a relay.
if [ "$HAVE_SLURM" = 1 ]; then
    for CMD in sbatch squeue scancel python3 flock; do command -v "$CMD" >/dev/null; done
    for FILE in "$RELAY" "$JOB_SCRIPT"; do
        [ -r "$FILE" ] || { echo "Missing/unreadable helper: $FILE" >&2; exit 1; }
    done
else
    for CMD in python3 flock; do command -v "$CMD" >/dev/null; done
fi
[ -r "$CONTAINER_SCRIPT" ] || { echo "Missing/unreadable helper: $CONTAINER_SCRIPT" >&2; exit 1; }

# --- Mode and state selection ----------------------------------------------
# Slurm jobs get a per-job state directory; direct mode keeps the target-level
# directory so the existing Docker shared-container workflow is unchanged.
TRACKED_MODE=""
if [ -s "$JOB_STATE/mode" ]; then
    TRACKED_MODE="$(head -n 1 "$JOB_STATE/mode" || true)"
elif [ -s "$STATE_ROOT/mode" ]; then
    TRACKED_MODE="$(head -n 1 "$STATE_ROOT/mode" || true)"
fi
case "$TRACKED_MODE" in
    ""|slurm|direct) ;;
    *) echo 'Invalid tracked mode.' >&2; exit 1 ;;
esac
MODE="${TRACKED_MODE:-}"
if [ -z "$MODE" ]; then
    [ "$HAVE_SLURM" = 1 ] && MODE=slurm || MODE=direct
fi
if [ "$MODE" = direct ]; then
    STATE="$STATE_ROOT"
else
    STATE="$JOB_STATE"
fi
export COMPUTEMCP_STATE_DIR="$STATE"
mkdir -p "$STATE"

# --- Locking ----------------------------------------------------------------
exec 9>"$STATE/provision.lock"
flock -w "$WAIT_SECONDS" 9

JOBID="$(cat "$STATE/jobid" 2>/dev/null || true)"
CLUSTER="$(cat "$STATE/cluster" 2>/dev/null || true)"
[[ -z "$JOBID" || "$JOBID" =~ ^[0-9]+$ ]] || { echo 'Invalid tracked job ID.' >&2; exit 1; }
[[ -z "$CLUSTER" || "$CLUSTER" =~ ^[A-Za-z0-9._-]+$ ]] || { echo 'Invalid tracked cluster.' >&2; exit 1; }
export COMPUTEMCP_SYSTEM="$SYSTEM"
export COMPUTEMCP_STORAGE_ROOT="$STORAGE_ROOT"
export COMPUTEMCP_STATE_DIR="$STATE"

# --- Runtime derivation for stop/close/status -------------------------------
if [ "$ACTION" = stop ] || [ "$ACTION" = close ] || [ "$ACTION" = status ] ||
    [ "$ACTION" = detach ] || [ "$ACTION" = teardown ] || [ "$ACTION" = cancel ]; then
    if [ -z "$RUNTIME" ] && [ -s "$STATE/container.runtime" ]; then
        RECORDED_RUNTIME="$(head -n 1 "$STATE/container.runtime" || true)"
        case "$RECORDED_RUNTIME" in
            apptainer|docker) RUNTIME="$RECORDED_RUNTIME" ;;
        esac
    fi
    if [ -z "$RUNTIME" ]; then
        if command -v docker >/dev/null 2>&1; then
            RUNTIME=docker
        elif command -v apptainer >/dev/null 2>&1; then
            RUNTIME=apptainer
        fi
    fi
    export COMPUTEMCP_CONTAINER_RUNTIME="$RUNTIME"
    if [ -n "${COMPUTEMCP_STATE_DIR:-}" ] && [ "$MODE" = direct ]; then
        DERIVED_SYSTEM_DIR="$(dirname -- "$STATE_ROOT")"
        DERIVED_ROOT="$(dirname -- "$DERIVED_SYSTEM_DIR")"
        export COMPUTEMCP_SYSTEM="$SYSTEM"
        export COMPUTEMCP_STORAGE_ROOT="$DERIVED_ROOT"
        export COMPUTEMCP_STATE_DIR="$STATE_ROOT"
        export COMPUTEMCP_SANDBOX_DIR="$SANDBOX"
        export COMPUTEMCP_HOST_HOME="$HOST_HOME"
        export COMPUTEMCP_CONTAINER_PORT="$CONTAINER_PORT"
    fi
fi

# --- Container endpoint helpers (direct mode) -------------------------------
is_valid_endpoint() {
    local VALUE="$1" ENDPOINT_PORT="${1##*:}"
    [[ "$VALUE" =~ ^127\.0\.0\.1:[0-9]{1,5}$ ]] || return 1
    [[ "$ENDPOINT_PORT" =~ ^[0-9]+$ ]] || return 1
    (( ENDPOINT_PORT >= 1 && ENDPOINT_PORT <= 65535 ))
}

resolve_container_endpoint() {
    local CACHED="" LINE=""
    if [ -s "$STATE/container.endpoint" ]; then
        CACHED="$(head -n 1 "$STATE/container.endpoint" || true)"
        is_valid_endpoint "$CACHED" || CACHED=""
    fi
    case "$RUNTIME" in
        docker)
            if command -v docker >/dev/null 2>&1; then
                while IFS= read -r LINE; do
                    if is_valid_endpoint "$LINE"; then
                        if [ "$LINE" != "$CACHED" ]; then
                            printf '%s\n' "$LINE" > "$STATE/container.endpoint"
                        fi
                        printf '%s\n' "$LINE"
                        return 0
                    fi
                done < <(docker container port "$NAME" "$CONTAINER_PORT/tcp" 2>/dev/null || true)
            fi
            if [ -n "$CACHED" ]; then
                printf '%s\n' "$CACHED"
                return 0
            fi
            printf '127.0.0.1:%s\n' "$CONTAINER_PORT"
            return 0
            ;;
        apptainer)
            printf '127.0.0.1:%s\n' "$CONTAINER_PORT"
            return 0
            ;;
    esac
    if [ -n "$CACHED" ]; then
        printf '%s\n' "$CACHED"
        return 0
    fi
    return 1
}

# --- Job-state helpers ------------------------------------------------------
SQUEUE_ARGS=(--noheader --user "$(id -un)" --format '%i|%T')
[ -z "$CLUSTER" ] || SQUEUE_ARGS+=(--clusters="$CLUSTER")
raw_job_state() {
    squeue "${SQUEUE_ARGS[@]}" 2>/dev/null |
        awk -F '|' -v job="$JOBID" '$1 == job {print $2}'
}
# A terminal job leaves the default squeue view.  Query the terminal states
# explicitly so `observe` can tell "finished" from "not yet visible".
terminal_job_state() {
    [ -n "$JOBID" ] || return 1
    local TERM_ARGS=(--noheader --user "$(id -un)" --format '%i|%T'
        --states=COMPLETED,CANCELLED,FAILED,TIMEOUT,NODE_FAIL)
    [ -z "$CLUSTER" ] || TERM_ARGS+=(--clusters="$CLUSTER")
    squeue "${TERM_ARGS[@]}" 2>/dev/null |
        awk -F '|' -v job="$JOBID" '$1 == job {print $2; exit}'
}
# Map a Slurm state plus the per-job readiness publication to the small
# computeMCP vocabulary used by the gateway.
mapped_state() {
    local RAW NODE=""
    [ -s "$STATE/ready" ] && { read -r NODE < "$STATE/ready" || true; }
    RAW="$(raw_job_state || true)"
    case "$RAW" in
        PENDING) printf 'pending\n' ;;
        CONFIGURING) printf 'starting\n' ;;
        RUNNING)
            if [ -n "$NODE" ]; then
                printf 'ready\n'
            else
                printf 'starting\n'
            fi
            ;;
        COMPLETING|SUSPENDED|STOPPED) printf 'terminal\n' ;;
        COMPLETED|CANCELLED|FAILED|TIMEOUT|NODE_FAIL) printf 'terminal\n' ;;
        "")
            # The default squeue view hides terminal jobs.  Ask explicitly
            # before deciding; a persisted id with a stale readiness file but no
            # scheduler entry is terminal, and a fully unknown id is absent.
            if [ -n "$(terminal_job_state || true)" ]; then
                printf 'terminal\n'
            elif [ -n "$JOBID" ]; then
                if [ -n "$NODE" ]; then printf 'ready\n'; else printf 'terminal\n'; fi
            else
                printf 'absent\n'
            fi
            ;;
        *) printf 'terminal\n' ;;
    esac
}

relay_running() {
    local PID
    PID="$(cat "$STATE/relay.pid" 2>/dev/null || true)"
    [[ "$PID" =~ ^[0-9]+$ ]] && kill -0 "$PID" 2>/dev/null &&
        [ -r "/proc/$PID/cmdline" ] &&
        tr '\0' '\n' < "/proc/$PID/cmdline" |
        grep -Fxq -e "$RELAY" -e "$STATE/relay.py"
}
relay_ready_port() {
    local LINE READY_PID READY_PORT
    [ -s "$STATE/relay.ready" ] || return 1
    read -r READY_PID READY_PORT < "$STATE/relay.ready" || true
    [[ "$READY_PORT" =~ ^[0-9]+$ ]] || return 1
    (( READY_PORT >= 1 && READY_PORT <= 65535 )) || return 1
    printf '%s\n' "$READY_PORT"
}
stop_relay() {
    if relay_running; then
        kill "$(cat "$STATE/relay.pid")"
        for (( I = 0; I < 50; I++ )); do relay_running || break; sleep 0.1; done
    fi
    rm -f "$STATE/relay.pid" "$STATE/tunnel-job" "$STATE/relay.ready" "$STATE/relay.port"
}
job_container_port() {
    local VALUE
    if [ -s "$STATE/container.port" ]; then
        VALUE="$(head -n 1 "$STATE/container.port" || true)"
        if [[ "$VALUE" =~ ^[0-9]+$ ]] && (( VALUE >= 1 && VALUE <= 65535 )); then
            printf '%s\n' "$VALUE"
            return 0
        fi
    fi
    if [ "$CONTAINER_PORT" != 0 ]; then
        printf '%s\n' "$CONTAINER_PORT"
        return 0
    fi
    return 1
}

# --- Direct (non-Slurm) mode helpers ---------------------------------------
container_state() {
    case "$RUNTIME" in
        apptainer)
            if apptainer instance list 2>/dev/null | awk -v NAME="$NAME" '$1 == NAME {FOUND=1} END {exit !FOUND}'; then
                return 0
            fi
            [ -d "$SANDBOX" ] && return 1
            return 2
            ;;
        docker)
            if docker container inspect "$NAME" >/dev/null 2>&1; then
                [ "$(docker container inspect --format '{{.State.Running}}' "$NAME")" = true ] && return 0
                return 1
            fi
            return 2
            ;;
        *) return 2 ;;
    esac
}

runtime_tool() {
    case "$RUNTIME" in
        apptainer) command -v apptainer >/dev/null || { echo 'apptainer not found.' >&2; exit 1; } ;;
        docker)    command -v docker >/dev/null    || { echo 'docker not found.' >&2; exit 1; } ;;
        *)
            if command -v apptainer >/dev/null; then RUNTIME=apptainer
            elif command -v docker >/dev/null; then RUNTIME=docker
            else echo 'Set COMPUTEMCP_CONTAINER_RUNTIME to apptainer or docker.' >&2; exit 1
            fi
            export COMPUTEMCP_CONTAINER_RUNTIME="$RUNTIME"
            ;;
    esac
}
container_missing() {
    if [ "$ROOT_MODE" = clean ]; then
        [ ! -r "$CLEAN_IMAGE" ]
        return
    fi
    case "$RUNTIME" in
        apptainer) [ ! -d "$SANDBOX" ] ;;
        docker)    ! docker image inspect "${NAME,,}:latest" >/dev/null 2>&1 ;;
        *) return 1 ;;
    esac
}

COMPUTE_BUILD=0
if [ "$BUILD_LOCATION" = compute ]; then
    if [ "$HAVE_SLURM" = 1 ]; then
        COMPUTE_BUILD=1
    else
        echo "WARNING: build-location = compute requires Slurm; falling back to building on the login/remote node." >&2
    fi
fi

ensure_container() {
    runtime_tool
    resolve_ssh_key
    if [ "$ROOT_MODE" = clean ]; then
        [ -r "$CLEAN_IMAGE" ] || { echo "Clean image missing/unreadable: $CLEAN_IMAGE" >&2; exit 1; }
        bash "$CONTAINER_SCRIPT" configure
        return 0
    fi
    [ -n "$IMAGE" ] || { echo 'COMPUTEMCP_IMAGE is required.' >&2; exit 1; }
    if [ "$COMPUTE_BUILD" = 1 ]; then
        if container_missing; then
            echo "Sandbox missing; it will be built on the compute node (build-location = compute)." >&2
            return 0
        fi
        echo "Container present; configuring it on the login node." >&2
        bash "$CONTAINER_SCRIPT" configure
        return 0
    fi
    if container_missing; then
        echo "Container missing; building it on the login node." >&2
        bash "$CONTAINER_SCRIPT" build
    else
        echo "Container present; configuring it on the login node." >&2
        bash "$CONTAINER_SCRIPT" configure
    fi
}

direct_provision() {
    if container_state; then
        echo "Reusing running container $NAME." >&2
    else
        echo "Starting container $NAME on the remote node." >&2
        bash "$CONTAINER_SCRIPT" start
    fi
    printf '%s\n' "$RUNTIME" > "$STATE/container.runtime"
    local ENDPOINT
    ENDPOINT="$(resolve_container_endpoint)" || {
        echo "Could not determine the container endpoint for $NAME." >&2
        exit 1
    }
    printf 'ENDPOINT %s\n' "$ENDPOINT"
}

# --- Per-job settings file -------------------------------------------------
write_settings() {
    local FILE="$1"
    {
        printf '# computeMCP per-job settings for job %s (slurm %s)\n' "$JOB_ID" "${JOBID:-none}"
        printf '# Sourced by computemcp-job.sh; generated by computemcp-provision.sh.\n'
        printf 'export COMPUTEMCP_SYSTEM=%q\n' "$SYSTEM"
        printf 'export COMPUTEMCP_STORAGE_ROOT=%q\n' "$STORAGE_ROOT"
        printf 'export COMPUTEMCP_STATE_DIR=%q\n' "$STATE"
        printf 'export COMPUTEMCP_JOB_ID=%q\n' "$JOB_ID"
        printf 'export COMPUTEMCP_CONTAINER_NAME=%q\n' "$NAME"
        printf 'export COMPUTEMCP_BUNDLE_DIR=%q\n' "$COMPUTEMCP_BUNDLE_DIR"
        printf 'export COMPUTEMCP_SANDBOX_DIR=%q\n' "$SANDBOX"
        printf 'export COMPUTEMCP_HOST_HOME=%q\n' "$HOST_HOME"
        printf 'export COMPUTEMCP_CONTAINER_RUNTIME=%q\n' "$RUNTIME"
        printf 'export COMPUTEMCP_ROOT_MODE=%q\n' "$ROOT_MODE"
        printf 'export COMPUTEMCP_CLEAN_IMAGE=%q\n' "$CLEAN_IMAGE"
        printf 'export COMPUTEMCP_JOB_KIND=%q\n' "$JOB_KIND"
        printf 'export COMPUTEMCP_CLIENT_ID=%q\n' "$CLIENT_ID"
        printf 'export COMPUTEMCP_CLEAN_HOME=%q\n' "$CLEAN_HOME"
        printf 'export COMPUTEMCP_IMAGE=%q\n' "$IMAGE"
        printf 'export COMPUTEMCP_GPU_VENDORS=%q\n' "$GPU_VENDORS"
        printf 'export COMPUTEMCP_BUILD_LOCATION=%q\n' "$BUILD_LOCATION"
        printf 'export COMPUTEMCP_CPUS_PER_NODE=%q\n' "${COMPUTEMCP_CPUS_PER_NODE:-}"
        printf 'export COMPUTEMCP_GPUS_PER_NODE=%q\n' "${COMPUTEMCP_GPUS_PER_NODE:-}"
        printf 'export COMPUTEMCP_MEMORY_PER_NODE_MIB=%q\n' "${COMPUTEMCP_MEMORY_PER_NODE_MIB:-}"
        printf 'export COMPUTEMCP_CONTAINER_PORT=%q\n' "$CONTAINER_PORT"
        printf 'export COMPUTEMCP_SSH_WAIT_SECONDS=%q\n' "$SSH_WAIT_SECONDS"
        printf 'export COMPUTEMCP_SSH_USER=%q\n' "${COMPUTEMCP_SSH_USER:-ubuntu}"
        printf 'export COMPUTEMCP_SSH_PUBLIC_KEY=%q\n' "${COMPUTEMCP_SSH_PUBLIC_KEY:-}"
        printf 'export COMPUTEMCP_CPU_BIND=%q\n' "$CPU_BIND"
        printf 'export COMPUTEMCP_SRUN_ARGS=%q\n' "${COMPUTEMCP_SRUN_ARGS:-}"
        printf 'export COMPUTEMCP_PROVISION_ENV=%q\n' "${COMPUTEMCP_PROVISION_ENV:-}"
    } > "$FILE.tmp"
    mv -- "$FILE.tmp" "$FILE"
    chmod 600 "$FILE"
}

# --- Submit stage -----------------------------------------------------------
submit_allocation() {
    apply_provision_env
    select_home
    export COMPUTEMCP_HOST_HOME="$HOST_HOME"
    ensure_container
    printf '%s\n' slurm > "$STATE/mode"
    printf '%s\n' "$ROOT_MODE" > "$STATE/root_mode"
    printf '%s\n' "$JOB_KIND" > "$STATE/job_kind"
    [ -z "$CLEAN_HOME" ] || printf '%s\n' "$CLEAN_HOME" > "$STATE/clean_home"
    printf '%s\n' "$HOST_HOME" > "$STATE/home"
    printf '%s\n' "$NAME" > "$STATE/instance"
    printf '%s\n' "$RUNTIME" > "$STATE/container.runtime"

    local SETTINGS="$STATE/srun-$SYSTEM.settings"
    local HELPER_ARGS=(--parsable --job-name="$NAME")
    sbatch_has() {
        local WANTED="$1" ARG
        for ARG in "${SBATCH_ARGS[@]}"; do
            case "$ARG" in "$WANTED"|"$WANTED"=*) return 0 ;; esac
        done
        return 1
    }
    sbatch_has --output || sbatch_has -o || HELPER_ARGS+=(--output="$STATE/slurm-%j.log")
    sbatch_has --error  || sbatch_has -e || HELPER_ARGS+=(--error="$STATE/slurm-%j.log")
    write_settings "$SETTINGS"
    local RESULT
    RESULT="$(sbatch "${SBATCH_ARGS[@]}" "${HELPER_ARGS[@]}" "$JOB_SCRIPT" "$SETTINGS")"
    JOBID="${RESULT%%;*}"
    [[ "$JOBID" =~ ^[0-9]+$ ]] || { echo "Invalid sbatch result: $RESULT" >&2; exit 1; }
    CLUSTER=""
    if [[ "$RESULT" == *';'* ]]; then CLUSTER="${RESULT#*;}"; fi
    # Persist the native id immediately: a later crash must not lose it.
    printf '%s\n' "$JOBID" > "$STATE/jobid"
    printf '%s\n' "$CLUSTER" > "$STATE/cluster"
    echo "Submitted job $JOB_ID as Slurm $JOBID (cluster ${CLUSTER:-default})." >&2
    printf 'JOB_ID %s\n' "$JOB_ID"
    printf 'SLURM_JOB_ID %s\n' "$JOBID"
    printf 'CLUSTER %s\n' "${CLUSTER:-default}"
    printf 'STATE pending\n'
}

# --- Observe stage ----------------------------------------------------------
observe_job() {
    local STATE_NAME
    STATE_NAME="$(mapped_state)"
    printf 'STATE %s\n' "$STATE_NAME"
    [ -z "$JOBID" ] || printf 'SLURM_JOB_ID %s\n' "$JOBID"
    [ -z "$CLUSTER" ] || printf 'CLUSTER %s\n' "$CLUSTER"
    if [ -s "$STATE/ready" ]; then
        printf 'NODE %s\n' "$(head -n 1 "$STATE/ready")"
    fi
    local CPORT
    if CPORT="$(job_container_port 2>/dev/null)"; then
        printf 'CONTAINER_PORT %s\n' "$CPORT"
    fi
    return 0
}

# --- Attach stage -----------------------------------------------------------
attach_job() {
    [ -n "$JOBID" ] || { echo 'No tracked job; submit first.' >&2; exit 1; }
    local END=$((SECONDS + WAIT_SECONDS))
    local NODE="" RAW=""
    while (( SECONDS < END )); do
        RAW="$(raw_job_state || true)"
        case "$RAW" in
            RUNNING)
                if [ -s "$STATE/ready" ]; then
                    read -r NODE < "$STATE/ready" || true
                    [ -n "$NODE" ] && break
                fi
                ;;
            PENDING|CONFIGURING) ;;
            COMPLETING|SUSPENDED|STOPPED)
                tail -n 60 "$STATE/slurm-$JOBID.log" >&2 2>/dev/null || true
                echo "Job $JOBID is terminal ($RAW)." >&2
                exit 1
                ;;
            *)
                tail -n 60 "$STATE/slurm-$JOBID.log" >&2 2>/dev/null || true
                echo "Job $JOBID is terminal or unknown ($RAW)." >&2
                exit 1
                ;;
        esac
        sleep 2
    done
    [ -n "$NODE" ] || {
        echo "Timed out waiting for job $JOBID; it stays tracked for the next call." >&2
        exit 1
    }
    [[ "$NODE" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "Invalid node name: $NODE" >&2; exit 1; }

    local CPORT
    CPORT="$(job_container_port)" || {
        echo 'Container port is not published yet.' >&2
        exit 1
    }
    export COMPUTEMCP_CONTAINER_PORT="$CPORT"
    export COMPUTEMCP_CPU_BIND="$CPU_BIND"

    if [ -z "$FORWARD_PORT_OVERRIDE" ]; then
        if relay_running && [ -s "$STATE/relay.port" ]; then
            RECORDED_PORT="$(head -n 1 "$STATE/relay.port" 2>/dev/null || true)"
            if [[ "$RECORDED_PORT" =~ ^[0-9]+$ ]] &&
               (( RECORDED_PORT >= 1 && RECORDED_PORT <= 65535 )); then
                PORT="$RECORDED_PORT"
            else
                PORT=0
            fi
        else
            PORT=0
        fi
    fi

    local WANTED="$JOBID $NODE $PORT $CPORT"
    if ! relay_running || [ "$(cat "$STATE/tunnel-job" 2>/dev/null || true)" != "$WANTED" ]; then
        stop_relay
        nohup python3 "$RELAY" --job-id "$JOB_ID" \
            "$PORT" "$JOBID" "$NODE" "$STATE/relay.ready" \
            </dev/null >>"$STATE/relay.log" 2>&1 9>&- &
        printf '%s\n' "$!" > "$STATE/relay.pid"
        for (( I = 0; I < 50; I++ )); do
            [ -s "$STATE/relay.ready" ] && break
            kill -0 "$(cat "$STATE/relay.pid")" 2>/dev/null || break
            sleep 0.1
        done
        if ! { relay_running && [ -s "$STATE/relay.ready" ]; }; then
            echo "Relay failed; see $STATE/relay.log" >&2
            stop_relay
            exit 1
        fi
    fi

    local PARSED_PORT
    PARSED_PORT="$(relay_ready_port || true)"
    if [ -z "$PARSED_PORT" ]; then
        echo "Relay ready file $STATE/relay.ready is missing or malformed." >&2
        if [ -n "$FORWARD_PORT_OVERRIDE" ]; then
            PORT="$FORWARD_PORT_OVERRIDE"
        else
            echo "Cannot determine the relay port; see $STATE/relay.log" >&2
            stop_relay
            exit 1
        fi
    else
        PORT="$PARSED_PORT"
    fi
    PORT_TMP="$STATE/relay.port.tmp.$$"
    printf '%s\n' "$PORT" > "$PORT_TMP"
    mv -f "$PORT_TMP" "$STATE/relay.port"
    # Rebuild WANTED with the concrete bound port so a reconnect sees a match
    # and reuses the relay instead of restarting it.
    WANTED="$JOBID $NODE $PORT $CPORT"
    printf '%s\n' "$WANTED" > "$STATE/tunnel-job"

    local READY=0
    for (( I = 0; I < 3; I++ )); do
        if python3 "$RELAY" --check "$PORT"; then READY=1; break; fi
        relay_running || { echo "Relay failed; see $STATE/relay.log" >&2; exit 1; }
        sleep 1
    done
    [ "$READY" = 1 ] || { echo 'Container SSH endpoint is unavailable.' >&2; exit 1; }

    echo "Ready: job $JOB_ID (Slurm $JOBID) on $NODE, local port $PORT, container port $CPORT." >&2
    printf 'ENDPOINT 127.0.0.1:%s\n' "$PORT"
}

# --- Cancel and detach ------------------------------------------------------
cancel_job() {
    if [ "$MODE" = direct ]; then
        bash "$CONTAINER_SCRIPT" stop
        echo "Stopped direct container ${SYSTEM}; files remain." >&2
        return 0
    fi
    if [ -n "$JOBID" ] && [ -n "$(raw_job_state || true)" ]; then
        local SCANCEL_ARGS=("$JOBID")
        [ -z "$CLUSTER" ] || SCANCEL_ARGS=(--clusters="$CLUSTER" "$JOBID")
        scancel "${SCANCEL_ARGS[@]}" || true
    fi
    stop_relay
    printf '%s\n' cancelled > "$STATE/cancelled.tmp.$$"
    mv -f "$STATE/cancelled.tmp.$$" "$STATE/cancelled"
    echo "Cancelled job ${JOB_ID} (Slurm ${JOBID:-none}); durable files remain." >&2
}

detach_job() {
    if [ "$MODE" = direct ]; then
        echo "Detached direct target ${SYSTEM}; the container keeps running." >&2
        return 0
    fi
    stop_relay
    echo "Detached job ${JOB_ID} (Slurm ${JOBID:-none}); it keeps running under Slurm." >&2
}

# --- Action dispatch --------------------------------------------------------
case "$ACTION" in
    stop|close)
        if [ "$MODE" = direct ]; then
            bash "$CONTAINER_SCRIPT" stop
            echo "Stopped direct container ${SYSTEM}; files remain." >&2
        else
            # Slurm teardown detaches, it never scancels.  Cancellation is a
            # separate explicit operation.
            detach_job
        fi
        exit 0
        ;;
    detach|teardown)
        detach_job
        exit 0
        ;;
    cancel)
        cancel_job
        exit 0
        ;;
    status)
        if [ "$MODE" = direct ]; then
            ENDPOINT="$(resolve_container_endpoint || true)"
            [ -n "$ENDPOINT" ] || ENDPOINT="127.0.0.1:$CONTAINER_PORT"
            printf 'mode: direct\nstate: %s\nendpoint: %s\n' \
                "$([ -s "$STATE/container.endpoint" ] && echo running || echo stopped)" \
                "$ENDPOINT"
            exit 0
        fi
        printf 'job_id: %s\njobid: %s\ncluster: %s\nstate: %s\n' \
            "$JOB_ID" "${JOBID:-none}" "${CLUSTER:-none}" "$(mapped_state)"
        if [ -s "$STATE/ready" ]; then
            printf 'node: %s\n' "$(head -n 1 "$STATE/ready")"
        fi
        STATUS_PORT="$(relay_ready_port 2>/dev/null || true)"
        if [ -z "$STATUS_PORT" ] && [ -s "$STATE/relay.port" ]; then
            STATUS_PORT="$(head -n 1 "$STATE/relay.port" 2>/dev/null || true)"
        fi
        if [ -z "$STATUS_PORT" ] && [ -n "$FORWARD_PORT_OVERRIDE" ]; then
            STATUS_PORT="$FORWARD_PORT_OVERRIDE"
        fi
        [ -n "$STATUS_PORT" ] || STATUS_PORT=none
        printf 'endpoint: 127.0.0.1:%s\n' "$STATUS_PORT"
        exit 0
        ;;
    observe)
        observe_job
        exit 0
        ;;
    submit)
        if [ "$HAVE_SLURM" != 1 ]; then
            apply_provision_env
            select_home
            ensure_container
            printf '%s\n' direct > "$STATE/mode"
            direct_provision
            exit 0
        fi
        submit_allocation
        exit 0
        ;;
    attach)
        attach_job
        exit 0
        ;;
esac

# --- Legacy provision = submit + attach ------------------------------------
if [ "$ACTION" = provision ]; then
    apply_provision_env
    select_home
    export COMPUTEMCP_HOST_HOME="$HOST_HOME"
    ensure_container

    if [ "$HAVE_SLURM" != 1 ]; then
        printf '%s\n' direct > "$STATE/mode"
        direct_provision
        exit 0
    fi
    printf '%s\n' slurm > "$STATE/mode"

    case "$(raw_job_state || true)" in
        PENDING|RUNNING|CONFIGURING) echo "Reusing tracked job $JOBID ($(raw_job_state))." >&2 ;;
        COMPLETING|SUSPENDED|STOPPED) echo "Job $JOBID cannot be reused; stop it first." >&2; exit 1 ;;
        *) JOBID=""; CLUSTER="" ;;
    esac

    report_job() {
        echo "Allocation state: job $JOB_ID slurm ${JOBID:-none} cluster ${CLUSTER:-none}; settings ${STATE}/srun-$SYSTEM.settings" >&2
    }
    trap 'STATUS=$?; if (( STATUS != 0 )); then report_job; fi' EXIT

    if [ -z "$JOBID" ]; then
        stop_relay
        # Re-use the submit stage without re-running ensure_container.
        SETTINGS="$STATE/srun-$SYSTEM.settings"
        HELPER_ARGS=(--parsable --job-name="$NAME")
        sbatch_has() {
            local WANTED="$1" ARG
            for ARG in "${SBATCH_ARGS[@]}"; do
                case "$ARG" in "$WANTED"|"$WANTED"=*) return 0 ;; esac
            done
            return 1
        }
        sbatch_has --output || sbatch_has -o || HELPER_ARGS+=(--output="$STATE/slurm-%j.log")
        sbatch_has --error  || sbatch_has -e || HELPER_ARGS+=(--error="$STATE/slurm-%j.log")
        write_settings "$SETTINGS"
        RESULT="$(sbatch "${SBATCH_ARGS[@]}" "${HELPER_ARGS[@]}" "$JOB_SCRIPT" "$SETTINGS")"
        JOBID="${RESULT%%;*}"
        [[ "$JOBID" =~ ^[0-9]+$ ]] || { echo "Invalid sbatch result: $RESULT" >&2; exit 1; }
        CLUSTER=""
        if [[ "$RESULT" == *';'* ]]; then CLUSTER="${RESULT#*;}"; fi
        printf '%s\n' "$JOBID" > "$STATE/jobid"
        printf '%s\n' "$CLUSTER" > "$STATE/cluster"
        printf '%s\n' "$ROOT_MODE" > "$STATE/root_mode"
        printf '%s\n' "$JOB_KIND" > "$STATE/job_kind"
        [ -z "$CLEAN_HOME" ] || printf '%s\n' "$CLEAN_HOME" > "$STATE/clean_home"
        printf '%s\n' "$HOST_HOME" > "$STATE/home"
        printf '%s\n' "$NAME" > "$STATE/instance"
        printf '%s\n' "$RUNTIME" > "$STATE/container.runtime"
        echo "Submitted job $JOBID (cluster ${CLUSTER:-default}). Settings: $SETTINGS" >&2
    fi

    # Attach inline (submit+attach for the legacy synchronous interface).
    attach_job
    trap - EXIT
    exit 0
fi

# --- Interactive shell into the tracked allocation -------------------------
if [ "$ACTION" = shell ]; then
    [ -n "$JOBID" ] || { echo 'No tracked job; run provision first.' >&2; exit 1; }
    NODE=""
    [ -s "$STATE/ready" ] && read -r NODE < "$STATE/ready"
    [ -n "$NODE" ] || { echo 'Tracked job has no ready node.' >&2; exit 1; }
    exec srun --jobid="$JOBID" --overlap --nodes=1 --ntasks=1 --cpus-per-task=1 \
        --cpu-bind="$CPU_BIND" --nodelist="$NODE" --pty \
        bash "$CONTAINER_SCRIPT" shell
fi

echo "Unhandled action: $ACTION" >&2
exit 2
