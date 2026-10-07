#!/usr/bin/env bash
# computeMCP Slurm provisioning entry point, run on the Slurm login node.
#
# Actions: provision (default) | stop | close | shell | status
#
# The gateway invokes this with a fully populated COMPUTEMCP_* environment (see
# the design doc, "Gateway-to-provisioner interface").  Every value has a
# documented fallback so the helper also works for manual/legacy invocations.
#
# Stages are strictly separate: SBATCH_ARGS request the allocation, SRUN_ARGS
# launch the container job step inside it.  They are never copied between each
# other or merged.
#
# SRUN_ARGS transport: the helper writes a per-job settings file and passes its
# path to the batch script as a positional argument (``sbatch ... job.sh
# SETTINGS``).  Slurm passes positional arguments to the batch script verbatim,
# so this survives ``--export=NONE`` and any custom export policy without the
# helper touching the user's ``--export``.  The batch script sources that file
# to obtain the SRUN_ARGS array and the container configuration.  An
# ``--export`` based mechanism was rejected because it can silently clobber or
# be clobbered by the user's export policy, which the design forbids.
set -euo pipefail
umask 077

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER_SCRIPT="$SCRIPT_DIR/computemcp-container.sh"
JOB_SCRIPT="$SCRIPT_DIR/computemcp-job.sh"
RELAY="$SCRIPT_DIR/computemcp-relay.py"

ACTION="${1:-provision}"
case "$ACTION" in
    provision|stop|close|shell|status) ;;
    *) echo "Usage: bash $0 [provision|stop|close|shell|status]" >&2; exit 2 ;;
esac

# --- Gateway environment and fallbacks -------------------------------------
SYSTEM="${COMPUTEMCP_SYSTEM:-computemcp}"
if ! { [[ "$SYSTEM" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] && [ "$SYSTEM" != . ] && [ "$SYSTEM" != .. ]; }; then
    echo "Invalid system name: $SYSTEM" >&2
    exit 2
fi
STORAGE_ROOT="${COMPUTEMCP_STORAGE_ROOT:-$HOME/.local/share/computemcp}"
# Accept a literal $HOME/~ prefix from the gateway and expand it here, so the
# operator can configure $HOME/computemcp without knowing the remote home path.
if [[ "$STORAGE_ROOT" =~ ^\$HOME(/|$) ]]; then
    STORAGE_ROOT="$HOME${STORAGE_ROOT#\$HOME}"
elif [[ "$STORAGE_ROOT" =~ ^~(/|$) ]]; then
    STORAGE_ROOT="$HOME${STORAGE_ROOT#\~}"
fi
SYSTEM_DIR="$STORAGE_ROOT/$SYSTEM"
STATE="${COMPUTEMCP_STATE_DIR:-$SYSTEM_DIR/state}"
SANDBOX="${COMPUTEMCP_SANDBOX_DIR:-$SYSTEM_DIR/sandbox}"
HOST_HOME="${COMPUTEMCP_HOST_HOME:-$SYSTEM_DIR/home}"
RUNTIME="${COMPUTEMCP_CONTAINER_RUNTIME:-}"
IMAGE="${COMPUTEMCP_IMAGE:-}"
GPU_VENDORS="${COMPUTEMCP_GPU_VENDORS:-}"
CONTAINER_PORT="${COMPUTEMCP_CONTAINER_PORT:-2222}"
SSH_WAIT_SECONDS="${COMPUTEMCP_SSH_WAIT_SECONDS:-120}"
PORT="${COMPUTEMCP_FORWARD_PORT:-2200}"
WAIT_SECONDS="${COMPUTEMCP_WAIT_SECONDS:-900}"
NODES="${COMPUTEMCP_NODES:-1}"

# Re-export the resolved layout so computemcp-container.sh uses the same paths
# on the login node and the compute node.
export COMPUTEMCP_SYSTEM="$SYSTEM"
export COMPUTEMCP_STATE_DIR="$STATE"
export COMPUTEMCP_SANDBOX_DIR="$SANDBOX"
export COMPUTEMCP_HOST_HOME="$HOST_HOME"
export COMPUTEMCP_CONTAINER_RUNTIME="$RUNTIME"
export COMPUTEMCP_IMAGE="$IMAGE"
export COMPUTEMCP_GPU_VENDORS="$GPU_VENDORS"
export COMPUTEMCP_CONTAINER_PORT="$CONTAINER_PORT"
export COMPUTEMCP_SSH_WAIT_SECONDS="$SSH_WAIT_SECONDS"
export COMPUTEMCP_SSH_USER="${COMPUTEMCP_SSH_USER:-agent}"

# Resolve the SSH public key to install in the container.  The gateway is
# expected to inject COMPUTEMCP_SSH_PUBLIC_KEY (or a file path); when it does
# not, an already configured authorized_keys is reused.
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

# The relay's data step binding.  Extract a cpu-bind from the rendered SRUN_ARGS
# when present; otherwise fall back to "none", matching the reference.
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
# provided no COMPUTEMCP_SBATCH_ARGS, so it cannot double-request resources
# alongside the gateway-rendered deck.  The variable names match the manual
# overrides of the reference scripts (COMPUTEMCP_ACCOUNT, COMPUTEMCP_PARTITION,
# ...).  It deliberately does not read the plan variables (CPUS_PER_NODE,
# GPUS_PER_NODE, MEMORY_PER_NODE_MIB): those describe the calculated plan,
# which the design says must NOT be silently turned into a scheduler request.
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
(( CONTAINER_PORT >= 1024 && CONTAINER_PORT <= 65535 && PORT >= 1024 && PORT <= 65535 &&
   SSH_WAIT_SECONDS > 0 && WAIT_SECONDS > 0 )) || {
    echo 'Invalid timeout or port configuration.' >&2
    exit 2
}
[ "$PORT" != "$CONTAINER_PORT" ] || { echo 'Forward and container ports must differ.' >&2; exit 2; }

for CMD in sbatch squeue scancel python3 flock; do command -v "$CMD" >/dev/null; done
for FILE in "$CONTAINER_SCRIPT" "$RELAY" "$JOB_SCRIPT"; do
    [ -r "$FILE" ] || { echo "Missing/unreadable helper: $FILE" >&2; exit 1; }
done

mkdir -p "$STATE"

# --- Locking and job helpers ------------------------------------------------
exec 9>"$STATE/provision.lock"
flock -w "$WAIT_SECONDS" 9

JOBID="$(cat "$STATE/jobid" 2>/dev/null || true)"
CLUSTER="$(cat "$STATE/cluster" 2>/dev/null || true)"
[[ -z "$JOBID" || "$JOBID" =~ ^[0-9]+$ ]] || { echo 'Invalid tracked job ID.' >&2; exit 1; }
[[ -z "$CLUSTER" || "$CLUSTER" =~ ^[A-Za-z0-9._-]+$ ]] || { echo 'Invalid tracked cluster.' >&2; exit 1; }

SQUEUE_ARGS=(--noheader --user "$(id -un)" --format '%i|%T')
[ -z "$CLUSTER" ] || SQUEUE_ARGS+=(--clusters="$CLUSTER")
job_state() {
    squeue "${SQUEUE_ARGS[@]}" 2>/dev/null |
        awk -F '|' -v job="$JOBID" '$1 == job {print $2}'
}
relay_running() {
    local PID
    PID="$(cat "$STATE/relay.pid" 2>/dev/null || true)"
    [[ "$PID" =~ ^[0-9]+$ ]] && kill -0 "$PID" 2>/dev/null &&
        [ -r "/proc/$PID/cmdline" ] &&
        tr '\0' '\n' < "/proc/$PID/cmdline" |
        grep -Fxq -e "$RELAY" -e "$STATE/relay.py"
}
stop_relay() {
    if relay_running; then
        kill "$(cat "$STATE/relay.pid")"
        for (( I = 0; I < 50; I++ )); do relay_running || break; sleep 0.1; done
    fi
    rm -f "$STATE/relay.pid" "$STATE/tunnel-job" "$STATE/relay.ready"
}

# --- Non-provision actions --------------------------------------------------
if [ "$ACTION" = stop ] || [ "$ACTION" = close ]; then
    SCANCEL_ARGS=("$JOBID")
    [ -z "$CLUSTER" ] || SCANCEL_ARGS=(--clusters="$CLUSTER" "$JOBID")
    if [ -n "$JOBID" ] && [ -n "$(job_state)" ]; then
        scancel "${SCANCEL_ARGS[@]}"
    fi
    stop_relay
    echo "Stopped tracked job ${JOBID:-none} and relay; container files remain." >&2
    exit 0
fi

if [ "$ACTION" = status ]; then
    printf 'jobid: %s\ncluster: %s\nstate: %s\n' \
        "${JOBID:-none}" "${CLUSTER:-none}" "$(job_state || true)"
    if [ -s "$STATE/ready-$JOBID" ]; then
        printf 'node: %s\n' "$(cat "$STATE/ready-$JOBID")"
    fi
    printf 'endpoint: 127.0.0.1:%s\n' "$PORT"
    exit 0
fi

# --- Auto-build the container on the login node, then configure the SSH key -
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
    case "$RUNTIME" in
        apptainer) [ ! -d "$SANDBOX" ] ;;
        docker)    ! docker image inspect "computemcp-${SYSTEM,,}:latest" >/dev/null 2>&1 ;;
        *) return 1 ;;
    esac
}

ensure_container() {
    runtime_tool
    [ -n "$IMAGE" ] || { echo 'COMPUTEMCP_IMAGE is required.' >&2; exit 1; }
    resolve_ssh_key
    if container_missing; then
        echo "Container missing; building it on the login node." >&2
        bash "$CONTAINER_SCRIPT" build
    else
        echo "Container present; configuring it on the login node." >&2
        bash "$CONTAINER_SCRIPT" configure
    fi
}

# --- Per-job SRUN settings file --------------------------------------------
write_settings() {
    local FILE="$1" ARG
    {
        printf '# computeMCP per-job settings for job %s\n' "$JOBID"
        printf '# Sourced by computemcp-job.sh; generated by computemcp-provision.sh.\n'
        printf 'export COMPUTEMCP_SYSTEM=%q\n' "$SYSTEM"
        printf 'export COMPUTEMCP_STATE_DIR=%q\n' "$STATE"
        printf 'export COMPUTEMCP_SANDBOX_DIR=%q\n' "$SANDBOX"
        printf 'export COMPUTEMCP_HOST_HOME=%q\n' "$HOST_HOME"
        printf 'export COMPUTEMCP_CONTAINER_RUNTIME=%q\n' "$RUNTIME"
        printf 'export COMPUTEMCP_IMAGE=%q\n' "$IMAGE"
        printf 'export COMPUTEMCP_GPU_VENDORS=%q\n' "$GPU_VENDORS"
        printf 'export COMPUTEMCP_CPUS_PER_NODE=%q\n' "${COMPUTEMCP_CPUS_PER_NODE:-}"
        printf 'export COMPUTEMCP_GPUS_PER_NODE=%q\n' "${COMPUTEMCP_GPUS_PER_NODE:-}"
        printf 'export COMPUTEMCP_MEMORY_PER_NODE_MIB=%q\n' "${COMPUTEMCP_MEMORY_PER_NODE_MIB:-}"
        printf 'export COMPUTEMCP_CONTAINER_PORT=%q\n' "$CONTAINER_PORT"
        printf 'export COMPUTEMCP_SSH_WAIT_SECONDS=%q\n' "$SSH_WAIT_SECONDS"
        printf 'export COMPUTEMCP_SSH_USER=%q\n' "${COMPUTEMCP_SSH_USER:-agent}"
        printf 'export COMPUTEMCP_SSH_PUBLIC_KEY=%q\n' "${COMPUTEMCP_SSH_PUBLIC_KEY:-}"
        printf 'export COMPUTEMCP_CPU_BIND=%q\n' "$CPU_BIND"
        printf 'export COMPUTEMCP_SRUN_ARGS=%q\n' "${COMPUTEMCP_SRUN_ARGS:-}"
    } > "$FILE.tmp"
    mv -- "$FILE.tmp" "$FILE"
    chmod 600 "$FILE"
}

# --- Submit the allocation --------------------------------------------------
if [ "$ACTION" = provision ]; then
    ensure_container

    case "$(job_state)" in
        PENDING|RUNNING|CONFIGURING) echo "Reusing tracked job $JOBID ($(job_state))." >&2 ;;
        COMPLETING|SUSPENDED|STOPPED) echo "Job $JOBID cannot be reused; stop it first." >&2; exit 1 ;;
        *) JOBID=""; CLUSTER="" ;;
    esac

    # Report, never silently drop, a half-started allocation.  Installed before
    # submission so a failure after sbatch (or jobid write) still reports it.
    report_job() {
        echo "Allocation state: job ${JOBID:-none} cluster ${CLUSTER:-none}; settings ${STATE}/srun-$SYSTEM.settings" >&2
    }
    trap 'STATUS=$?; if (( STATUS != 0 )); then report_job; fi' EXIT

    if [ -z "$JOBID" ]; then
        stop_relay
        SETTINGS="$STATE/srun-$SYSTEM.settings"        HELPER_ARGS=(--parsable --job-name="computemcp-$SYSTEM")
        sbatch_has() {
            local WANTED="$1" ARG
            for ARG in "${SBATCH_ARGS[@]}"; do
                case "$ARG" in "$WANTED"|"$WANTED"=*) return 0 ;; esac
            done
            return 1
        }
        sbatch_has --output || sbatch_has -o || HELPER_ARGS+=(--output="$STATE/slurm-%j.log")
        sbatch_has --error  || sbatch_has -e || HELPER_ARGS+=(--error="$STATE/slurm-%j.log")
        # Write the settings file BEFORE sbatch: a fast scheduler can start the
        # job step before a post-submission write lands, and job.sh sources it.
        write_settings "$SETTINGS"
        RESULT="$(sbatch "${SBATCH_ARGS[@]}" "${HELPER_ARGS[@]}" "$JOB_SCRIPT" "$SETTINGS")"
        JOBID="${RESULT%%;*}"
        [[ "$JOBID" =~ ^[0-9]+$ ]] || { echo "Invalid sbatch result: $RESULT" >&2; exit 1; }
        if [[ "$RESULT" == *';'* ]]; then CLUSTER="${RESULT#*;}"; fi
        printf '%s\n' "$JOBID" > "$STATE/jobid"
        printf '%s\n' "$CLUSTER" > "$STATE/cluster"
        echo "Submitted job $JOBID (cluster ${CLUSTER:-default}). Settings: $SETTINGS" >&2
    fi

    END=$((SECONDS + WAIT_SECONDS))
    NODE=""
    while (( SECONDS < END )); do
        case "$(job_state)" in
            RUNNING)
                if [ -s "$STATE/ready-$JOBID" ]; then read -r NODE < "$STATE/ready-$JOBID"; break; fi
                ;;
            PENDING|CONFIGURING) ;;
            *)
                tail -n 60 "$STATE/slurm-$JOBID.log" >&2 2>/dev/null || true
                report_job
                exit 1
                ;;
        esac
        sleep 5
    done
    [ -n "$NODE" ] || { echo "Timed out waiting for job $JOBID; it stays tracked for the next call." >&2; exit 1; }
    [[ "$NODE" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] || { echo "Invalid node name: $NODE" >&2; exit 1; }

    WANTED="$JOBID $NODE $PORT $CONTAINER_PORT"
    if ! relay_running || [ "$(cat "$STATE/tunnel-job" 2>/dev/null || true)" != "$WANTED" ]; then
        stop_relay
        nohup python3 "$RELAY" "$PORT" "$JOBID" "$NODE" "$STATE/relay.ready" \
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
        printf '%s\n' "$WANTED" > "$STATE/tunnel-job"
    fi

    READY=0
    for (( I = 0; I < 3; I++ )); do
        if python3 "$RELAY" --check "$PORT"; then READY=1; break; fi
        relay_running || { echo "Relay failed; see $STATE/relay.log" >&2; exit 1; }
        sleep 1
    done
    [ "$READY" = 1 ] || { echo 'Container SSH endpoint is unavailable.' >&2; exit 1; }

    trap - EXIT
    echo "Ready: job $JOBID on $NODE, local port $PORT." >&2
    printf 'ENDPOINT 127.0.0.1:%s\n' "$PORT"
    exit 0
fi

# --- Interactive shell into the tracked allocation -------------------------
if [ "$ACTION" = shell ]; then
    [ -n "$JOBID" ] || { echo 'No tracked job; run provision first.' >&2; exit 1; }
    NODE=""
    [ -s "$STATE/ready-$JOBID" ] && read -r NODE < "$STATE/ready-$JOBID"
    [ -n "$NODE" ] || { echo 'Tracked job has no ready node.' >&2; exit 1; }
    exec srun --jobid="$JOBID" --overlap --nodes=1 --ntasks=1 --cpus-per-task=1 \
        --cpu-bind="$CPU_BIND" --nodelist="$NODE" --pty \
        bash "$CONTAINER_SCRIPT" shell
fi
