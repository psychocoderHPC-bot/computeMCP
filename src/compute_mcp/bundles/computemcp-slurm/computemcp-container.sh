#!/usr/bin/env bash
# computeMCP container runtime dispatcher.
#
# Actions: build | configure | update | start | stop | close | shell | fingerprint | endpoint
#
# The runtime is selected by COMPUTEMCP_CONTAINER_RUNTIME (apptainer|docker).
# Every path, name and resource comes from the COMPUTEMCP_* environment that
# the gateway injects; nothing is tied to a specific site.  This file is the
# only place that knows how to build/configure/start a container.  The Slurm
# batch job calls it with "start" on the compute node; the provisioning helper
# calls it with "build"/"configure" on the login node.
set -euo pipefail
umask 077

ACTION="${1:-start}"

case "$ACTION" in
    build|configure|update|start|stop|close|shell|fingerprint|endpoint) ;;
    *)
        echo "Usage: bash $0 [build|configure|update|start|stop|close|shell|fingerprint|endpoint]" >&2
        exit 2
        ;;
esac

# --- Configuration from the gateway environment ---------------------------
SYSTEM="${COMPUTEMCP_SYSTEM:-computemcp}"
STORAGE_ROOT="${COMPUTEMCP_STORAGE_ROOT:-$HOME/.local/share/computemcp}"
# Expand a literal $HOME/~ prefix passed by the gateway.
if [[ "$STORAGE_ROOT" =~ ^\$HOME(/|$) ]]; then
    STORAGE_ROOT="$HOME${STORAGE_ROOT#\$HOME}"
elif [[ "$STORAGE_ROOT" =~ ^~(/|$) ]]; then
    STORAGE_ROOT="$HOME${STORAGE_ROOT#\~}"
fi
SYSTEM_DIR="$STORAGE_ROOT/$SYSTEM"
SANDBOX="${COMPUTEMCP_SANDBOX_DIR:-$SYSTEM_DIR/sandbox}"
HOST_HOME="${COMPUTEMCP_HOST_HOME:-$SYSTEM_DIR/home}"
STATE="$SYSTEM_DIR/state"

# Runtime name.  Docker is daemon-global and Apptainer instances are per-host,
# so the target name alone collides for two users on the same node.  The remote
# numeric uid is the only discriminator available on the remote host and is
# stable across login and compute nodes; include it.  COMPUTEMCP_CONTAINER_NAME
# is an optional operator/test override and is validated exactly like the
# derived value.
REMOTE_UID="$(id -u 2>/dev/null || true)"
if [ -n "$REMOTE_UID" ]; then
    NAME="${COMPUTEMCP_CONTAINER_NAME:-computemcp-$REMOTE_UID-$SYSTEM}"
else
    NAME="${COMPUTEMCP_CONTAINER_NAME:-computemcp-$SYSTEM}"
fi
if ! { [[ "$NAME" =~ ^[A-Za-z0-9][A-Za-z0-9_.-]*$ ]] && [ "$NAME" != . ] && [ "$NAME" != .. ]; }; then
    echo "Invalid container name: $NAME" >&2
    exit 2
fi

RUNTIME="${COMPUTEMCP_CONTAINER_RUNTIME:-}"
IMAGE="${COMPUTEMCP_IMAGE:-}"
GPU_VENDORS="${COMPUTEMCP_GPU_VENDORS:-}"
CONTAINER_PORT="${COMPUTEMCP_CONTAINER_PORT:-2222}"
SSH_WAIT_SECONDS="${COMPUTEMCP_SSH_WAIT_SECONDS:-120}"

# Docker-only defaults.  The gateway never needs to know these.
SSH_USER="${COMPUTEMCP_SSH_USER:-agent}"
if ! { [[ "$SSH_USER" =~ ^[a-z_][a-z0-9_-]*$ ]] && [ "$SSH_USER" != root ]; }; then
    echo "Invalid COMPUTEMCP_SSH_USER: $SSH_USER" >&2
    exit 2
fi
DOCKER_IMAGE="${NAME,,}:latest"
DOCKER_RESTART="unless-stopped"

# Legacy/manual fallbacks used only when the gateway DID NOT provide the
# corresponding COMPUTEMCP_* variable.  They never override gateway values.
LEGACY_CPUS="${COMPUTEMCP_CPUS:-1}"
LEGACY_MEMORY="${COMPUTEMCP_MEMORY:-20G}"

if ! { [[ "$SYSTEM" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$ ]] && [ "$SYSTEM" != . ] && [ "$SYSTEM" != .. ]; }; then
    echo "Invalid system name: $SYSTEM" >&2
    exit 2
fi
case "$RUNTIME" in
    apptainer|docker) ;;
    "") echo 'COMPUTEMCP_CONTAINER_RUNTIME must be apptainer or docker.' >&2; exit 2 ;;
    *) echo "Unsupported container runtime: $RUNTIME" >&2; exit 2 ;;
esac
if ! { [[ "$CONTAINER_PORT" =~ ^[0-9]+$ ]] && (( CONTAINER_PORT >= 1024 && CONTAINER_PORT <= 65535 )); }; then
    echo "Invalid container port: $CONTAINER_PORT" >&2
    exit 2
fi
if ! { [[ "$SSH_WAIT_SECONDS" =~ ^[0-9]+$ ]] && (( SSH_WAIT_SECONDS > 0 )); }; then
    echo "Invalid SSH wait seconds: $SSH_WAIT_SECONDS" >&2
    exit 2
fi

mkdir -p "$SYSTEM_DIR" "$STATE"

# Normalize the comma-separated vendor list into space-separated tokens.
VENDORS=()
if [ -n "$GPU_VENDORS" ]; then
    IFS=',' read -r -a VENDORS <<< "$GPU_VENDORS"
fi
has_vendor() {
    local WANTED="$1" VENDOR
    for VENDOR in "${VENDORS[@]}"; do
        [ "$VENDOR" = "$WANTED" ] && return 0
    done
    return 1
}

# Wait until a TCP endpoint serves an SSH banner; used by start.
wait_for_banner() {
    local HOST="$1" PORT="$2"
    python3 - "$HOST" "$PORT" "$SSH_WAIT_SECONDS" <<'PY'
import socket
import sys
import time

host, port, timeout = sys.argv[1], int(sys.argv[2]), int(sys.argv[3])
deadline = time.monotonic() + timeout
while time.monotonic() < deadline:
    try:
        with socket.create_connection((host, port), timeout=2) as conn:
            with conn.makefile("rb") as stream:
                for _ in range(20):
                    line = stream.readline(4096)
                    if line.startswith(b"SSH-"):
                        raise SystemExit(0)
                    if not line:
                        break
    except OSError:
        pass
    time.sleep(1)
raise SystemExit("Container SSH server did not become ready on port %d" % port)
PY
}

install_authorized_key() {
    [ -n "${COMPUTEMCP_SSH_PUBLIC_KEY:-}" ] || {
        echo 'COMPUTEMCP_SSH_PUBLIC_KEY is required to install the SSH key.' >&2
        exit 1
    }
    local AUTH="$HOST_HOME/.ssh/authorized_keys" KEY="$COMPUTEMCP_SSH_PUBLIC_KEY"
    mkdir -p "$HOST_HOME"
    chmod 700 "$HOST_HOME"
    install -d -m 700 "$HOST_HOME/.ssh"
    if [ -e "$AUTH" ] && ! cmp -s "$AUTH" <(printf '%s\n' "$KEY"); then
        local BACKUP
        BACKUP="$(mktemp "$HOST_HOME/.ssh/authorized_keys.backup.XXXXXX")"
        cp -- "$AUTH" "$BACKUP"
        chmod 600 "$BACKUP"
        echo "Previous authorized_keys backed up to $BACKUP" >&2
    fi
    printf '%s\n' "$KEY" > "$AUTH"
    chmod 600 "$AUTH"
}

# ---------------------------------------------------------------------------
# Apptainer runtime
# ---------------------------------------------------------------------------
apptainer_build() {
    command -v apptainer >/dev/null
    [ -n "$IMAGE" ] || { echo 'COMPUTEMCP_IMAGE is required to build a sandbox.' >&2; exit 1; }
    [ "${IMAGE#docker://}" != "$IMAGE" ] || { echo 'Apptainer IMAGE must use docker://.' >&2; exit 2; }
    if [ -e "$SANDBOX" ]; then
        echo "Sandbox exists; use configure/start, not build." >&2
        exit 1
    fi
    local DEF
    DEF="$(mktemp)"
    trap 'rm -f -- "$DEF"' EXIT
    printf 'Bootstrap: docker\nFrom: %s\n' "${IMAGE#docker://}" > "$DEF"
    cat >> "$DEF" <<'DEF'
%post
    set -eu
    export DEBIAN_FRONTEND=noninteractive
    export FAKEROOTDONTTRYCHOWN=1
    printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d
    chmod 755 /usr/sbin/policy-rc.d
    printf 'APT::Sandbox::User "root";\n' > /etc/apt/apt.conf.d/99-root-mapped

    if ! getent group _ssh >/dev/null; then
        SSH_GID="$(
            awk -F: '
                { USED[$3] = 1 }
                END {
                    for (GID = 100; GID < 1000; GID++) {
                        if (!(GID in USED)) { print GID; exit }
                    }
                    exit 1
                }
            ' /etc/group
        )"
        printf '_ssh:x:%s:\n' "$SSH_GID" >> /etc/group
    fi
    if [ -f /etc/gshadow ] && ! grep -q '^_ssh:' /etc/gshadow; then
        printf '_ssh:!::\n' >> /etc/gshadow
    fi
    getent group _ssh
    apt-get update
    apt-get install -y --no-install-recommends fakeroot

    env -u LD_PRELOAD -u LD_LIBRARY_PATH -u FAKEROOTKEY \
        FAKEROOTDONTTRYCHOWN=1 \
        /usr/bin/fakeroot-sysv /bin/bash -eu <<'CONTAINER_SETUP'
export DEBIAN_FRONTEND=noninteractive
apt-get install -y --no-install-recommends \
    dropbear-bin openssh-sftp-server openssh-client ca-certificates \
    git curl build-essential cmake ninja-build python3
mkdir -p /root /home/ubuntu /etc/dropbear /.singularity.d/libs /usr/libexec
touch /usr/bin/nvidia-smi
test -x /usr/lib/openssh/sftp-server
for DESTINATION in /usr/lib/sftp-server /usr/libexec/sftp-server; do
    if [ ! -e "$DESTINATION" ] && [ ! -L "$DESTINATION" ]; then
        ln -s /usr/lib/openssh/sftp-server "$DESTINATION"
    fi
done
dropbearkey -t ed25519 -f /etc/dropbear/dropbear_ed25519_host_key
chmod 600 /etc/dropbear/dropbear_ed25519_host_key
CONTAINER_SETUP
DEF
    apptainer build --fakeroot --sandbox "$SANDBOX" "$DEF"
    rm -f -- "$DEF"
    trap - EXIT
}

apptainer_configure() {
    command -v apptainer >/dev/null
    [ -d "$SANDBOX" ] || { echo "Missing sandbox; use build first." >&2; exit 1; }
    command -v python3 >/dev/null
    # Edit the actual sandbox files, not Apptainer's runtime /etc/passwd mount.
    # No host account files are changed. Existing Ubuntu UID/GID are retained.
    # The pre-existing ``ubuntu`` account is renamed to the gateway's login
    # account (``COMPUTEMCP_SSH_USER``, default ``agent``) so the same account
    # the gateway dials exists inside the sandbox.  Keeping one account and
    # renaming it (rather than adding a second) avoids a duplicate UID/GID and
    # every downstream reference (home, shell, authorized_keys, startscript)
    # follows the renamed account.  The emitted account line goes to stderr so
    # the helper keeps stdout clean for the gateway's ENDPOINT parse.
    python3 - "$SANDBOX" "$CONTAINER_PORT" "$SSH_USER" >&2 <<'CONFIGURE'
import os
import shutil
import stat
import sys
import tempfile
from pathlib import Path

root = Path(sys.argv[1]).resolve(strict=True)
port = int(sys.argv[2])
user = sys.argv[3]
if not __import__("re").fullmatch(r"[a-z_][a-z0-9_-]*", user) or user == "root":
    raise SystemExit(f"Invalid container user: {user!r}")
home = "/home/" + user
shell = "/usr/local/bin/computemcp-" + user + "-shell"


def target(relative):
    path = root / relative
    resolved = path.resolve(strict=False)
    if not resolved.is_relative_to(root) or path.is_symlink():
        raise SystemExit(f"Refusing configuration through symlink: {path}")
    return path


def write(relative, content, mode=0o644):
    path = target(relative)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".computemcp-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            stream.write(content)
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


for relative in (
    "usr/bin/fakeroot", "usr/sbin/dropbear", "usr/lib/openssh/sftp-server"
):
    if not os.access(root / relative, os.X_OK):
        raise SystemExit(f"Missing executable: {relative}")
if not target("etc/dropbear/dropbear_ed25519_host_key").stat().st_size:
    raise SystemExit("Missing Dropbear host key")

passwd = target("etc/passwd")
rows = [line.split(":") for line in passwd.read_text().splitlines()]
accounts = [row for row in rows if row[0] == "ubuntu"]
if len(accounts) != 1 or len(accounts[0]) != 7:
    raise SystemExit("Expected exactly one existing ubuntu account")
account = accounts[0]
if account[2] == "0":
    raise SystemExit("Refusing ubuntu account with UID 0")

for relative in ("etc/passwd", "etc/shadow"):
    path = target(relative)
    backup = target(relative + ".computemcp-backup")
    if path.exists() and not backup.exists():
        shutil.copy2(path, backup)

# Rename the sandbox account to the gateway's login account, keeping UID/GID.
account[0] = user
account[1] = "*"  # Public-key access only; no usable password.
account[5] = home
account[6] = shell
write("etc/passwd", "".join(":".join(row) + "\n" for row in rows),
      stat.S_IMODE(passwd.stat().st_mode))
shadow = target("etc/shadow")
if shadow.exists():
    rows = [line.split(":") for line in shadow.read_text().splitlines()]
    for row in rows:
        if row[0] == "ubuntu" and len(row) > 1:
            row[0] = user
            row[1] = "*"
    write("etc/shadow", "".join(":".join(row) + "\n" for row in rows),
          stat.S_IMODE(shadow.stat().st_mode))

for relative in ("root", "home/" + user, "usr/local/bin", "usr/libexec",
                 ".singularity.d/libs", "run"):
    target(relative).mkdir(parents=True, exist_ok=True)

write(shell.lstrip("/"), '''#!/bin/sh
export HOME=%s USER=%s LOGNAME=%s
export LANG=C.UTF-8 LC_ALL=C.UTF-8
export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
exec env -u LD_PRELOAD -u FAKEROOTKEY -u FAKEROOTUID -u FAKEROOTGID \\
    -u FAKEROOTEUID -u FAKEROOTEGID FAKEROOTDONTTRYCHOWN=1 \\
    /usr/bin/fakeroot /bin/bash "$@"
''' % (home, user, user), 0o755)
shells = target("etc/shells")
content = shells.read_text() if shells.exists() else ""
if shell not in content.splitlines():
    write("etc/shells", content.rstrip("\n") + "\n" + shell + "\n")
write("etc/apt/apt.conf.d/99-root-mapped", 'APT::Sandbox::User "root";\n')

for relative in ("usr/lib/sftp-server", "usr/libexec/sftp-server"):
    path = root / relative
    # Absolute symlinks point into the container when it is running.
    if not path.exists() and not path.is_symlink():
        path.symlink_to("/usr/lib/openssh/sftp-server")

write(".singularity.d/startscript", '''#!/bin/sh
set -eu
test -s %s/.ssh/authorized_keys
export LANG=C.UTF-8 LC_ALL=C.UTF-8
# User namespaces are not required for the fakeroot-command fallback.
exec env -u LD_PRELOAD -u FAKEROOTKEY FAKEROOTDONTTRYCHOWN=1 \\
    /usr/bin/fakeroot /usr/sbin/dropbear -F -E -e -s -j -k \\
    -p 127.0.0.1:%s -P /run/dropbear-computemcp.pid \\
    -r /etc/dropbear/dropbear_ed25519_host_key
''' % (home, port), 0o755)
print(":".join(account))
CONFIGURE

    install_authorized_key
    echo "Configured sandbox $SSH_USER account and authorized_keys." >&2
}

apptainer_start() {
    command -v apptainer >/dev/null
    [ -d "$SANDBOX" ] || { echo "Missing sandbox; use build first." >&2; exit 1; }
    test -x "$SANDBOX$(printf '/usr/local/bin/computemcp-%s-shell' "$SSH_USER")" || {
        echo "Run configure once before starting this existing sandbox." >&2
        exit 1
    }
    test -s "$HOST_HOME/.ssh/authorized_keys" || {
        echo "Missing authorized_keys; run configure first." >&2
        exit 1
    }
    if apptainer instance list | awk -v NAME="$NAME" '$1 == NAME {FOUND=1} END {exit !FOUND}'; then
        echo "STOP: $NAME is already running on this node." >&2
        exit 1
    fi
    local GPU_ARGS=()
    if has_vendor nvidia; then
        GPU_ARGS+=(--nv)
    fi
    if has_vendor amd; then
        if [ -c /dev/kfd ]; then GPU_ARGS+=(--bind /dev/kfd); else echo 'AMD selected but /dev/kfd is absent; skipping it.' >&2; fi
    fi
    if has_vendor amd || has_vendor intel; then
        if [ -d /dev/dri ]; then GPU_ARGS+=(--bind /dev/dri); else echo 'AMD/Intel selected but /dev/dri is absent; skipping it.' >&2; fi
    fi
    if command -v ss >/dev/null && [ -n "$(ss -H -ltn "sport = :$CONTAINER_PORT")" ]; then
        echo "STOP: port $CONTAINER_PORT is already in use." >&2
        exit 1
    fi
    if [ "${CUDA_VISIBLE_DEVICES+x}" = x ]; then
        export APPTAINERENV_CUDA_VISIBLE_DEVICES="$CUDA_VISIBLE_DEVICES"
    fi
    local COMMON=(--fakeroot --writable --containall --no-mount "home,cwd,hostfs,bind-paths")
    apptainer instance start "${COMMON[@]}" \
        --bind "$HOST_HOME:/root" \
        --bind "$HOST_HOME:/home/$SSH_USER" \
        --bind /etc/resolv.conf:/etc/resolv.conf:ro \
        "${GPU_ARGS[@]}" "$SANDBOX" "$NAME"
    wait_for_banner 127.0.0.1 "$CONTAINER_PORT" || {
        echo 'Apptainer SSH endpoint did not become ready.' >&2
        exit 1
    }
    printf '%s\n' "127.0.0.1:$CONTAINER_PORT" > "$STATE/container.endpoint"
    echo "Container ready on $(hostname -s), port $CONTAINER_PORT." >&2
}

apptainer_stop() {
    if command -v apptainer >/dev/null; then
        apptainer instance stop "$NAME" || true
    fi
    rm -f "$STATE/container.endpoint"
}

apptainer_shell() {
    exec apptainer exec --pwd "/home/$SSH_USER" "instance://$NAME" \
        "$(printf '/usr/local/bin/computemcp-%s-shell' "$SSH_USER")" -l
}

apptainer_fingerprint() {
    exec apptainer exec --pwd / "instance://$NAME" \
        /usr/bin/dropbearkey -y -f /etc/dropbear/dropbear_ed25519_host_key
}

# ---------------------------------------------------------------------------
# Docker runtime
# ---------------------------------------------------------------------------
DOCKER_CMD=(docker)

docker_entrypoint_content() {
    cat <<'ENTRYPOINT'
#!/usr/bin/env bash
# Runs as real root INSIDE Docker, never on the host.
set -euo pipefail
: "${AGENT_UID:?}" "${AGENT_GID:?}" "${SSH_USER:?}" "${CONTAINER_PORT:?}"
[[ "$AGENT_UID" =~ ^[0-9]+$ && "$AGENT_GID" =~ ^[0-9]+$ ]] && (( AGENT_UID > 0 ))
[[ "$SSH_USER" =~ ^[a-z_][a-z0-9_-]*$ ]] && [ "$SSH_USER" != root ]
[[ "$CONTAINER_PORT" =~ ^[0-9]+$ ]] && (( CONTAINER_PORT >= 1024 && CONTAINER_PORT <= 65535 ))
CONTAINER_HOME="/home/$SSH_USER"

if ! getent group "$AGENT_GID" >/dev/null; then
    GROUP_NAME="$SSH_USER"
    if getent group "$GROUP_NAME" >/dev/null; then GROUP_NAME="computemcp-$AGENT_GID"; fi
    groupadd --gid "$AGENT_GID" "$GROUP_NAME"
fi
if ! id -u "$SSH_USER" >/dev/null 2>&1; then
    EXISTING="$(getent passwd "$AGENT_UID" | cut -d: -f1 || true)"
    if [ -n "$EXISTING" ]; then
        usermod --login "$SSH_USER" "$EXISTING"
    else
        useradd --uid "$AGENT_UID" --gid "$AGENT_GID" --no-create-home \
            --home-dir "$CONTAINER_HOME" --shell /usr/local/bin/computemcp-shell "$SSH_USER"
    fi
fi
[ "$(id -u "$SSH_USER")" = "$AGENT_UID" ] || {
    echo 'Existing container account has a different UID; refusing to change ownership.' >&2; exit 1;
}
usermod --gid "$AGENT_GID" --home "$CONTAINER_HOME" \
    --shell /usr/local/bin/computemcp-shell --password '*' "$SSH_USER"
# Discover supplementary groups AND actual GPU device ownership.
# sshd initializes agent groups from /etc/group, so membership must be explicit.
fix_gpu_groups() {
    local DEVICE EXTRA_GID EXTRA_GROUP
    local -A GPU_GROUP_IDS=()
    for EXTRA_GID in $(id -G); do GPU_GROUP_IDS["$EXTRA_GID"]=1; done
    for DEVICE in /dev/kfd /dev/dri/* /dev/nvidia*; do
        [ -c "$DEVICE" ] || continue
        GPU_GROUP_IDS["$(stat -c %g "$DEVICE")"]=1
    done
    for EXTRA_GID in "${!GPU_GROUP_IDS[@]}"; do
        [ "$EXTRA_GID" != 0 ] && [ "$EXTRA_GID" != "$AGENT_GID" ] || continue
        if ! getent group "$EXTRA_GID" >/dev/null; then
            EXTRA_GROUP="host-gpu-$EXTRA_GID"
            while getent group "$EXTRA_GROUP" >/dev/null; do
                EXTRA_GROUP="${EXTRA_GROUP}-extra"
            done
            groupadd --gid "$EXTRA_GID" "$EXTRA_GROUP"
        fi
        usermod --append --groups "$EXTRA_GID" "$SSH_USER"
    done
}
fix_gpu_groups
if [ "${1:-}" = --fix-groups ]; then exit 0; fi

test -s "$CONTAINER_HOME/.ssh/authorized_keys"
chmod 700 "$CONTAINER_HOME" "$CONTAINER_HOME/.ssh"
chmod 600 "$CONTAINER_HOME/.ssh/authorized_keys"
# Only change the mount root/key ownership, never recursively change user files.
chown "$AGENT_UID:$AGENT_GID" "$CONTAINER_HOME" "$CONTAINER_HOME/.ssh" \
    "$CONTAINER_HOME/.ssh/authorized_keys"
printf '%s ALL=(ALL) NOPASSWD:ALL\n' "$SSH_USER" > /etc/sudoers.d/computemcp
chmod 440 /etc/sudoers.d/computemcp
visudo -c
mkdir -p /run/sshd
ssh-keygen -A

cat > /etc/ssh/sshd_config_computemcp <<SSHD
Port $CONTAINER_PORT
ListenAddress 0.0.0.0
HostKey /etc/ssh/ssh_host_ed25519_key
PidFile /run/sshd-computemcp.pid
AllowUsers $SSH_USER
PermitRootLogin no
PubkeyAuthentication yes
AuthenticationMethods publickey
AuthorizedKeysFile .ssh/authorized_keys
PasswordAuthentication no
KbdInteractiveAuthentication no
PermitEmptyPasswords no
UsePAM no
AllowAgentForwarding no
AllowTcpForwarding no
X11Forwarding no
PermitTunnel no
Subsystem sftp internal-sftp
SSHD
# Register libraries injected by NVIDIA's Docker runtime for SSH sessions.
ldconfig
/usr/sbin/sshd -t -f /etc/ssh/sshd_config_computemcp
exec /usr/sbin/sshd -D -e -f /etc/ssh/sshd_config_computemcp
ENTRYPOINT
}

docker_exists() { "${DOCKER_CMD[@]}" container inspect "$NAME" >/dev/null 2>&1; }

docker_check_owner() {
    local OWNER SYSTEM_LABEL
    OWNER="$("${DOCKER_CMD[@]}" container inspect --format '{{index .Config.Labels "org.computemcp.owner-uid"}}' "$NAME")"
    SYSTEM_LABEL="$("${DOCKER_CMD[@]}" container inspect --format '{{index .Config.Labels "org.computemcp.system"}}' "$NAME")"
    if ! { [ "$OWNER" = "$(id -u)" ] && [ "$SYSTEM_LABEL" = "$SYSTEM" ]; }; then
        echo "STOP: $NAME does not have the expected owner/system labels; it was not modified." >&2
        exit 1
    fi
}

docker_build_image() {
    [ -n "$IMAGE" ] || { echo 'COMPUTEMCP_IMAGE is required to build a Docker image.' >&2; exit 1; }
    local BASE_IMAGE="${IMAGE#docker://}" CONTEXT
    CONTEXT="$(mktemp -d "$STATE/build.XXXXXX")"
    docker_entrypoint_content > "$CONTEXT/computemcp-entrypoint"
    cat > "$CONTEXT/Dockerfile" <<DOCKERFILE
ARG BASE_IMAGE=$BASE_IMAGE
FROM \${BASE_IMAGE}
USER root
RUN printf '#!/bin/sh\nexit 101\n' > /usr/sbin/policy-rc.d \\
    && chmod 755 /usr/sbin/policy-rc.d \\
    && apt-get update \\
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \\
        openssh-server sudo ca-certificates git curl build-essential cmake ninja-build python3 \\
    && rm -rf /var/lib/apt/lists/* /etc/ssh/ssh_host_* \\
    && mkdir -p /run/sshd
COPY computemcp-entrypoint /usr/local/bin/computemcp-entrypoint
RUN chmod 755 /usr/local/bin/computemcp-entrypoint \\
    && printf '%s\n' '#!/bin/sh' \\
       'export PATH="\$HOME/.local/bin:/usr/local/cuda/bin:/usr/local/nvidia/bin:\$PATH"' \\
       'exec /bin/bash "\$@"' > /usr/local/bin/computemcp-shell \\
    && chmod 755 /usr/local/bin/computemcp-shell \\
    && printf '%s\n' /usr/local/bin/computemcp-shell >> /etc/shells
ENTRYPOINT ["/usr/local/bin/computemcp-entrypoint"]
DOCKERFILE
    if "${DOCKER_CMD[@]}" build --build-arg "BASE_IMAGE=$BASE_IMAGE" --tag "$DOCKER_IMAGE" "$CONTEXT" >&2; then
        rm -rf -- "$CONTEXT"
    else
        rm -rf -- "$CONTEXT"
        return 1
    fi
}

docker_install_entrypoint() {
    local TEMP_ENTRYPOINT
    TEMP_ENTRYPOINT="$(mktemp "$STATE/entrypoint.XXXXXX")"
    docker_entrypoint_content > "$TEMP_ENTRYPOINT"
    chmod 755 "$TEMP_ENTRYPOINT"
    if "${DOCKER_CMD[@]}" cp "$TEMP_ENTRYPOINT" "$NAME:/usr/local/bin/computemcp-entrypoint" >&2; then
        rm -f -- "$TEMP_ENTRYPOINT"
    else
        rm -f -- "$TEMP_ENTRYPOINT"
        return 1
    fi
}

docker_create_container() {
    "${DOCKER_CMD[@]}" image inspect "$DOCKER_IMAGE" >/dev/null 2>&1 || {
        echo 'Missing derived Docker image; run build first.' >&2
        exit 1
    }
    install_authorized_key
    local ARGS AGENT_UID AGENT_GID CPUS
    CPUS="${COMPUTEMCP_CPUS_PER_NODE:-$LEGACY_CPUS}"
    if ! { [[ "$CPUS" =~ ^[0-9]+$ ]] && (( CPUS > 0 )); }; then
        echo "Invalid CPU count: $CPUS" >&2
        exit 2
    fi
    AGENT_UID="$(stat -c %u "$HOST_HOME")"
    AGENT_GID="$(stat -c %g "$HOST_HOME")"
    printf 'Container home will use %s (uid:gid %s:%s)\n' "$HOST_HOME" "$AGENT_UID" "$AGENT_GID" >&2
    ARGS=(--name "$NAME" --restart "$DOCKER_RESTART"
        --label "org.computemcp.owner-uid=$(id -u)" --label "org.computemcp.system=$SYSTEM"
        --publish "127.0.0.1::$CONTAINER_PORT"
        --mount "type=bind,src=$HOST_HOME,dst=/home/$SSH_USER"
        --env "AGENT_UID=$AGENT_UID" --env "AGENT_GID=$AGENT_GID"
        --env "SSH_USER=$SSH_USER" --env "CONTAINER_PORT=$CONTAINER_PORT")
    [ -z "${COMPUTEMCP_MEMORY_PER_NODE_MIB:-}" ] || ARGS+=(--memory "${COMPUTEMCP_MEMORY_PER_NODE_MIB}m")
    [ -n "${COMPUTEMCP_MEMORY_PER_NODE_MIB:-}" ] || { [ -z "$LEGACY_MEMORY" ] || ARGS+=(--memory "$LEGACY_MEMORY"); }
    ARGS+=(--cpus "$CPUS")
    if has_vendor nvidia; then ARGS+=(--gpus all); fi
    local DEVICE GID
    local -A DEVICE_GROUPS=()
    if has_vendor amd; then
        if [ -c /dev/kfd ]; then
            ARGS+=(--device /dev/kfd --security-opt seccomp=unconfined)
            DEVICE_GROUPS["$(stat -c %g /dev/kfd)"]=1
        else
            echo 'AMD selected but /dev/kfd is absent on this host; skipping it.' >&2
        fi
    fi
    if has_vendor amd || has_vendor intel; then
        if [ -d /dev/dri ]; then
            ARGS+=(--device /dev/dri:/dev/dri)
            for DEVICE in /dev/dri/*; do
                [ -c "$DEVICE" ] || continue
                DEVICE_GROUPS["$(stat -c %g "$DEVICE")"]=1
            done
        else
            echo 'AMD/Intel selected but /dev/dri is absent on this host; skipping it.' >&2
        fi
    fi
    for GID in "${!DEVICE_GROUPS[@]}"; do
        [ "$GID" = 0 ] || ARGS+=(--group-add "$GID")
    done
    "${DOCKER_CMD[@]}" create "${ARGS[@]}" "$DOCKER_IMAGE" >&2
    docker_install_entrypoint
}

docker_endpoint() {
    local MAPPING
    MAPPING="$("${DOCKER_CMD[@]}" container port "$NAME" "$CONTAINER_PORT/tcp")"
    [[ "$MAPPING" =~ ^127\.0\.0\.1:([0-9]+)$ ]] || {
        echo "Unexpected SSH mapping for $NAME: $MAPPING" >&2
        exit 1
    }
    printf '%s\n' "$MAPPING"
}

docker_ensure_started() {
    if docker_exists; then
        docker_check_owner
    else
        docker_create_container
    fi
    if [ "$("${DOCKER_CMD[@]}" container inspect --format '{{.State.Running}}' "$NAME")" != true ]; then
        "${DOCKER_CMD[@]}" start "$NAME" >&2
    fi
    local HOSTENDPOINT
    HOSTENDPOINT="$(docker_endpoint)"
    if ! wait_for_banner "${HOSTENDPOINT%:*}" "${HOSTENDPOINT##*:}"; then
        "${DOCKER_CMD[@]}" logs --tail 60 "$NAME" >&2 || true
        return 1
    fi
    printf '%s\n' "$HOSTENDPOINT" > "$STATE/container.endpoint"
}

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------
case "$RUNTIME" in
    apptainer)
        case "$ACTION" in
            build)      apptainer_build; apptainer_configure ;;
            configure)  apptainer_configure ;;
            update)     apptainer_configure ;;
            start)      apptainer_start ;;
            stop|close) apptainer_stop ;;
            shell)      apptainer_shell ;;
            fingerprint) apptainer_fingerprint ;;
            endpoint)
                [ -d "$SANDBOX" ] || { echo 'Missing sandbox.' >&2; exit 1; }
                printf 'ENDPOINT 127.0.0.1:%s\n' "$CONTAINER_PORT"
                ;;
        esac
        ;;
    docker)
        for CMD in "${DOCKER_CMD[0]}" python3; do command -v "$CMD" >/dev/null; done
        "${DOCKER_CMD[@]}" info >/dev/null
        case "$ACTION" in
            build)
                docker_exists && { docker_check_owner; echo "STOP: $NAME already exists; use update." >&2; exit 1; }
                docker_build_image
                docker_create_container
                echo "Created $NAME. Run start to launch it." >&2
                ;;
            configure)
                if docker_exists; then docker_check_owner; fi
                install_authorized_key
                echo "Configured SSH public key in $HOST_HOME." >&2
                ;;
            update)
                docker_exists || { echo 'No container to update; use build first.' >&2; exit 1; }
                docker_check_owner
                docker_install_entrypoint
                if [ "$("${DOCKER_CMD[@]}" container inspect --format '{{.State.Running}}' "$NAME")" = true ]; then
                    "${DOCKER_CMD[@]}" exec --user root "$NAME" \
                        /usr/local/bin/computemcp-entrypoint --fix-groups >&2
                fi
                ;;
            start)      docker_ensure_started ;;
            stop|close) docker_exists && { docker_check_owner; "${DOCKER_CMD[@]}" stop "$NAME" >&2; }; rm -f "$STATE/container.endpoint" ;;
            shell)      docker_ensure_started; exec "${DOCKER_CMD[@]}" exec -it --user "$SSH_USER" --workdir "/home/$SSH_USER" "$NAME" /usr/local/bin/computemcp-shell -l ;;
            fingerprint) docker_ensure_started; exec "${DOCKER_CMD[@]}" exec "$NAME" ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub ;;
            endpoint)
                docker_exists && docker_check_owner
                printf 'ENDPOINT %s\n' "$(docker_endpoint)"
                ;;
        esac
        ;;
esac
