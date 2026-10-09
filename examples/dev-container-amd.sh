#!/usr/bin/env bash
# dev-container-amd.sh - create a persistent, AMD/ROCm-accelerated computeMCP
# development container (workflow B: manual / pre-existing environment).
#
# Companion to dev-container-nvidia.sh. Runs ON THE REMOTE HOST. Both scripts
# share the same inner entrypoint and the same idempotency invariants from
# dev-container-entrypoint.sh; the only difference between the two is the
# device/group flags passed to `docker run`:
#
#   NVIDIA  (nvidia.sh):  --device /dev/dri:/dev/dri --gpus all
#   AMD     (this script): --device /dev/kfd --device /dev/dri
#                          --security-opt seccomp=unconfined
#                          --group-add <video> --group-add <render>
#
# Why these flags:
#   * --gpus all (NVIDIA) is driven by the NVIDIA Container Toolkit and does
#     NOT expose an AMD GPU. For ROCm the KFD (/dev/kfd, the compute device)
#     AND the DRM character devices (/dev/dri, renderD*/card*) must both be
#     present.
#   * The default Docker seccomp profile blocks the `ioctls` that ROCm uses on
#     /dev/kfd and /dev/dri, producing HSA_STATUS_ERROR or "no GPU". This is
#     why seccomp=unconfined is effectively required.
#   * Group access: the container process must be in the host's video and
#     render groups to open the devices. The script resolves host names to
#     GIDs (portable across Ubuntu / non-Ubuntu hosts) and falls back to
#     `--group-add V <name>` if a name resolves to its numeric GID.
#
# Driver policy: the `amdgpu` / ROCm kernel driver lives on the HOST. Do NOT
# install the kernel driver inside the container. The base ubuntu:24.04 image
# ships no ROCm user space; the agent installs the matching ROCm userspace
# toolchain later (analogous to the CUDA toolkit for NVIDIA). Verify with
# `docker exec <container> rocminfo` (and `rocm-smi`) after install.
#
# Usage: see dev-container-nvidia.sh; CLI is identical.
#
# SPDX-License-Identifier: ISC
# SPDX-FileCopyrightText: René Widera

set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]:-$0}")" && pwd)"
# shellcheck disable=SC1091
source "$SCRIPT_DIR/dev-container-entrypoint.sh"

CONTAINER_NAME="computeMCP-container"
HOST_HOME="${HOME}/workspace/computeMCP-container"
SSH_PUBLIC_KEY=""
IMAGE="ubuntu:24.04"
HOST_PORT="2222"

while [ $# -gt 0 ]; do
    case "$1" in
      --name)       CONTAINER_NAME="$2"; shift 2 ;;
      --home-dir)   HOST_HOME="$2"; shift 2 ;;
      --public-key) SSH_PUBLIC_KEY="$2"; shift 2 ;;
      --image)      IMAGE="$2"; shift 2 ;;
      --port)       HOST_PORT="$2"; shift 2 ;;
      -h|--help)
        sed -n '2,28p' "$0" | sed 's/^# \{0,1\}//'
        exit 0 ;;
      *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
done

if ! [ -n "$SSH_PUBLIC_KEY" ]; then
    echo "STOP: --public-key is required (the .pub half only)." >&2
    exit 1
fi
case "$SSH_PUBLIC_KEY" in
    *BEGIN*PRIVATE*KEY*)
        echo "STOP: looks like a PRIVATE key; paste the PUBLIC key only." >&2
        exit 1 ;;
esac

mkdir -p "$HOST_HOME"

if docker container inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
    echo "STOP: $CONTAINER_NAME already exists; it was not modified." >&2
    exit 1
fi

prepare_entrypoint_vars

# Resolve the host group names to numeric GIDs so this script works on hosts
# where the container image has a different /etc/group than the host, and on
# hosts where one of the two names (video, render) does not exist.
#
# --group-add accepts either a name (resolved against the container's
# /etc/group) or a numeric GID (always valid). Numeric GIDs are preferred
# here because a name that does not exist in the container silently drops
# the flag.
GROUP_FLAGS=()
for gname in video render; do
    ggid="$(getent group "$gname" | cut -d: -f3 || true)"
    if [ -n "$ggid" ]; then
        GROUP_FLAGS+=(--group-add "$ggid")
        echo "  --group-add $ggid   (host group: $gname)"
    else
        echo "  (skipping --group-add for '$gname': not present in this host's /etc/group)"
    fi
done

# Same idempotent entrypoint as the NVIDIA companion. The user it creates
# inside the image is named "agent" so the gateway's `container_user` and
# the entrypoint's own `usermod -l agent` / `useradd -u 1000 ...` agree.
ENTRYPOINT='
    if ! command -v sshd >/dev/null 2>&1; then
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server sudo
    fi

    if ! id -u agent >/dev/null 2>&1; then
      existing="$(getent passwd "$AGENT_UID" | cut -d: -f1 || true)"
      if [ -n "$existing" ]; then
        usermod -l agent "$existing"
        grp="$(getent group "$AGENT_GID" | cut -d: -f1 || true)"
        { [ -n "$grp" ] && [ "$grp" != "agent" ] && groupmod -n agent "$grp"; } || true
      else
        getent group agent >/dev/null || groupadd --gid "$AGENT_GID" agent
        useradd --uid "$AGENT_UID" --gid "$AGENT_GID" \
          --home-dir /home/agent --no-create-home --shell /bin/bash agent
      fi
      usermod -d /home/agent agent
    fi

    for u in agent "#${AGENT_UID}"; do
      printf "%s ALL=(ALL) NOPASSWD:ALL\n" "$u" \
        > "/etc/sudoers.d/computemcp-$(printf "%s" "$u" | tr -c "A-Za-z0-9" "_")"
    done
    chmod 440 /etc/sudoers.d/computemcp-*
    visudo -c
    install -d -m 700 -o agent -g agent /home/agent/.ssh
    printf "%s\n" "$SSH_PUBLIC_KEY" > /home/agent/.ssh/authorized_keys
    chown agent:agent /home/agent/.ssh/authorized_keys
    chmod 600 /home/agent/.ssh/authorized_keys
    printf "PasswordAuthentication no\nKbdInteractiveAuthentication no\nPermitRootLogin no\nPubkeyAuthentication yes\nAllowUsers agent\n" \
      > /etc/ssh/sshd_config.d/00-computemcp.conf
    mkdir -p /run/sshd
    ssh-keygen -A
    /usr/sbin/sshd -t || true
    exec /usr/sbin/sshd -D -e
'

# Creation-time only. --device / --security-opt / --group-add are frozen in
# HostConfig and cannot be added by stop/start.
docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    --device /dev/kfd \
    --device /dev/dri \
    --security-opt seccomp=unconfined \
    "${GROUP_FLAGS[@]}" \
    -p "127.0.0.1:${HOST_PORT}:22" \
    --mount "type=bind,src=${HOST_HOME},dst=/home/agent" \
    -e "SSH_PUBLIC_KEY=${SSH_PUBLIC_KEY}" \
    -e "AGENT_UID=${AGENT_UID}" \
    -e "AGENT_GID=${AGENT_GID}" \
    "$IMAGE" \
    bash -euc "$ENTRYPOINT"

echo
echo "Started container $CONTAINER_NAME. Read the host-key fingerprint:"
echo
echo "  docker exec $CONTAINER_NAME ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub"
echo
echo "Use the SHA256:... line as [targets.<name>] host_key_sha256."
