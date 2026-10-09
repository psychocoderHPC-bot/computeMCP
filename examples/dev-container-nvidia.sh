#!/usr/bin/env bash
# dev-container-nvidia.sh - create a persistent, NVIDIA-accelerated computeMCP
# development container (workflow B: manual / pre-existing environment).
#
# Parameterized Docker/NVIDIA recipe. Runs ON THE REMOTE HOST. It wraps the
# same entrypoint logic as dev-container-amd.sh, so the two scripts share one
# implementation (dev-container-entrypoint.sh) and differ only in the device
# flags passed to `docker run`.
#
# Marketing: this container is NOT needed when the gateway target has a
# [targets.X.bundle] block. The gateway deploys and runs the shipped bundle
# itself. You need this script for:
#   (a) workflow B where provision_command expects a pre-existing container,
#   (b) availability tests before you switch to a bundle target, or
#   (c) verifying that the gateway's route + key + port wiring is correct
#       before you provision through the bundle.
#
# Driver policy: the NVIDIA kernel driver lives on the host. Do NOT install
# `nvidia-smi` or any CUDA library inside the container; the agent installs
# its own toolchain later.
#
# Usage:
#   bash dev-container-nvidia.sh \
#     --name computeMCP-container \
#     --home-dir /home/USER/workspace/computemcp-container \
#     --public-key "$(ssh-keygen -y -f /path/to/gateway/computemcp_container)"
#
# Options:
#   --name         container name (default computeMCP-container)
#   --home-dir     absolute path to the persistent host home (created if absent)
#   --public-key   single-line public key for the gateway (the .pub half,
#                  NEVER the private key)
#   --image        base image (default ubuntu:24.04)
#   --port         loopback port mapped to container 22 (default 2222)
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
        sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//'
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

# This Cmd re-runs on every container start, so every step must tolerate
# already-existing state; otherwise `bash -euc` aborts before `exec sshd`
# and `--restart unless-stopped` loops forever.
ENTRYPOINT='
    if ! command -v sshd >/dev/null 2>&1; then
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server sudo
    fi

    # Exactly one user named "agent" owns AGENT_UID. ubuntu:24.04 already ships
    # a user at uid 1000 ("ubuntu"); AGENT_UID is the host-home owner (often
    # 1000), so that uid is normally already taken. Reuse (rename) the existing
    # account instead of `useradd -o`, which would create a SECOND user sharing
    # the uid and make the SSH login resolve to the wrong name. Renaming keeps
    # the same uid/gid, so the bind-mounted home (owned by AGENT_UID) stays
    # owned by the login user.
    if ! id -u agent >/dev/null 2>&1; then
      existing="$(getent passwd "$AGENT_UID" | cut -d: -f1 || true)"
      if [ -n "$existing" ]; then
        usermod -l agent "$existing"          # rename the existing account
        grp="$(getent group "$AGENT_GID" | cut -d: -f1 || true)"
        { [ -n "$grp" ] && [ "$grp" != "agent" ] && groupmod -n agent "$grp"; } || true
      else
        getent group agent >/dev/null || groupadd --gid "$AGENT_GID" agent
        useradd --uid "$AGENT_UID" --gid "$AGENT_GID" \
          --home-dir /home/agent --no-create-home --shell /bin/bash agent
      fi
      usermod -d /home/agent agent            # point home at the bind mount
    fi

    # Passwordless sudo for "agent" and the numeric uid, defense in depth.
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

# GPU flags: creation-time only (frozen in HostConfig).
#  --device /dev/dri:/dev/dri   DRM render devices (also used by some CUDA
#                               paths and by the OpenCL renderer)
#  --gpus all                   resolved by the NVIDIA Container Toolkit.
#  NOTE: --gpus is NOT available on ROCm hosts; the AMD companion uses
#  --device /dev/kfd and seccomp=unconfined instead.
docker run -d \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    --device /dev/dri:/dev/dri \
    --gpus all \
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
