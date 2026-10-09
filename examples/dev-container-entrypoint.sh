#!/usr/bin/env bash
# dev-container-entrypoint.sh - shared, idempotent entrypoint for the
# manual Docker/Podman dev-container scripts in this examples/ directory.
#
# Do NOT run this file directly; it is source-ed by dev-container-nvidia.sh,
# dev-container-amd.sh, and any sister script that needs the same inner
# setup. Provide these variables in the environment before sourcing:
#
#   CONTAINER_NAME   name of the container (used for the gpu_x_dir hint)
#   HOST_HOME        absolute path to the persistent host-home bind mount
#   SSH_PUBLIC_KEY   single-line public key the container will trust
#   ENTRYPOINT_CMD   the inner `bash -euc '<...>'` string (the source of a
#                    the Cmd set by docker run; re-execs on every start)
#
# `prepare_entrypoint_vars` sets AGENT_UID and AGENT_GID from HOST_HOME and
# prints the values to stdout so the calling script can decide whether the
# bind mount will be reused and its owner is sensible.
#
# SPDX-License-Identifier: ISC
# SPDX-FileCopyrightText: René Widera

set -euo pipefail

if [ -f "${BASH_SOURCE[1]:-}" ]; then
    # Sourced, not executed. Good.
    :
fi

prepare_entrypoint_vars() {
    # Derive AGENT_UID/AGENT_GID from the host-home owner so the container
    # login user owns the bind mount.
    AGENT_UID="$(stat -c %u "$HOST_HOME")"
    AGENT_GID="$(stat -c %g "$HOST_HOME")"

    # A persistent home is allowed to be non-empty on recreate: the toolchain
    # is what makes the container persistent across recreation.
    if [ -n "$(find "$HOST_HOME" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        echo "NOTE: $HOST_HOME is not empty; reusing it (toolchain preserved)."
    fi

    printf 'Container home will use %s (uid:gid %s:%s)\n' \
        "$HOST_HOME" "$AGENT_UID" "$AGENT_GID"
}
