#!/usr/bin/env bash
# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
#
# ensure-container.sh - start the computeMCP development container if it is not
# already running.
#
# This is the reference `connect_command` for the computeMCP gateway.  It runs
# ON THE REMOTE HOST (through the target's SSH route) when the gateway cannot
# reach the container, i.e. when the container is stopped.
#
# It is IDEMPOTENT: with `connect_command_mode = "always"` it also runs before
# every connect, so a healthy container must be left untouched (the script then
# simply does nothing and exits 0).  With the default mode ("on_failure") it
# only runs when the container is unreachable.
#
# It must be executable and reachable on the remote host, e.g.:
#
#   connect_command = ["/home/USER/.config/computeMCP-gateway/ensure-container.sh"]
#   connect_command_timeout = 120.0
#   connect_command_mode = "on_failure"   # or "always"
#
# Exit code is advisory: the gateway always re-probes the container after the
# command and only succeeds if the container is really reachable.

set -euo pipefail

CONTAINER_NAME="${COMPUTEMCP_CONTAINER_NAME:-computeMCP-container}"

# Pick the available container runtime (Docker or Podman).
if command -v docker >/dev/null 2>&1; then
  RUNTIME=docker
elif command -v podman >/dev/null 2>&1; then
  RUNTIME=podman
else
  echo "ensure-container: neither docker nor podman found on $HOSTNAME" >&2
  exit 1
fi

if ! "$RUNTIME" container inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "ensure-container: container '$CONTAINER_NAME' does not exist" >&2
  exit 1
fi

state="$("$RUNTIME" inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || echo unknown)"
if [ "$state" = "running" ]; then
  # No-op on the happy path (required for connect_command_mode = "always").
  exit 0
fi

echo "ensure-container: '$CONTAINER_NAME' is $state; starting it"
"$RUNTIME" start "$CONTAINER_NAME" >/dev/null

# Wait until it reports running so the following gateway probe succeeds.
for _ in $(seq 1 30); do
  state="$("$RUNTIME" inspect -f '{{.State.Status}}' "$CONTAINER_NAME" 2>/dev/null || echo unknown)"
  [ "$state" = "running" ] && exit 0
  sleep 1
done

echo "ensure-container: '$CONTAINER_NAME' did not reach 'running' (last: $state)" >&2
exit 1
