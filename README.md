# computeMCP gateway

Secure host-side gateway and single MCP bridge that let AI agents running
inside Terok containers use remote development/compute containers without ever
receiving the host's SSH credentials.

```
OpenCode / agent
  |  MCP over stdio
  v
computeMCP-mcp (inside Terok)
  |  authenticated HTTP over the one allowed host endpoint
  v
computeMCP-gateway (on the Terok host)
  |  authenticated SSH route connections (owned by the gateway)
  +--> agent@hal development container
  +--> agent@fwk394 development container
```

The gateway TOML is the single source of truth for targets. A client can only
name a configured target; an arbitrary SSH hostname is never accepted.

## Quick start

Three commands bring a fresh host to a working gateway. Run them as the normal
(user, non-root) host account that will own the gateway.

```bash
python3 -m venv ~/.local/share/computeMCP-gateway/venv
~/.local/share/computeMCP-gateway/venv/bin/pip install .
ln -s ~/.local/share/computeMCP-gateway/venv/bin/computeMCP-gateway ~/.local/bin/computeMCP-gateway

computeMCP-gateway --bootstrap        # interactive: questions below
computeMCP-gateway                    # start; config + tokens already written
```

`--bootstrap` asks for the server address, one client (your Terok project) and
optionally a first target, writes `~/.config/computeMCP-gateway/config.toml` and
`tokens.toml` (hashes only, mode 0600), and prints the client's plaintext token
once. Add more systems later with
`computeMCP-gatewayctl --add-target`. To write elsewhere, pass `--config-dir
DIR` (or `--config FILE`); `--force` overwrites an existing config.

### Every gateway start: what to do

1. **Create the container key once** (skip if it exists):
   `ssh-keygen -t ed25519 -f ~/.ssh/computemcp_container -C computeMCP-gateway`.
   Install only the `.pub` half as `authorized_keys` of the target `user`
   (default `agent`).
2. **Start the gateway**: `computeMCP-gateway` (or
   `systemctl --user start computeMCP-gateway` with the shipped unit). It reads
   `config.toml` and `tokens.toml`.
3. **Check it is healthy**: `computeMCP-gatewayctl status`. Targets listed
   `connected` are usable; `disconnected` usually means the container is down
   (`connect_command`) or the allocation is not provisioned yet.
4. **In each Terok task**: run the handshake once and approve it (see
   [Set up the MCP inside a Terok task](#set-up-the-mcp-inside-a-terok-task)).

The gateway validates `config.toml` on start and on reload, so a bad edit fails
loudly instead of silently serving a stale target.

## Layout

```
src/compute_mcp/
  allocation.py   Slurm allocation planning and sbatch/srun argument rendering
  config.py       TOML loading + validation, transport selection
  auth.py         constant-time bearer auth and per-client ACLs
  tunnel.py       asyncio SSH tunnel manager, route failover, recovery
  ssh_backend.py  AsyncSSH: exec, PTY sessions, SFTP, host-key pinning
  sessions.py     persistent PTY session manager (bounded buffers, quotas)
  files.py        SFTP file operations
  gateway.py      state machine, HTTP API, interactive console
  enrollment.py   unauthenticated request queue + operator approval
  setup.py        interactive --bootstrap / --add-target configuration wizard
  handshake.py    computeMCP-handshake: request access from inside a container
  mcp_server.py   MCP server (stdio) exposing computeMCP_* tools
  control.py      computeMCP-gatewayctl operator CLI
tests/            unit tests (see `pytest -q`)
scripts/computemcp-slurm/  Slurm provisioning bundle (packaged under src, symlinked; see its README)
config.example.toml
systemd/compute-mcp-gateway.service
```

## Install (gateway on the host, not as root)

```bash
python3 -m venv ~/.local/share/computeMCP-gateway/venv
~/.local/share/computeMCP-gateway/venv/bin/pip install -U pip
~/.local/share/computeMCP-gateway/venv/bin/pip install .
ln -s ~/.local/share/computeMCP-gateway/venv/bin/computeMCP-gateway ~/.local/bin/computeMCP-gateway
mkdir -p ~/.config/computeMCP-gateway
cp config.example.toml ~/.config/computeMCP-gateway/config.toml
```

`--bootstrap` (see [Quick start](#quick-start)) writes `config.toml` and
`tokens.toml` for you; the manual `cp` above is the alternative when you prefer
to start from the fully commented template.

### Configure the gateway interactively

`computeMCP-gateway --bootstrap` creates the initial configuration. Every
question prints a short description; questions with a fixed set of answers list
them, and a default appears in brackets (press Enter to accept it).

| Question | Meaning |
| --- | --- |
| Listen address / Port | Where the gateway serves its HTTP API (default `127.0.0.1:2222`) |
| Allow interactive enrollment | Enables `computeMCP-handshake`; approval stays manual |
| Client id / label | The Terok project this token belongs to |
| Set up a target | Whether to configure a remote system now |
| Target name | Internal label, e.g. `hal` |
| Transport | `tunnel` (SSH alias, recommended) or `direct` (host:port) |
| SSH alias / Remote user / Private key | Route connection details; the key default is `~/.ssh/computemcp_container` |
| Container host-key fingerprint | Optional `SHA256:...` pin of the container sshd key |
| Second factor | Whether the login node needs a password/OTP |
| Container runtime / storage / image / GPU vendors | Drives the provisioning bundle (see below) |
| Slurm node capacities / allocation / sbatch | Optional; needed for `--set` overrides and dry-run |

On success it writes `config.toml` (0600), writes `tokens.toml` (0600, sha256
hashes only) and prints the client token once. Add another system later:

```bash
computeMCP-gatewayctl --add-target                 # default config path
computeMCP-gatewayctl --config FILE --add-target   # explicit config
```

`--add-target` validates the existing file, appends one `[targets.X]` block,
re-validates the whole file and rolls back if the result would not load. It does
not need a running gateway.

Create a dedicated gateway-to-container key (never the user's normal key):

```bash
ssh-keygen -t ed25519 -f ~/.ssh/computemcp_container -C computeMCP-gateway
# install only the .pub half in the remote development containers, as the
# `authorized_keys` of the target `user` (default "agent"):
ssh-copy-id -i ~/.ssh/computemcp_container.pub agent@<container-host>
```

> **Replace every `/home/USER` placeholder.** `config.example.toml` uses
> `/home/USER/...` so it is obviously a template; a target that keeps the
> literal string `USER` (e.g. `client_key = "/home/USER/.ssh/..."`) *parses*
> fine but every `exec`/session fails with
> `SSH connection to target '<t>' failed: [Errno 2] No such file or directory`.
> Set the real absolute path for `client_key`, `token_file`, etc.

## Create the remote development container

The gateway needs a persistent SSH server to dial. Any host with Docker/Podman
and an OpenSSH sshd works; a plain `ubuntu:24.04` container with
`openssh-server` is enough and is the setup the gateway was verified against.
The container is intentionally minimal — the agent installs its own toolchain
later — and **persistent** (no `--rm`).

Recipe (adapted from the HAL remote-development handoff,
`agent-config/terok/hal-remote-development-handoff.md`). Run this **on the
remote host**, after generating `~/.ssh/computemcp_container` on the gateway
host (previous step). Paste only the **public** key; never copy the private key.
The `HOST_HOME` bind mount is what makes the toolchain persistent across
container recreation; its `uid:gid` is reused for the `agent` user.

```bash
mkdir -p ~/workspace/computeMCP-container

bash <<'BASH'
set -euo pipefail

CONTAINER_NAME="computeMCP-container"
HOST_HOME="$HOME/workspace/computeMCP-container"
SSH_PUBLIC_KEY='REPLACE_WITH_COMPUTEMCP_CONTAINER_PUBLIC_KEY'   # .pub half only

if docker container inspect "$CONTAINER_NAME" >/dev/null 2>&1; then
  echo "STOP: $CONTAINER_NAME already exists; it was not modified."
  exit 1
fi

if [ ! -d "$HOST_HOME" ]; then
  echo "STOP: home directory not found: $HOST_HOME"
  exit 1
fi

# A persistent home is intentionally allowed to be non-empty on recreate.
if [ -n "$(find "$HOST_HOME" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
  echo "NOTE: $HOST_HOME is not empty; reusing it (toolchain preserved)."
fi

AGENT_UID="$(stat -c %u "$HOST_HOME")"
AGENT_GID="$(stat -c %g "$HOST_HOME")"
printf 'Container home will use %s (uid:gid %s:%s)\n' "$HOST_HOME" "$AGENT_UID" "$AGENT_GID"

docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  --device /dev/dri:/dev/dri \
  --gpus all \
  -p 127.0.0.1:2222:22 \
  --mount "type=bind,src=$HOST_HOME,dst=/home/agent" \
  -e "SSH_PUBLIC_KEY=$SSH_PUBLIC_KEY" \
  -e "AGENT_UID=$AGENT_UID" \
  -e "AGENT_GID=$AGENT_GID" \
  ubuntu:24.04 \
  bash -euc '
    # This Cmd re-runs on every container start, so every step must tolerate
    # already-existing state; otherwise `bash -euc` aborts before `exec sshd`
    # and `--restart unless-stopped` loops forever.
    if ! command -v sshd >/dev/null 2>&1; then
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server sudo
    fi

    # Exactly one user named "agent" owns AGENT_UID. ubuntu:24.04 already ships
    # a user at uid 1000 ("ubuntu"); AGENT_UID is the host-home owner (usually
    # 1000), so that uid is normally already taken. Reuse (rename) the existing
    # account instead of `useradd -o`, which would create a SECOND user sharing
    # the uid and make the SSH login resolve to the wrong name. Renaming keeps
    # the same uid/gid, so the bind-mounted home (owned by AGENT_UID) stays owned
    # by the login user.
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

    # Passwordless sudo for "agent" and the numeric uid, so it works whichever
    # name AGENT_UID resolves to (defense in depth after the rename).
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
    printf "PasswordAuthentication no\nKbdInteractiveAuthentication no\nPermitRootLogin no\nPubkeyAuthentication yes\n" \
      > /etc/ssh/sshd_config.d/00-computemcp.conf
    mkdir -p /run/sshd
    ssh-keygen -A
    # Start sshd even if the key check above warned; a real failure then shows
    # up in `docker logs` instead of crash-looping.
    /usr/sbin/sshd -t || true
    exec /usr/sbin/sshd -D -e
  '
BASH
```

Notes and invariants:

- **Bind the SSH port to loopback only** (`127.0.0.1:2222`), never `0.0.0.0`.
  The gateway reaches it through its own authenticated route connection; the
  container port must not be exposed publicly.
- **Persistence:** `--restart unless-stopped` and the bind-mounted `HOST_HOME`
  keep the toolchain and `agent` home across container recreation. Never recreate
  the container merely to restart it.
- **Install `ssh-keygen -A` before first boot** (as above): the container needs
  its own host keys, and the gateway pins that key.
- **GPU flags are creation-time only.** `--device`, `--gpus`, `--group-add`,
  `--security-opt` and `-p` are frozen in `HostConfig`; `stop`/`start` cannot add
  them. To change devices later, snapshot (`docker commit --change 'CMD
  ["/usr/sbin/sshd","-D","-e"]' computeMCP-container computeMCP-container-snapshot`), rename the old
  container, and re-run under the same name — never delete it blindly. Keep the
  image tag free of an environment suffix.
- **Driver policy:** the NVIDIA kernel driver lives on the host and MUST NOT be
  installed inside the container. Verify with `docker exec computeMCP-container nvidia-smi`.
- The `agent` user, `sudo` without password, and key-only auth (no passwords, no
  root login) match the gateway's default target (`user = "agent"`).
- **Single `agent` user, no duplicate uid.** The entrypoint reuses (renames) the
  base account that already owns `AGENT_UID` instead of `useradd -o`-ing a second
  one, so the SSH login and `whoami` both resolve to `agent` rather than the stock
  `ubuntu`. Passwordless sudo is granted to both `agent` and the numeric `#<uid>`
  so it keeps working whichever name `AGENT_UID` resolves to, and the whole config
  is checked with `visudo -c`.
- **The entrypoint must be idempotent.** Docker stores the `bash -euc '...'` as
  the container `Cmd` and re-runs it on **every** start. The first version of
  this recipe installed the packages and created the user unconditionally, so on
  the second start `groupadd`/`useradd` failed, `set -e` aborted before
  `exec sshd`, and `--restart unless-stopped` crash-looped forever. The guards
  above (`command -v sshd`, `getent group`, `id -u`) make each step a no-op when
  it already ran.
- **`ubuntu:24.04` already owns uid/gid `1000:1000`** (the stock `ubuntu` user).
  `AGENT_UID`/`AGENT_GID` come from the host home's owner (often `1000`), so that
  uid/gid is usually already taken. The entrypoint therefore renames the existing
  `AGENT_UID` account to `agent` (keeping the same uid/gid) instead of adding a
  duplicate; if you want `agent` to have its own distinct uid, set
  `AGENT_UID`/`AGENT_GID` to a free range in the `docker run -e` lines and
  `chown -R` the existing `HOST_HOME` to match.
- For the next step, the sshd the gateway pins is this container's — read it from
  inside (`/etc/ssh/ssh_host_ed25519_key.pub`) or scan the loopback port.

**Recreating a container changes its host key.** A freshly created container
generates new sshd host keys, so the `host_key_sha256` pinned in the gateway
config becomes stale and connections fail with
`Host key is not trusted for host`. After every recreate, read the new
fingerprint and update the target:

```bash
docker exec computeMCP-container ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
# -> SHA256:...  (use this as [targets.<name>] host_key_sha256)
computeMCP-gatewayctl --config ~/.config/computeMCP-gateway/config.toml target-refresh <name>
```

### AMD / HIP (ROCm) container variant

The gateway is GPU-agnostic (it only tunnels SSH), so an AMD target needs no
gateway change. Only the **dev container** recipe differs: replace the NVIDIA
`--device /dev/dri:/dev/dri --gpus all` with the ROCm device- and group-access
flags. `--gpus` (NVIDIA runtime) does **not** expose an AMD GPU; ROCm needs the
KFD and DRM character devices plus the `video`/`render` groups.

```bash
docker run -d \
  --name "$CONTAINER_NAME" \
  --restart unless-stopped \
  --device /dev/kfd --device /dev/dri \
  --security-opt seccomp=unconfined \
  --group-add video --group-add render \
  -p 127.0.0.1:2222:22 \
  --mount "type=bind,src=$HOST_HOME,dst=/home/agent" \
  -e "SSH_PUBLIC_KEY=$SSH_PUBLIC_KEY" \
  -e "AGENT_UID=$AGENT_UID" \
  -e "AGENT_GID=$AGENT_GID" \
  ubuntu:24.04 \
  bash -euc '
    # This Cmd re-runs on every container start, so every step must tolerate
    # already-existing state; otherwise `bash -euc` aborts before `exec sshd`
    # and `--restart unless-stopped` loops forever.
    if ! command -v sshd >/dev/null 2>&1; then
      apt-get update
      DEBIAN_FRONTEND=noninteractive apt-get install -y openssh-server sudo
    fi

    # Exactly one user named "agent" owns AGENT_UID. ubuntu:24.04 already ships
    # a user at uid 1000 ("ubuntu"); AGENT_UID is the host-home owner (usually
    # 1000), so that uid is normally already taken. Reuse (rename) the existing
    # account instead of `useradd -o`, which would create a SECOND user sharing
    # the uid and make the SSH login resolve to the wrong name. Renaming keeps
    # the same uid/gid, so the bind-mounted home (owned by AGENT_UID) stays owned
    # by the login user.
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

    # Passwordless sudo for "agent" and the numeric uid, so it works whichever
    # name AGENT_UID resolves to (defense in depth after the rename).
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
    printf "PasswordAuthentication no\nKbdInteractiveAuthentication no\nPermitRootLogin no\nPubkeyAuthentication yes\n" \
      > /etc/ssh/sshd_config.d/00-computemcp.conf
    mkdir -p /run/sshd
    ssh-keygen -A
    # Start sshd even if the key check above warned; a real failure then shows
    # up in `docker logs` instead of crash-looping.
    /usr/sbin/sshd -t || true
    exec /usr/sbin/sshd -D -e
  '
```

The same guards, host-key generation and persistence notes as the NVIDIA recipe
apply. Additional AMD-specific points:

- **`--security-opt seccomp=unconfined`** is effectively required: the default
  Docker seccomp profile blocks the `ioctls` ROCm needs (`kfd` / `amdgpu`),
  causing `HSA_STATUS_ERROR` or "no GPU".

- **`/dev/kfd` is the ROCm compute device**; `/dev/dri` (`renderD*`, `card*`) is
  the DRM/render device. Both are needed; `--gpus all` alone is not enough on
  AMD.

- **Group access:** the container process must be in the host's `video` and
  `render` groups to open those devices. `--group-add video --group-add render`
  resolves the names against the **container's** `/etc/group`; if the host GIDs
  differ (common on non-Ubuntu hosts, and `render` may be absent in the image),
  add the numeric host GIDs instead:

  ```bash
  --group-add "$(getent group video  | cut -d: -f3)" \
  --group-add "$(getent group render | cut -d: -f3)"
  ```

- **Driver policy (AMD):** the `amdgpu` / ROCm kernel driver lives on the **host**
  and MUST NOT be installed inside the container. The base `ubuntu:24.04` image
  ships **no ROCm user space** — the agent installs the matching ROCm userspace
  toolchain later, exactly as it installs CUDA toolkits for an NVIDIA target.
  Once installed, verify with `docker exec computeMCP-container rocminfo` (and
  `rocm-smi`), analogously to `nvidia-smi`.

- **Creation-time only:** as with the NVIDIA flags, `--device`, `--group-add`
  and `--security-opt` are frozen in `HostConfig`; they cannot be added by
  `stop`/`start`. Use the snapshot/rename/recreate procedure above.

### Obtain the container host-key fingerprint

For a tunnelled target, set `host_key_sha256` to the fingerprint of the sshd the
gateway dials — the development **container's** sshd (`remote_port`, usually
2222), not the host's own sshd on port 22. A `known_hosts` file cannot be used
reliably here because a tunnel maps an ephemeral local port.

Run this on the host, using an SSH alias that actually reaches the container:

```bash
# If the alias drops you into the container, read its own host key:
ssh <alias> 'cat /etc/ssh/ssh_host_ed25519_key.pub' | ssh-keygen -lf - | awk '{print $2}'

# Otherwise, have the alias scan the port-2222 sshd on its own loopback:
ssh <alias> 'ssh-keyscan -t ed25519 -p 2222 127.0.0.1 2>/dev/null' \
    | ssh-keygen -lf - | awk '{print $2}'
# => SHA256:l/zxYjSYKwDVXFzcUwD/buQoCiznnL+eMZaxL7GsNEE
```

Beware of entries for the plain hostname in `~/.ssh/known_hosts`: an unqualified
`hostname` means **port 22**, so its fingerprint is usually the wrong key. Look
for an explicitly port-qualified entry (`[host]:2222`) instead:

```bash
grep -i '<container-host>' ~/.ssh/known_hosts
ssh-keygen -lf ~/.ssh/known_hosts
```

Because a DNS name may only resolve on one network (e.g. a company LAN) and not
another (home/VPN), put both routes in `ssh_targets` so the gateway can fail
over: the first alias is tried, then the next. The pin is route-independent —
every route reaches the same container sshd, so one fingerprint covers them all.

## Example configuration

The full annotated file is [`config.example.toml`](config.example.toml). For
most users `computeMCP-gateway --bootstrap` writes this file for them; the
snippets below are the reference for editing it by hand. A minimal working
`~/.config/computeMCP-gateway/config.toml` is:

```toml
[server]
# Bind where Terok/Podman reaches the host (often host.containers.internal).
# Keep it loopback unless containers must connect from another address.
listen = "127.0.0.1"
port = 2222
exec_timeout = 900.0

[ssh]
internal_port_min = 31000
internal_port_max = 31999

[sessions]
idle_timeout = 3600.0
max_per_client = 16

[auth]
# Filled in by `--generate-tokens` (hashes only; keep plaintext out of it).
token_file = "/home/USER/.config/computeMCP-gateway/tokens.toml"

# Each Terok project gets its own token (the hash comes from the token file)
# and an explicit list of the targets it may use. "*" means every target.
[clients.alpaka]
label = "alpaka CI"
targets = ["hal", "fwk394"]

[clients.picongpu]
label = "PIConGPU"
targets = ["hal"]

[clients.admin]
label = "operator"
targets = ["*"]

# A target reached through an SSH tunnel. `ssh_targets` are aliases from the
# gateway user's SSH config, tried in order.
[targets.hal]
ssh_targets = ["hal", "ex_hal"]
remote_host = "127.0.0.1"
remote_port = 2222
user = "agent"
client_key = "/home/USER/.ssh/computemcp_container"
# Mandatory: pin the container host key (see "Configuration notes").
host_key_sha256 = "SHA256:REPLACE_WITH_CONTAINER_HOST_KEY_FINGERPRINT"
host_key_algorithms = ["ssh-ed25519"]
auto_connect = true

# A second target, reached without a tunnel (gateway co-located with the
# container, or a test endpoint). No `ssh_targets` => transport = "direct".
[targets.hal-direct]
transport = "direct"
remote_host = "host.containers.internal"
remote_port = 2222
user = "agent"
client_key = "/home/USER/.ssh/computemcp_container"
known_hosts = "/home/USER/.ssh/known_hosts"

# Another tunnelled target, used by the `alpaka` client above.
[targets.fwk394]
ssh_targets = ["fwk394", "ex_fwk394"]
remote_host = "127.0.0.1"
remote_port = 2222
user = "agent"
client_key = "/home/USER/.ssh/computemcp_container"
host_key_sha256 = "SHA256:REPLACE_WITH_CONTAINER_HOST_KEY_FINGERPRINT"
```

After editing, generate token hashes, then start the gateway. When the files
live in the conventional location, `--config` and `--generate-tokens` can be
omitted:

```bash
computeMCP-gateway --generate-tokens   # -> ~/.config/computeMCP-gateway/tokens.toml
computeMCP-gateway                     # -> ~/.config/computeMCP-gateway/config.toml
```

**Path resolution.** `--config` defaults to
`$XDG_CONFIG_HOME/computeMCP-gateway/config.toml` (or
`~/.config/computeMCP-gateway/config.toml`); an explicit `--config` always
wins. `--token-file` is resolved in this order: the flag, then
`[auth] token_file`, then a `tokens.toml` sitting next to the config **if it
exists**. A missing *explicit* token file is an error; a missing *conventional*
one is not (inline client tokens may still be in use). Create the config
directory once with `mkdir -p ~/.config/computeMCP-gateway`.

### Generate project tokens

Tokens are generated by the gateway, never hand-written. A per-project token is
issued once; only its `sha256` hash is stored on the gateway. The plaintext is
printed once for you to inject into that project's Terok environment.

```bash
# writes hashes to the default tokens.toml (chmod 600) and prints the tokens
computeMCP-gateway --generate-tokens

# or point somewhere else explicitly:
computeMCP-gateway --config /path/config.toml --generate-tokens /path/tokens.toml
```

Point the gateway at that file with `[auth] token_file = "..."` (or
`--token-file`). If neither is set, a `tokens.toml` next to `config.toml` is
used automatically. Then, in each Terok container set the matching plaintext token:

```
COMPUTEMCP_GATEWAY=http://host.containers.internal:2222
COMPUTEMCP_TOKEN=<that project's generated token>
```

The token is the only credential the container holds; it is never the host SSH
key and never committed to a repository.

### Interactive enrollment with `computeMCP-handshake`

Instead of pre-generating a token and copying it around, a container can ask for
one. Run `computeMCP-handshake` **inside the Terok container**; it queues a request,
waits while you approve it on the gateway console, receives the token, and wires
it into the container. This is step 3 of
[Set up the MCP inside a Terok task](#set-up-the-mcp-inside-a-terok-task).

```bash
computeMCP-handshake picongpu-bot-dev2 --port 2222 --system hal,fwk394
# --port must match [server] port in the gateway config (default 2222).
# --system is a comma-separated allow-list; omit it for an empty ACL
# (no targets) or pass --system '*' for all targets.
```

Flow:

```
container                                     gateway (host)
computeMCP-handshake <client> --port 2222          console:
   │  POST /v1/enroll  {client_id,targets}       gateway> enrollments
   ├──────────────────────────────────────────►  REQUEST ... CLIENT ... TARGETS
   │  (request queued, pending operator)         gateway> approve <request-id>
   │  GET /v1/enroll/<id>  (poll)              → mint token, append
   │◄──────── {status: approved, token} ──────    [clients.<id>] + tokens.toml,
   └ writes COMPUTEMCP_* to ~/.bashrc          reload
     and prints the MCP env snippet
```

The `/v1/enroll` request is **unauthenticated but grants nothing** — it only
places a bounded, expiring entry in a queue. Nothing is created until an
operator approves it (console `approve <id>`, or
`computeMCP-gatewayctl approve <id>` with an admin token). On approval the
gateway appends `[clients.<id>]` to `config.toml`, writes the token hash to
`tokens.toml`, reloads, and hands the plaintext token to the requester exactly
once; a poll secret proves ownership of the request.

Container-side writing:

- Default: an idempotent, marker-delimited block in `~/.bashrc` (re-running
  replaces it).
- `--env-file ~/.config/computeMCP/env`: write the token to that file (0600)
  and only add a `source` line to `~/.bashrc`, keeping the secret out of the
  shell history file.
- `--no-write`: don't touch any file; just print the MCP `environment` snippet.
- The running opencode will **not** see new shell variables (a long-lived
  tmux/session manager keeps its old environment); paste the printed
  `environment` block into the MCP entry, or restart from a fresh shell.

Turn it off with `[server] allow_enrollment = false`. `enroll_ttl` (default
600 s) and `enroll_max_pending` (default 32) bound the unauthenticated surface.

## Allow the gateway in the Terok Shield

Terok Shield is default-deny: a task container cannot open a TCP connection to
the gateway until the project explicitly allows the destination. If the MCP
fails with a connection error even though the gateway is healthy, this is the
cause. Add the gateway host to the project's `project.toml`:

```toml
shield:
  allow:
    - localhost:2222
  override:
    - host: 10.0.2.2
      reason: Temporary access to the HAL development tunnel
      expires: 2027-10-06
```

- `override.host` must be the **gateway host as the container sees it** — the
  address `host.containers.internal` resolves to inside the task (commonly
  `10.0.2.2`; verify with `getent hosts host.containers.internal`). It is not a
  host interface address, so do not try to bind it on the host.
- The `override` grants access to that exact gateway address (all ports to that
  address, not a specific port); set a **short, future `expires`** and remove it
  when no longer needed.
- A new task must be created after changing the Shield config; an existing task
  does not pick up later project changes.
- Adjust the port (`2222` above) to match `[server] port` in the gateway config.

## Set up the MCP inside a Terok task

Do these steps once per Terok task, after the gateway is running. They install
the MCP bridge, let the task ask the gateway for its token, and record that
token where the agent can read it.

1. **Install the MCP bridge in the task** (not on the host):

   ```bash
   python3 -m venv /home/dev/.local/share/computeMCP/venv
   /home/dev/.local/share/computeMCP/venv/bin/pip install <this-package>
   ln -s /home/dev/.local/share/computeMCP/venv/bin/computeMCP-mcp /home/dev/.local/bin/computeMCP-mcp
   ```

2. **Allow the gateway through the Terok Shield** (default-deny). See
   [Allow the gateway in the Terok Shield](#allow-the-gateway-in-the-terok-shield);
   without this the handshake cannot connect.

3. **Request access with the handshake.** This is the one command the task
   needs:

   ```bash
   computeMCP-handshake <client-id> --port 2222 --system hal,fwk394
   # --system is the target allow-list; omit for none, or pass '*' for all.
   ```

   The task prints a request id and waits. On the host, approve it:

   ```bash
   computeMCP-gatewayctl enrollments
   computeMCP-gatewayctl approve <request-id>
   ```

   On approval the gateway appends `[clients.<client-id>]`, writes the token
   hash, reloads, and hands the plaintext token to the task once.

4. **Point the MCP at the gateway.** The handshake writes `COMPUTEMCP_GATEWAY`
   and `COMPUTEMCP_TOKEN` to the task (default: a marked block in `~/.bashrc`;
   `--env-file` keeps the secret in a separate 0600 file). A running agent does
   not see new shell variables, so also paste the printed `environment` snippet
   into the MCP entry, or restart the agent from a fresh shell:

   ```json
   {
     "mcp": {
       "compute": {
         "type": "local",
         "command": ["/home/dev/.local/bin/computeMCP-mcp"],
         "enabled": true,
         "environment": {
           "COMPUTEMCP_GATEWAY": "http://host.containers.internal:2222",
           "COMPUTEMCP_TOKEN": "<project-specific-token>"
         }
       }
     }
   }
   ```

   `COMPUTEMCP_GATEWAY` is what the *container* uses to reach the host
   (`host.containers.internal`, not the host's own address);
   `COMPUTEMCP_TOKEN` is the per-project token.

Prefer the explicit `environment` block: an `export` in `~/.bashrc` (or a
login-shell config) does **not** reliably reach an already-running agent/TUI
because a long-lived `tmux`/session manager keeps its original environment.
Setting the variables in the MCP entry makes the token independent of shell
inheritance. The token then lives in `opencode.json`; keep that file out of any
repository and treat it as a secret, or inject it from a file the MCP reads, and
never commit it.

The MCP does not need SSH credentials and never learns `hal`, `ex_hal`,
ProxyJump aliases or internal forward ports.

### Agent skill

A ready-to-use skill that teaches an AI agent how to drive the `compute` MCP
(discover systems, run commands, parallel sessions, file transfer) ships in
[`skills/computeMCP/SKILL.md`](skills/computeMCP/SKILL.md). Install it
where the agent loads skills, for example:

```bash
mkdir -p ~/.config/opencode/skills/computeMCP
cp skills/computeMCP/SKILL.md ~/.config/opencode/skills/computeMCP/SKILL.md
```

Running `opencode run` remotely through `computeMCP_exec` may need `</dev/null`:
stdin is a non-TTY pipe there, and `opencode run` can wait on it until EOF. The
skill covers the symptom and the workaround.

## Configuration includes

A top-level `include` key splits the gateway configuration across files.
Relative paths in the list resolve against the directory of the including
file. Absolute paths are accepted as-is.

```toml
include = [
    "systems/rosi.toml",
    "systems/hal.toml",
    "/opt/computeMCP-gateway/systems/other.toml",
]

[server]
listen = "127.0.0.1"
port = 2222

[clients.alpaka]
targets = ["rosi", "hal"]
```

Each included file keeps its full `[targets.NAME]` structure, including the
node, allocation, slurm, and container tables. The loader parses each file
independently and then merges the tables. If two files set the same leaf
value, the merge is rejected and names both files. A missing include file or
a cycle among the includes is a load error. `include` keys are stripped from
the merged result, so the rest of the configuration is parsed as a plain
gateway file.

A reload reads the complete include graph, validates it, and applies it
atomically, so a broken include file leaves the active configuration intact.

## Slurm allocation and container configuration

When a target describes a Slurm node, an allocation policy, manual
`sbatch`/`srun` options, and an optional container runtime, the gateway
computes the resource plan, renders the per-stage scheduler arguments, and
exports them to the trusted `provision_command` as environment variables.
The login node needs only Bash for the argument bridge. The shipped
provisioning bundle, its auto-build flow, GPU vendor transitions, and
end-to-end lifecycle are documented in
[`scripts/computemcp-slurm/README.md`](scripts/computemcp-slurm/README.md).

### `[targets.X.node]`

Per-node capacity description. All fields are optional; an unset field carries
no capacity and the plan resolves it at the policy level (e.g. a missing
`gpus` makes `gpu-proportional` fall back to the full per-node CPU and
memory share).

| Key | Type | Notes |
| --- | --- | --- |
| `cpus` | positive integer | Slurm CPUs per node under the site's SMT policy (not physical cores) |
| `gpus` | positive integer | Scheduler-visible GPUs per node |
| `memory` | string | Allocatable host memory per node, e.g. `"378000M"`. A bare integer is MiB. Not installed RAM, not GPU memory |

Use these to tell the gateway what a node at this site can hand out. The
plan does not restate capacities as min/max ranges; `max-nodes` is the
independent upper bound.

### `[targets.X.allocation]`

Defaults and the allocation policies. One node is the default. A GPU system
defaults to one GPU per node; a CPU-only system can set `default-cpus`.

| Key | Accepts |
| --- | --- |
| `default-gpus` | positive integer; accepted but currently the mode itself fixes the GPU count (one GPU in `gpu-proportional`, the node capacity in `full`/`exclusive`) |
| `default-cpus` | positive integer; used by `cpu-proportional` as the CPU request when the node has no `cpus` description. With `node.cpus` set and no override, the full per-node capacity is the request |
| `single-node` | one of `gpu-proportional`, `cpu-proportional`, `full`, `exclusive` |
| `multi-node` | `full` or `exclusive` |
| `max-nodes` | positive integer; hard bound on the requested node count |

Mode semantics (per node):

| Mode | Calculated intent |
| --- | --- |
| `gpu-proportional` | CPU and memory scale with the requested GPU count. An integer per-GPU CPU share and a whole-MiB per-GPU memory share are derived from node capacities first, then multiplied by the requested GPUs. The division truncates and the preview shows the calculated result. |
| `cpu-proportional` | Memory scales with the requested CPU count against the node capacity. Intended for CPU-only targets. |
| `full` | The full configured per-node capacities, without an exclusivity flag. |
| `exclusive` | The full configured per-node capacities, plus the exclusivity intent. The intent alone does not emit `--exclusive`; the `exclusive` mapping (or a manual `exclusive = true`) does. |

### `[targets.X.slurm.sbatch]` and `[targets.X.slurm.srun]`

Free-form manual options for the two Slurm stages. Keys are option names
without the leading `--`. The value is a scalar, a boolean, or an array:

| Value form | Rendered |
| --- | --- |
| string or integer | `--key=value` |
| `true` | `--key` (bare flag) |
| `false` | omitted |
| array of strings/ints | `--key=value` repeated once per entry |

A stage absent from the config emits no argument at all for that stage. An
empty `[slurm]` table is equivalent to no Slurm block.

Protocol options the provisioning helper owns (`parsable`, `quiet`, `wrap`)
are rejected in manual options; the helper adds its own launcher flags.
Option names must be plain tokens (no leading `--`, no whitespace), and
a value containing a newline, carriage return, or NUL is rejected.

### `[targets.X.slurm.sbatch-map]` and `[targets.X.slurm.srun-map]`

Bounded mappings from calculated values to scheduler options. Each mapping
entry is optional; a missing entry emits nothing. The vocabulary is fixed:

| Calculated key | Mapping value | Emitted option |
| --- | --- | --- |
| `nodes` | `nodes` | `--nodes=N` |
| `gpus-per-node` | `gres` | `--gres=gpu:N` |
| `gpus-per-node` | `gpus-per-node` | `--gpus-per-node=N` |
| `cpus-per-node` | `cpus-per-task` | `--cpus-per-task=C`, only valid with one task per node in the same stage |
| `memory-per-node` | `mem` | `--mem=<MiB>M`, whole MiB with an explicit unit |
| `exclusive` | `exclusive` | `--exclusive` when the plan computed exclusivity; omitted otherwise |

The `cpus-per-node` -> `cpus-per-task` mapping requires a one-task-per-node
layout in the same stage. Set `ntasks-per-node = 1` (or `ntasks = 1`) in the
matching manual options. The validator rejects a mapping whose task-layout
precondition is not met.

A mapping and a manual option for the same family in the same stage are
rejected at load time, not silently prioritized: the error names the target,
stage, calculated field, and manual key. Alternative forms of the same
family conflict too: manual `mem` or `mem-per-cpu` against `memory-per-node`,
manual `gres`/`gpus`/`gpus-per-task`/`gpus-per-node` against `gpus-per-node`,
manual `nodes` or `n` against `nodes`, manual `exclusive` against `exclusive`.
The two stages are checked independently, so `slurm.sbatch-map` and
`slurm.srun-map` may map the same calculated value differently.

Without a mapping, no resource argument is generated for that value. The
gateway can still compute a plan for discovery and preview; the computed
value then appears in the preview's `not_emitted` list.

### `[targets.X.container]`

Describes the container runtime for the provisioning bundle.

| Key | Type | Notes |
| --- | --- | --- |
| `runtime` | `"apptainer"` or `"docker"` | Required whenever the block is present |
| `storage-root` | string | Base for the system state, sandbox, and home directories under. See the bundle README for the resulting layout |
| `image` | string | Base image. `docker://<ref>` for Apptainer, plain reference for Docker |
| `gpus` | string array | Subset of `nvidia`, `amd`, `intel`. Missing device nodes are reported and skipped |
| `host-home` | string | Host directory carrying `.ssh/authorized_keys` that the container trusts |
| `sandbox` | boolean | Informational flag; the helper reads the actual sandbox path |

### `[targets.X.bundle]`

Tells the gateway to deploy the shipped provisioning bundle over the route
connection and run it from there. No hand-placed copy is needed, and the
container's authorized public key is derived from `client_key`.

| Key | Type | Notes |
| --- | --- | --- |
| `source` | string | Bundled identifier. Currently `computemcp-slurm` |
| `deploy-dir` | string | Remote absolute directory on shared storage. Defaults to `<container.storage-root>/bundle` |
| `auto-deploy` | boolean | Default `true`: upload when the remote content marker differs. `false` pins the already-deployed copy, even after a gateway upgrade |

The deploy directory must be visible to the login node (which runs
`computemcp-provision.sh` and builds the container) and to the compute node
(which runs `computemcp-job.sh` and starts the container). On most clusters
`/tmp` is per-node and not shared, so the default lives under `storage-root`.
The gateway writes files atomically and never touches `authorized_keys`,
allocation state, or a running job. Set `provision_command`/`close_command`
explicitly to manage the bundle by hand instead.

### Worked example: GPU target with Apptainer (ROSI illustration)

This template mirrors the design-document ROSI illustration: one GPU per
node by default, `ntasks-per-node = 1` in both stages, and a manual
`mem = "100G"` that opts the target out of the calculated memory share.
The `cpus-per-node -> cpus-per-task` mapping is legal because each stage's
manual options pin the one-task-per-node layout. Replace the partition,
the `/home/USER` paths, and the storage paths with your site values.

```toml
[targets.rosi]
ssh_targets = ["rosi"]
user = "agent"
client_key = "/home/USER/.ssh/computemcp_container"
host_key_sha256 = "SHA256:REPLACE_WITH_CONTAINER_HOST_KEY_FINGERPRINT"
host_key_algorithms = ["ssh-ed25519"]
host_key_check = "on"
auto_connect = true
sharing = "exclusive"
provision_timeout = 960.0

# Let the gateway deploy and run the shipped bundle over the route connection.
# The upload is skipped when the remote content marker matches; after a gateway
# upgrade the changed bundle is re-deployed on the next connect.  deploy-dir
# defaults to <container.storage-root>/bundle and must be on storage visible to
# login and compute nodes.
[targets.rosi.bundle]
source = "computemcp-slurm"
# deploy-dir = "/scratch/USER/computemcp/bundle"
# auto-deploy = true          # false pins the copy already on the login node

# Node capacities.  The numbers mirror the reviewed ROSI PIConGPU template,
# not current verified cluster hardware:
[targets.rosi.node]
cpus = 24
gpus = 4
memory = "378000M"

# Defaults and policies:
[targets.rosi.allocation]
default-gpus = 1
single-node = "gpu-proportional"
multi-node = "exclusive"
max-nodes = 4

# Manual submission options.  The map fills the resource numbers on top.
[targets.rosi.slurm.sbatch]
partition = "REPLACE_WITH_PARTITION_NAME"
time = "02:00:00"
mem = "100G"                # fixed memory; the calculated share is not emitted
ntasks-per-node = 1

# Calculated-value mappings for submission:
[targets.rosi.slurm.sbatch-map]
nodes = "nodes"
gpus-per-node = "gres"
cpus-per-node = "cpus-per-task"
exclusive = "exclusive"
# No memory mapping: the manual mem option is authoritative.

# Job-step launch options:
[targets.rosi.slurm.srun]
ntasks-per-node = 1
cpu-bind = "none"

[targets.rosi.slurm.srun-map]
nodes = "nodes"
cpus-per-node = "cpus-per-task"

# Container runtime for the provisioning bundle:
[targets.rosi.container]
runtime = "apptainer"
storage-root = "/scratch/USER/computemcp"
image = "docker://ubuntu:24.04"
gpus = ["nvidia"]
host-home = "/scratch/USER/computemcp/home"
sandbox = true
```

For the node above, one GPU per node computes 6 CPUs (24/4) and 94500 MiB of
per-node memory (378000/4). With `--set gpus-per-node=2`, the same mode
computes 12 CPUs and 189000 MiB. The rendered `COMPUTEMCP_SBATCH_ARGS` is:

```
--partition=REPLACE_WITH_PARTITION_NAME
--time=02:00:00
--mem=100G
--ntasks-per-node=1
--nodes=1
--gres=gpu:2
--cpus-per-task=12
```

The calculated `memory-per-node` does not emit an `--mem` option because no
mapping is configured; the preview lists it under `not_emitted` and the
fixed manual option remains authoritative.

### Worked example: CPU-only target with Docker

CPU-only target. `cpu-proportional` requests the full per-node CPU capacity
by default (16 here), memory follows the same ratio (128 GiB for the whole
node), and the two mappings emit the CPU and memory requests. `default-cpus`
applies when the node description has no `cpus` key.

```toml
[targets.cpuhost]
ssh_targets = ["cpuhost"]
user = "agent"
client_key = "/home/USER/.ssh/computemcp_container"
host_key_sha256 = "SHA256:REPLACE_WITH_CONTAINER_HOST_KEY_FINGERPRINT"
host_key_algorithms = ["ssh-ed25519"]
auto_connect = false
sharing = "unknown"
provision_timeout = 600.0

[targets.cpuhost.bundle]
source = "computemcp-slurm"

# Node capacities: no GPUs described.
[targets.cpuhost.node]
cpus = 16
memory = "128G"

# Policies.  default-cpus applies when node.cpus is unset; with cpus set
# (16 here) the full per-node capacity is the request and this value is
# recorded for completeness.
[targets.cpuhost.allocation]
default-cpus = 4
single-node = "cpu-proportional"

[targets.cpuhost.slurm.sbatch]
partition = "REPLACE_WITH_PARTITION_NAME"
time = "01:00:00"
ntasks-per-node = 1

[targets.cpuhost.slurm.sbatch-map]
cpus-per-node = "cpus-per-task"
memory-per-node = "mem"

[targets.cpuhost.slurm.srun]
ntasks-per-node = 1
cpu-bind = "none"

[targets.cpuhost.slurm.srun-map]
cpus-per-node = "cpus-per-task"

# Container runtime for the provisioning bundle:
[targets.cpuhost.container]
runtime = "docker"
storage-root = "/scratch/USER/computemcp"
image = "ubuntu:24.04"
```

With the defaults, the plan resolves to 16 CPUs and 131072 MiB of per-node
memory (the full 128 GiB node capacity). A `--set cpus-per-node=8` override
scales memory to the same ratio (half the node) and renders
`--cpus-per-task=8 --mem=65536M`.

### Connect-time overrides, refresh, and dry-run preview

`computeMCP-gatewayctl target-connect <t>` and `target-refresh <t>` accept
repeatable `--set KEY=VALUE` entries. The `--dry-run` flag renders the
allocation without connecting. Valid `--set` keys: `nodes`,
`gpus-per-node`, `cpus-per-node`, `mem-per-node`, `mode`. The `mode` value
is one of the allocation modes; a multi-node `mode` limited to `full` and
`exclusive` follows the same rule as the configuration.

```bash
computeMCP-gatewayctl target-connect rosi --set gpus-per-node=2
computeMCP-gatewayctl target-connect rosi --set nodes=2 --set cpus-per-node=12 --set mem-per-node=100G
# Dry-run: print the plan without connecting or allocating
computeMCP-gatewayctl target-connect rosi --set gpus-per-node=2 --dry-run
```

The preview prints the planned intent (mode, nodes, per-node calculations,
exclusive), the manual settings per stage, the rendered arguments per stage,
the calculated fields not emitted, and the final `COMPUTEMCP_SBATCH_ARGS`
and `COMPUTEMCP_SRUN_ARGS`. The HTTP equivalent is
`POST /v1/targets/{target}/preview` with the same override body.

A preview changes no live allocation. For a connected target whose current
allocation differs from the previewed one, the result carries
`needs_refresh: true` and a warning.

#### Lifecycle semantics

- **Disconnected connect**: applies the allocation defaults plus any
  `--set` overrides.
- **Connected target, no overrides**: preserves the active allocation; the
  target keeps its resolved settings.
- **Connected connect with differing settings**: reports a mismatch
  (`needs_refresh: true` with a warning). Nothing is torn down, and the
  resulting allocation continues on the connection.
- **Refresh** (`target-refresh <t> --set …`): applies the overrides to the
  retained resolved settings and recreates the allocation
  (releases the old one via `close_command`, then provisions a new one).
- **Recovery without overrides**: `target-refresh <t>` re-applies the
  target's retained resolved settings, not the configured defaults, so the
  operator's previous `--set` request continues to apply after a reconnection
  triggered by a route loss or a restart.
- **Validation**: overrides resolve before mapping; an explicit partial
  GPU/CPU request that conflicts with a `full`/`exclusive` multi-node policy
  is an error, not a silent replacement.

### Environment transport contract

The gateway exports the resolved allocation and container description as
`COMPUTEMCP_*` environment variables around the existing
`provision_command`, over the authenticated route connection:

- `COMPUTEMCP_SBATCH_ARGS` and `COMPUTEMCP_SRUN_ARGS`: one complete argument
  per line, no trailing newline. An empty value means an empty argument list.
  The provisioning helper parses each with `mapfile -t` into Bash arrays and
  does not use `eval` or unquoted expansion.
- `COMPUTEMCP_NODES`, `COMPUTEMCP_CPUS_PER_NODE`,
  `COMPUTEMCP_GPUS_PER_NODE`, `COMPUTEMCP_MEMORY_PER_NODE_MIB`,
  `COMPUTEMCP_EXCLUSIVE`, `COMPUTEMCP_MODE`: the resolved plan. Numeric
  per-node fields are empty when the plan has no value; the other three are
  concrete.
- `COMPUTEMCP_SYSTEM`, `COMPUTEMCP_CONTAINER_RUNTIME`,
  `COMPUTEMCP_STORAGE_ROOT`, `COMPUTEMCP_IMAGE`,
  `COMPUTEMCP_GPU_VENDORS`, `COMPUTEMCP_HOST_HOME`,
  `COMPUTEMCP_SANDBOX`: the container description from
  `[targets.X.container]`.

`sbatch` and `srun` remain separate stages: the helper submits
`sbatch "${SBATCH_ARGS[@]}" …`; the batch job launches
`srun "${SRUN_ARGS[@]}" …`. No value moves from one list to the other.

### Requirements and validation

- The helper on the login node parses both `*_ARGS` variables with
  `mapfile -t` into Bash arrays and runs them quoted; `eval` and unquoted
  expansion are not used. See
  [`scripts/computemcp-slurm/README.md`](scripts/computemcp-slurm/README.md)
  for the full bridge and the manual-fallback path.
- The `cpus-per-node` -> `cpus-per-task` mapping is re-checked at plan time
  in addition to the load-time check, so the two stages stay semantically
  aligned as the config evolves.
- A request that prints `--nodes=N` with `N > 1` passes the gateway-side
  allocation, but the provisioning helper exits with `multi-node not yet
  supported` before `sbatch`. Multi-node allocation is not currently
  supported end-to-end.
- `reload` re-runs the conflict validation (mappings and manual options)
  over the loaded config; a load-time error rejects the new config and the
  previous one stays active.

## Run the gateway

```bash
# interactive (operator console); uses the default config location
computeMCP-gateway

# headless (systemd)
computeMCP-gateway --no-console

# or point at an explicit file
computeMCP-gateway --config /path/to/config.toml
```

Console commands: `targets`, `status [target]`, `connect`, `refresh`,
`reconnect`, `stop`, `connect-all`, `stop-all`, `reload`, `clients`,
`client <name>`, `client-refresh|connect|stop <name> [target]`,
`client-kill <name>`, `sessions [target]`, `close-session <id>`, `quit`.
`connect` and `refresh` accept `--2fa SECRET` for an interactive target, for
example `connect --2fa SECRET hal`.

`reload` fully parses and validates the TOML before replacing the active
configuration; on failure the old configuration is retained. Unchanged
connected targets keep their tunnels, removed targets are stopped, and changed
targets are marked `needs_refresh`. `reload` also re-reads the token file, so
adding or rotating a project token is a reload away.

## Operator terminal: refresh config and manage API keys

Under systemd the gateway runs `--no-console`, so use the
`computeMCP-gatewayctl` operator CLI. It talks to the running gateway's
authenticated API (nothing needs to be stopped or restarted) and reads the
plaintext token from `[auth] token_file` or `COMPUTEMCP_TOKEN`.

### Refresh the configuration

```bash
# re-read config.toml + tokens.toml on the running gateway
computeMCP-gatewayctl --config ~/.config/computeMCP-gateway/config.toml reload

# under systemd, equivalent and cleaner:
systemctl --user reload computeMCP-gateway     # sends SIGHUP
```

`systemctl --user reload` sends `SIGHUP`; the daemon re-parses the config and
token file without dropping live tunnels. A malformed file is logged and the
previous configuration is kept.

### See each API key (Terok client), its systems and live sessions

```bash
$ computeMCP-gatewayctl --config config.toml clients
CLIENT            LABEL                 TARGETS                SESSIONS
alpaka            alpaka CI             hal                    1
picongpu          PIConGPU              hal-dedicated          0
admin             operator              *                      0

$ computeMCP-gatewayctl --config config.toml client alpaka
client:    alpaka
label:     alpaka CI
token fp:  sha256:dfa2d0e94f1f        # fingerprint only; the token is never shown
allow_all: False
targets:   hal
sessions:  1
  4rF8_FtwECBBLc_Vmq4WzRp4  target=hal  idle=3.2
```

### Activate / refresh / stop connections for one API key

```bash
# activate every target this key may use
computeMCP-gatewayctl --config config.toml client-connect alpaka
# refresh (re-run route failover) only that key's targets
computeMCP-gatewayctl --config config.toml client-refresh alpaka
# or a single target of that key
computeMCP-gatewayctl --config config.toml client-refresh alpaka hal
computeMCP-gatewayctl --config config.toml client-stop alpaka
```

Only targets inside the key's ACL are touched; anything else is refused.

### Other options

```bash
computeMCP-gatewayctl --config config.toml status
computeMCP-gatewayctl --config config.toml --add-target   # wizard: add a system
computeMCP-gatewayctl --config config.toml target-connect hal
computeMCP-gatewayctl --config config.toml target-refresh hal
computeMCP-gatewayctl --config config.toml target-stop hal
# interactive targets: pass the per-request second factor
computeMCP-gatewayctl --config config.toml target-connect --2fa SECRET hal
computeMCP-gatewayctl --config config.toml target-refresh --2fa SECRET hal
computeMCP-gatewayctl --config config.toml sessions          # all clients (admin)
computeMCP-gatewayctl --config config.toml client-kill alpaka # close its sessions
computeMCP-gatewayctl --config config.toml --json clients
computeMCP-gatewayctl --config config.toml --client alpaka clients  # ACL-checked
```

`clients`, `client*` and the all-clients `sessions` view require an admin
(`targets = ["*"]`) token; other clients receive `403`. Token values and full
hashes are never returned — only a short `sha256:` fingerprint.

`--timeout` is optional. For `target-connect` and `target-refresh` (and
`client-connect`/`client-refresh`) the CLI waits for the target's
`provision_timeout` plus a short handshake margin, because a slow
`provision_command` can legitimately run that long. This means `provision_timeout`
also bounds how long the CLI waits. Pass `--timeout SECONDS` before or after the
subcommand to override it for one call; the subcommand form wins over the
global one. For every other command the timeout stays at 60 seconds unless
`--timeout` is given.

Add a human label to any client so the listings are readable:

```toml
[clients.alpaka]
label = "alpaka CI"
targets = ["hal", "fwk394"]
```

## Configuration notes

- `ssh_targets` are SSH config aliases used **in priority order**. The gateway
  resolves each alias with `ssh -G` (host, user, port, identityfile, ProxyJump)
  and keeps the first working route. A `proxycommand` (rather than a ProxyJump
  alias) is rejected with a clear error. Only TOML values are ever passed to
  `ssh`.
- `transport = "tunnel"` (default when `ssh_targets` is present) forwards to
  `remote_host:remote_port` through the host. `transport = "direct"` connects
  straight to `remote_host:remote_port` (useful for tests or a co-located
  gateway) and skips the SSH tunnel.
- Internal forward ports always bind `127.0.0.1` and are allocated from
  `[ssh] internal_port_min..internal_port_max`.
- Host-key verification is on by default. A target must set either
  `host_key_sha256` or `known_hosts`; otherwise the connection is refused.
  Because a tunnel maps an ephemeral port, fingerprint pinning via
  `host_key_sha256` is the recommended mode. Restrict `host_key_algorithms`
  (e.g. `["ssh-ed25519"]`) to the pinned key's algorithm when a server offers
  several.
- `host_key_check = "off"` explicitly disables host-key verification and
  accepts any host key; `host_key_algorithms` (if set) then only restricts
  negotiation and no longer verifies the server's identity. It is only safe when
  the forwarded endpoint itself is trusted (e.g. a single-user dev box); never
  use it on a host where another user could hijack the forwarded port. A warning
  is logged for every such target on connect. `host_key_check` itself is
  optional, but `host_key_algorithms` should still be set explicitly (see
  below).

  ```toml
  [targets.fwk388]
  ssh_targets = ["fwk388", "ex_fwk388"]
  user = "agent"
  client_key = "/home/USER/.ssh/computemcp_container"
  host_key_check = "off"     # accept any host key (trusted dev box only)
  host_key_algorithms = ["ssh-ed25519"]  # needed even with host_key_check = "off"
  ```
- Set `host_key_algorithms = ["ssh-ed25519"]` whenever the container sshd
  offers several host-key algorithms (for example dropbear, which offers
  `ssh-ed25519`, `rsa-sha2-256` and `ssh-rsa`). This is required even when
  `host_key_check = "off"`: `"off"` only disables verification of the server's
  identity, it does not control which host-key algorithm is negotiated. Without
  an explicit `host_key_algorithms`, asyncssh can pick an algorithm the server
  cannot complete key exchange with and the connection aborts with
  `Connection lost`. Restrict it to an algorithm the server supports, e.g.
  `["ssh-ed25519"]`.
- `host_key_algorithms` must be a TOML array of strings (`["ssh-ed25519"]`),
  not a quoted string; a string is treated as a sequence of characters and the
  gateway fails with `ValueError: s is not a valid host key algorithm`.
- `connect_mode` controls concurrency per target:
  - `"shared"` (default): all exec/sessions multiplex over one cached SSH
    connection. Fast, but bounded by the remote sshd's `MaxSessions`
    (OpenSSH default 10) for simultaneous channels.
  - `"dedicated"`: each exec/session opens its own SSH connection and closes it
    afterward, so the `MaxSessions` limit no longer bounds total parallel
    sessions. Costs one handshake per operation.
- Interactive (second-factor) authentication: set `interactive_auth = true`
  when the LOGIN/ROUTE connection to the target requires a second factor
  instead of accepting the key alone. The factor can be a one-time password
  (keyboard-interactive/OTP), a password, or the passphrase of the SSH client
  key. It is used once per request and is never persisted or logged.

  When `interactive_auth = true` the gateway does not connect at startup or on
  config reload: it overrides and ignores `auto_connect`. The operator supplies
  the factor per request on the operator CLI or console. If a request omits it,
  the gateway logs a warning and does not connect or refresh, so the target
  state is unchanged, and the CLI/console print the warning. `exec`/session on
  such a disconnected target returns a clear error pointing at
  `target-connect --2fa`.

  When `interactive_auth` is false or absent and a `--2fa` factor IS given, the
  gateway warns, ignores the factor, and proceeds with the normal key-based
  connect.

  The container hop stays key-based; there is no separate container password
  prompt. The old console `getpass` container prompt was removed.

  ```toml
  [targets.hal]
  ssh_targets = ["hal", "ex_hal"]
  user = "agent"
  interactive_auth = true
  host_key_sha256 = "SHA256:..."
  ```

  Operator CLI:

  ```bash
  computeMCP-gatewayctl --config config.toml target-connect --2fa SECRET hal
  computeMCP-gatewayctl --config config.toml target-refresh --2fa SECRET hal
  ```

  Console transcript:

  ```
  gateway> connect --2fa SECRET hal
  {'name': 'hal', 'state': 'connected', ...}
  ```

- Route loss, and how targets recover:
  - Non-interactive targets auto-reconnect with backoff. Reconnection re-runs
    provisioning, so `provision_command` must stay idempotent.
  - Interactive targets FAIL CLOSED: they do NOT auto-reconnect, because the
    second factor may have rotated. The gateway leaves them disconnected and
    sets an actionable `last_error` (pointing at `target-connect --2fa`).

- `route_host_key_sha256` pins the LOGIN node host key. Unset uses the local
  `~/.ssh/known_hosts`, the previous behavior. It is distinct from
  `host_key_sha256`, which pins the CONTAINER key.

  ```toml
  [targets.rosi5]
  ssh_targets = ["rosi5"]
  # route_host_key_sha256 = "SHA256:..."   # login node; falls back to known_hosts
  host_key_sha256 = "SHA256:..."           # container sshd
  ```

- `sharing` documents whether a system is dedicated or shared, so agents can
  judge benchmark reliability. `computeMCP_targets()`/`computeMCP_status()` return it:
  - `"exclusive"` — dedicated to this task (e.g. a whole Slurm allocation);
    benchmarks are meaningful.
  - `"shared"` — other users/jobs may run concurrently; timings can be noisy.
  - `"unknown"` — not declared (default).

  ```toml
  [targets.rosi5]
  ssh_targets = ["rosi5"]
  sharing = "exclusive"     # Slurm allocation is ours
  ```

- `node_info` is an optional list of free-form, operator-provided, unstructured
  hints about the underlying system (for example `"x86 CPU"` or `"AMD GPU"`).
  There is no fixed schema or meaning; agents treat them as starting
  hypotheses and verify the actual hardware. Unset or empty means no extra
  information was provided, so the agent discovers the system itself or the
  user guides it. `computeMCP_targets()`/`computeMCP_status()` return it.

  ```toml
  [targets.rosi5]
  ssh_targets = ["rosi5"]
  sharing = "exclusive"     # Slurm allocation is ours
  node_info = ["x86 CPU", "AMD GPU"]
  ```

- `agent` is an optional ordered list of remote AI agents this target can
  delegate work to, such as a test run or an implementation. Each entry is a
  table with exactly `agent` and `model`, both non-empty strings (spaces are
  allowed in both). The list order is the priority order: the caller should
  try the entries in order and fall back to the first working one.
  Unset or empty means no remote agent is configured.
  `computeMCP_targets()`/`computeMCP_status()` return it as a list of
  `{"agent": ..., "model": ...}` objects. Remote agents do not have the Terok
  skills, so bring their results back for local review under the Terok rules.
  This config field is unrelated to the SSH `user = "agent"` account name.

  ```toml
  [targets.rosi5]
  ssh_targets = ["rosi5"]
  sharing = "exclusive"     # Slurm allocation is ours
  agent = [
    { agent = "opencode", model = "GWen 3.5" },
    { agent = "codex", model = "Sole" },
  ]
  ```

- HPC / Slurm targets: a fixed login node can be reached with `proxy_jump`,
  while the dynamic compute node is discovered at connect time by a trusted
  `provision_command` (see the next subsection).

### HPC / Slurm: dynamic compute nodes

On an HPC system the login node is fixed, but the development container runs in
a Slurm job on a compute node whose name (and the forwarded port) only exist
once the job starts. The gateway supports this with `provision_command`, a
trusted script that acquires the node and prints the endpoint to dial. A
config-driven bundle ships as
[`scripts/computemcp-slurm/`](scripts/computemcp-slurm/README.md): it builds
the container, submits the allocation from the gateway-rendered arguments
(see "Slurm allocation and container configuration"), starts a relay, and
prints the endpoint. Use the bundle for new Slurm targets; the operator-written
example below remains useful when the site scripts already own the job.

How the pieces connect:

```
compute-gateway (host)
  |  authenticated SSH route connection to rosi5        (your local key)
  v
rosi5 login node
  |  login-node forward 127.0.0.1:2200 -> <compute-node>:2222  (login node's key)
  v
<compute-node>  ->  127.0.0.1:2222  (development container sshd)
```

`provision_command` does the middle step (submit/wait for the job and create the
login-node forward) and prints `127.0.0.1:2200`; the gateway then dials it
through `ssh_targets`.

Config:

```toml
[targets.rosi5]
user = "agent"
client_key = "/home/USER/.ssh/computemcp_container"
host_key_sha256 = "SHA256:..."     # pin of the container host key
ssh_targets = ["rosi5"]            # SSH config alias of the login node
# Optional: pin the LOGIN node host key. Unset falls back to the local
# ~/.ssh/known_hosts. Distinct from host_key_sha256 (the container key).
# route_host_key_sha256 = "SHA256:..."
provision_command = ["/home/USER/.config/computeMCP-gateway/rosi5-provision.sh"]
provision_timeout = 900.0
```

`provision_command` now runs ON the remote machine over the authenticated route
connection, not as a local subprocess and not via a separate `ssh -T`. Its
contract is unchanged: it prints the first `host:port` (an optional `ENDPOINT `
prefix is allowed) and always yields a free forward channel. The gateway
forwards that endpoint over the same connection (`forward_local_port`), so the
external `ssh -N -L` tunnel is gone.

**The contract** (what the script must do):

1. Ensure a Slurm job is running for this target (reuse or submit).
2. Ensure a listener `127.0.0.1:<port>` exists on the login node that forwards
   to the container's SSH port on the compute node.
3. Print that endpoint as `host:port` on stdout (an optional `ENDPOINT ` prefix
   and other log lines are fine — the first valid `host:port` line wins).

The minimal possible script, useful for testing the wiring:

```bash
#!/bin/sh
# minimal contract example: just print the endpoint the gateway can dial
printf 'ENDPOINT 127.0.0.1:2200\n'
```

A realistic operator script (the gateway can supply `COMPUTEMCP_SBATCH_ARGS`
when the target carries an allocation, so the job request follows the gateway's
plan; the `sleep infinity` step predates the current bundle):

```bash
#!/usr/bin/env bash
set -euo pipefail

JOB_NAME="terok-dev"
CONTAINER_PORT=2222                              # container's sshd port on the node
LOGIN_FORWARD_PORT=2200
SBATCH_ARGS=()
if [ -n "${COMPUTEMCP_SBATCH_ARGS:-}" ]; then
    mapfile -t SBATCH_ARGS <<< "$COMPUTEMCP_SBATCH_ARGS"
fi

# 1. Reuse a running job for this target, or submit one that stays alive.
#    When the gateway exports COMPUTEMCP_SBATCH_ARGS, the rendered plan
#    replaces the inline defaults below.
#    (The shipped bundle wraps the same idea into computemcp-job.sh and keeps
#    the allocation alive with a helper script instead of --wrap.)
jobid="$(squeue -h -u "$USER" -n "$JOB_NAME" -t R -o '%A' | head -n1 || true)"
if [ -z "$jobid" ]; then
    jobid="$(sbatch --parsable --job-name "$JOB_NAME" "${SBATCH_ARGS[@]:---nodes=1 --time=08:00:00}" --wrap 'sleep infinity')"
fi

# 2. Wait until the job is running and report its node.
node=""
for _ in $(seq 1 120); do
    node="$(squeue -h -j "$jobid" -t R -o '%N' | head -n1 || true)"
    [ -n "$node" ] && break
    sleep 5
done
[ -n "$node" ] || { echo "job $jobid never started" >&2; exit 1; }

# 3. Create the login-node listener if it is not already up. The script runs on
#    the login node over the route connection, so the login -> compute hop uses
#    the login node's own keys/ssh-agent with no extra ssh hop.
if ! ss -ltn 2>/dev/null | grep -q "127.0.0.1:${LOGIN_FORWARD_PORT}"; then
    nohup ssh -N -o ExitOnForwardFailure=yes -o BatchMode=yes \
      -L 127.0.0.1:${LOGIN_FORWARD_PORT}:${node}:${CONTAINER_PORT} ${node} \
      >/dev/null 2>&1 &
fi

# 4. Print the endpoint; the gateway reaches it through ssh_targets=["rosi5"].
printf 'ENDPOINT 127.0.0.1:%s\n' "$LOGIN_FORWARD_PORT"
```

The compute node and the port may change per job; the only contract is the
printed endpoint. `refresh rosi5` re-runs the script and follows the new node.

Alternative: if your site lets the gateway reach the compute node directly
(both hops accept the gateway's key), skip the login-node listener entirely —
print `ENDPOINT <compute-node>:<port>` and add `proxy_jump = "rosi5"`, so the
gateway dials `rosi5` first and forwards straight to the node over that
connection.

Troubleshooting: run the script by hand first — the gateway logs its stdout and
includes its stderr in the target's `last_error`, and the discovered address is
shown as `provisioned_endpoint` in `status`/`GET /v1/targets/{name}`.

### Releasing the allocation (`close_command`)

`provision_command` acquires a Slurm job, so stopping the target must release it
again or the allocation is left behind. `close_command` is the symmetric trusted
argv that runs ON the remote machine over the live route connection, for example
`scancel` of the job the target acquired.

```toml
[targets.rosi5]
provision_command = ["/home/USER/.config/computeMCP-gateway/rosi5-provision.sh"]
close_command = ["scancel", "--name", "terok-dev"]
close_command_timeout = 120.0
```

`close_command` runs on an explicit operator stop (the `target-stop`,
`client-stop` and console `stop`/`stop-all` paths trigger it), on gateway
shutdown, and before a target refresh so the old job is released before
`provision_command` acquires a fresh one. It does NOT run when a config reload
removes a target, because that removal only drops the target from the running
config. Exit status and output are advisory: a non-zero exit or a timeout is
logged as a warning and teardown proceeds. It has its own
`close_command_timeout`, default 120 seconds.

### Recovering a stopped container (`connect_command`)

The tunnel can be healthy while the development **container itself is stopped**.
`provision_command` does not help there — it discovers an endpoint, it does not
start the container. Use `connect_command` for that: a trusted argv that the
gateway runs **on the remote host** (the machine hosting the container) through
the target's SSH route when the container cannot be reached.

```toml
[targets.fwk388]
ssh_targets = ["fwk388", "ex_fwk388"]
user = "agent"
client_key = "/home/USER/.ssh/computemcp_container"
host_key_sha256 = "SHA256:..."
connect_command = ["/home/USER/.config/computeMCP-gateway/ensure-container.sh"]
connect_command_timeout = 120.0
connect_command_mode = "on_failure"     # default; "always" runs pre-connect
```

Behaviour:

- `connect_command_mode = "on_failure"` (default): run the command only after the
  container connection fails, then retry the connection once. A healthy
  container never pays for it.
- `connect_command_mode = "always"`: run the command **before every** connection
  attempt. This is simpler to reason about but the script **must be an
  idempotent no-op when the container already runs**; otherwise every call pays
  the command's cost.
- Exit status is advisory: the gateway always re-probes/retries and only reports
  success if the container is really reachable. stdout/stderr are logged.
- The command runs ON the remote machine over the authenticated route
  connection, so it executes on the remote host, not on the gateway. There is
  no separate `ssh -T` subprocess.

A reference script ships as
[`scripts/ensure-container.sh`](scripts/ensure-container.sh). It inspects the
`computeMCP-container` (override with `COMPUTEMCP_CONTAINER_NAME`), starts it if
it is not running, waits for `running`, and is a **no-op when it already runs**
(so it is safe with `connect_command_mode = "always"`). Docker and Podman are
both supported. Copy it to the remote host and reference its absolute path:

```bash
scp scripts/ensure-container.sh <remote-host>:~/.config/computeMCP-gateway/
ssh <remote-host> chmod +x ~/.config/computeMCP-gateway/ensure-container.sh
```

Run it by hand first to verify it detects and starts the container before wiring
it into the gateway.

Troubleshooting: run the script by hand first — the gateway logs its stdout and
includes its stderr in the target's `last_error`, and the discovered address is
shown as `provisioned_endpoint` in `status`/`GET /v1/targets/{name}`.

- File transfer: small files use `computeMCP_file_read`/`computeMCP_file_write`
  (content in the response). For large or binary files use
  `computeMCP_file_upload`/`computeMCP_file_download`, which stream over SFTP and
  never place file bytes in the tool output. `computeMCP_file_upload_tree` mirrors
  a whole directory incrementally (`skip_existing`, `include`/`exclude` globs),
  and `computeMCP_file_download(..., recursive=True)` mirrors a remote tree.
  Permissions can be set with `computeMCP_file_chmod`.
- Command execution supports `env` (exported in the remote shell, so it works
  even when the container sshd does not accept env) and `stdin`. Persistent
  reads accept `wait=<seconds>` to block for new output instead of polling.

## HTTP API (all requests require `Authorization: Bearer <token>`)

```
GET    /v1/health
GET    /v1/targets
GET    /v1/targets/{target}
POST   /v1/targets/{target}/connect | /refresh | /stop | /preview
POST   /v1/exec
POST   /v1/sessions ; GET /v1/sessions ; GET|DELETE /v1/sessions/{id}
POST   /v1/sessions/{id}/write | /read | /resize
POST   /v1/reload                          # admin: re-read config + tokens
GET    /v1/clients ; GET /v1/clients/{name}          # admin: token/ACL/sessions
GET    /v1/clients/{name}/sessions                   # admin
DELETE /v1/clients/{name}/sessions                   # admin: close its sessions
GET    /v1/files/stat|list|read ; PUT /v1/files/write|upload
POST   /v1/files/mkdir|remove|rename|chmod
POST   /v1/enroll                            # UNAUTHENTICATED: queue a request
GET    /v1/enroll/{request}                  # poll (X-Enroll-Secret header)
GET    /v1/enroll-requests                   # admin: list pending
POST   /v1/enroll-requests/{request}/approve # admin
POST   /v1/enroll-requests/{request}/deny    # admin
```

`GET /v1/files/read?encoding=stream` streams raw bytes; `PUT /v1/files/upload`
streams the request body into SFTP without buffering. Both support large files.

`POST /v1/targets/{target}/connect` and `/refresh` accept an optional JSON body
`{"factor": "...", "set": {"gpus-per-node": 2, ...}}` carrying the per-request
second factor and the allocation overrides. The factor is used once, never
persisted or logged. `POST /v1/targets/{target}/preview` accepts the same
override body and returns the rendered plan with no connection state change.
The MCP/agent tool surface is unchanged; agents do not call these endpoints
directly.

`GET /v1/targets` returns only targets allowed by the authenticated client's
ACL. An unauthorized target produces `403` without revealing whether it exists.
`GET /v1/sessions` lists only the caller's sessions; admins may add `?all=true`
or `?client=<name>`. Admin-only endpoints never return token values, only a
short `sha256:` fingerprint.

## Security invariants

- Agents never receive host SSH keys, `~/.ssh`, Docker/Podman sockets, or local
  command execution. The only credential in a Terok container is its bearer
  token.
- Tokens are minted by `--generate-tokens`; the gateway stores only
  `sha256:` hashes and the plaintext is never written into a tracked file.
- The gateway never accepts an SSH destination from a client.
- Sessions are owned by the client that created them; another client gets
  `404`.
- Tokens are compared in constant time and stored as `sha256:` hashes.
- A second factor passed with `--2fa` (or the JSON `factor` field) is used once
  per request and is never persisted or logged. An interactive target does not
  auto-reconnect, so a rotated factor cannot be replayed by the gateway.
- The gateway runs provisioning and recovery commands on the remote machine over
  the authenticated route connection; they are trusted operator TOML and are
  never taken from a client.
- The service runs as a normal user, never root.
- Enrollment grants nothing on its own: `/v1/enroll` is unauthenticated but
  only queues a bounded, expiring request; access exists only after an explicit
  operator approval. The delivered token is readable exactly once, gated by a
  poll secret, and a request cannot name an unknown target.

## Tests

```bash
pip install -e '.[test]'
pytest -q
```

The suite covers config validation/reload (including the default path
resolution and atomic client/token append), auth/ACL, tunnel allocation and
route failover (against a fake `ssh`), `connect_mode` validation, the HTTP API
(auth, discovery, exec, dedicated-connection open/close, session isolation,
streaming upload, reload, malformed-config safety), and the enrollment flow
(unauthenticated request, admin approval, one-shot token delivery, `.bashrc`
update, and end-to-end `computeMCP-handshake` against a live gateway).

End-to-end against the `dev-hal` development container used
`transport = "direct"` for exec/PTY/SFTP/MCP plus a route-connected target with
`ssh_targets = ["broken-alias", "hal-test"]`, verifying route failover,
`refresh`, loopback binding, stop, and automatic recovery after the route
connection was killed.