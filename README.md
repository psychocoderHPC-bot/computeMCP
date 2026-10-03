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
  |  ssh -N -L tunnels (owned by the gateway)
  +--> agent@hal development container
  +--> agent@fwk394 development container
```

The gateway TOML is the single source of truth for targets. A client can only
name a configured target; an arbitrary SSH hostname is never accepted.

## Layout

```
src/compute_mcp/
  config.py       TOML loading + validation, transport selection
  auth.py         constant-time bearer auth and per-client ACLs
  tunnel.py       asyncio SSH tunnel manager, route failover, recovery
  ssh_backend.py  AsyncSSH: exec, PTY sessions, SFTP, host-key pinning
  sessions.py     persistent PTY session manager (bounded buffers, quotas)
  files.py        SFTP file operations
  gateway.py      state machine, HTTP API, interactive console
  enrollment.py   unauthenticated request queue + operator approval
  handshake.py    computeMCP-handshake: request access from inside a container
  mcp_server.py   MCP server (stdio) exposing computeMCP_* tools
  control.py      computeMCP-gatewayctl operator CLI
tests/            unit tests (118)
config.example.toml
systemd/computeMCP-gateway.service
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

    # ubuntu:24.04 already owns 1000:1000; -o lets the agent account reuse the
    # host uid/gid instead of failing `groupadd`/`useradd`.
    getent group agent >/dev/null ||
      groupadd -o --gid "$AGENT_GID" agent
    id -u agent >/dev/null 2>&1 ||
      useradd -o --uid "$AGENT_UID" --gid "$AGENT_GID" \
        --home-dir /home/agent --no-create-home --shell /bin/bash agent

    echo "agent ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/agent
    chmod 440 /etc/sudoers.d/agent
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
  The gateway reaches it through its own `ssh -N -L` tunnel; the container port
  must not be exposed publicly.
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
- **The entrypoint must be idempotent.** Docker stores the `bash -euc '...'` as
  the container `Cmd` and re-runs it on **every** start. The first version of
  this recipe installed the packages and created the user unconditionally, so on
  the second start `groupadd`/`useradd` failed, `set -e` aborted before
  `exec sshd`, and `--restart unless-stopped` crash-looped forever. The guards
  above (`command -v sshd`, `getent group`, `id -u`) make each step a no-op when
  it already ran.
- **`ubuntu:24.04` already owns uid/gid `1000:1000`** (the stock `ubuntu` user).
  Because `AGENT_UID`/`AGENT_GID` come from the host home's owner (often `1000`),
  a plain `groupadd --gid 1000` fails. `-o` (non-unique) lets the `agent` account
  reuse those ids; if you need unique ids, set `AGENT_UID`/`AGENT_GID` to a free
  range in the `docker run -e` lines and `chown -R` the existing `HOST_HOME`.
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

    # ubuntu:24.04 already owns 1000:1000; -o lets the agent account reuse the
    # host uid/gid instead of failing `groupadd`/`useradd`.
    getent group agent >/dev/null ||
      groupadd -o --gid "$AGENT_GID" agent
    id -u agent >/dev/null 2>&1 ||
      useradd -o --uid "$AGENT_UID" --gid "$AGENT_GID" \
        --home-dir /home/agent --no-create-home --shell /bin/bash agent

    echo "agent ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/agent
    chmod 440 /etc/sudoers.d/agent
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

The full annotated file is [`config.example.toml`](config.example.toml); a
minimal working `~/.config/computeMCP-gateway/config.toml` is:

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
it into the container:

```bash
computeMCP-handshake picongpu-bot-dev2 --port 2223 --system hal,fwk394
# --system is a comma-separated allow-list; omit it for an empty ACL
# (no targets) or pass --system '*' for all targets.
```

Flow:

```
container                                     gateway (host)
computeMCP-handshake <client> --port 2223          console:
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

## Install the MCP inside a Terok container

```bash
python3 -m venv /home/dev/.local/share/computeMCP/venv
/home/dev/.local/share/computeMCP/venv/bin/pip install <this-package>
ln -s /home/dev/.local/share/computeMCP/venv/bin/computeMCP-mcp /home/dev/.local/bin/computeMCP-mcp
```

Configure the task environment. The MCP process reads two variables at start:

```
COMPUTEMCP_GATEWAY=http://host.containers.internal:2222
COMPUTEMCP_TOKEN=<project-specific-token>
```

Pass them either through the environment or, more robustly, directly in the MCP
entry:

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
computeMCP-gatewayctl --config config.toml target-connect hal
computeMCP-gatewayctl --config config.toml target-refresh hal
computeMCP-gatewayctl --config config.toml target-stop hal
computeMCP-gatewayctl --config config.toml sessions          # all clients (admin)
computeMCP-gatewayctl --config config.toml client-kill alpaka # close its sessions
computeMCP-gatewayctl --config config.toml --json clients
computeMCP-gatewayctl --config config.toml --client alpaka clients  # ACL-checked
```

`clients`, `client*` and the all-clients `sessions` view require an admin
(`targets = ["*"]`) token; other clients receive `403`. Token values and full
hashes are never returned — only a short `sha256:` fingerprint.

Add a human label to any client so the listings are readable:

```toml
[clients.alpaka]
label = "alpaka CI"
targets = ["hal", "fwk394"]
```

## Configuration notes

- `ssh_targets` are SSH config aliases used **in priority order**. The gateway
  runs `ssh -N -o ExitOnForwardFailure=yes ... -L 127.0.0.1:<port>:<remote>` and
  keeps the first working route. Only TOML values are ever passed to `ssh`.
- `transport = "tunnel"` (default when `ssh_targets` is present) forwards to
  `remote_host:remote_port` through the host. `transport = "direct"` connects
  straight to `remote_host:remote_port` (useful for tests or a co-located
  gateway) and skips the SSH tunnel.
- Internal forward ports always bind `127.0.0.1` and are allocated from
  `[ssh] internal_port_min..internal_port_max`.
- Host-key verification is mandatory. A target must set either
  `host_key_sha256` or `known_hosts`; otherwise the connection is refused.
  Because a tunnel maps an ephemeral port, fingerprint pinning via
  `host_key_sha256` is the recommended mode. Restrict `host_key_algorithms`
  (e.g. `["ssh-ed25519"]`) to the pinned key's algorithm when a server offers
  several.
- `connect_mode` controls concurrency per target:
  - `"shared"` (default): all exec/sessions multiplex over one cached SSH
    connection. Fast, but bounded by the remote sshd's `MaxSessions`
    (OpenSSH default 10) for simultaneous channels.
  - `"dedicated"`: each exec/session opens its own SSH connection and closes it
    afterward, so the `MaxSessions` limit no longer bounds total parallel
    sessions. Costs one handshake per operation.
- Interactive (second-factor) authentication: set `interactive_auth = true`
  when the development container asks for a password or a keyboard-interactive
  challenge (OTP/2FA) instead of accepting the key alone. The gateway prompts
  on the operator console during `connect`, `refresh`, and the first `exec`/
  session. Passwords are read without echo; up to 3 attempts are allowed. Run
  the gateway in the foreground console for this to work. Headless (systemd)
  operations on such a target fail with a clear `503` error rather than
  hanging, because there is no one to prompt. Only the container hop is
  prompted; the host `ssh -N -L` tunnel must stay key/agent-based.

  ```toml
  [targets.hal]
  ssh_targets = ["hal", "ex_hal"]
  user = "agent"
  interactive_auth = true
  host_key_sha256 = "SHA256:..."
  ```

  Console transcript:

  ```
  gateway> connect hal
  [hal] Password:
  {'name': 'hal', 'state': 'connected', ...}
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

- HPC / Slurm targets: a fixed login node can be reached with `proxy_jump`,
  while the dynamic compute node is discovered at connect time by a trusted
  `provision_command` (see the next subsection).

### HPC / Slurm: dynamic compute nodes

On an HPC system the login node is fixed, but the development container runs in
a Slurm job on a compute node whose name (and the forwarded port) only exist
once the job starts. The gateway supports this with `provision_command`, a
trusted script that acquires the node and prints the endpoint to dial.

How the pieces connect:

```
compute-gateway (host)
  |  ssh -N -L 127.0.0.1:<local>:127.0.0.1:2200 rosi5     (your local key)
  v
rosi5 login node
  |  ssh -N -L 127.0.0.1:2200:<compute-node>:2222 <compute-node>  (login node's key)
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
provision_command = ["/home/USER/.config/computeMCP-gateway/rosi5-provision.sh"]
provision_timeout = 900.0
```

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

A realistic self-started-job script (operator-written; a later mode can let the
gateway own `sbatch` by changing only this script):

```bash
#!/usr/bin/env bash
set -euo pipefail

LOGIN="${TEROK_LOGIN:-rosi5}"                    # ssh alias of the login node
JOB_NAME="${TEROK_JOB_NAME:-terok-dev}"
CONTAINER_PORT="${TEROK_CONTAINER_PORT:-2222}"   # container's sshd port on the node
LOGIN_FORWARD_PORT="${TEROK_FORWARD_PORT:-2200}"
ALLOC="${TEROK_SBATCH_ARGS:---nodes=1 --time=08:00:00}"

# 1. Reuse a running job for this target, or submit one that stays alive.
jobid="$(squeue -h -u "$USER" -n "$JOB_NAME" -t R -o '%A' | head -n1 || true)"
if [ -z "$jobid" ]; then
    jobid="$(sbatch --parsable --job-name "$JOB_NAME" $ALLOC --wrap 'sleep infinity')"
fi

# 2. Wait until the job is running and report its node.
node=""
for _ in $(seq 1 120); do
    node="$(squeue -h -j "$jobid" -t R -o '%N' | head -n1 || true)"
    [ -n "$node" ] && break
    sleep 5
done
[ -n "$node" ] || { echo "job $jobid never started" >&2; exit 1; }

# 3. Create the login-node listener if it is not already up. The login -> compute
#    hop uses the login node's own keys/ssh-agent.
remote_cmd="$(cat <<EOF
if ! ss -ltn 2>/dev/null | grep -q '127.0.0.1:${LOGIN_FORWARD_PORT}'; then
  nohup ssh -N -o ExitOnForwardFailure=yes -o BatchMode=yes \
    -L 127.0.0.1:${LOGIN_FORWARD_PORT}:${node}:${CONTAINER_PORT} ${node} \
    >/dev/null 2>&1 &
fi
EOF
)"
ssh "$LOGIN" "$remote_cmd"

# 4. Print the endpoint; the gateway reaches it through ssh_targets=["rosi5"].
printf 'ENDPOINT 127.0.0.1:%s\n' "$LOGIN_FORWARD_PORT"
```

The compute node and the port may change per job; the only contract is the
printed endpoint. `refresh rosi5` re-runs the script and follows the new node.

Alternative: if your site lets the gateway reach the compute node directly
(both hops accept the gateway's key), skip the login-node listener entirely —
print `ENDPOINT <compute-node>:<port>` and add `proxy_jump = "rosi5"`, so the
gateway runs `ssh -J rosi5 <ssh_target>` and forwards straight to the node.

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
POST   /v1/targets/{target}/connect | /refresh | /stop
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
`transport = "direct"` for exec/PTY/SFTP/MCP plus a real `ssh -N -L` tunnel
target with `ssh_targets = ["broken-alias", "hal-test"]`, verifying route
failover, `refresh`, loopback binding, stop, and automatic recovery after the
tunnel process was killed.