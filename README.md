# computeMCP gateway

computeMCP is built first for agents running inside a
[Terok](https://github.com/terok-ai/terok) sandbox, but it also works as a
generic compute-system abstraction without one. It lets an AI agent reach
remote development and compute environments without ever receiving the host's
SSH credentials. The system has two halves. A host gateway,
`computeMCP-gateway`, runs on the machine that owns the SSH accounts; it holds
the private keys, keeps the SSH route connections alive, and dials into each
remote development container. An in-container MCP bridge, `computeMCP-mcp`, runs
beside the agent and speaks only to that gateway over authenticated HTTP,
carrying a small per-project bearer token. The agent never sees the host's
`~/.ssh`, its key pairs, or its SSH configuration; the token is the only
credential inside the container. A client can only name a target the operator
has configured, so no arbitrary SSH hostname is ever accepted from the agent.

When a target describes a container runtime, the gateway provisions that
environment for you: it deploys the shipped provisioning bundle over the route
connection and builds and starts the remote development container when you wire
the target in, so you do not create it by hand. The gateway TOML is the single
source of truth for which targets exist and how each is reached.

Quick links: [Quick start](#quick-start) · [Documentation](#documentation).

## Quick start

One recommended path. The gateway **builds and starts the remote development
container for you** (the wizard's *Should the gateway build and start this
container?* answer `Yes`); you do not create it by hand. The automatically
provisioned container is **`ubuntu`-based only** - a hand-built container
(expert topic, linked below) can be any distribution.

Three machines, named by the step captions:

- **Gateway host** - owns the SSH accounts; runs `computeMCP-gateway`.
- **Terok agent container** - runs the agent and the `computeMCP-mcp` bridge.
- **Remote host** - where the gateway provisions the development container.

Prerequisites: on the gateway host, `pipx`, Python 3.11+, and an SSH alias
that already reaches the remote host (here `myTargetHost`, from
`~/.ssh/config`); in the agent container, `pipx` and `git`.

### 1. Gateway host - install

```bash
git clone <computeMCP-repo-url> && cd computeMCP   # run from the repo root
pipx install .                                     # gateway, gatewayctl, handshake, mcp
pipx ensurepath                                    # once, if ~/.local/bin is not on PATH
ssh-keygen -t ed25519 -f ~/.ssh/computemcp_container -C computeMCP-gateway
```

The key is the gateway's identity for the container. Do not reuse your personal
key.

### 2. Gateway host - configure (bootstrap)

```bash
computeMCP-gateway --bootstrap
```

Answer the wizard to keep the gateway-managed container (this is the recommended
path):

| Question | Answer |
| --- | --- |
| Listen address / Port | `127.0.0.1` / `2222` |
| Allow interactive enrollment | **Yes** (needed for the step 6 handshake) |
| Set up a target | **Yes** |
| Target name | `myTargetHost` |
| Transport / SSH alias | `tunnel` / `myTargetHost` |
| Private key | `~/.ssh/computemcp_container` |
| Container host-key fingerprint | leave blank (sets `host_key_check = "off"`; pin it later) |
| Should the gateway build and start this container? | **Yes** - the gateway deploys and runs the bundle |
| Bundle deploy directory / container runtime / storage / image | accept defaults (`docker`, `$HOME/computemcp`, `ubuntu:24.04`) |

The wizard writes `config.toml`, `tokens.toml`, `operator.token`, and
`systems/myTargetHost.toml`, then exits.

### 3. Gateway host - start and check

```bash
computeMCP-gateway            # foreground; keep it running. Or use the shipped unit:
                              #   cp systemd/compute-mcp-gateway.service ~/.config/systemd/user/
                              #   systemctl --user daemon-reload && systemctl --user start compute-mcp-gateway
```

In a second terminal on the gateway host:

```bash
curl -fsS http://127.0.0.1:2222/v1/health   # -> {"status": "ok", ...}
computeMCP-gatewayctl status                # 'myTargetHost' -> connected (first build takes a while)
```

### 4. Terok agent container - install the bridge

The agent container is not the remote development container; it hosts the agent
and the MCP bridge.

```bash
git clone <computeMCP-repo-url> && cd computeMCP   # run from the repo root
sudo chown -R dev:dev ~/.local                     # Terok only: ~/.local starts root-owned
pipx install .                                     # exposes computeMCP-mcp and computeMCP-handshake
pipx ensurepath                                    # once, if ~/.local/bin is not on PATH
```

### 5. Terok agent container - reach the gateway (Terok Shield)

Terok Shield is default-deny. The gateway is reached at
`host.containers.internal:2222`, but a Shield `allow` entry for that hostname is
not enough: Terok's reserved `localhost:PORT` host-service grant is what opens
the gateway's port on the host (the client still dials
`host.containers.internal`). Check the connection, add the grant to the
project's `project.yml`, and recreate the task:

```bash
getent hosts host.containers.internal        # should resolve, e.g. 10.0.2.2
curl -fsS http://host.containers.internal:2222/v1/health
```

```yaml
# ~/.config/terok/projects/<project>/project.yml
shield:
  allow:
    - "localhost:2222"   # host-service grant opens the gateway port on the host
```

After changing `project.yml`, rebuild the project's container; the change only
takes effect in new Terok tasks. An already-running task keeps the old Shield
settings.

### 6. Terok agent container - request access

```bash
computeMCP-handshake myTaskName --port 2222 --system myTargetHost
# note the request id; this command waits for approval
```

`--system myTargetHost` is the target allow-list (`*` for all, omit for none).

### 7. Gateway host - approve

```bash
computeMCP-gatewayctl enrollments            # note the pending request id
computeMCP-gatewayctl approve <request-id>
```

The gateway appends the client, reloads, and hands the token back once.

### 8. Terok agent container - configure the MCP and smoke-test

A running agent does not see new shell variables; put the two values the
handshake printed into the MCP entry:

```json
{
  "mcp": {
    "compute": {
      "type": "local",
      "command": ["computeMCP-mcp"],
      "enabled": true,
      "environment": {
        "COMPUTEMCP_GATEWAY": "http://host.containers.internal:2222",
        "COMPUTEMCP_TOKEN": "<token-printed-by-the-handshake>"
      }
    }
  }
}
```

Then, in the agent: `computeMCP_targets()` lists `myTargetHost`, and
`computeMCP_exec(target="myTargetHost", command="hostname")` returns the remote
hostname. That is the full chain - agent, MCP bridge, gateway, routed SSH,
gateway-provisioned container - with no host SSH credentials in the agent.

## Create the remote development container

You normally do not create it. When the wizard answer *Should the gateway build
and start this container?* is **Yes** and the target keeps its `[targets.X.bundle]`
block (both are the defaults in the quick start), the gateway **deploys the
shipped provisioning bundle and builds and starts the container for you** on the
remote host. The auto-provisioned container is `ubuntu`-based.

Deselect that answer only if you want to manage the container yourself. That is
expert knowledge: the recipes, including **non-`ubuntu`** hand-built containers
and AMD/ROCm variants, live in [docs/provisioning.md](docs/provisioning.md) and
the [`examples/`](examples/README.md) directory
([NVIDIA](examples/dev-container-nvidia.sh), [AMD](examples/dev-container-amd.sh)).
A hand-built container works with any distribution; you then point the target at
it with a manual `provision_command` (workflow B).

## Slurm allocation and container configuration

Targets on a Slurm cluster carry per-node capacity, an allocation policy,
and manual `sbatch`/`srun` options, and an optional container runtime.
The shipped provisioning bundle reads the gateway-computed plan from its
`COMPUTEMCP_*` environment variables and submits a single-node Slurm
allocation when present.

Full semantics for every key are in [docs/configuration.md](docs/configuration.md);
worked targets are in
[examples/slurm-gpu-apptainer.toml](examples/slurm-gpu-apptainer.toml)
(Slurm + GPU + Apptainer, the canonical HPC shape),
[examples/non-slurm-docker-host.toml](examples/non-slurm-docker-host.toml)
(the same target tree with no Slurm blocks), and
[examples/manual-connect-recovery.toml](examples/manual-connect-recovery.toml)
(a workflow B target with manual `provision_command`, `connect_command`,
and `close_command`).

### Connect-time overrides and previews

`computeMCP-gatewayctl target-connect <t>` and `target-refresh <t>` accept
repeatable `--set KEY=VALUE` entries, and the `--dry-run` flag renders the
allocation without connecting. Valid `--set` keys: `nodes`, `gpus-per-node`,
`cpus-per-node`, `mem-per-node`, `mode`. The HTTP equivalent is
`POST /v1/targets/{target}/preview` with the same override body.

```bash
computeMCP-gatewayctl target-connect myTargetHost --set gpus-per-node=2
computeMCP-gatewayctl target-connect myTargetHost --set nodes=2 --set cpus-per-node=12 --set mem-per-node=100G
computeMCP-gatewayctl target-connect myTargetHost --set gpus-per-node=2 --dry-run
computeMCP-gatewayctl target-refresh myTargetHost --set gpus-per-node=2
```

For the `sbatch`/`srun` argument transport, the `account` field,
the mapping vocabulary, the conflict rules, the
`COMPUTEMCP_SBATCH_ARGS` / `COMPUTEMCP_SRUN_ARGS` stage semantics, and
the preview contract, see
[docs/configuration.md](docs/configuration.md)
and [docs/provisioning.md](docs/provisioning.md).

See [docs/operations.md](docs/operations.md) for the `target-connect` /
`target-refresh` / `target-stop` lifecycle, `--2fa` second factors, timeout
precedence, the enrollment lifecycle, and the MCP bridge setup inside an
agent container.

## Host-key verification

The wizard above starts the container with `host_key_check = "off"`, the
recommended minimum so the first container can come up before its fingerprint is
known; the gateway logs a warning on every connect. This is only safe on a
trusted, single-user dev box. Once the container exists, pin it. The gateway
pins the **container** sshd host key; `route_host_key_sha256` pins the
**login node's** key. Read the container key with one of these:

```bash
# read from inside the container:
docker exec computeMCP-container ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
# or sweep the container's published port on the route host:
ssh myTargetHost 'ssh-keyscan -t ed25519 -p 2222 127.0.0.1 2>/dev/null' | ssh-keygen -lf -
```

Set the result as `host_key_sha256` in the `myTargetHost` target, add
`host_key_algorithms = ["ssh-ed25519"]`, and restore `host_key_check` to its
default `"on"`; then `computeMCP-gatewayctl reload`. A recreation of the
container changes its host key, so re-read it and `target-refresh myTargetHost`
after recreating. Full reference in [docs/provisioning.md](docs/provisioning.md).

## Common pitfalls

- **`connected` but every `computeMCP_exec`/file call is `502`**: the container
  sshd is refusing the dialed account. The dialed account is `container_user`
  (default `ubuntu`), never the route `user`. Keep `container_user` in sync with
  the account the container was built for, then `target-refresh <t>`. See
  [docs/troubleshooting.md](docs/troubleshooting.md).
- **`Host key is not trusted`**: the container was recreated (new host key) or
  the pin is a placeholder. Read the new fingerprint and re-pin, then refresh;
  see [Host-key verification](#host-key-verification).
- **Handshake or MCP cannot reach the gateway**: Terok Shield is default-deny.
   Add `localhost:2222` to `project.yml` `shield.allow` (the reserved
  host-service grant) and create a new task;
  see [docs/operations.md](docs/operations.md).
- **`pipx install .` permission error in the agent container**: `~/.local` is
  root-owned in Terok containers; run `sudo chown -R dev:dev ~/.local` once.
  See [Quick start, step 4](#4-terok-agent-container---install-the-bridge).

## Documentation

- [docs/configuration.md](docs/configuration.md) - full configuration key
  reference: every key, type, default, and constraint.
- [docs/provisioning.md](docs/provisioning.md) - the two provisioning
  workflows (A: shipped bundle, B: manual `provision_command`), the
  `COMPUTEMCP_*` environment transport contract, and the
  `provision_command` / `close_command` / `connect_command` semantics.
- [docs/operations.md](docs/operations.md) - running the gateway, the
  `computeMCP-gatewayctl` operator CLI, the enrollment lifecycle, the shipped
  systemd unit, the HTTP API, the MCP bridge setup, and the Terok Shield.
- [docs/troubleshooting.md](docs/troubleshooting.md) - how to read
  `last_error` and a symptom-to-fix table.
- [docs/architecture.md](docs/architecture.md) - component diagram, the
  security boundary, transport and host-key model, and source layout.
- [examples/README.md](examples/README.md) - worked provisioning examples
  (non-Slurm Docker host, Slurm + Apptainer, manual recovery, NVIDIA/AMD
  container scripts), indexed.
- [config.example.toml](config.example.toml) - the annotated reference
  configuration.
- [skills/computeMCP/SKILL.md](skills/computeMCP/SKILL.md) - the agent skill
  that teaches how to drive the `compute` MCP server.

## Reference configuration

A minimal working `~/.config/computeMCP-gateway/config.toml` for a non-Slurm
Docker-host bundle target. Replace the `/home/USER` placeholder path. It starts
with host-key verification off, which the wizard also does when no fingerprint
is given; pin the container key later (see [Host-key verification](#host-key-verification)):

```toml
[server]
listen = "127.0.0.1"
port = 2222
exec_timeout = 900.0
allow_enrollment = true          # required for computeMCP-handshake / enrollment

[sessions]
idle_timeout = 3600.0
max_per_client = 16

[clients.admin]
token_hash = "sha256:REPLACE_WITH_OPERATOR_TOKEN_HASH"
targets = ["*"]

[targets.myTargetHost]
ssh_targets = ["myTargetHost"]   # SSH alias(es) of the route login node, tried in order
user = "agent"                   # route login account (may be empty -> SSH config decides)
client_key = "/home/USER/.ssh/computemcp_container"
host_key_algorithms = ["ssh-ed25519"]
host_key_check = "off"           # verification off; pin the container key later (see Host-key verification)
auto_connect = true

[targets.myTargetHost.bundle]
source = "computemcp-container"  # or legacy alias "computemcp-slurm"
deploy-dir = "$HOME/computemcp/bundle"
provision-env = ["source /etc/profile.d/docker.sh"]

[targets.myTargetHost.container]
runtime = "docker"
storage-root = "/home/USER/computemcp"
image = "ubuntu:24.04"
gpus = ["nvidia"]
```

Every key here is documented, with defaults and constraints, in
[docs/configuration.md](docs/configuration.md). For Slurm clusters, Apptainer,
CPU-only, and manual (`provision_command`) targets, see
[docs/provisioning.md](docs/provisioning.md) and [examples/](examples/README.md).

### Include and per-target files

A top-level `include` key splits the configuration across files. Relative
entries resolve against the including file's directory; absolute entries are
used as-is. Each included file keeps its full `[targets.NAME]` structure.
`--bootstrap` and `--add-target` give every target its own `systems/<name>.toml`
file and list it under `include`:

```
~/.config/computeMCP-gateway/
  config.toml            # server, auth, clients, include list
  tokens.toml            # hashed client tokens
  operator.token         # operator bearer (written by --bootstrap, mode 0600)
  systems/
    myTargetHost.toml    # [targets.myTargetHost] and its nested tables
```

Add a target without restarting the gateway: `computeMCP-gatewayctl --add-target`
writes the file and appends the include (no running gateway needed), then
`computeMCP-gatewayctl reload` (or `systemctl --user reload compute-mcp-gateway`)
applies it. The gateway re-parses, validates, and applies the full graph
atomically; a malformed file keeps the previous configuration live.
