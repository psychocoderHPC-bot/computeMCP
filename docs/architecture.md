# Architecture

How the `computeMCP` pieces fit together, where the security boundary is, what
lives in the source tree, and how the gateway talks to a provisioner.
Operator procedures live in [operations.md](operations.md); environment
variables and CLI flags in [configuration.md](configuration.md); the
provisioning bundle and the `provision_command` contract in
[provisioning.md](provisioning.md).

## Component diagram

```
OpenCode / agent
  |  MCP over stdio
  v
computeMCP-mcp (inside the Terok container)
  |  authenticated HTTP over the one allowed host endpoint
  v
computeMCP-gateway (on the Terok host, normal user)
  |  authenticated SSH route connections (owned by the gateway)
  +--> agent@myTargetHost development container
  +--> agent@secondTarget development container
```

- **The agent** (OpenCode or another MCP client) only knows the `compute`
  server and its `computeMCP_*` tools.
- **`computeMCP-mcp`** is a small stdio MCP server that runs *inside the Terok
  container*. It speaks only to the gateway over authenticated HTTP, carries
  the bearer token, and holds no SSH credentials, no host SSH config, and no
  file paths on the gateway host.
- **`computeMCP-gateway`** runs on the Terok host as a normal user. It owns all
  SSH: it resolves the configured routes, keeps the route connections alive,
  binds loopback forward ports, and dials the container sshd. It also runs execs
  and file transfers, and owns the persistent PTY sessions.
- **Remote development containers** are any host with Docker/Podman and an
  OpenSSH sshd; minimal and persistent, they provide the durable endpoint the
  gateway dials.

The gateway TOML is the single source of truth for targets: a client can only
name a configured target, and an arbitrary SSH hostname is never accepted.

## The security boundary

- **No host SSH credentials reach the agent.** The Terok container holds only
  its bearer token. `~/.ssh`, host keys, ProxyJump aliases, internal forward
  ports, and the Docker/Podman sockets stay on the host and behind the gateway.
- **The gateway owns the SSH.** It is the only component that knows route
  aliases, resolves them, and opens connections. The MCP bridge cannot steer an
  SSH destination; it can request operations on already configured targets.
- **Tokens are the whole credential surface.** Hash-only on the gateway
  (`sha256:`), compared in constant time, and minted only by
  `--generate-tokens` or an approved enrollment.
- **Second factors are ephemeral.** A `--2fa` value is used once per request
  and never persisted or logged; interactive targets fail closed on route loss
  rather than replaying a rotated factor.
- **Commands and recovery scripts are operator TOML, never client input.**
  `provision_command`, `close_command`, and `connect_command` run on the remote
  machine over the authenticated route connection and come only from the
  gateway configuration.

## Source-code layout

| Path | Role |
| --- | --- |
| `src/compute_mcp/allocation.py` | Slurm allocation planning and `sbatch`/`srun` argument rendering |
| `src/compute_mcp/config.py` | TOML loading, validation, includes, and transport selection |
| `src/compute_mcp/auth.py` | Constant-time bearer auth and per-client ACLs |
| `src/compute_mcp/tunnel.py` | asyncio SSH tunnel manager, route failover, and recovery |
| `src/compute_mcp/ssh_backend.py` | AsyncSSH: exec, PTY sessions, SFTP, and host-key pinning |
| `src/compute_mcp/sessions.py` | Persistent PTY session manager (bounded buffers, quotas) |
| `src/compute_mcp/files.py` | SFTP file operations |
| `src/compute_mcp/gateway.py` | State machine, HTTP API, interactive console, and the provisioner environment contract |
| `src/compute_mcp/enrollment.py` | Unauthenticated request queue and operator approval |
| `src/compute_mcp/setup.py` | Interactive `--bootstrap` / `--add-target` configuration wizard |
| `src/compute_mcp/handshake.py` | `computeMCP-handshake`: request access from inside a container |
| `src/compute_mcp/mcp_server.py` | `computeMCP-mcp`: MCP server (stdio) exposing `computeMCP_*` tools |
| `src/compute_mcp/control.py` | `computeMCP-gatewayctl` operator CLI |
| `src/compute_mcp/bundle.py` | Bundle deployment over the route connection |
| `src/compute_mcp/bundles/computemcp-slurm/` | The shipped provisioning bundle (canonical source; see its README) |
| `tests/` | Unit and integration tests (`pytest -q`) |
| `scripts/ensure-container.sh` | Reference `connect_command` script (starts a stopped container) |
| `scripts/computemcp-slurm` | Symlink to the packaged bundle under `src/compute_mcp/bundles/computemcp-slurm` (legacy directory name; the canonical source is `computemcp-container`) |
| `config.example.toml` | Annotated example configuration |
| `systemd/` | The shipped `compute-mcp-gateway.service` unit (run as a normal user) |
| `skills/` | The agent skill that teaches how to drive the `compute` MCP |

## Routing, transports, and host keys

A target is reached one of two ways, driven by `transport`:

- **Tunnel transport** (selected whenever `ssh_targets` is present): the
  gateway opens an SSH **route connection** to a login/forwarding host named by
  an `ssh_targets` entry (a TOML array of OpenSSH alias strings, tried in
  priority order; the wizard accepts a comma-separated answer), and forwards
  to the container at `remote_host:remote_port`
  (typically `127.0.0.1:2222`). The route connection is long-lived, and
  internal forward ports (always on `127.0.0.1`) are allocated from
  `[ssh] internal_port_min..internal_port_max`.
- **Direct transport** (`transport = "direct"`): the gateway connects straight
  to `remote_host:remote_port` with no tunnel. Used for co-located gateways and
  tests.

Host-key pinning is on by default and enforced per hop:

- `host_key_sha256` pins the **container's** sshd (the forwarded endpoint),
  which is what a tunnel reaches. A tunnel maps an ephemeral local port, so a
  `known_hosts` file cannot be used reliably on that hop; fingerprint pinning
  is the recommended mode.
- `route_host_key_sha256` pins the **route/login node**. Unset, the previous
  behavior applies: verification is done against the local
  `~/.ssh/known_hosts`.
- `host_key_algorithms` restricts negotiation (e.g. `["ssh-ed25519"]`). It
  must be set explicitly even with `host_key_check = "off"`; otherwise asyncssh
  can pick an algorithm the server cannot complete key exchange with.
- `host_key_check = "off"` accepts any host key. It is only safe when the
  forwarded endpoint itself is trusted (e.g. a single-user dev box); a warning
  is logged on every connect.
- `connect_mode` controls concurrency per target: `"shared"` multiplexes
  all execs/sessions over one cached SSH connection (bounded by the remote
  sshd's `MaxSessions`); `"dedicated"` opens and closes a connection per
  operation.

`user` is the login/route account for the gateway's SSH hop (it may be empty,
in which case the SSH config decides). `container_user` (default `ubuntu`) is
the account the gateway logs into **inside the development container**; a
mismatch is the usual cause of a target that connects but fails every
`exec`/file call with `502`.

## The gateway to provisioner contract (summary)

When a target has any of `[node]`, `[allocation]`, `[slurm]`, `[container]`,
or `[bundle]` configured, the gateway resolves the allocation plan and exports
it as `COMPUTEMCP_*` environment variables around the provision command. The
login node needs only Bash for the argument bridge.

The emitted variables and their semantics are detailed in
[configuration.md](configuration.md) and [provisioning.md](provisioning.md).
The gateway always exports `COMPUTEMCP_SYSTEM`; the plan variables
(`COMPUTEMCP_NODES`, `COMPUTEMCP_CPUS_PER_NODE`, `COMPUTEMCP_GPUS_PER_NODE`,
`COMPUTEMCP_MEMORY_PER_NODE_MIB`, `COMPUTEMCP_EXCLUSIVE`, `COMPUTEMCP_MODE`,
`COMPUTEMCP_SBATCH_ARGS`, `COMPUTEMCP_SRUN_ARGS`) are concrete to the resolved
plan, with numeric per-node fields empty when the plan has no value. The
container description variables mirror the `[targets.X.container]` block,
with empty values when the block is absent. `COMPUTEMCP_SSH_USER` is the
resolved container login account (emitted whenever a `container` or `bundle`
block is present). `COMPUTEMCP_SSH_PUBLIC_KEY` is derived from `client_key`
and emitted only for a `[bundle]` target, so no manual `authorized_keys`
placement is needed. `COMPUTEMCP_PROVISION_ENV` carries the bundle's
`provision-env` lines and is emitted only for a `[bundle]` target.

The helper parses `COMPUTEMCP_SBATCH_ARGS` and `COMPUTEMCP_SRUN_ARGS` with
`mapfile -t` into arrays and never uses `eval`; the two stages stay
independent. See [provisioning.md](provisioning.md) for the `sbatch`/`srun`
rendering rules, argument conflict validation, and the full bridge.

## Tests

```bash
pip install -e '.[test]'
pytest -q
```

The suite (`tests/`) covers config validation and reload (including default
path resolution and atomic client/token append), auth/ACL, tunnel allocation
and route failover (against a fake `ssh`), `connect_mode` validation, the HTTP
API (auth, discovery, exec, dedicated-connection open/close, session isolation,
streaming upload, reload, malformed-config safety), the enrollment flow
(unauthenticated request, admin approval, one-shot token delivery, bashrc
update, and end-to-end `computeMCP-handshake` against a live gateway), plus
provisioning-bridge and SSH-backend unit tests.
