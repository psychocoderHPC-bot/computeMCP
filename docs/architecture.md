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
- **Second factors are ephemeral.** A `--2fa` prompt value is used once per
  request, never persisted or logged, and never replayed on a re-prompt;
  interactive targets fail closed on route loss rather than reusing a rotated
  factor. The token is not accepted on the command line.
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
| `src/compute_mcp/gateway.py` | Route state machine, HTTP API, interactive console, job wiring, and the provisioner environment contract |
| `src/compute_mcp/jobs.py` | Durable job registry, quota reservations, state graph, and crash reconciliation |
| `src/compute_mcp/mux.py` | Gateway-side frame codec and stream multiplexer for the single outer channel |
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

## Route state and job state

A durable multi-job target separates two state machines that the legacy
single-container path merged.

- **Route state** belongs to the target runtime. It records whether the
  gateway has an authenticated SSH connection to the configured login host,
  which alias answered, the route epoch, and the last route error. Opening the
  route never submits a Slurm job and never starts a container.
- **Job state** belongs to one durable job record. It records the computeMCP
  `job_id`, the native `slurm_job_id`, the owner client, the kind (shared or
  private), the requested settings, the scheduler state, and the endpoint once
  the container reports ready.

The gateway treats these independently. A lost route changes route state and
leaves job records intact. A scheduler state of `RUNNING` proves the allocation
exists; it does not prove that the container sshd answers. A job becomes `ready`
only after the helper publishes a verified endpoint.

At this revision the durable multi-job path applies to a target that has both
a `[targets.X.slurm]` block and a `[targets.X.bundle]` block. A target without
the bundle keeps the legacy single-allocation connect path. Non-Slurm Docker
targets keep their existing shared-container transport.

Job states are `submitting`, `pending`, `starting`, `ready`, `stopping`,
`terminal`, and `submission_unknown`. The native Slurm state is kept in
`scheduler_state`. A job that Slurm queues for hours stays `pending`; computeMCP
imposes no queue deadline.

## One outer channel and the mux

Multi-job Slurm targets run over one authenticated SSH route connection per
target and at most one persistent outer session channel. The gateway starts the
shipped `computemcp-mux.py` helper as a user process on the login node over that
channel, with binary stdin and stdout, no PTY, and separately drained stderr.

The gateway and helper speak one bounded length-prefixed protocol. Each frame
has a 12-byte header: version, type, flags, stream id, and payload length.
Stream zero carries control messages as small JSON payloads. Non-zero streams
carry raw `DATA` frames. Flow control is credit based per stream, and `EOF` is
ordered after queued data.

For each `OPEN(job_id)`, the helper reads its loopback relay registry, dials the
relay registered for that job, and pumps opaque container SSH bytes. The gateway
keeps the container SSH key and host-key verification. A slow job stream cannot
stall control messages or another job because the mux schedules streams and
bounds every buffer.

The gateway keys its inner SSH cache, locks, and cleanup by job identity and
endpoint generation. Stopping job B never detaches job A's cached connection.
The registry maps a `job_id` to a loopback relay endpoint and is rewritten when
a job becomes ready or is cancelled. Relay ports stay internal to the gateway
and helper.

The registry file is `<state_root>/mux/<target>.json`. The gateway writes it on
the gateway host and the helper reads it on the login node, so `state_root` must
be the same absolute path on both. A missing or unreadable file makes the helper
reject `OPEN` with `registry_unreadable: cannot read <path> ...`. The gateway
maps that rejection to HTTP 502. Retrying does not help; the operator must put
the state root on shared storage.

## Shared and private jobs

A target has at most one active shared job. Any authorized client may start it
with `target-shared`; concurrent calls converge on the same `job_id`, including
while the job is pending. The shared job belongs to the target, consumes no
client quota, and always uses the shared home. Only an operator may stop or
replace it. A request whose settings differ from the active shared job returns
the existing `job_id` with a settings conflict; the gateway never replaces the
job as a side effect.

A private job belongs to the authenticated client that submitted it. It
consumes one quota slot from the moment the gateway reserves the slot, before
`sbatch`, until a definitive submission failure or a scheduler-confirmed
terminal state. A client sees its own private jobs and each target's shared
job. A foreign job id and an unknown job id return the same public error.

Exec, PTY, and file operations with no `job_id` address the target's current
shared job only. They never fall back to the caller's single private job. An
explicit `job_id` addresses that job after target and ownership checks.

## Durable job state

The gateway resolves the durable state root from `--state-dir`, then
`COMPUTEMCP_STATE_DIR`, then `<config-dir>/state`. It has no temporary
fallback: without one of those three, startup fails with
`no durable job state root`. For a Slurm target the root must be on storage
visible at the same absolute path to the gateway and the login/compute nodes,
because the mux helper reads `<state_root>/mux/<target>.json` there.

Job records live under `<state_root>/jobs/<job_id>/`. Each record holds the job
identity, owner, request id and effective parameters, the scheduler id and
cluster, the resolved root mode and home, the ready node and endpoint
generation, and the terminal or uncertain reason.

Writes are atomic: a temp file, `fsync`, `os.replace`, and a directory `fsync`.
A crash leaves the previous or the next complete record. Shared-job creation
serializes get-or-create under a target-level lock. Quota reservations take a
client/target transaction and re-read durable records under a cross-process
lock, so concurrent reservations cannot both consume the last slot.

## Detach versus cancel

Gateway shutdown, route loss, a target stop, and a route refresh detach a Slurm
target. Detach closes the route, the job-keyed SSH connections, and the PTY
sessions, and it never sends `scancel`. The jobs keep running or stay queued.

On the multi-job path, only an explicit `job-stop` (or the admin `job-cancel`
console verb) calls `scancel`. Cancellation accepts pending, starting, and
ready jobs, and repeated requests are safe. Stopping one job closes only that
job's PTY, SFTP, and cached SSH resources.

The non-Slurm path keeps its older behavior: an explicit target stop runs
`close_command` and releases the remote allocation.

## Gateway restart and reconciliation

A gateway restart rebuilds the job manager from disk and reconciles before it
accepts new submissions. Reconciliation never calls `sbatch`.

- A record that already has a scheduler id is queried with `squeue`.
- A record without a scheduler id is passed to the bundle adapter's
  `find_by_token`. The helper persists the correlation token before `sbatch`,
  passes it as `--comment`, and recovery compares it against `squeue %k` and
  then `sacct` `Comment`, with a bounded timeout.
- A recovered scheduler id moves `submitting` or `submission_unknown` to
  `pending` and keeps the quota slot.
- When recovery finds nothing, the job stays `submission_unknown` and keeps
  its slot until operator reconciliation. The gateway never resubmits it
  automatically.
- A missing `squeue` entry alone does not prove that a submission failed.

After the restart, `target-open` restores the shared-job view and `job-list`
returns the surviving private jobs under their original computeMCP ids. The
job record keeps its scheduler id and endpoint generation, so a connection
built on one route is not handed out for another.

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
