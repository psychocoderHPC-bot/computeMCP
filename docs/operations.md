# Operations

How to run the `computeMCP-gateway`, manage its targets and clients on a daily
basis, enroll Terok tasks, and reach it from the agent side. Environment
variable and CLI-flag reference: [configuration.md](configuration.md). Dynamic
nodes, the provisioning bundle, and the `provision_command` contract:
[provisioning.md](provisioning.md).

## Running the gateway

The gateway reads its TOML configuration and token store, then serves its
authenticated HTTP API. It runs either in the foreground with an operator
console or headless as a service.

```bash
# interactive (operator console); uses the default config location
computeMCP-gateway

# headless (systemd)
computeMCP-gateway --no-console

# or point at an explicit file
computeMCP-gateway --config /path/to/config.toml
```

The console is enabled automatically on a TTY and disabled with `--no-console`
(or when standard input is not a TTY). Console commands: `targets`,
`status [target]`, `connect`, `refresh`, `reconnect`, `stop`, `connect-all`,
`stop-all`, `reload`, `clients`, `client <name>`,
`client-refresh|connect|stop <name> [target]`, `client-kill <name>`,
`sessions [target]`, `close-session <id>`, `enrollments`,
`approve <request-id>`, `deny <request-id>`, `quit`. `connect` and `refresh`
accept `--2fa SECRET` for an interactive target, for example
`connect --2fa SECRET myTargetHost`.

Every configuration change, new target, or token rotation is applied with
`reload`: the full include graph is re-parsed and validated, the token file is
re-read, and unchanged connected targets keep their live tunnels. A malformed
file keeps the previous configuration active.

### The shipped systemd unit

[systemd/compute-mcp-gateway.service](../systemd/compute-mcp-gateway.service)
runs the gateway headless as a normal user (never root):

```ini
[Service]
Type=simple
ExecStart=%h/.local/bin/computeMCP-gateway --no-console
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
PrivateTmp=true
```

- `ExecStart` invokes the pipx shim directly, so the service needs no shell
  and no PATH. `--config` is omitted on purpose: the gateway defaults to
  `~/.config/computeMCP-gateway/config.toml` (and its sibling `tokens.toml`).
- `ExecReload` sends `SIGHUP`; the daemon re-parses the configuration and token
  file without dropping live tunnels. A malformed file is logged and the
  previous configuration is kept.
- `NoNewPrivileges` and `PrivateTmp` keep the service from running as root and
  from seeing other users' `/tmp`.

### Reloading

Two equivalent triggers re-read the full graph (config, tokens, `systems/`):

```bash
# re-read config.toml + tokens.toml on the running gateway
computeMCP-gatewayctl reload

# under systemd, equivalent and cleaner:
systemctl --user reload computeMCP-gateway     # sends SIGHUP
```

## Operator workflow

The typical day-to-day sequence on the host:

```bash
# 1. Add a system (wizard; no running gateway needed).
computeMCP-gatewayctl --add-target

# 2. Make a running gateway pick up new/changed files (config, tokens, systems/).
computeMCP-gatewayctl reload

# 3. Connect a target and watch its state.
computeMCP-gatewayctl target-connect myTargetHost
computeMCP-gatewayctl status

# 4. Preview an allocation change before committing it to a live target.
computeMCP-gatewayctl target-connect myTargetHost --set gpus-per-node=2 --dry-run

# 5. Re-apply corrected settings to an already-connected target (releases the
#    old allocation via close_command, then provisions a new one).
computeMCP-gatewayctl target-refresh myTargetHost --set gpus-per-node=2

# 6. Stop a target, releasing its allocation (runs close_command on the remote).
computeMCP-gatewayctl target-stop myTargetHost

# 7. Manage enrollment from inside a Terok task: the task runs the handshake,
#    you approve or deny the request on the host.
computeMCP-gatewayctl enrollments
computeMCP-gatewayctl approve <request-id>
computeMCP-gatewayctl deny <request-id>
```

`--add-target` writes one `systems/<name>.toml` file, appends it to the main
`include` list, and re-validates the whole include graph (rolling back both
files on failure). It needs no gateway and no token. `reload` is what a
running gateway then needs to see the new file.

The interactive console of a running `computeMCP-gateway` offers the same verbs
without a separate CLI call.

## The operator CLI (`computeMCP-gatewayctl`)

The CLI talks to the running gateway's authenticated API; nothing is stopped or
restarted. Under systemd (where the gateway runs `--no-console`) it is the
operator's interface.

### Token resolution order

1. `--token` flag (explicit bearer token)
2. the `operator.token` file next to `--config` (written mode 0600 by
   `--bootstrap`); this outranks the environment so a stray ambient
   `COMPUTEMCP_TOKEN` cannot shadow the gateway the command was pointed at
3. the `COMPUTEMCP_TOKEN` environment variable
4. a **plaintext** token under the configured `--client` (default `admin`) in
   the `[auth] token_file` tokens file; hashed entries cannot be used

If none of these is available the CLI exits with an actionable error naming
the four ways to fix it (`--bootstrap`, `COMPUTEMCP_TOKEN`, `--token`, or a
plaintext token in the tokens file).

### Subcommands

| Command | Purpose |
| --- | --- |
| `status [target]` | Target states (default: all); one target with detail |
| `targets` | List targets allowed by the caller's ACL |
| `clients` | All clients: label, target ACL, session count (admin) |
| `client <name>` | One client: labels, short token fingerprint, ACL, sessions (admin) |
| `sessions [target]` | The caller's sessions; `--client <name>`/`all` for admins |
| `client-connect <name> [target]` | Connect all (or one) of a client's ACL targets |
| `client-refresh <name> [target]` | Re-run route failover for a client's targets |
| `client-stop <name> [target]` | Stop a client's targets |
| `client-kill <name>` | Close all of a client's sessions |
| `target-connect <target>` | Connect a target (allocation defaults, or `--set` overrides) |
| `target-refresh <target>` | Re-apply retained/`--set` settings, recreating the allocation |
| `target-stop <target>` | Stop a target and release its allocation (`close_command`) |
| `reload` | Re-read the full config + token graph (admin) |
| `enrollments` | List pending enrollment requests (admin) |
| `approve <request-id>` | Approve an enrollment (admin) |
| `deny <request-id>` | Deny an enrollment (admin) |

`client*` and the all-clients `sessions` view require an admin
(`targets = ["*"]`) token; other clients receive `403`. Token values and full
hashes are never returned, only a short `sha256:` fingerprint.

### Second factor (`--2fa`)

For an `interactive_auth` target, pass the per-request second factor:

```bash
computeMCP-gatewayctl target-connect --2fa SECRET myTargetHost
computeMCP-gatewayctl target-refresh --2fa SECRET myTargetHost
```

The factor is used once per request and is never persisted or logged. An
interactive target does not auto-connect or auto-reconnect; a request that
omits the factor fails closed (state unchanged) with a warning. When
`interactive_auth` is false and `--2fa` is given, the gateway warns, ignores
the factor, and proceeds with a key-based connect.

### Allocation overrides (`--set`) and previews (`--dry-run`)

`target-connect` and `target-refresh` accept repeatable `--set KEY=VALUE`
entries. The `--dry-run` flag renders the allocation without connecting. Valid
`--set` keys (allocation mode values also apply):

- `nodes`
- `gpus-per-node`
- `cpus-per-node`
- `mem-per-node`
- `mode` (one of the allocation modes; a multi-node `mode` limited to `full`
  and `exclusive`)

```bash
computeMCP-gatewayctl target-connect myTargetHost --set gpus-per-node=2
computeMCP-gatewayctl target-connect myTargetHost --set nodes=2 --set cpus-per-node=12 --set mem-per-node=100G
computeMCP-gatewayctl target-connect myTargetHost --set gpus-per-node=2 --dry-run   # preview only
computeMCP-gatewayctl target-refresh myTargetHost --set gpus-per-node=2
```

The preview prints the planned intent (mode, nodes, per-node calculations,
exclusive), the manual settings per stage, the rendered arguments per stage,
the calculated fields not emitted, and the final `COMPUTEMCP_SBATCH_ARGS` and
`COMPUTEMCP_SRUN_ARGS`. It changes no live allocation. For a connected target
whose current allocation differs, the result carries `needs_refresh: true` with
a warning. The HTTP equivalent is `POST /v1/targets/{target}/preview` with the
same override body.

Lifecycle semantics:

- **Disconnected connect**: applies allocation defaults plus `--set` overrides.
- **Connected target, no overrides**: preserves the active allocation.
- **Connected connect with differing settings**: reports a mismatch
  (`needs_refresh: true`); nothing is torn down, the existing allocation
  continues.
- **Refresh** (`target-refresh <t> --set …`): applies overrides to the retained
  resolved settings and recreates the allocation (releases the old one via
  `close_command`, then provisions a new one).
- **Recovery without overrides**: `target-refresh <t>` re-applies the target's
  retained resolved settings, not the configured defaults, so a previous `--set`
  request survives route loss or a restart.
- **Validation**: overrides resolve before mapping; an explicit partial
  GPU/CPU request that conflicts with a `full`/`exclusive` multi-node policy is
  an error, not a silent replacement.

### Timeout precedence (`--timeout`)

For `target-connect` and `target-refresh` (and `client-connect`/
`client-refresh`) the HTTP client waits for the target's `provision_timeout`
plus a short handshake margin, because a slow `provision_command` can
legitimately run that long. Precedence:

1. subcommand `--timeout SECONDS`
2. the global `--timeout SECONDS` (before or after the subcommand)
3. the longest known `provision_timeout` among the named targets + 60 s margin
4. 60 s default

For every other command the timeout stays at 60 seconds unless `--timeout` is
given.

## Enrollment lifecycle

A Terok task that has not been pre-configured asks the gateway for access. The
request is unauthenticated and grants nothing: it only places a pending entry
in an in-memory queue. An operator must approve it on the host; only then is a
token minted and handed back once.

Flow:

```
inside the Terok container                     gateway host (operator)
---------------------------------------------  ----------------------------------
computeMCP-handshake <client> --port 2222      computeMCP-gatewayctl enrollments
  |  POST /v1/enroll {client_id, targets}       computeMCP-gatewayctl approve <id>
  |  (request queued, pending operator)          -> mint token, append
  |  GET /v1/enroll/<id>  (poll, X-Enroll-Secret) [clients.<id>] + tokens.toml,
  |<------ {status: approved, token} --------     reload
  |  writes COMPUTEMCP_* to ~/.bashrc and         (token held in memory)
  |  prints the MCP environment snippet            handed to the waiter exactly once
```

- **Handshake in the container**: `computeMCP-handshake <client-id>
  --port 2222 --system myTargetHost`. The client id names the new
  `[clients.<id>]` entry once approved.
- **Approval on the host**: an operator action only. `computeMCP-gatewayctl
  enrollments` lists pending requests; `approve <request-id>` or `deny
  <request-id>` decides. The console offers the same `approve`/`deny` verbs.
- **One-shot token delivery**: on approval the gateway mints the token,
  appends `[clients.<id>]` to `config.toml`, writes the hash to `tokens.toml`,
  reloads, and returns the plaintext token to the waiting handshake exactly
  once. A high-entropy poll secret (sent in the `X-Enroll-Secret` header) proves
  ownership, so another waiter cannot read or replay it. The plaintext is
  dropped from memory after the first successful poll and is never written to a
  tracked file.

Request constraints: a request cannot name an unknown target; a client id
already queued or already present in the config is rejected as a duplicate. The
endpoint is disabled by default; enable it explicitly with
`[server] allow_enrollment = true`. `enroll_ttl` (default 600 s) and
`enroll_max_pending` (default 32) bound the unauthenticated surface; entries
expire and the pending queue is capped.

Handshake flags: `--gateway` (base URL override; wins over `COMPUTEMCP_GATEWAY`
and the `--host`/`--port` defaults `host.containers.internal:2222`; `--port`
must match `[server] port`), `--system` (comma-separated target allow-list;
omit for none, `--system '*'` for all), `--label`, `--bashrc` (shell file to
update, default `~/.bashrc`; the block it writes is idempotent and
marker-delimited so re-runs replace it), `--env-file <file>` (write the token
to that 0600 file and add only a `source` line to the shell file, keeping the
secret out of `~/.bashrc`), `--no-write` (print the MCP environment snippet
only, touch no file), `--timeout` (how long to wait for approval, default
900 s), `--poll-interval` (default 3 s), `--json`.

## Tokens

Tokens are generated by the gateway, never hand-written. A per-project token is
issued once; only its `sha256` hash is stored on the gateway, and the plaintext
is printed once so it can be injected into that project's Terok environment.

```bash
# writes hashes to the default tokens.toml (chmod 600) and prints the tokens
computeMCP-gateway --generate-tokens

# or point somewhere else explicitly
computeMCP-gateway --config /path/config.toml --generate-tokens /path/tokens.toml
```

Point the gateway at that file with `[auth] token_file = "..."` (or
`--token-file`); failing that, a `tokens.toml` next to `config.toml` is used
automatically.

Hash-only storage and rotation: the running gateway reads the token file on
start and on every `reload`. To rotate, generate or edit the tokens file and
`reload`; adding or rotating a project token is one `reload` away and does not
drop live tunnels. Client listings expose only a short `sha256:` fingerprint,
never the value or full hash.

## MCP bridge setup in the agent container

The agent side is a single MCP bridge (`computeMCP-mcp`, stdio) that speaks only
to the gateway over authenticated HTTP. It holds no SSH credentials and never
learns the host's SSH configuration. Set up once per Terok task, after the
gateway is running:

1. **Install the MCP bridge in the task** (not on the host):

   ```bash
   # Terok container: `~/.local` is owned by root, so pipx cannot create its
   # venvs and `pipx install .` fails with a permission error. Fix it once:
   sudo chown -R dev:dev ~/.local     # one-time fix for this container
   pipx install .                     # install from the current folder
   pipx ensurepath                    # once; makes ~/.local/bin available in new shells
   ```

   On a normal host the `chown` line is unnecessary. The permission error is
   `pipx install .` failing because `~/.local` is owned by root in fresh Terok
   containers; the `chown` fix is what clears it.

2. **Allow the gateway through the Terok Shield** (see below).

3. **Request access from inside the task, then approve it on the host**
   (enrollment flow above).

4. **Point the MCP at the gateway.** The handshake writes
   `COMPUTEMCP_GATEWAY` and `COMPUTEMCP_TOKEN` to the task (default: a
   marker-delimited block in `~/.bashrc`; `--env-file` keeps the secret in a
   separate 0600 file). A running agent does not see new shell variables, so
   also paste the printed `environment` snippet into the MCP entry, or restart
   the agent from a fresh shell:

   ```json
   {
     "mcp": {
       "compute": {
         "type": "local",
         "command": ["computeMCP-mcp"],
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
   (`host.containers.internal`, not the host's own address); `COMPUTEMCP_TOKEN`
   is the per-project token. The MCP has no CLI arguments and reads only those
   two variables; if either is missing it exits with
   `COMPUTEMCP_GATEWAY is not set` / `COMPUTEMCP_TOKEN is not set`.

Prefer the explicit `environment` block: an `export` in `~/.bashrc` (or a
login-shell config) does not reliably reach an already-running agent/TUI,
because a long-lived `tmux` or session manager keeps its original environment
and does not re-source a later start file. Setting the variables in the MCP
entry makes the token independent of shell inheritance. The token then lives in
that config file (e.g. `opencode.json`); keep it out of any repository and
treat it as a secret, or inject it from a file the MCP reads, and never commit
it.

## Terok Shield allow/override

Terok Shield is default-deny: a task container cannot open a TCP connection to
the gateway until the project explicitly allows the destination. If the MCP
fails with a connection error even though the gateway is healthy, this is the
cause. Add the gateway to the project's `project.toml`:

```yaml
shield:
  allow:
    - localhost:2222        # reserved host-service grant: opens the gateway port on the host
  override:
    - host: 10.0.2.2        # optional; the slirp4netns gateway address, if reachable
      reason: computeMCP gateway on the host
      expires: 2027-10-06
```

- Use `localhost:PORT`, Terok Shield's reserved host-service grant. It opens the
  gateway's port on the **host** even though `localhost` inside the container is
  the container's own loopback. A plain `allow` for `host.containers.internal`
  is not enough: the hostname resolves into the project-allow IP set, but the
  port grant lives in a separate preamble rule that only `localhost:PORT`
  populates, and a resolved private (RFC 1918) or link-local address is rejected
  before the allow tiers.
- `override.host` sits above the security-deny and is the break-glass path for a
  private address; set it to the **gateway host as the container sees it** — the
  address `host.containers.internal` resolves to inside the task (commonly
  `10.0.2.2`; verify with `getent hosts host.containers.internal`). It is not a
  host interface address, so do not try to bind it on the host.
- A new task must be created after changing the Shield config; an existing task
  does not pick up later project changes.
- Adjust the port (`2222` above) to match `[server] port` in the gateway
  config.

## Security invariants

- Agents never receive host SSH keys, `~/.ssh`, Docker/Podman sockets, or local
  command execution. The only credential a Terok container holds is its bearer
  token.
- Tokens are minted by `--generate-tokens`; the gateway stores only `sha256:`
  hashes and the plaintext is never written to a tracked file.
- The gateway never accepts an SSH destination from a client; a client can only
  name a configured target.
- Sessions are owned by the client that created them; another client's
  session id maps to `404`.
- Tokens are compared in constant time and stored as `sha256:` hashes.
- A second factor passed with `--2fa` (or the JSON `factor` field) is used once
  per request and is never persisted or logged.
- The gateway runs provisioning and recovery commands on the remote machine
  over the authenticated route connection; they are trusted operator TOML and
  are never taken from a client.
- The service runs as a normal user, never root.
- Enrollment grants nothing on its own: `/v1/enroll` is unauthenticated but
  only queues a bounded, expiring request; access exists only after an explicit
  operator approval, and the delivered token is readable exactly once, gated by
  a poll secret.

## HTTP API reference

All requests require `Authorization: Bearer <token>` unless marked
unauthenticated.

```
GET    /v1/health
GET    /v1/targets
GET    /v1/targets/{target}
POST   /v1/targets/{target}/connect | /refresh | /stop | /preview
POST   /v1/exec
POST   /v1/sessions ; GET /v1/sessions
GET|DELETE /v1/sessions/{session}
POST   /v1/sessions/{session}/write | /read | /resize
POST   /v1/reload                          # admin: re-read config + tokens
GET    /v1/clients ; GET /v1/clients/{name} # admin: token/ACL/sessions
GET    /v1/clients/{name}/sessions          # admin
DELETE /v1/clients/{name}/sessions          # admin: close its sessions
GET    /v1/files/stat|list|read ; PUT /v1/files/write|upload
POST   /v1/files/mkdir|remove|rename|chmod
POST   /v1/enroll                          # unauthenticated: queue a request
GET    /v1/enroll/{request}                 # poll (X-Enroll-Secret header)
GET    /v1/enroll-requests                   # admin: list pending
POST   /v1/enroll-requests/{request}/approve # admin
POST   /v1/enroll-requests/{request}/deny    # admin
```

`GET /v1/files/read?encoding=stream` streams raw bytes; `PUT
/v1/files/upload` streams the request body into SFTP without buffering; both
support large files.

`POST /v1/targets/{target}/connect` and `/refresh` accept an optional JSON body
`{"factor": "...", "set": {"gpus-per-node": 2, ...}}` carrying the per-request
second factor and allocation overrides. The factor is used once, never
persisted or logged. `POST /v1/targets/{target}/preview` accepts the same
override body and returns the rendered plan with no connection state change.

`GET /v1/targets` and `GET /v1/targets/{target}` return only targets allowed by
the caller's ACL. `GET /v1/sessions` lists only the caller's sessions; admins
may add `?all=true` or `?client=<name>`. Admin-only endpoints never return
token values, only a short `sha256:` fingerprint.
