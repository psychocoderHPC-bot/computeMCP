# computeMCP gateway configuration reference

This is the detailed configuration reference for the computeMCP gateway. The
gateway is configured in TOML; this file documents every key the loader
accepts, with exact key names, types, defaults, constraints, and error
behavior. Examples are realistic but simplified; for a fully commented
template start from [config.example.toml](../config.example.toml).

Key conventions used throughout:

- A hyphenated TOML key (`storage-root`) is read by the loader under its
  exact TOML spelling; there is no automatic dash/underscore conversion at the
  field level.
- A group header like `[targets.<name>.container]` names the *role* of the
  table: `<name>` is any concrete target name. The literal table name in the
  file must be, for example, `[targets.myTargetHost.container]`.
- "not set / pass-through" means the key is absent from the file and the
  loader supplies nothing — where a dataclass field exists it takes its
  annotation default, and where no field exists the value simply stays unset.
- "No default" means the loader supplies nothing when the key is absent; the
  field stays unset (`None`) and there is no derived fallback either.
- Fixed value sets are listed in full, with the meaning of each value.
  Unrestricted fields carry a realistic example that is *illustrative*, not
  exhaustive.

## 1. Configuration files

The default configuration location is:

    $XDG_CONFIG_HOME/computeMCP-gateway/config.toml

`$XDG_CONFIG_HOME` is honored as-is; when unset the base directory is
`~/.config`. The equivalent default paths for the companion files are:

- `$XDG_CONFIG_HOME/computeMCP-gateway/tokens.toml` — token file holding
  `sha256:` hashes (see [tokens.toml and operator.token](#15-tokenstoml-and-operatortoken-file-level-semantics));
- `$XDG_CONFIG_HOME/computeMCP-gateway/operator.token` — host-local plaintext
  operator token written by `computeMCP-gateway --bootstrap` with mode 0600.
  The operator CLI reads this file so an operator need not export a token at
  all.

The gateway `config.toml` is the **single source of truth** for which targets
exist and how they are reached. Nothing that arrives over the gateway protocol
may be used as an SSH destination; only values loaded from these files are
legal. Every key below is read, validated, and coerced at gateway start and
again at every `computeMCP-gatewayctl reload`; a validation failure is a
`ConfigError` that aborts the start (or rejects the reload) with a message that
names the file and the offending leaf.

Files involved:

| File | Purpose |
|---|---|
| `config.toml` | Entry gateway configuration: server, ssh, sessions, auth, clients, targets, and the optional `include` list. |
| `tokens.toml` | Token store read at load time; may live elsewhere, but by convention it sits next to `config.toml`. |
| `operator.token` | Plaintext operator bearer used by `computeMCP-gatewayctl` on the gateway host; created by `--bootstrap`, never read by a client. |
| `systems/<name>.toml` | Per-system configuration split out of the entry file via `include`. The directory name and layout are convention; only the files actually named in `include` are read. |

### `include` and merge semantics

`include` is a top-level array of strings. TOML requires it (like any bare
key) to precede the first table. If `include` is present:

- A missing file raises `configuration file not found: <path>`; malformed TOML
  raises `malformed TOML in <path>: <details>`; `include` itself must be a list
  of strings or the load fails.
- Relative entries resolve against the **declaring file's directory**, never
  the process working directory. Absolute entries are used as-is.
- Files are visited depth-first. Each file is visited at most once: a repeated
  include is a no-op (first-seen wins), so a diamond include with no cycle is
  fine. A cycle raises `include cycle detected: <chain>`.
- Two files that define the same leaf `dotted.key` raise
  `duplicate configuration value '<leaf>': defined in both <prev> and <p>` —
  duplicates are rejected, never silently overridden.
- The `include` key itself is stripped from the merged body before the rest
  of the configuration is processed; comment-only files contribute nothing
  and therefore never collide.

Splitting a large configuration across `systems/` works by keeping only
`include` and the target tables in the files it names:

````toml
# `config.toml`:
include = ["systems/myTargetHost.toml", "systems/secondTarget.toml"]

[clients.admin]
token_hash = "sha256:0123456789abcdef"
targets = ["*"]
````

````toml
# `systems/myTargetHost.toml`:
[targets.myTargetHost]
user = "alice"
remote_host = "myTargetHost.example.org"
````

The merged configuration then contains the entry file's `[clients]` table plus
the included files' `[targets]` tables; the `include` keys never appear in the
result. The gateway records the *included* files (in first-seen order, entry
excluded) as `include_paths` for diagnostics; `config_path` is always the
entry file the operator named.

## 2. Files and token resolution

`tokens.toml` is read at load time and its entries are normalized to
`client_id -> "sha256:<hash>"`. The loader accepts three shapes:

```toml
# tokens.toml:
[tokens]
# key = client id, value = plaintext (hashed at load time)
"alpaka" = "plaintext-token"
# key = client id, value = already hashed
"opencode" = "sha256:5f6f7c8e9d0a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2e3f4a5b6c"
# legacy reverse form: key = hash, value = client id
"sha256:1f2e3d4c5b6a798800112233445566778899aabbccddeeff00112233445566aa" = "legacy-client"
```

If the *key* starts with `sha256:` the value is the client id; if the *value*
starts with `sha256:` the key is the client id; otherwise the key is the
client id and the plaintext value is hashed at load time.

Token resolution precedence for a given client id (highest first):

1. the `token_hash` key in `[clients.<id>]` (must start `sha256:`);
2. the `token` key in `[clients.<id>]` (plaintext, hashed at load time);
3. the entry for the same client id in the token file.

If none exists, loading fails:
`[clients.<id>] needs token_hash, token, or an entry in the tokens file`.

Which token file is used follows this order:

1. the `--token-file` CLI flag of the gateway (or of
   `computeMCP-gatewayctl`), if given;
2. `[auth] token_file`;
3. the conventional `tokens.toml` next to the entry config, **only if it
   exists** — a missing conventional file is not an error (inline client
   tokens may be in use), but a missing explicitly named file is.

## 3. `[server]`

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `listen` | optional | string | Address the gateway HTTP server binds to. The gateway is only reachable where this address is reachable; keep it loopback unless placed behind a trusted path. | any address string, e.g. `127.0.0.1` or `::1` | `"127.0.0.1"` |
| `port` | optional | integer | TCP port the gateway HTTP server listens on. Port range 1–65535, no further range check. | `2222` | `2222` |
| `request_timeout` | optional | number (seconds) | Loaded into the server configuration; currently no runtime consumer reads it. Retained for forward compatibility. | `30.0` | `30.0` |
| `exec_timeout` | optional | number (seconds) | Default timeout applied to `computeMCP_exec` requests whose body does not carry an explicit `timeout`. | `900.0` | `900.0` |
| `max_body_bytes` | optional | integer | Maximum HTTP request body size accepted by the server. | `268435456` (256 MiB) | `268435456` |
| `allow_enrollment` | optional | bool | Enable out-of-band client enrollment (`computeMCP-handshake`). Approval of a pending request is always an explicit operator action; these keys only bound the unauthenticated surface. | `true` / `false` | `false` |
| `enroll_ttl` | optional | number (seconds) | Lifetime of a pending enrollment request before it expires. | `600.0` | `600.0` |
| `enroll_max_pending` | optional | integer | Maximum number of concurrently pending enrollment requests. | `32` | `32` |

The `--listen` and `--port` CLI flags of `computeMCP-gateway` override
`server.listen` / `server.port` after the config loads.

## 4. `[ssh]`

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `connect_timeout` | optional | number (seconds) | Timeout for SSH alias resolution (`ssh -G`) and the default dial timeout for a tunnel hop. | `10.0` | `10.0` |
| `server_alive_interval` | optional | integer (seconds) | SSH keepalive probe interval passed to asyncssh. | `30` | `30` |
| `server_alive_count_max` | optional | integer | Number of consecutive missed keepalive probes before the connection is dropped. | `3` | `3` |
| `internal_port_min` | optional | integer | Lower bound of the loopback port range scanned when allocating a forwarded port for a target. | `31000` | `31000` |
| `internal_port_max` | optional | integer | Upper bound of the forwarded-port range. Must be ≥ `internal_port_min`. | `31999` | `31999` |
| `config` | optional | string | Path to an SSH client config passed as `ssh -F <path>` for alias resolution (enables custom `Host` blocks and `ProxyJump`). | e.g. `/etc/ssh/computemcp.conf` | not set (no default; pass-through) |

Validation: `ssh.internal_port_min > ssh.internal_port_max` raises
`ssh.internal_port_min cannot exceed internal_port_max`.

## 5. `[sessions]`

Session manager limits for the persistent PTY sessions handed out through
`computeMCP_session_*`.

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `idle_timeout` | optional | number (seconds) | Sessions idle beyond this are closed by the session manager. | `3600.0` | `3600.0` |
| `max_per_client` | optional | integer | Maximum live sessions per client. | `16` | `16` |
| `output_buffer_bytes` | optional | integer | Size of the per-session output buffer (append-only window the consumer can reset); bytes outside it are discarded. | `4194304` (4 MiB) | `4194304` |

## 6. `[auth]`

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `token_file` | optional | string | Path to the tokens file for client token lookups. See [Files and token resolution](#2-files-and-token-resolution) for the full precedence chain and acceptance rules. A missing explicitly named file is an error; the conventional default is used only when it exists. | e.g. `/etc/computemcp/tokens.toml` | not set (no default; pass-through) |

## 7. `[clients.<id>]` (at least one required)

Each table under `[clients]` registers one authenticated client. The client id
is the table name and must match the target name grammar:
`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$` (at least one client is required).

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `token` | optional (one of `token_hash` / `token` / tokens-file entry is mandatory per client) | string | Plaintext token. Hashed via `sha256:` at load time. Takes precedence over an external tokens-file entry for the same client id, and sets a danger in the config file: prefer `token_hash` for long-lived deployments. | e.g. `pTc-…` (random string) | not set |
| `token_hash` | optional (same condition) | string | Precomputed token hash. Must start with `sha256:`. Highest precedence in the token resolution chain. | `sha256:<64 hex chars>` | not set |
| `targets` | optional | array of strings | ACL: target names this client may access. `"*"` means all targets. Every listed name except `"*"` must exist in `[targets]` or loading fails. | `["myTargetHost", "secondTarget"]`, `["*"]` | `()` (no targets) |
| `label` | optional | string | Free-form human-readable label for this client (shown in list output). | e.g. `"opencode for issue 42"` | not set |

Uniqueness: two clients must not resolve to the same token hash. The loader
checks this explicitly (ordering is insertion order, so the error names the
two affected ids); a directly built authenticator also fails closed with
`multiple clients share the same token`.

## 8. `[targets.<name>]` — route and tunnel scalars

A target is one dialable endpoint: a login environment that carries the
gateway's intended work. The table name is the target name, which is the value
operators use everywhere (ACLs, `computeMCP_targets()`, `--system` allow-lists,
`COMPUTEMCP_SYSTEM`). It must match `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`.

### Two distinct accounts: `user` vs `container_user`

- **`user`** is the *route* login account: the human/site account the gateway
  connects with when dialing the route host (the login or gateway host, and
  any proxy hop). It may be empty — an empty value lets the SSH config alias
  or the local account decide.
- **`container_user`** is the account dialed *inside the development
  container*. The container sshd only accepts this dedicated account; the
  provisioning helper creates it (default `ubuntu`). When unset the loader
  resolves it at dial time to `$COMPUTEMCP_SSH_USER` (when exported), else
  `ubuntu`. When set it must match `^[a-z_][a-z0-9_-]*$` and must not be
  `root`.

Unlike `user`, `container_user` is never shown to agents: it is an
infrastructure detail, not an operator hint.

### Table rows

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `user` | optional | string | *Route* login account used for the gateway → route hop. Empty (or absent) defers the user to the SSH alias or local account. Must be a string. | e.g. `"alice"`, `""` | not set → loader falls back to `""` |
| `container_user` | optional (conditionally mandatory — see note) | string | Container login account (see note above). Required to be valid and non-root when set; `root` is rejected. | e.g. `"ubuntu"`, `"test"` | not set → resolves to `$COMPUTEMCP_SSH_USER` else `"ubuntu"` |
| `client_key` | optional (conditionally mandatory when `[bundle]` is present) | string | Path (local, to the gateway host) of the private key the tunnel uses to dial the container. A bundle derives the container's public authorized key from it, so a bundle without `client_key` is a load error. | e.g. `"~/.ssh/computemcp_ed25519"` | not set |
| `known_hosts` | optional | string | Path to a known-hosts file. Used when `host_key_sha256` is not set and `host_key_check = "on"`. | e.g. `"~/.ssh/computemcp_known_hosts"` | not set |
| `host_key_sha256` | optional (conditionally mandatory when `host_key_check = "on"` and `known_hosts` is unset) | string | Exact SHA-256 host key fingerprint pinning the **container** host. Must start with `SHA256:` and be at least 12 characters long. | e.g. `"SHA256:aaa…"` (real fingerprint) | not set |
| `route_host_key_sha256` | optional | string | Exact SHA-256 fingerprint pinning the **route/login** host, checked before any container dial. Same shape constraint as `host_key_sha256`. | e.g. `"SHA256:bbb…"` | not set |
| `host_key_algorithms` | optional | array of strings | Offered host key algorithms (asyncssh negotiation order). Restrict listing changes negotiation; leave unset for the library default. | e.g. `["ssh-ed25519"]` | `()` (library default) |
| `host_key_check` | optional | string | Host key verification policy for this target. `"on"` (default) requires `host_key_sha256` or `known_hosts` and refuses to connect otherwise. `"off"` disables verification; only safe when the forwarded endpoint is trusted. | `"on"` / `"off"` | `"on"` |
| `connect_mode` | optional | string | How the gateway contacts this target when it is already connected: `"shared"` reuses an existing connection; `"dedicated"` opens a fresh one. | `"shared"` / `"dedicated"` | `"shared"` |
| `sharing` | optional | string | Whether this system is dedicated to this task or shared with other users/jobs. Exposed via `computeMCP_targets()`; benchmark hygiene depends on it. | `"exclusive"` (dedicated), `"shared"`, `"unknown"` | `"unknown"` |
| `node_info` | optional | array of strings | Free-form operator-authored hints about the system (unstructured; no fixed schema). Exposed to agents via `computeMCP_targets()` / `computeMCP_status()`. Use as a starting hypothesis only; verify actual hardware. | e.g. `["GPU nvidia A30", "x86 CPU", "100GbE"]` | `()` (no hints) |
| `agent` | optional | array of tables | Ordered list of remote AI agents this target can delegate to. Each entry is an inline table with exactly the keys `agent` and `model`, both non-empty strings. List order is the retry priority order; an empty list means no remote agent. Unrelated to the SSH `user` account. | `[{ agent = "opencode", model = "qwen3.6-27b-fp8" }]` | `()` (no agent) |
| `interactive_auth` | optional | bool | Whether the target requires interactive authentication (e.g. a second factor via `--2fa`). Targets with `interactive_auth` skip auto-connect. | `true` / `false` | `false` |
| `auto_connect` | optional | bool | Connect the target at gateway start rather than lazily on first use. Interaction rule: targets with `interactive_auth` are never auto-connected. | `true` / `false` | `false` |
| `provision_command` | optional (conditionally mandatory — requires `transport = "tunnel"`) | array of strings (argv) | Trusted, positional-free argv executed **on the remote route (login) host** over the authenticated route connection, after the tunnel is opened and before the container is dialed; its first `host:port` token on stdout becomes the forwarded endpoint. Not run through a shell; each element is `shlex`-quoted. Use it for a custom provisioner instead of `[bundle]`. | e.g. `["/home/USER/.config/computeMCP-gateway/myTargetHost-provision.sh"]` | `()` (no provisioner) |
| `provision_timeout` | optional | number (seconds) | Timeout for `provision_command`. | `900.0` | `900.0` |
| `close_command` | optional (conditionally mandatory — requires `transport = "tunnel"`) | array of strings (argv) | Trusted, positional-free argv executed **on the remote route host** over the live route connection when the target is stopped (explicit stop, gateway shutdown, or before a refresh). Advisory: a non-zero exit or timeout is logged and teardown proceeds. | e.g. `["scancel", "--signal=KILL"]` | `()` (no stop hook) |
| `close_command_timeout` | optional | number (seconds) | Timeout for `close_command`. | `120.0` | `120.0` |
| `connect_command` | optional (conditionally mandatory — requires `transport = "tunnel"`) | array of strings (argv) | Trusted, positional-free argv executed **on the remote route host** (through the base alias) when the tunnel probe fails. It is a recovery hook (e.g. start a stopped container) and never moves the endpoint. | e.g. `["docker", "start", "dev"]` | `()` (no recovery hook) |
| `connect_command_timeout` | optional | number (seconds) | Timeout for `connect_command`. | `120.0` | `120.0` |
| `connect_command_mode` | optional | string | When `connect_command` is run: `"on_failure"` (default) only when the initial tunnel probe fails; `"always"` before every connect attempt (the script must be a no-op when the container already runs). | `"on_failure"` / `"always"` | `"on_failure"` |
| `connect_backoff_initial` | optional | number (seconds) | Initial reconnect backoff delay. | `1.0` | `1.0` |
| `connect_backoff_max` | optional | number (seconds) | Upper bound on reconnect backoff delay. | `60.0` | `60.0` |

Transport selection. An explicit `transport` key wins. When absent, an
`ssh_targets` key (even an empty array) selects `tunnel` transport; its
absence selects `direct` transport. All transport keys are at the target root,
not under a nested table:

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `transport` | optional (conditionally mandatory — see note) | string | Force the transport kind for this target, overriding inference. | `"tunnel"` / `"direct"` | inferred: `"direct"` if no `ssh_targets` key, else `"tunnel"` |
| `remote_host` | optional | string | In `direct` mode this is the host the gateway dials directly. In `tunnel` mode with `ssh_targets`, the gateway follows them (the final entry is the container endpoint); `remote_host` is then the fallback endpoint used by the recovery probe. | e.g. `"127.0.0.1"`, `"login.example.org"` | `"127.0.0.1"` |
| `remote_port` | optional | integer | Port the container SSH server listens on (reachable after the tunnel is up). Must be in 1..65535. | `2222` | `2222` |
| `ssh_targets` | optional (conditionally mandatory when `transport = "tunnel"` and inferred from absence; at least one required) | array of strings | Ordered list of SSH alias/route entries for the tunnel. Each entry is a plain alias, or the syntax `user@host` for a direct route alias, or `user@host:port` / `user@port` for a non-standard port. The last confirmed reachable entry becomes the active dial path; earlier entries are intermediate hops. An empty array with `transport = "tunnel"` is a load error. | e.g. `["myTargetHost"]` | `()` (irrelevant unless tunnel) |
| `proxy_jump` | optional (conditionally mandatory — only valid with `transport = "tunnel"`) | string | SSH `ProxyJump` alias for the gateway → first-route-hop. Applies to the hop that reaches the first `ssh_targets` entry, not to the container dial. | e.g. `"myTargetHost"` | not set |

Transport validation (`__post_init__`):

- `kind` must be `"tunnel"` or `"direct"`.
- `remote_port` outside 1..65535 is rejected.
- `transport = "tunnel"` with empty `ssh_targets` is rejected.
- `proxy_jump` with `transport = "direct"` is rejected.

The `--transport` CLI flag of `computeMCP-gateway` forces the kind for **all**
targets after load. When forcing `tunnel` into a target that has no
`ssh_targets`, the target name becomes the single route entry.

## 9. `[targets.<name>.node]`

Describes the allocatable capacity of a single compute node. The
`gpu-proportional`, `cpu-proportional`, `full`, and `exclusive` allocation
modes consult these values. All fields are optional descriptions; an unset
field carries no capacity information and is not an error.

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `cpus` | optional | positive integer | Number of Slurm CPUs (under the site SMT policy) allocatable per node. Used by `cpu-proportional` as the full-node baseline; when unset, `allocation.default-cpus` (if set) is used instead. | e.g. `64` | not set (no default; pass-through) |
| `gpus` | optional | positive integer | Number of scheduler-visible GPU units per node. Used by `gpu-proportional` and `full` as the full-node baseline. | e.g. `8` | not set (no default; pass-through) |
| `memory` | optional | string | Allocatable host memory per node in Slurm memory units: `K`/`M`/`G`/`T`, with optional `i` (binary) and trailing `B` (e.g. `378000M`, `384G`, `384GiB`). A bare integer in the file is interpreted as MiB. Must resolve to at least 1 MiB. | e.g. `"378000M"`, `"384G"` | not set (no default; pass-through) |

Unknown keys under `[targets.<name>.node]` are a load error.

## 10. `[targets.<name>.allocation]`

Default allocation quantities and per-mode policy selection. One node is the
default allocation. A single-node allocation uses `single-node` (defaulting to
`gpu-proportional` when unset); a multi-node allocation uses `multi-node`
(defaulting to `full` when unset).

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `default-gpus` | optional | positive integer | Undocumented intent: the default GPU count for a GPU system. **Currently inert**: no runtime code path reads it, and `gpu-proportional` hard-codes a per-node GPU count of `1`. Set it for forward compatibility only. | e.g. `8` | not set (no default; pass-through) |
| `default-cpus` | optional | positive integer | Default CPU count for a CPU-only system (i.e. `node.gpus` unset). Used by `cpu-proportional` as the full-node baseline when `node.cpus` is unset. | e.g. `32` | not set (no default; pass-through) |
| `single-node` | optional | string | Allocation policy applied when one node is allocated. | one of `gpu-proportional` (scale a partial GPU/CPU request to one GPU or CPU per node), `cpu-proportional` (request CPUs only), `full` (request all node resources), `exclusive` (all node resources plus `--exclusive`) | one of the four listed values | not set → `gpu-proportional` |
| `multi-node` | optional | string | Allocation policy applied when several nodes are allocated. Deliberately restricted. | one of `full` (all resources per node), `exclusive` (all resources per node plus `--exclusive`) | one of the two listed values | not set → `full` |
| `max-nodes` | optional | positive integer | Hard upper bound on the number of nodes a single allocation may request. | e.g. `4` | not set (no default; pass-through) |

Unknown keys under `[targets.<name>.allocation]` are a load error.

## 11. Slurm stages: sbatch, srun, and their maps

The `[slurm]` table is the per-target Slurm configuration. It holds two
independent stages:

- `sbatch` — manual submission options (and an optional mapping) controlling
  what `sbatch` receives.
- `srun` — manual job-step options (and an optional mapping) controlling what
  `srun` receives inside the batch job.

Nothing is copied between stages: the two can map the same calculated value
differently. The accepted TOML uses exactly four nested table names:
`[targets.<name>.slurm.sbatch]`, `[targets.<name>.slurm.sbatch-map]`,
`[targets.<name>.slurm.srun]`, `[targets.<name>.slurm.srun-map]`. An empty
`[targets.<name>.slurm]` table is equivalent to no slurm block at all. A
missing stage becomes an empty stage configuration (no args rendered).

### `account` (reserved first-class key, per stage)

`account` is pulled out of the manual options into a first-class field and
never appears in the rendered manual argument list. When set and non-empty it
is rendered as `--account=<value>` **first** in the stage's argument list.
`None` and an empty/whitespace-only string both mean "emit no `--account`".
Non-strings and strings containing whitespace, newlines, carriage returns, or
NUL are a load error. Setting it on one stage does not affect the other.

### Manual options: rendering rules

Keys are Slurm option names **without** the leading `--`. Spelling is
preserved verbatim. The value decides the rendering:

| TOML value form | Rendered argument |
|---|---|
| string or integer (non-empty text) | `--<key>=<value>` |
| string or integer (empty text) | `--<key>` |
| `true` | `--<key>` |
| `false` | omitted (no argument) |
| array of strings/integers | repeated: one `--<key>=<item>` per item; empty array drops the option entirely |
| array containing a bool or non-string/non-int | load error (array items must be strings or integers) |
| `null`, table, or other unsupported type | load error (must be string, integer, bool, or array) |

Expected values:

- Option names must be plain tokens: no surrounding whitespace, no
  whitespace or control characters, and must not start with `-`, `,`, `\`,
  `"`, or `'`.
- Values must not contain newline, carriage return, or NUL.
- The reserved protocol options `parsable`, `quiet`, `wrap` (matched
  case-insensitively) are owned by the provisioning helper and are rejected in
  manual options: the helper supplies its own `sbatch --parsable` etc.

Realistic examples (not exhaustive, not a Slurm man page):

```toml
[targets.myTargetHost.slurm.sbatch]
account = "proj-42"          # rendered first: --account=proj-42
partition = "gpu"            # --partition=gpu
time = "01:30:00"            # --time=01:30:00
job-name = "devcontainer"    # --job-name=devcontainer
ntasks-per-node = 1          # --ntasks-per-node=1
gres = ["gpu:2", "v100"]     # --gres=gpu:2 --gres=v100
mem = "100G"                 # --mem=100G

[targets.myTargetHost.slurm.srun]
ntasks-per-node = 1
cpus-per-task = 8
gpu-bind = "per-socket"
```

### Mapping vocabulary (`sbatch-map` / `srun-map`)

A mapping ties a *calculated* plan field to a *concrete* Slurm option the
stage emits. Each mapping key must be one of the vocabulary keys below, and
each value must be one of the listed representations. Unknown key or
out-of-set value is a load error. A mapping entry whose calculated value is
not set in the plan emits nothing:

| Mapping key | Allowed values | Emitted regardless of value? | Meaning |
|---|---|---|---|
| `nodes` | `"nodes"` | yes (when node count > 0) | `--nodes=<N>` |
| `gpus-per-node` | `"gres"`, `"gpus-per-node"` | yes (when > 0) | `--gres=gpu:<N>` or `--gpus-per-node=<N>` |
| `cpus-per-node` | `"cpus-per-task"` | yes (when ≥ 1) | `--cpus-per-task=<N>` — see precondition |
| `memory-per-node` | `"mem"` | yes (when > 0) | `--mem=<MiB>M` |
| `exclusive` | `"exclusive"` | no: only emits `--exclusive` when the resolved plan is exclusive | request all node resources plus `--exclusive` |

**`cpus-per-node` → `cpus-per-task` precondition.** `--cpus-per-task` pins one
task per node. The mapping is therefore only admitted when the stage's manual
options declare exactly one task per node: the effective task count is read
from `ntasks-per-node` (first) then `ntasks` (fallback); an absent or invalid
value fails with an actionable message naming the option to set. The rule is
validated at load time and re-checked from the stage's own manual options by
`validate_conflicts` at start and every reload, so plan-time and config-time
semantics are locked.

**Same-stage conflict rules.** An enabled mapping and a conflicting manual
option in the **same stage** is always an error, never a silent precedence
rule:

| Mapping key | Conflicting manual option names |
|---|---|
| `nodes` | `nodes`, `n` |
| `gpus-per-node` | `gres`, `gpus`, `gpus-per-task`, `gpus-per-node` |
| `memory-per-node` | `mem`, `mem-per-cpu` |
| `exclusive` | `exclusive` |

Manual options in the *other* stage never conflict. Cross-resource MCP
conflicts (an explicit partial GPU/CPU override against an `exclusive` plan)
are a `conflict` error at plan time, not a mapping-manual issue.

**Render order per stage** is deterministic: `account` (if set), then manual
options in configuration order, then mapped options in mapping order. If a
rendered argument appears twice the gateway raises an internal assertion
(failure indicator, not a user-configurable path).

**`--set` override keys.** The `computeMCP-gatewayctl --set KEY=VALUE` flag
accepts only: `nodes`, `gpus-per-node`, `cpus-per-node`, `mem-per-node`, `mode`.
Unknown keys are rejected. Overrides resolve before mapping: they feed the
mappings, never the other way around; a multi-node plan with an explicit
partial GPU/CPU override conflicts with `multi-node = "full"`.

## 12. `[targets.<name>.container]`

Describes the development-container runtime the gateway provisions. Present
when the gateway manages a container (apptainer or docker) on the login
or compute node. `runtime` is the only required key.

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `runtime` | required (when this block is present) | string | Container runtime used to provision the development environment. | `"apptainer"` / `"docker"` | required |
| `storage-root` | optional | string | Storage root visible to both login and compute nodes where apptainer pulls or caches its container. Non-empty. | e.g. `"/scratch/aaa/computemcp"` | not set (no default; pass-through) |
| `image` | optional | string | Container image reference to pull/use. Non-empty. | e.g. `"nvcr.io/nvidia/apptainer:2310"` | not set (no default; pass-through) |
| `gpus` | optional | array of strings | GPU vendors the container can expose. Must be a subset of `nvidia`, `amd`, `intel`; duplicates are preserved in first-seen order. Empty means no GPU passthrough. | e.g. `["nvidia"]`, `["nvidia", "amd"]` | `()` (no GPU passthrough) |
| `host-home` | optional | string | Host path to bind-mount into the container as `$HOME`. Non-empty. | e.g. `"/home/alice"` | not set (no default; pass-through) |
| `sandbox` | optional | bool | Enable sandbox mode (a writable image layer instead of a read-only run). | `true` / `false` | `false` |
| `build-location` | optional | string | Where the apptainer sandbox is built: `"login"` (default) builds and configures on the login/head node before submitting the allocation; `"compute"` skips the login build and builds on the first allocated compute node (required on an architecture-mismatched partition and requires Slurm). `"compute-node"` is an accepted alias normalized to `"compute"`. | `"login"` / `"compute"` (alias `"compute-node"`) | `"login"` |

Unknown keys under `[targets.<name>.container]` are a load error. This block
alone (no node/allocation/slurm) still makes `_env_configured` true so the
`COMPUTEMCP_*` provisioner contract is built (see [Gateway to provisioner contract](#18-gateway-to-provisioner-contract)).

## 13. `[targets.<name>.bundle]`

Names the helper bundle the gateway deploys to provision the container.
Requires `transport = "tunnel"` (also `provision-env`). A bundle without
`deploy-dir` and without `container.storage-root` is a load error; a bundle
without `client_key` is a load error.

| Keyword | Required / optional | Type | Description | Allowed values / example | Default |
|---|---|---|---|---|---|
| `source` | required (when this block is present) | string | Bundle identifier shipped in the `compute_mcp.bundles` package. Both names resolve to the same directory and digest. | `"computemcp-container"` (canonical) or `"computemcp-slurm"` (legacy alias) | required |
| `deploy-dir` | optional (conditionally mandatory if `container.storage-root` is not set) | string | Remote directory on shared storage where the bundle is deployed. Must be absolute or start with `$HOME`/`~` (the remote helper expands `~`/`$HOME`). | e.g. `"/scratch/aaa/computemcp/bundle"`, `"$HOME/computemcp/bundle"` | not set → derived from `container.storage-root` + `/bundle` |
| `auto-deploy` | optional | bool | Upload the bundle when the remote hash marker differs. `false` pins the existing copy and logs a warning when the marker is absent. | `true` / `false` | `true` |
| `provision-env` | optional (requires `transport = "tunnel"`) | array of strings | Shell lines executed on the remote **before** the container runtime is used. Non-empty; no NUL or carriage return. | e.g. `["export CUDA_HOME=/usr/local/cuda-12.4"]` | `()` (no hooks) |

Unknown keys under `[targets.<name>.bundle]` are a load error. When the bundle
is present, `COMPUTEMCP_SSH_PUBLIC_KEY` (derived from `client_key`) and
`COMPUTEMCP_PROVISION_ENV` (joined `provision-env` lines) are entered into the
provisioner contract; see [Gateway to provisioner contract](#18-gateway-to-provisioner-contract).

## 14. Nested tables, arrays, dynamic names, pass-through

- **Dynamic target names.** Tables under `[targets.<name>]` carry the target
  name in the table name itself. The name is validated against
  `^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`. Any number of target tables is legal;
  zero is also legal (the gateway then has nothing to dial and only the
  clients are meaningful). The same regex is used for client ids, `--system`
  entries, and the MCP tool parameter `target`.
- **Nested and inline tables.** `[targets.<name>.node]`,
  `[targets.<name>.allocation]`, `[targets.<name>.slurm.{sbatch,sbatch-map,
  srun,srun-map}]`, `[targets.<name>.container]`, and
  `[targets.<name>.bundle]` are all nested tables. Unknown keys inside any of
  them are a load error. The `agent` key is not a nested table but an array
  of inline tables with exactly the two keys `agent` and `model`.
- **Arrays.** All array-valued keys are open lists. A few arrays carry a fixed
  value set at the element level: `container.gpus` (`nvidia`/`amd`/`intel`),
  `client.targets` (target names plus `"*"`), `host_key_algorithms`
  (negotiated strings), `ssh_targets` (alias strings). Everything else is
  unrestricted and examples above are illustrative.
- **Open-ended manual Slurm options.** `slurm.sbatch` and `slurm.srun` are the
  one place in the configuration where the key set is *not* exhaustive: any
  valid Slurm option name (without leading `--`) is accepted, subject to the
  rendering rules in [Slurm stages](#11-slurm-stages-sbatch-srun-and-their-maps) and the protocol-option rejection list. The two
  dictionary entries above (`ntasks-per-node`, `partition`, `gres`, `mem`,
  `account`, etc.) are representative; this reference does not enumerate the
  Slurm manual options.
- **Pass-through vs. concrete default.** Most keys have no default: absent
  means the dataclass field takes its annotation default (e.g.
  `container_user = None`, `client_key = None`), which the loader records as
  unset. A few keys have concrete loader defaults (e.g.
  `host_key_check = "on"`, `connect_mode = "shared"`, `sharing = "unknown"`,
  `provision_timeout = 900.0`, `auto-deploy = True`). "No default" in the
  tables above always means the loader supplies nothing.

## 15. `tokens.toml` and `operator.token` (file-level semantics)

`tokens.toml` and `operator.token` are not renewed by the config loader; they
are companion files:

- `tokens.toml` sits by convention next to the entry config. Its `[tokens]`
  table (or the raw table if the wrapper is absent) is read at load time and
  entries are normalized as described in [Files and token resolution](#2-files-and-token-resolution). The file may hold plaintext or
  hashed entries; hashes are recommended for long-lived setups because a
  plaintext entry in the file is only less exposed than one in `config.toml`,
  not zero.
- `operator.token` is written with mode 0600 by
`computeMCP-gateway --bootstrap`.
  `computeMCP-gatewayctl` reads it (precedence: `--token` flag →
  `operator.token` file → `COMPUTEMCP_TOKEN` env → plaintext in tokens file)
  so an operator on the gateway host does not need to export a token.

## 16. CLI arguments

The CLI surface is separate from the TOML config: a CLI flag either overrides
a specific config value or blocks the whole request.

### `computeMCP-gateway`

| Flag | Required / optional | Default | Meaning |
|---|---|---|---|
| `--config PATH` | optional | default config path (see [Configuration files](#1-configuration-files)) | Path to `config.toml`. |
| `--listen HOST` | optional | from `server.listen` | Override `server.listen` after load. |
| `--port PORT` | optional | from `server.port` | Override `server.port` after load. |
| `--transport {tunnel,direct}` | optional | per-target inference | Force the transport kind for all targets after load; forces `tunnel` into a target with no `ssh_targets` by setting the target name as the single route entry. |
| `--token-file PATH` | optional | from `[auth] token_file` | Override `[auth] token_file` after load. |
| `--no-console` | optional | false | Run headless. Headless is also implied when stdin is not a TTY. |
| `--generate-tokens [OUT]` | optional (arg optional; defaults to the default token path) | — | Generate `sha256:` hashes for all clients, write them to OUT (or the default token path), print the plaintext tokens once, and exit without binding. |
| `--bootstrap` | optional | false | Run the interactive setup wizard, write the config, optionally add a target, and exit. |
| `--config-dir DIR` | optional | directory of `--config` | Location to write into when `--bootstrap` runs. |
| `--force` | optional | false | `--bootstrap`: overwrite an existing configuration. |
| `--non-interactive` | optional | false | `--bootstrap`: fail instead of prompting. |
| `--log-level LEVEL` | optional | `INFO` | Logging level. |
| `--version` | optional | — | Print version and exit. |

### `computeMCP-gatewayctl`

Global flags (before the subcommand) plus one subcommand:

Global flags:

| Flag | Required / optional | Default | Meaning |
|---|---|---|---|
| `--config PATH` | optional | default config path | Path to `config.toml` (used to read targets and clients). |
| `--token-file PATH` | optional | from `[auth] token_file` | Override `[auth] token_file`. |
| `--gateway URL` | optional | from env or `http://host:port` | Gateway base URL. `--gateway` wins over `COMPUTEMCP_GATEWAY` env which wins over the host/port pair. |
| `--token TOKEN` | optional | operator default (see [tokens.toml and operator.token](#15-tokenstoml-and-operatortoken-file-level-semantics)) | Operator bearer token for admin operations. Precendence: `--token` → `operator.token` file → `COMPUTEMCP_TOKEN` env → plaintext in tokens file. |
| `--client NAME` | optional | `"admin"` | Client id whose token to use for the `--token` fallback chain. |
| `--json` | optional | false | Print raw JSON instead of human output. |
| `--timeout SECONDS` | optional | derived | Global HTTP timeout. Subcommand-level `--timeout` (if set) overrides this. |
| `--add-target` | optional | false | Interactively append a `[targets.X]` block to the config and exit. No running gateway required. |

Subcommands: `status`, `targets`, `clients`, `sessions`, `reload`,
`enrollments`, `approve <request_id>`, `deny <request_id>`,
`client <name>`, `target-connect <target>`, `target-refresh <target>`,
`target-stop <target>`, `client-connect <name> [target]`,
`client-refresh <name> [target]`, `client-stop <name> [target]`,
`client-kill <name> [target]`. The subcommand is optional at parse time;
`main` errors if neither a command nor `--add-target` is given.
`target-connect` and `target-refresh` additionally accept:

- `--2fa SECRET` — second factor (password/OTP) for `interactive_auth` targets;
- `--set KEY=VALUE` (repeatable) — allocation overrides; accepted keys are
  `nodes`, `gpus-per-node`, `cpus-per-node`, `mem-per-node`, `mode`;
- `--timeout SECONDS` — per-request timeout override (default: target's
  `provision_timeout` plus a 60 s margin);
- `--dry-run` — preview the allocation without connecting (no allocation).

### `computeMCP-handshake`

| Flag | Required / optional | Default | Meaning |
|---|---|---|---|
| `client_id` (positional) | required | — | Project/client id to enroll, e.g. `myTaskName`. |
| `--gateway URL` | optional | none | Gateway base URL; overrides env / host / port. |
| `--host HOST` | optional | `host.containers.internal` | Gateway host (used when `--gateway` is unset). |
| `--port PORT` | optional | `2222` | Gateway port (used when `--gateway` is unset). |
| `--system LIST` | optional | none (empty ACL) | Comma-separated target allow-list, e.g. `myTargetHost`; set `--system myTargetHost,secondTarget` for two targets. Omit for an empty ACL; `*` inside the list means all targets. |
| `--label TEXT` | optional | none | Human-readable label for this client. |
| `--bashrc PATH` | optional | `~/.bashrc` | Shell file to update with the exports. |
| `--env-file PATH` | optional | none | Write exports to this file and source it from `--bashrc` (keeps the token out of `~/.bashrc`). |
| `--no-write` | optional | false | Do not touch any file; print the snippet only. |
| `--timeout SECONDS` | optional | `900.0` | Seconds to wait for operator approval. |
| `--poll-interval SECONDS` | optional | `3.0` | Poll interval for approval status. |
| `--json` | optional | false | Machine-readable result on stdout. |

### `computeMCP-mcp`

No CLI arguments. `main()` reads only the runtime environment
(`COMPUTEMCP_GATEWAY` and `COMPUTEMCP_TOKEN`); missing either is a hard
error with a message pointing to the handshake flow.

## 17. Environment variables

| Variable | Set by | Read by | Meaning and emission condition |
|---|---|---|---|
| `XDG_CONFIG_HOME` | operator | gateway config loader | Base directory for the default `config.toml`, `tokens.toml`, and `operator.token` paths. When unset, `~/.config` is used. No fallback beyond that. |
| `COMPUTEMCP_GATEWAY` | operator (per project) or generated by handshake / MCP | `computeMCP-gatewayctl`, `computeMCP-handshake`, `computeMCP-mcp` | Gateway base URL. For `gatewayctl`: `--gateway` flag wins, then this env, then `http://host:port`. For `handshake`: `--gateway` flag wins, then this env, then `http://host:port`. For `mcp`: required; its absence is a hard error. |
| `COMPUTEMCP_TOKEN` | operator (per project) or generated by handshake / MCP | `computeMCP-gatewayctl`, `computeMCP-mcp` | Bearer token. For `gatewayctl`: `--token` flag wins, then `operator.token` file, then env, then the plaintext in tokens file (by the `--client` id). For `mcp`: required; its absence is a hard error. |
| `COMPUTEMCP_SSH_USER` | operator (exported on the gateway host) | gateway dial path | Alternate login account inside the container when `container_user` is not set in the target. Emits into the provisioner contract (see below) only when a container or bundle block is present. |

Note on `COMPUTEMCP_SSH_USER`: it is both an operator-side env variable (read
by the gateway when resolving `container_user`) **and** a value emitted by the
gateway into the provisioner contract (see next section) when a container or
bundle is present. The two directions use the same resolution
(`container_login_user`).

## 18. Gateway to provisioner contract

When a target has an allocation-configured plan (`node`, `allocation`, or
`slurm` block, or a `--set` override), or a container/bundle block, the
gateway builds a contract of `COMPUTEMCP_*` variables and emits them as
shell-quoted `export NAME='value';` statements prepended to `provision_command`
(or to the bundle provisioner's entry script). A target that has only
`provision_command` and none of those blocks runs its command **without** any
`COMPUTEMCP_*` exports.

The whole contract is built only when `_env_configured(target)` is true —
i.e. any of `[node]`, `[allocation]`, `[slurm]`, `[container]`, `[bundle]` is
present — or when connect-time `--set` overrides are supplied. On the
close/connect-recovery path the gateway falls back to a minimal
`{COMPUTEMCP_SYSTEM}` + container fields when the plan cannot be resolved.

Every contract value containing a NUL or carriage return is rejected with an
error before the provision command is started.

The contract, with the condition under which each variable is emitted:

| Variable | Value / when emitted |
|---|---|
| `COMPUTEMCP_SBATCH_ARGS` | One rendered argv per newline, no trailing newline. Empty string = empty sbatch stage. Emitted on every plan path. |
| `COMPUTEMCP_SRUN_ARGS` | Same, for the srun stage. |
| `COMPUTEMCP_NODES` | `str(plan.nodes)`. Always concrete. |
| `COMPUTEMCP_CPUS_PER_NODE` | Plan value as decimal string, or `""` when the plan has no value. |
| `COMPUTEMCP_GPUS_PER_NODE` | Plan value as decimal string, or `""` when the plan has no value. |
| `COMPUTEMCP_MEMORY_PER_NODE_MIB` | Plan value as decimal string, or `""` when the plan has no value. |
| `COMPUTEMCP_EXCLUSIVE` | `"true"` / `"false"`. Always concrete. |
| `COMPUTEMCP_MODE` | `plan.mode`. Always concrete. |
| `COMPUTEMCP_SYSTEM` | `target.name`. Always emitted. |
| `COMPUTEMCP_CONTAINER_RUNTIME` | `container.runtime` value, or `""` when no container block. Emitted whenever the contract is built. |
| `COMPUTEMCP_STORAGE_ROOT` | `container.storage_root`, or `""`. |
| `COMPUTEMCP_IMAGE` | `container.image`, or `""`. |
| `COMPUTEMCP_GPU_VENDORS` | `container.gpus` joined by `,`, or `""`. |
| `COMPUTEMCP_HOST_HOME` | `container.host_home`, or `""`. |
| `COMPUTEMCP_SANDBOX` | `"true"` iff a container exists and `sandbox` is true; else `"false"`. |
| `COMPUTEMCP_BUILD_LOCATION` | `container.build_location` (normalised: `login`/`compute`), or `"login"`. |
| `COMPUTEMCP_SSH_USER` | `container_login_user(target)` — the resolved container dial account. Emitted **only** when a container or bundle block is present. |
| `COMPUTEMCP_SSH_PUBLIC_KEY` | Public key derived from `client_key`. Emitted **only** when a bundle block is present **and** the derived public key is non-empty. |
| `COMPUTEMCP_PROVISION_ENV` | `bundle.provision_env` lines joined by newline. Emitted **only** when a bundle block is present. An empty tuple yields `""`. |

Out of the gateway → provisioner contract (managed by the bundle, not the
gateway, and listed here only to separate responsibilities):
`COMPUTEMCP_STATE_DIR`, `COMPUTEMCP_SANDBOX_DIR`, `COMPUTEMCP_CONTAINER_PORT`,
`COMPUTEMCP_SSH_WAIT_SECONDS`, `COMPUTEMCP_FORWARD_PORT`. The bundle derives
or sets these from what the gateway exports; the gateway never sets them
directly.

Additionally, SSH exec operations (the remote exec surface used by
`computeMCP_exec` / session write) inject per-call environment variables into
the remote shell as `export NAME=...;` statements. This is a separate
contract from the provision environment above and is out of scope for the
gateway configuration file.

## 19. Configuration vs CLI vs environment

| Concern | Decided by |
|---|---|
| Which targets exist, their route and dial parameters | `config.toml` (single source of truth) |
| Which clients may connect, which tokens they use | `config.toml` (`[clients.*]`) plus the tokens file, or inline `token`/`token_hash` |
| Server bind, ports, enrollment behavior | `config.toml` `[server]`; `--listen`/`--port` CLI flags after load |
| Transport mode override for all targets | `computeMCP-gateway --transport` |
| Allocation overrides at connect time | `computeMCP-gatewayctl --set` (with `target-connect`/`target-refresh` only) |
| Where a client/MCP finds the gateway and its token | `COMPUTEMCP_GATEWAY` and `COMPUTEMCP_TOKEN` env (or `--gateway`/`--token` flags for `gatewayctl`/`handshake`) |
| Base config dir | `XDG_CONFIG_HOME` env |
| Container login account dial fallback | `container_user` key in the target, else `COMPUTEMCP_SSH_USER` env, else `ubuntu` |
| Per-exec (non-provision) environment | `ssh_backend` exec-phase env, injected as shell exports |

Rule of thumb: if the gateway restarts with a different answer, the answer is
in `config.toml`. If only one project on the gateway host is different, the
answer is in that project's env. If the operator wanted a one-off preview, it
is in `--set` / `--dry-run`. Mixing purposes is allowed but not required:
there is always exactly one source of truth for the gateway's static
behavior (the config file) and exactly one source for per-run decisions
(`--set` / env / CLI).

## Cross-references

- Example configuration: [config.example.toml](../config.example.toml)
- Overview and quick start: [README.md](../README.md)
- Operator/compute runtime contracts: [skills/computeMCP/SKILL.md](../skills/computeMCP/SKILL.md)
