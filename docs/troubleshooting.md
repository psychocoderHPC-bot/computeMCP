# Troubleshooting

Symptoms mapped to the most likely cause and the fix. For configuration
details see [configuration.md](configuration.md); for the provisioning bundle
and the `provision_command` contract see [provisioning.md](provisioning.md).

## Reading `last_error` and target status

Before changing anything, read the state:

- **`computeMCP-gatewayctl status [target]`** (or the console `status`) shows
  each target's state, active route, session count, the announced
  `provisioned_endpoint` (when a container target has an endpoint), and
  `last_error`.
- **`GET /v1/targets/{target}`** carries the same detail for one target.
- **`last_error`** is the gateway's own failure record for a target. Provision
  and connect scripts run on the remote over the route connection, so the
  gateway captures their output: when `provision_command`, `close_command`, or
  `connect_command` fails, its **stderr is embedded in `last_error`**. Checking
  it is the first step for every `disconnected` target.
- The MCP layer surfaces the gateway's HTTP status code as-is, so a `502 Bad
  Gateway` in `computeMCP_exec` / `computeMCP_file_*` output maps to a
  gateway-side dial failure on the table below.
- `provisioned_endpoint` shows the address the gateway currently dials (the
  container's published loopback host:port, or the allocation relay's local
  port); `null` means no endpoint is active.

## Symptom table

| Symptom | Cause | Fix |
| --- | --- | --- |
| Target is `connected` but every `exec`/file call fails with `502` | The container dial is refused by the container's sshd. Docker enforces `AllowUsers <container_user>` (key-only); Apptainer's Dropbear serves the account the sandbox was configured with. An explicit `container_user` that does not name an account the container has is rejected; the dialed account is `container_user`, or the `COMPUTEMCP_SSH_USER` override, or `ubuntu`, never the route `user` | Make `container_user` name the in-container account (the bundle default is `ubuntu`); unset it to get the helper default. For Apptainer the sandbox account is renamed to it on the next `configure`/`target-refresh`, so an explicit name works there too. Then `target-refresh <t>`. The route `user` (login) is fine as is; keep `container_user` in sync with the account the container was built for |
| `connected` + `502`, or the dial cannot reach the container at all | Stale endpoint: on a Slurm target the allocation was recycled or lost a node; on Docker a daemon restart re-published the container on a new ephemeral host port | `computeMCP-gatewayctl target-refresh <t>` re-runs provisioning, follows the new node/port, and refreshes the recorded endpoint (the helper queries the live published port instead of trusting its cached mapping). On a recreate, also update `host_key_sha256` (see "Host key not trusted" below) |
| Target fails to connect: `no host-key verification` / `Host key is not trusted` | Missing pin or stale pin: the container was recreated (new sshd host keys), or `host_key_sha256` is still a placeholder | Read the new fingerprint from the container and set `host_key_sha256` (+ `host_key_algorithms`), then `target-refresh <t>` |
| Target fails to connect: `Host key is not trusted for host` | Route or container key pin mismatch after recreate, or the alias now reaches a different sshd | Same as above; verify with `ssh-keygen -lf` on the key the gateway dials. Remember the pin covers the container sshd at the forwarded endpoint, not the login node |
| Route connects, but the container never starts ("container not built") | Missing `[targets.X.bundle]` block (and no manual `provision_command`): the gateway connected the route but is not told to build the container | Add the `[targets.X.bundle]` block (with `container.storage-root` or `bundle.deploy-dir`), `reload`, then `target-connect <t>`; see [provisioning.md](provisioning.md) |
| Bundle target fails to deploy: `bundle requires 'client_key'` / `bundle needs 'deploy-dir' or a container 'storage-root'` | Bundle validation is strict: it needs `client_key` (source of the container's authorized key) and a specific deploy location | Set `client_key` and either `bundle.deploy-dir` or `container.storage-root`; the deploy location must be visible to login and compute nodes |
| "Route works but nothing listens" (the provisioned endpoint never answers) | The container is not running (a stopped container keeps its files but has no listener), or the published/fetched port no longer matches the state the helper recorded after a start | Check on the remote: `docker ps -a` / `apptainer instance list`. To bring the container back automatically, set `connect_command` (a trusted remote script such as the shipped `scripts/ensure-container.sh`, or the bundle's own start path); otherwise start it by hand and run `target-refresh <t>` |
| `multi-node not yet supported` | `--set nodes=N` with `N > 1`: the gateway computes the plan, but the shipped helper rejects multi-node today | Use `nodes = 1` until multi-node support lands; the plan itself is computed and previewed correctly, the limitation is in the helper |
| Handshake cannot reach the gateway (connection refused/timeout from inside a Terok task) | Terok Shield is default-deny: the task cannot open the gateway endpoint until the project allows it | Add the reserved `localhost:<port>` host-service grant to the project's `project.toml` `shield.allow` (e.g. `localhost:2222`) and create a new task; a `host.containers.internal` allow alone does not open the port. See operations.md, "Terok Shield allow/override" |
| MCP tools fail with `COMPUTEMCP_GATEWAY is not set` / `COMPUTEMCP_TOKEN is not set` / `invalid token` | `COMPUTEMCP_GATEWAY`/`COMPUTEMCP_TOKEN` are missing from the MCP process environment, or a stale/rotated token | Re-run the handshake (or set the token per project); after a rotation, `reload` the gateway. A running agent does not see later `export`s, so paste the `environment` block into the MCP entry or start the agent from a fresh shell; see operations.md, "MCP bridge setup in the agent container" |
| `pipx install .` fails with a permission error in a fresh Terok container | `~/.local` is owned by root in Terok containers | `sudo chown -R dev:dev ~/.local` once |

## HPC / dynamic nodes

On an HPC system the login node is fixed, but the development container runs in
a Slurm job on a compute node whose name (and the forwarded port) only exist
once the job starts. The gateway supports this with `provision_command` (or the
shipped bundle), a trusted script that acquires the node and prints the
endpoint to dial. For the full contract, the worked examples, and the relay
port behavior, see [provisioning.md](provisioning.md).

Troubleshooting a dynamic target:

- **Run the provision script by hand first.** The gateway logs its stdout and
  includes its stderr in the target's `last_error`; the discovered address is
  reported as `provisioned_endpoint` in `status` and in
  `GET /v1/targets/{name}`. A script that prints the endpoint as
  `host:port` (an optional `ENDPOINT ` prefix is allowed; other log lines are
  fine) is the only contract.
- **Stale endpoint after a node recycle**: the allocation was recycled or lost
  a node, or a Docker daemon restart re-published the container on a new
  ephemeral host port. `target-refresh <t>` re-runs provisioning, follows the
  new node/port, and refreshes the recorded endpoint. On a recreate, also fix
  the stale `host_key_sha256`.
- **Relayed port moved**: the shipped bundle persists the concrete relay port;
  a reconnect against the same state directory reuses it while the tracked
  relay is still running. A `stop`/`close` removes the relay state, so a fresh
  provision picks a new port.

## The manual script paths

The reference `connect_command` script ships as
[`scripts/ensure-container.sh`](../scripts/ensure-container.sh). It inspects the
`computeMCP-container` (override with `COMPUTEMCP_CONTAINER_NAME`), starts it if
it is not running, waits for `running`, and is a no-op when it already runs, so
it is safe with `connect_command_mode = "always"`. Docker and Podman are both
supported. Copy it to the remote host and reference its absolute path; run it by
hand first to verify it detects and starts the container before wiring it into
the gateway.

Run the script by hand first for any manual `provision_command` or
`connect_command`: the gateway logs its stdout and includes its stderr in the
target's `last_error`. The full `provision_command` reference script (job
reuse, wait for the node, login-node forward, `ENDPOINT` output) and the
`connect_command` guidance live in [provisioning.md](provisioning.md), not here.
