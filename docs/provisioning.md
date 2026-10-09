# Provisioning the remote development container

The gateway needs a persistent, key-only SSH endpoint inside a remote
development container to dial. Handing it to the gateway is one of two
workflows; you use whichever matches your system. This page organizes the
provisioning content that used to sit across several README sections.

| Workflow | When to use it |
| --- | --- |
| **A: computeMCP-managed provisioning** via `[targets.X.bundle]` | The route connection is your only trusted path to the machine and you want the gateway to deploy, build, start, and keep the container alive. Cover: bare Docker host, Slurm login node, Slurm + Apptainer. |
| **B: Existing or custom environment** via `provision_command` / `close_command` / `connect_command` | The container (or tool) already exists, the scheduler is site-specific and out of band, or the site scripts already own the job. The operator maintains the recreation logic by hand. |

Key ownership rule that applies to both under workflow A: **the gateway
deploys "and runs" the bundle over the route connection, so no manual
`docker run` and no manual `apptainer build` are required.** Manual creation
scripts in [`examples/`](../examples/) are for workflow B only (or for testing
wiring in workflow A); they are never "required" when a `[bundle]` block is
selected.

Every example lives under
[`examples/`](../examples/) and is indexed at
[`examples/README.md`](../examples/README.md).

---

## 1. Workflow A: computeMCP-managed provisioning

### 1.1 Select the bundle

Add a `[targets.X.bundle]` block. The two declared bundle identifiers resolve
to the same shipped package (`src/compute_mcp/bundles/computemcp-slurm/`):

- `source = "computemcp-container"` (canonical).
- `source = "computemcp-slurm"` (legacy alias; same payload).

All other configuration is placed in the surrounding target block:
`[targets.X]`, `[targets.X.node]`, `[targets.X.allocation]`,
`[targets.X.slurm.*]`, `[targets.X.container]`.

```toml
[targets.example]
ssh_targets = ["example-login"]
user = "agent"
client_key = "/home/USER/.ssh/computemcp_container"
host_key_sha256 = "SHA256:REPLACE_WITH_CONTAINER_HOST_KEY_FINGERPRINT"
host_key_algorithms = ["ssh-ed25519"]

[targets.example.bundle]
source = "computemcp-container"
# deploy-dir = "$HOME/computemcp/bundle"    # default: <container.storage-root>/bundle
# auto-deploy = true                        # default; false pins the deployed copy
# provision-env = ["module load apptainer"] # one shell line per array entry

[targets.example.container]
runtime = "apptainer"                       # or "docker"
storage-root = "/scratch/USER/computemcp"
image = "docker://ubuntu:24.04"
gpus = ["nvidia"]                           # subset of nvidia, amd, intel
host-home = "/scratch/USER/computemcp/home"
# build-location = "login"                  # or "compute" (Slurm only)
```

Because a bundle block is present, `provision_command` is **not** required:
the gateway itself runs the deployed helper. Only set `provision_command` to
opt back into manual management.

### 1.2 Bundle keys

| Key | Type | Behavior |
| --- | --- | --- |
| `source` | string | `computemcp-container` or legacy alias `computemcp-slurm`. Both map to the same shipped directory. |
| `deploy-dir` | string | Remote absolute directory on shared storage. Default: `<container.storage-root>/bundle`. Must be visible to **both** login and compute nodes. |
| `auto-deploy` | bool | `true` (default) re-uploads when the content marker differs; `false` pins the copy already on the login node. |
| `provision-env` | list of strings | Each entry is one shell line, run before the container runtime is used: once on the login node before the build, and again inside the container-start path on the job node. |

Login-node prerequisites: Slurm client tools (`sbatch`, `squeue`, `scancel`,
`srun`), Bash, `flock`, `python3`, and the selected runtime (`apptainer` or
`docker`). Compute nodes need the same runtime and `python3`.

### 1.3 What the helper does

When the gateway deploys and runs `computemcp-provision.sh`:

1. **Build or reuse the sandbox / image** on the login (or compute) node
   depending on `container.build-location`. The default location builds on
   the login node. On Apptainer, the sandbox is content-addressed by the
   gateway bundle hash when `auto-deploy = true` (the default), so a gateway
   upgrade re-builds only when needed.
2. **Install the container SSH key**. For a bundle target the gateway
   derives it from `client_key` (the `.pub` half, or `ssh-keygen -y` if
   only the private half is present). The private key stays on the gateway
   host; only the public half travels.
3. **Submit a single-node Slurm allocation** when
   `[targets.X.slurm.sbatch]` / `[targets.X.container]` are present and
   Slurm is on PATH. Direct (non-Slurm) hosts skip this step entirely and
   the container starts on the login/route node.
4. **Start a relay** on the login node (`computemcp-relay.py`) that persists
   the concrete forwarded port across reconnects, so several targets on one
   login node never collide. The default is an ephemeral loopback port; a
   fixed override is possible via `COMPUTEMCP_FORWARD_PORT`.
5. **Print `ENDPOINT host:port`** to stdout. The gateway follows the
   endpoint through `ssh_targets` and dials into the container's sshd.

### 1.4 Sensible defaults

- **GPU default** on a Slurm allocation is **one GPU per node** in
  `gpu-proportional` mode. Setting `node.gpus` and `allocation.single-node =
  "gpu-proportional"` is the minimal configuration for a typical
  HPC cluster.
- **CPU-only targets** set `allocation.single-node = "cpu-proportional"`,
  optionally `allocation.default-cpus = N`.
- **`allocation.default-gpus`** is accepted but currently inert: the
  allocation mode itself fixes the per-node GPU count (one in
  `gpu-proportional`, the full node capacity in `full` / `exclusive`).
  Do not set it to change the request.

### 1.5 Slurm blocks

The four Slurm blocks are filled in only when the target sits on a Slurm
cluster. Their full semantics are documented in
[README `Slurm allocation and container configuration`](../README.md#slurm-allocation-and-container-configuration):

- `[targets.X.node]` — per-node capacity (`cpus`, `gpus`, `memory`).
- `[targets.X.allocation]` — mode and defaults
  (`single-node`, `multi-node`, `default-cpus`, `max-nodes`).
- `[targets.X.slurm.sbatch]` / `.srun` — free-form manual scheduler options
  plus `account`.
- `[targets.X.slurm.sbatch-map]` / `.srun-map` — mappings from the
  gateway-computed plan to scheduler flags (see
  [`../examples/slurm-gpu-apptainer.toml`](../examples/slurm-gpu-apptainer.toml)
  for a concrete combination, and
  [`../examples/non-slurm-docker-host.toml`](../examples/non-slurm-docker-host.toml)
  for the same target tree with no Slurm blocks).

The `[targets.X.container]` block is described in
[section 2.7](#27-targetsxcontainer-block) below.

### 1.6 `multi-node not yet supported`

The gateway computes the allocation plan whenever a request carries `N > 1`
nodes, and the rendered `COMPUTEMCP_SBATCH_ARGS` will include `--nodes=N`.
But the shipped helper **exits before `sbatch`** with the message
`multi-node not yet supported` when `N > 1`. Multi-node allocation is not
currently supported end-to-end. Use `--set nodes=1` (or `nodes = 1` in the
config) until support lands.

### 1.7 `build-location`

- `login` (default): the sandbox (Apptainer) or image (Docker) is built or
  pulled on the login node, then reused across allocations.
- `compute`: the build happens on the **first allocated compute node**. Use
  this when the architecture of the login/head nodes does not match the
  architecture of the compute partition (e.g. x86-64 login with an ARM
  compute partition) so a login-node-built image would not run on a
  compute node. Only meaningful for Apptainer: the Docker runtime pulls the
  image on the node where it starts, so `build-location` is orthogonal for
  Docker. `compute` **requires Slurm**: the build happens inside the
  allocation, and there is no allocation on a non-Slurm host.

### 1.8 Non-Slurm Docker hosts

The regular `[targets.X.bundle]` block covers hosts without a scheduler
(`myTargetHost`-style dev boxes). No `[targets.X.slurm.*]` block is needed;
the helper detects the absence of Slurm tools, skips the submission path,
builds or reuses the container on the route (login) node, starts it there, and
prints `ENDPOINT 127.0.0.1:<relay-port>` through the relay.
See [`../examples/non-slurm-docker-host.toml`](../examples/non-slurm-docker-host.toml).

---

## 2. Workflow B: existing or custom environment

### 2.1 When you use it

Pick this workflow when:

- The container (or tool) already exists on the remote host and you do not
  want the gateway to build it.
- The site's Slurm / LSF / SGE setup has custom naming, a site-specific
  wrapper, or a queue plugin that is simpler to run by hand.
- You want to control `authorized_keys` placement.

Then you write `provision_command` yourself. The gateway:

1. Runs `provision_command` on the **remote** machine over the authenticated
   route (login-node) connection — it is **not** a local subprocess and it is
   **not** a separate `ssh -T`.
2. Parses stdout for the **first** `host:port` token (an optional `ENDPOINT `
   prefix is accepted).
3. Forwards that endpoint back over the same route connection.

`provision_command` semantics:

- Must be idempotent (route loss auto-reconnects and re-runs it).
- Prints `host:port` (optionally with `ENDPOINT ` prefix) on stdout.
- Typically: (a) ensure a Slurm job exists for this target (reuse or submit),
  (b) ensure a loopback listener on the login node forwards to the container
  SSH port on the compute node, (c) print the endpoint.
- Site-specific scheduler differences stay in this script; the gateway never
  guesses scheduler syntax.

### 2.2 `client_key` and public-key placement

The route connection (gateway → login node) authenticates with the gateway
user's SSH key, which the alias in `~/.ssh/config` names. That is separate
from the key the **container** sshd accepts:

- **Bundle target.** `client_key` gives the gateway the private key for the
  container account, and the gateway runs `ssh-keygen -y` on it (or reads the
  `.pub` half if present) to derive the **public** key. The bundle receives
  it as `COMPUTEMCP_SSH_PUBLIC_KEY` and the helper installs it into the
  container's `authorized_keys`. Manual placement is **not** needed.
- **Manual target.** The public key for the account the gateway dials inside
  the container must already be in `authorized_keys` on the host.
  Typically:

  ```bash
  ssh-keygen -y -f /home/USER/.ssh/computemcp_container | \
    ssh <login-alias> 'cat >> ~/.ssh/authorized_keys'
  ```

  The private key stays on the gateway host; only the public key is placed.

### 2.3 Route `user` vs in-container `container_user`

Three accounts exist per target, and confusing any two is the most common
source of `502 Bad Gateway`:

| Name | Where it applies |
| --- | --- |
| `user` | The SSH login account used for the **route** connection (gateway → login node). Can be empty: in that case the `~/.ssh/config` alias decides. |
| `container_user` | The account the gateway dials **inside** the container. Default `ubuntu`. |
| (host user) | The OS user on the route host that actually runs the provision script. The gateway does not dial this user directly; the connection is already established through `user`. |

Docker **creates** the `container_user` account inside the image (the
entrypoint runs `useradd` if it does not exist). Apptainer **renames** the
existing `ubuntu` account to the target `container_user` while preserving its
`uid` / `gid` and rewriting the home to `/home/<container_user>`. Either way
the resolved name is the same source of truth the gateway exports as
`COMPUTEMCP_SSH_USER`, so the helper and the gateway dial disagree only when
the operator sets `container_user` to a name the container has no account
for. In that case every `exec` / file call fails with `502`.

### 2.4 `host_key_sha256` pinning

`host_key_sha256` pins the **container** host key (the sshd on `remote_port`,
usually 2222). `route_host_key_sha256` pins the **login-node** host key. Both
are optional; when unset the gateway falls back to the local `~/.ssh/known_hosts`.

To obtain the container fingerprint:

```bash
# On the host, via the ssh alias that reaches the container:
ssh <alias> 'cat /etc/ssh/ssh_host_ed25519_key.pub' | ssh-keygen -lf - | awk '{print $2}'
#    -> SHA256:xxxxxx...   (this is the value for host_key_sha256)

# Or, scanning the container port on the login node's own loopback:
ssh <alias> 'ssh-keyscan -t ed25519 -p 2222 127.0.0.1 2>/dev/null' | ssh-keygen -lf - | awk '{print $2}'
```

Restrict `host_key_algorithms` to the algorithm of the container's host key
(e.g. `["ssh-ed25519"]`). A recreate of the container changes the host key;
update the pin and `target-refresh`.

### 2.5 `provision_command` argv constraints

- It must be a list of strings, in `argv` form (the gateway quotes each
  element with `shlex.quote` before echoing into the remote shell). There is
  no shell interpolation; write the script to accept positional arguments and
  pass them here.
- The first positional argument is the target name. No other positional
  arguments are passed.
- The `COMPUTEMCP_*` variables in [section 3](#3-environment-transport-contract) (the environment transport contract) are **only** exported when at
  least one of `[node]`, `[allocation]`, `[slurm]`, `[container]`, `[bundle]`
  is present. If a manual target has **only** `provision_command` and none of
  those blocks, the command runs without any `COMPUTEMCP_*` exports.

### 2.6 `connect_command` for recovery - see [section 4.2](#42-connect_command-recovery)

A stopped container (not a lost route) is recovered with `connect_command`
rather than `provision_command`. The shipped reference is
[`scripts/ensure-container.sh`](../scripts/ensure-container.sh).

### 2.7 `[targets.X.container]` block

The container block describes the runtime to the provisioning helper (bundle
or manual). Keys:

| Key | Type | Notes |
| --- | --- | --- |
| `runtime` | `"apptainer"` or `"docker"` | Required when the block is present. |
| `storage-root` | string | Base for state, sandbox, and home directories. The default for `bundle.deploy-dir` derives from this. |
| `image` | string | Base image. `docker://<ref>` for Apptainer, plain reference for Docker. |
| `gpus` | string array | Subset of `nvidia`, `amd`, `intel`. Missing device nodes are reported and skipped. |
| `host-home` | string | Host directory carrying `.ssh/authorized_keys` that the container trusts. |
| `sandbox` | boolean | Informational flag; the helper reads the actual sandbox path. |
| `build-location` | `"login"` or `"compute"` | Where the sandbox is built (see [section 1.7](#17-build-location)). |

A `[container]` block alone is enough to build the `COMPUTEMCP_*` transport
contract even on a manual target (see [section 3](#3-environment-transport-contract)).

### 2.8 Manual-target `toml` example

The manual-target configuration has the same `user` / `client_key` /
`host_key_sha256` / `provision_command` / `connect_command` / `close_command`
shape as a bundle target; the only differences are (a) no `[bundle]` block and
(b) `provision_command` and `close_command` point at operator scripts the
operator already maintains.
See [`../examples/manual-connect-recovery.toml`](../examples/manual-connect-recovery.toml)
for a full working example.

---

## 3. Environment transport contract

The gateway exports the resolved plan and the container description as
`COMPUTEMCP_*` environment variables around both the bundle helper and any
manual `provision_command`, over the authenticated route connection.

The table below lists every variable the **gateway** emits. A few other
variables are consumed by the bundle helper but set **by the helper**
(`COMPUTEMCP_STATE_DIR`, `COMPUTEMCP_SANDBOX_DIR`, `COMPUTEMCP_CONTAINER_PORT`,
`COMPUTEMCP_SSH_WAIT_SECONDS`, `COMPUTEMCP_FORWARD_PORT`); they are not part of
the gateway → provisioner contract. An operator can nevertheless pre-set
`COMPUTEMCP_FORWARD_PORT` to pin the relay port.

### 3.1 Always (plan path)

| Variable | Value |
| --- | --- |
| `COMPUTEMCP_NODES` | Plan node count (concrete integer). |
| `COMPUTEMCP_CPUS_PER_NODE` | Plan value or empty. |
| `COMPUTEMCP_GPUS_PER_NODE` | Plan value or empty. |
| `COMPUTEMCP_MEMORY_PER_NODE_MIB` | Plan value or empty. |
| `COMPUTEMCP_EXCLUSIVE` | `"true"` / `"false"`. |
| `COMPUTEMCP_MODE` | Plan allocation mode name. |
| `COMPUTEMCP_SYSTEM` | Target name. |

### 3.2 Scheduler (only with `[node]`, `[allocation]`, or `[slurm]`)

| Variable | Value |
| --- | --- |
| `COMPUTEMCP_SBATCH_ARGS` | One rendered argv entry per line, **no** trailing newline. Empty string means an empty argv. |
| `COMPUTEMCP_SRUN_ARGS` | Same for the `srun` stage. |

Parsing rule for receivers: read the value as a string, split on `\n` with a
Bash `mapfile -t` (no `eval`, no unquoted expansion), iterate the resulting
array.

**Stage separation.** `sbatch` and `srun` are separate, independent stages:

- `SBATCH_ARGS` only ever appear on `sbatch`.
- `SRUN_ARGS` only ever appear on the `srun` command **inside the container**
  (typically used by the container's internal workload / relay steps).
- The batch script itself (the file passed to `sbatch`) sources a settings
  file and starts the container **directly** on the batch node, **not** inside
  an `srun` step, so a fakeroot Apptainer instance is not killed when an
  `srun`-scoped step ends.
- A mapping or manual option configured in the `sbatch` stage never leaks into
  the `srun` stage and vice versa.

### 3.3 Container (only with `[container]`)

| Variable | Value |
| --- | --- |
| `COMPUTEMCP_CONTAINER_RUNTIME` | `apptainer` or `docker`, from `container.runtime`. |
| `COMPUTEMCP_STORAGE_ROOT` | `container.storage-root`. |
| `COMPUTEMCP_IMAGE` | `container.image`. |
| `COMPUTEMCP_GPU_VENDORS` | Comma-joined `container.gpus`. |
| `COMPUTEMCP_HOST_HOME` | `container.host-home`. |
| `COMPUTEMCP_SANDBOX` | `"true"` only if a container block exists and `sandbox = true`; else `"false"`. |
| `COMPUTEMCP_BUILD_LOCATION` | `container.build-location` or literal `login`. |

### 3.4 Bundle-only (only with `[bundle]`)

| Variable | Value |
| --- | --- |
| `COMPUTEMCP_SSH_USER` | The account the container sshd must allow (see [section 2.3](#23-route-user-vs-in-container-container_user)). Emitted whenever a `[container]` or `[bundle]` block exists. |
| `COMPUTEMCP_SSH_PUBLIC_KEY` | The public key derived from `client_key`. Emitted only for a `[bundle]` target with a derivable key. Manual targets do not get it. |
| `COMPUTEMCP_PROVISION_ENV` | `provision-env` lines joined with a single newline. Empty tuple → empty string (helper no-ops). |

### 3.5 `sbatch` / `srun` stage separation in practice

Minimal Bash receiver pattern:

```bash
mapfile -t SBATCH_ARGS <<< "$COMPUTEMCP_SBATCH_ARGS"   # from shorter string
mapfile -t SRUN_ARGS   <<< "$COMPUTEMCP_SRUN_ARGS"

# GPU submission: pass only SBATCH_ARGS to sbatch.
sbatch --parsable "${SBATCH_ARGS[@]}" batch.sh

# The batch script launches the container directly on the node (it sources
# the settings file, then runs the runtime dispatcher "start").
# The relay / internal workload jobs, when launched via srun, take SRUN_ARGS.
```

The shipped bundle follows this exactly (see
`src/compute_mcp/bundles/computemcp-slurm/computemcp-provision.sh`
"SBATCH and SRUN argument transport" section of its own README).

---

## 4. `connect_command` / `close_command` recovery and release semantics

### 4.1 `close_command`

Mirrors `provision_command`: when the target acquires a Slurm job,
`close_command` is the argv that **releases it**. Runs **on the remote
machine** over the live route (login-node) connection.

Semantic rule (**mandatory**):

| Trigger | `close_command` runs |
| --- | --- |
| Explicit operator stop (`target-stop`, `client-stop`, console `stop`, `stop-all`) | **Yes** |
| Gateway shutdown | **Yes** |
| Refresh (`target-refresh`, `client-refresh`, console `refresh`: releases the old allocation before the new `provision_command` starts) | **Yes** |
| Config reload that **removes** a target | **No** |
| Config reload that **modifies** an existing target | **No** (the target is marked `needs_refresh`; release happens on the next refresh / stop) |

Recovery rule: if a refresh is part of reconnect after route loss, the
`close_command` **before** the re-provision is only run if the previous
allocation is still tracked. A half-dead target with no tracked job just
calls `provision_command` again (the job is no longer resolvable by name and
an orphaned Slurm allocation typically times out or is picked up by the
site's cleanup job).

Behavior notes:

- Timeout defaults to 120 s (`close_command_timeout`).
- Exit status is advisory: a non-zero exit is logged as a warning and the
  teardown path continues.
- If `[bundle]` is present, the gateway **does not** run a separate
  `close_command`; the bundle helper's own `close` action does the release.
  The operator's `close_command` only fires when the operator has explicitly
  opted out of bundle management by deleting the `[bundle]` block and
  re-adding `provision_command`/`close_command`.

### 4.2 `connect_command` recovery

A healthy tunnel with a **stopped** development container is different from
a lost route: `provision_command` re-discovers an endpoint but does not
start the container; `connect_command` **does**.

Configuration keys:

| Key | Type | Notes |
| --- | --- | --- |
| `connect_command` | argv | Same argv form as `provision_command`. |
| `connect_command_timeout` | number | Default 120.0. |
| `connect_command_mode` | string | `"on_failure"` (default) or `"always"`. |

Mode semantics:

- `"on_failure"` (default): the command runs **only after** the container
  connection has failed, then the gateway retries the connection once. A
  healthy container never pays the cost.
- `"always"`: the command runs **before every** connection attempt. The
  reference script `scripts/ensure-container.sh` is a **no-op** when the
  container is already running, so `"always"` mode is safe with it. A custom
  script **must** be an idempotent no-op under `"always"`.

The gateway always re-probes the container after `connect_command` and only
reports success if the container is really reachable; a non-zero exit is logged
as a warning and the gateway still retries once.

### 4.3 `scripts/ensure-container.sh`

The reference recovery script:

```bash
connect_command = ["/home/USER/.config/computeMCP-gateway/ensure-container.sh"]
connect_command_timeout = 120.0
connect_command_mode = "on_failure"
```

It inspects `COMPUTEMCP_CONTAINER_NAME` (default `computeMCP-container`),
starts it if the runtime is not running, and waits for it to reach
`running`. Supports both Docker and Podman. Full source:
[`scripts/ensure-container.sh`](../scripts/ensure-container.sh).

To install it on a remote host today:

```bash
scp scripts/ensure-container.sh <remote-host>:~/.config/computeMCP-gateway/
ssh  <remote-host> chmod +x  ~/.config/computeMCP-gateway/ensure-container.sh
```

---

## 5. Examples index

The full six-column table is at
[`examples/README.md`](../examples/README.md). Inline summary:

| Example | Purpose | Prerequisites | Required customization | Execution location | Related configuration |
| --- | --- | --- | --- | --- | --- |
| [`examples/dev-container-nvidia.sh`](../examples/dev-container-nvidia.sh) | Docker + NVIDIA dev-container creation (manual, workflow B) | Docker on remote host, NVIDIA kernel driver on host, keypair `~/.ssh/computemcp_container` on gateway host | `SSH_PUBLIC_KEY`, `HOST_HOME`, `CONTAINER_NAME` | Remote host | Manual target, `provision_command`, `connect_command`; or pre-existing container for a bundle target |
| [`examples/dev-container-amd.sh`](../examples/dev-container-amd.sh) | Docker + AMD/ROCm dev-container creation (manual, workflow B) | Docker on remote host, `amdgpu` kernel driver on host, keypair `~/.ssh/computemcp_container` on gateway host | `SSH_PUBLIC_KEY`, `HOST_HOME`, `CONTAINER_NAME` | Remote host | Manual target, `provision_command`, `connect_command`; or pre-existing container for a bundle target |
| [`examples/ensure-container.sh`](../examples/ensure-container.sh) | `connect_command` recovery (no-op when running) | Docker or Podman on remote host, container already created | `COMPUTEMCP_CONTAINER_NAME` (optional) | Remote host | `connect_command`, `connect_command_mode`, `connect_command_timeout` |
| [`examples/slurm-gpu-apptainer.toml`](../examples/slurm-gpu-apptainer.toml) | Slurm + GPU + Apptainer bundle target (workflow A) | Slurm cluster, Apptainer on login and compute nodes, login/compute shared storage | `ssh_targets`, `client_key`, `host_key_sha256`, `storage-root`, `partition` | Gateway config parse; helper runs on login/compute nodes | `[targets.X.bundle]`, `[targets.X.container]`, `[targets.X.slurm.*]` |
| [`examples/non-slurm-docker-host.toml`](../examples/non-slurm-docker-host.toml) | Non-Slurm Docker-host bundle target (workflow A) | Docker on the route host; no Slurm needed | `ssh_targets`, `client_key`, `host_key_sha256`, `storage-root` | Gateway config parse; helper runs on the route node | `[targets.X.bundle]`, `[targets.X.container]` |
| [`examples/manual-connect-recovery.toml`](../examples/manual-connect-recovery.toml) | Manual target: `provision_command` + `connect_command` + `close_command` (workflow B) | Operator-maintained scripts on the remote host, Docker container exists | `provision_command` / `close_command` / `connect_command` absolute paths | Gateway config parse; scripts run on the remote host | `provision_command`, `close_command`, `connect_command` (no `[bundle]` block) |

`examples/README.md` holds the index page with usage and prerequisites. It is
not itself an executable example, so it is not listed in the table above.
