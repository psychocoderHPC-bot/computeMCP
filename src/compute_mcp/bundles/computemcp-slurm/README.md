# computeMCP container provisioning bundle

One config-driven bundle replaces the per-system scripts.  The gateway renders a
resource plan and two argument lists, exports them as `COMPUTEMCP_*` variables,
and runs `computemcp-provision.sh` on the route (login) node.  The helper builds
the container and, when the target has a scheduler, submits one single-node
allocation, starts a relay, and prints `ENDPOINT host:port` once the forwarded
SSH endpoint answers.  On a host without a scheduler it skips `sbatch`/`srun`
and starts the container directly on the route node.

## Files

| File | Role |
| --- | --- |
| `computemcp-provision.sh` | Login-node entry point: `submit`, `observe`, `attach`, `cancel`, `detach`, `teardown`, legacy `provision`, `stop`, `close`, `shell`, `status` |
| `computemcp-container.sh` | Runtime dispatcher for `apptainer` and `docker` |
| `computemcp-job.sh` | Batch script that runs on the compute node and keeps the allocation alive |
| `computemcp-relay.py` | Loopback relay that carries SSH over `srun` steps |
| `computemcp-mux.py` | Login-node multiplexer for the single-channel proxy; dials the relay registered for a `job_id` |

The gateway ships this bundle as package data and can deploy it for you: a
`[targets.X.bundle]` block in the gateway config makes the gateway upload the
files over the route connection into `deploy-dir`, then run
`computemcp-provision.sh` from there.  The upload happens only when the remote
content marker differs from the gateway's bundle hash, so a repeat connect moves
no bytes and a gateway upgrade re-deploys the new revision on the next connect.
Set `auto-deploy = false` to pin the copy already on the login node.

`deploy-dir` is optional and defaults to `<container.storage-root>/bundle`.  It
must be on storage visible to the login node and to the compute nodes: the batch
step runs `computemcp-job.sh` and `computemcp-container.sh` there, so a per-node
`/tmp` breaks multi-node and most batch setups.  Place the five code files side
by side only when you manage deployment by hand (the manual
`provision_command`/`close_command` path).  No site name, account, partition,
image, or path is hardcoded either way.

## Login-node requirements

Slurm client tools (`sbatch`, `squeue`, `scancel`, `srun`), Bash, `flock`,
`python3` (3.8 or newer), and the selected runtime (`apptainer` or `docker`).
The Apptainer configure step uses `python3` on the login node, so Python 3
remains a login node requirement even though the argument bridge itself is pure
Bash.  Compute nodes need the same runtime and `python3`.

### Apptainer prerequisites

`computemcp-container.sh` probes Apptainer before `build`/`configure`/`start`
and fails with an actionable message instead of letting a later step emit an
opaque error:

* **Apptainer 1.x or newer.**  Legacy Singularity (major version `0`) is
  rejected.  An unparseable `apptainer --version` output is only a warning,
  because a site wrapper may hide it.
* **Working `--fakeroot`.**  The fakeroot sandbox needs either a setuid-root
  Apptainer install or unprivileged user namespaces
  (`unshare --user --map-root-user true`).  Running as root also satisfies it.
  When neither is available the helper names both remedies.
* **A writable sandbox account.**  Some base images ship no `ubuntu` account.
  `%post` creates one when the shadow tools are present, and `configure` creates
  a non-root account with a free UID/GID when the image still has none.

For immutable `clean` root mode, `COMPUTEMCP_IMAGE_DIGEST` pins the expected
base-image SHA-256.  A mismatch aborts the run; without a pin, the first digest
is recorded and a later change is reported as a warning.

## Gateway configuration

```toml
[targets.example]
ssh_targets = ["example-login"]
user = "agent"
provision_timeout = 960.0

# Let the gateway deploy and run this bundle; no manual copy or command.
[targets.example.bundle]
source = "computemcp-container"
# deploy-dir = "/scratch/agent/computemcp/bundle"   # default <storage-root>/bundle
# auto-deploy = true                                # false pins the deployed copy

[targets.example.node]
cpus = 24
gpus = 4
memory = "378000M"

[targets.example.allocation]
default-gpus = 1
single-node = "gpu-proportional"

[targets.example.slurm.sbatch]
partition = "gpu"
time = "02:00:00"

[targets.example.slurm.srun]
ntasks-per-node = 1
cpu-bind = "none"

[targets.example.container]
runtime = "apptainer"          # or "docker"
storage-root = "/scratch/agent/computemcp"
image = "docker://ubuntu:24.04"
gpus = ["nvidia"]              # subset of nvidia, amd, intel
host-home = "/scratch/agent/computemcp/home"
sandbox = true
```

`COMPUTEMCP_SSH_PUBLIC_KEY` is derived by the gateway from the target's
`client_key` (the `.pub` half, or `ssh-keygen -y`); the private key stays on the
gateway host.  An explicit `provision_command` target keeps the older behavior:
the operator places the key, or the helper reuses an existing
`authorized_keys`.

`provision_command` and `close_command` are only needed for manual deployment; a
`[bundle]` block replaces them, and the gateway derives the command from
`deploy-dir`.  Replace the placeholder target, partition, and paths with site
values.

### Gateway keys to behavior

| `[targets.X.bundle]` key | Behavior |
| --- | --- |
| `source` | Which shipped bundle to deploy. `computemcp-container` is canonical; `computemcp-slurm` is the legacy alias for the same bundle |
| `deploy-dir` | Remote directory; defaults to `<storage-root>/bundle`. Overwritten only when the content marker differs |
| `auto-deploy` | `true` (default) re-deploys on a hash change; `false` pins the deployed copy |

| `[targets.X.container]` key | Behavior |
| --- | --- |
| `runtime` | Selects the Apptainer or Docker code path |
| `storage-root` | Base for the system state, sandbox, and home directories |
| `image` | Base image; `docker://` for Apptainer, plain reference for Docker |
| `gpus` | GPU vendors to expose; missing devices are reported and skipped |
| `host-home` | Directory whose `.ssh/authorized_keys` the container trusts |
| `sandbox` | Informational request flag; the helper reads the actual path |
| `build-location` | `login` (default) builds the sandbox on the login/head node; `compute` builds it on the first allocated compute node (use on an architecture-mismatched partition such as an ARM partition with x86-64 login nodes). `compute` requires Slurm, and it is meaningful for the Apptainer runtime (an architecture-mismatched partition); with the Docker runtime it is orthogonal because the image is pulled and started where it runs |

The gateway exports the resolved values as `COMPUTEMCP_SYSTEM`,
`COMPUTEMCP_STORAGE_ROOT`, `COMPUTEMCP_IMAGE`, `COMPUTEMCP_GPU_VENDORS`,
`COMPUTEMCP_HOST_HOME`, `COMPUTEMCP_CONTAINER_RUNTIME`,
`COMPUTEMCP_BUILD_LOCATION`, and `COMPUTEMCP_SANDBOX`.  It also exports
`COMPUTEMCP_NODES`, `COMPUTEMCP_CPUS_PER_NODE`, `COMPUTEMCP_GPUS_PER_NODE`,
`COMPUTEMCP_MEMORY_PER_NODE_MIB`, `COMPUTEMCP_EXCLUSIVE`, `COMPUTEMCP_MODE`,
`COMPUTEMCP_SBATCH_ARGS`, and `COMPUTEMCP_SRUN_ARGS`.

`COMPUTEMCP_SBATCH_ARGS` and `COMPUTEMCP_SRUN_ARGS` hold one complete argument
per line, with no trailing newline.  An empty value means no arguments.  The
helper parses both with `mapfile -t` into Bash arrays and never uses `eval` or
unquoted expansion.  `sbatch` and `srun` stay separate stages: the helper never
copies submission options into the job step.

## Storage layout

By default the helper uses
`$COMPUTEMCP_STORAGE_ROOT/$COMPUTEMCP_SYSTEM`, and the gateway default root is
`$HOME/.local/share/computemcp`.  Under the system directory:

| Path | Contents |
| --- | --- |
| `sandbox/` | Writable Apptainer sandbox for fakeroot jobs (Apptainer runtime) |
| `home/` | Default home base (see "Homes" below) |
| `state/` | Target-level state root |
| `state/jobs/<job_id>/` | Per-job record: Slurm id, cluster, mode, root mode, home, instance name, readiness node, container port, per-job settings, locks, logs and relay files |
| `state/jobs/<job_id>/runtime/` | Per-job writable runtime directory bound into the instance (port file, Dropbear PID, `/run`, `/tmp`) |

Slurm jobs always use `state/jobs/<job_id>/`; the legacy single-job id of an
older gateway is `default`.  Direct (non-Slurm) Docker targets keep the
target-level `state/` directory, so their shared-container workflow is
unchanged.  There is no global `state/jobid` or `state/relay.ready`: two jobs
never share a numeric id, a relay port, a PID file or a readiness marker.

## Homes

The host directory mounted as the container home comes from
`COMPUTEMCP_HOME_BASE` (default `<system-dir>/home`):

| Job | Host directory |
| --- | --- |
| Shared job (`COMPUTEMCP_JOB_KIND=shared`, default) | `<home-base>/shared/` |
| Private job (`COMPUTEMCP_JOB_KIND=private`) of client X | `<home-base>/agents/X/` |
| Private job with `COMPUTEMCP_CLEAN_HOME=/benchmarks/run-42` | `<home-base>/agents/X/benchmarks/run-42/` |

`COMPUTEMCP_CLIENT_ID` identifies the authenticated client for a private job.
The helper strips a leading `/` from `COMPUTEMCP_CLEAN_HOME`, so
`benchmarks/run-42` and `/benchmarks/run-42` name the same directory.  It
rejects an empty path, the home root, `..` traversal, client ids containing a
path separator, and symlinks anywhere below the resolved home base.  Missing
directories are created; existing content is preserved.  Shared jobs always use
the shared home, so `COMPUTEMCP_CLEAN_HOME` on a shared job is rejected.

## Root modes

`COMPUTEMCP_ROOT_MODE` selects `fakeroot` (default) or `clean`.

* **fakeroot** keeps one persistent writable Apptainer sandbox per target.
  Running fakeroot jobs on one node see packages installed by another, and
  changes survive job termination.
* **clean** starts an operator-prepared immutable SIF named by
  `COMPUTEMCP_CLEAN_IMAGE`.  The helper records its SHA-256 in
  `state/jobs/<job_id>/image.digest` and prints the digest; the base is never
  modified.  Each job gets ephemeral writable `/run` and `/tmp` plus its own
  runtime directory.  A prepared clean base must install the gateway SSH
  account/key bootstrap and a start script that reads
  `/computemcp-runtime/container.port`.  Clean mode requires Apptainer.

## Stages

* `submit` builds/configures the container when needed, calls
  `sbatch --parsable`, persists the numeric id and cluster immediately, and
  returns.  It never waits in the queue and never starts a relay.  Before
  `sbatch` it persists a correlation token (from `COMPUTEMCP_CORRELATION_TOKEN`
  or generated) and passes it as `--comment=<token>`, so a crash between
  acceptance and id persistence is recoverable.
* `observe` reads `squeue` and the job's readiness file and prints
  `STATE pending|starting|ready|terminal` without blocking.  It falls back to
  `sacct` when `squeue` no longer shows the id; a purged or unknown id is
  `terminal` so a quota slot cannot stick.
* `attach` polls until the job publishes readiness, starts (or reuses) this
  job's login-node relay, verifies the endpoint and prints
  `ENDPOINT 127.0.0.1:<port>`.  If the persisted id was lost, `observe`,
  `attach` and `cancel` recover it from the token via `squeue`/`sacct`
  comments before doing anything else.
* `cancel` runs an explicit `scancel` for the tracked job and stops its relay.
* `detach` / `teardown` stop only the relay.  They never call `scancel`; the
  Slurm job and its durable files keep running.
* `provision` is the legacy synchronous `submit` + `attach` used by the current
  gateway.  `stop`/`close` stop a direct container and detach (never cancel) a
  Slurm job.

The job id can be passed as the second argument or in `COMPUTEMCP_JOB_ID`; it
is an opaque computeMCP identity, not the numeric Slurm id.  Each job gets a
distinct instance name (`computemcp-<uid>-<system>-<job_id>` unless
`COMPUTEMCP_CONTAINER_NAME` is pinned) and, when
`COMPUTEMCP_CONTAINER_PORT` is `0` or unset, a free dropbear port chosen on the
compute node and published in its own state directory.  Two jobs on one compute
node therefore get distinct instance names, ports and runtime files.

Set `storage-root` on a filesystem reachable from login and compute nodes.
Avoid a small home quota when building images.

## Auto-build and SSH key

The login-node helper checks for the sandbox directory (Apptainer) or the
derived image `computemcp-<system>:latest` (Docker).  When it is absent, the
helper builds it on the login node and then configures the SSH public key.  The
build never runs inside the Slurm batch job.  When the container exists, the
helper only configures it.

The SSH public key comes from `COMPUTEMCP_SSH_PUBLIC_KEY`, then
`COMPUTEMCP_SSH_PUBLIC_KEY_FILE`, then an existing
`$COMPUTEMCP_HOST_HOME/.ssh/authorized_keys`.  A `[bundle]` target gets
`COMPUTEMCP_SSH_PUBLIC_KEY` derived from the gateway `client_key`, so no manual
placement is needed; an explicit `provision_command` target relies on the other
two.  Public-key-only access, per-user UID checks, and symlink-safe config
writes are preserved from the reference scripts.

## SBATCH and SRUN argument transport

The helper parses `COMPUTEMCP_SBATCH_ARGS` into `SBATCH_ARGS` and appends only
protocol options it owns (`--parsable`, `--job-name`, `--comment=<correlation
token>`, and output/error when the deck has none).  It submits with:

```bash
sbatch "${SBATCH_ARGS[@]}" "${HELPER_ARGS[@]}" "$JOB_SCRIPT" "$SETTINGS"
```

When the target's `slurm.sbatch` stage configures an account, the gateway emits
`--account=<account>` into `COMPUTEMCP_SBATCH_ARGS`; an unset or empty account
adds nothing.  The helper's `COMPUTEMCP_ACCOUNT` fallback below is unchanged.

The settings file carries the container configuration and `COMPUTEMCP_SRUN_ARGS`
for the relay's `--connect` steps, and is passed to the batch script as a
positional argument.  Slurm delivers positional arguments verbatim even under
`--export=NONE`, so the helper does not touch the user's `--export` policy.  An
`--export`-based bridge was rejected because it can silently override or be
overridden by that policy, which the design forbids.  `computemcp-job.sh`
sources it and starts the container DIRECTLY on the batch node (not via `srun`),
so a fakeroot instance is not torn down with a transient step.

## GPU vendors

`gpus` is a comma list drawn from `nvidia`, `amd`, `intel`.  The Apptainer path
adds `--nv` for NVIDIA and binds `/dev/kfd` (AMD) and `/dev/dri` (AMD/Intel).
The Docker path adds `--gpus all` for NVIDIA, `/dev/kfd` plus
`seccomp=unconfined` for AMD, and `/dev/dri` for AMD/Intel.  The helper reports
and skips a vendor whose device node is missing.  Devices are shared; these
flags do not reserve GPUs.

## Manual and legacy use

The helper works with an empty gateway environment.  When
`COMPUTEMCP_SBATCH_ARGS` is empty it fills `SBATCH_ARGS` from the manual
override variables of the reference scripts: `COMPUTEMCP_PARTITION`,
`COMPUTEMCP_ACCOUNT`, `COMPUTEMCP_CPUS`, `COMPUTEMCP_GPUS`, `COMPUTEMCP_MEMORY`,
and `COMPUTEMCP_TIME_LIMIT`.  The fallback never reads the plan variables, so
the calculated plan is not silently submitted.  When both are empty, Slurm
defaults apply.

Useful variables when running by hand: `COMPUTEMCP_CONTAINER_PORT` (container
SSH port, default 2222), `COMPUTEMCP_FORWARD_PORT` (optional explicit
login-node relay port; by default the port is chosen dynamically, see below),
`COMPUTEMCP_WAIT_SECONDS` (job wait, default 900), and
`COMPUTEMCP_SSH_WAIT_SECONDS` (banner wait, default 120).

## Environment bootstrap

`computemcp-job.sh` sources the per-job settings file first, so the container
start survives `--export=NONE` or a custom export policy.  The settings file
carries the resolved absolute paths and the site hook
`COMPUTEMCP_PROVISION_ENV` (one `source`/`module load` line per newline).  The
helper applies the same lines on the login node before the first Python/runtime
operation and the batch script applies them again on the compute node, where
the login-node module environment does not propagate.  The batch script uses no
PTY, and the relay's `srun --connect` steps use no PTY either.

## Relay port allocation

The helper runs on the login/route node and is the authority for what is free
there, so it lets each relay bind an ephemeral loopback port by default (binding
port `0` and reading the concrete port back from its own
`state/jobs/<job_id>/relay.ready`).  Relay starts pass the job id and an
explicit endpoint file, and publish the ready record atomically, so several
relays can run concurrently on one login node and concurrent starts do not
clobber a shared registry.  Two bundle targets that share one login node
therefore never collide, which a fixed default port would.  The resolved
concrete port is persisted to `state/jobs/<job_id>/relay.port`; a reconnect
against the same job reuses it as long as the tracked relay is still running,
so the endpoint does not move and the relay is not restarted.  `detach`,
`teardown` and `stop`/`close` remove the relay files, so a fresh attach after a
detach picks a new port.  Setting `COMPUTEMCP_FORWARD_PORT` to an integer
`1..65535` overrides this dynamic choice and is used verbatim; any other value
fails with exit 2.

## Lifecycle

`provision` reuses a tracked `PENDING`, `RUNNING`, or `CONFIGURING` job and
otherwise submits one.  `cancel` is the only operation that runs `scancel`;
`detach`, `teardown` and Slurm `stop`/`close` leave the allocation alive.
Container homes and immutable base images are preserved by every teardown.  On
a timeout the job stays tracked so the next `attach` can reuse it.  When a run
fails after submission, the helper prints the job ID and cluster so a
half-started allocation is never silent.  `status` reports the job id, numeric
Slurm id, state, ready node and relay endpoint.  Multi-node requests fail with
`multi-node not yet supported`.

## Connect-time overrides

The gateway accepts `--set` overrides before it renders the argument lists, for
example `--set gpus-per-node=2` or `--set nodes=2`.  Overrides feed the mappings
and never the other way around.  A `nodes` value above one fails in the helper
until multi-node support lands.
