# Provisioning examples

Runnable example scripts and TOML configurations that pair with
[`docs/provisioning.md`](../docs/provisioning.md).

Every example follows the same four-axis convention:

- **Who runs it** (gateway-side parse vs. remote-host script).
- **Which workflow it belongs to** (A: bundle, or B: manual).
- **Which GPU / scheduler variant is covered** (NVIDIA, AMD,
  scheduler-less Docker, Slurm + Apptainer, Slurm + Docker, CPU-only).
- **Whether it exercises the gateway transport contract**
  (`COMPUTEMCP_*` variables, `sbatch` / `srun` stages).

## Shell scripts (remote-host)

These run **on the remote machine** (over the route connection). They are
referenced from a gateway config by absolute path.

| Script | Purpose | Prerequisites | Customize to | Execution location |
| --- | --- | --- | --- | --- |
| [`dev-container-nvidia.sh`](./dev-container-nvidia.sh) | Creates a persistent, GPU-aware Docker dev-container (NVIDIA flavor) | Docker, NVIDIA kernel driver on host, gateway-host keypair, shared `HOST_HOME` bind | `SSH_PUBLIC_KEY`, `HOST_HOME`, `CONTAINER_NAME` | Remote host |
| [`dev-container-amd.sh`](./dev-container-amd.sh) | Same, but with AMD/ROCm device flags | Docker, `amdgpu` kernel driver on host, gateway-host keypair, shared `HOST_HOME` bind | same as above | Remote host |
| [`ensure-container.sh`](./ensure-container.sh) | `connect_command` recovery: start the dev-container if it is stopped, no-op if it is running (both Docker and Podman) | Docker or Podman, container already created | `COMPUTEMCP_CONTAINER_NAME` (optional) | Remote host |

The scripts mirror the NVIDIA / AMD recipes shipped in
[`README.md "Create the remote development container"`](../README.md#create-the-remote-development-container).
They use the exact same idempotent entrypoint (the `bash -euc` Cmd that
re-runs on every container start), the same loopback-only port binding, and
the same key-only sshd hardening.

## TOML target examples (gateway config)

These are **fragments** of `~/.config/computeMCP-gateway/config.toml`.
Copy the `[targets.X]` / nested-table block into your own config file.

Each example is TOML-valid and parseable by `python3 -c
"import tomllib; tomllib.load(open(...))"`. The file is valid TOML **on its
own** because it declares only a `[targets.X]` tree, which is a legal
stand-alone slice of the full config (the loader requires `[clients.X]` and,
in practice, a `[server]` — those are omitted here **by design** so the
example can be pasted next to your existing file without producing
duplicate-sections).

| Example file | Purpose | Prerequisites | Customize to | Execution location |
| --- | --- | --- | --- | --- |
| [`slurm-gpu-apptainer.toml`](./slurm-gpu-apptainer.toml) | Slurm + GPU + Apptainer bundle target (workflow A, canonical HPC) | Slurm on the cluster, Apptainer on login+compute, shared storage | `ssh_targets`, `client_key`, `host_key_sha256`, `storage-root`, `partition` | Parsed by the gateway at `reload`; helper runs on remote |
| [`non-slurm-docker-host.toml`](./non-slurm-docker-host.toml) | Non-Slurm Docker-host bundle target (workflow A, single dev box) | Docker on the route host; no scheduler needed | `ssh_targets`, `client_key`, `host_key_sha256`, `storage-root` | parsed above |
| [`manual-connect-recovery.toml`](./manual-connect-recovery.toml) | Manual target: `provision_command` + `connect_command` + `close_command` (workflow B, no `[bundle]` block) | Operator-maintained scripts on the remote, container already exists | The three command argvs (absolute paths) | parsed above |

## Quick start for each example

**NVIDIA dev-container (workflow B, or pre-existing container for A):**

Run from the `examples/` directory (or pass the full path to `dev-container-nvidia.sh`):

```bash
# on the remote host, BEFORE the first gateway connect:
bash dev-container-nvidia.sh \
  --name computeMCP-container \
  --home-dir /home/USER/workspace/computemcp-container \
  --public-key "$(ssh-keygen -y -f /path/to/gateway/computemcp_container)"

# read the new fingerprint and set it as host_key_sha256 in your config:
docker exec computeMCP-container ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub
```

**AMD dev-container:**

```bash
bash dev-container-amd.sh \
  --name computeMCP-container \
  --home-dir /home/USER/workspace/computemcp-container \
  --public-key "$(ssh-keygen -y -f /path/to/gateway/computemcp_container)"
```

**`connect_command` recovery:**

```bash
scp ensure-container.sh <remote-host>:~/.config/computeMCP-gateway/
ssh <remote-host> chmod +x ~/.config/computeMCP-gateway/ensure-container.sh

# gateway config (examples/manual-connect-recovery.toml, or the relevant
# sub-block in any target file):
#   connect_command = ["/home/USER/.config/computeMCP-gateway/ensure-container.sh"]
#   connect_command_mode = "on_failure"   # or "always"
```

**Workflow A targets (bundle):**

Copy the `[targets.X]` block from the relevant TOML file into your running
config, then run `computeMCP-gatewayctl --config ... reload` (or restart the
daemon under systemd). No `docker run`, no `apptainer build`, no manual
deploy: the gateway deploys the bundle over the route connection and runs it.

## Gateway-generated vs. user-maintained vs. shipped files

| File type | Where | Ownership |
| --- | --- | --- |
| Gateway-generated (deployed over the route) | `<bundle.deploy-dir>/{computemcp-provision.sh, computemcp-container.sh, computemcp-job.sh, computemcp-relay.py}` on the remote | Gateway process (both flows re-use the same payload; re-deployed when content marker changes) |
| User-maintained gateway config | `~/.config/computeMCP-gateway/config.toml` (and `include`d per-system files) | Human operator |
| Shipped provisioning scripts (in this repo) | `examples/` (this directory), `scripts/ensure-container.sh` | Human operator (may be modified to match the site) |
| Optional custom recipes | anywhere on the remote; conventional `~/.config/computeMCP-gateway/*.sh` | Human operator |

None of the files in this directory contain tokens, private keys, or
library-specific site values. Placeholders are flagged `REPLACE_WITH_...`,
`USER`, or `<remote-host>`; substitute them before running.
