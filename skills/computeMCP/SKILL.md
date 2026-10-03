---
name: computeMCP
description: Use the Terok `compute` MCP to work on remote development/compute containers. Covers discovering available systems, running commands, persistent parallel PTY sessions, and transferring files. Trigger on remote compute, remote build/test/debug, run on another machine, GPU job, parallel remote sessions, copy files to/from a remote system, computeMCP_targets, computeMCP_exec, computeMCP_session_*, computeMCP_file_*.
---

# computeMCP

A single MCP named **`compute`** gives access to one or more remote
development containers. All operations run **inside the remote development
container**, never on the gateway or compute host, and never on this container
unless a tool says so. You hold no SSH credentials; the gateway does the SSH.

The connection model is:

```
you (this container) --> compute MCP --> gateway --> remote dev container
```

## Before you act: load this skill

Read this entire skill before the first `computeMCP_*` call. Loading it is
mandatory for every compute MCP action: discovery, exec, sessions, file
transfer, and remote-agent delegation. Do not call `computeMCP_targets()`,
open a session, transfer a file, or delegate to a remote agent before you
have read this document. Skipping the load has already caused missed rules,
such as the non-TTY stdin warning in section 2.

## 0. Tool names and client namespacing

This document names tools as the MCP server defines them (`computeMCP_targets`,
`computeMCP_exec`, `computeMCP_file_upload`, `computeMCP_session_*`, …).

Some MCP clients name-space every tool with the **server name**. In the Terok
container this server is registered as `compute` (see the MCP entry in
`opencode.json`), so such a client exposes a tool as `<server>_<tool>` — for
example, under **opencode**:

| Server tool (this doc) | opencode-visible tool |
| --- | --- |
| `computeMCP_exec` | `compute_computeMCP_exec` |
| `computeMCP_targets` | `compute_computeMCP_targets` |
| `computeMCP_file_upload` | `compute_computeMCP_file_upload` |
| `computeMCP_session_read` | `compute_computeMCP_session_read` |

The leading `compute_` is only the client's server-name prefix; the tool itself
is called `computeMCP_*`. Call whatever name the client exposes. Clients that
do not name-space (or a direct MCP call) use the bare `computeMCP_*` names
above.

## 1. Always discover first

Call `computeMCP_targets()` before anything else. It lists the systems this task
is allowed to use, with their state. Use **only** target names it returns —
targets are ACL-scoped and any other name is rejected.

```
computeMCP_targets()
```

Then, optionally, inspect one:

```
computeMCP_status(target)      # state, active route, clients, sharing, node_info, agent, last_error
```

Each target also carries a **`sharing`** field, which matters for benchmarks:

- `"exclusive"` — the system is dedicated to this task (e.g. a Slurm
  allocation). Timings are meaningful.
- `"shared"` — other users/jobs may run concurrently. Benchmark results can be
  noisy; measure repeatedly and report the variance.
- `"unknown"` — the operator did not declare it. Treat as potentially shared.

Before running or reporting a benchmark, check `sharing` and say which it was.

Each target also carries a **`node_info`** field: an optional list of free-form
strings the operator wrote about the system, such as hardware, architecture, or
accelerators. For example:

```
["GPU nvidia", "x86 CPU", "AMD GPU"]
```

There is no fixed schema and no limited vocabulary — the operator can put any
human-readable text there. `computeMCP_targets()` and
`computeMCP_status(target)` return it per target.

An empty list means the operator provided no hints. You get no extra information
and must determine the system yourself (for example, inspect `/proc/cpuinfo`,
`nvidia-smi`, or `lscpu`) or ask the user.

Treat the hints as a starting hypothesis, understand them with your own
reasoning, and verify them against the actual system. They are guidance, never a
hard constraint.

### Remote AI agents (`agent`)

Each target may carry an **`agent`** field: an optional ordered list of remote
AI agents that the operator configured for that system, such as a different
model with different strengths. Each entry is a table with exactly two keys,
`agent` and `model`, both non-empty strings; spaces are allowed in both. For
example:

```toml
agent = [{ agent = "opencode", model = "GWen 3.5" }, { agent = "codex", model = "Sole" }]
```

`computeMCP_targets()` and `computeMCP_status(target)` return it as a list of
objects:

```
[{"agent": "opencode", "model": "GWen 3.5"}, {"agent": "codex", "model": "Sole"}]
```

An empty list means the operator configured no remote agent for the target. The
list order is the **priority order**: try the entries in order and fall back to
the **first working one**. The field is unrelated to the SSH `user = "agent"`
account name.

**Delegation workflow.** You can delegate a bounded task — for example running a
test matrix or drafting an implementation — to a remote agent on the target.
Start probes with an agent you can invoke there (for example `opencode` or
`codex` in a session), move to the next entry if it is unavailable, and keep the
work scoped and reviewable.

**Mandatory local review.** Remote agents do **not** have the Terok skills,
rules, or context. Treat their output as an untrusted draft: bring every result
back into this container and review it locally under the Terok rules before
using or reporting it. Never let a remote agent's claims stand as evidence.

If `computeMCP_targets` does not return a system you expect, or a target shows
`state != "connected"` with an error, report the gateway status. If the MCP
cannot reach the gateway at all (connection refused/timeout while
`COMPUTEMCP_GATEWAY` is set), the Terok Shield is likely blocking the
destination — the project `shield.allow`/`override` must permit the gateway host
(see the project's `project.toml`). **Do not try to bypass the gateway** (no
direct `ssh`, no host access).

If the `compute` MCP is not configured at all (no tools available, or
`COMPUTEMCP_GATEWAY`/`COMPUTEMCP_TOKEN` unset), you can request access
with the `computeMCP-handshake` tool **inside this container**:

```
computeMCP-handshake <project-id> --port <gateway-port> [--system hal,fwk394]
```

It queues a request; a human must approve it on the gateway console
(`approve <request-id>`). Tell the human the request id and that it is waiting.
On approval the token is written to `~/.bashrc`, but the running agent will not
see it until restarted (tmux keeps its old environment) — the command prints an
`environment` block to paste into the MCP config. Never attempt to bypass the
gateway while waiting.

## 2. Run a command

For short, non-interactive commands use `computeMCP_exec`. It returns
`exit_status`, `stdout`, `stderr`.

```
computeMCP_exec(target="hal", command="nvidia-smi")
computeMCP_exec(target="hal", command="cmake --build build -j", cwd="/work/picongpu")
```

- `cwd` is a directory **inside the remote container** (the shell runs there).
- `env` sets environment variables for the command (e.g. `OMP_NUM_THREADS`,
  `CUDA_VISIBLE_DEVICES`, `LD_LIBRARY_PATH`). They are exported in the remote
  shell, so they work even when the container sshd does not accept env.
- `stdin` pipes text to the command's standard input.
- `timeout` is in seconds; use it for commands that may hang.
- For anything expected to run more than a few seconds (builds, tests,
  debuggers, servers), use a persistent session instead — see below.

```
computeMCP_exec(target="hal", command="make -j", cwd="/work/proj",
             env={"OMP_NUM_THREADS": "32", "CUDA_VISIBLE_DEVICES": "0"})
computeMCP_exec(target="hal", command="wc -l", stdin="a\nb\nc\n")
```

### Non-TTY stdin can block stdin-reading CLIs

`opencode run ...` launched through `computeMCP_exec` can hang right after
logging `init` and never create a session. The process parks in `ep_poll` with
no network or DB activity, and only a hard kill ends it.

`computeMCP_exec` runs the command over SSH with no input payload, so the
remote command's stdin is an open pipe that never reaches EOF. Reading that
pipe blocks until the tool timeout. Redirect stdin explicitly:

```bash
opencode run "..." </dev/null
```

Passing a finite `stdin` payload through `computeMCP_exec` writes the input and
closes the channel, so a finite stdin payload closes the pipe after the input.
An interactive terminal gives a TTY, which is why the same command does not
block there.

Other non-TTY CLIs that read stdin can block the same way. Redirect stdin with
`</dev/null` unless the command consumes input.

## 3. Persistent and parallel sessions

Long-running work belongs in a PTY session. Sessions are **independent**: you
can open several on the **same target** and they run concurrently, so a long
build in one does not block another call or another session. Use a second
session to inspect a running build (`ps`, `tail`, `nvidia-smi`).

```
sid = computeMCP_session_create(target="hal", cwd="/work/picongpu")["session_id"]

computeMCP_session_write(sid, "cmake --build build -j32\n")   # include the newline
# ... start a second session while the build runs ...
sid2 = computeMCP_session_create(target="hal")["session_id"]
computeMCP_session_write(sid2, "nvidia-smi\n")

computeMCP_session_read(sid2)              # drain buffered output (max_bytes=0 = all)
computeMCP_session_read(sid)               # poll the build; exit_status set when done

computeMCP_session_resize(sid, columns=200, rows=50)
computeMCP_session_close(sid)              # terminates the remote process
```

Rules of thumb:

- `computeMCP_session_write` sends raw input; end commands with `\n`.
- `computeMCP_session_read` **returns and clears** the buffer. Pass `wait=<seconds>`
  to block until new output arrives, the session closes, or the timeout — this
  is the right way to watch a build without polling. The response carries
  `exit_status` (set once the process exits) and `closed`.

  ```
  computeMCP_session_read(sid, wait=30)     # wait for the next output, up to 30 s
  ```
- Always `computeMCP_session_close` sessions you no longer need.
- `computeMCP_sessions(target=None)` lists your sessions (optionally per target).

## 4. Transfer files

Two styles:

**Small text / inline** — content goes in the tool call/response:

```
computeMCP_file_write(target="hal", path="/work/notes.txt", content="hello\n")
computeMCP_file_read(target="hal", path="/work/notes.txt")
computeMCP_file_read_base64(target="hal", path="/work/blob.bin")   # binary read
computeMCP_file_write(target="hal", path="/work/x.bin", content="<base64>", encoding="base64")
```

**Large / binary / artifacts — streamed, nothing in context:**

```
computeMCP_file_upload(target="hal", local_path="/tmp/build.tar.gz", remote_path="/work/build.tar.gz")
computeMCP_file_download(target="hal", remote_path="/work/results.dat", local_path="/tmp/results.dat")
```

**Whole directory trees — also streamed per file:**

```
computeMCP_file_upload_tree(target="hal", local_path="/work/src", remote_path="/work/src")
computeMCP_file_download(target="hal", remote_path="/work/build", local_path="/tmp/build", recursive=True)
```

`computeMCP_file_upload_tree` mirrors a directory tree. It is incremental: re-run
with `skip_existing=True` to send only files still missing remotely, and use
`include=["*.cpp", "*.h"]` / `exclude=["build/*", "*.o"]` to filter.
`computeMCP_file_download(..., recursive=True)` mirrors a remote tree to disk.

- For `computeMCP_file_upload`/`computeMCP_file_download`, `local_path` is **in this
  container**, `remote_path` is **in the remote container**. Prefer these for
  anything large so file bytes never enter the conversation.
- Other file tools: `computeMCP_file_list(target, path)`,
  `computeMCP_file_stat(target, path)`, `computeMCP_file_mkdir`,
  `computeMCP_file_remove`, `computeMCP_file_rename(target, source, destination)`,
  `computeMCP_file_chmod(target, path, mode)` (octal, e.g. `"755"`).
- All paths are paths **inside the remote container**. The gateway host
  filesystem is not accessible.

## 5. Tool reference

| Tool | Purpose |
| --- | --- |
| `computeMCP_targets()` | List available systems + `sharing`/`node_info`/`agent` (do this first) |
| `computeMCP_status(target)` | State/route/clients/`sharing`/`node_info`/`agent`/errors for one system |
| `computeMCP_exec(target, command, cwd?, timeout?, env?, stdin?)` | Short non-interactive command |
| `computeMCP_session_create(target, cwd?, columns?, rows?)` | New PTY session |
| `computeMCP_session_write(session_id, data)` | Send input to a session |
| `computeMCP_session_read(session_id, max_bytes?, wait?)` | Read+clear output; `wait` blocks for new output |
| `computeMCP_session_resize(session_id, columns, rows)` | Resize terminal |
| `computeMCP_session_close(session_id)` | Close session (kills remote process) |
| `computeMCP_sessions(target?)` | List your sessions |
| `computeMCP_file_read(target, path)` | Read text file |
| `computeMCP_file_read_base64(target, path)` | Read binary file |
| `computeMCP_file_write(target, path, content, encoding?)` | Write text/base64 |
| `computeMCP_file_upload(target, local_path, remote_path, append?, parents?, recursive?)` | Stream upload (file or dir) |
| `computeMCP_file_upload_tree(target, local_path, remote_path, skip_existing?, include?, exclude?)` | Recursive incremental upload |
| `computeMCP_file_download(target, remote_path, local_path, recursive?)` | Stream download (file or dir) |
| `computeMCP_file_chmod(target, path, mode)` | Change permissions |
| `computeMCP_file_list(target, path)` | List directory |
| `computeMCP_file_stat(target, path)` | Stat a path |
| `computeMCP_file_mkdir(target, path)` | Create directory (+parents) |
| `computeMCP_file_remove(target, path)` | Remove file/dir |
| `computeMCP_file_rename(target, source, destination)` | Rename/move |

## 6. Workflow example (remote build + test on a GPU system)

```
systems = computeMCP_targets()                       # 1. discover
computeMCP_status("hal")                             #    check it is connected

sid = computeMCP_session_create(target="hal", cwd="/work/alpaka")["session_id"]
computeMCP_session_write(sid, "cmake -S . -B build -DCMAKE_BUILD_TYPE=Release\n")
computeMCP_session_write(sid, "cmake --build build -j\n")

# while it builds, use a second session to watch
sid2 = computeMCP_session_create(target="hal")["session_id"]
computeMCP_session_write(sid2, "nvidia-smi; ps -eo pid,pcpu,comm --sort=-pcpu | head\n")
computeMCP_session_read(sid2)
computeMCP_session_close(sid2)

computeMCP_session_read(sid)                         # poll until exit_status is set
computeMCP_session_write(sid, "ctest --test-dir build --output-on-failure\n")
computeMCP_session_read(sid)
computeMCP_file_download(target="hal", remote_path="/work/alpaka/build/results.xml",
                      local_path="/tmp/results.xml")
computeMCP_session_close(sid)
```

## 7. Rules

- Read this skill before the first `computeMCP_*` call; loading is mandatory
  for all compute MCP actions.
- Call `computeMCP_targets()` first; use only returned target names.
- All `computeMCP_*` operations run inside the **remote development container**,
  not on the host and not on the gateway.
- Use `computeMCP_exec` for short commands; sessions for anything long-running.
- Use multiple sessions on the same target for parallelism.
- Use `computeMCP_file_upload`/`computeMCP_file_download` for large or binary files.
- If a target is unavailable, report the `computeMCP_status`/error — do not try to
  reach the host directly.
- Close sessions you create.
- Check `sharing` before trusting benchmark numbers; only `"exclusive"` systems
  give stable measurements.
- Read `node_info` and treat it as a hypothesis, not ground truth; verify it
  against the actual system.
- Try `agent` entries in list order and fall back to the first working one.
  Remote agents lack the Terok skills: review every result locally before
  trusting or reporting it.
