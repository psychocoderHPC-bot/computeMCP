---
name: terok-compute
description: Use the Terok `compute` MCP to work on remote development/compute containers. Covers discovering available systems, running commands, persistent parallel PTY sessions, and transferring files. Trigger on remote compute, remote build/test/debug, run on another machine, GPU job, parallel remote sessions, copy files to/from a remote system, compute_targets, compute_exec, compute_session_*, compute_file_*.
---

# Terok compute MCP

A single MCP named **`compute`** gives access to one or more remote
development containers. All operations run **inside the remote development
container**, never on the gateway or compute host, and never on this container
unless a tool says so. You hold no SSH credentials; the gateway does the SSH.

The connection model is:

```
you (this container) --> compute MCP --> gateway --> remote dev container
```

## 0. Tool names and client namespacing

This document names tools as the MCP server defines them (`compute_targets`,
`compute_exec`, `compute_file_upload`, `compute_session_*`, …).

Some MCP clients name-space every tool with the **server name**. This server is
registered as `compute`, so such a client exposes the server tool name with an
extra `compute_` prefix — for example, under **opencode**:

| Server tool (this doc) | opencode-visible tool |
| --- | --- |
| `compute_exec` | `compute_compute_exec` |
| `compute_targets` | `compute_compute_targets` |
| `compute_file_upload` | `compute_compute_file_upload` |
| `compute_session_read` | `compute_compute_session_read` |

The doubled **`compute_compute_`** prefix is expected, not an error: it is the
server name (`compute`) plus the tool name (`compute_*`). Call whatever name the
client exposes; do not rename the server to remove it, and do not "fix" the
prefix. Clients that do not name-space (or a direct MCP call) use the bare
`compute_*` names above.

## 1. Always discover first

Call `compute_targets()` before anything else. It lists the systems this task
is allowed to use, with their state. Use **only** target names it returns —
targets are ACL-scoped and any other name is rejected.

```
compute_targets()
```

Then, optionally, inspect one:

```
compute_status(target)      # state, active route, clients, sharing, last_error
```

Each target also carries a **`sharing`** field, which matters for benchmarks:

- `"exclusive"` — the system is dedicated to this task (e.g. a Slurm
  allocation). Timings are meaningful.
- `"shared"` — other users/jobs may run concurrently. Benchmark results can be
  noisy; measure repeatedly and report the variance.
- `"unknown"` — the operator did not declare it. Treat as potentially shared.

Before running or reporting a benchmark, check `sharing` and say which it was.

If `compute_targets` does not return a system you expect, or a target shows
`state != "connected"` with an error, report the gateway status. If the MCP
cannot reach the gateway at all (connection refused/timeout while
`TEROK_COMPUTE_GATEWAY` is set), the Terok Shield is likely blocking the
destination — the project `shield.allow`/`override` must permit the gateway host
(see the project's `project.toml`). **Do not try to bypass the gateway** (no
direct `ssh`, no host access).

If the `compute` MCP is not configured at all (no tools available, or
`TEROK_COMPUTE_GATEWAY`/`TEROK_COMPUTE_TOKEN` unset), you can request access
with the `terok-handshake` tool **inside this container**:

```
terok-handshake <project-id> --port <gateway-port> [--system hal,fwk394]
```

It queues a request; a human must approve it on the gateway console
(`approve <request-id>`). Tell the human the request id and that it is waiting.
On approval the token is written to `~/.bashrc`, but the running agent will not
see it until restarted (tmux keeps its old environment) — the command prints an
`environment` block to paste into the MCP config. Never attempt to bypass the
gateway while waiting.

## 2. Run a command

For short, non-interactive commands use `compute_exec`. It returns
`exit_status`, `stdout`, `stderr`.

```
compute_exec(target="hal", command="nvidia-smi")
compute_exec(target="hal", command="cmake --build build -j", cwd="/work/picongpu")
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
compute_exec(target="hal", command="make -j", cwd="/work/proj",
             env={"OMP_NUM_THREADS": "32", "CUDA_VISIBLE_DEVICES": "0"})
compute_exec(target="hal", command="wc -l", stdin="a\nb\nc\n")
```

## 3. Persistent and parallel sessions

Long-running work belongs in a PTY session. Sessions are **independent**: you
can open several on the **same target** and they run concurrently, so a long
build in one does not block another call or another session. Use a second
session to inspect a running build (`ps`, `tail`, `nvidia-smi`).

```
sid = compute_session_create(target="hal", cwd="/work/picongpu")["session_id"]

compute_session_write(sid, "cmake --build build -j32\n")   # include the newline
# ... start a second session while the build runs ...
sid2 = compute_session_create(target="hal")["session_id"]
compute_session_write(sid2, "nvidia-smi\n")

compute_session_read(sid2)              # drain buffered output (max_bytes=0 = all)
compute_session_read(sid)               # poll the build; exit_status set when done

compute_session_resize(sid, columns=200, rows=50)
compute_session_close(sid)              # terminates the remote process
```

Rules of thumb:

- `compute_session_write` sends raw input; end commands with `\n`.
- `compute_session_read` **returns and clears** the buffer. Pass `wait=<seconds>`
  to block until new output arrives, the session closes, or the timeout — this
  is the right way to watch a build without polling. The response carries
  `exit_status` (set once the process exits) and `closed`.

  ```
  compute_session_read(sid, wait=30)     # wait for the next output, up to 30 s
  ```
- Always `compute_session_close` sessions you no longer need.
- `compute_sessions(target=None)` lists your sessions (optionally per target).

## 4. Transfer files

Two styles:

**Small text / inline** — content goes in the tool call/response:

```
compute_file_write(target="hal", path="/work/notes.txt", content="hello\n")
compute_file_read(target="hal", path="/work/notes.txt")
compute_file_read_base64(target="hal", path="/work/blob.bin")   # binary read
compute_file_write(target="hal", path="/work/x.bin", content="<base64>", encoding="base64")
```

**Large / binary / artifacts — streamed, nothing in context:**

```
compute_file_upload(target="hal", local_path="/tmp/build.tar.gz", remote_path="/work/build.tar.gz")
compute_file_download(target="hal", remote_path="/work/results.dat", local_path="/tmp/results.dat")
```

**Whole directory trees — also streamed per file:**

```
compute_file_upload_tree(target="hal", local_path="/work/src", remote_path="/work/src")
compute_file_download(target="hal", remote_path="/work/build", local_path="/tmp/build", recursive=True)
```

`compute_file_upload_tree` mirrors a directory tree. It is incremental: re-run
with `skip_existing=True` to send only files still missing remotely, and use
`include=["*.cpp", "*.h"]` / `exclude=["build/*", "*.o"]` to filter.
`compute_file_download(..., recursive=True)` mirrors a remote tree to disk.

- For `compute_file_upload`/`compute_file_download`, `local_path` is **in this
  container**, `remote_path` is **in the remote container**. Prefer these for
  anything large so file bytes never enter the conversation.
- Other file tools: `compute_file_list(target, path)`,
  `compute_file_stat(target, path)`, `compute_file_mkdir`,
  `compute_file_remove`, `compute_file_rename(target, source, destination)`,
  `compute_file_chmod(target, path, mode)` (octal, e.g. `"755"`).
- All paths are paths **inside the remote container**. The gateway host
  filesystem is not accessible.

## 5. Tool reference

| Tool | Purpose |
| --- | --- |
| `compute_targets()` | List available systems + `sharing` (do this first) |
| `compute_status(target)` | State/route/clients/`sharing`/errors for one system |
| `compute_exec(target, command, cwd?, timeout?, env?, stdin?)` | Short non-interactive command |
| `compute_session_create(target, cwd?, columns?, rows?)` | New PTY session |
| `compute_session_write(session_id, data)` | Send input to a session |
| `compute_session_read(session_id, max_bytes?, wait?)` | Read+clear output; `wait` blocks for new output |
| `compute_session_resize(session_id, columns, rows)` | Resize terminal |
| `compute_session_close(session_id)` | Close session (kills remote process) |
| `compute_sessions(target?)` | List your sessions |
| `compute_file_read(target, path)` | Read text file |
| `compute_file_read_base64(target, path)` | Read binary file |
| `compute_file_write(target, path, content, encoding?)` | Write text/base64 |
| `compute_file_upload(target, local_path, remote_path, append?, parents?, recursive?)` | Stream upload (file or dir) |
| `compute_file_upload_tree(target, local_path, remote_path, skip_existing?, include?, exclude?)` | Recursive incremental upload |
| `compute_file_download(target, remote_path, local_path, recursive?)` | Stream download (file or dir) |
| `compute_file_chmod(target, path, mode)` | Change permissions |
| `compute_file_list(target, path)` | List directory |
| `compute_file_stat(target, path)` | Stat a path |
| `compute_file_mkdir(target, path)` | Create directory (+parents) |
| `compute_file_remove(target, path)` | Remove file/dir |
| `compute_file_rename(target, source, destination)` | Rename/move |

## 6. Workflow example (remote build + test on a GPU system)

```
systems = compute_targets()                       # 1. discover
compute_status("hal")                             #    check it is connected

sid = compute_session_create(target="hal", cwd="/work/alpaka")["session_id"]
compute_session_write(sid, "cmake -S . -B build -DCMAKE_BUILD_TYPE=Release\n")
compute_session_write(sid, "cmake --build build -j\n")

# while it builds, use a second session to watch
sid2 = compute_session_create(target="hal")["session_id"]
compute_session_write(sid2, "nvidia-smi; ps -eo pid,pcpu,comm --sort=-pcpu | head\n")
compute_session_read(sid2)
compute_session_close(sid2)

compute_session_read(sid)                         # poll until exit_status is set
compute_session_write(sid, "ctest --test-dir build --output-on-failure\n")
compute_session_read(sid)
compute_file_download(target="hal", remote_path="/work/alpaka/build/results.xml",
                      local_path="/tmp/results.xml")
compute_session_close(sid)
```

## 7. Rules

- Call `compute_targets()` first; use only returned target names.
- All `compute_*` operations run inside the **remote development container**,
  not on the host and not on the gateway.
- Use `compute_exec` for short commands; sessions for anything long-running.
- Use multiple sessions on the same target for parallelism.
- Use `compute_file_upload`/`compute_file_download` for large or binary files.
- If a target is unavailable, report the `compute_status`/error — do not try to
  reach the host directly.
- Close sessions you create.
- Check `sharing` before trusting benchmark numbers; only `"exclusive"` systems
  give stable measurements.