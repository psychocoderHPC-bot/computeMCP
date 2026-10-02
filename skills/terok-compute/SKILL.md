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

## 1. Always discover first

Call `compute_targets()` before anything else. It lists the systems this task
is allowed to use, with their state. Use **only** target names it returns —
targets are ACL-scoped and any other name is rejected.

```
compute_targets()
```

Then, optionally, inspect one:

```
compute_status(target)      # state, active route, connected clients, last_error
```

If `compute_targets` does not return a system you expect, or a target shows
`state != "connected"` with an error, report the gateway status. **Do not try
to bypass the gateway** (no direct `ssh`, no host access).

## 2. Run a command

For short, non-interactive commands use `compute_exec`. It returns
`exit_status`, `stdout`, `stderr`.

```
compute_exec(target="hal", command="nvidia-smi")
compute_exec(target="hal", command="cmake --build build -j", cwd="/work/picongpu")
```

- `cwd` is a directory **inside the remote container** (the shell runs there).
- `timeout` is in seconds; use it for commands that may hang.
- For anything expected to run more than a few seconds (builds, tests,
  debuggers, servers), use a persistent session instead — see below.

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
- `compute_session_read` **returns and clears** the buffer. Read periodically
  for long jobs; the response carries `exit_status` (set once the process
  exits) and `closed`.
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

- For `compute_file_upload`/`compute_file_download`, `local_path` is **in this
  container**, `remote_path` is **in the remote container**. Prefer these for
  anything large so file bytes never enter the conversation.
- Other file tools: `compute_file_list(target, path)`,
  `compute_file_stat(target, path)`, `compute_file_mkdir`,
  `compute_file_remove`, `compute_file_rename(target, source, destination)`.
- All paths are paths **inside the remote container**. The gateway host
  filesystem is not accessible.

## 5. Tool reference

| Tool | Purpose |
| --- | --- |
| `compute_targets()` | List available systems (do this first) |
| `compute_status(target)` | State/route/clients/errors for one system |
| `compute_exec(target, command, cwd?, timeout?)` | Short non-interactive command |
| `compute_session_create(target, cwd?, columns?, rows?)` | New PTY session |
| `compute_session_write(session_id, data)` | Send input to a session |
| `compute_session_read(session_id, max_bytes?)` | Read+clear buffered output |
| `compute_session_resize(session_id, columns, rows)` | Resize terminal |
| `compute_session_close(session_id)` | Close session (kills remote process) |
| `compute_sessions(target?)` | List your sessions |
| `compute_file_read(target, path)` | Read text file |
| `compute_file_read_base64(target, path)` | Read binary file |
| `compute_file_write(target, path, content, encoding?)` | Write text/base64 |
| `compute_file_upload(target, local_path, remote_path, append?, parents?)` | Stream upload |
| `compute_file_download(target, remote_path, local_path)` | Stream download |
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
