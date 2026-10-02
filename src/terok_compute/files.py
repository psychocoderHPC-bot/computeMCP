# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""SFTP file operations inside a remote development container.

Every path is a path inside the selected container.  The gateway host
filesystem is never exposed: all operations go through the SSH connection's
SFTP subsystem.
"""

from __future__ import annotations

import contextlib
import posixpath
import stat as stat_module

import asyncssh

from .ssh_backend import SSHError

MAX_INLINE_READ = 8 * 1024 * 1024


def _sftp_error(exc: Exception) -> SSHError:
    if isinstance(exc, asyncssh.SFTPError):
        return SSHError(f"SFTP error: {exc}")
    return SSHError(f"file operation failed: {exc}")


async def read(sftp: asyncssh.SFTPClient, path: str) -> tuple[bytes, dict]:
    try:
        info = await sftp.stat(path)
        async with sftp.open(path, "rb") as handle:
            data = await handle.read()
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return data, _stat_dict(path, info)


async def write(sftp: asyncssh.SFTPClient, path: str, content: bytes) -> dict:
    try:
        async with sftp.open(path, "wb") as handle:
            await handle.write(content)
        info = await sftp.stat(path)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return _stat_dict(path, info)


async def list_dir(sftp: asyncssh.SFTPClient, path: str) -> list[dict]:
    try:
        names = await sftp.readdir(path)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    entries = []
    for entry in names:
        name = entry.filename
        if name in (".", ".."):
            continue
        entries.append(
            {
                "name": name,
                "path": posixpath.join(path, name),
                "type": _type_of(entry.attrs),
                "size": entry.attrs.size,
                "mode": oct(entry.attrs.permissions & 0o7777) if entry.attrs.permissions else None,
            }
        )
    entries.sort(key=lambda item: (item["type"] != "directory", item["name"]))
    return entries


async def stat(sftp: asyncssh.SFTPClient, path: str) -> dict:
    try:
        info = await sftp.stat(path)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return _stat_dict(path, info)


async def mkdir(sftp: asyncssh.SFTPClient, path: str) -> dict:
    try:
        await sftp.makedirs(path, exist_ok=False)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return {"path": path, "created": True}


async def remove(sftp: asyncssh.SFTPClient, path: str) -> dict:
    try:
        info = await sftp.stat(path)
        if stat_module.S_ISDIR(info.permissions) if info.permissions else False:
            await sftp.rmtree(path)
        else:
            await sftp.remove(path)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return {"path": path, "removed": True}


async def rename(sftp: asyncssh.SFTPClient, source: str, destination: str) -> dict:
    try:
        await sftp.rename(source, destination)
    except (asyncssh.Error, OSError) as exc:
        raise _sftp_error(exc) from exc
    return {"source": source, "destination": destination, "renamed": True}


def _type_of(attrs) -> str:
    perms = attrs.permissions or 0
    if stat_module.S_ISDIR(perms):
        return "directory"
    if stat_module.S_ISLNK(perms):
        return "symlink"
    return "file"


def _stat_dict(path: str, info) -> dict:
    return {
        "path": path,
        "type": _type_of(info),
        "size": info.size,
        "mode": oct(info.permissions & 0o7777) if info.permissions else None,
        "uid": info.uid,
        "gid": info.gid,
        "mtime": info.mtime,
    }


@contextlib.asynccontextmanager
async def sftp_client(conn: asyncssh.SSHClientConnection):
    client = None
    try:
        client = await conn.start_sftp_client()
        yield client
    finally:
        if client is not None:
            with contextlib.suppress(Exception):
                client.exit()
