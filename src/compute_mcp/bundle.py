# SPDX-FileCopyrightText: René Widera
#
# SPDX-License-Identifier: ISC
"""Deploy versioned helper bundles to a login node over the route connection.

The gateway ships the Slurm provisioning bundle as package data.  On connect it
uploads the exact revision it was built from into a configurable remote
directory when the remote content marker differs, then runs the helper from
there.  Nothing is uploaded when the marker already matches, and
``bundle.auto-deploy = false`` pins whatever copy is already deployed.

All transfer happens over the already-open authenticated route connection: the
SFTP subsystem when available, otherwise a base64 stream over ``conn.run``.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import importlib.resources
import logging
import os
import posixpath
import shlex
import stat as stat_module
import subprocess

import asyncssh

from . import files
from .config import TargetConfig

log = logging.getLogger(__name__)

MARKER_PREFIX = ".computemcp-bundle-"
PROVISION_SCRIPT = "computemcp-provision.sh"
_EXECUTABLE = {
    "computemcp-container.sh",
    "computemcp-job.sh",
    "computemcp-provision.sh",
}


class BundleError(Exception):
    """A bundle could not be loaded or deployed."""


def load_bundle(source: str) -> "BundleContents":
    """Read a shipped bundle and compute its content digest."""
    root = importlib.resources.files("compute_mcp.bundles").joinpath(source)
    try:
        names = sorted(
            entry.name for entry in root.iterdir() if entry.is_file()
        )
    except (FileNotFoundError, NotADirectoryError, OSError) as exc:
        raise BundleError(f"bundle {source!r} is not available: {exc}") from exc
    if PROVISION_SCRIPT not in names:
        raise BundleError(
            f"bundle {source!r} is missing {PROVISION_SCRIPT}"
        )
    files_: list[tuple[str, bytes]] = []
    for name in names:
        data = (root / name).read_bytes()
        files_.append((name, data))
    return BundleContents(tuple(files_), _digest(files_))


def _digest(files_: list[tuple[str, bytes]]) -> str:
    digest = hashlib.sha256()
    for name, data in sorted(files_):
        digest.update(name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(len(data)).encode("ascii"))
        digest.update(b"\0")
        digest.update(data)
    return digest.hexdigest()


class BundleContents:
    """Immutable bundle payload and its content digest."""

    __slots__ = ("files", "digest")

    def __init__(self, files_: tuple[tuple[str, bytes], ...], digest: str) -> None:
        self.files = files_
        self.digest = digest


def marker_name(digest: str) -> str:
    return f"{MARKER_PREFIX}{digest}"


def resolve_deploy_dir(target: TargetConfig) -> str:
    """Absolute remote directory for the target's bundle.

    An explicit ``bundle.deploy-dir`` wins; otherwise the container
    ``storage-root`` with ``/bundle`` appended.  Load-time validation guarantees
    one of the two is present.
    """
    bundle = target.bundle
    if bundle is None:
        raise BundleError(f"target {target.name!r} has no bundle configured")
    if bundle.deploy_dir:
        return bundle.deploy_dir
    root = target.container.storage_root if target.container else None
    if not root:
        raise BundleError(
            f"target {target.name!r} needs bundle.deploy-dir or "
            "container.storage-root"
        )
    return posixpath.join(root, "bundle")


def public_key_for(target: TargetConfig) -> str | None:
    """Derive the container's authorized public key from the target key.

    Prefers ``<client_key>.pub`` and falls back to ``ssh-keygen -y -f``.  The
    result is the public half only; the private key never leaves the gateway
    host.  Returns ``None`` when there is no client key to derive from.
    """
    key_path = target.client_key
    if not key_path:
        return None
    pub_path = key_path + ".pub"
    if os.path.isfile(pub_path):
        with contextlib.suppress(OSError):
            with open(pub_path, encoding="utf-8") as handle:
                first = handle.readline().strip()
            if first:
                return first
    if not os.path.isfile(key_path):
        return None
    try:
        result = subprocess.run(
            ["ssh-keygen", "-y", "-f", key_path],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise BundleError(
            f"target {target.name!r}: cannot derive public key from "
            f"{key_path!r}: {exc}"
        ) from exc
    if result.returncode != 0:
        raise BundleError(
            f"target {target.name!r}: ssh-keygen failed for {key_path!r}: "
            f"{result.stderr.strip() or 'no stderr'}"
        )
    return result.stdout.strip() or None


def provision_argv(target: TargetConfig, action: str) -> tuple[str, ...]:
    """Provisioning argv for ``action`` ('provision'/'stop').

    An explicit ``provision_command``/``close_command`` always wins; a bundle
    target without one uses the deployed helper.
    """
def provision_argv(
    target: TargetConfig, action: str, *, deploy_dir: str | None = None
) -> tuple[str, ...]:
    """Provisioning argv for ``action`` ('provision'/'stop').

    An explicit ``provision_command``/``close_command`` always wins; a bundle
    target without one uses the deployed helper.  ``deploy_dir`` is the already
    resolved remote directory (``$HOME`` expanded); without it the configured
    value is used verbatim, which the caller must not shell-quote-escape.
    """
    if action == "provision" and target.provision_command:
        return target.provision_command
    if action == "stop" and target.close_command:
        return target.close_command
    if target.bundle is None:
        return ()
    directory = deploy_dir or resolve_deploy_dir(target)
    script = posixpath.join(directory, PROVISION_SCRIPT)
    return ("bash", script, "provision" if action == "provision" else "stop")


async def resolve_remote_dir(conn, path: str, run=None) -> str:
    """Expand a leading ``$HOME``/``~`` in ``path`` using the remote account.

    The gateway does not know the remote home directory, so it asks the login
    node once and substitutes.  An absolute path is returned unchanged.
    """
    if path.startswith("/"):
        return path
    if not (path == "$HOME" or path.startswith("$HOME/") or path == "~" or path.startswith("~/")):
        raise BundleError(f"deploy directory must be absolute or start with $HOME/~: {path!r}")
    run = run or _run
    result = await run(conn, 'printf %s "$HOME"')
    if getattr(result, "exit_status", 1) != 0:
        raise BundleError("could not resolve the remote home directory")
    home = (getattr(result, "stdout", b"") or b"").decode(errors="replace").strip()
    if not home.startswith("/"):
        raise BundleError(f"remote home directory is not absolute: {home!r}")
    remainder = path[1:] if path == "~" else path[len("$HOME") :] if path.startswith("$HOME") else path[1:]
    return home + (remainder or "")


async def ensure_deployed(
    conn,
    target: TargetConfig,
    *,
    sftp_factory=None,
    run=None,
) -> str:
    """Deploy the bundle when needed; return the resolved remote directory.

    ``conn`` is the live route connection.  The SFTP subsystem is the fast path;
    a base64 stream over ``run`` is the fallback.  Marker presence is checked
    first so a repeat connect transfers nothing.
    """
    bundle_cfg = target.bundle
    if bundle_cfg is None:
        return ""
    contents = load_bundle(bundle_cfg.source)
    run = run or _run
    deploy_dir = await resolve_remote_dir(conn, resolve_deploy_dir(target), run)
    marker = marker_name(contents.digest)

    if await _marker_present(conn, deploy_dir, marker, run):
        return deploy_dir
    if not bundle_cfg.auto_deploy:
        log.warning(
            "target %s: bundle auto-deploy disabled and marker absent in %s; "
            "using the existing copy",
            target.name,
            deploy_dir,
        )
        return deploy_dir

    factory = sftp_factory or files.sftp_client
    try:
        await _deploy_sftp(factory, conn, deploy_dir, contents, marker)
    except (asyncssh.Error, OSError, BundleError) as exc:
        log.warning(
            "target %s: SFTP bundle deploy failed (%s); falling back to base64",
            target.name,
            exc,
        )
        await _deploy_shell(conn, deploy_dir, contents, marker, run)
    log.info("target %s: deployed bundle %s to %s", target.name, bundle_cfg.source, deploy_dir)
    return deploy_dir


async def _run(conn, command: str, timeout: float = 120.0):
    return await conn.run(command, check=False, encoding=None, timeout=timeout)


async def _marker_present(conn, deploy_dir: str, marker: str, run) -> bool:
    path = posixpath.join(deploy_dir, marker)
    result = await run(conn, f"test -f {shlex.quote(path)}")
    return getattr(result, "exit_status", 1) == 0


async def _deploy_sftp(factory, conn, deploy_dir, contents: BundleContents, marker: str) -> None:
    async with factory(conn) as sftp:
        await sftp.makedirs(deploy_dir, exist_ok=True)
        for name, data in contents.files:
            path = posixpath.join(deploy_dir, name)
            try:
                info = await sftp.lstat(path)
            except (FileNotFoundError, OSError):
                info = None
            if info is not None and stat_module.S_ISLNK(info.permissions or 0):
                raise BundleError(f"refusing to deploy through symlink: {path}")
            async with sftp.open(path, "wb") as handle:
                await handle.write(data)
            await sftp.chmod(path, 0o700 if name in _EXECUTABLE else 0o600)
        # Remove stale markers only after the new files and marker are in place.
        for entry in await sftp.readdir(deploy_dir):
            name = entry.filename
            if name.startswith(MARKER_PREFIX) and name != marker:
                with contextlib.suppress(Exception):
                    await sftp.remove(posixpath.join(deploy_dir, name))
        async with sftp.open(posixpath.join(deploy_dir, marker), "wb") as handle:
            await handle.write(contents.digest.encode("ascii"))
        await sftp.chmod(posixpath.join(deploy_dir, marker), 0o600)


async def _deploy_shell(conn, deploy_dir: str, contents: BundleContents, marker: str, run) -> None:
    quoted_dir = shlex.quote(deploy_dir)
    result = await run(conn, f"mkdir -p {quoted_dir}")
    if getattr(result, "exit_status", 1) != 0:
        raise BundleError(f"could not create deploy dir {deploy_dir}")
    for name, data in contents.files:
        payload = base64.b64encode(data).decode("ascii")
        path = shlex.quote(posixpath.join(deploy_dir, name))
        mode = "700" if name in _EXECUTABLE else "600"
        # Refuse a planted symlink before writing, matching the SFTP path.
        # `> path` would otherwise follow it outside the deploy directory.
        command = (
            f'[ -L {path} ] && {{ echo "refusing symlink: " {path} >&2; exit 1; }}; '
            f"printf %s {shlex.quote(payload)} | base64 -d > {path} && "
            f"chmod {mode} {path}"
        )
        result = await run(conn, command)
        if getattr(result, "exit_status", 1) != 0:
            raise BundleError(f"could not write {name} to {deploy_dir}")
    marker_path = shlex.quote(posixpath.join(deploy_dir, marker))
    cleanup = (
        f"for f in {quoted_dir}/{MARKER_PREFIX}*; do "
        f'[ -e "$f" ] && [ "$f" != {marker_path} ] && rm -f "$f"; done; true'
    )
    await run(conn, cleanup)
    result = await run(
        conn,
        f"printf %s {shlex.quote(contents.digest)} > {marker_path} && "
        f"chmod 600 {marker_path}",
    )
    if getattr(result, "exit_status", 1) != 0:
        raise BundleError(f"could not write bundle marker in {deploy_dir}")
