"""Private installation identity for stable Herdr worker continuity."""

from __future__ import annotations

import errno
import hashlib
import hmac
import json
import os
import secrets
import stat
from collections.abc import Callable
from pathlib import Path

INSTALLATION_KEY_FILENAME = "installation.key"
INSTALLATION_KEY_MARKER_FILENAME = "installation.key.sha256"
INSTALLATION_KEY_SENTINEL_FILENAME = "installation.key.initialized"
INSTALLATION_KEY_BYTES = 32
_INSTALLATION_KEY_SENTINEL_CONTENT = b"1"
STABLE_KEY_VERSION = 1
STABLE_KEY_PREFIX = "wsk1_"
_HERDR_PUBLIC_ID_ALPHABET = frozenset("123456789ABCDEFGHJKMNPQRSTVWXYZ0")
_STABLE_KEY_DOMAIN = "tendwire.worker-stable-key"


class InstallationKeyError(RuntimeError):
    """The installation key cannot be used safely."""


def canonical_herdr_pane_identity(workspace_id: str | None, pane_id: str | None) -> tuple[str, str] | None:
    """Validate an authoritative Herdr workspace/public-pane identity."""
    if not isinstance(workspace_id, str) or not workspace_id.startswith("w"):
        return None
    workspace_number = workspace_id[1:]
    if not workspace_number or any(
        character not in _HERDR_PUBLIC_ID_ALPHABET
        for character in workspace_number
    ):
        return None
    if not isinstance(pane_id, str):
        return None
    prefix = f"{workspace_id}:p"
    if not pane_id.startswith(prefix):
        return None
    public_number = pane_id[len(prefix) :]
    if not public_number or any(character not in _HERDR_PUBLIC_ID_ALPHABET for character in public_number):
        return None
    return workspace_id, pane_id


def stable_worker_key(
    installation_key: bytes,
    *,
    backend: str,
    host_id: str,
    workspace_id: str,
    pane_id: str,
) -> str:
    """Derive the public opaque key without public sanitizers or binding hashes."""
    if len(installation_key) != INSTALLATION_KEY_BYTES:
        raise InstallationKeyError("installation identity is unavailable")
    message = json.dumps(
        {
            "backend": str(backend),
            "domain": _STABLE_KEY_DOMAIN,
            "host_id": str(host_id),
            "pane_id": pane_id,
            "version": STABLE_KEY_VERSION,
            "workspace_id": workspace_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    digest = hmac.new(installation_key, message, hashlib.sha256).hexdigest()
    return f"{STABLE_KEY_PREFIX}{digest}"

def is_stable_worker_key(value: object) -> bool:
    """Return whether a value has the exact current public key shape."""
    if not isinstance(value, str) or not value.startswith(STABLE_KEY_PREFIX):
        return False
    digest = value[len(STABLE_KEY_PREFIX) :]
    return len(digest) == 64 and all(character in "0123456789abcdef" for character in digest)


def _same_file(first: os.stat_result, second: os.stat_result) -> bool:
    return first.st_dev == second.st_dev and first.st_ino == second.st_ino


def _require_owned(expected: os.stat_result) -> None:
    if expected.st_uid != os.geteuid():
        raise InstallationKeyError("installation identity is unavailable")


def _require_private_mode(current: int) -> None:
    if stat.S_IMODE(current) & 0o077:
        raise InstallationKeyError("installation identity is unavailable")


def _open_data_dir(data_dir: Path) -> int:
    created = False
    try:
        os.makedirs(data_dir, mode=0o700, exist_ok=False)
        created = True
    except FileExistsError:
        pass
    except OSError as exc:
        raise InstallationKeyError("installation identity is unavailable") from exc

    try:
        expected = os.lstat(data_dir)
        if not stat.S_ISDIR(expected.st_mode):
            raise InstallationKeyError("installation identity is unavailable")
        _require_owned(expected)
        if not created:
            _require_private_mode(expected.st_mode)
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
        flags |= getattr(os, "O_CLOEXEC", 0)
        fd = os.open(data_dir, flags)
    except InstallationKeyError:
        raise
    except OSError as exc:
        raise InstallationKeyError("installation identity is unavailable") from exc

    try:
        current = os.fstat(fd)
        if not stat.S_ISDIR(current.st_mode) or not _same_file(expected, current):
            raise InstallationKeyError("installation identity is unavailable")
        _require_owned(current)
        if created:
            os.fchmod(fd, 0o700)
        else:
            _require_private_mode(current.st_mode)
        return fd
    except Exception:
        os.close(fd)
        raise


def _entry_stat(dir_fd: int, name: str) -> os.stat_result | None:
    try:
        return os.stat(name, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise InstallationKeyError("installation identity is unavailable") from exc


def _validate_file_stat(value: os.stat_result) -> None:
    if not stat.S_ISREG(value.st_mode):
        raise InstallationKeyError("installation identity is unavailable")
    _require_owned(value)
    _require_private_mode(value.st_mode)


def _read_exact_file(dir_fd: int, name: str, expected_size: int) -> tuple[bytes, os.stat_result]:
    expected = _entry_stat(dir_fd, name)
    if expected is None:
        raise FileNotFoundError(name)
    _validate_file_stat(expected)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(name, flags, dir_fd=dir_fd)
    except OSError as exc:
        raise InstallationKeyError("installation identity is unavailable") from exc
    try:
        current = os.fstat(fd)
        _validate_file_stat(current)
        if not _same_file(expected, current):
            raise InstallationKeyError("installation identity is unavailable")
        _require_private_mode(current.st_mode)
        chunks: list[bytes] = []
        remaining = expected_size + 1
        while remaining:
            chunk = os.read(fd, remaining)
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        content = b"".join(chunks)
        if len(content) != expected_size:
            raise InstallationKeyError("installation identity is unavailable")
        return content, current
    finally:
        os.close(fd)


def _write_all(fd: int, content: bytes) -> None:
    view = memoryview(content)
    while view:
        try:
            written = os.write(fd, view)
        except InterruptedError:
            continue
        if written <= 0:
            raise InstallationKeyError("installation identity is unavailable")
        view = view[written:]


def _create_exact_file(dir_fd: int, name: str, content: bytes) -> os.stat_result:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_CLOEXEC", 0)
    temporary_name = ""
    fd = -1
    for _attempt in range(16):
        try:
            temporary_name = f".{name}.{os.getpid()}.{secrets.token_hex(16)}.tmp"
            fd = os.open(temporary_name, flags, 0o600, dir_fd=dir_fd)
            break
        except FileExistsError:
            continue
        except OSError as exc:
            raise InstallationKeyError("installation identity is unavailable") from exc
    if fd < 0:
        raise InstallationKeyError("installation identity is unavailable")

    temporary_stat: os.stat_result | None = None
    try:
        temporary_stat = os.fstat(fd)
        _validate_file_stat(temporary_stat)
        os.fchmod(fd, 0o600)
        _write_all(fd, content)
        os.fsync(fd)
        temporary_stat = os.fstat(fd)
        if temporary_stat.st_size != len(content):
            raise InstallationKeyError("installation identity is unavailable")
        os.link(
            temporary_name,
            name,
            src_dir_fd=dir_fd,
            dst_dir_fd=dir_fd,
            follow_symlinks=False,
        )
        final_stat = _entry_stat(dir_fd, name)
        if final_stat is None:
            raise InstallationKeyError("installation identity is unavailable")
        _validate_file_stat(final_stat)
        if not _same_file(temporary_stat, final_stat):
            raise InstallationKeyError("installation identity is unavailable")
        os.fsync(dir_fd)
        return final_stat
    finally:
        os.close(fd)
        if temporary_name and temporary_stat is not None:
            try:
                current = _entry_stat(dir_fd, temporary_name)
                if current is not None and _same_file(current, temporary_stat):
                    os.unlink(temporary_name, dir_fd=dir_fd)
                    os.fsync(dir_fd)
            except (InstallationKeyError, OSError):
                pass


def _create_or_read_file(dir_fd: int, name: str, content: bytes) -> tuple[bytes, os.stat_result]:
    try:
        created = _create_exact_file(dir_fd, name, content)
        return content, created
    except FileExistsError:
        return _read_exact_file(dir_fd, name, len(content))
    except InstallationKeyError:
        raise
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            return _read_exact_file(dir_fd, name, len(content))
        raise InstallationKeyError("installation identity is unavailable") from exc


def _verify_entry_unchanged(dir_fd: int, name: str, expected: os.stat_result) -> None:
    current = _entry_stat(dir_fd, name)
    if current is None:
        raise InstallationKeyError("installation identity is unavailable")
    _validate_file_stat(current)
    if not _same_file(expected, current):
        raise InstallationKeyError("installation identity is unavailable")


def _unlink_verified_entry(dir_fd: int, name: str, expected: os.stat_result) -> None:
    _verify_entry_unchanged(dir_fd, name, expected)
    try:
        os.unlink(name, dir_fd=dir_fd)
        os.fsync(dir_fd)
    except OSError as exc:
        raise InstallationKeyError("installation identity is unavailable") from exc


def load_or_create_installation_key(
    data_dir: Path,
    *,
    random_bytes: Callable[[int], bytes] = secrets.token_bytes,
) -> bytes:
    """Load a verified installation key or publish the first installation identity."""
    dir_fd = _open_data_dir(Path(data_dir))
    try:
        key_stat = _entry_stat(dir_fd, INSTALLATION_KEY_FILENAME)
        marker_stat = _entry_stat(dir_fd, INSTALLATION_KEY_MARKER_FILENAME)
        sentinel_stat = _entry_stat(dir_fd, INSTALLATION_KEY_SENTINEL_FILENAME)

        sentinel: bytes | None = None
        if sentinel_stat is not None:
            sentinel, sentinel_stat = _read_exact_file(
                dir_fd,
                INSTALLATION_KEY_SENTINEL_FILENAME,
                len(_INSTALLATION_KEY_SENTINEL_CONTENT),
            )
            if not hmac.compare_digest(sentinel, _INSTALLATION_KEY_SENTINEL_CONTENT):
                raise InstallationKeyError("installation identity is unavailable")
            if key_stat is None or marker_stat is None:
                raise InstallationKeyError("installation identity is unavailable")

        if key_stat is None and marker_stat is not None:
            # A concurrent creator publishes the key before its marker. Refresh
            # once so a coherent completed publication is not mistaken for loss.
            key_stat = _entry_stat(dir_fd, INSTALLATION_KEY_FILENAME)
        if key_stat is None and (marker_stat is not None or sentinel_stat is not None):
            raise InstallationKeyError("installation identity is unavailable")

        if key_stat is None:
            try:
                candidate = bytes(random_bytes(INSTALLATION_KEY_BYTES))
            except Exception as exc:
                raise InstallationKeyError("installation identity is unavailable") from exc
            if len(candidate) != INSTALLATION_KEY_BYTES:
                raise InstallationKeyError("installation identity is unavailable")
            key, key_stat = _create_or_read_file(dir_fd, INSTALLATION_KEY_FILENAME, candidate)
        else:
            key, key_stat = _read_exact_file(dir_fd, INSTALLATION_KEY_FILENAME, INSTALLATION_KEY_BYTES)

        marker_expected = hashlib.sha256(key).hexdigest().encode("ascii")
        if marker_stat is None:
            marker, marker_stat = _create_or_read_file(
                dir_fd,
                INSTALLATION_KEY_MARKER_FILENAME,
                marker_expected,
            )
        else:
            marker, marker_stat = _read_exact_file(
                dir_fd,
                INSTALLATION_KEY_MARKER_FILENAME,
                len(marker_expected),
            )
        if not hmac.compare_digest(marker, marker_expected):
            raise InstallationKeyError("installation identity is unavailable")

        _verify_entry_unchanged(dir_fd, INSTALLATION_KEY_FILENAME, key_stat)
        _verify_entry_unchanged(dir_fd, INSTALLATION_KEY_MARKER_FILENAME, marker_stat)

        if sentinel_stat is None:
            sentinel, sentinel_stat = _create_or_read_file(
                dir_fd,
                INSTALLATION_KEY_SENTINEL_FILENAME,
                _INSTALLATION_KEY_SENTINEL_CONTENT,
            )
        if sentinel is None or not hmac.compare_digest(
            sentinel,
            _INSTALLATION_KEY_SENTINEL_CONTENT,
        ):
            raise InstallationKeyError("installation identity is unavailable")

        _verify_entry_unchanged(dir_fd, INSTALLATION_KEY_FILENAME, key_stat)
        _verify_entry_unchanged(dir_fd, INSTALLATION_KEY_MARKER_FILENAME, marker_stat)
        _verify_entry_unchanged(dir_fd, INSTALLATION_KEY_SENTINEL_FILENAME, sentinel_stat)
        return key
    except InstallationKeyError:
        raise
    except OSError as exc:
        raise InstallationKeyError("installation identity is unavailable") from exc
    finally:
        os.close(dir_fd)


def reset_installation_key(
    data_dir: Path,
    *,
    acknowledge_continuity_break: bool,
) -> None:
    """Explicitly reset a verified installation identity while all users are offline."""
    if acknowledge_continuity_break is not True:
        raise InstallationKeyError("installation identity reset was not acknowledged")

    dir_fd = _open_data_dir(Path(data_dir))
    try:
        key, key_stat = _read_exact_file(
            dir_fd,
            INSTALLATION_KEY_FILENAME,
            INSTALLATION_KEY_BYTES,
        )
        marker_expected = hashlib.sha256(key).hexdigest().encode("ascii")
        marker, marker_stat = _read_exact_file(
            dir_fd,
            INSTALLATION_KEY_MARKER_FILENAME,
            len(marker_expected),
        )
        sentinel, sentinel_stat = _read_exact_file(
            dir_fd,
            INSTALLATION_KEY_SENTINEL_FILENAME,
            len(_INSTALLATION_KEY_SENTINEL_CONTENT),
        )
        if not hmac.compare_digest(marker, marker_expected) or not hmac.compare_digest(
            sentinel,
            _INSTALLATION_KEY_SENTINEL_CONTENT,
        ):
            raise InstallationKeyError("installation identity is unavailable")

        _verify_entry_unchanged(dir_fd, INSTALLATION_KEY_FILENAME, key_stat)
        _verify_entry_unchanged(dir_fd, INSTALLATION_KEY_MARKER_FILENAME, marker_stat)
        _verify_entry_unchanged(dir_fd, INSTALLATION_KEY_SENTINEL_FILENAME, sentinel_stat)

        # Keep the sentinel until last: an interrupted reset either recovers
        # the intact key or remains fail-closed instead of rotating on load.
        _unlink_verified_entry(dir_fd, INSTALLATION_KEY_MARKER_FILENAME, marker_stat)
        _unlink_verified_entry(dir_fd, INSTALLATION_KEY_FILENAME, key_stat)
        _unlink_verified_entry(dir_fd, INSTALLATION_KEY_SENTINEL_FILENAME, sentinel_stat)
    except InstallationKeyError:
        raise
    except OSError as exc:
        raise InstallationKeyError("installation identity is unavailable") from exc
    finally:
        os.close(dir_fd)
