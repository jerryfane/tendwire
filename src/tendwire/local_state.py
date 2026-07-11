"""POSIX-only primitives for Tendwire private local state.

The module deliberately keeps creation, inspection, and repair as distinct
operations.  Callers can therefore decide when a migration is allowed while
sharing the race-resistant path handling and validation rules.
"""

from __future__ import annotations

import errno
import os
import secrets
import stat
import threading
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable, Iterator, NoReturn

PRIVATE_DIRECTORY_MODE = 0o700
PRIVATE_FILE_MODE = 0o600
PRIVATE_SOCKET_MODE = 0o600
GROUP_SOCKET_MODE = 0o660


class LocalStateErrorCode(str, Enum):
    """Stable, path-free classifications for local-state failures."""

    UNSUPPORTED_PLATFORM = "unsupported_platform"
    INVALID_ENTRY_NAME = "invalid_entry_name"
    MISSING_ENTRY = "missing_entry"
    ENTRY_EXISTS = "entry_exists"
    WRONG_TYPE = "wrong_type"
    WRONG_OWNER = "wrong_owner"
    WRONG_GROUP = "wrong_group"
    ENTRY_CHANGED = "entry_changed"
    INSECURE_MODE = "insecure_mode"
    INVALID_SOCKET_GROUP = "invalid_socket_group"
    INSECURE_SOCKET_PARENT = "insecure_socket_parent"
    OPERATION_FAILED = "operation_failed"


class LocalStateError(RuntimeError):
    """A typed local-state failure whose text never contains private values."""

    def __init__(self, code: LocalStateErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code


class LocalStateKind(str, Enum):
    STATE_DIRECTORY = "state_directory"
    PRIVATE_FILE = "private_file"
    DATABASE = "database"
    DATABASE_WAL = "database_wal"
    DATABASE_SHM = "database_shm"
    DATABASE_JOURNAL = "database_journal"
    SOCKET = "socket"
    SOCKET_GROUP = "socket_group"


class EntryType(str, Enum):
    DIRECTORY = "directory"
    REGULAR_FILE = "regular_file"
    SOCKET = "socket"


class PermissionState(str, Enum):
    ABSENT = "absent"
    PRIVATE = "private"
    REPAIR_REQUIRED = "repair_required"
    CREATED = "created"
    REPAIRED = "repaired"
    REPLACED = "replaced"


@dataclass(frozen=True)
class EntryIdentity:
    """Filesystem identity suitable for detecting a replaced directory entry."""

    device: int
    inode: int


@dataclass(frozen=True)
class PermissionResult:
    """Path-free result of inspecting or changing one local-state object."""

    kind: LocalStateKind
    state: PermissionState
    mode: int | None


@dataclass(frozen=True)
class LocalStateIssue:
    """Path-free and safe-to-publish local-state audit failure."""

    kind: LocalStateKind
    code: LocalStateErrorCode
    remediation: str


@dataclass(frozen=True)
class ConfigStateReport:
    """Aggregate local-state audit report containing no configured paths."""

    ok: bool
    entries: tuple[PermissionResult, ...]
    issues: tuple[LocalStateIssue, ...]


@dataclass(frozen=True)
class SocketGroup:
    """A group already resolved and verified for the current process."""

    group_id: int


_ERROR_TEXT = {
    LocalStateErrorCode.UNSUPPORTED_PLATFORM: (
        "secure local state requires supported POSIX filesystem operations"
    ),
    LocalStateErrorCode.INVALID_ENTRY_NAME: "local-state entry name is invalid",
    LocalStateErrorCode.MISSING_ENTRY: "required local-state entry is missing",
    LocalStateErrorCode.ENTRY_EXISTS: "local-state entry already exists",
    LocalStateErrorCode.WRONG_TYPE: (
        "local-state entry has an unexpected type; remove it and retry"
    ),
    LocalStateErrorCode.WRONG_OWNER: (
        "local-state entry has an unexpected owner; restore ownership and retry"
    ),
    LocalStateErrorCode.WRONG_GROUP: (
        "local socket has an unexpected group; restore its group and retry"
    ),
    LocalStateErrorCode.ENTRY_CHANGED: (
        "local-state entry changed during validation; retry the operation"
    ),
    LocalStateErrorCode.INSECURE_MODE: (
        "local-state permissions are too broad; run `tendwire daemon` with the "
        "same state configuration or restrict permissions manually, then retry"
    ),
    LocalStateErrorCode.INVALID_SOCKET_GROUP: (
        "socket group is invalid or unavailable; select an existing group for the current process"
    ),
    LocalStateErrorCode.INSECURE_SOCKET_PARENT: (
        "socket parent permissions are unsafe; use a dedicated protected directory"
    ),
    LocalStateErrorCode.OPERATION_FAILED: (
        "secure local-state operation failed; check filesystem permissions and retry"
    ),
}

_REMEDIATION = {
    LocalStateErrorCode.UNSUPPORTED_PLATFORM: "use a supported POSIX host",
    LocalStateErrorCode.INVALID_ENTRY_NAME: "use a private local-state leaf entry",
    LocalStateErrorCode.MISSING_ENTRY: "initialize local state and retry",
    LocalStateErrorCode.ENTRY_EXISTS: "inspect the existing local-state entry",
    LocalStateErrorCode.WRONG_TYPE: "remove the unexpected entry and retry",
    LocalStateErrorCode.WRONG_OWNER: "restore local-state ownership and retry",
    LocalStateErrorCode.WRONG_GROUP: "restore the configured socket group and retry",
    LocalStateErrorCode.ENTRY_CHANGED: "retry after local-state activity has stopped",
    LocalStateErrorCode.INSECURE_MODE: "restrict local-state permissions and retry",
    LocalStateErrorCode.INVALID_SOCKET_GROUP: (
        "select an existing socket group for the current process"
    ),
    LocalStateErrorCode.INSECURE_SOCKET_PARENT: (
        "use an owned protected directory with the required socket access policy"
    ),
    LocalStateErrorCode.OPERATION_FAILED: "check local filesystem permissions and retry",
}

_BIND_UMASK_LOCK = threading.RLock()


def local_state_error(code: LocalStateErrorCode) -> LocalStateError:
    """Construct a typed local-state failure with centralized, path-free text."""

    return LocalStateError(code, _ERROR_TEXT[code])


def _raise(code: LocalStateErrorCode) -> NoReturn:
    raise local_state_error(code) from None


def require_posix_support() -> None:
    """Raise rather than silently weakening guarantees on unsupported hosts."""

    if os.name != "posix":
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    required_dir_fd = tuple(
        getattr(os, name, None)
        for name in ("open", "stat", "mkdir", "unlink", "chmod", "chown", "link")
    )
    chown = getattr(os, "chown", None)
    if (
        None in required_dir_fd
        or not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_DIRECTORY")
        or any(operation not in os.supports_dir_fd for operation in required_dir_fd)
        or os.stat not in os.supports_follow_symlinks
        or chown not in os.supports_follow_symlinks
    ):
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)


def same_inode(first: os.stat_result, second: os.stat_result) -> bool:
    """Return whether two snapshots identify the same filesystem object."""

    return first.st_dev == second.st_dev and first.st_ino == second.st_ino


def entry_identity(value: os.stat_result) -> EntryIdentity:
    """Reduce a stat snapshot to the identity needed for later verification."""

    return EntryIdentity(device=int(value.st_dev), inode=int(value.st_ino))


def identity_matches(identity: EntryIdentity, value: os.stat_result) -> bool:
    return identity.device == value.st_dev and identity.inode == value.st_ino


def _leaf_name(name: str) -> str:
    if (
        not isinstance(name, str)
        or not name
        or name in {".", ".."}
        or os.sep in name
        or (os.altsep is not None and os.altsep in name)
        or "\x00" in name
    ):
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    return name


def _path_parts(path: str | os.PathLike[str]) -> tuple[Path, str]:
    try:
        raw = os.fspath(path)
    except (TypeError, ValueError):
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    if (
        not isinstance(raw, str)
        or not raw
        or "\x00" in raw
        or raw.endswith(os.sep)
    ):
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    components = raw.split(os.sep)
    if ".." in components:
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    try:
        candidate = Path(raw)
    except (TypeError, ValueError):
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    return candidate.parent, _leaf_name(candidate.name)


def _directory_open_flags(*, path_only: bool = False) -> int:
    access = getattr(os, "O_PATH", os.O_RDONLY) if path_only else os.O_RDONLY
    return (
        access
        | os.O_DIRECTORY
        | os.O_NOFOLLOW
        | getattr(os, "O_CLOEXEC", 0)
    )


def _open_parent(
    path: str | os.PathLike[str],
    *,
    missing_ok: bool = False,
    create_missing: bool = False,
    path_only: bool = False,
) -> tuple[int, str] | None:
    """Resolve a parent one pinned directory component at a time.

    The returned descriptor belongs to the caller.  Absolute paths are walked
    from an opened ``/`` and relative paths from an opened ``.``.  No path
    component is ever followed through a symlink.
    """

    require_posix_support()
    parent, name = _path_parts(path)
    raw_parent = os.fspath(parent)
    absolute = raw_parent.startswith(os.sep)
    components = tuple(
        component
        for component in raw_parent.split(os.sep)
        if component not in {"", "."}
    )
    if ".." in components:
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)

    flags = _directory_open_flags(path_only=path_only and not create_missing)
    try:
        current_fd = os.open(os.sep if absolute else ".", flags)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)

    try:
        for component in components:
            expected = lstat_at(current_fd, component)
            created = False
            if expected is None and create_missing:
                try:
                    os.mkdir(component, PRIVATE_DIRECTORY_MODE, dir_fd=current_fd)
                    created = True
                except FileExistsError:
                    pass
                except OSError:
                    _raise(LocalStateErrorCode.OPERATION_FAILED)
                expected = lstat_at(current_fd, component)
                if expected is None:
                    _raise(LocalStateErrorCode.ENTRY_CHANGED)
            elif expected is None:
                if missing_ok:
                    os.close(current_fd)
                    return None
                _raise(LocalStateErrorCode.MISSING_ENTRY)

            assert expected is not None
            if not stat.S_ISDIR(expected.st_mode):
                _raise(LocalStateErrorCode.WRONG_TYPE)
            try:
                next_fd = os.open(component, flags, dir_fd=current_fd)
            except FileNotFoundError:
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
            except OSError as exc:
                if exc.errno in {errno.ELOOP, errno.ENOTDIR}:
                    _raise(LocalStateErrorCode.WRONG_TYPE)
                _raise(LocalStateErrorCode.OPERATION_FAILED)
            try:
                opened = os.fstat(next_fd)
                if not stat.S_ISDIR(opened.st_mode):
                    _raise(LocalStateErrorCode.WRONG_TYPE)
                if not same_inode(expected, opened):
                    _raise(LocalStateErrorCode.ENTRY_CHANGED)
                if created:
                    _validate_owned(opened)
                    try:
                        os.fchmod(next_fd, PRIVATE_DIRECTORY_MODE)
                        os.fsync(next_fd)
                    except OSError:
                        _raise(LocalStateErrorCode.OPERATION_FAILED)
                    opened = os.fstat(next_fd)
                    if (
                        not same_inode(expected, opened)
                        or stat.S_IMODE(opened.st_mode) != PRIVATE_DIRECTORY_MODE
                    ):
                        _raise(LocalStateErrorCode.ENTRY_CHANGED)
                    linked = lstat_at(current_fd, component)
                    if linked is None or not same_inode(opened, linked):
                        _raise(LocalStateErrorCode.ENTRY_CHANGED)
                    _sync_directory(current_fd)
            except OSError:
                os.close(next_fd)
                _raise(LocalStateErrorCode.OPERATION_FAILED)
            except Exception:
                os.close(next_fd)
                raise
            os.close(current_fd)
            current_fd = next_fd
        return current_fd, name
    except Exception:
        os.close(current_fd)
        raise


def open_resolved_parent(
    path: str | os.PathLike[str],
    *,
    create_missing: bool = False,
    path_only: bool = False,
) -> tuple[int, str]:
    """Return a caller-owned securely resolved parent descriptor and leaf."""

    opened = _open_parent(
        path,
        create_missing=create_missing,
        path_only=path_only,
    )
    assert opened is not None
    return opened


def proc_fd_path(dir_fd: int, name: str) -> str:
    """Return a validated Linux proc-fd pathname anchored to ``dir_fd``.

    The caller must retain the directory descriptor for the entire lifetime
    of the API consuming the returned pathname.
    """

    require_posix_support()
    leaf = _leaf_name(name)
    if not sys.platform.startswith("linux") or not isinstance(dir_fd, int) or dir_fd < 0:
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    try:
        expected = os.fstat(dir_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    if not stat.S_ISDIR(expected.st_mode):
        _raise(LocalStateErrorCode.WRONG_TYPE)
    anchor = f"/proc/self/fd/{dir_fd}"
    try:
        current = os.stat(anchor)
    except OSError:
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    if not same_inode(expected, current):
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    return f"{anchor}/{leaf}"


def lstat_at(dir_fd: int, name: str) -> os.stat_result | None:
    """Inspect a leaf entry relative to an open directory without following it."""

    require_posix_support()
    leaf = _leaf_name(name)
    try:
        return os.stat(leaf, dir_fd=dir_fd, follow_symlinks=False)
    except FileNotFoundError:
        return None
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)


def _validate_owned(value: os.stat_result) -> None:
    try:
        owned = value.st_uid == os.geteuid()
    except (AttributeError, OSError):
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    if not owned:
        _raise(LocalStateErrorCode.WRONG_OWNER)


def validate_owned_stat(value: os.stat_result, expected_type: EntryType) -> None:
    """Validate the owner and exact supported file type of a stat snapshot."""

    require_posix_support()
    type_ok = {
        EntryType.DIRECTORY: stat.S_ISDIR,
        EntryType.REGULAR_FILE: stat.S_ISREG,
        EntryType.SOCKET: stat.S_ISSOCK,
    }[expected_type](value.st_mode)
    if not type_ok:
        _raise(LocalStateErrorCode.WRONG_TYPE)
    _validate_owned(value)


def validate_owned_directory_stat(value: os.stat_result) -> None:
    validate_owned_stat(value, EntryType.DIRECTORY)


def validate_owned_regular_stat(value: os.stat_result) -> None:
    validate_owned_stat(value, EntryType.REGULAR_FILE)


def validate_owned_socket_stat(value: os.stat_result) -> None:
    validate_owned_stat(value, EntryType.SOCKET)


def verify_entry_identity(
    dir_fd: int,
    name: str,
    expected: EntryIdentity,
    *,
    expected_type: EntryType,
) -> os.stat_result:
    """Revalidate owner, type, and inode before a pathname mutation."""

    current = lstat_at(dir_fd, name)
    if current is None:
        _raise(LocalStateErrorCode.ENTRY_CHANGED)
    validate_owned_stat(current, expected_type)
    if not identity_matches(expected, current):
        _raise(LocalStateErrorCode.ENTRY_CHANGED)
    return current


def _mode_state(mode: int, maximum_mode: int) -> PermissionState:
    return (
        PermissionState.REPAIR_REQUIRED
        if stat.S_IMODE(mode) & ~maximum_mode
        else PermissionState.PRIVATE
    )


def _result(
    kind: LocalStateKind, value: os.stat_result, maximum_mode: int
) -> PermissionResult:
    mode = stat.S_IMODE(value.st_mode)
    return PermissionResult(kind=kind, state=_mode_state(mode, maximum_mode), mode=mode)


def _open_verified_at(
    dir_fd: int,
    name: str,
    *,
    flags: int,
    expected_type: EntryType,
) -> tuple[int, os.stat_result]:
    expected = lstat_at(dir_fd, name)
    if expected is None:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    validate_owned_stat(expected, expected_type)
    open_flags = flags | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    if expected_type is EntryType.DIRECTORY:
        open_flags |= os.O_DIRECTORY
    try:
        fd = os.open(_leaf_name(name), open_flags, dir_fd=dir_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    try:
        current = os.fstat(fd)
        validate_owned_stat(current, expected_type)
        if not same_inode(expected, current):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        return fd, current
    except Exception:
        os.close(fd)
        raise


def open_private_directory_at(dir_fd: int, name: str) -> int:
    """Open an owned directory leaf with no-follow and inode validation."""

    fd, current = _open_verified_at(
        dir_fd,
        name,
        flags=os.O_RDONLY,
        expected_type=EntryType.DIRECTORY,
    )
    if _mode_state(current.st_mode, PRIVATE_DIRECTORY_MODE) is PermissionState.REPAIR_REQUIRED:
        os.close(fd)
        _raise(LocalStateErrorCode.INSECURE_MODE)
    return fd


def open_private_directory(path: str | os.PathLike[str]) -> int:
    """Open a private owned directory and return a caller-owned descriptor."""

    opened = _open_parent(path)
    assert opened is not None
    parent_fd, name = opened
    try:
        return open_private_directory_at(parent_fd, name)
    finally:
        os.close(parent_fd)


def inspect_private_directory(path: str | os.PathLike[str]) -> PermissionResult:
    """Inspect a directory without creating or repairing it."""

    opened = _open_parent(path, missing_ok=True)
    if opened is None:
        return PermissionResult(
            kind=LocalStateKind.STATE_DIRECTORY,
            state=PermissionState.ABSENT,
            mode=None,
        )
    parent_fd, name = opened
    try:
        current = lstat_at(parent_fd, name)
        if current is None:
            return PermissionResult(
                kind=LocalStateKind.STATE_DIRECTORY,
                state=PermissionState.ABSENT,
                mode=None,
            )
        validate_owned_directory_stat(current)
        return _result(LocalStateKind.STATE_DIRECTORY, current, PRIVATE_DIRECTORY_MODE)
    finally:
        os.close(parent_fd)


def create_private_directory(
    path: str | os.PathLike[str], *, create_missing_parents: bool = False
) -> PermissionResult:
    """Create one private directory leaf securely with exact mode ``0700``."""

    opened = _open_parent(path, create_missing=create_missing_parents)
    assert opened is not None
    parent_fd, name = opened
    created_identity: EntryIdentity | None = None
    try:
        try:
            os.mkdir(name, PRIVATE_DIRECTORY_MODE, dir_fd=parent_fd)
        except FileExistsError:
            _raise(LocalStateErrorCode.ENTRY_EXISTS)
        except OSError:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        created = lstat_at(parent_fd, name)
        if created is None:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        validate_owned_directory_stat(created)
        created_identity = entry_identity(created)
        fd, current = _open_verified_at(
            parent_fd,
            name,
            flags=os.O_RDONLY,
            expected_type=EntryType.DIRECTORY,
        )
        try:
            os.fchmod(fd, PRIVATE_DIRECTORY_MODE)
            current = os.fstat(fd)
            validate_owned_directory_stat(current)
            if not identity_matches(created_identity, current):
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
            if stat.S_IMODE(current.st_mode) != PRIVATE_DIRECTORY_MODE:
                _raise(LocalStateErrorCode.OPERATION_FAILED)
            os.fsync(fd)
            linked = lstat_at(parent_fd, name)
            if linked is None or not same_inode(current, linked):
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
        except OSError:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        finally:
            os.close(fd)
        _sync_directory(parent_fd)
        return PermissionResult(
            kind=LocalStateKind.STATE_DIRECTORY,
            state=PermissionState.CREATED,
            mode=PRIVATE_DIRECTORY_MODE,
        )
    except Exception:
        if created_identity is not None:
            try:
                current = lstat_at(parent_fd, name)
                if (
                    current is not None
                    and identity_matches(created_identity, current)
                    and stat.S_ISDIR(current.st_mode)
                ):
                    os.rmdir(name, dir_fd=parent_fd)
            except (LocalStateError, OSError):
                pass
        raise
    finally:
        os.close(parent_fd)


def _chmod_verified_at(
    dir_fd: int,
    name: str,
    expected: EntryIdentity,
    *,
    expected_type: EntryType,
    mode: int,
) -> os.stat_result:
    verify_entry_identity(
        dir_fd,
        name,
        expected,
        expected_type=expected_type,
    )
    if expected_type is not EntryType.SOCKET:
        try:
            fd, current = _open_verified_at(
                dir_fd,
                name,
                flags=os.O_RDONLY,
                expected_type=expected_type,
            )
        except LocalStateError as exc:
            if exc.code is not LocalStateErrorCode.OPERATION_FAILED:
                raise
        else:
            try:
                if not identity_matches(expected, current):
                    _raise(LocalStateErrorCode.ENTRY_CHANGED)
                os.fchmod(fd, mode)
                current = os.fstat(fd)
                validate_owned_stat(current, expected_type)
                if not identity_matches(expected, current):
                    _raise(LocalStateErrorCode.ENTRY_CHANGED)
                if stat.S_IMODE(current.st_mode) != mode:
                    _raise(LocalStateErrorCode.OPERATION_FAILED)
            except OSError:
                _raise(LocalStateErrorCode.OPERATION_FAILED)
            finally:
                os.close(fd)
            return verify_entry_identity(
                dir_fd,
                name,
                expected,
                expected_type=expected_type,
            )
    try:
        if os.chmod in os.supports_follow_symlinks:
            os.chmod(name, mode, dir_fd=dir_fd, follow_symlinks=False)
        else:
            # AF_UNIX pathnames cannot be opened for fchmod on Linux.  The
            # dir-fd operation and immediately adjacent inode checks are the
            # strongest stdlib-only implementation available there.
            os.chmod(name, mode, dir_fd=dir_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    current = verify_entry_identity(
        dir_fd,
        name,
        expected,
        expected_type=expected_type,
    )
    if stat.S_IMODE(current.st_mode) != mode:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    return current


def repair_private_directory(path: str | os.PathLike[str]) -> PermissionResult:
    """Narrow an existing owned directory mode without widening stricter bits."""

    opened = _open_parent(path)
    assert opened is not None
    parent_fd, name = opened
    try:
        current = lstat_at(parent_fd, name)
        if current is None:
            _raise(LocalStateErrorCode.MISSING_ENTRY)
        validate_owned_directory_stat(current)
        mode = stat.S_IMODE(current.st_mode)
        desired = mode & PRIVATE_DIRECTORY_MODE
        if desired == mode:
            return PermissionResult(
                LocalStateKind.STATE_DIRECTORY, PermissionState.PRIVATE, mode
            )
        current = _chmod_verified_at(
            parent_fd,
            name,
            entry_identity(current),
            expected_type=EntryType.DIRECTORY,
            mode=desired,
        )
        _sync_directory(parent_fd)
        return PermissionResult(
            LocalStateKind.STATE_DIRECTORY,
            PermissionState.REPAIRED,
            stat.S_IMODE(current.st_mode),
        )
    finally:
        os.close(parent_fd)


def prepare_and_open_private_directory(
    path: str | os.PathLike[str],
) -> tuple[int, PermissionResult]:
    """Create or repair a private directory and return that exact open inode."""

    opened = _open_parent(path, create_missing=True)
    assert opened is not None
    parent_fd, name = opened
    fd = -1
    created_identity: EntryIdentity | None = None
    try:
        current = lstat_at(parent_fd, name)
        created = False
        if current is None:
            try:
                os.mkdir(name, PRIVATE_DIRECTORY_MODE, dir_fd=parent_fd)
                created = True
            except FileExistsError:
                pass
            except OSError:
                _raise(LocalStateErrorCode.OPERATION_FAILED)
            current = lstat_at(parent_fd, name)
            if current is None:
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
        validate_owned_directory_stat(current)
        if created:
            created_identity = entry_identity(current)

        fd, opened_stat = _open_verified_at(
            parent_fd,
            name,
            flags=os.O_RDONLY,
            expected_type=EntryType.DIRECTORY,
        )
        identity = entry_identity(opened_stat)
        if created_identity is not None and identity != created_identity:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        mode = stat.S_IMODE(opened_stat.st_mode)
        desired = PRIVATE_DIRECTORY_MODE if created else mode & PRIVATE_DIRECTORY_MODE
        changed = created or desired != mode
        if desired != mode:
            try:
                os.fchmod(fd, desired)
            except OSError:
                _raise(LocalStateErrorCode.OPERATION_FAILED)
        try:
            current = os.fstat(fd)
        except OSError:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        validate_owned_directory_stat(current)
        if not identity_matches(identity, current):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        if stat.S_IMODE(current.st_mode) != desired:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        linked = lstat_at(parent_fd, name)
        if linked is None or not identity_matches(identity, linked):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        if changed:
            try:
                os.fsync(fd)
            except OSError:
                _raise(LocalStateErrorCode.OPERATION_FAILED)
            _sync_directory(parent_fd)
        state = (
            PermissionState.CREATED
            if created
            else PermissionState.REPAIRED
            if desired != mode
            else PermissionState.PRIVATE
        )
        result = PermissionResult(
            LocalStateKind.STATE_DIRECTORY,
            state,
            desired,
        )
        caller_fd = fd
        fd = -1
        return caller_fd, result
    except Exception:
        if created_identity is not None:
            try:
                linked = lstat_at(parent_fd, name)
                if (
                    linked is not None
                    and identity_matches(created_identity, linked)
                    and stat.S_ISDIR(linked.st_mode)
                ):
                    os.rmdir(name, dir_fd=parent_fd)
            except (LocalStateError, OSError):
                pass
        raise
    finally:
        if fd >= 0:
            os.close(fd)
        os.close(parent_fd)


def prepare_resolved_private_parent(
    path: str | os.PathLike[str],
) -> tuple[int, str, PermissionResult]:
    """Prepare and retain the exact private parent of a database/socket leaf."""
    require_posix_support()

    parent, leaf = _path_parts(path)
    if os.fspath(parent) in {".", os.sep}:
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    parent_fd, result = prepare_and_open_private_directory(parent)
    return parent_fd, leaf, result


def prepare_private_directory(path: str | os.PathLike[str]) -> PermissionResult:
    """Create a missing private leaf or repair a validated existing one."""

    fd, result = prepare_and_open_private_directory(path)
    os.close(fd)
    return result


def inspect_private_file_at(
    dir_fd: int,
    name: str,
    *,
    kind: LocalStateKind = LocalStateKind.PRIVATE_FILE,
) -> PermissionResult:
    current = lstat_at(dir_fd, name)
    if current is None:
        return PermissionResult(kind=kind, state=PermissionState.ABSENT, mode=None)
    validate_owned_regular_stat(current)
    return _result(kind, current, PRIVATE_FILE_MODE)


def open_private_file_at(dir_fd: int, name: str, *, flags: int = os.O_RDONLY) -> int:
    """Open an owned private regular file without following or inode races."""
    if flags & (os.O_CREAT | os.O_EXCL | os.O_TRUNC):
        _raise(LocalStateErrorCode.OPERATION_FAILED)

    fd, current = _open_verified_at(
        dir_fd,
        name,
        flags=flags,
        expected_type=EntryType.REGULAR_FILE,
    )
    if _mode_state(current.st_mode, PRIVATE_FILE_MODE) is PermissionState.REPAIR_REQUIRED:
        os.close(fd)
        _raise(LocalStateErrorCode.INSECURE_MODE)
    return fd


def open_private_file(
    path: str | os.PathLike[str],
    *,
    flags: int = os.O_RDONLY,
) -> int:
    """Open a private owned regular file and return a caller-owned descriptor."""

    opened = _open_parent(path)
    assert opened is not None
    parent_fd, name = opened
    try:
        return open_private_file_at(parent_fd, name, flags=flags)
    finally:
        os.close(parent_fd)


def read_private_file_at(
    dir_fd: int,
    name: str,
    *,
    maximum_bytes: int | None = None,
) -> bytes:
    """Read a validated private regular file from an already verified directory."""

    if maximum_bytes is not None and maximum_bytes < 0:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    fd = open_private_file_at(dir_fd, name)
    chunks: list[bytes] = []
    remaining = None if maximum_bytes is None else maximum_bytes + 1
    try:
        while remaining is None or remaining > 0:
            size = 64 * 1024 if remaining is None else min(64 * 1024, remaining)
            try:
                chunk = os.read(fd, size)
            except InterruptedError:
                continue
            except OSError:
                _raise(LocalStateErrorCode.OPERATION_FAILED)
            if not chunk:
                break
            chunks.append(chunk)
            if remaining is not None:
                remaining -= len(chunk)
        content = b"".join(chunks)
        if maximum_bytes is not None and len(content) > maximum_bytes:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        return content
    finally:
        os.close(fd)


def create_private_file_at(
    dir_fd: int,
    name: str,
    *,
    flags: int = os.O_RDWR,
) -> int:
    """Securely create an empty regular file and return its open descriptor."""

    require_posix_support()
    leaf = _leaf_name(name)
    disallowed = os.O_CREAT | os.O_EXCL | os.O_TRUNC
    if flags & disallowed:
        _raise(LocalStateErrorCode.INVALID_ENTRY_NAME)
    open_flags = flags | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW
    open_flags |= getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(leaf, open_flags, PRIVATE_FILE_MODE, dir_fd=dir_fd)
    except FileExistsError:
        _raise(LocalStateErrorCode.ENTRY_EXISTS)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    created_identity: EntryIdentity | None = None
    try:
        current = os.fstat(fd)
        validate_owned_regular_stat(current)
        created_identity = entry_identity(current)
        os.fchmod(fd, PRIVATE_FILE_MODE)
        current = os.fstat(fd)
        validate_owned_regular_stat(current)
        if not identity_matches(created_identity, current):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        if stat.S_IMODE(current.st_mode) != PRIVATE_FILE_MODE:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        published = lstat_at(dir_fd, leaf)
        if published is None or not same_inode(current, published):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        validate_owned_regular_stat(published)
        return fd
    except OSError:
        error = LocalStateError(
            LocalStateErrorCode.OPERATION_FAILED,
            _ERROR_TEXT[LocalStateErrorCode.OPERATION_FAILED],
        )
    except LocalStateError as exc:
        error = exc
    except Exception:
        os.close(fd)
        raise
    os.close(fd)
    if created_identity is not None:
        try:
            current = lstat_at(dir_fd, leaf)
            if current is not None and identity_matches(created_identity, current):
                os.unlink(leaf, dir_fd=dir_fd)
                os.fsync(dir_fd)
        except (LocalStateError, OSError):
            pass
    raise error from None


def inspect_private_file(
    path: str | os.PathLike[str],
    *,
    kind: LocalStateKind = LocalStateKind.PRIVATE_FILE,
) -> PermissionResult:
    opened = _open_parent(path, missing_ok=True)
    if opened is None:
        return PermissionResult(kind=kind, state=PermissionState.ABSENT, mode=None)
    parent_fd, name = opened
    try:
        return inspect_private_file_at(parent_fd, name, kind=kind)
    finally:
        os.close(parent_fd)


def create_private_file(path: str | os.PathLike[str]) -> PermissionResult:
    """Securely create an empty private regular file with exact mode ``0600``."""

    opened = _open_parent(path)
    assert opened is not None
    parent_fd, name = opened
    fd = -1
    try:
        fd = create_private_file_at(parent_fd, name)
        os.fsync(fd)
        _sync_directory(parent_fd)
        return PermissionResult(
            LocalStateKind.PRIVATE_FILE,
            PermissionState.CREATED,
            PRIVATE_FILE_MODE,
        )
    finally:
        if fd >= 0:
            os.close(fd)
        os.close(parent_fd)


def repair_private_file_at(
    dir_fd: int,
    name: str,
    *,
    kind: LocalStateKind = LocalStateKind.PRIVATE_FILE,
) -> PermissionResult:
    """Narrow an existing owned regular-file mode by bitwise intersection."""

    current = lstat_at(dir_fd, name)
    if current is None:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    validate_owned_regular_stat(current)
    mode = stat.S_IMODE(current.st_mode)
    desired = mode & PRIVATE_FILE_MODE
    if desired == mode:
        return PermissionResult(kind, PermissionState.PRIVATE, mode)
    current = _chmod_verified_at(
        dir_fd,
        name,
        entry_identity(current),
        expected_type=EntryType.REGULAR_FILE,
        mode=desired,
    )
    return PermissionResult(kind, PermissionState.REPAIRED, stat.S_IMODE(current.st_mode))


def repair_private_file(
    path: str | os.PathLike[str],
    *,
    kind: LocalStateKind = LocalStateKind.PRIVATE_FILE,
) -> PermissionResult:
    opened = _open_parent(path)
    assert opened is not None
    parent_fd, name = opened
    try:
        result = repair_private_file_at(parent_fd, name, kind=kind)
        if result.state is PermissionState.REPAIRED:
            _sync_directory(parent_fd)
        return result
    finally:
        os.close(parent_fd)


def prepare_private_file(
    path: str | os.PathLike[str],
    *,
    kind: LocalStateKind = LocalStateKind.PRIVATE_FILE,
) -> PermissionResult:
    """Create a missing private file or repair a validated existing file."""

    opened = _open_parent(path)
    assert opened is not None
    parent_fd, name = opened
    fd = -1
    try:
        inspected = inspect_private_file_at(parent_fd, name, kind=kind)
        if inspected.state is PermissionState.ABSENT:
            try:
                fd = create_private_file_at(parent_fd, name)
            except LocalStateError as exc:
                if exc.code is not LocalStateErrorCode.ENTRY_EXISTS:
                    raise
                return repair_private_file_at(parent_fd, name, kind=kind)
            os.fsync(fd)
            _sync_directory(parent_fd)
            return PermissionResult(kind, PermissionState.CREATED, PRIVATE_FILE_MODE)
        return repair_private_file_at(parent_fd, name, kind=kind)
    finally:
        if fd >= 0:
            os.close(fd)
        os.close(parent_fd)


def _write_all(fd: int, content: bytes | bytearray | memoryview) -> None:
    try:
        remaining = memoryview(content)
    except TypeError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    while remaining:
        try:
            written = os.write(fd, remaining)
        except InterruptedError:
            continue
        except OSError:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        if written <= 0:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        remaining = remaining[written:]


def _sync_directory(dir_fd: int) -> None:
    try:
        os.fsync(dir_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)


def unlink_verified_entry(
    dir_fd: int,
    name: str,
    expected: EntryIdentity,
    *,
    expected_type: EntryType,
) -> None:
    """Unlink only the exact owned entry previously inspected by the caller."""

    verify_entry_identity(
        dir_fd,
        name,
        expected,
        expected_type=expected_type,
    )
    try:
        os.unlink(_leaf_name(name), dir_fd=dir_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    _sync_directory(dir_fd)
def publish_private_file_at(
    dir_fd: int,
    name: str,
    content: bytes | bytearray | memoryview,
    *,
    kind: LocalStateKind = LocalStateKind.PRIVATE_FILE,
) -> PermissionResult:
    """Publish complete private content without replacing an existing entry."""

    leaf = _leaf_name(name)
    existing = lstat_at(dir_fd, leaf)
    if existing is not None:
        validate_owned_regular_stat(existing)
        _raise(LocalStateErrorCode.ENTRY_EXISTS)
    temporary_name = ""
    temporary_identity: EntryIdentity | None = None
    fd = -1
    try:
        for _attempt in range(16):
            temporary_name = f".tendwire-{secrets.token_hex(16)}.tmp"
            try:
                fd = create_private_file_at(dir_fd, temporary_name, flags=os.O_WRONLY)
                break
            except LocalStateError as exc:
                if exc.code is LocalStateErrorCode.ENTRY_EXISTS:
                    continue
                raise
        if fd < 0:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        temporary = os.fstat(fd)
        temporary_identity = entry_identity(temporary)
        _write_all(fd, content)
        try:
            os.fsync(fd)
        except OSError:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        verify_entry_identity(
            dir_fd,
            temporary_name,
            temporary_identity,
            expected_type=EntryType.REGULAR_FILE,
        )
        if lstat_at(dir_fd, leaf) is not None:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        try:
            os.link(
                temporary_name,
                leaf,
                src_dir_fd=dir_fd,
                dst_dir_fd=dir_fd,
                follow_symlinks=False,
            )
        except FileExistsError:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        except OSError:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        published = lstat_at(dir_fd, leaf)
        if published is None:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        validate_owned_regular_stat(published)
        if not identity_matches(temporary_identity, published):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        os.unlink(temporary_name, dir_fd=dir_fd)
        temporary_name = ""
        _sync_directory(dir_fd)
        return PermissionResult(kind, PermissionState.CREATED, PRIVATE_FILE_MODE)
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary_name and temporary_identity is not None:
            try:
                current = lstat_at(dir_fd, temporary_name)
                if current is not None and identity_matches(temporary_identity, current):
                    os.unlink(temporary_name, dir_fd=dir_fd)
                    os.fsync(dir_fd)
            except (LocalStateError, OSError):
                pass


def publish_private_file(
    path: str | os.PathLike[str],
    content: bytes | bytearray | memoryview,
    *,
    kind: LocalStateKind = LocalStateKind.PRIVATE_FILE,
) -> PermissionResult:
    """Path wrapper for no-replace private-file publication."""

    opened = _open_parent(path)
    assert opened is not None
    parent_fd, name = opened
    try:
        return publish_private_file_at(parent_fd, name, content, kind=kind)
    finally:
        os.close(parent_fd)


def atomic_replace_private_file(
    path: str | os.PathLike[str],
    content: bytes | bytearray | memoryview,
    *,
    kind: LocalStateKind = LocalStateKind.PRIVATE_FILE,
) -> PermissionResult:
    """Atomically publish private content while retaining stricter prior mode."""

    opened = _open_parent(path)
    assert opened is not None
    parent_fd, name = opened
    existing: os.stat_result | None = None
    expected_identity: EntryIdentity | None = None
    desired_mode = PRIVATE_FILE_MODE
    temporary_name = ""
    temporary_identity: EntryIdentity | None = None
    fd = -1
    try:
        existing = lstat_at(parent_fd, name)
        if existing is not None:
            validate_owned_regular_stat(existing)
            desired_mode = stat.S_IMODE(existing.st_mode) & PRIVATE_FILE_MODE
            expected_identity = entry_identity(existing)
        for _attempt in range(16):
            temporary_name = f".tendwire-{secrets.token_hex(16)}.tmp"
            try:
                fd = create_private_file_at(parent_fd, temporary_name, flags=os.O_WRONLY)
                break
            except LocalStateError as exc:
                if exc.code is LocalStateErrorCode.ENTRY_EXISTS:
                    continue
                raise
        if fd < 0:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        temporary = os.fstat(fd)
        temporary_identity = entry_identity(temporary)
        _write_all(fd, content)
        try:
            os.fsync(fd)
            os.fchmod(fd, desired_mode)
            temporary = os.fstat(fd)
        except OSError:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        validate_owned_regular_stat(temporary)
        if not identity_matches(temporary_identity, temporary):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        if stat.S_IMODE(temporary.st_mode) != desired_mode:
            _raise(LocalStateErrorCode.OPERATION_FAILED)

        current = lstat_at(parent_fd, name)
        if expected_identity is None:
            if current is not None:
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
        else:
            if current is None:
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
            validate_owned_regular_stat(current)
            if not identity_matches(expected_identity, current):
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
        try:
            if expected_identity is None:
                os.link(
                    temporary_name,
                    name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                os.unlink(temporary_name, dir_fd=parent_fd)
            else:
                os.replace(
                    temporary_name,
                    name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
        except FileExistsError:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        except (OSError, TypeError):
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        published = lstat_at(parent_fd, name)
        if published is None:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        validate_owned_regular_stat(published)
        if not identity_matches(temporary_identity, published):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        temporary_name = ""
        _sync_directory(parent_fd)
        return PermissionResult(
            kind,
            PermissionState.CREATED if existing is None else PermissionState.REPLACED,
            desired_mode,
        )
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary_name and temporary_identity is not None:
            try:
                current = lstat_at(parent_fd, temporary_name)
                if current is not None and identity_matches(temporary_identity, current):
                    os.unlink(temporary_name, dir_fd=parent_fd)
                    os.fsync(parent_fd)
            except (LocalStateError, OSError):
                pass
        os.close(parent_fd)


_SQLITE_SUFFIXES = (
    (LocalStateKind.DATABASE, ""),
    (LocalStateKind.DATABASE_WAL, "-wal"),
    (LocalStateKind.DATABASE_SHM, "-shm"),
    (LocalStateKind.DATABASE_JOURNAL, "-journal"),
)


def _sqlite_leaf_names(name: str) -> tuple[tuple[LocalStateKind, str], ...]:
    leaf = _leaf_name(name)
    return tuple((kind, f"{leaf}{suffix}") for kind, suffix in _SQLITE_SUFFIXES)


def _sqlite_names(
    db_path: str | os.PathLike[str],
) -> tuple[Path, tuple[tuple[LocalStateKind, str], ...]]:
    parent, name = _path_parts(db_path)
    return parent, _sqlite_leaf_names(name)


def inspect_sqlite_family_at(
    parent_fd: int, name: str
) -> tuple[PermissionResult, ...]:
    """Inspect a SQLite family relative to a caller-owned resolved parent."""

    return tuple(
        inspect_private_file_at(parent_fd, member, kind=kind)
        for kind, member in _sqlite_leaf_names(name)
    )


def inspect_sqlite_family(
    db_path: str | os.PathLike[str],
) -> tuple[PermissionResult, ...]:
    """Inspect the SQLite main, WAL, SHM, and rollback-journal entries."""

    _parent, names = _sqlite_names(db_path)
    opened = _open_parent(db_path, missing_ok=True)
    if opened is None:
        return tuple(
            PermissionResult(kind, PermissionState.ABSENT, None) for kind, _name in names
        )
    parent_fd, name = opened
    try:
        return inspect_sqlite_family_at(parent_fd, name)
    finally:
        os.close(parent_fd)


def repair_sqlite_family_at(
    parent_fd: int, name: str
) -> tuple[PermissionResult, ...]:
    """Validate then narrow a SQLite family below a resolved parent fd."""

    names = _sqlite_leaf_names(name)
    inspected = tuple(
        inspect_private_file_at(parent_fd, member, kind=kind)
        for kind, member in names
    )
    results: list[PermissionResult] = []
    repaired = False
    for (kind, member), result in zip(names, inspected):
        if result.state is PermissionState.ABSENT:
            results.append(result)
            continue
        changed = repair_private_file_at(parent_fd, member, kind=kind)
        repaired = repaired or changed.state is PermissionState.REPAIRED
        results.append(changed)
    if repaired:
        _sync_directory(parent_fd)
    return tuple(results)


def repair_sqlite_family(
    db_path: str | os.PathLike[str],
) -> tuple[PermissionResult, ...]:
    """Validate the whole SQLite family, then narrow every existing member."""

    opened = _open_parent(db_path)
    assert opened is not None
    parent_fd, name = opened
    try:
        return repair_sqlite_family_at(parent_fd, name)
    finally:
        os.close(parent_fd)


def prepare_sqlite_family_at(
    parent_fd: int, name: str
) -> tuple[PermissionResult, ...]:
    """Create and repair a SQLite family below a resolved private parent fd."""

    names = _sqlite_leaf_names(name)
    fd = -1
    try:
        inspected = tuple(
            inspect_private_file_at(parent_fd, member, kind=kind)
            for kind, member in names
        )
        results: list[PermissionResult] = []
        main_kind, main_name = names[0]
        if inspected[0].state is PermissionState.ABSENT:
            try:
                fd = create_private_file_at(parent_fd, main_name)
            except LocalStateError as exc:
                if exc.code is not LocalStateErrorCode.ENTRY_EXISTS:
                    raise
            else:
                os.fsync(fd)
                results.append(
                    PermissionResult(main_kind, PermissionState.CREATED, PRIVATE_FILE_MODE)
                )
        if not results:
            results.append(repair_private_file_at(parent_fd, main_name, kind=main_kind))
        elif fd < 0:
            results[0] = repair_private_file_at(parent_fd, main_name, kind=main_kind)

        for (kind, member), result in zip(names[1:], inspected[1:]):
            if result.state is PermissionState.ABSENT:
                results.append(result)
            else:
                results.append(repair_private_file_at(parent_fd, member, kind=kind))
        _sync_directory(parent_fd)
        return tuple(results)
    finally:
        if fd >= 0:
            os.close(fd)


def prepare_sqlite_family(
    db_path: str | os.PathLike[str],
) -> tuple[PermissionResult, ...]:
    """Securely create the main database and repair every present sidecar."""

    parent_fd, name, _parent_result = prepare_resolved_private_parent(db_path)
    try:
        return prepare_sqlite_family_at(parent_fd, name)
    finally:
        os.close(parent_fd)


def resolve_socket_group(name: str | None) -> SocketGroup | None:
    """Resolve an opt-in socket group and verify current process membership."""

    require_posix_support()
    if name is None:
        return None
    if not isinstance(name, str) or not name or "\x00" in name:
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    try:
        import grp

        group_id = int(grp.getgrnam(name).gr_gid)
        memberships = {int(os.getegid()), *(int(value) for value in os.getgroups())}
    except (ImportError, KeyError, OSError, TypeError, ValueError):
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    if group_id not in memberships:
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    return SocketGroup(group_id=group_id)


def _open_private_socket_parent(
    socket_path: str | os.PathLike[str],
) -> tuple[int, os.stat_result]:
    opened = _open_parent(socket_path)
    assert opened is not None
    parent_fd, _name = opened
    try:
        current = os.fstat(parent_fd)
        _validate_private_socket_parent_stat(current)
        return parent_fd, current
    except OSError:
        os.close(parent_fd)
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    except Exception:
        os.close(parent_fd)
        raise


def _validate_private_socket_parent_stat(current: os.stat_result) -> None:
    validate_owned_directory_stat(current)
    if stat.S_IMODE(current.st_mode) & (stat.S_IWGRP | stat.S_IWOTH):
        _raise(LocalStateErrorCode.INSECURE_SOCKET_PARENT)


def validate_private_socket_parent_at(parent_fd: int) -> PermissionResult:
    """Validate a caller-owned resolved private socket parent."""

    try:
        current = os.fstat(parent_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    _validate_private_socket_parent_stat(current)
    return PermissionResult(
        LocalStateKind.STATE_DIRECTORY,
        PermissionState.PRIVATE,
        stat.S_IMODE(current.st_mode),
    )


def validate_private_socket_parent(
    socket_path: str | os.PathLike[str],
) -> PermissionResult:
    """Validate an existing private socket parent without changing its mode."""

    parent_fd, _current = _open_private_socket_parent(socket_path)
    try:
        return validate_private_socket_parent_at(parent_fd)
    finally:
        os.close(parent_fd)


def prepare_private_socket_parent(
    socket_path: str | os.PathLike[str],
) -> PermissionResult:
    """Validate an existing private socket parent or create one at ``0700``."""

    try:
        return validate_private_socket_parent(socket_path)
    except LocalStateError as exc:
        if exc.code is not LocalStateErrorCode.MISSING_ENTRY:
            raise

    parent, _name = _path_parts(socket_path)
    try:
        create_private_directory(parent, create_missing_parents=True)
    except LocalStateError as exc:
        if exc.code is not LocalStateErrorCode.ENTRY_EXISTS:
            raise
        return validate_private_socket_parent(socket_path)

    validated = validate_private_socket_parent(socket_path)
    return PermissionResult(
        validated.kind,
        PermissionState.CREATED,
        validated.mode,
    )


def _validate_socket_group_parent_stat(
    current: os.stat_result,
    resolved: SocketGroup,
    *,
    require_current_owner: bool,
) -> None:
    if not stat.S_ISDIR(current.st_mode):
        _raise(LocalStateErrorCode.WRONG_TYPE)
    if require_current_owner:
        _validate_owned(current)
    mode = stat.S_IMODE(current.st_mode)
    if (
        current.st_gid != resolved.group_id
        or not mode & stat.S_IXGRP
        or mode & stat.S_IWGRP
        or mode & stat.S_IRWXO
    ):
        _raise(LocalStateErrorCode.INSECURE_SOCKET_PARENT)


def validate_socket_group_parent_at(
    parent_fd: int, socket_group: str, *, require_current_owner: bool = True
) -> SocketGroup:
    """Validate group socket policy on a caller-owned resolved parent fd."""

    resolved = resolve_socket_group(socket_group)
    if resolved is None:
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    try:
        current = os.fstat(parent_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    _validate_socket_group_parent_stat(
        current,
        resolved,
        require_current_owner=require_current_owner,
    )
    return resolved


def validate_socket_group_parent(
    socket_path: str | os.PathLike[str], socket_group: str
) -> SocketGroup:
    """Validate the dedicated parent required for explicit group sharing."""

    resolved = resolve_socket_group(socket_group)
    if resolved is None:
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    try:
        opened = _open_parent(socket_path)
    except LocalStateError as exc:
        if exc.code is LocalStateErrorCode.WRONG_TYPE:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        raise
    assert opened is not None
    parent_fd, _name = opened
    try:
        return validate_socket_group_parent_at(parent_fd, socket_group)
    finally:
        os.close(parent_fd)


def _inspect_socket_group_configuration(
    socket_path: str | os.PathLike[str] | None,
    socket_group: str,
) -> PermissionResult:
    if socket_path is None:
        resolve_socket_group(socket_group)
        _raise(LocalStateErrorCode.INSECURE_SOCKET_PARENT)
    validate_socket_group_parent(socket_path, socket_group)
    return PermissionResult(
        LocalStateKind.SOCKET_GROUP,
        PermissionState.PRIVATE,
        None,
    )


@contextmanager
def socket_bind_umask(socket_group: str | None = None) -> Iterator[SocketGroup | None]:
    """Apply the process-wide restrictive umask required while binding AF_UNIX."""

    resolved = resolve_socket_group(socket_group)
    mask = 0o117 if resolved is not None else 0o177
    with _BIND_UMASK_LOCK:
        try:
            previous = os.umask(mask)
        except OSError:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        try:
            yield resolved
        finally:
            os.umask(previous)


def _socket_snapshot_at(
    parent_fd: int, name: str
) -> tuple[str, os.stat_result] | None:
    leaf = _leaf_name(name)
    current = lstat_at(parent_fd, leaf)
    if current is None:
        return None
    validate_owned_socket_stat(current)
    return leaf, current


def _socket_snapshot(
    path: str | os.PathLike[str],
) -> tuple[int, str, os.stat_result] | None:
    opened = _open_parent(path, missing_ok=True)
    if opened is None:
        return None
    parent_fd, name = opened
    try:
        snapshot = _socket_snapshot_at(parent_fd, name)
        if snapshot is None:
            os.close(parent_fd)
            return None
        opened_name, current = snapshot
        return parent_fd, opened_name, current
    except Exception:
        os.close(parent_fd)
        raise


def inspect_owned_socket_at(
    parent_fd: int,
    name: str,
    *,
    socket_group: str | None = None,
) -> PermissionResult:
    """Inspect an owned socket relative to a caller-owned resolved parent."""

    resolved = resolve_socket_group(socket_group)
    snapshot = _socket_snapshot_at(parent_fd, name)
    if snapshot is None:
        return PermissionResult(LocalStateKind.SOCKET, PermissionState.ABSENT, None)
    _leaf, current = snapshot
    if resolved is not None and current.st_gid != resolved.group_id:
        _raise(LocalStateErrorCode.WRONG_GROUP)
    maximum = GROUP_SOCKET_MODE if resolved is not None else PRIVATE_SOCKET_MODE
    return _result(LocalStateKind.SOCKET, current, maximum)


def inspect_owned_socket(
    path: str | os.PathLike[str],
    *,
    socket_group: str | None = None,
) -> PermissionResult:
    """Inspect an owned Unix socket without following its pathname."""

    snapshot = _socket_snapshot(path)
    if snapshot is None:
        return PermissionResult(LocalStateKind.SOCKET, PermissionState.ABSENT, None)
    parent_fd, name, _current = snapshot
    try:
        return inspect_owned_socket_at(parent_fd, name, socket_group=socket_group)
    finally:
        os.close(parent_fd)


def owned_socket_identity_at(parent_fd: int, name: str) -> EntryIdentity | None:
    """Return an owned socket identity below a resolved parent, if present."""

    snapshot = _socket_snapshot_at(parent_fd, name)
    if snapshot is None:
        return None
    _leaf, current = snapshot
    return entry_identity(current)


def owned_socket_identity(
    path: str | os.PathLike[str],
) -> EntryIdentity | None:
    """Return the identity of an owned socket, or ``None`` when absent."""

    snapshot = _socket_snapshot(path)
    if snapshot is None:
        return None
    parent_fd, name, _current = snapshot
    try:
        return owned_socket_identity_at(parent_fd, name)
    finally:
        os.close(parent_fd)


def pin_owned_socket_at(
    parent_fd: int, name: str
) -> tuple[int, EntryIdentity] | None:
    """Pin an owned socket relative to a caller-owned resolved parent."""

    path_flag = getattr(os, "O_PATH", None)
    if path_flag is None:
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    flags = path_flag | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(_leaf_name(name), flags, dir_fd=parent_fd)
    except FileNotFoundError:
        return None
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    try:
        current = os.fstat(fd)
        validate_owned_socket_stat(current)
        return fd, entry_identity(current)
    except OSError:
        os.close(fd)
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    except Exception:
        os.close(fd)
        raise


def pin_owned_socket(
    path: str | os.PathLike[str],
) -> tuple[int, EntryIdentity] | None:
    """Open and pin an owned socket inode for a later identity-checked unlink."""

    opened = _open_parent(path, missing_ok=True)
    if opened is None:
        return None
    parent_fd, name = opened
    try:
        return pin_owned_socket_at(parent_fd, name)
    finally:
        os.close(parent_fd)


def pin_group_socket_for_client_at(
    parent_fd: int, name: str, socket_group: str
) -> tuple[int, EntryIdentity, int]:
    """Pin and validate a group socket below a caller-owned parent fd."""

    resolved = validate_socket_group_parent_at(
        parent_fd,
        socket_group,
        require_current_owner=False,
    )
    path_flag = getattr(os, "O_PATH", None)
    if path_flag is None:
        _raise(LocalStateErrorCode.UNSUPPORTED_PLATFORM)
    try:
        parent = os.fstat(parent_fd)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    flags = path_flag | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0)
    try:
        fd = os.open(_leaf_name(name), flags, dir_fd=parent_fd)
    except FileNotFoundError:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    try:
        current = os.fstat(fd)
        if not stat.S_ISSOCK(current.st_mode):
            _raise(LocalStateErrorCode.WRONG_TYPE)
        if current.st_uid != parent.st_uid:
            _raise(LocalStateErrorCode.WRONG_OWNER)
        if current.st_gid != resolved.group_id:
            _raise(LocalStateErrorCode.WRONG_GROUP)
        if stat.S_IMODE(current.st_mode) != GROUP_SOCKET_MODE:
            _raise(LocalStateErrorCode.INSECURE_MODE)
        return fd, entry_identity(current), int(current.st_uid)
    except OSError:
        os.close(fd)
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    except Exception:
        os.close(fd)
        raise


def pin_group_socket_for_client(
    path: str | os.PathLike[str],
    socket_group: str,
) -> tuple[int, EntryIdentity, int]:
    """Pin an exact group socket and return the correlated daemon owner."""

    resolved = resolve_socket_group(socket_group)
    if resolved is None:
        _raise(LocalStateErrorCode.INVALID_SOCKET_GROUP)
    try:
        opened = _open_parent(path, path_only=True)
    except LocalStateError as exc:
        if exc.code is LocalStateErrorCode.WRONG_TYPE:
            _raise(LocalStateErrorCode.OPERATION_FAILED)
        raise
    assert opened is not None
    parent_fd, name = opened
    try:
        return pin_group_socket_for_client_at(parent_fd, name, socket_group)
    finally:
        os.close(parent_fd)


def unlink_verified_socket_at(
    parent_fd: int, name: str, expected: EntryIdentity
) -> None:
    """Unlink an exact socket identity below a caller-owned parent fd."""

    snapshot = _socket_snapshot_at(parent_fd, name)
    if snapshot is None:
        _raise(LocalStateErrorCode.ENTRY_CHANGED)
    leaf, current = snapshot
    if not identity_matches(expected, current):
        _raise(LocalStateErrorCode.ENTRY_CHANGED)
    unlink_verified_entry(
        parent_fd,
        leaf,
        expected,
        expected_type=EntryType.SOCKET,
    )


def unlink_verified_socket(
    path: str | os.PathLike[str], expected: EntryIdentity
) -> None:
    """Unlink only the exact owned socket identity previously inspected."""

    snapshot = _socket_snapshot(path)
    if snapshot is None:
        _raise(LocalStateErrorCode.ENTRY_CHANGED)
    parent_fd, name, _current = snapshot
    try:
        unlink_verified_socket_at(parent_fd, name, expected)
    finally:
        os.close(parent_fd)


def _chgrp_socket_at(
    dir_fd: int, name: str, expected: EntryIdentity, group: SocketGroup
) -> os.stat_result:
    verify_entry_identity(
        dir_fd,
        name,
        expected,
        expected_type=EntryType.SOCKET,
    )
    try:
        os.chown(
            name,
            -1,
            group.group_id,
            dir_fd=dir_fd,
            follow_symlinks=False,
        )
    except OSError:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    current = verify_entry_identity(
        dir_fd,
        name,
        expected,
        expected_type=EntryType.SOCKET,
    )
    if current.st_gid != group.group_id:
        _raise(LocalStateErrorCode.OPERATION_FAILED)
    return current


def enforce_bound_socket_permissions_at(
    parent_fd: int,
    name: str,
    *,
    socket_group: str | None = None,
    expected: EntryIdentity | None = None,
) -> PermissionResult:
    """Set exact permissions on a bound socket below a resolved parent fd."""

    resolved = resolve_socket_group(socket_group)
    snapshot = _socket_snapshot_at(parent_fd, name)
    if snapshot is None:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    leaf, current = snapshot
    identity = entry_identity(current)
    if expected is not None and expected != identity:
        _raise(LocalStateErrorCode.ENTRY_CHANGED)
    if resolved is not None and current.st_gid != resolved.group_id:
        current = _chgrp_socket_at(parent_fd, leaf, identity, resolved)
    maximum = GROUP_SOCKET_MODE if resolved is not None else PRIVATE_SOCKET_MODE
    if stat.S_IMODE(current.st_mode) != maximum:
        current = _chmod_verified_at(
            parent_fd,
            leaf,
            identity,
            expected_type=EntryType.SOCKET,
            mode=maximum,
        )
    _sync_directory(parent_fd)
    return PermissionResult(
        LocalStateKind.SOCKET,
        PermissionState.CREATED,
        stat.S_IMODE(current.st_mode),
    )


def enforce_bound_socket_permissions(
    path: str | os.PathLike[str],
    *,
    socket_group: str | None = None,
    expected: EntryIdentity | None = None,
) -> PermissionResult:
    """Set exact permissions on a newly bound, owned Unix socket."""

    snapshot = _socket_snapshot(path)
    if snapshot is None:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    parent_fd, name, _current = snapshot
    try:
        return enforce_bound_socket_permissions_at(
            parent_fd,
            name,
            socket_group=socket_group,
            expected=expected,
        )
    finally:
        os.close(parent_fd)


def repair_owned_socket_at(
    parent_fd: int,
    name: str,
    *,
    socket_group: str | None = None,
) -> PermissionResult:
    """Narrow a socket below a caller-owned resolved parent fd."""

    resolved = resolve_socket_group(socket_group)
    snapshot = _socket_snapshot_at(parent_fd, name)
    if snapshot is None:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    leaf, current = snapshot
    identity = entry_identity(current)
    changed = False
    if resolved is not None and current.st_gid != resolved.group_id:
        current = _chgrp_socket_at(parent_fd, leaf, identity, resolved)
        changed = True
    maximum = GROUP_SOCKET_MODE if resolved is not None else PRIVATE_SOCKET_MODE
    mode = stat.S_IMODE(current.st_mode)
    desired = mode & maximum
    if desired != mode:
        current = _chmod_verified_at(
            parent_fd,
            leaf,
            identity,
            expected_type=EntryType.SOCKET,
            mode=desired,
        )
        changed = True
    if changed:
        _sync_directory(parent_fd)
    return PermissionResult(
        LocalStateKind.SOCKET,
        PermissionState.REPAIRED if changed else PermissionState.PRIVATE,
        stat.S_IMODE(current.st_mode),
    )


def repair_owned_socket(
    path: str | os.PathLike[str],
    *,
    socket_group: str | None = None,
) -> PermissionResult:
    """Narrow an existing socket and optionally apply a prevalidated group."""

    snapshot = _socket_snapshot(path)
    if snapshot is None:
        _raise(LocalStateErrorCode.MISSING_ENTRY)
    parent_fd, name, _current = snapshot
    try:
        return repair_owned_socket_at(
            parent_fd,
            name,
            socket_group=socket_group,
        )
    finally:
        os.close(parent_fd)


def _audit_one(
    entries: list[PermissionResult],
    issues: list[LocalStateIssue],
    kind: LocalStateKind,
    inspect: object,
) -> None:
    try:
        result = inspect()  # type: ignore[operator]
    except LocalStateError as exc:
        issues.append(
            LocalStateIssue(kind=kind, code=exc.code, remediation=_REMEDIATION[exc.code])
        )
        return
    entries.append(result)
    if result.state is PermissionState.REPAIR_REQUIRED:
        issues.append(
            LocalStateIssue(
                kind=kind,
                code=LocalStateErrorCode.INSECURE_MODE,
                remediation=_REMEDIATION[LocalStateErrorCode.INSECURE_MODE],
            )
        )


def inspect_config_state(
    data_dir: str | os.PathLike[str],
    db_path: str | os.PathLike[str],
    *,
    socket_path: str | os.PathLike[str] | None = None,
    private_files: Iterable[str | os.PathLike[str]] = (),
    socket_group: str | None = None,
) -> ConfigStateReport:
    """Inspect configured local state and return only path-free report values."""

    entries: list[PermissionResult] = []
    issues: list[LocalStateIssue] = []
    _audit_one(
        entries,
        issues,
        LocalStateKind.STATE_DIRECTORY,
        lambda: inspect_private_directory(data_dir),
    )
    for kind, suffix in _SQLITE_SUFFIXES:
        _audit_one(
            entries,
            issues,
            kind,
            lambda kind=kind, suffix=suffix: inspect_private_file(
                Path(f"{os.fspath(db_path)}{suffix}"), kind=kind
            ),
        )
    for private_path in private_files:
        _audit_one(
            entries,
            issues,
            LocalStateKind.PRIVATE_FILE,
            lambda private_path=private_path: inspect_private_file(private_path),
        )
    if socket_group is not None:
        _audit_one(
            entries,
            issues,
            LocalStateKind.SOCKET_GROUP,
            lambda: _inspect_socket_group_configuration(socket_path, socket_group),
        )
    if socket_path is not None:
        _audit_one(
            entries,
            issues,
            LocalStateKind.SOCKET,
            lambda: inspect_owned_socket(socket_path, socket_group=socket_group),
        )
    return ConfigStateReport(
        ok=not issues,
        entries=tuple(entries),
        issues=tuple(issues),
    )


@dataclass(frozen=True)
class _ConfigRepairEntry:
    path: Path
    kind: LocalStateKind
    expected_type: EntryType
    maximum_mode: int
    inspected: PermissionResult
    parent_fd: int | None
    name: str
    expected: EntryIdentity | None


def _inspect_config_repair_entry(
    path: str | os.PathLike[str],
    *,
    kind: LocalStateKind,
    expected_type: EntryType,
    maximum_mode: int,
) -> _ConfigRepairEntry:
    parent, name = _path_parts(path)
    stored_path = parent / name
    opened = _open_parent(stored_path, missing_ok=True)
    if opened is None:
        return _ConfigRepairEntry(
            stored_path,
            kind,
            expected_type,
            maximum_mode,
            PermissionResult(kind, PermissionState.ABSENT, None),
            None,
            name,
            None,
        )
    parent_fd, opened_name = opened
    try:
        current = lstat_at(parent_fd, opened_name)
        if current is None:
            inspected = PermissionResult(kind, PermissionState.ABSENT, None)
            expected = None
        else:
            validate_owned_stat(current, expected_type)
            inspected = _result(kind, current, maximum_mode)
            expected = entry_identity(current)
        return _ConfigRepairEntry(
            stored_path,
            kind,
            expected_type,
            maximum_mode,
            inspected,
            parent_fd,
            opened_name,
            expected,
        )
    except Exception:
        os.close(parent_fd)
        raise


def _verify_config_repair_entry(entry: _ConfigRepairEntry) -> None:
    if entry.parent_fd is not None:
        current = lstat_at(entry.parent_fd, entry.name)
        if entry.expected is None:
            if current is not None:
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
        else:
            verify_entry_identity(
                entry.parent_fd,
                entry.name,
                entry.expected,
                expected_type=entry.expected_type,
            )

    opened = _open_parent(entry.path, missing_ok=True)
    if opened is None:
        if entry.expected is not None:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        return
    current_parent_fd, current_name = opened
    try:
        current = lstat_at(current_parent_fd, current_name)
        if entry.expected is None:
            if current is not None:
                _raise(LocalStateErrorCode.ENTRY_CHANGED)
            return
        if current is None:
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
        validate_owned_stat(current, entry.expected_type)
        if not identity_matches(entry.expected, current):
            _raise(LocalStateErrorCode.ENTRY_CHANGED)
    finally:
        os.close(current_parent_fd)


def _repair_config_entry(entry: _ConfigRepairEntry) -> PermissionResult:
    if entry.expected is None:
        return entry.inspected
    assert entry.parent_fd is not None
    current = verify_entry_identity(
        entry.parent_fd,
        entry.name,
        entry.expected,
        expected_type=entry.expected_type,
    )
    mode = stat.S_IMODE(current.st_mode)
    desired = mode & entry.maximum_mode
    if desired == mode:
        return PermissionResult(entry.kind, PermissionState.PRIVATE, mode)
    current = _chmod_verified_at(
        entry.parent_fd,
        entry.name,
        entry.expected,
        expected_type=entry.expected_type,
        mode=desired,
    )
    _sync_directory(entry.parent_fd)
    return PermissionResult(
        entry.kind,
        PermissionState.REPAIRED,
        stat.S_IMODE(current.st_mode),
    )


def repair_config_state(
    data_dir: str | os.PathLike[str],
    db_path: str | os.PathLike[str] | None,
    *,
    private_files: Iterable[str | os.PathLike[str]] = (),
) -> ConfigStateReport:
    """Narrow every existing configured state entry after full prevalidation.

    Missing entries remain absent.  Callers that intend to initialize state
    must do so separately through the explicit ``prepare_*`` operations.
    """

    entries: list[_ConfigRepairEntry] = []
    try:
        entries.append(
            _inspect_config_repair_entry(
                data_dir,
                kind=LocalStateKind.STATE_DIRECTORY,
                expected_type=EntryType.DIRECTORY,
                maximum_mode=PRIVATE_DIRECTORY_MODE,
            )
        )
        if db_path is not None:
            sqlite_parent, sqlite_names = _sqlite_names(db_path)
            for kind, name in sqlite_names:
                entries.append(
                    _inspect_config_repair_entry(
                        sqlite_parent / name,
                        kind=kind,
                        expected_type=EntryType.REGULAR_FILE,
                        maximum_mode=PRIVATE_FILE_MODE,
                    )
                )
        for path in private_files:
            entries.append(
                _inspect_config_repair_entry(
                    path,
                    kind=LocalStateKind.PRIVATE_FILE,
                    expected_type=EntryType.REGULAR_FILE,
                    maximum_mode=PRIVATE_FILE_MODE,
                )
            )

        for entry in entries:
            _verify_config_repair_entry(entry)
        repaired = tuple(_repair_config_entry(entry) for entry in entries)
        for entry in entries:
            _verify_config_repair_entry(entry)
        return ConfigStateReport(ok=True, entries=repaired, issues=())
    finally:
        for entry in entries:
            if entry.parent_fd is not None:
                os.close(entry.parent_fd)
