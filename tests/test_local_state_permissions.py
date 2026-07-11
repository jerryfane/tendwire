from __future__ import annotations

import errno
import os
import socket
import stat
import sys
from dataclasses import asdict
from pathlib import Path

import pytest

import tendwire.local_state as local_state
from tendwire.local_state import (
    EntryType,
    LocalStateError,
    LocalStateErrorCode,
    LocalStateKind,
    PermissionState,
    atomic_replace_private_file,
    create_private_directory,
    create_private_file,
    enforce_bound_socket_permissions,
    entry_identity,
    inspect_config_state,
    inspect_owned_socket,
    inspect_private_directory,
    inspect_private_file,
    inspect_sqlite_family,
    open_private_directory,
    publish_private_file_at,
    pin_group_socket_for_client,
    pin_owned_socket,
    prepare_private_socket_parent,
    prepare_sqlite_family,
    read_private_file_at,
    repair_owned_socket,
    repair_private_directory,
    repair_private_file,
    repair_config_state,
    repair_sqlite_family,
    socket_bind_umask,
    unlink_verified_socket,
    unlink_verified_entry,
    validate_owned_regular_stat,
    validate_private_socket_parent,
    validate_socket_group_parent,
)

pytestmark = pytest.mark.skipif(
    os.name != "posix" or not sys.platform.startswith("linux"),
    reason="Linux/POSIX local-state permission contract",
)


def _mode(path: Path) -> int:
    return stat.S_IMODE(os.lstat(path).st_mode)


def _bind(path: Path) -> socket.socket:
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        listener.bind(str(path))
    except Exception:
        listener.close()
        raise
    return listener


def _assert_path_free(value: object, *paths: Path) -> None:
    rendered = repr(value)
    for path in paths:
        assert str(path) not in rendered


def test_permissive_umask_creation_is_private_before_use(tmp_path: Path) -> None:
    state_dir = tmp_path / "private-state"
    private_file = state_dir / "private-value"
    socket_path = state_dir / "daemon-endpoint"
    listener: socket.socket | None = None
    previous_umask = os.umask(0)
    try:
        try:
            directory_result = create_private_directory(state_dir)
            file_result = create_private_file(private_file)
            with socket_bind_umask():
                listener = _bind(socket_path)
            socket_result = enforce_bound_socket_permissions(socket_path)
        finally:
            os.umask(previous_umask)

        assert directory_result.state is PermissionState.CREATED
        assert file_result.state is PermissionState.CREATED
        assert socket_result.state is PermissionState.CREATED
        assert _mode(state_dir) == 0o700
        assert _mode(private_file) == 0o600
        assert _mode(socket_path) == 0o600
    finally:
        if listener is not None:
            listener.close()
        socket_path.unlink(missing_ok=True)


def test_directory_and_file_repairs_intersect_modes_and_never_widen(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    private_file = state_dir / "value"
    private_file.write_bytes(b"private")

    os.chmod(state_dir, 0o775)
    os.chmod(private_file, 0o6754)
    assert inspect_private_directory(state_dir).state is PermissionState.REPAIR_REQUIRED
    assert inspect_private_file(private_file).state is PermissionState.REPAIR_REQUIRED

    assert repair_private_directory(state_dir).state is PermissionState.REPAIRED
    assert repair_private_file(private_file).state is PermissionState.REPAIRED
    assert _mode(state_dir) == (0o775 & 0o700)
    assert _mode(private_file) == (0o6754 & 0o600)

    os.chmod(state_dir, 0o500)
    os.chmod(private_file, 0o400)
    assert repair_private_directory(state_dir).state is PermissionState.PRIVATE
    assert repair_private_file(private_file).state is PermissionState.PRIVATE
    assert _mode(state_dir) == 0o500
    assert _mode(private_file) == 0o400


def test_symlinks_and_wrong_types_are_refused_without_mutation(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    regular = state_dir / "regular"
    regular.write_bytes(b"unchanged")
    os.chmod(regular, 0o600)

    directory_link = tmp_path / "directory-link"
    directory_link.symlink_to(state_dir, target_is_directory=True)
    file_link = state_dir / "file-link"
    file_link.symlink_to(regular)
    wrong_file_type = state_dir / "not-a-file"
    wrong_file_type.mkdir(mode=0o700)
    wrong_socket_type = state_dir / "not-a-socket"
    wrong_socket_type.write_bytes(b"unchanged")

    for operation in (
        lambda: inspect_private_directory(directory_link),
        lambda: inspect_private_file(file_link),
        lambda: repair_private_file(wrong_file_type),
        lambda: inspect_owned_socket(wrong_socket_type),
    ):
        with pytest.raises(LocalStateError) as caught:
            operation()
        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
        _assert_path_free(caught.value, state_dir, regular, directory_link, file_link)

    assert regular.read_bytes() == b"unchanged"
    assert wrong_socket_type.read_bytes() == b"unchanged"
    assert file_link.is_symlink()
    assert directory_link.is_symlink()


def test_wrong_owner_stat_is_rejected_with_no_uid_or_path_in_error(
    tmp_path: Path,
) -> None:
    private_file = tmp_path / "private-owner-value"
    private_file.write_bytes(b"private")
    values = list(os.lstat(private_file))
    unexpected_uid = os.geteuid() + 1
    values[stat.ST_UID] = unexpected_uid
    wrong_owner = os.stat_result(values)

    with pytest.raises(LocalStateError) as caught:
        validate_owned_regular_stat(wrong_owner)

    assert caught.value.code is LocalStateErrorCode.WRONG_OWNER
    assert str(unexpected_uid) not in str(caught.value)
    _assert_path_free(caught.value, private_file)


def test_no_replace_publication_uses_verified_dir_fd_and_complete_content(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "state"
    create_private_directory(state_dir)
    dir_fd = open_private_directory(state_dir)
    try:
        result = publish_private_file_at(dir_fd, "identity", b"complete-private-value")
        assert result.state is PermissionState.CREATED
        assert read_private_file_at(dir_fd, "identity") == b"complete-private-value"
        with pytest.raises(LocalStateError) as caught:
            publish_private_file_at(dir_fd, "identity", b"replacement")
        assert caught.value.code is LocalStateErrorCode.ENTRY_EXISTS
        assert read_private_file_at(dir_fd, "identity") == b"complete-private-value"
    finally:
        os.close(dir_fd)

    assert _mode(state_dir / "identity") == 0o600
    assert not tuple(state_dir.glob(".tendwire-*.tmp"))


def test_atomic_replacement_retains_stricter_mode_and_repairs_broad_mode(
    tmp_path: Path,
) -> None:
    private_file = tmp_path / "replace-value"
    private_file.write_bytes(b"old")
    os.chmod(private_file, 0o400)

    strict_result = atomic_replace_private_file(private_file, b"strict-new")
    assert strict_result.state is PermissionState.REPLACED
    assert private_file.read_bytes() == b"strict-new"
    assert _mode(private_file) == 0o400

    os.chmod(private_file, 0o6744)
    broad_result = atomic_replace_private_file(private_file, b"private-new")
    assert broad_result.state is PermissionState.REPLACED
    assert private_file.read_bytes() == b"private-new"
    assert _mode(private_file) == (0o6744 & 0o600)
    assert not tuple(tmp_path.glob(".tendwire-*.tmp"))


def test_sqlite_family_preparation_and_post_creation_repair(tmp_path: Path) -> None:
    db_path = tmp_path / "state" / "tendwire.db"
    previous_umask = os.umask(0)
    try:
        prepared = prepare_sqlite_family(db_path)
        for suffix in ("-wal", "-shm", "-journal"):
            Path(f"{db_path}{suffix}").write_bytes(b"sqlite-sidecar")
    finally:
        os.umask(previous_umask)

    assert prepared[0].kind is LocalStateKind.DATABASE
    assert prepared[0].state is PermissionState.CREATED
    assert _mode(db_path.parent) == 0o700
    assert _mode(db_path) == 0o600
    before = inspect_sqlite_family(db_path)
    assert tuple(result.state for result in before[1:]) == (
        PermissionState.REPAIR_REQUIRED,
        PermissionState.REPAIR_REQUIRED,
        PermissionState.REPAIR_REQUIRED,
    )

    repaired = repair_sqlite_family(db_path)
    assert tuple(result.mode for result in repaired) == (0o600, 0o600, 0o600, 0o600)
    for suffix in ("", "-wal", "-shm", "-journal"):
        assert _mode(Path(f"{db_path}{suffix}")) == 0o600


def test_sqlite_family_validates_every_member_before_repairing(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    sidecar_target = state_dir / "sidecar-target"
    sidecar_target.write_bytes(b"unchanged")
    os.chmod(sidecar_target, 0o600)
    Path(f"{db_path}-wal").symlink_to(sidecar_target)

    with pytest.raises(LocalStateError) as caught:
        repair_sqlite_family(db_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert _mode(db_path) == 0o644
    assert sidecar_target.read_bytes() == b"unchanged"
    _assert_path_free(caught.value, db_path, sidecar_target)


def test_config_state_repair_is_startup_wide_idempotent_and_never_creates(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    Path(f"{db_path}").write_bytes(b"database")
    Path(f"{db_path}-wal").write_bytes(b"wal")
    identity_paths = (
        state_dir / "installation.key",
        state_dir / "installation.key.sha256",
        state_dir / "installation.key.initialized",
    )
    for path in (db_path, Path(f"{db_path}-wal"), *identity_paths):
        if path in identity_paths:
            path.write_bytes(b"identity")
        os.chmod(path, 0o644)
    missing_identity = state_dir / "missing-private-file"

    first = repair_config_state(
        state_dir,
        db_path,
        private_files=(*identity_paths, missing_identity),
    )

    assert first.ok is True
    assert first.issues == ()
    assert _mode(state_dir) == 0o700
    assert _mode(db_path) == 0o600
    assert _mode(Path(f"{db_path}-wal")) == 0o600
    assert all(_mode(path) == 0o600 for path in identity_paths)
    assert not missing_identity.exists()
    _assert_path_free(first, state_dir, db_path, *identity_paths, missing_identity)

    second = repair_config_state(
        state_dir,
        db_path,
        private_files=(*identity_paths, missing_identity),
    )

    assert all(
        result.state in {PermissionState.PRIVATE, PermissionState.ABSENT}
        for result in second.entries
    )
    assert not missing_identity.exists()


def test_config_state_repair_without_database_still_repairs_other_state(
    tmp_path: Path,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    identity_path = state_dir / "installation.key"
    identity_path.write_bytes(b"identity")
    os.chmod(identity_path, 0o644)

    result = repair_config_state(
        state_dir,
        None,
        private_files=(identity_path,),
    )

    assert tuple(entry.kind for entry in result.entries) == (
        LocalStateKind.STATE_DIRECTORY,
        LocalStateKind.PRIVATE_FILE,
    )
    assert _mode(state_dir) == 0o700
    assert _mode(identity_path) == 0o600
    _assert_path_free(result, state_dir, identity_path)


def test_config_state_repair_rejects_entry_appearing_after_prevalidation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    target = state_dir / "protected-target"
    target.write_bytes(b"unchanged")
    os.chmod(target, 0o600)
    missing_identity = state_dir / "installation.key"
    original_verify = local_state._verify_config_repair_entry
    introduced = False

    def introduce_symlink(entry) -> None:
        nonlocal introduced
        if not introduced:
            introduced = True
            missing_identity.symlink_to(target)
        original_verify(entry)

    monkeypatch.setattr(
        local_state,
        "_verify_config_repair_entry",
        introduce_symlink,
    )

    with pytest.raises(LocalStateError) as caught:
        repair_config_state(
            state_dir,
            db_path,
            private_files=(missing_identity,),
        )

    assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
    assert _mode(state_dir) == 0o755
    assert _mode(db_path) == 0o644
    assert missing_identity.is_symlink()
    assert target.read_bytes() == b"unchanged"
    _assert_path_free(caught.value, state_dir, db_path, target, missing_identity)


@pytest.mark.parametrize("defect_kind", ["symlink", "wrong_type"])
def test_config_state_repair_prevalidates_all_entries_before_mode_changes(
    tmp_path: Path,
    defect_kind: str,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    target = state_dir / "protected-target"
    target.write_bytes(b"unchanged")
    os.chmod(target, 0o600)
    defective_identity = state_dir / "installation.key"
    if defect_kind == "symlink":
        defective_identity.symlink_to(target)
    else:
        defective_identity.mkdir()
        os.chmod(defective_identity, 0o700)

    with pytest.raises(LocalStateError) as caught:
        repair_config_state(
            state_dir,
            db_path,
            private_files=(defective_identity,),
        )

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert _mode(state_dir) == 0o755
    assert _mode(db_path) == 0o644
    assert target.read_bytes() == b"unchanged"
    if defect_kind == "symlink":
        assert defective_identity.is_symlink()
    else:
        assert defective_identity.is_dir()
    _assert_path_free(caught.value, state_dir, db_path, target, defective_identity)


def test_config_state_repair_rejects_wrong_owner_without_changing_modes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state_dir = tmp_path / "configured-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o755)
    db_path = state_dir / "state.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o644)
    unexpected_uid = os.geteuid() + 100_000
    monkeypatch.setattr("tendwire.local_state.os.geteuid", lambda: unexpected_uid)

    with pytest.raises(LocalStateError) as caught:
        repair_config_state(state_dir, db_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_OWNER
    assert _mode(state_dir) == 0o755
    assert _mode(db_path) == 0o644
    assert str(unexpected_uid) not in str(caught.value)
    _assert_path_free(caught.value, state_dir, db_path)


def test_socket_repair_intersects_mode_and_stricter_mode_is_retained(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "daemon-socket"
    listener = _bind(socket_path)
    try:
        os.chmod(socket_path, 0o775)
        assert inspect_owned_socket(socket_path).state is PermissionState.REPAIR_REQUIRED
        assert repair_owned_socket(socket_path).state is PermissionState.REPAIRED
        assert _mode(socket_path) == (0o775 & 0o600)

        os.chmod(socket_path, 0o400)
        assert repair_owned_socket(socket_path).state is PermissionState.PRIVATE
        assert _mode(socket_path) == 0o400
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)


def test_invalid_socket_group_is_rejected_before_chgrp_or_chmod(tmp_path: Path) -> None:
    socket_path = tmp_path / "group-socket"
    listener = _bind(socket_path)
    invalid_group = "tendwire-group-that-does-not-exist-73b8e80e"
    try:
        os.chmod(socket_path, 0o777)
        before = os.lstat(socket_path)
        with pytest.raises(LocalStateError) as caught:
            enforce_bound_socket_permissions(socket_path, socket_group=invalid_group)
        after = os.lstat(socket_path)

        assert caught.value.code is LocalStateErrorCode.INVALID_SOCKET_GROUP
        assert invalid_group not in str(caught.value)
        assert (after.st_dev, after.st_ino, after.st_gid, stat.S_IMODE(after.st_mode)) == (
            before.st_dev,
            before.st_ino,
            before.st_gid,
            stat.S_IMODE(before.st_mode),
        )
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)
def test_explicit_member_socket_group_uses_exact_group_private_mode(
    tmp_path: Path,
) -> None:
    import grp

    socket_path = tmp_path / "group-socket"
    group_name = grp.getgrgid(os.getegid()).gr_name
    listener: socket.socket | None = None
    previous_umask = os.umask(0)
    try:
        try:
            with socket_bind_umask(group_name):
                listener = _bind(socket_path)
            result = enforce_bound_socket_permissions(
                socket_path,
                socket_group=group_name,
            )
        finally:
            os.umask(previous_umask)

        assert result.mode == 0o660
        assert _mode(socket_path) == 0o660
        assert os.lstat(socket_path).st_gid == os.getegid()
    finally:
        if listener is not None:
            listener.close()
        socket_path.unlink(missing_ok=True)


def test_socket_unlink_requires_the_inspected_identity(tmp_path: Path) -> None:
    socket_path = tmp_path / "daemon-socket"
    moved_path = tmp_path / "moved-socket"
    first = _bind(socket_path)
    second: socket.socket | None = None
    try:
        expected = entry_identity(os.lstat(socket_path))
        os.rename(socket_path, moved_path)
        second = _bind(socket_path)
        with pytest.raises(LocalStateError) as caught:
            unlink_verified_socket(socket_path, expected)
        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
    finally:
        first.close()
        if second is not None:
            second.close()
        socket_path.unlink(missing_ok=True)
        moved_path.unlink(missing_ok=True)


def test_verified_socket_unlink_removes_only_the_expected_socket(tmp_path: Path) -> None:
    socket_path = tmp_path / "daemon-socket"
    listener = _bind(socket_path)
    try:
        expected = entry_identity(os.lstat(socket_path))
        unlink_verified_socket(socket_path, expected)
        assert not socket_path.exists()
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)


def test_config_state_report_and_errors_are_path_free(tmp_path: Path) -> None:
    state_dir = tmp_path / "secret-state-location"
    state_dir.mkdir(mode=0o700)
    db_path = state_dir / "secret-database-name"
    target = state_dir / "secret-target-name"
    target.write_bytes(b"secret-content-value")
    os.chmod(target, 0o600)
    db_path.symlink_to(target)
    socket_path = state_dir / "secret-socket-name"
    socket_path.write_bytes(b"not-a-socket")

    report = inspect_config_state(
        state_dir,
        db_path,
        socket_path=socket_path,
        private_files=(target,),
    )
    rendered = repr(asdict(report))

    assert not report.ok
    assert {issue.code for issue in report.issues} == {LocalStateErrorCode.WRONG_TYPE}
    for forbidden in (
        str(tmp_path),
        state_dir.name,
        db_path.name,
        target.name,
        socket_path.name,
        "secret-content-value",
        "uid",
    ):
        assert forbidden not in rendered
    assert all(not hasattr(result, "path") for result in report.entries)


def test_generic_unlink_type_contract_does_not_follow_symlinks(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    state_dir.mkdir(mode=0o700)
    target = state_dir / "target"
    target.write_bytes(b"unchanged")
    os.chmod(target, 0o600)
    link = state_dir / "link"
    link.symlink_to(target)
    dir_fd = os.open(state_dir, os.O_RDONLY | os.O_DIRECTORY)
    try:
        with pytest.raises(LocalStateError) as caught:
            unlink_verified_entry(
                dir_fd,
                "link",
                entry_identity(os.lstat(link)),
                expected_type=EntryType.REGULAR_FILE,
            )
        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    finally:
        os.close(dir_fd)

    assert link.is_symlink()
    assert target.read_bytes() == b"unchanged"


@pytest.mark.parametrize("unsafe_mode", [0o1777, 0o720, 0o702])
def test_private_socket_parent_rejects_group_or_world_write_without_mutation(
    tmp_path: Path,
    unsafe_mode: int,
) -> None:
    parent = tmp_path / "private-socket-parent"
    parent.mkdir()
    os.chmod(parent, unsafe_mode)
    sentinel = parent / "sentinel"
    sentinel.write_bytes(b"unchanged")
    socket_path = parent / "daemon.sock"

    with pytest.raises(LocalStateError) as caught:
        prepare_private_socket_parent(socket_path)

    assert caught.value.code is LocalStateErrorCode.INSECURE_SOCKET_PARENT
    assert _mode(parent) == unsafe_mode
    assert sentinel.read_bytes() == b"unchanged"
    assert not os.path.lexists(socket_path)
    _assert_path_free(caught.value, parent, sentinel, socket_path)


def test_private_socket_parent_accepts_owned_nonwritable_directory_unchanged(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "private-socket-parent"
    parent.mkdir()
    os.chmod(parent, 0o755)

    result = validate_private_socket_parent(parent / "daemon.sock")

    assert result.state is PermissionState.PRIVATE
    assert result.mode == 0o755
    assert _mode(parent) == 0o755


def test_private_socket_parent_securely_creates_missing_dedicated_directory(
    tmp_path: Path,
) -> None:
    parent = tmp_path / "private-socket-parent"

    result = prepare_private_socket_parent(parent / "daemon.sock")

    assert result.state is PermissionState.CREATED
    assert result.mode == 0o700
    assert _mode(parent) == 0o700


@pytest.mark.parametrize("parent_kind", ["symlink", "regular-file"])
def test_private_socket_parent_rejects_non_directory_without_mutation(
    tmp_path: Path,
    parent_kind: str,
) -> None:
    protected = tmp_path / "protected"
    if parent_kind == "symlink":
        protected.mkdir()
        os.chmod(protected, 0o755)
        parent = tmp_path / "linked-parent"
        parent.symlink_to(protected, target_is_directory=True)
    else:
        protected.write_bytes(b"unchanged")
        parent = protected
    socket_path = parent / "daemon.sock"

    with pytest.raises(LocalStateError) as caught:
        prepare_private_socket_parent(socket_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    if parent_kind == "symlink":
        assert parent.is_symlink()
        assert _mode(protected) == 0o755
    else:
        assert protected.read_bytes() == b"unchanged"
    _assert_path_free(caught.value, parent, protected, socket_path)


def test_private_socket_parent_rejects_wrong_owner_without_chmod(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "wrong-owner-parent"
    parent.mkdir()
    os.chmod(parent, 0o755)
    socket_path = parent / "daemon.sock"
    unexpected_uid = os.geteuid() + 100_000
    monkeypatch.setattr(local_state.os, "geteuid", lambda: unexpected_uid)

    with pytest.raises(LocalStateError) as caught:
        prepare_private_socket_parent(socket_path)

    assert caught.value.code is LocalStateErrorCode.WRONG_OWNER
    assert str(unexpected_uid) not in str(caught.value)
    assert _mode(parent) == 0o755
    assert not os.path.lexists(socket_path)
    _assert_path_free(caught.value, parent, socket_path)


@pytest.mark.parametrize("unsafe_mode", [0o700, 0o730, 0o711])
def test_socket_group_parent_requires_group_search_without_shared_writes(
    tmp_path: Path,
    unsafe_mode: int,
) -> None:
    import grp

    parent = tmp_path / "shared-socket-parent"
    parent.mkdir()
    os.chmod(parent, unsafe_mode)
    socket_path = parent / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name

    with pytest.raises(LocalStateError) as caught:
        validate_socket_group_parent(socket_path, group_name)

    assert caught.value.code is LocalStateErrorCode.INSECURE_SOCKET_PARENT
    assert _mode(parent) == unsafe_mode
    _assert_path_free(caught.value, parent, socket_path)


def test_socket_group_parent_accepts_owned_target_group_directory(
    tmp_path: Path,
) -> None:
    import grp

    parent = tmp_path / "shared-socket-parent"
    parent.mkdir()
    os.chmod(parent, 0o710)
    socket_path = parent / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name

    resolved = validate_socket_group_parent(socket_path, group_name)

    assert resolved.group_id == os.getegid()
    assert _mode(parent) == 0o710


def test_socket_group_parent_rejects_symlink_without_touching_target(
    tmp_path: Path,
) -> None:
    import grp

    target = tmp_path / "protected-target"
    target.mkdir()
    os.chmod(target, 0o710)
    linked_parent = tmp_path / "linked-parent"
    linked_parent.symlink_to(target, target_is_directory=True)
    socket_path = linked_parent / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name

    with pytest.raises(LocalStateError) as caught:
        validate_socket_group_parent(socket_path, group_name)

    assert caught.value.code is LocalStateErrorCode.OPERATION_FAILED
    assert linked_parent.is_symlink()
    assert _mode(target) == 0o710
    assert not (target / "daemon.sock").exists()
    _assert_path_free(caught.value, linked_parent, target, socket_path)


def test_invalid_socket_group_is_rejected_before_parent_lookup_or_mutation(
    tmp_path: Path,
) -> None:
    missing_parent = tmp_path / "missing-parent"
    socket_path = missing_parent / "daemon.sock"
    invalid_group = "tendwire-group-that-does-not-exist-1ea05e79"

    with pytest.raises(LocalStateError) as caught:
        validate_socket_group_parent(socket_path, invalid_group)

    assert caught.value.code is LocalStateErrorCode.INVALID_SOCKET_GROUP
    assert not missing_parent.exists()
    assert invalid_group not in str(caught.value)
    _assert_path_free(caught.value, missing_parent, socket_path)


@pytest.mark.parametrize("explicit_socket", [False, True])
def test_config_state_reports_unsafe_group_parent_without_mutation(
    tmp_path: Path,
    explicit_socket: bool,
) -> None:
    import grp

    state_dir = tmp_path / "private-state"
    state_dir.mkdir()
    os.chmod(state_dir, 0o700)
    socket_path = state_dir / "daemon.sock" if explicit_socket else None
    group_name = grp.getgrgid(os.getegid()).gr_name

    report = inspect_config_state(
        state_dir,
        state_dir / "state.db",
        socket_path=socket_path,
        socket_group=group_name,
    )

    group_issues = [
        issue for issue in report.issues if issue.kind is LocalStateKind.SOCKET_GROUP
    ]
    assert len(group_issues) == 1
    assert group_issues[0].code is LocalStateErrorCode.INSECURE_SOCKET_PARENT
    assert _mode(state_dir) == 0o700
    _assert_path_free(report, state_dir)


def test_pinned_socket_identity_cannot_be_reused_during_verified_cleanup(
    tmp_path: Path,
) -> None:
    socket_path = tmp_path / "pinned-socket"
    moved_path = tmp_path / "moved-pinned-socket"
    first = _bind(socket_path)
    pinned = pin_owned_socket(socket_path)
    assert pinned is not None
    pin_fd, expected = pinned
    second: socket.socket | None = None
    try:
        os.rename(socket_path, moved_path)
        second = _bind(socket_path)

        with pytest.raises(LocalStateError) as caught:
            unlink_verified_socket(socket_path, expected)

        assert caught.value.code is LocalStateErrorCode.ENTRY_CHANGED
        assert stat.S_ISSOCK(os.fstat(pin_fd).st_mode)
        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
    finally:
        os.close(pin_fd)
        first.close()
        if second is not None:
            second.close()
        socket_path.unlink(missing_ok=True)
        moved_path.unlink(missing_ok=True)


def test_pin_owned_socket_rejects_symlink_without_following_target(
    tmp_path: Path,
) -> None:
    target = tmp_path / "target-socket"
    link = tmp_path / "linked-socket"
    listener = _bind(target)
    link.symlink_to(target)
    try:
        with pytest.raises(LocalStateError) as caught:
            pin_owned_socket(link)

        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
        assert link.is_symlink()
        assert stat.S_ISSOCK(os.lstat(target).st_mode)
    finally:
        listener.close()
        link.unlink(missing_ok=True)
        target.unlink(missing_ok=True)


def test_group_client_pin_trusts_correlated_daemon_owner_not_client_uid(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import grp

    parent = tmp_path / "shared-parent"
    parent.mkdir()
    os.chmod(parent, 0o710)
    socket_path = parent / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name
    listener: socket.socket | None = None
    try:
        with socket_bind_umask(group_name):
            listener = _bind(socket_path)
        enforce_bound_socket_permissions(socket_path, socket_group=group_name)
        daemon_owner = os.lstat(socket_path).st_uid

        with monkeypatch.context() as client_process:
            client_process.setattr(
                "tendwire.local_state.os.geteuid",
                lambda: daemon_owner + 100_000,
            )
            pin_fd, identity, expected_peer_uid = pin_group_socket_for_client(
                socket_path,
                group_name,
            )
        try:
            assert expected_peer_uid == daemon_owner
            assert identity == entry_identity(os.fstat(pin_fd))
        finally:
            os.close(pin_fd)
    finally:
        if listener is not None:
            listener.close()
        socket_path.unlink(missing_ok=True)


def test_intermediate_symlink_data_dir_is_refused_without_target_mutation(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected-target"
    target.mkdir()
    sentinel = target / "sentinel"
    sentinel.write_bytes(b"unchanged")
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    requested = linked / "nested" / "state"

    with pytest.raises(LocalStateError) as caught:
        local_state.prepare_private_directory(requested)

    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert sentinel.read_bytes() == b"unchanged"
    assert not (target / "nested").exists()
    _assert_path_free(caught.value, target, linked, requested)


def test_intermediate_symlink_sqlite_family_is_refused_without_target_mutation(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected-target"
    state = target / "state"
    state.mkdir(parents=True)
    db_path = state / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o666)
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    configured = linked / "state" / "tendwire.db"

    for operation in (inspect_sqlite_family, repair_sqlite_family, prepare_sqlite_family):
        with pytest.raises(LocalStateError) as caught:
            operation(configured)
        assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
        _assert_path_free(caught.value, target, linked, configured)

    assert db_path.read_bytes() == b"database"
    assert _mode(db_path) == 0o666


def test_intermediate_symlink_socket_paths_are_refused_without_target_mutation(
    tmp_path: Path,
) -> None:
    import grp

    target = tmp_path / "protected-target"
    target.mkdir()
    shared_parent = target / "shared"
    shared_parent.mkdir()
    os.chmod(shared_parent, 0o710)
    socket_path = shared_parent / "daemon.sock"
    listener = _bind(socket_path)
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    configured = linked / "shared" / "daemon.sock"
    group_name = grp.getgrgid(os.getegid()).gr_name
    try:
        for operation in (inspect_owned_socket, prepare_private_socket_parent):
            with pytest.raises(LocalStateError) as private_error:
                operation(configured)
            assert private_error.value.code is LocalStateErrorCode.WRONG_TYPE
            _assert_path_free(private_error.value, target, linked, configured)

        with pytest.raises(LocalStateError) as group_error:
            validate_socket_group_parent(configured, group_name)
        assert group_error.value.code is LocalStateErrorCode.OPERATION_FAILED

        assert stat.S_ISSOCK(os.lstat(socket_path).st_mode)
        assert _mode(shared_parent) == 0o710
        _assert_path_free(group_error.value, target, linked, configured)
    finally:
        listener.close()
        socket_path.unlink(missing_ok=True)


def test_config_helpers_refuse_intermediate_symlink_before_target_repair(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected-target"
    state = target / "nested" / "state"
    state.mkdir(parents=True)
    os.chmod(state, 0o777)
    db_path = state / "tendwire.db"
    db_path.write_bytes(b"database")
    os.chmod(db_path, 0o666)
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)
    configured_state = linked / "nested" / "state"
    configured_db = configured_state / "tendwire.db"

    report = inspect_config_state(configured_state, configured_db)
    assert not report.ok
    _assert_path_free(report, target, linked, configured_state, configured_db)

    with pytest.raises(LocalStateError) as caught:
        repair_config_state(configured_state, configured_db)
    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert _mode(state) == 0o777
    assert _mode(db_path) == 0o666
    assert db_path.read_bytes() == b"database"
    _assert_path_free(caught.value, target, linked, configured_state, configured_db)


def test_secure_component_creation_handles_absolute_and_controlled_relative_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    absolute_db = tmp_path / "absolute" / "deep" / "state" / "tendwire.db"
    prepared = prepare_sqlite_family(absolute_db)
    assert prepared[0].state is PermissionState.CREATED
    assert absolute_db.is_file()
    for directory in (
        tmp_path / "absolute",
        tmp_path / "absolute" / "deep",
        tmp_path / "absolute" / "deep" / "state",
    ):
        assert _mode(directory) == 0o700

    monkeypatch.chdir(tmp_path)
    relative_socket = Path("relative") / "deep" / "sockets" / "daemon.sock"
    result = prepare_private_socket_parent(relative_socket)
    assert result.state is PermissionState.CREATED
    assert _mode(tmp_path / "relative") == 0o700
    assert _mode(tmp_path / "relative" / "deep") == 0o700
    assert _mode(tmp_path / "relative" / "deep" / "sockets") == 0o700

    parent_fd, leaf = local_state.open_resolved_parent(relative_socket)
    try:
        assert leaf == "daemon.sock"
        assert os.path.samefile(
            f"/proc/self/fd/{parent_fd}",
            tmp_path / "relative" / "deep" / "sockets",
        )
        assert local_state.proc_fd_path(parent_fd, leaf).endswith(
            "/daemon.sock"
        )
    finally:
        os.close(parent_fd)


@pytest.mark.parametrize(
    "configured",
    (
        "state/../outside/value",
        "../outside/value",
        "state/value/",
        "",
    ),
)
def test_component_resolver_rejects_parent_escape_nul_and_empty_leaf(
    tmp_path: Path,
    configured: str,
) -> None:
    path = f"{tmp_path}/{configured}" if configured else ""
    with pytest.raises(LocalStateError) as caught:
        local_state.open_resolved_parent(path)
    assert caught.value.code is LocalStateErrorCode.INVALID_ENTRY_NAME
    _assert_path_free(caught.value, tmp_path)

    with pytest.raises(LocalStateError) as nul_error:
        local_state.open_resolved_parent(f"{tmp_path}/state/\x00/value")
    assert nul_error.value.code is LocalStateErrorCode.INVALID_ENTRY_NAME
    _assert_path_free(nul_error.value, tmp_path)


def test_concurrent_directory_winner_is_reopened_and_verified(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path / "race"
    base.mkdir()
    destination = base / "winner" / "leaf"
    real_mkdir = os.mkdir
    raced = False

    def racing_mkdir(
        name: str | bytes,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal raced
        if name == "winner" and not raced:
            raced = True
            real_mkdir(name, 0o700, dir_fd=dir_fd)
            raise FileExistsError(errno.EEXIST, "concurrent winner")
        real_mkdir(name, mode, dir_fd=dir_fd)

    supported = set(os.supports_dir_fd)
    supported.discard(real_mkdir)
    supported.add(racing_mkdir)
    monkeypatch.setattr(local_state.os, "supports_dir_fd", supported)
    monkeypatch.setattr(local_state.os, "mkdir", racing_mkdir)
    result = local_state.create_private_directory(
        destination,
        create_missing_parents=True,
    )

    assert raced
    assert result.state is PermissionState.CREATED
    assert _mode(base / "winner") == 0o700
    assert _mode(destination) == 0o700


def test_concurrent_non_directory_winner_fails_closed_and_cleans_fds(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    base = tmp_path / "race"
    base.mkdir()
    destination = base / "winner" / "leaf"
    real_mkdir = os.mkdir
    raced = False
    before = set(os.listdir("/proc/self/fd"))

    def racing_mkdir(
        name: str | bytes,
        mode: int = 0o777,
        *,
        dir_fd: int | None = None,
    ) -> None:
        nonlocal raced
        if name == "winner" and not raced:
            raced = True
            fd = os.open(
                name,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                0o600,
                dir_fd=dir_fd,
            )
            os.close(fd)
            raise FileExistsError(errno.EEXIST, "concurrent non-directory")
        real_mkdir(name, mode, dir_fd=dir_fd)

    supported = set(os.supports_dir_fd)
    supported.discard(real_mkdir)
    supported.add(racing_mkdir)
    monkeypatch.setattr(local_state.os, "supports_dir_fd", supported)
    monkeypatch.setattr(local_state.os, "mkdir", racing_mkdir)
    with pytest.raises(LocalStateError) as caught:
        local_state.create_private_directory(
            destination,
            create_missing_parents=True,
        )

    assert raced
    assert caught.value.code is LocalStateErrorCode.WRONG_TYPE
    assert (base / "winner").is_file()
    assert not destination.exists()
    assert set(os.listdir("/proc/self/fd")) == before
    _assert_path_free(caught.value, base, destination)


def test_prepare_and_open_private_directory_returns_exact_created_and_repaired_inode(
    tmp_path: Path,
) -> None:
    created_path = tmp_path / "created" / "deep" / "state"
    created_fd, created = local_state.prepare_and_open_private_directory(created_path)
    try:
        assert created.state is PermissionState.CREATED
        assert created.mode == 0o700
        assert entry_identity(os.fstat(created_fd)) == entry_identity(
            os.lstat(created_path)
        )
        assert stat.S_IMODE(os.fstat(created_fd).st_mode) == 0o700
    finally:
        os.close(created_fd)

    repaired_path = tmp_path / "repaired"
    repaired_path.mkdir()
    os.chmod(repaired_path, 0o777)
    expected = entry_identity(os.lstat(repaired_path))
    repaired_fd, repaired = local_state.prepare_and_open_private_directory(
        repaired_path
    )
    try:
        assert repaired.state is PermissionState.REPAIRED
        assert repaired.mode == 0o700
        assert entry_identity(os.fstat(repaired_fd)) == expected
        assert entry_identity(os.lstat(repaired_path)) == expected
        assert stat.S_IMODE(os.fstat(repaired_fd).st_mode) == 0o700
    finally:
        os.close(repaired_fd)


def test_prepare_resolved_private_parent_returns_exact_parent_and_leaf(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "new" / "deep" / "state" / "tendwire.db"
    parent_fd, leaf, result = local_state.prepare_resolved_private_parent(db_path)
    try:
        assert leaf == "tendwire.db"
        assert result.state is PermissionState.CREATED
        assert result.mode == 0o700
        assert entry_identity(os.fstat(parent_fd)) == entry_identity(
            os.lstat(db_path.parent)
        )
        assert stat.S_IMODE(os.fstat(parent_fd).st_mode) == 0o700
        assert local_state.proc_fd_path(parent_fd, leaf).endswith("/tendwire.db")
    finally:
        os.close(parent_fd)


def test_retained_directory_helpers_refuse_intermediate_symlink_unchanged(
    tmp_path: Path,
) -> None:
    target = tmp_path / "protected-target"
    nested = target / "nested"
    nested.mkdir(parents=True)
    os.chmod(nested, 0o755)
    sentinel = nested / "sentinel"
    sentinel.write_bytes(b"unchanged")
    linked = tmp_path / "linked"
    linked.symlink_to(target, target_is_directory=True)

    with pytest.raises(LocalStateError) as directory_error:
        local_state.prepare_and_open_private_directory(linked / "nested" / "state")
    with pytest.raises(LocalStateError) as parent_error:
        local_state.prepare_resolved_private_parent(
            linked / "nested" / "tendwire.db"
        )

    assert directory_error.value.code is LocalStateErrorCode.WRONG_TYPE
    assert parent_error.value.code is LocalStateErrorCode.WRONG_TYPE
    assert sentinel.read_bytes() == b"unchanged"
    assert _mode(nested) == 0o755
    assert not (nested / "state").exists()
    _assert_path_free(directory_error.value, target, linked)
    _assert_path_free(parent_error.value, target, linked)


def test_proc_fd_path_has_no_non_linux_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent_fd, leaf = local_state.open_resolved_parent(tmp_path / "value")

    try:
        monkeypatch.setattr(local_state.sys, "platform", "not-linux")
        with pytest.raises(LocalStateError) as caught:
            local_state.proc_fd_path(parent_fd, leaf)
        assert caught.value.code is LocalStateErrorCode.UNSUPPORTED_PLATFORM
        _assert_path_free(caught.value, tmp_path)
    finally:
        os.close(parent_fd)


def test_canonical_path_from_fd_tracks_validated_directory_after_rename(
    tmp_path: Path,
) -> None:
    original = tmp_path / "original"
    original.mkdir()
    parent_fd, leaf = local_state.open_resolved_parent(original / "state.db")
    moved = tmp_path / "moved"

    try:
        original.rename(moved)
        resolved = Path(local_state.canonical_path_from_fd(parent_fd, leaf))
        assert resolved == moved / "state.db"
    finally:
        os.close(parent_fd)


def test_path_wrappers_close_every_resolved_component_fd(tmp_path: Path) -> None:
    db_path = tmp_path / "deep" / "state" / "tendwire.db"
    prepare_sqlite_family(db_path)
    socket_path = tmp_path / "deep" / "sockets" / "daemon.sock"
    prepare_private_socket_parent(socket_path)
    before = set(os.listdir("/proc/self/fd"))

    for _ in range(20):
        inspect_sqlite_family(db_path)
        inspect_owned_socket(socket_path)
        inspect_private_directory(db_path.parent)

    assert set(os.listdir("/proc/self/fd")) == before
