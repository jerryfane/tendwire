#!/usr/bin/env bash
# Hermetic approximation of systemd KillMode=control-group shutdown.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
PYTHON=${PYTHON:-python3}
TMPDIR_ROOT=$(mktemp -d "${TMPDIR:-/tmp}/tendwired-lifecycle.XXXXXX")
LEADER_PID=""

cleanup() {
    if [[ -n "$LEADER_PID" ]]; then
        kill -KILL -- "-$LEADER_PID" 2>/dev/null || true
        wait "$LEADER_PID" 2>/dev/null || true
    fi
    rm -rf "$TMPDIR_ROOT"
}
trap cleanup EXIT INT TERM

if ! command -v setsid >/dev/null 2>&1; then
    echo "tendwired lifecycle smoke requires setsid" >&2
    exit 77
fi

ADAPTER="$TMPDIR_ROOT/herdr-dummy"
cat >"$ADAPTER" <<'PY'
#!/usr/bin/env python3
import json

print(json.dumps({"result": []}))
PY
chmod 700 "$ADAPTER"

DATA_DIR="$TMPDIR_ROOT/data"
SOCKET_PATH="$DATA_DIR/tendwire.sock"
CHILD_PID_FILE="$TMPDIR_ROOT/dummy-child.pid"
mkdir -p "$DATA_DIR"

# The shell becomes the service MainPID via exec after creating a supervised
# dummy child. Both remain in the same fresh process group, just as they remain
# in one systemd service cgroup with KillMode=control-group.
PYTHONPATH="$ROOT/src${PYTHONPATH:+:$PYTHONPATH}" \
TENDWIRE_DATA_DIR="$DATA_DIR" \
TENDWIRE_HERDR_BIN="$ADAPTER" \
TENDWIRE_HERDR_TIMEOUT_SECONDS=1 \
setsid sh -c '
    sleep 300 &
    printf "%s\n" "$!" >"$1"
    exec "$2" -m tendwire.cli --socket-path "$3" daemon --db-path "$4"
' tendwired-smoke "$CHILD_PID_FILE" "$PYTHON" "$SOCKET_PATH" "$DATA_DIR/tendwire.db" \
    >"$TMPDIR_ROOT/daemon.stdout" 2>"$TMPDIR_ROOT/daemon.stderr" &
LEADER_PID=$!

for _attempt in $(seq 1 200); do
    if [[ -S "$SOCKET_PATH" && -s "$CHILD_PID_FILE" ]]; then
        break
    fi
    if ! kill -0 "$LEADER_PID" 2>/dev/null; then
        echo "tendwired exited before publishing its socket" >&2
        sed -n '1,120p' "$TMPDIR_ROOT/daemon.stderr" >&2
        exit 1
    fi
    sleep 0.05
done

if [[ ! -S "$SOCKET_PATH" || ! -s "$CHILD_PID_FILE" ]]; then
    echo "tendwired did not become ready" >&2
    exit 1
fi
DUMMY_PID=$(tr -d '[:space:]' <"$CHILD_PID_FILE")

# This is the process-group equivalent of systemd sending SIGTERM to every
# process in the unit's control group.
kill -TERM -- "-$LEADER_PID"

for _attempt in $(seq 1 200); do
    if ! kill -0 "$LEADER_PID" 2>/dev/null; then
        break
    fi
    sleep 0.05
done

if kill -0 "$LEADER_PID" 2>/dev/null; then
    echo "tendwired survived service-group SIGTERM" >&2
    exit 1
fi
wait "$LEADER_PID"
LEADER_PID=""

for _attempt in $(seq 1 40); do
    if ! kill -0 "$DUMMY_PID" 2>/dev/null; then
        break
    fi
    sleep 0.05
done

if kill -0 "$DUMMY_PID" 2>/dev/null; then
    echo "supervised dummy child survived service-group SIGTERM" >&2
    exit 1
fi
if [[ -e "$SOCKET_PATH" || -L "$SOCKET_PATH" ]]; then
    echo "daemon socket survived service shutdown" >&2
    exit 1
fi

echo "tendwired lifecycle smoke: ok"
