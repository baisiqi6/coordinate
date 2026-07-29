#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_DIR"

ENV_FILE="${COORDINATOR_DAEMON_ENV:-$REPO_DIR/.coordinator/daemon.env}"
PID_FILE="$REPO_DIR/.coordinator/coordinator-daemon.pid"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "error: missing coordinator daemon env file: $ENV_FILE" >&2
  exit 1
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

: "${COORDINATOR_BOT_TOKEN:?COORDINATOR_BOT_TOKEN is required}"
: "${COORDINATOR_CHANNEL_ID:?COORDINATOR_CHANNEL_ID is required}"

python_bin="${COORDINATOR_PYTHON_BIN:-$(command -v python3)}"
db_path="${MULTI_AGENT_COORDINATOR_DB:-data/coordinator.sqlite3}"
pump_interval="${COORDINATOR_PUMP_INTERVAL:-30}"

export COORDINATOR_BOT_TOKEN
export COORDINATOR_CHANNEL_ID
export COORDINATOR_ALLOWED_USER_IDS="${COORDINATOR_ALLOWED_USER_IDS:-}"
export COORDINATOR_GIT_SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
export PYTHONPATH="$REPO_DIR/src${PYTHONPATH:+:$PYTHONPATH}"

mkdir -p "$(dirname "$PID_FILE")"
echo "$$" > "$PID_FILE"

exec "$python_bin" -m coordinate \
  --db "$db_path" \
  serve \
  --pump-interval "$pump_interval"
