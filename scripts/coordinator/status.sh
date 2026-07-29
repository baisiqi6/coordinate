#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_DIR"

PID_FILE="$REPO_DIR/.coordinator/coordinator-daemon.pid"
ENV_FILE="${COORDINATOR_DAEMON_ENV:-$REPO_DIR/.coordinator/daemon.env}"
SERVICE_LABEL="local.coordinate.daemon"
LAUNCHD_DOMAIN="gui/$(id -u)"

launchctl_pid() {
  launchctl print "$LAUNCHD_DOMAIN/$SERVICE_LABEL" 2>/dev/null \
    | awk -F'= ' '/^[[:space:]]*pid = /{print $2; exit}'
}

launchctl_state() {
  launchctl print "$LAUNCHD_DOMAIN/$SERVICE_LABEL" 2>/dev/null \
    | awk -F'= ' '/^[[:space:]]*state = /{print $2; exit}'
}

is_daemon_pid() {
  local pid="$1"
  ps -p "$pid" -o command= 2>/dev/null | grep -q -- "-m coordinate .*serve"
}

pid_cwd() {
  local pid="$1"
  lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | awk 'substr($0,1,1)=="n"{print substr($0,2); exit}'
}

find_daemon_pid() {
  local launch_pid
  launch_pid="$(launchctl_pid || true)"
  if [[ -n "$launch_pid" ]] && is_daemon_pid "$launch_pid"; then
    echo "$launch_pid"
    return 0
  fi

  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && is_daemon_pid "$pid"; then
      echo "$pid"
      return 0
    fi
  fi

  while IFS= read -r pid; do
    [[ -n "$pid" ]] || continue
    if [[ "$(pid_cwd "$pid")" == "$REPO_DIR" ]] && is_daemon_pid "$pid"; then
      echo "$pid"
      return 0
    fi
  done < <(pgrep -f "coordinate.*serve" 2>/dev/null || true)
  return 1
}

env_from_pid() {
  local pid="$1"
  local name="$2"
  ps eww -p "$pid" 2>/dev/null \
    | tr ' ' '\n' \
    | awk -F= -v key="$name" '$1 == key {sub(/^[^=]*=/, ""); print; exit}'
}

arg_from_command() {
  local command="$1"
  local flag="$2"
  awk -v flag="$flag" '{
    for (i = 1; i <= NF; i++) {
      if ($i == flag && i < NF) {
        print $(i + 1)
        exit
      }
    }
  }' <<< "$command"
}

repo_sha="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
branch="$(git branch --show-current 2>/dev/null || echo unknown)"
dirty_count="$(git status --short 2>/dev/null | wc -l | tr -d ' ')"

pid="$(find_daemon_pid || true)"

echo "Coordinator daemon status"
echo "repo: $REPO_DIR"
echo "repo_git_sha: $repo_sha"
echo "branch: ${branch:-unknown}"
echo "dirty_files: $dirty_count"
echo "env_file: $ENV_FILE"
echo "launchd_service: $SERVICE_LABEL"
echo "launchd_state: $(launchctl_state || echo unloaded)"

if [[ -z "$pid" ]]; then
  echo "status: stopped"
  if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    echo "configured_db: ${MULTI_AGENT_COORDINATOR_DB:-data/coordinator.sqlite3}"
    echo "configured_channel: ${COORDINATOR_CHANNEL_ID:-unset}"
    echo "configured_token: $([[ -n "${COORDINATOR_BOT_TOKEN:-}" ]] && echo present || echo missing)"
  fi
  exit 3
fi

command="$(ps -p "$pid" -o command=)"
started="$(ps -p "$pid" -o lstart= | sed 's/^ *//')"
elapsed="$(ps -p "$pid" -o etime= | sed 's/^ *//')"
process_sha="$(env_from_pid "$pid" COORDINATOR_GIT_SHA)"
channel_id="$(env_from_pid "$pid" COORDINATOR_CHANNEL_ID)"
allowed_users="$(env_from_pid "$pid" COORDINATOR_ALLOWED_USER_IDS)"
token="$(env_from_pid "$pid" COORDINATOR_BOT_TOKEN)"
db_path="$(arg_from_command "$command" "--db")"
pump_interval="$(arg_from_command "$command" "--pump-interval")"

echo "status: running"
echo "pid: $pid"
echo "started: $started"
echo "elapsed: $elapsed"
echo "process_git_sha: ${process_sha:-unknown}"
echo "db: ${db_path:-unknown}"
echo "channel: ${channel_id:-unknown}"
echo "allowed_users: ${allowed_users:-unset}"
echo "pump_interval: ${pump_interval:-unknown}"
echo "token: $([[ -n "$token" ]] && echo present || echo missing)"
echo "stdout_log: $REPO_DIR/.coordinator/logs/coordinator-daemon.log"
echo "stderr_log: $REPO_DIR/.coordinator/logs/coordinator-daemon.err.log"
