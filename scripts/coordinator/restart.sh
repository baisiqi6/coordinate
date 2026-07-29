#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_DIR"

PID_FILE="$REPO_DIR/.coordinator/coordinator-daemon.pid"
ENV_FILE="${COORDINATOR_DAEMON_ENV:-$REPO_DIR/.coordinator/daemon.env}"
LOG_DIR="$REPO_DIR/.coordinator/logs"
OUT_LOG="$LOG_DIR/coordinator-daemon.log"
ERR_LOG="$LOG_DIR/coordinator-daemon.err.log"
LAUNCHD_DIR="$REPO_DIR/.coordinator/launchd"
SERVICE_LABEL="local.coordinate.daemon"
PLIST_FILE="$LAUNCHD_DIR/$SERVICE_LABEL.plist"
LAUNCHD_DOMAIN="gui/$(id -u)"

mkdir -p "$LOG_DIR"

is_daemon_pid() {
  local pid="$1"
  ps -p "$pid" -o command= 2>/dev/null | grep -q -- "-m coordinate .*serve"
}

pid_cwd() {
  local pid="$1"
  lsof -a -p "$pid" -d cwd -Fn 2>/dev/null | awk 'substr($0,1,1)=="n"{print substr($0,2); exit}'
}

find_daemon_pid() {
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

shell_quote() {
  printf "%q" "$1"
}

write_env_from_pid() {
  local pid="$1"
  local command python_bin db_path pump_interval token channel allowed tmp

  command="$(ps -p "$pid" -o command=)"
  python_bin="$(awk '{print $1; exit}' <<< "$command")"
  db_path="$(arg_from_command "$command" "--db")"
  pump_interval="$(arg_from_command "$command" "--pump-interval")"
  token="$(env_from_pid "$pid" COORDINATOR_BOT_TOKEN)"
  channel="$(env_from_pid "$pid" COORDINATOR_CHANNEL_ID)"
  allowed="$(env_from_pid "$pid" COORDINATOR_ALLOWED_USER_IDS)"

  if [[ -z "$token" || -z "$channel" ]]; then
    echo "error: cannot capture required daemon env from pid $pid" >&2
    return 1
  fi

  tmp="$ENV_FILE.tmp"
  {
    echo "# Private coordinator daemon env. Do not commit."
    echo "# Generated from running daemon pid $pid."
    echo "COORDINATOR_PYTHON_BIN=$(shell_quote "${python_bin:-$(command -v python3)}")"
    echo "MULTI_AGENT_COORDINATOR_DB=$(shell_quote "${db_path:-data/coordinator.sqlite3}")"
    echo "COORDINATOR_PUMP_INTERVAL=$(shell_quote "${pump_interval:-30}")"
    echo "COORDINATOR_CHANNEL_ID=$(shell_quote "$channel")"
    echo "COORDINATOR_ALLOWED_USER_IDS=$(shell_quote "$allowed")"
    echo "COORDINATOR_BOT_TOKEN=$(shell_quote "$token")"
  } > "$tmp"
  chmod 600 "$tmp"
  mv "$tmp" "$ENV_FILE"
  echo "captured env_file: $ENV_FILE (token hidden)"
}

current_pid="$(find_daemon_pid || true)"

if [[ ! -f "$ENV_FILE" ]]; then
  if [[ -n "$current_pid" ]]; then
    write_env_from_pid "$current_pid"
  else
    cat >&2 <<EOF
error: missing env file: $ENV_FILE

Create it with:
  COORDINATOR_BOT_TOKEN=...
  COORDINATOR_CHANNEL_ID=...
  COORDINATOR_ALLOWED_USER_IDS=...
  MULTI_AGENT_COORDINATOR_DB=data/coordinator.sqlite3
  COORDINATOR_PUMP_INTERVAL=30
EOF
    exit 1
  fi
fi

# shellcheck disable=SC1090
source "$ENV_FILE"

: "${COORDINATOR_BOT_TOKEN:?COORDINATOR_BOT_TOKEN is required}"
: "${COORDINATOR_CHANNEL_ID:?COORDINATOR_CHANNEL_ID is required}"

db_path="${MULTI_AGENT_COORDINATOR_DB:-data/coordinator.sqlite3}"
pump_interval="${COORDINATOR_PUMP_INTERVAL:-30}"

write_launchd_plist() {
  mkdir -p "$LAUNCHD_DIR"
  cat > "$PLIST_FILE" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>$SERVICE_LABEL</string>
  <key>ProgramArguments</key>
  <array>
    <string>/bin/bash</string>
    <string>$REPO_DIR/scripts/coordinator/run-daemon.sh</string>
  </array>
  <key>WorkingDirectory</key>
  <string>$REPO_DIR</string>
  <key>StandardOutPath</key>
  <string>$OUT_LOG</string>
  <key>StandardErrorPath</key>
  <string>$ERR_LOG</string>
  <key>KeepAlive</key>
  <true/>
  <key>ThrottleInterval</key>
  <integer>30</integer>
  <key>EnvironmentVariables</key>
  <dict>
    <key>COORDINATOR_DAEMON_ENV</key>
    <string>$ENV_FILE</string>
  </dict>
</dict>
</plist>
EOF
}

write_launchd_plist
plutil -lint "$PLIST_FILE" >/dev/null

echo "bootstrapping coordinator daemon service=$SERVICE_LABEL db=$db_path channel=$COORDINATOR_CHANNEL_ID pump_interval=$pump_interval"
launchctl bootout "$LAUNCHD_DOMAIN/$SERVICE_LABEL" >/dev/null 2>&1 || true

current_pid="$(find_daemon_pid || true)"
if [[ -n "$current_pid" ]]; then
  echo "stopping unmanaged pid $current_pid"
  kill "$current_pid"
  for _ in {1..20}; do
    if ! ps -p "$current_pid" >/dev/null 2>&1; then
      break
    fi
    sleep 0.5
  done
  if ps -p "$current_pid" >/dev/null 2>&1; then
    echo "pid $current_pid did not stop after 10s; sending SIGKILL" >&2
    kill -KILL "$current_pid"
  fi
fi

launchctl bootstrap "$LAUNCHD_DOMAIN" "$PLIST_FILE"
launchctl kickstart -kp "$LAUNCHD_DOMAIN/$SERVICE_LABEL" >/dev/null

sleep 4

new_pid="$(cat "$PID_FILE" 2>/dev/null || true)"
if [[ -z "$new_pid" ]] || ! is_daemon_pid "$new_pid"; then
  echo "error: daemon failed to stay running; stderr tail:" >&2
  tail -n 40 "$ERR_LOG" >&2 || true
  exit 1
fi

"$SCRIPT_DIR/status.sh"
