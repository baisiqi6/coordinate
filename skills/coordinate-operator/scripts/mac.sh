#!/usr/bin/env bash
set -euo pipefail

REPO="${MAC_REPO:-${COORDINATE_REPO:-$HOME/projects/coordinate}}"
DB="${MAC_DB:-${MULTI_AGENT_COORDINATOR_DB:-$REPO/data/coordinator.sqlite3}}"

# Pick a Python interpreter that has the runtime's dependencies installed
# (notably `python-dotenv` after 7.1.1-coord-dotenv). Resolution order:
#   1. $COORDINATOR_PYTHON_BIN if set in the shell or via sourced .env
#   2. $REPO/.venv/bin/python (repo-managed venv, if present)
#   3. whatever `python3` resolves to on $PATH (may be missing project
#      dependencies; prefer the repo venv or an explicit override)
COORDSH_PY="${COORDINATOR_PYTHON_BIN:-}"
if [ -z "$COORDSH_PY" ] || [ ! -x "$COORDSH_PY" ]; then
    if [ -x "$REPO/.venv/bin/python" ]; then
        COORDSH_PY="$REPO/.venv/bin/python"
    else
        COORDSH_PY="python3"
    fi
fi

cd "$REPO"
exec env PYTHONPATH=src "$COORDSH_PY" -m coordinate --db "$DB" "$@"
