#!/usr/bin/env bash
set -euo pipefail

REPO="${MAC_REPO:-${COORDINATE_REPO:-$HOME/projects/coordinate}}"
DB="${MAC_DB:-${MULTI_AGENT_COORDINATOR_DB:-$REPO/data/coordinator.sqlite3}}"
WORKSPACE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      REPO="$2"
      shift 2
      ;;
    --db)
      DB="$2"
      shift 2
      ;;
    --workspace)
      WORKSPACE="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

MAC="$REPO/skills/coordinate-operator/scripts/mac.sh"

echo "repo: $REPO"
echo "db: $DB"
echo

MAC_REPO="$REPO" MAC_DB="$DB" "$MAC" workspace list
echo

if [[ -n "$WORKSPACE" ]]; then
  MAC_REPO="$REPO" MAC_DB="$DB" "$MAC" event list --workspace-id "$WORKSPACE"
  echo
  MAC_REPO="$REPO" MAC_DB="$DB" "$MAC" job list --workspace-id "$WORKSPACE"
else
  MAC_REPO="$REPO" MAC_DB="$DB" "$MAC" event list
  echo
  MAC_REPO="$REPO" MAC_DB="$DB" "$MAC" job list
fi
echo

echo "deliveries (all ledger records; platform=none is audit-only, not send backlog):"
MAC_REPO="$REPO" MAC_DB="$DB" "$MAC" delivery list
