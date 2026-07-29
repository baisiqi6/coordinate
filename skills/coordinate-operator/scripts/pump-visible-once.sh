#!/usr/bin/env bash
set -euo pipefail

REPO="${MAC_REPO:-${COORDINATE_REPO:-$HOME/projects/coordinate}}"
DB="${MAC_DB:-${MULTI_AGENT_COORDINATOR_DB:-$REPO/data/coordinator.sqlite3}}"
WORKSPACE=""
PLATFORM="stdout"
DESTINATION="local"
LIMIT="20"

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
    --platform)
      PLATFORM="$2"
      shift 2
      ;;
    --destination)
      DESTINATION="$2"
      shift 2
      ;;
    --limit)
      LIMIT="$2"
      shift 2
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if [[ -z "$WORKSPACE" ]]; then
  echo "--workspace is required" >&2
  exit 2
fi

MAC="$REPO/skills/coordinate-operator/scripts/mac.sh"

MAC_REPO="$REPO" MAC_DB="$DB" "$MAC" policy pump-events \
  --workspace-id "$WORKSPACE" \
  --platform "$PLATFORM" \
  --destination "$DESTINATION" \
  --limit "$LIMIT"

MAC_REPO="$REPO" MAC_DB="$DB" "$MAC" worker delivery \
  --platform "$PLATFORM" \
  --once \
  --limit "$LIMIT"
