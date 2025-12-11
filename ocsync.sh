#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage:
  $(basename "$0") (--version <tag-or-hash> | --latest-sha) [dep_dir] [importer_dir]

Examples:
  # Use latest commit SHA from Opencost
  $(basename "$0") --latest-sha

  # Use explicit version/tag/hash
  $(basename "$0") --version v1.118.0
EOF
}

MODE=""
TARGET_REF=""     # This will be either an explicit version or a SHA
OPENCOST_DIR=""
AGENT_DIR="./"

# ---- argument parsing ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      MODE="version"
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --version requires an argument" >&2
        usage
        exit 1
      fi
      TARGET_REF="$2"
      shift 2
      ;;
    --latest-sha)
      MODE="latest_sha"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      if [[ -z "$OPENCOST_DIR" ]]; then
        OPENCOST_DIR="$1"
      elif [[ -z "$AGENT_DIR" ]]; then
        AGENT_DIR="$1"
      else
        echo "ERROR: Unexpected argument: $1" >&2
        usage
        exit 1
      fi
      shift
      ;;
  esac
done

# Validate mode
if [[ -z "$MODE" ]]; then
  echo "ERROR: You must specify either --version or --latest-sha" >&2
  usage
  exit 1
fi

if [[ "$MODE" == "version" && -z "$TARGET_REF" ]]; then
  echo "ERROR: --version was specified but no version provided" >&2
  usage
  exit 1
fi

# Defaults for directories
OPENCOST_DIR="${OPENCOST_DIR:-../opencost}"
AGENT_DIR="${AGENT_DIR:-.}"

check_clean() {
  local dir="$1"
  if ! git -C "$dir" diff --quiet || ! git -C "$dir" diff --cached --quiet; then
    echo "ERROR: Repository '$dir' has unstaged or uncommitted changes." >&2
    echo "Please commit or stash them before running this script." >&2
    exit 1
  fi
}

# Ensure directories exist
[[ -d "$OPENCOST_DIR" ]] || { echo "Opencost directory '$OPENCOST_DIR' not found" >&2; exit 1; }
[[ -d "$AGENT_DIR" ]] || { echo "Agent directory '$AGENT_DIR' not found" >&2; exit 1; }

# Safety check: both repos must be clean
check_clean "$OPENCOST_DIR"
check_clean "$AGENT_DIR"

# Determine what ref we are updating to
if [[ "$MODE" == "latest_sha" ]]; then
  TARGET_REF="$(git -C "$OPENCOST_DIR" rev-parse HEAD)"
fi

echo "Updating agent go.mod to use $MODE $TARGET_REF:"

(
  cd "$AGENT_DIR"

  # Update dependency to explicit version or SHA
  echo "  go get github.com/opencost/opencost@${TARGET_REF}"
  go get github.com/opencost/opencost@${TARGET_REF}
  echo "  go get github.com/opencost/opencost/core@${TARGET_REF}"
  go get github.com/opencost/opencost/core@${TARGET_REF}
  echo "  go get github.com/opencost/opencost/modules/collector-source@${TARGET_REF}"
  go get github.com/opencost/opencost/modules/collector-source@${TARGET_REF}
  echo "  go get github.com/opencost/opencost/modules/prometheus-source@${TARGET_REF}"
  go get github.com/opencost/opencost/modules/prometheus-source@${TARGET_REF}
  echo "  go mod tidy"
  go mod tidy

  # Commit only if there are changes
  if ! git diff --quiet; then
    #git commit -am "Update ${MODULE_PATH} to ${TARGET_REF}"
    echo "Ready to commit dependency chage to ${TARGET_REF}."
  else
    echo "No changes to commit — already at ${TARGET_REF}."
  fi
)
