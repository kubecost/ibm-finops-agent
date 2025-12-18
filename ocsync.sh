#!/usr/bin/env bash
set -euo pipefail

# To be able to run `ocsync` from this directory, copy this shell script to ibm-finops-agent/bin
# directory, give it executable permission, and then add ibm-finops-agent/bin dir
# to your PATH:
#
#   mkdir -p bin && cp ./ocsync.sh ./bin/ocsync && chmod +x ./bin/ocsync
#
# Then add the following to ~/.bashrc or ~/.zshrc:
#
#   export PATH=/path/to/model/ibm-finops-agent/bin:$PATH
#
# And, lastly, refresh your terminal with `source ~/.bashrc` or `source ~/.zshrc`

usage() {
  cat <<EOF
Usage:
  $(basename "$0") [flags]

Options:
  -v, --version <tag-or-hash>   Update dependency to an explicit Opencost version/tag/hash
  -l, --latest-sha              Update dependency to the latest Opencost commit SHA
  -c, --commit                  Commit changes to the agent repository after updating
  -h, --help                    Show this help message

Examples:
  # Use latest commit SHA from Opencost and commit
  $(basename "$0") -l -c

  # Use explicit version/tag/hash from Opencost, but do not commit
  $(basename "$0") -v v1.118.0
EOF
}

MODE=""
TARGET_REF=""    # This will be either an explicit version or a SHA
OPENCOST_DIR=""
AGENT_DIR="./"
DO_COMMIT=0      # default: do NOT commit automatically

# ---- argument parsing ----
while [[ $# -gt 0 ]]; do
  case "$1" in
    --commit|-c)
      DO_COMMIT=1
      shift
      ;;
    --version|-v)
      MODE="version"
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --version requires an argument" >&2
        usage
        exit 1
      fi
      TARGET_REF="$2"
      shift 2
      ;;
    --latest-sha|-l)
      MODE="SHA"
      shift
      ;;
    --help|-h)
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
if [[ "$MODE" == "SHA" ]]; then
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

  # If there are changes, decide whether to commit
  if ! git diff --quiet; then
    echo "Changes detected."

    if [[ "$DO_COMMIT" -eq 1 ]]; then
      git commit -am "Update Opencost dependencies to ${TARGET_REF}"
      echo "Committed Opencost dependency update."
    else
      echo "NOTE: --commit not passed; not committing changes."
    fi
  else
    echo "No changes to commit — already at ${TARGET_REF}."
  fi
)
