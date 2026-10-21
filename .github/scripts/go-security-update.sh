#!/bin/bash
set -euo pipefail

# Applies Go security fixes to the Go module in the current directory.
# Prints each change as a markdown list item, which the workflow uses as the PR description.

# Prints the Go version in go.mod, e.g. "1.27.3".
go_mod_version() {
    awk '$1 == "go" { print $2 }' go.mod
}

# Succeeds if version $1 is older than version $2, e.g. is_older v0.59.0 v0.60.0.
is_older() {
    [ "$1" != "$2" ] && [ "$(printf '%s\n%s\n' "$1" "$2" | sort -V | head -n 1)" = "$1" ]
}

# 1. Upgrade Go. Standard library vulnerabilities are fixed by new Go releases.

current_go=$(go_mod_version)

# The newest stable Go release, e.g. "1.27.3".
latest_go=$(curl -fsSL 'https://go.dev/dl/?mode=json&include=all' |
    jq -r '.[] | select(.stable) | .version | ltrimstr("go")' |
    sort -V | tail -n 1)

if is_older "$current_go" "$latest_go"; then
    # The go line in go.mod sets the Go version the module builds with.
    go mod edit -go="$latest_go"
    echo "- Go $current_go -> $latest_go"
fi

# The Dockerfile's golang image builds the agent, so keep it equal to go.mod.
# (-i.bak works with both GNU and macOS sed.)
sed -i.bak "s/^FROM golang:[^ ]*-alpine/FROM golang:$(go_mod_version)-alpine/" Dockerfile
rm Dockerfile.bak

# 2. Upgrade vulnerable modules.

# govulncheck checks the standard library of the Go that runs it, so run it on the go.mod version.
GOTOOLCHAIN="go$(go_mod_version)"
export GOTOOLCHAIN

# For each vulnerability our code calls, print "<module> <fixed_version>",
# e.g. "golang.org/x/net v0.60.0". The standard library is skipped: the Go upgrade above fixes it.
# (Unpinned: the vulnerability database is always live anyway, and only the latest
# govulncheck is guaranteed to support the newest Go release.)
go run golang.org/x/vuln/cmd/govulncheck@latest -format json ./... |
    jq -r '.finding // empty
        | select(.trace[0].function and .trace[0].module != "stdlib" and .fixed_version)
        | "\(.trace[0].module) \(.fixed_version)"' |
    sort --unique --reverse --version-sort | # highest fix first, so lower fixes for the same module are skipped
    while read -r module fixed_version; do
        current=$(go list -m -f '{{.Version}}' "$module")
        # An earlier go get may already have upgraded this module past the fix. Skip it so we never downgrade.
        if is_older "$current" "$fixed_version"; then
            go get "$module@$fixed_version"
            echo "- $module $current -> $fixed_version"
        fi
    done

# go get leaves stale go.sum entries and can misplace requirements between the direct and
# indirect blocks; tidy cleans both up so the PR matches what a developer would commit.
go mod tidy
