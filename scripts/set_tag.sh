#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
cd ..

cd duckdb

TAG_NAME="$1"
REMOTE_NAME="cwida" # Explicitly define the target remote

echo "--- Updating tag '$TAG_NAME' on remote '$REMOTE_NAME' ---"

# 1. Delete the remote tag on your fork, ignoring errors if it doesn't exist.
git push --delete "$REMOTE_NAME" "$TAG_NAME" || true

# 2. Force-delete the local tag to ensure it can be recreated.
git tag -d "$TAG_NAME" || true

# 3. Create the new annotated tag locally on your current commit.
# The '-f' flag will force replacement if it somehow still exists.
git tag -fa "$TAG_NAME" -m "DuckPGQ custom tag '$TAG_NAME'"

# 4. Force-push the new tag to your fork, overwriting the remote one if it exists.
git push "$REMOTE_NAME" --tags --force

echo "--- Successfully updated tag '$TAG_NAME' on remote '$REMOTE_NAME' ---"