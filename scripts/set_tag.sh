#!/bin/bash

set -eu
set -o pipefail

cd "$( cd "$( dirname "${BASH_SOURCE[0]:-${(%):-%x}}" )" >/dev/null 2>&1 && pwd )"
cd ..

#git tag -d $1 || true
#git push --delete origin $1 || true
#git tag -a $1 -m "DuckPGQ '$1'"
#git push --tags

cd duckdb-pgq

git tag -d $1 || true
git push --delete origin $1 || true
git tag -a $1 -m "DuckPGQ '$1'"
git push --tags