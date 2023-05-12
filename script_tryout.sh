#!/bin/zsh

cd duckdb-pgq
git fetch --tags
export DUCKDB_VERSION=`git tag --points-at HEAD`
echo $DUCKDB_VERSION
export DUCKDB_VERSION=${DUCKDB_VERSION:=`git log -1 --format=%h`}
echo $DUCKDB_VERSION
cd ..
echo $GITHUB_REF $DUCKDB_VERSION $BUCKET_NAME
echo $AWS_ACCESS_KEY_ID $AWS_SECRET_ACCESS_KEY $AWS_DEFAULT_REGION $BUCKET_NAME
echo ^(refs/tags/v.+)$ = "$GITHUB_REF"
if [ "$GITHUB_REF" =~ ^(refs/tags/v.+)$ ] ; then
  echo 'got in first if statement'
elif [ "$GITHUB_REF" =~ ^(refs/heads/main)$ ] ; then
  echo 'got in second if statement'
fi