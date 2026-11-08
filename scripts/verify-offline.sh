#!/bin/sh
set -eu
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$project_dir"
mvn -q -o -DskipTests package
mvn -q -o -Dtest=WatchEventChangefeedTest,ByteUtilTest,FdbPathTest test
cd ../zk-conformance
mvn -q -o -DskipTests package
