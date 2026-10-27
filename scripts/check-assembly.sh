#!/usr/bin/env bash
# Packaging check for the online assembly jar (task 15.1).
# Usage: scripts/check-assembly.sh [path-to-assembly.jar]
# With no argument it checks every target/scala-*/aerospike-chronon-assembly-*.jar.
# The jar must contain com/aerospike/chronon/ and com/aerospike/client/ classes
# and nothing under ai/chronon/, org/apache/spark/, ch/qos/logback/ or scala/
# (those come from the host Chronon JVM and its Spark runtime).
set -euo pipefail

check_jar() {
  local jar="$1"
  [ -f "$jar" ] || { echo "FAIL: $jar does not exist"; return 1; }
  local listing
  listing="$(unzip -l "$jar" | awk '{print $4}')"
  local ok=1
  for required in 'com/aerospike/chronon/' 'com/aerospike/client/'; do
    if ! grep -q "$required" <<<"$listing"; then
      echo "FAIL: $jar is missing $required"; ok=0
    fi
  done
  for forbidden in 'ai/chronon/' 'org/apache/spark/' 'ch/qos/logback/' 'scala/'; do
    if grep -qE "(^|[[:space:]])${forbidden}" <<<"$listing"; then
      echo "FAIL: $jar contains forbidden package $forbidden"; ok=0
    fi
  done
  if [ "$ok" -eq 1 ]; then
    echo "PASS: $jar"
  else
    return 1
  fi
}

status=0
if [ "$#" -ge 1 ]; then
  check_jar "$1" || status=1
else
  found=0
  for jar in "$(dirname "$0")"/../target/scala-*/aerospike-chronon-assembly-*.jar; do
    [ -e "$jar" ] || continue
    found=1
    check_jar "$jar" || status=1
  done
  [ "$found" -eq 1 ] || { echo "FAIL: no assembly jars found (run sbt +assembly first)"; exit 1; }
fi
exit "$status"
