#!/bin/bash

FWDIR="$(cd `dirname $0`; pwd)"

# Find the Spark JAR
for jar in `find $FWDIR/core/target -name '*assembly*.jar'`; do
  SPARK_JAR="$jar"
done
export SPARK_JAR

# Find the Scala JAR

exec "./run" "$@"