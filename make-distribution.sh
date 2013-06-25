#!/bin/bash
#
# Script to create a binary distribution for easy deploys of Spark.
# The distribution directory defaults to dist/ but can be overridden below.
# The distribution contains fat (assembly) jars that include the Scala library,
# so it is completely self contained.
# It does not contain source or *.class files.
#
# Recommended deploy/testing procedure (standalone mode):
# 1) Rsync / deploy the dist/ dir to one host
# 2) cd to deploy dir; ./bin/start-master.sh
# 3) Verify master is up by visiting web page, ie http://master-ip:8080.  Note the spark:// URL.
# 4) ./bin/start-slave.sh 1 <<spark:// URL>>
# 5) MASTER="spark://my-master-ip:7077" ./spark-shell

# Figure out where the Spark framework is installed
FWDIR="$(cd `dirname $0`; pwd)"
DISTDIR="$FWDIR/dist"

# Get version from SBT
VERSION=$($FWDIR/sbt/sbt "show version" | tail -1 | cut -f 2)
echo "Making distribution for Spark $VERSION in $DISTDIR..."

# Build fat JAR
$FWDIR/sbt/sbt "repl/assembly"

# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/jars"
echo "$VERSION" >$DISTDIR/RELEASE

# Copy jars
cp $FWDIR/repl/target/*.jar "$DISTDIR/jars/"

# Copy other things
cp -r "$FWDIR/bin" "$DISTDIR"
cp -r "$FWDIR/conf" "$DISTDIR"
cp "$FWDIR/run" "$FWDIR/spark-shell" "$DISTDIR"