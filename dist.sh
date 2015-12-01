#!/usr/bin/env bash

# Run after sbt assembly..

# Figure out where the Spark framework is installed
SPARK_HOME="$(cd "`dirname "$0"`"; pwd)"
DISTDIR="$SPARK_HOME/dist"


# Make directories
rm -rf "$DISTDIR"
mkdir -p "$DISTDIR/lib"
echo "Spark $VERSION$GITREVSTRING built for Hadoop $SPARK_HADOOP_VERSION" > "$DISTDIR/RELEASE"
echo "Build flags: $@" >> "$DISTDIR/RELEASE"

# Copy jars
cp "$SPARK_HOME"/assembly/target/scala*/*assembly*hadoop*.jar "$DISTDIR/lib/"
cp "$SPARK_HOME"/examples/target/scala*/spark-examples*.jar "$DISTDIR/lib/"
# This will fail if the -Pyarn profile is not provided
# In this case, silence the error and ignore the return code of this command
cp "$SPARK_HOME"/network/yarn/target/scala*/spark-*-yarn-shuffle.jar "$DISTDIR/lib/" &> /dev/null || :

# Copy example sources (needed for python and SQL)
mkdir -p "$DISTDIR/examples/src/main"
cp -r "$SPARK_HOME"/examples/src/main "$DISTDIR/examples/src/"

# Copy Hive files.
cp "$SPARK_HOME"/lib_managed/jars/datanucleus*.jar "$DISTDIR/lib/"

# Copy license and ASF files
cp "$SPARK_HOME/LICENSE" "$DISTDIR"
cp "$SPARK_HOME/NOTICE" "$DISTDIR"

if [ -e "$SPARK_HOME"/CHANGES.txt ]; then
  cp "$SPARK_HOME/CHANGES.txt" "$DISTDIR"
fi

# Copy data files
cp -r "$SPARK_HOME/data" "$DISTDIR"

# Copy other things
mkdir "$DISTDIR"/conf
cp "$SPARK_HOME"/conf/*.template "$DISTDIR"/conf
cp "$SPARK_HOME/README.md" "$DISTDIR"
cp -r "$SPARK_HOME/bin" "$DISTDIR"
cp -r "$SPARK_HOME/python" "$DISTDIR"
cp -r "$SPARK_HOME/sbin" "$DISTDIR"
cp -r "$SPARK_HOME/ec2" "$DISTDIR"
# Copy SparkR if it exists
if [ -d "$SPARK_HOME"/R/lib/SparkR ]; then
  mkdir -p "$DISTDIR"/R/lib
  cp -r "$SPARK_HOME/R/lib/SparkR" "$DISTDIR"/R/lib
fi
