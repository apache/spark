# A BASH script to set the classpath for running Spark out of the developer/github tree

SCALA_VERSION=2.9.3

# Figure out where the Scala framework is installed
FWDIR="$(cd `dirname $0`; pwd)"

if [ "$SPARK_LAUNCH_WITH_SCALA" == "1" ]; then
  if [ "$SCALA_HOME" ]; then
    RUNNER="${SCALA_HOME}/bin/scala"
  else
    if [ `command -v scala` ]; then
      RUNNER="scala"
    else
      echo "SCALA_HOME is not set and scala is not in PATH" >&2
      exit 1
    fi
  fi
else
  if [ `command -v java` ]; then
    RUNNER="java"
  else
    if [ -z "$JAVA_HOME" ]; then
      echo "JAVA_HOME is not set" >&2
      exit 1
    fi
    RUNNER="${JAVA_HOME}/bin/java"
  fi
  if [ -z "$SCALA_LIBRARY_PATH" ]; then
    if [ -z "$SCALA_HOME" ]; then
      echo "SCALA_HOME is not set" >&2
      exit 1
    fi
    SCALA_LIBRARY_PATH="$SCALA_HOME/lib"
  fi
fi

CORE_DIR="$FWDIR/core"
REPL_DIR="$FWDIR/repl"
REPL_BIN_DIR="$FWDIR/repl-bin"
EXAMPLES_DIR="$FWDIR/examples"
BAGEL_DIR="$FWDIR/bagel"
STREAMING_DIR="$FWDIR/streaming"
PYSPARK_DIR="$FWDIR/python"

# Exit if the user hasn't compiled Spark
if [ ! -e "$CORE_DIR/target" ]; then
  echo "Failed to find Spark classes in $CORE_DIR/target" >&2
  echo "You need to compile Spark before running this program" >&2
  exit 1
fi

if [[ "$@" = *repl* && ! -e "$REPL_DIR/target" ]]; then
  echo "Failed to find Spark classes in $REPL_DIR/target" >&2
  echo "You need to compile Spark repl module before running this program" >&2
  exit 1
fi

# Build up classpath
CLASSPATH="$SPARK_CLASSPATH"
CLASSPATH="$CLASSPATH:$FWDIR/conf"
CLASSPATH="$CLASSPATH:$CORE_DIR/target/scala-$SCALA_VERSION/classes"
if [ -n "$SPARK_TESTING" ] ; then
  CLASSPATH="$CLASSPATH:$CORE_DIR/target/scala-$SCALA_VERSION/test-classes"
  CLASSPATH="$CLASSPATH:$STREAMING_DIR/target/scala-$SCALA_VERSION/test-classes"
fi
CLASSPATH="$CLASSPATH:$CORE_DIR/src/main/resources"
CLASSPATH="$CLASSPATH:$REPL_DIR/target/scala-$SCALA_VERSION/classes"
CLASSPATH="$CLASSPATH:$EXAMPLES_DIR/target/scala-$SCALA_VERSION/classes"
CLASSPATH="$CLASSPATH:$STREAMING_DIR/target/scala-$SCALA_VERSION/classes"
CLASSPATH="$CLASSPATH:$STREAMING_DIR/lib/org/apache/kafka/kafka/0.7.2-spark/*" # <-- our in-project Kafka Jar
if [ -e "$FWDIR/lib_managed" ]; then
  CLASSPATH="$CLASSPATH:$FWDIR/lib_managed/jars/*"
  CLASSPATH="$CLASSPATH:$FWDIR/lib_managed/bundles/*"
fi
CLASSPATH="$CLASSPATH:$REPL_DIR/lib/*"
if [ -e $REPL_BIN_DIR/target ]; then
  for jar in `find "$REPL_BIN_DIR/target" -name 'spark-repl-*-shaded-hadoop*.jar'`; do
    CLASSPATH="$CLASSPATH:$jar"
  done
fi
CLASSPATH="$CLASSPATH:$BAGEL_DIR/target/scala-$SCALA_VERSION/classes"
for jar in `find $PYSPARK_DIR/lib -name '*jar'`; do
  CLASSPATH="$CLASSPATH:$jar"
done

# Figure out the JAR file that our examples were packaged into. This includes a bit of a hack
# to avoid the -sources and -doc packages that are built by publish-local.
if [ -e "$EXAMPLES_DIR/target/scala-$SCALA_VERSION/spark-examples"*[0-9T].jar ]; then
  # Use the JAR from the SBT build
  export SPARK_EXAMPLES_JAR=`ls "$EXAMPLES_DIR/target/scala-$SCALA_VERSION/spark-examples"*[0-9T].jar`
fi
if [ -e "$EXAMPLES_DIR/target/spark-examples-"*hadoop[12].jar ]; then
  # Use the JAR from the Maven build
  export SPARK_EXAMPLES_JAR=`ls "$EXAMPLES_DIR/target/spark-examples-"*hadoop[12].jar`
fi

# Figure out whether to run our class with java or with the scala launcher.
# In most cases, we'd prefer to execute our process with java because scala
# creates a shell script as the parent of its Java process, which makes it
# hard to kill the child with stuff like Process.destroy(). However, for
# the Spark shell, the wrapper is necessary to properly reset the terminal
# when we exit, so we allow it to set a variable to launch with scala.
if [ "$SPARK_LAUNCH_WITH_SCALA" == "1" ]; then
  EXTRA_ARGS=""     # Java options will be passed to scala as JAVA_OPTS
else
  CLASSPATH="$CLASSPATH:$SCALA_LIBRARY_PATH/scala-library.jar"
  CLASSPATH="$CLASSPATH:$SCALA_LIBRARY_PATH/scala-compiler.jar"
  CLASSPATH="$CLASSPATH:$SCALA_LIBRARY_PATH/jline.jar"
  # The JVM doesn't read JAVA_OPTS by default so we need to pass it in
  EXTRA_ARGS="$JAVA_OPTS"
fi
