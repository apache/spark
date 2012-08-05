# Spark

Lightning-Fast Cluster Computing - <http://www.spark-project.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the project wiki at <http://github.com/mesos/spark/wiki>. This
file only contains basic setup instructions.


## Building

Spark requires Scala 2.9.1. This version has been tested with 2.9.1.final.

The project is built using Simple Build Tool (SBT), which is packaged with it.
To build Spark and its example programs, run:

    sbt/sbt compile

To run Spark, you will need to have Scala's bin in your `PATH`, or you
will need to set the `SCALA_HOME` environment variable to point to where
you've installed Scala. Scala must be accessible through one of these
methods on Mesos slave nodes as well as on the master.

To run one of the examples, first run `sbt/sbt package` to create a JAR with
the example classes. Then use `./run <class> <params>`. For example:

    ./run spark.examples.SparkLR local[2]

will run the Logistic Regression example locally on 2 CPUs.

Each of the example programs prints usage help if no params are given.

All of the Spark samples take a `<host>` parameter that is the Mesos master
to connect to. This can be a Mesos URL, or "local" to run locally with one
thread, or "local[N]" to run locally with N threads.


## A Note About Hadoop

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the HDFS API has changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.
You can change the version by setting the `HADOOP_VERSION` variable at the top
of `project/SparkBuild.scala`, then rebuilding Spark.


## Configuration

Spark can be configured through two files: `conf/java-opts` and
`conf/spark-env.sh`.

In `java-opts`, you can add flags to be passed to the JVM when running Spark.

In `spark-env.sh`, you can set any environment variables you wish to be available
when running Spark programs, such as `PATH`, `SCALA_HOME`, etc. There are also
several Spark-specific variables you can set:

- `SPARK_CLASSPATH`: Extra entries to be added to the classpath, separated by ":".

- `SPARK_MEM`: Memory for Spark to use, in the format used by java's `-Xmx`
  option (for example, `-Xmx200m` means 200 MB, `-Xmx1g` means 1 GB, etc).

- `SPARK_LIBRARY_PATH`: Extra entries to add to `java.library.path` for locating
  shared libraries.

- `SPARK_JAVA_OPTS`: Extra options to pass to JVM.

- `MESOS_NATIVE_LIBRARY`: Your Mesos library, if you want to run on a Mesos
  cluster. For example, this might be `/usr/local/lib/libmesos.so` on Linux.

Note that `spark-env.sh` must be a shell script (it must be executable and start
with a `#!` header to specify the shell to use).
