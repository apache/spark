---
layout: global
title: Spark Configuration
---
# Spark Configuration

Spark is configured primarily through the `conf/spark-env.sh` script. This script doesn't exist in the Git repository, but you can create it by copying `conf/spark-env.sh.template`. Make sure the script is executable.

Inside this script, you can set several environment variables:

* `SCALA_HOME` to point to your Scala installation.
* `MESOS_NATIVE_LIBRARY` if you are [[running on a Mesos cluster|Running Spark on Mesos]].
* `SPARK_MEM` to set the amount of memory used per node (this should be in the same format as the JVM's -Xmx option, e.g. `300m` or `1g`)
* `SPARK_JAVA_OPTS` to add JVM options. This includes system properties that you'd like to pass with `-D`.
* `SPARK_CLASSPATH` to add elements to Spark's classpath.
* `SPARK_LIBRARY_PATH` to add search directories for native libraries.

The `spark-env.sh` script is executed both when you submit jobs with `run`, when you start the interpreter with `spark-shell`, and on each worker node on a Mesos cluster to set up the environment for that worker.

The most important thing to set first will probably be the memory (`SPARK_MEM`). Make sure you set it high enough to be able to run your job but lower than the total memory on the machines (leave at least 1 GB for the operating system).

## Logging Configuration

Spark uses [[log4j|http://logging.apache.org/log4j/]] for logging. You can configure it by adding a `log4j.properties` file in the `conf` directory. One way to start is to copy the existing `log4j.properties.template` located there.
