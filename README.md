# Spark

Lightning-Fast Cluster Computing - <http://www.spark-project.org/>


## Online Documentation

You can find the latest Spark documentation, including a programming
guide, on the project webpage at <http://spark-project.org/documentation.html>.
This README file only contains basic setup instructions.


## Building

Spark requires Scala 2.9.2 (Scala 2.10 is not yet supported). The project is
built using Simple Build Tool (SBT), which is packaged with it. To build
Spark and its example programs, run:

    sbt/sbt package

Spark also supports building using Maven. If you would like to build using Maven,
see the [instructions for building Spark with Maven](http://spark-project.org/docs/latest/building-with-maven.html)
in the spark documentation..

To run Spark, you will need to have Scala's bin directory in your `PATH`, or
you will need to set the `SCALA_HOME` environment variable to point to where
you've installed Scala. Scala must be accessible through one of these
methods on your cluster's worker nodes as well as its master.

To run one of the examples, use `./run <class> <params>`. For example:

    ./run spark.examples.SparkLR local[2]

will run the Logistic Regression example locally on 2 CPUs.

Each of the example programs prints usage help if no params are given.

All of the Spark samples take a `<host>` parameter that is the cluster URL
to connect to. This can be a mesos:// or spark:// URL, or "local" to run
locally with one thread, or "local[N]" to run locally with N threads.


## A Note About Hadoop Versions

Spark uses the Hadoop core library to talk to HDFS and other Hadoop-supported
storage systems. Because the HDFS API has changed in different versions of
Hadoop, you must build Spark against the same version that your cluster runs.
You can change the version by setting the `HADOOP_VERSION` variable at the top
of `project/SparkBuild.scala`, then rebuilding Spark.


## Configuration

Please refer to the "Configuration" guide in the online documentation for a
full overview on how to configure Spark. At the minimum, you will need to
create a `conf/spark-env.sh` script (copy `conf/spark-env.sh.template`) and
set the following two variables:

- `SCALA_HOME`: Location where Scala is installed.

- `MESOS_NATIVE_LIBRARY`: Your Mesos library (only needed if you want to run
  on Mesos). For example, this might be `/usr/local/lib/libmesos.so` on Linux.


## Contributing to Spark

Contributions via GitHub pull requests are gladly accepted from their original
author. Along with any pull requests, please state that the contribution is
your original work and that you license the work to the project under the
project's open source license. Whether or not you state this explicitly, by
submitting any copyrighted material via pull request, email, or other means
you agree to license the material under the project's open source license and
warrant that you have the legal authority to do so.
