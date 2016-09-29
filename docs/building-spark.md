---
layout: global
title: Building Spark
redirect_from: "building-with-maven.html"
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Building Apache Spark

## Apache Maven

The Maven-based build is the build of reference for Apache Spark.
Building Spark using Maven requires Maven 3.3.9 or newer and Java 7+.

### Setting up Maven's Memory Usage

You'll need to configure Maven to use more memory than usual by setting `MAVEN_OPTS`:

    export MAVEN_OPTS="-Xmx2g -XX:ReservedCodeCacheSize=512m"

When compiling with Java 7, you will need to add the additional option "-XX:MaxPermSize=512M" to MAVEN_OPTS.

If you don't add these parameters to `MAVEN_OPTS`, you may see errors and warnings like the following:

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_BINARY_VERSION}}/classes...
    [ERROR] PermGen space -> [Help 1]

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_BINARY_VERSION}}/classes...
    [ERROR] Java heap space -> [Help 1]

    [INFO] Compiling 233 Scala sources and 41 Java sources to /Users/me/Development/spark/sql/core/target/scala-{site.SCALA_BINARY_VERSION}/classes...
    OpenJDK 64-Bit Server VM warning: CodeCache is full. Compiler has been disabled.
    OpenJDK 64-Bit Server VM warning: Try increasing the code cache size using -XX:ReservedCodeCacheSize=

You can fix these problems by setting the `MAVEN_OPTS` variable as discussed before.

**Note:**

* If using `build/mvn` with no `MAVEN_OPTS` set, the script will automatically add the above options to the `MAVEN_OPTS` environment variable.
* The `test` phase of the Spark build will automatically add these options to `MAVEN_OPTS`, even when not using `build/mvn`.
* You may see warnings like "ignoring option MaxPermSize=1g; support was removed in 8.0" when building or running tests with Java 8 and `build/mvn`. These warnings are harmless.
    

### build/mvn

Spark now comes packaged with a self-contained Maven installation to ease building and deployment of Spark from source located under the `build/` directory. This script will automatically download and setup all necessary build requirements ([Maven](https://maven.apache.org/), [Scala](http://www.scala-lang.org/), and [Zinc](https://github.com/typesafehub/zinc)) locally within the `build/` directory itself. It honors any `mvn` binary if present already, however, will pull down its own copy of Scala and Zinc regardless to ensure proper version requirements are met. `build/mvn` execution acts as a pass through to the `mvn` call allowing easy transition from previous build methods. As an example, one can build a version of Spark as follows:

    ./build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests clean package

Other build examples can be found below.

## Building a Runnable Distribution

To create a Spark distribution like those distributed by the
[Spark Downloads](http://spark.apache.org/downloads.html) page, and that is laid out so as
to be runnable, use `./dev/make-distribution.sh` in the project root directory. It can be configured
with Maven profile settings and so on like the direct Maven build. Example:

    ./dev/make-distribution.sh --name custom-spark --tgz -Psparkr -Phadoop-2.4 -Phive -Phive-thriftserver -Pmesos -Pyarn

For more information on usage, run `./dev/make-distribution.sh --help`

## Specifying the Hadoop Version

Because HDFS is not protocol-compatible across versions, if you want to read from HDFS, you'll need to build Spark against the specific HDFS version in your environment. You can do this through the `hadoop.version` property. If unset, Spark will build against Hadoop 2.2.0 by default. Note that certain build profiles are required for particular Hadoop versions:

<table class="table">
  <thead>
    <tr><th>Hadoop version</th><th>Profile required</th></tr>
  </thead>
  <tbody>
    <tr><td>2.2.x</td><td>hadoop-2.2</td></tr>
    <tr><td>2.3.x</td><td>hadoop-2.3</td></tr>
    <tr><td>2.4.x</td><td>hadoop-2.4</td></tr>
    <tr><td>2.6.x</td><td>hadoop-2.6</td></tr>
    <tr><td>2.7.x and later 2.x</td><td>hadoop-2.7</td></tr>
  </tbody>
</table>


You can enable the `yarn` profile and optionally set the `yarn.version` property if it is different from `hadoop.version`. Spark only supports YARN versions 2.2.0 and later.

Examples:

    # Apache Hadoop 2.2.X
    ./build/mvn -Pyarn -Phadoop-2.2 -DskipTests clean package

    # Apache Hadoop 2.3.X
    ./build/mvn -Pyarn -Phadoop-2.3 -Dhadoop.version=2.3.0 -DskipTests clean package

    # Apache Hadoop 2.4.X or 2.5.X
    ./build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=VERSION -DskipTests clean package

    # Apache Hadoop 2.6.X
    ./build/mvn -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests clean package

    # Apache Hadoop 2.7.X and later
    ./build/mvn -Pyarn -Phadoop-2.7 -Dhadoop.version=VERSION -DskipTests clean package

    # Different versions of HDFS and YARN.
    ./build/mvn -Pyarn -Phadoop-2.3 -Dhadoop.version=2.3.0 -Dyarn.version=2.2.0 -DskipTests clean package

## Building With Hive and JDBC Support

To enable Hive integration for Spark SQL along with its JDBC server and CLI,
add the `-Phive` and `Phive-thriftserver` profiles to your existing build options.
By default Spark will build with Hive 1.2.1 bindings.

    # Apache Hadoop 2.4.X with Hive 1.2.1 support
    ./build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -Phive -Phive-thriftserver -DskipTests clean package

## Packaging without Hadoop Dependencies for YARN

The assembly directory produced by `mvn package` will, by default, include all of Spark's
dependencies, including Hadoop and some of its ecosystem projects. On YARN deployments, this
causes multiple versions of these to appear on executor classpaths: the version packaged in
the Spark assembly and the version on each node, included with `yarn.application.classpath`.
The `hadoop-provided` profile builds the assembly without including Hadoop-ecosystem projects,
like ZooKeeper and Hadoop itself.

## Building with Mesos support

    ./build/mvn -Pmesos -DskipTests clean package

## Building for Scala 2.10
To produce a Spark package compiled with Scala 2.10, use the `-Dscala-2.10` property:

    ./dev/change-scala-version.sh 2.10
    ./build/mvn -Pyarn -Phadoop-2.4 -Dscala-2.10 -DskipTests clean package

## Building submodules individually

It's possible to build Spark sub-modules using the `mvn -pl` option.

For instance, you can build the Spark Streaming module using:

    ./build/mvn -pl :spark-streaming_2.11 clean install

where `spark-streaming_2.11` is the `artifactId` as defined in `streaming/pom.xml` file.

## Continuous Compilation

We use the scala-maven-plugin which supports incremental and continuous compilation. E.g.

    ./build/mvn scala:cc

should run continuous compilation (i.e. wait for changes). However, this has not been tested
extensively. A couple of gotchas to note:

* it only scans the paths `src/main` and `src/test` (see
[docs](http://scala-tools.org/mvnsites/maven-scala-plugin/usage_cc.html)), so it will only work
from within certain submodules that have that structure.

* you'll typically need to run `mvn install` from the project root for compilation within
specific submodules to work; this is because submodules that depend on other submodules do so via
the `spark-parent` module).

Thus, the full flow for running continuous-compilation of the `core` submodule may look more like:

    $ ./build/mvn install
    $ cd core
    $ ../build/mvn scala:cc

## Speeding up Compilation with Zinc

[Zinc](https://github.com/typesafehub/zinc) is a long-running server version of SBT's incremental
compiler. When run locally as a background process, it speeds up builds of Scala-based projects
like Spark. Developers who regularly recompile Spark with Maven will be the most interested in
Zinc. The project site gives instructions for building and running `zinc`; OS X users can
install it using `brew install zinc`.

If using the `build/mvn` package `zinc` will automatically be downloaded and leveraged for all
builds. This process will auto-start after the first time `build/mvn` is called and bind to port
3030 unless the `ZINC_PORT` environment variable is set. The `zinc` process can subsequently be
shut down at any time by running `build/zinc-<version>/bin/zinc -shutdown` and will automatically
restart whenever `build/mvn` is called.

## Building with SBT

Maven is the official build tool recommended for packaging Spark, and is the *build of reference*.
But SBT is supported for day-to-day development since it can provide much faster iterative
compilation. More advanced developers may wish to use SBT.

The SBT build is derived from the Maven POM files, and so the same Maven profiles and variables
can be set to control the SBT build. For example:

    ./build/sbt -Pyarn -Phadoop-2.3 package

To avoid the overhead of launching sbt each time you need to re-compile, you can launch sbt
in interactive mode by running `build/sbt`, and then run all build commands at the command
prompt. For more recommendations on reducing build time, refer to the
[wiki page](https://cwiki.apache.org/confluence/display/SPARK/Useful+Developer+Tools#UsefulDeveloperTools-ReducingBuildTimes).

## Encrypted Filesystems

When building on an encrypted filesystem (if your home directory is encrypted, for example), then the Spark build might fail with a "Filename too long" error. As a workaround, add the following in the configuration args of the `scala-maven-plugin` in the project `pom.xml`:

    <arg>-Xmax-classfile-name</arg>
    <arg>128</arg>

and in `project/SparkBuild.scala` add:

    scalacOptions in Compile ++= Seq("-Xmax-classfile-name", "128"),

to the `sharedSettings` val. See also [this PR](https://github.com/apache/spark/pull/2883/files) if you are unsure of where to add these lines.

## IntelliJ IDEA or Eclipse

For help in setting up IntelliJ IDEA or Eclipse for Spark development, and troubleshooting, refer to the
[wiki page for IDE setup](https://cwiki.apache.org/confluence/display/SPARK/Useful+Developer+Tools#UsefulDeveloperTools-IDESetup).


# Running Tests

Tests are run by default via the [ScalaTest Maven plugin](http://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin). All tests should not be run by `root` or any admin user.

Some of the tests require Spark to be packaged first, so always run `mvn package` with `-DskipTests` the first time.  The following is an example of a correct (build, test) sequence:

    ./build/mvn -Pyarn -Phadoop-2.3 -DskipTests -Phive -Phive-thriftserver clean package
    ./build/mvn -Pyarn -Phadoop-2.3 -Phive -Phive-thriftserver test

The ScalaTest plugin also supports running only a specific Scala test suite as follows:

    ./build/mvn -P... -Dtest=none -DwildcardSuites=org.apache.spark.repl.ReplSuite test
    ./build/mvn -P... -Dtest=none -DwildcardSuites=org.apache.spark.repl.* test

or a Java test:

    ./build/mvn test -P... -DwildcardSuites=none -Dtest=org.apache.spark.streaming.JavaAPISuite

## Testing with SBT

Some of the tests require Spark to be packaged first, so always run `build/sbt package` the first time.  The following is an example of a correct (build, test) sequence:

    ./build/sbt -Pyarn -Phadoop-2.3 -Phive -Phive-thriftserver package
    ./build/sbt -Pyarn -Phadoop-2.3 -Phive -Phive-thriftserver test

To run only a specific test suite as follows:

    ./build/sbt -Pyarn -Phadoop-2.3 -Phive -Phive-thriftserver "test-only org.apache.spark.repl.ReplSuite"
    ./build/sbt -Pyarn -Phadoop-2.3 -Phive -Phive-thriftserver "test-only org.apache.spark.repl.*"

To run test suites of a specific sub project as follows:

    ./build/sbt -Pyarn -Phadoop-2.3 -Phive -Phive-thriftserver core/test

## Running Java 8 Test Suites

Running only Java 8 tests and nothing else.

    ./build/mvn install -DskipTests
    ./build/mvn -pl :java8-tests_2.11 test

or

    ./build/sbt java8-tests/test

Java 8 tests are automatically enabled when a Java 8 JDK is detected.
If you have JDK 8 installed but it is not the system default, you can set JAVA_HOME to point to JDK 8 before running the tests.

## PySpark Tests with Maven

If you are building PySpark and wish to run the PySpark tests you will need to build Spark with Hive support.

    ./build/mvn -DskipTests clean package -Phive
    ./python/run-tests

The run-tests script also can be limited to a specific Python version or a specific module

    ./python/run-tests --python-executables=python --modules=pyspark-sql

**Note:** You can also run Python tests with an sbt build, provided you build Spark with Hive support.

## Running R Tests

To run the SparkR tests you will need to install the R package `testthat`
(run `install.packages(testthat)` from R shell).  You can run just the SparkR tests using
the command:

    ./R/run-tests.sh

## Running Docker-based Integration Test Suites

In order to run Docker integration tests, you have to install the `docker` engine on your box.
The instructions for installation can be found at [the Docker site](https://docs.docker.com/engine/installation/).
Once installed, the `docker` service needs to be started, if not already running.
On Linux, this can be done by `sudo service docker start`.

    ./build/mvn install -DskipTests
    ./build/mvn -Pdocker-integration-tests -pl :spark-docker-integration-tests_2.11

or

    ./build/sbt docker-integration-tests/test
