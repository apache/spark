---
layout: global
title: Building Spark
redirect_from: "building-with-maven.html"
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* This will become a table of contents (this text will be scraped).
{:toc}

# Building Apache Spark

## Apache Maven

The Maven-based build is the build of reference for Apache Spark.
Building Spark using Maven requires Maven 3.8.4 and Java 8.
Spark requires Scala 2.12/2.13; support for Scala 2.11 was removed in Spark 3.0.0.

### Setting up Maven's Memory Usage

You'll need to configure Maven to use more memory than usual by setting `MAVEN_OPTS`:

    export MAVEN_OPTS="-Xss64m -Xmx2g -XX:ReservedCodeCacheSize=1g"

(The `ReservedCodeCacheSize` setting is optional but recommended.)
If you don't add these parameters to `MAVEN_OPTS`, you may see errors and warnings like the following:

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_BINARY_VERSION}}/classes...
    [ERROR] Java heap space -> [Help 1]

You can fix these problems by setting the `MAVEN_OPTS` variable as discussed before.

**Note:**

* If using `build/mvn` with no `MAVEN_OPTS` set, the script will automatically add the above options to the `MAVEN_OPTS` environment variable.
* The `test` phase of the Spark build will automatically add these options to `MAVEN_OPTS`, even when not using `build/mvn`.    

### build/mvn

Spark now comes packaged with a self-contained Maven installation to ease building and deployment of Spark from source located under the `build/` directory. This script will automatically download and setup all necessary build requirements ([Maven](https://maven.apache.org/), [Scala](https://www.scala-lang.org/)) locally within the `build/` directory itself. It honors any `mvn` binary if present already, however, will pull down its own copy of Scala regardless to ensure proper version requirements are met. `build/mvn` execution acts as a pass through to the `mvn` call allowing easy transition from previous build methods. As an example, one can build a version of Spark as follows:

    ./build/mvn -DskipTests clean package

Other build examples can be found below.

## Building a Runnable Distribution

To create a Spark distribution like those distributed by the
[Spark Downloads](https://spark.apache.org/downloads.html) page, and that is laid out so as
to be runnable, use `./dev/make-distribution.sh` in the project root directory. It can be configured
with Maven profile settings and so on like the direct Maven build. Example:

    ./dev/make-distribution.sh --name custom-spark --pip --r --tgz -Psparkr -Phive -Phive-thriftserver -Pmesos -Pyarn -Pkubernetes

This will build Spark distribution along with Python pip and R packages. For more information on usage, run `./dev/make-distribution.sh --help`

## Specifying the Hadoop Version and Enabling YARN

You can specify the exact version of Hadoop to compile against through the `hadoop.version` property.

You can enable the `yarn` profile and optionally set the `yarn.version` property if it is different
from `hadoop.version`.

Example:

    ./build/mvn -Pyarn -Dhadoop.version=3.3.0 -DskipTests clean package

If you want to build with Hadoop 2.x, enable `hadoop-2` profile:

    ./build/mvn -Phadoop-2 -Pyarn -Dhadoop.version=2.8.5 -DskipTests clean package

## Building With Hive and JDBC Support

To enable Hive integration for Spark SQL along with its JDBC server and CLI,
add the `-Phive` and `-Phive-thriftserver` profiles to your existing build options.
By default Spark will build with Hive 2.3.9.

    # With Hive 2.3.9 support
    ./build/mvn -Pyarn -Phive -Phive-thriftserver -DskipTests clean package

## Packaging without Hadoop Dependencies for YARN

The assembly directory produced by `mvn package` will, by default, include all of Spark's
dependencies, including Hadoop and some of its ecosystem projects. On YARN deployments, this
causes multiple versions of these to appear on executor classpaths: the version packaged in
the Spark assembly and the version on each node, included with `yarn.application.classpath`.
The `hadoop-provided` profile builds the assembly without including Hadoop-ecosystem projects,
like ZooKeeper and Hadoop itself.

## Building with Mesos support

    ./build/mvn -Pmesos -DskipTests clean package

## Building with Kubernetes support

    ./build/mvn -Pkubernetes -DskipTests clean package

## Building submodules individually

It's possible to build Spark submodules using the `mvn -pl` option.

For instance, you can build the Spark Streaming module using:

    ./build/mvn -pl :spark-streaming_{{site.SCALA_BINARY_VERSION}} clean install

where `spark-streaming_{{site.SCALA_BINARY_VERSION}}` is the `artifactId` as defined in `streaming/pom.xml` file.

## Continuous Compilation

We use the scala-maven-plugin which supports incremental and continuous compilation. E.g.

    ./build/mvn scala:cc

should run continuous compilation (i.e. wait for changes). However, this has not been tested
extensively. A couple of gotchas to note:

* it only scans the paths `src/main` and `src/test` (see
[docs](https://davidb.github.io/scala-maven-plugin/example_cc.html)), so it will only work
from within certain submodules that have that structure.

* you'll typically need to run `mvn install` from the project root for compilation within
specific submodules to work; this is because submodules that depend on other submodules do so via
the `spark-parent` module).

Thus, the full flow for running continuous-compilation of the `core` submodule may look more like:

    $ ./build/mvn install
    $ cd core
    $ ../build/mvn scala:cc

## Building with SBT

Maven is the official build tool recommended for packaging Spark, and is the *build of reference*.
But SBT is supported for day-to-day development since it can provide much faster iterative
compilation. More advanced developers may wish to use SBT.

The SBT build is derived from the Maven POM files, and so the same Maven profiles and variables
can be set to control the SBT build. For example:

    ./build/sbt package

To avoid the overhead of launching sbt each time you need to re-compile, you can launch sbt
in interactive mode by running `build/sbt`, and then run all build commands at the command
prompt.

### Setting up SBT's Memory Usage
Configure the JVM options for SBT in `.jvmopts` at the project root, for example:

    -Xmx2g
    -XX:ReservedCodeCacheSize=1g

For the meanings of these two options, please carefully read the [Setting up Maven's Memory Usage section](https://spark.apache.org/docs/latest/building-spark.html#setting-up-mavens-memory-usage).

## Speeding up Compilation

Developers who compile Spark frequently may want to speed up compilation; e.g., by avoiding re-compilation of the
assembly JAR (for developers who build with SBT).  For more information about how to do this, refer to the
[Useful Developer Tools page](https://spark.apache.org/developer-tools.html#reducing-build-times).

## Encrypted Filesystems

When building on an encrypted filesystem (if your home directory is encrypted, for example), then the Spark build might fail with a "Filename too long" error. As a workaround, add the following in the configuration args of the `scala-maven-plugin` in the project `pom.xml`:

    <arg>-Xmax-classfile-name</arg>
    <arg>128</arg>

and in `project/SparkBuild.scala` add:

    scalacOptions in Compile ++= Seq("-Xmax-classfile-name", "128"),

to the `sharedSettings` val. See also [this PR](https://github.com/apache/spark/pull/2883/files) if you are unsure of where to add these lines.

## IntelliJ IDEA or Eclipse

For help in setting up IntelliJ IDEA or Eclipse for Spark development, and troubleshooting, refer to the
[Useful Developer Tools page](https://spark.apache.org/developer-tools.html).


# Running Tests

Tests are run by default via the [ScalaTest Maven plugin](http://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin).
Note that tests should not be run as root or an admin user.

The following is an example of a command to run the tests:

    ./build/mvn test

## Testing with SBT

The following is an example of a command to run the tests:

    ./build/sbt test

## Running Individual Tests

For information about how to run individual tests, refer to the
[Useful Developer Tools page](https://spark.apache.org/developer-tools.html#running-individual-tests).

## PySpark pip installable

If you are building Spark for use in a Python environment and you wish to pip install it, you will first need to build the Spark JARs as described above. Then you can construct an sdist package suitable for setup.py and pip installable package.

    cd python; python setup.py sdist

**Note:** Due to packaging requirements you can not directly pip install from the Python directory, rather you must first build the sdist package as described above.

Alternatively, you can also run make-distribution with the --pip option.

## PySpark Tests with Maven or SBT

If you are building PySpark and wish to run the PySpark tests you will need to build Spark with Hive support.

    ./build/mvn -DskipTests clean package -Phive
    ./python/run-tests

If you are building PySpark with SBT and wish to run the PySpark tests, you will need to build Spark with Hive support and also build the test components:

    ./build/sbt -Phive clean package
    ./build/sbt test:compile
    ./python/run-tests

The run-tests script also can be limited to a specific Python version or a specific module

    ./python/run-tests --python-executables=python --modules=pyspark-sql

## Running R Tests

To run the SparkR tests you will need to install the [knitr](https://cran.r-project.org/package=knitr), [rmarkdown](https://cran.r-project.org/package=rmarkdown), [testthat](https://cran.r-project.org/package=testthat), [e1071](https://cran.r-project.org/package=e1071) and [survival](https://cran.r-project.org/package=survival) packages first:

    Rscript -e "install.packages(c('knitr', 'rmarkdown', 'devtools', 'testthat', 'e1071', 'survival'), repos='https://cloud.r-project.org/')"

You can run just the SparkR tests using the command:

    ./R/run-tests.sh

## Running Docker-based Integration Test Suites

In order to run Docker integration tests, you have to install the `docker` engine on your box.
The instructions for installation can be found at [the Docker site](https://docs.docker.com/engine/installation/).
Once installed, the `docker` service needs to be started, if not already running.
On Linux, this can be done by `sudo service docker start`.

    ./build/mvn install -DskipTests
    ./build/mvn test -Pdocker-integration-tests -pl :spark-docker-integration-tests_{{site.SCALA_BINARY_VERSION}}

or

    ./build/sbt docker-integration-tests/test

## Change Scala Version

When other versions of Scala like 2.13 are supported, it will be possible to build for that version.
Change the major Scala version using (e.g. 2.13):

    ./dev/change-scala-version.sh 2.13

Enable the profile (e.g. 2.13):

    # For Maven
    ./build/mvn -Pscala-2.13 compile

    # For sbt
    ./build/sbt -Pscala-2.13 compile

## Running Jenkins tests with GitHub Enterprise

To run tests with Jenkins:

    ./dev/run-tests-jenkins

If use an individual repository or a repository on GitHub Enterprise, export below environment variables before running above command.

### Related environment variables

<table class="table">
<tr><th>Variable Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>SPARK_PROJECT_URL</code></td>
  <td>https://github.com/apache/spark</td>
  <td>
    The Spark project URL of GitHub Enterprise.
  </td>
</tr>
<tr>
  <td><code>GITHUB_API_BASE</code></td>
  <td>https://api.github.com/repos/apache/spark</td>
  <td>
    The Spark project API server URL of GitHub Enterprise.
  </td>
</tr>
</table>

### Building and testing on IPv6-only environment

Use Apache Spark GitBox URL because GitHub doesn't support IPv6 yet.

    https://gitbox.apache.org/repos/asf/spark.git

To build and run tests on IPv6-only environment, the following configurations are required.

    export SPARK_LOCAL_HOSTNAME="your-IPv6-address" # e.g. '[2600:1700:232e:3de0:...]'
    export DEFAULT_ARTIFACT_REPOSITORY=https://ipv6.repo1.maven.org/maven2/
    export MAVEN_OPTS="-Djava.net.preferIPv6Addresses=true"
    export SBT_OPTS="-Djava.net.preferIPv6Addresses=true"
    export SERIAL_SBT_TESTS=1
