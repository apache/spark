---
layout: global
title: Building Spark with Maven
---

* This will become a table of contents (this text will be scraped).
{:toc}

Building Spark using Maven Requires Maven 3 (the build process is tested with Maven 3.0.4) and Java 1.6 or newer.


## Setting up Maven's Memory Usage ##

You'll need to configure Maven to use more memory than usual by setting `MAVEN_OPTS`. We recommend the following settings:

    export MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

If you don't run this, you may see errors like the following:

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_BINARY_VERSION}}/classes...
    [ERROR] PermGen space -> [Help 1]

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_BINARY_VERSION}}/classes...
    [ERROR] Java heap space -> [Help 1]

You can fix this by setting the `MAVEN_OPTS` variable as discussed before.

## Specifying the Hadoop version ##

Because HDFS is not protocol-compatible across versions, if you want to read from HDFS, you'll need to build Spark against the specific HDFS version in your environment. You can do this through the "hadoop.version" property. If unset, Spark will build against Hadoop 1.0.4 by default.

For Apache Hadoop versions 1.x, Cloudera CDH MRv1, and other Hadoop versions without YARN, use:

    # Apache Hadoop 1.2.1
    $ mvn -Dhadoop.version=1.2.1 -DskipTests clean package

    # Cloudera CDH 4.2.0 with MapReduce v1
    $ mvn -Dhadoop.version=2.0.0-mr1-cdh4.2.0 -DskipTests clean package

For Apache Hadoop 2.x, 0.23.x, Cloudera CDH MRv2, and other Hadoop versions with YARN, you should enable the "yarn-alpha" or "yarn" profile and set the "hadoop.version", "yarn.version" property:

    # Apache Hadoop 2.0.5-alpha
    $ mvn -Pyarn-alpha -Dhadoop.version=2.0.5-alpha -Dyarn.version=2.0.5-alpha -DskipTests clean package

    # Cloudera CDH 4.2.0 with MapReduce v2
    $ mvn -Pyarn-alpha -Dhadoop.version=2.0.0-cdh4.2.0 -Dyarn.version=2.0.0-chd4.2.0 -DskipTests clean package

    # Apache Hadoop 2.2.X ( e.g. 2.2.0 as below ) and newer
    $ mvn -Pyarn -Dhadoop.version=2.2.0 -Dyarn.version=2.2.0 -DskipTests clean package

## Spark Tests in Maven ##

Tests are run by default via the [ScalaTest Maven plugin](http://www.scalatest.org/user_guide/using_the_scalatest_maven_plugin). Some of the require Spark to be packaged first, so always run `mvn package` with `-DskipTests` the first time. You can then run the tests with `mvn -Dhadoop.version=... test`.

The ScalaTest plugin also supports running only a specific test suite as follows:

    $ mvn -Dhadoop.version=... -Dsuites=spark.repl.ReplSuite test


## Continuous Compilation ##

We use the scala-maven-plugin which supports incremental and continuous compilation. E.g.

    $ mvn scala:cc

should run continuous compilation (i.e. wait for changes). However, this has not been tested extensively.

## Using With IntelliJ IDEA ##

This setup works fine in IntelliJ IDEA 11.1.4. After opening the project via the pom.xml file in the project root folder, you only need to activate either the hadoop1 or hadoop2 profile in the "Maven Properties" popout. We have not tried Eclipse/Scala IDE with this.

## Building Spark Debian Packages ##

The maven build includes support for building a Debian package containing the assembly 'fat-jar', PySpark, and the necessary scripts and configuration files. This can be created by specifying the following:

    $ mvn -Pdeb -DskipTests clean package

The debian package can then be found under assembly/target. We added the short commit hash to the file name so that we can distinguish individual packages built for SNAPSHOT versions.
