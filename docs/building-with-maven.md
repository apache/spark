---
layout: global
title: Building Spark with Maven
---

* This will become a table of contents (this text will be scraped).
{:toc}

Building Spark using Maven Requires Maven 3 (the build process is tested with Maven 3.0.4) and Java 1.6 or newer.

## Specifying the Hadoop version ##

To enable support for HDFS and other Hadoop-supported storage systems, specify the exact Hadoop version by setting the "hadoop.version" property. If unset, Spark will build against Hadoop 1.0.4 by default.

For Apache Hadoop versions 1.x, Cloudera CDH MRv1, and other Hadoop versions without YARN, use:

    # Apache Hadoop 1.2.1
    $ mvn -Dhadoop.version=1.2.1 clean package

    # Cloudera CDH 4.2.0 with MapReduce v1
    $ mvn -Dhadoop.version=2.0.0-mr1-cdh4.2.0 clean package

For Apache Hadoop 2.x, 0.23.x, Cloudera CDH MRv2, and other Hadoop versions with YARN, enable the "hadoop2-yarn" profile:

    # Apache Hadoop 2.0.5-alpha
    $ mvn -Phadoop2-yarn -Dhadoop.version=2.0.5-alpha clean package

    # Cloudera CDH 4.2.0 with MapReduce v2
    $ mvn -Phadoop2-yarn -Dhadoop.version=2.0.0-cdh4.2.0 clean package


## Spark Tests in Maven ##

Tests are run by default via the scalatest-maven-plugin. With this you can do things like:

Skip test execution (but not compilation):

    $ mvn -Dhadoop.version=... -DskipTests clean package

To run a specific test suite:

    $ mvn -Dhadoop.version=... -Dsuites=spark.repl.ReplSuite test


## Setting up JVM Memory Usage Via Maven ##

You might run into the following errors if you're using a vanilla installation of Maven:

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_VERSION}}/classes...
    [ERROR] PermGen space -> [Help 1]

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_VERSION}}/classes...
    [ERROR] Java heap space -> [Help 1]

To fix these, you can do the following:

    export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128M"


## Continuous Compilation ##

We use the scala-maven-plugin which supports incremental and continuous compilation. E.g.

    $ mvn scala:cc

…should run continuous compilation (i.e. wait for changes). However, this has not been tested extensively.


## Using With IntelliJ IDEA ##

This setup works fine in IntelliJ IDEA 11.1.4. After opening the project via the pom.xml file in the project root folder, you only need to activate either the hadoop1 or hadoop2 profile in the "Maven Properties" popout. We have not tried Eclipse/Scala IDE with this.

## Building Spark Debian Packages ##

It includes support for building a Debian package containing a 'fat-jar' which includes the repl, the examples and bagel. This can be created by specifying the following profiles:

    $ mvn -Prepl-bin -Pdeb clean package

The debian package can then be found under repl/target. We added the short commit hash to the file name so that we can distinguish individual packages build for SNAPSHOT versions.
