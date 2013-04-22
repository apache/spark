---
layout: global
title: Building Spark with Maven
---

* This will become a table of contents (this text will be scraped).
{:toc}

Building Spark using Maven Requires Maven 3 (the build process is tested with Maven 3.0.4) and Java 1.6 or newer.

Building with Maven requires that a Hadoop profile be specified explicitly at the command line, there is no default. There are two profiles to choose from, one for building for Hadoop 1 or Hadoop 2.

for Hadoop 1 (using 0.20.205.0) use:

    $ mvn -Phadoop1 clean install


for Hadoop 2 (using 2.0.0-mr1-cdh4.1.1) use:

    $ mvn -Phadoop2 clean install

It uses the scala-maven-plugin which supports incremental and continuous compilation. E.g.

    $ mvn -Phadoop2 scala:cc

…should run continuous compilation (i.e. wait for changes). However, this has not been tested extensively.

## Spark Tests in Maven ##

Tests are run by default via the scalatest-maven-plugin. With this you can do things like:

Skip test execution (but not compilation):

    $ mvn -DskipTests -Phadoop2 clean install

To run a specific test suite:

    $ mvn -Phadoop2 -Dsuites=spark.repl.ReplSuite test


## Setting up JVM Memory Usage Via Maven ##

You might run into the following errors if you're using a vanilla installation of Maven:

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_VERSION}}/classes...
    [ERROR] PermGen space -> [Help 1]

    [INFO] Compiling 203 Scala sources and 9 Java sources to /Users/me/Development/spark/core/target/scala-{{site.SCALA_VERSION}}/classes...
    [ERROR] Java heap space -> [Help 1]

To fix these, you can do the following:

    export MAVEN_OPTS="-Xmx1024m -XX:MaxPermSize=128M"


## Using With IntelliJ IDEA ##

This setup works fine in IntelliJ IDEA 11.1.4. After opening the project via the pom.xml file in the project root folder, you only need to activate either the hadoop1 or hadoop2 profile in the "Maven Properties" popout. We have not tried Eclipse/Scala IDE with this.

## Building Spark Debian Packages ##

It includes support for building a Debian package containing a 'fat-jar' which includes the repl, the examples and bagel. This can be created by specifying the deb profile:

    $ mvn -Phadoop2,deb clean install

The debian package can then be found under repl/target. We added the short commit hash to the file name so that we can distinguish individual packages build for SNAPSHOT versions.
