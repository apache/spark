---
layout: global
title: Third-Party Hadoop Distributions
---

Spark can run against all versions of Cloudera's Distribution Including Apache Hadoop (CDH) and
the Hortonworks Data Platform (HDP). There are a few things to keep in mind when using Spark
with these distributions:

# Compile-time Hadoop Version

When compiling Spark, you'll need to specify the Hadoop version by defining the `hadoop.version`
property. For certain versions, you will need to specify additional profiles. For more detail,
see the guide on [building with maven](building-spark.html#specifying-the-hadoop-version):

    mvn -Dhadoop.version=1.0.4 -DskipTests clean package
    mvn -Phadoop-2.2 -Dhadoop.version=2.2.0 -DskipTests clean package

The table below lists the corresponding `hadoop.version` code for each CDH/HDP release. Note that
some Hadoop releases are binary compatible across client versions. This means the pre-built Spark
distribution may "just work" without you needing to compile. That said, we recommend compiling with
the _exact_ Hadoop version you are running to avoid any compatibility errors.

<table>
  <tr valign="top">
    <td>
      <h3>CDH Releases</h3>
      <table class="table" style="width:350px; margin-right: 20px;">
        <tr><th>Release</th><th>Version code</th></tr>
        <tr><td>CDH 4.X.X (YARN mode)</td><td>2.0.0-cdh4.X.X</td></tr>
        <tr><td>CDH 4.X.X</td><td>2.0.0-mr1-cdh4.X.X</td></tr>
      </table>
    </td>
    <td>
      <h3>HDP Releases</h3>
      <table class="table" style="width:350px;">
        <tr><th>Release</th><th>Version code</th></tr>
        <tr><td>HDP 1.3</td><td>1.2.0</td></tr>
        <tr><td>HDP 1.2</td><td>1.1.2</td></tr>
        <tr><td>HDP 1.1</td><td>1.0.3</td></tr>
        <tr><td>HDP 1.0</td><td>1.0.3</td></tr>
        <tr><td>HDP 2.0</td><td>2.2.0</td></tr>
      </table>
    </td>
  </tr>
</table>

In SBT, the equivalent can be achieved by setting the the `hadoop.version` property:

    build/sbt -Dhadoop.version=1.0.4 assembly

# Linking Applications to the Hadoop Version

In addition to compiling Spark itself against the right version, you need to add a Maven dependency on that
version of `hadoop-client` to any Spark applications you run, so they can also talk to the HDFS version
on the cluster. If you are using CDH, you also need to add the Cloudera Maven repository.
This looks as follows in SBT:

{% highlight scala %}
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "<version>"

// If using CDH, also add Cloudera repo
resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
{% endhighlight %}

Or in Maven:

{% highlight xml %}
<project>
  <dependencies>
    ...
    <dependency>
      <groupId>org.apache.hadoop</groupId>
      <artifactId>hadoop-client</artifactId>
      <version>[version]</version>
    </dependency>
  </dependencies>

  <!-- If using CDH, also add Cloudera repo -->
  <repositories>
    ...
    <repository>
      <id>Cloudera repository</id>
      <url>https://repository.cloudera.com/artifactory/cloudera-repos/</url>
    </repository>
  </repositories>
</project>

{% endhighlight %}

# Where to Run Spark

As described in the [Hardware Provisioning](hardware-provisioning.html#storage-systems) guide,
Spark can run in a variety of deployment modes:

* Using dedicated set of Spark nodes in your cluster. These nodes should be co-located with your
  Hadoop installation.
* Running on the same nodes as an existing Hadoop installation, with a fixed amount memory and
  cores dedicated to Spark on each node.
* Run Spark alongside Hadoop using a cluster resource manager, such as YARN or Mesos.

These options are identical for those using CDH and HDP.

# Inheriting Cluster Configuration

If you plan to read and write from HDFS using Spark, there are two Hadoop configuration files that
should be included on Spark's classpath:

* `hdfs-site.xml`, which provides default behaviors for the HDFS client.
* `core-site.xml`, which sets the default filesystem name.

The location of these configuration files varies across CDH and HDP versions, but
a common location is inside of `/etc/hadoop/conf`. Some tools, such as Cloudera Manager, create
configurations on-the-fly, but offer a mechanisms to download copies of them.

To make these files visible to Spark, set `HADOOP_CONF_DIR` in `$SPARK_HOME/spark-env.sh`
to a location containing the configuration files.
