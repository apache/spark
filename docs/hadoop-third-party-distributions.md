---
layout: global
title: Running with Cloudera and HortonWorks
---

Spark can run against all versions of Cloudera's Distribution Including Apache Hadoop (CDH) and
the Hortonworks Data Platform (HDP). There are a few things to keep in mind when using Spark
with these distributions:

# Compile-time Hadoop Version

When compiling Spark, you'll need to 
[set the SPARK_HADOOP_VERSION flag](index.html#a-note-about-hadoop-versions):

    SPARK_HADOOP_VERSION=1.0.4 sbt/sbt assembly

The table below lists the corresponding `SPARK_HADOOP_VERSION` code for each CDH/HDP release. Note that
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
        <tr><td>CDH 3u6</td><td>0.20.2-cdh3u6</td></tr>
        <tr><td>CDH 3u5</td><td>0.20.2-cdh3u5</td></tr>
        <tr><td>CDH 3u4</td><td>0.20.2-cdh3u4</td></tr>
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

There are a few ways to make these files visible to Spark:

* You can copy these files into `$SPARK_HOME/conf` and they will be included in Spark's
classpath automatically.
* If you are running Spark on the same nodes as Hadoop _and_ your distribution includes both
`hdfs-site.xml` and `core-site.xml` in the same directory, you can set `HADOOP_CONF_DIR` 
in `$SPARK_HOME/spark-env.sh` to that directory.
