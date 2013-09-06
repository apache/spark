---
layout: global
title: Running with Cloudera and HortonWorks Distributions
---

Spark is fully compatible with all versions of Cloudera's Distribution Including Hadoop (CDH) and
the Hortonworks Data Platform (HDP). There are a few things to keep in mind when running against
these distributions:

# Compile-time Hadoop Version
When compiling Spark, you'll need to 
[set the HADOOP_VERSION flag](http://localhost:4000/index.html#a-note-about-hadoop-versions). 
The table below lists the corresponding HADOOP_VERSION for each CDH/HDP release. _Note_ that
some Hadoop releases are binary compatible across client versions. This means the pre-built Spark
distribution may "just work" without you needing to compile. That said, we recommend compiling with 
the _exact_ Hadoop version you are running to avoid any compatibility errors.

<table>
  <tr valign="top">
    <td>
      <h3>CDH Releases</h3>
      <table class="table" style="width:350px;">
        <tr><th>Version</th><th>HADOOP_VERSION</th></tr>
        <tr><td>CDH 4.X.X (YARN mode)</td><td>2.0.0-chd4.X.X</td></tr>
        <tr><td>CDH 4.X.X</td><td>2.0.0-mr1-chd4.X.X</td></tr>
        <tr><td>CDH 3u6</td><td>0.20.2-cdh3u6</td></tr>
        <tr><td>CDH 3u5</td><td>0.20.2-cdh3u5</td></tr>
        <tr><td>CDH 3u4</td><td>0.20.2-cdh3u4</td></tr>
      </table>
    </td>
    <td>
      <h3>HDP Releases</h3>
      <table class="table" style="width:350px;">
        <tr><th>Version</th><th>HADOOP_VERSION</th></tr>
        <tr><td>HDP 1.3</td><td>1.2.0</td></tr>
        <tr><td>HDP 1.2</td><td>1.1.2</td></tr>
        <tr><td>HDP 1.1</td><td>1.0.3</td></tr>
        <tr><td>HDP 1.0</td><td>1.0.3</td></tr>
      </table>
    </td>
  </tr>
</table>

# Where to Run Spark
As described in the [Hardware Provisioning](hardware-provisioning.html#storage-systems) guide,
Spark can run in a variety of deployment modes:

* Using dedicated set of Spark nodes in your cluster. These nodes should be co-located with your
  Hadoop installation.
* Running on the same nodes as an existing Hadoop installation, with a fixed amount memory and 
  cores dedicated to Spark on each node.
* Run Spark alongside Hadoop using a cluster resource manager, such as YARN or Mesos.

These options are identical for those using CDH and HDP. Note that if you are running a YARN 
cluster, you may still choose to run Spark on dedicated nodes. In this case, you should use 
the `mr1` versions of HADOOP_HOME when compiling, not the YARN versions.

# Inheriting Cluster Configuration
If you plan to read and write from HDFS using Spark, it is good to include copies of two relevant 
Hadoop configuration files in your $SPARK_HOME/conf directory. These are `hdfs-site.xml`, which 
provides default behaviors for the HDFS client, and `core-site.xml`, which sets the default 
filesystem name. The location of these configuration files varies across CDH and HDP versions, but
a common location is inside of `/etc/hadoop/conf`. Some tools, such as Cloudera Manager, create
configurations on-the-fly, but offer a mechanisms to download copies of them.

If you can locate these files, copy them into $SPARK_HOME/conf/.
