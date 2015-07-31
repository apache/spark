---
layout: global
displayTitle: Using Spark's "Hadoop Free" Build
title: Using Spark's "Hadoop Free" Build
---

Spark uses Hadoop client libraries for HDFS and YARN. Starting in version Spark 1.4, the project packages "Hadoop free" builds that lets you more easily connect a single Spark binary to any Hadoop version. To use these builds, you need to modify `SPARK_DIST_CLASSPATH` to include Hadoop's package jars. The most convenient place to do this is by adding an entry in `conf/spark-env.sh`.

This page describes how to connect Spark to Hadoop for different types of distributions.

# Apache Hadoop
For Apache distributions, you can use Hadoop's 'classpath' command. For instance:

{% highlight bash %}
### in conf/spark-env.sh ###

# If 'hadoop' binary is on your PATH
export SPARK_DIST_CLASSPATH=$(hadoop classpath)

# With explicit path to 'hadoop' binary
export SPARK_DIST_CLASSPATH=$(/path/to/hadoop/bin/hadoop classpath)

# Passing a Hadoop configuration directory
export SPARK_DIST_CLASSPATH=$(hadoop --config /path/to/configs classpath)

{% endhighlight %}
