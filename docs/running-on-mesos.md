---
layout: global
title: Running Spark on Mesos
---

Spark can run on clusters managed by [Apache Mesos](http://mesos.apache.org/). Follow the steps below to install Mesos and Spark:

1. Download and build Spark using the instructions [here](index.html). **Note:** Don't forget to consider what version of HDFS you might want to use!
2. Download, build, install, and start Mesos {{site.MESOS_VERSION}} on your cluster. You can download the Mesos distribution from a [mirror](http://www.apache.org/dyn/closer.cgi/mesos/{{site.MESOS_VERSION}}/). See the Mesos [Getting Started](http://mesos.apache.org/gettingstarted) page for more information. **Note:** If you want to run Mesos without installing it into the default paths on your system (e.g., if you don't have administrative privileges to install it), you should also pass the `--prefix` option to `configure` to tell it where to install. For example, pass `--prefix=/home/user/mesos`. By default the prefix is `/usr/local`.
3. Create a Spark "distribution" using `make-distribution.sh`.
4. Rename the `dist` directory created from `make-distribution.sh` to `spark-{{site.SPARK_VERSION}}`.
5. Create a `tar` archive: `tar czf spark-{{site.SPARK_VERSION}}.tar.gz spark-{{site.SPARK_VERSION}}`
6. Upload this archive to HDFS or another place accessible from Mesos via `http://`, e.g., [Amazon Simple Storage Service](http://aws.amazon.com/s3): `hadoop fs -put spark-{{site.SPARK_VERSION}}.tar.gz /path/to/spark-{{site.SPARK_VERSION}}.tar.gz`
7. Create a file called `spark-env.sh` in Spark's `conf` directory, by copying `conf/spark-env.sh.template`, and add the following lines to it:
   * `export MESOS_NATIVE_LIBRARY=<path to libmesos.so>`. This path is usually `<prefix>/lib/libmesos.so` (where the prefix is `/usr/local` by default, see above). Also, on Mac OS X, the library is called `libmesos.dylib` instead of `libmesos.so`.
   * `export SPARK_EXECUTOR_URI=<path to spark-{{site.SPARK_VERSION}}.tar.gz uploaded above>`.
   * `export MASTER=mesos://HOST:PORT` where HOST:PORT is the host and port (default: 5050) of your Mesos master (or `zk://...` if using Mesos with ZooKeeper).
8. To run a Spark application against the cluster, when you create your `SparkContext`, pass the string `mesos://HOST:PORT` as the first parameter. In addition, you'll need to set the `spark.executor.uri` property. For example:

{% highlight scala %}
System.setProperty("spark.executor.uri", "<path to spark-{{site.SPARK_VERSION}}.tar.gz uploaded above>")
val sc = new SparkContext("mesos://HOST:5050", "App Name", ...)
{% endhighlight %}

If you want to run Spark on Amazon EC2, you can use the Spark [EC2 launch scripts](ec2-scripts.html), which provide an easy way to launch a cluster with Mesos, Spark, and HDFS pre-configured. This will get you a cluster in about five minutes without any configuration on your part.

# Mesos Run Modes

Spark can run over Mesos in two modes: "fine-grained" and "coarse-grained". In fine-grained mode, which is the default,
each Spark task runs as a separate Mesos task. This allows multiple instances of Spark (and other frameworks) to share
machines at a very fine granularity, where each application gets more or fewer machines as it ramps up, but it comes with an
additional overhead in launching each task, which may be inappropriate for low-latency applications (e.g. interactive queries or serving web requests). The coarse-grained mode will instead
launch only *one* long-running Spark task on each Mesos machine, and dynamically schedule its own "mini-tasks" within
it. The benefit is much lower startup overhead, but at the cost of reserving the Mesos resources for the complete duration
of the application.

To run in coarse-grained mode, set the `spark.mesos.coarse` system property to true *before* creating your SparkContext:

{% highlight scala %}
System.setProperty("spark.mesos.coarse", "true")
val sc = new SparkContext("mesos://HOST:5050", "App Name", ...)
{% endhighlight %}

In addition, for coarse-grained mode, you can control the maximum number of resources Spark will acquire. By default,
it will acquire *all* cores in the cluster (that get offered by Mesos), which only makes sense if you run just one
application at a time. You can cap the maximum number of cores using `System.setProperty("spark.cores.max", "10")` (for example).
Again, this must be done *before* initializing a SparkContext.


# Running Alongside Hadoop

You can run Spark and Mesos alongside your existing Hadoop cluster by just launching them as a separate service on the machines. To access Hadoop data from Spark, just use a hdfs:// URL (typically `hdfs://<namenode>:9000/path`, but you can find the right URL on your Hadoop Namenode's web UI).

In addition, it is possible to also run Hadoop MapReduce on Mesos, to get better resource isolation and sharing between the two. In this case, Mesos will act as a unified scheduler that assigns cores to either Hadoop or Spark, as opposed to having them share resources via the Linux scheduler on each node. Please refer to [Hadoop on Mesos](https://github.com/mesos/hadoop).

In either case, HDFS runs separately from Hadoop MapReduce, without going through Mesos.
