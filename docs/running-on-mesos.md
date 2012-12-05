---
layout: global
title: Running Spark on Mesos
---

Spark can run on private clusters managed by the [Apache Mesos](http://incubator.apache.org/mesos/) resource manager. Follow the steps below to install Mesos and Spark:

1. Download and build Spark using the instructions [here](index.html).
2. Download Mesos {{site.MESOS_VERSION}} from a [mirror](http://www.apache.org/dyn/closer.cgi/incubator/mesos/mesos-{{site.MESOS_VERSION}}/).
3. Configure Mesos using the `configure` script, passing the location of your `JAVA_HOME` using `--with-java-home`. Mesos comes with "template" configure scripts for different platforms, such as `configure.macosx`, that you can run. See the README file in Mesos for other options. **Note:** If you want to run Mesos without installing it into the default paths on your system (e.g. if you don't have administrative privileges to install it), you should also pass the `--prefix` option to `configure` to tell it where to install. For example, pass `--prefix=/home/user/mesos`. By default the prefix is `/usr/local`.
4. Build Mesos using `make`, and then install it using `make install`.
5. Create a file called `spark-env.sh` in Spark's `conf` directory, by copying `conf/spark-env.sh.template`, and add the following lines in it:
   * `export MESOS_NATIVE_LIBRARY=<path to libmesos.so>`. This path is usually `<prefix>/lib/libmesos.so` (where the prefix is `/usr/local` by default). Also, on Mac OS X, the library is called `libmesos.dylib` instead of `.so`.
   * `export SCALA_HOME=<path to Scala directory>`.
6. Copy Spark and Mesos to the _same_ paths on all the nodes in the cluster (or, for Mesos, `make install` on every node).
7. Configure Mesos for deployment:
   * On your master node, edit `<prefix>/var/mesos/deploy/masters` to list your master and `<prefix>/var/mesos/deploy/slaves` to list the slaves, where `<prefix>` is the prefix where you installed Mesos (`/usr/local` by default).
   * On all nodes, edit `<prefix>/var/mesos/conf/mesos.conf` and add the line `master=HOST:5050`, where HOST is your master node.
   * Run `<prefix>/sbin/mesos-start-cluster.sh` on your master to start Mesos. If all goes well, you should see Mesos's web UI on port 8080 of the master machine.
   * See Mesos's README file for more information on deploying it.
8. To run a Spark job against the cluster, when you create your `SparkContext`, pass the string `mesos://HOST:5050` as the first parameter, where `HOST` is the machine running your Mesos master. In addition, pass the location of Spark on your nodes as the third parameter, and a list of JAR files containing your JAR's code as the fourth (these will automatically get copied to the workers). For example:

{% highlight scala %}
new SparkContext("mesos://HOST:5050", "My Job Name", "/home/user/spark", List("my-job.jar"))
{% endhighlight %}

If you want to run Spark on Amazon EC2, you can use the Spark [EC2 launch scripts](ec2-scripts.html), which provide an easy way to launch a cluster with Mesos, Spark, and HDFS pre-configured. This will get you a cluster in about five minutes without any configuration on your part.

# Mesos Run Modes

Spark can run over Mesos in two modes: "fine-grained" and "coarse-grained". In fine-grained mode, which is the default,
each Spark task runs as a separate Mesos task. This allows multiple instances of Spark (and other applications) to share
machines at a very fine granularity, where each job gets more or fewer machines as it ramps up, but it comes with an
additional overhead in launching each task, which may be inappropriate for low-latency applications that aim for
sub-second Spark operations (e.g. interactive queries or serving web requests). The coarse-grained mode will instead
launch only *one* long-running Spark task on each Mesos machine, and dynamically schedule its own "mini-tasks" within
it. The benefit is much lower startup overhead, but at the cost of reserving the Mesos resources for the complete duration
of the job.

To run in coarse-grained mode, set the `spark.mesos.coarse` system property to true *before* creating your SparkContext:

{% highlight scala %}
System.setProperty("spark.mesos.coarse", "true")
val sc = new SparkContext("mesos://HOST:5050", "Job Name", ...)
{% endhighlight %}

In addition, for coarse-grained mode, you can control the maximum number of resources Spark will acquire. By default,
it will acquire *all* cores in the cluster (that get offered by Mesos), which only makes sense if you run just a single
job at a time. You can cap the maximum number of cores using `System.setProperty("spark.cores.max", "10")` (for example).
Again, this must be done *before* initializing a SparkContext.


# Running Alongside Hadoop

You can run Spark and Mesos alongside your existing Hadoop cluster by just launching them as a separate service on the machines. To access Hadoop data from Spark, just use a hdfs:// URL (typically `hdfs://<namenode>:9000/path`, but you can find the right URL on your Hadoop Namenode's web UI).

In addition, it is possible to also run Hadoop MapReduce on Mesos, to get better resource isolation and sharing between the two. In this case, Mesos will act as a unified scheduler that assigns cores to either Hadoop or Spark, as opposed to having them share resources via the Linux scheduler on each node. Please refer to the Mesos wiki page on [Running Hadoop on Mesos](https://github.com/mesos/mesos/wiki/Running-Hadoop-on-Mesos).

In either case, HDFS runs separately from Hadoop MapReduce, without going through Mesos.
