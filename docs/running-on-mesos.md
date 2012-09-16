---
layout: global
title: Running Spark on Mesos
---

To run on a cluster, Spark uses the [Apache Mesos](http://incubator.apache.org/mesos/) resource manager. Follow the steps below to install Mesos and Spark:

1. Download and build Spark using the instructions [here]({{ HOME_DIR }}Home).
2. Download Mesos 0.9.0 from a [mirror](http://www.apache.org/dyn/closer.cgi/incubator/mesos/mesos-0.9.0-incubating/).
3. Configure Mesos using the `configure` script, passing the location of your `JAVA_HOME` using `--with-java-home`. Mesos comes with "template" configure scripts for different platforms, such as `configure.macosx`, that you can run. See the README file in Mesos for other options. **Note:** If you want to run Mesos without installing it into the default paths on your system (e.g. if you don't have administrative privileges to install it), you should also pass the `--prefix` option to `configure` to tell it where to install. For example, pass `--prefix=/home/user/mesos`. By default the prefix is `/usr/local`.
4. Build Mesos using `make`, and then install it using `make install`.
5. Create a file called `spark-env.sh` in Spark's `conf` directory, by copying `conf/spark-env.sh.template`, and add the following lines in it:
   * `export MESOS_NATIVE_LIBRARY=<path to libmesos.so>`. This path is usually `<prefix>/lib/libmesos.so` (where the prefix is `/usr/local` by default). Also, on Mac OS X, the library is called `libmesos.dylib` instead of `.so`.
   * `export SCALA_HOME=<path to Scala directory>`.
6. Copy Spark and Mesos to the _same_ paths on all the nodes in the cluster (or, for Mesos, `make install` on every node).
7. Configure Mesos for deployment:
   * On your master node, edit `<prefix>/var/mesos/deploy/masters` to list your master and `<prefix>/var/mesos/deploy/slaves` to list the slaves, where `<prefix>` is the prefix where you installed Mesos (`/usr/local` by default).
   * On all nodes, edit `<prefix>/var/mesos/deploy/mesos.conf` and add the line `master=HOST:5050`, where HOST is your master node.
   * Run `<prefix>/sbin/mesos-start-cluster.sh` on your master to start Mesos. If all goes well, you should see Mesos's web UI on port 8080 of the master machine.
   * See Mesos's README file for more information on deploying it.
8. To run a Spark job against the cluster, when you create your `SparkContext`, pass the string `HOST:5050` as the first parameter, where `HOST` is the machine running your Mesos master. In addition, pass the location of Spark on your nodes as the third parameter, and a list of JAR files containing your JAR's code as the fourth (these will automatically get copied to the workers). For example:

{% highlight scala %}
new SparkContext("HOST:5050", "My Job Name", "/home/user/spark", List("my-job.jar"))
{% endhighlight %}

## Running on Amazon EC2

If you want to run Spark on Amazon EC2, you can use the Spark [EC2 launch scripts]({{HOME_PATH}}ec2-scripts.html), which provide an easy way to launch a cluster with Mesos, Spark, and HDFS pre-configured. This will get you a cluster in about five minutes without any configuration on your part.

## Running Alongside Hadoop

You can run Spark and Mesos alongside your existing Hadoop cluster by just launching them as a separate service on the machines. To access Hadoop data from Spark, just use a hdfs:// URL (typically `hdfs://<namenode>:9000/path`, but you can find the right URL on your Hadoop Namenode's web UI).

In addition, it is possible to also run Hadoop MapReduce on Mesos, to get better resource isolation and sharing between the two. In this case, Mesos will act as a unified scheduler that assigns cores to either Hadoop or Spark, as opposed to having them share resources via the Linux scheduler on each node. Please refer to the Mesos wiki page on [Running Hadoop on Mesos](https://github.com/mesos/mesos/wiki/Running-Hadoop-on-Mesos).

In either case, HDFS runs separately from Hadoop MapReduce, without going through Mesos.
