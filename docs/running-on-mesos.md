---
layout: global
title: Running Spark on Mesos
---
* This will become a table of contents (this text will be scraped).
{:toc}

Spark can run on hardware clusters managed by [Apache Mesos](http://mesos.apache.org/).

The advantages of deploying Spark with Mesos include:

- dynamic partitioning between Spark and other
  [frameworks](https://mesos.apache.org/documentation/latest/mesos-frameworks/)
- scalable partitioning between multiple instances of Spark

# How it Works

In a standalone cluster deployment, the cluster manager in the below diagram is a Spark master
instance.  When using Mesos, the Mesos master replaces the Spark master as the cluster manager.

<p style="text-align: center;">
  <img src="img/cluster-overview.png" title="Spark cluster components" alt="Spark cluster components" />
</p>

Now when a driver creates a job and starts issuing tasks for scheduling, Mesos determines what
machines handle what tasks.  Because it takes into account other frameworks when scheduling these
many short-lived tasks, multiple frameworks can coexist on the same cluster without resorting to a
static partitioning of resources.

To get started, follow the steps below to install Mesos and deploy Spark jobs via Mesos.


# Installing Mesos

Spark {{site.SPARK_VERSION}} is designed for use with Mesos {{site.MESOS_VERSION}} and does not
require any special patches of Mesos.

If you already have a Mesos cluster running, you can skip this Mesos installation step.

Otherwise, installing Mesos for Spark is no different than installing Mesos for use by other
frameworks.  You can install Mesos either from source or using prebuilt packages.

## From Source

To install Apache Mesos from source, follow these steps:

1. Download a Mesos release from a
   [mirror](http://www.apache.org/dyn/closer.cgi/mesos/{{site.MESOS_VERSION}}/)
2. Follow the Mesos [Getting Started](http://mesos.apache.org/gettingstarted) page for compiling and
   installing Mesos

**Note:** If you want to run Mesos without installing it into the default paths on your system
(e.g., if you lack administrative privileges to install it), pass the
`--prefix` option to `configure` to tell it where to install. For example, pass
`--prefix=/home/me/mesos`. By default the prefix is `/usr/local`.

## Third-Party Packages

The Apache Mesos project only publishes source releases, not binary packages.  But other
third party projects publish binary releases that may be helpful in setting Mesos up.

One of those is Mesosphere.  To install Mesos using the binary releases provided by Mesosphere:

1. Download Mesos installation package from [downloads page](http://mesosphere.io/downloads/)
2. Follow their instructions for installation and configuration

The Mesosphere installation documents suggest setting up ZooKeeper to handle Mesos master failover,
but Mesos can be run without ZooKeeper using a single master as well.

## Verification

To verify that the Mesos cluster is ready for Spark, navigate to the Mesos master webui at port
`:5050`  Confirm that all expected machines are present in the slaves tab.


# Connecting Spark to Mesos

To use Mesos from Spark, you need a Spark binary package available in a place accessible by Mesos, and
a Spark driver program configured to connect to Mesos.

## Uploading Spark Package

When Mesos runs a task on a Mesos slave for the first time, that slave must have a Spark binary
package for running the Spark Mesos executor backend.
The Spark package can be hosted at any Hadoop-accessible URI, including HTTP via `http://`,
[Amazon Simple Storage Service](http://aws.amazon.com/s3) via `s3n://`, or HDFS via `hdfs://`.

To use a precompiled package:

1. Download a Spark binary package from the Spark [download page](https://spark.apache.org/downloads.html)
2. Upload to hdfs/http/s3

To host on HDFS, use the Hadoop fs put command: `hadoop fs -put spark-{{site.SPARK_VERSION}}.tar.gz
/path/to/spark-{{site.SPARK_VERSION}}.tar.gz`


Or if you are using a custom-compiled version of Spark, you will need to create a package using
the `make-distribution.sh` script included in a Spark source tarball/checkout.

1. Download and build Spark using the instructions [here](index.html)
2. Create a binary package using `make-distribution.sh --tgz`.
3. Upload archive to http/s3/hdfs


## Using a Mesos Master URL

The Master URLs for Mesos are in the form `mesos://host:5050` for a single-master Mesos
cluster, or `mesos://zk://host:2181` for a multi-master Mesos cluster using ZooKeeper.

The driver also needs some configuration in `spark-env.sh` to interact properly with Mesos:

1. In `spark-env.sh` set some environment variables:
 * `export MESOS_NATIVE_JAVA_LIBRARY=<path to libmesos.so>`. This path is typically
   `<prefix>/lib/libmesos.so` where the prefix is `/usr/local` by default. See Mesos installation
   instructions above. On Mac OS X, the library is called `libmesos.dylib` instead of
   `libmesos.so`.
 * `export SPARK_EXECUTOR_URI=<URL of spark-{{site.SPARK_VERSION}}.tar.gz uploaded above>`.
2. Also set `spark.executor.uri` to `<URL of spark-{{site.SPARK_VERSION}}.tar.gz>`.

Now when starting a Spark application against the cluster, pass a `mesos://`
URL as the master when creating a `SparkContext`. For example:

{% highlight scala %}
val conf = new SparkConf()
  .setMaster("mesos://HOST:5050")
  .setAppName("My app")
  .set("spark.executor.uri", "<path to spark-{{site.SPARK_VERSION}}.tar.gz uploaded above>")
val sc = new SparkContext(conf)
{% endhighlight %}

(You can also use [`spark-submit`](submitting-applications.html) and configure `spark.executor.uri`
in the [conf/spark-defaults.conf](configuration.html#loading-default-configurations) file. Note
that `spark-submit` currently only supports deploying the Spark driver in `client` mode for Mesos.)

When running a shell, the `spark.executor.uri` parameter is inherited from `SPARK_EXECUTOR_URI`, so
it does not need to be redundantly passed in as a system property.

{% highlight bash %}
./bin/spark-shell --master mesos://host:5050
{% endhighlight %}


# Mesos Run Modes

Spark can run over Mesos in two modes: "fine-grained" (default) and "coarse-grained".

In "fine-grained" mode (default), each Spark task runs as a separate Mesos task. This allows
multiple instances of Spark (and other frameworks) to share machines at a very fine granularity,
where each application gets more or fewer machines as it ramps up and down, but it comes with an
additional overhead in launching each task. This mode may be inappropriate for low-latency
requirements like interactive queries or serving web requests.

The "coarse-grained" mode will instead launch only *one* long-running Spark task on each Mesos
machine, and dynamically schedule its own "mini-tasks" within it. The benefit is much lower startup
overhead, but at the cost of reserving the Mesos resources for the complete duration of the
application.

To run in coarse-grained mode, set the `spark.mesos.coarse` property in your
[SparkConf](configuration.html#spark-properties):

{% highlight scala %}
conf.set("spark.mesos.coarse", "true")
{% endhighlight %}

In addition, for coarse-grained mode, you can control the maximum number of resources Spark will
acquire. By default, it will acquire *all* cores in the cluster (that get offered by Mesos), which
only makes sense if you run just one application at a time. You can cap the maximum number of cores
using `conf.set("spark.cores.max", "10")` (for example).

# Running Alongside Hadoop

You can run Spark and Mesos alongside your existing Hadoop cluster by just launching them as a
separate service on the machines. To access Hadoop data from Spark, a full `hdfs://` URL is required
(typically `hdfs://<namenode>:9000/path`, but you can find the right URL on your Hadoop Namenode web
UI).

In addition, it is possible to also run Hadoop MapReduce on Mesos for better resource isolation and
sharing between the two. In this case, Mesos will act as a unified scheduler that assigns cores to
either Hadoop or Spark, as opposed to having them share resources via the Linux scheduler on each
node. Please refer to [Hadoop on Mesos](https://github.com/mesos/hadoop).

In either case, HDFS runs separately from Hadoop MapReduce, without being scheduled through Mesos.


# Configuration

See the [configuration page](configuration.html) for information on Spark configurations.  The following configs are specific for Spark on Mesos.

#### Spark Properties

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.mesos.coarse</code></td>
  <td>false</td>
  <td>
    If set to "true", runs over Mesos clusters in
    <a href="running-on-mesos.html#mesos-run-modes">"coarse-grained" sharing mode</a>,
    where Spark acquires one long-lived Mesos task on each machine instead of one Mesos task per
    Spark task. This gives lower-latency scheduling for short queries, but leaves resources in use
    for the whole duration of the Spark job.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.extra.cores</code></td>
  <td>0</td>
  <td>
    Set the extra amount of cpus to request per task. This setting is only used for Mesos coarse grain mode.
    The total amount of cores requested per task is the number of cores in the offer plus the extra cores configured.
    Note that total amount of cores the executor will request in total will not exceed the spark.cores.max setting.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.mesosExecutor.cores</code></td>
  <td>1.0</td>
  <td>
    (Fine-grained mode only) Number of cores to give each Mesos executor. This does not
    include the cores used to run the Spark tasks. In other words, even if no Spark task
    is being run, each Mesos executor will occupy the number of cores configured here.
    The value can be a floating point number.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.executor.home</code></td>
  <td>driver side <code>SPARK_HOME</code></td>
  <td>
    Set the directory in which Spark is installed on the executors in Mesos. By default, the
    executors will simply use the driver's Spark home directory, which may not be visible to
    them. Note that this is only relevant if a Spark binary package is not specified through
    <code>spark.executor.uri</code>.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.executor.memoryOverhead</code></td>
  <td>executor memory * 0.10, with minimum of 384</td>
  <td>
    The amount of additional memory, specified in MB, to be allocated per executor. By default,
    the overhead will be larger of either 384 or 10% of `spark.executor.memory`. If it's set,
    the final overhead will be this value.
  </td>
</tr>
</table>

# Troubleshooting and Debugging

A few places to look during debugging:

- Mesos master on port `:5050`
  - Slaves should appear in the slaves tab
  - Spark applications should appear in the frameworks tab
  - Tasks should appear in the details of a framework
  - Check the stdout and stderr of the sandbox of failed tasks
- Mesos logs
  - Master and slave logs are both in `/var/log/mesos` by default

And common pitfalls:

- Spark assembly not reachable/accessible
  - Slaves must be able to download the Spark binary package from the `http://`, `hdfs://` or `s3n://` URL you gave
- Firewall blocking communications
  - Check for messages about failed connections
  - Temporarily disable firewalls for debugging and then poke appropriate holes
