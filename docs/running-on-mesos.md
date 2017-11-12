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

Spark {{site.SPARK_VERSION}} is designed for use with Mesos {{site.MESOS_VERSION}} or newer and does not
require any special patches of Mesos.

If you already have a Mesos cluster running, you can skip this Mesos installation step.

Otherwise, installing Mesos for Spark is no different than installing Mesos for use by other
frameworks.  You can install Mesos either from source or using prebuilt packages.

## From Source

To install Apache Mesos from source, follow these steps:

1. Download a Mesos release from a
   [mirror](http://www.apache.org/dyn/closer.lua/mesos/{{site.MESOS_VERSION}}/)
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

Alternatively, you can also install Spark in the same location in all the Mesos slaves, and configure
`spark.mesos.executor.home` (defaults to SPARK_HOME) to point to that location.

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
the `dev/make-distribution.sh` script included in a Spark source tarball/checkout.

1. Download and build Spark using the instructions [here](index.html)
2. Create a binary package using `./dev/make-distribution.sh --tgz`.
3. Upload archive to http/s3/hdfs


## Using a Mesos Master URL

The Master URLs for Mesos are in the form `mesos://host:5050` for a single-master Mesos
cluster, or `mesos://zk://host1:2181,host2:2181,host3:2181/mesos` for a multi-master Mesos cluster using ZooKeeper.

## Client Mode

In client mode, a Spark Mesos framework is launched directly on the client machine and waits for the driver output.

The driver needs some configuration in `spark-env.sh` to interact properly with Mesos:

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
in the [conf/spark-defaults.conf](configuration.html#loading-default-configurations) file.)

When running a shell, the `spark.executor.uri` parameter is inherited from `SPARK_EXECUTOR_URI`, so
it does not need to be redundantly passed in as a system property.

{% highlight bash %}
./bin/spark-shell --master mesos://host:5050
{% endhighlight %}

## Cluster mode

Spark on Mesos also supports cluster mode, where the driver is launched in the cluster and the client
can find the results of the driver from the Mesos Web UI.

To use cluster mode, you must start the `MesosClusterDispatcher` in your cluster via the `sbin/start-mesos-dispatcher.sh` script,
passing in the Mesos master URL (e.g: mesos://host:5050). This starts the `MesosClusterDispatcher` as a daemon running on the host.

If you like to run the `MesosClusterDispatcher` with Marathon, you need to run the `MesosClusterDispatcher` in the foreground (i.e: `bin/spark-class org.apache.spark.deploy.mesos.MesosClusterDispatcher`). Note that the `MesosClusterDispatcher` not yet supports multiple instances for HA.

The `MesosClusterDispatcher` also supports writing recovery state into Zookeeper. This will allow the `MesosClusterDispatcher` to be able to recover all submitted and running containers on relaunch.   In order to enable this recovery mode, you can set SPARK_DAEMON_JAVA_OPTS in spark-env by configuring `spark.deploy.recoveryMode` and related spark.deploy.zookeeper.* configurations.
For more information about these configurations please refer to the configurations [doc](configurations.html#deploy).

You can also specify any additional jars required by the `MesosClusterDispatcher` in the classpath by setting the environment variable SPARK_DAEMON_CLASSPATH in spark-env.

From the client, you can submit a job to Mesos cluster by running `spark-submit` and specifying the master URL
to the URL of the `MesosClusterDispatcher` (e.g: mesos://dispatcher:7077). You can view driver statuses on the
Spark cluster Web UI.

For example:
{% highlight bash %}
./bin/spark-submit \
  --class org.apache.spark.examples.SparkPi \
  --master mesos://207.184.161.138:7077 \
  --deploy-mode cluster \
  --supervise \
  --executor-memory 20G \
  --total-executor-cores 100 \
  http://path/to/examples.jar \
  1000
{% endhighlight %}


Note that jars or python files that are passed to spark-submit should be URIs reachable by Mesos slaves, as the Spark driver doesn't automatically upload local jars.

# Mesos Run Modes

Spark can run over Mesos in two modes: "coarse-grained" (default) and
"fine-grained" (deprecated).

## Coarse-Grained

In "coarse-grained" mode, each Spark executor runs as a single Mesos
task.  Spark executors are sized according to the following
configuration variables:

* Executor memory: `spark.executor.memory`
* Executor cores: `spark.executor.cores`
* Number of executors: `spark.cores.max`/`spark.executor.cores`

Please see the [Spark Configuration](configuration.html) page for
details and default values.

Executors are brought up eagerly when the application starts, until
`spark.cores.max` is reached.  If you don't set `spark.cores.max`, the
Spark application will reserve all resources offered to it by Mesos,
so we of course urge you to set this variable in any sort of
multi-tenant cluster, including one which runs multiple concurrent
Spark applications.

The scheduler will start executors round-robin on the offers Mesos
gives it, but there are no spread guarantees, as Mesos does not
provide such guarantees on the offer stream.

In this mode spark executors will honor port allocation if such is
provided from the user. Specifically if the user defines
`spark.executor.port` or `spark.blockManager.port` in Spark configuration,
the mesos scheduler will check the available offers for a valid port
range containing the port numbers. If no such range is available it will
not launch any task. If no restriction is imposed on port numbers by the
user, ephemeral ports are used as usual. This port honouring implementation
implies one task per host if the user defines a port. In the future network
isolation shall be supported.

The benefit of coarse-grained mode is much lower startup overhead, but
at the cost of reserving Mesos resources for the complete duration of
the application.  To configure your job to dynamically adjust to its
resource requirements, look into
[Dynamic Allocation](#dynamic-resource-allocation-with-mesos).

## Fine-Grained (deprecated)

**NOTE:** Fine-grained mode is deprecated as of Spark 2.0.0.  Consider
 using [Dynamic Allocation](#dynamic-resource-allocation-with-mesos)
 for some of the benefits.  For a full explanation see
 [SPARK-11857](https://issues.apache.org/jira/browse/SPARK-11857)

In "fine-grained" mode, each Spark task inside the Spark executor runs
as a separate Mesos task. This allows multiple instances of Spark (and
other frameworks) to share cores at a very fine granularity, where
each application gets more or fewer cores as it ramps up and down, but
it comes with an additional overhead in launching each task. This mode
may be inappropriate for low-latency requirements like interactive
queries or serving web requests.

Note that while Spark tasks in fine-grained will relinquish cores as
they terminate, they will not relinquish memory, as the JVM does not
give memory back to the Operating System.  Neither will executors
terminate when they're idle.

To run in fine-grained mode, set the `spark.mesos.coarse` property to false in your
[SparkConf](configuration.html#spark-properties):

{% highlight scala %}
conf.set("spark.mesos.coarse", "false")
{% endhighlight %}

You may also make use of `spark.mesos.constraints` to set
attribute-based constraints on Mesos resource offers. By default, all
resource offers will be accepted.

{% highlight scala %}
conf.set("spark.mesos.constraints", "os:centos7;us-east-1:false")
{% endhighlight %}

For example, Let's say `spark.mesos.constraints` is set to `os:centos7;us-east-1:false`, then the resource offers will
be checked to see if they meet both these constraints and only then will be accepted to start new executors.

To constrain where driver tasks are run, use `spark.mesos.driver.constraints`

# Mesos Docker Support

Spark can make use of a Mesos Docker containerizer by setting the property `spark.mesos.executor.docker.image`
in your [SparkConf](configuration.html#spark-properties).

The Docker image used must have an appropriate version of Spark already part of the image, or you can
have Mesos download Spark via the usual methods.

Requires Mesos version 0.20.1 or later.

Note that by default Mesos agents will not pull the image if it already exists on the agent. If you use mutable image
tags you can set `spark.mesos.executor.docker.forcePullImage` to `true` in order to force the agent to always pull the
image before running the executor. Force pulling images is only available in Mesos version 0.22 and above.

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

# Dynamic Resource Allocation with Mesos

Mesos supports dynamic allocation only with coarse-grained mode, which can resize the number of
executors based on statistics of the application. For general information,
see [Dynamic Resource Allocation](job-scheduling.html#dynamic-resource-allocation).

The External Shuffle Service to use is the Mesos Shuffle Service. It provides shuffle data cleanup functionality
on top of the Shuffle Service since Mesos doesn't yet support notifying another framework's
termination. To launch it, run `$SPARK_HOME/sbin/start-mesos-shuffle-service.sh` on all slave nodes, with `spark.shuffle.service.enabled` set to `true`.

This can also be achieved through Marathon, using a unique host constraint, and the following command: `bin/spark-class org.apache.spark.deploy.mesos.MesosExternalShuffleService`.

# Configuration

See the [configuration page](configuration.html) for information on Spark configurations.  The following configs are specific for Spark on Mesos.

#### Spark Properties

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.mesos.coarse</code></td>
  <td>true</td>
  <td>
    If set to <code>true</code>, runs over Mesos clusters in "coarse-grained" sharing mode, where Spark acquires one long-lived Mesos task on each machine.
    If set to <code>false</code>, runs over Mesos cluster in "fine-grained" sharing mode, where one Mesos task is created per Spark task.
    Detailed information in <a href="running-on-mesos.html#mesos-run-modes">'Mesos Run Modes'</a>.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.extra.cores</code></td>
  <td><code>0</code></td>
  <td>
    Set the extra number of cores for an executor to advertise. This
    does not result in more cores allocated.  It instead means that an
    executor will "pretend" it has more cores, so that the driver will
    send it more tasks.  Use this to increase parallelism.  This
    setting is only used for Mesos coarse-grained mode.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.mesosExecutor.cores</code></td>
  <td><code>1.0</code></td>
  <td>
    (Fine-grained mode only) Number of cores to give each Mesos executor. This does not
    include the cores used to run the Spark tasks. In other words, even if no Spark task
    is being run, each Mesos executor will occupy the number of cores configured here.
    The value can be a floating point number.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.executor.docker.image</code></td>
  <td>(none)</td>
  <td>
    Set the name of the docker image that the Spark executors will run in. The selected
    image must have Spark installed, as well as a compatible version of the Mesos library.
    The installed path of Spark in the image can be specified with <code>spark.mesos.executor.home</code>;
    the installed path of the Mesos library can be specified with <code>spark.executorEnv.MESOS_NATIVE_JAVA_LIBRARY</code>.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.executor.docker.forcePullImage</code></td>
  <td>false</td>
  <td>
    Force Mesos agents to pull the image specified in <code>spark.mesos.executor.docker.image</code>.
    By default Mesos agents will not pull images they already have cached.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.executor.docker.parameters</code></td>
  <td>(none)</td>
  <td>
    Set the list of custom parameters which will be passed into the <code>docker run</code> command when launching the Spark executor on Mesos using the docker containerizer. The format of this property is a comma-separated list of
    key/value pairs. Example:

    <pre>key1=val1,key2=val2,key3=val3</pre>
  </td>
</tr>
<tr>
  <td><code>spark.mesos.executor.docker.volumes</code></td>
  <td>(none)</td>
  <td>
    Set the list of volumes which will be mounted into the Docker image, which was set using
    <code>spark.mesos.executor.docker.image</code>. The format of this property is a comma-separated list of
    mappings following the form passed to <code>docker run -v</code>. That is they take the form:

    <pre>[host_path:]container_path[:ro|:rw]</pre>
  </td>
</tr>
<tr>
  <td><code>spark.mesos.task.labels</code></td>
  <td>(none)</td>
  <td>
    Set the Mesos labels to add to each task. Labels are free-form key-value pairs.
    Key-value pairs should be separated by a colon, and commas used to list more than one.
    Ex. key:value,key2:value2.
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
    the overhead will be larger of either 384 or 10% of <code>spark.executor.memory</code>. If set,
    the final overhead will be this value.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.uris</code></td>
  <td>(none)</td>
  <td>
    A comma-separated list of URIs to be downloaded to the sandbox
    when driver or executor is launched by Mesos.  This applies to
    both coarse-grained and fine-grained mode.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.principal</code></td>
  <td>(none)</td>
  <td>
    Set the principal with which Spark framework will use to authenticate with Mesos.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.secret</code></td>
  <td>(none)</td>
  <td>
    Set the secret with which Spark framework will use to authenticate with Mesos.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.role</code></td>
  <td><code>*</code></td>
  <td>
    Set the role of this Spark framework for Mesos. Roles are used in Mesos for reservations
    and resource weight sharing.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.constraints</code></td>
  <td>(none)</td>
  <td>
    Attribute based constraints on mesos resource offers. By default, all resource offers will be accepted. This setting
    applies only to executors. Refer to <a href="http://mesos.apache.org/documentation/attributes-resources/">Mesos
    Attributes & Resources</a> for more information on attributes.
    <ul>
      <li>Scalar constraints are matched with "less than equal" semantics i.e. value in the constraint must be less than or equal to the value in the resource offer.</li>
      <li>Range constraints are matched with "contains" semantics i.e. value in the constraint must be within the resource offer's value.</li>
      <li>Set constraints are matched with "subset of" semantics i.e. value in the constraint must be a subset of the resource offer's value.</li>
      <li>Text constraints are matched with "equality" semantics i.e. value in the constraint must be exactly equal to the resource offer's value.</li>
      <li>In case there is no value present as a part of the constraint any offer with the corresponding attribute will be accepted (without value check).</li>
    </ul>
  </td>
</tr>
<tr>
  <td><code>spark.mesos.driver.constraints</code></td>
  <td>(none)</td>
  <td>
    Same as <code>spark.mesos.constraints</code> except applied to drivers when launched through the dispatcher. By default,
    all offers with sufficient resources will be accepted.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.containerizer</code></td>
  <td><code>docker</code></td>
  <td>
    This only affects docker containers, and must be one of "docker"
    or "mesos".  Mesos supports two types of
    containerizers for docker: the "docker" containerizer, and the preferred
    "mesos" containerizer.  Read more here: http://mesos.apache.org/documentation/latest/container-image/
  </td>
</tr>
<tr>
  <td><code>spark.mesos.driver.webui.url</code></td>
  <td><code>(none)</code></td>
  <td>
    Set the Spark Mesos driver webui_url for interacting with the framework.
    If unset it will point to Spark's internal web UI.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.driverEnv.[EnvironmentVariableName]</code></td>
  <td><code>(none)</code></td>
  <td>
    This only affects drivers submitted in cluster mode.  Add the
    environment variable specified by EnvironmentVariableName to the
    driver process. The user can specify multiple of these to set
    multiple environment variables.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.dispatcher.webui.url</code></td>
  <td><code>(none)</code></td>
  <td>
    Set the Spark Mesos dispatcher webui_url for interacting with the framework.
    If unset it will point to Spark's internal web UI.
  </td>
  </tr>
<tr>
  <td><code>spark.mesos.dispatcher.driverDefault.[PropertyName]</code></td>
  <td><code>(none)</code></td>
  <td>
    Set default properties for drivers submitted through the
    dispatcher.  For example,
    spark.mesos.dispatcher.driverProperty.spark.executor.memory=32g
    results in the executors for all drivers submitted in cluster mode
    to run in 32g containers.
</td>
</tr>
<tr>
  <td><code>spark.mesos.dispatcher.historyServer.url</code></td>
  <td><code>(none)</code></td>
  <td>
    Set the URL of the <a href="http://spark.apache.org/docs/latest/monitoring.html#viewing-after-the-fact">history
    server</a>.  The dispatcher will then link each driver to its entry
    in the history server.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.gpus.max</code></td>
  <td><code>0</code></td>
  <td>
    Set the maximum number GPU resources to acquire for this job. Note that executors will still launch when no GPU resources are found
    since this configuration is just a upper limit and not a guaranteed amount.
  </td>
  </tr>
<tr>
  <td><code>spark.mesos.network.name</code></td>
  <td><code>(none)</code></td>
  <td>
    Attach containers to the given named network.  If this job is
    launched in cluster mode, also launch the driver in the given named
    network.  See
    <a href="http://mesos.apache.org/documentation/latest/cni/">the Mesos CNI docs</a>
    for more details.
  </td>
</tr>
<tr>
  <td><code>spark.mesos.fetcherCache.enable</code></td>
  <td><code>false</code></td>
  <td>
    If set to `true`, all URIs (example: `spark.executor.uri`,
    `spark.mesos.uris`) will be cached by the <a
    href="http://mesos.apache.org/documentation/latest/fetcher/">Mesos
    Fetcher Cache</a>
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
