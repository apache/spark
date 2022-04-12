---
layout: global
title: Running Spark on YARN
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---
* This will become a table of contents (this text will be scraped).
{:toc}

Support for running on [YARN (Hadoop
NextGen)](http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html)
was added to Spark in version 0.6.0, and improved in subsequent releases.

# Security

Security features like authentication are not enabled by default. When deploying a cluster that is open to the internet
or an untrusted network, it's important to secure access to the cluster to prevent unauthorized applications
from running on the cluster.
Please see [Spark Security](security.html) and the specific security sections in this doc before running Spark.

# Launching Spark on YARN

Ensure that `HADOOP_CONF_DIR` or `YARN_CONF_DIR` points to the directory which contains the (client side) configuration files for the Hadoop cluster.
These configs are used to write to HDFS and connect to the YARN ResourceManager. The
configuration contained in this directory will be distributed to the YARN cluster so that all
containers used by the application use the same configuration. If the configuration references
Java system properties or environment variables not managed by YARN, they should also be set in the
Spark application's configuration (driver, executors, and the AM when running in client mode).

There are two deploy modes that can be used to launch Spark applications on YARN. In `cluster` mode, the Spark driver runs inside an application master process which is managed by YARN on the cluster, and the client can go away after initiating the application. In `client` mode, the driver runs in the client process, and the application master is only used for requesting resources from YARN.

Unlike other cluster managers supported by Spark in which the master's address is specified in the `--master`
parameter, in YARN mode the ResourceManager's address is picked up from the Hadoop configuration.
Thus, the `--master` parameter is `yarn`.

To launch a Spark application in `cluster` mode:

    $ ./bin/spark-submit --class path.to.your.Class --master yarn --deploy-mode cluster [options] <app jar> [app options]

For example:

    $ ./bin/spark-submit --class org.apache.spark.examples.SparkPi \
        --master yarn \
        --deploy-mode cluster \
        --driver-memory 4g \
        --executor-memory 2g \
        --executor-cores 1 \
        --queue thequeue \
        examples/jars/spark-examples*.jar \
        10

The above starts a YARN client program which starts the default Application Master. Then SparkPi will be run as a child thread of Application Master. The client will periodically poll the Application Master for status updates and display them in the console. The client will exit once your application has finished running.  Refer to the [Debugging your Application](#debugging-your-application) section below for how to see driver and executor logs.

To launch a Spark application in `client` mode, do the same, but replace `cluster` with `client`. The following shows how you can run `spark-shell` in `client` mode:

    $ ./bin/spark-shell --master yarn --deploy-mode client

## Adding Other JARs

In `cluster` mode, the driver runs on a different machine than the client, so `SparkContext.addJar` won't work out of the box with files that are local to the client. To make files on the client available to `SparkContext.addJar`, include them with the `--jars` option in the launch command.

    $ ./bin/spark-submit --class my.main.Class \
        --master yarn \
        --deploy-mode cluster \
        --jars my-other-jar.jar,my-other-other-jar.jar \
        my-main-jar.jar \
        app_arg1 app_arg2


# Preparations

Running Spark on YARN requires a binary distribution of Spark which is built with YARN support.
Binary distributions can be downloaded from the [downloads page](https://spark.apache.org/downloads.html) of the project website.
There are two variants of Spark binary distributions you can download. One is pre-built with a certain
version of Apache Hadoop; this Spark distribution contains built-in Hadoop runtime, so we call it `with-hadoop` Spark
distribution. The other one is pre-built with user-provided Hadoop; since this Spark distribution
doesn't contain a built-in Hadoop runtime, it's smaller, but users have to provide a Hadoop installation separately.
We call this variant `no-hadoop` Spark distribution. For `with-hadoop` Spark distribution, since
it contains a built-in Hadoop runtime already, by default, when a job is submitted to Hadoop Yarn cluster, to prevent jar conflict, it will not
populate Yarn's classpath into Spark. To override this behavior, you can set <code>spark.yarn.populateHadoopClasspath=true</code>.
For `no-hadoop` Spark distribution, Spark will populate Yarn's classpath by default in order to get Hadoop runtime. For `with-hadoop` Spark distribution,
if your application depends on certain library that is only available in the cluster, you can try to populate the Yarn classpath by setting
the property mentioned above. If you run into jar conflict issue by doing so, you will need to turn it off and include this library
in your application jar.

To build Spark yourself, refer to [Building Spark](building-spark.html).

To make Spark runtime jars accessible from YARN side, you can specify `spark.yarn.archive` or `spark.yarn.jars`. For details please refer to [Spark Properties](#spark-properties). If neither `spark.yarn.archive` nor `spark.yarn.jars` is specified, Spark will create a zip file with all jars under `$SPARK_HOME/jars` and upload it to the distributed cache.

# Configuration

Most of the configs are the same for Spark on YARN as for other deployment modes. See the [configuration page](configuration.html) for more information on those.  These are configs that are specific to Spark on YARN.

# Debugging your Application

In YARN terminology, executors and application masters run inside "containers". YARN has two modes for handling container logs after an application has completed. If log aggregation is turned on (with the `yarn.log-aggregation-enable` config), container logs are copied to HDFS and deleted on the local machine. These logs can be viewed from anywhere on the cluster with the `yarn logs` command.

    yarn logs -applicationId <app ID>

will print out the contents of all log files from all containers from the given application. You can also view the container log files directly in HDFS using the HDFS shell or API. The directory where they are located can be found by looking at your YARN configs (`yarn.nodemanager.remote-app-log-dir` and `yarn.nodemanager.remote-app-log-dir-suffix`). The logs are also available on the Spark Web UI under the Executors Tab. You need to have both the Spark history server and the MapReduce history server running and configure `yarn.log.server.url` in `yarn-site.xml` properly. The log URL on the Spark history server UI will redirect you to the MapReduce history server to show the aggregated logs.

When log aggregation isn't turned on, logs are retained locally on each machine under `YARN_APP_LOGS_DIR`, which is usually configured to `/tmp/logs` or `$HADOOP_HOME/logs/userlogs` depending on the Hadoop version and installation. Viewing logs for a container requires going to the host that contains them and looking in this directory.  Subdirectories organize log files by application ID and container ID. The logs are also available on the Spark Web UI under the Executors Tab and doesn't require running the MapReduce history server.

To review per-container launch environment, increase `yarn.nodemanager.delete.debug-delay-sec` to a
large value (e.g. `36000`), and then access the application cache through `yarn.nodemanager.local-dirs`
on the nodes on which containers are launched. This directory contains the launch script, JARs, and
all environment variables used for launching each container. This process is useful for debugging
classpath problems in particular. (Note that enabling this requires admin privileges on cluster
settings and a restart of all node managers. Thus, this is not applicable to hosted clusters).

To use a custom log4j configuration for the application master or executors, here are the options:

- upload a custom `log4j.properties` using `spark-submit`, by adding it to the `--files` list of files
  to be uploaded with the application.
- add `-Dlog4j.configuration=<location of configuration file>` to `spark.driver.extraJavaOptions`
  (for the driver) or `spark.executor.extraJavaOptions` (for executors). Note that if using a file,
  the `file:` protocol should be explicitly provided, and the file needs to exist locally on all
  the nodes.
- update the `$SPARK_CONF_DIR/log4j.properties` file and it will be automatically uploaded along
  with the other configurations. Note that other 2 options has higher priority than this option if
  multiple options are specified.

Note that for the first option, both executors and the application master will share the same
log4j configuration, which may cause issues when they run on the same node (e.g. trying to write
to the same log file).

If you need a reference to the proper location to put log files in the YARN so that YARN can properly display and aggregate them, use `spark.yarn.app.container.log.dir` in your `log4j.properties`. For example, `log4j.appender.file_appender.File=${spark.yarn.app.container.log.dir}/spark.log`. For streaming applications, configuring `RollingFileAppender` and setting file location to YARN's log directory will avoid disk overflow caused by large log files, and logs can be accessed using YARN's log utility.

To use a custom metrics.properties for the application master and executors, update the `$SPARK_CONF_DIR/metrics.properties` file. It will automatically be uploaded with other configurations, so you don't need to specify it manually with `--files`.

#### Spark Properties

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.yarn.am.memory</code></td>
  <td><code>512m</code></td>
  <td>
    Amount of memory to use for the YARN Application Master in client mode, in the same format as JVM memory strings (e.g. <code>512m</code>, <code>2g</code>).
    In cluster mode, use <code>spark.driver.memory</code> instead.
    <p/>
    Use lower-case suffixes, e.g. <code>k</code>, <code>m</code>, <code>g</code>, <code>t</code>, and <code>p</code>, for kibi-, mebi-, gibi-, tebi-, and pebibytes, respectively.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.yarn.am.resource.{resource-type}.amount</code></td>
  <td><code>(none)</code></td>
  <td>
    Amount of resource to use for the YARN Application Master in client mode.
    In cluster mode, use <code>spark.yarn.driver.resource.&lt;resource-type&gt;.amount</code> instead.
    Please note that this feature can be used only with YARN 3.0+
    For reference, see YARN Resource Model documentation: https://hadoop.apache.org/docs/r3.0.1/hadoop-yarn/hadoop-yarn-site/ResourceModel.html
    <p/>
    Example: 
    To request GPU resources from YARN, use: <code>spark.yarn.am.resource.yarn.io/gpu.amount</code>
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.applicationType</code></td>
  <td><code>SPARK</code></td>
  <td>
    Defines more specific application types, e.g. <code>SPARK</code>, <code>SPARK-SQL</code>, <code>SPARK-STREAMING</code>,
    <code>SPARK-MLLIB</code> and <code>SPARK-GRAPH</code>. Please be careful not to exceed 20 characters.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td><code>spark.yarn.driver.resource.{resource-type}.amount</code></td>
  <td><code>(none)</code></td>
  <td>
    Amount of resource to use for the YARN Application Master in cluster mode.
    Please note that this feature can be used only with YARN 3.0+
    For reference, see YARN Resource Model documentation: https://hadoop.apache.org/docs/r3.0.1/hadoop-yarn/hadoop-yarn-site/ResourceModel.html
    <p/>
    Example: 
    To request GPU resources from YARN, use: <code>spark.yarn.driver.resource.yarn.io/gpu.amount</code>
  </td>
  <td>3.0.0</td> 
</tr>
<tr>
  <td><code>spark.yarn.executor.resource.{resource-type}.amount</code></td>
  <td><code>(none)</code></td>
  <td>
    Amount of resource to use per executor process.
    Please note that this feature can be used only with YARN 3.0+
    For reference, see YARN Resource Model documentation: https://hadoop.apache.org/docs/r3.0.1/hadoop-yarn/hadoop-yarn-site/ResourceModel.html
    <p/>
    Example: 
    To request GPU resources from YARN, use: <code>spark.yarn.executor.resource.yarn.io/gpu.amount</code>
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.resourceGpuDeviceName</code></td>
  <td><code>yarn.io/gpu</code></td>
  <td>
    Specify the mapping of the Spark resource type of <code>gpu</code> to the YARN resource
    representing a GPU. By default YARN uses <code>yarn.io/gpu</code> but if YARN has been
    configured with a custom resource type, this allows remapping it.
    Applies when using the <code>spark.{driver/executor}.resource.gpu.*</code> configs.
  </td>
  <td>3.2.1</td>
</tr>
<tr>
  <td><code>spark.yarn.resourceFpgaDeviceName</code></td>
  <td><code>yarn.io/fpga</code></td>
  <td>
    Specify the mapping of the Spark resource type of <code>fpga</code> to the YARN resource
    representing a FPGA. By default YARN uses <code>yarn.io/fpga</code> but if YARN has been
    configured with a custom resource type, this allows remapping it.
    Applies when using the <code>spark.{driver/executor}.resource.fpga.*</code> configs.
  </td>
  <td>3.2.1</td>
</tr>
<tr>
  <td><code>spark.yarn.am.cores</code></td>
  <td><code>1</code></td>
  <td>
    Number of cores to use for the YARN Application Master in client mode.
    In cluster mode, use <code>spark.driver.cores</code> instead.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.yarn.am.waitTime</code></td>
  <td><code>100s</code></td>
  <td>
    Only used in <code>cluster</code> mode. Time for the YARN Application Master to wait for the
    SparkContext to be initialized.
  </td>
 <td>1.3.0</td> 
</tr>
<tr>
  <td><code>spark.yarn.submit.file.replication</code></td>
  <td>The default HDFS replication (usually <code>3</code>)</td>
  <td>
    HDFS replication level for the files uploaded into HDFS for the application. These include things like the Spark jar, the app jar, and any distributed cache files/archives.
  </td>
  <td>0.8.1</td>
</tr>
<tr>
  <td><code>spark.yarn.stagingDir</code></td>
  <td>Current user's home directory in the filesystem</td>
  <td>
    Staging directory used while submitting applications.
  </td>
 <td>2.0.0</td> 
</tr>
<tr>
  <td><code>spark.yarn.preserve.staging.files</code></td>
  <td><code>false</code></td>
  <td>
    Set to <code>true</code> to preserve the staged files (Spark jar, app jar, distributed cache files) at the end of the job rather than delete them.
  </td>
  <td>1.1.0</td> 
</tr>
<tr>
  <td><code>spark.yarn.scheduler.heartbeat.interval-ms</code></td>
  <td><code>3000</code></td>
  <td>
    The interval in ms in which the Spark application master heartbeats into the YARN ResourceManager.
    The value is capped at half the value of YARN's configuration for the expiry interval, i.e.
    <code>yarn.am.liveness-monitor.expiry-interval-ms</code>.
  </td>
  <td>0.8.1</td>
</tr>
<tr>
  <td><code>spark.yarn.scheduler.initial-allocation.interval</code></td>
  <td><code>200ms</code></td>
  <td>
    The initial interval in which the Spark application master eagerly heartbeats to the YARN ResourceManager
    when there are pending container allocation requests. It should be no larger than
    <code>spark.yarn.scheduler.heartbeat.interval-ms</code>. The allocation interval will doubled on
    successive eager heartbeats if pending containers still exist, until
    <code>spark.yarn.scheduler.heartbeat.interval-ms</code> is reached.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.yarn.max.executor.failures</code></td>
  <td>numExecutors * 2, with minimum of 3</td>
  <td>
    The maximum number of executor failures before failing the application.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.historyServer.address</code></td>
  <td>(none)</td>
  <td>
    The address of the Spark history server, e.g. <code>host.com:18080</code>. The address should not contain a scheme (<code>http://</code>). Defaults to not being set since the history server is an optional service. This address is given to the YARN ResourceManager when the Spark application finishes to link the application from the ResourceManager UI to the Spark history server UI.
    For this property, YARN properties can be used as variables, and these are substituted by Spark at runtime. For example, if the Spark history server runs on the same node as the YARN ResourceManager, it can be set to <code>${hadoopconf-yarn.resourcemanager.hostname}:18080</code>.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.dist.archives</code></td>
  <td>(none)</td>
  <td>
    Comma separated list of archives to be extracted into the working directory of each executor.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.dist.files</code></td>
  <td>(none)</td>
  <td>
    Comma-separated list of files to be placed in the working directory of each executor.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.dist.jars</code></td>
  <td>(none)</td>
  <td>
    Comma-separated list of jars to be placed in the working directory of each executor.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.dist.forceDownloadSchemes</code></td>
  <td><code>(none)</code></td>
  <td>
    Comma-separated list of schemes for which resources will be downloaded to the local disk prior to
    being added to YARN's distributed cache. For use in cases where the YARN service does not
    support schemes that are supported by Spark, like http, https and ftp, or jars required to be in the
    local YARN client's classpath. Wildcard '*' is denoted to download resources for all the schemes.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
 <td><code>spark.executor.instances</code></td>
  <td><code>2</code></td>
  <td>
    The number of executors for static allocation. With <code>spark.dynamicAllocation.enabled</code>, the initial set of executors will be at least this large.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.am.memoryOverhead</code></td>
  <td>AM memory * 0.10, with minimum of 384 </td>
  <td>
    Same as <code>spark.driver.memoryOverhead</code>, but for the YARN Application Master in client mode.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.yarn.queue</code></td>
  <td><code>default</code></td>
  <td>
    The name of the YARN queue to which the application is submitted.
  </td>
  <td>1.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.jars</code></td>
  <td>(none)</td>
  <td>
    List of libraries containing Spark code to distribute to YARN containers.
    By default, Spark on YARN will use Spark jars installed locally, but the Spark jars can also be
    in a world-readable location on HDFS. This allows YARN to cache it on nodes so that it doesn't
    need to be distributed each time an application runs. To point to jars on HDFS, for example,
    set this configuration to <code>hdfs:///some/path</code>. Globs are allowed.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.archive</code></td>
  <td>(none)</td>
  <td>
    An archive containing needed Spark jars for distribution to the YARN cache. If set, this
    configuration replaces <code>spark.yarn.jars</code> and the archive is used in all the
    application's containers. The archive should contain jar files in its root directory.
    Like with the previous option, the archive can also be hosted on HDFS to speed up file
    distribution.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.appMasterEnv.[EnvironmentVariableName]</code></td>
  <td>(none)</td>
  <td>
     Add the environment variable specified by <code>EnvironmentVariableName</code> to the
     Application Master process launched on YARN. The user can specify multiple of
     these and to set multiple environment variables. In <code>cluster</code> mode this controls
     the environment of the Spark driver and in <code>client</code> mode it only controls
     the environment of the executor launcher.
  </td>
  <td>1.1.0</td>
</tr>
<tr>
  <td><code>spark.yarn.containerLauncherMaxThreads</code></td>
  <td><code>25</code></td>
  <td>
    The maximum number of threads to use in the YARN Application Master for launching executor containers.
  </td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.yarn.am.extraJavaOptions</code></td>
  <td>(none)</td>
  <td>
  A string of extra JVM options to pass to the YARN Application Master in client mode.
  In cluster mode, use <code>spark.driver.extraJavaOptions</code> instead. Note that it is illegal
  to set maximum heap size (-Xmx) settings with this option. Maximum heap size settings can be set
  with <code>spark.yarn.am.memory</code>
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.yarn.am.extraLibraryPath</code></td>
  <td>(none)</td>
  <td>
    Set a special library path to use when launching the YARN Application Master in client mode.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.yarn.populateHadoopClasspath</code></td>
  <td>
    For <code>with-hadoop</code> Spark distribution, this is set to false; 
    for <code>no-hadoop</code> distribution, this is set to true.
  </td>
  <td>
    Whether to populate Hadoop classpath from <code>yarn.application.classpath</code> and
    <code>mapreduce.application.classpath</code> Note that if this is set to <code>false</code>, 
    it requires a <code>with-Hadoop</code> Spark distribution that bundles Hadoop runtime or
    user has to provide a Hadoop installation separately.
  </td>
  <td>2.4.6</td>
</tr>
<tr>
  <td><code>spark.yarn.maxAppAttempts</code></td>
  <td><code>yarn.resourcemanager.am.max-attempts</code> in YARN</td>
  <td>
  The maximum number of attempts that will be made to submit the application.
  It should be no larger than the global number of max attempts in the YARN configuration.
  </td>
  <td>1.3.0</td> 
</tr>
<tr>
  <td><code>spark.yarn.am.attemptFailuresValidityInterval</code></td>
  <td>(none)</td>
  <td>
  Defines the validity interval for AM failure tracking.
  If the AM has been running for at least the defined interval, the AM failure count will be reset.
  This feature is not enabled if not configured.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.yarn.am.clientModeTreatDisconnectAsFailed</code></td>
  <td>false</td>
  <td>
  Treat yarn-client unclean disconnects as failures. In yarn-client mode, normally the application will always finish
  with a final status of SUCCESS because in some cases, it is not possible to know if the Application was terminated
  intentionally by the user or if there was a real error. This config changes that behavior such that if the Application
  Master disconnects from the driver uncleanly (ie without the proper shutdown handshake) the application will
  terminate with a final status of FAILED. This will allow the caller to decide if it was truly a failure. Note that if
  this config is set and the user just terminate the client application badly it may show a status of FAILED when it wasn't really FAILED.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.yarn.am.clientModeExitOnError</code></td>
  <td>false</td>
  <td>
  In yarn-client mode, when this is true, if driver got application report with final status of KILLED or FAILED,
  driver will stop corresponding SparkContext and exit program with code 1.
  Note, if this is true and called from another application, it will terminate the parent application as well.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.yarn.executor.failuresValidityInterval</code></td>
  <td>(none)</td>
  <td>
  Defines the validity interval for executor failure tracking.
  Executor failures which are older than the validity interval will be ignored.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.submit.waitAppCompletion</code></td>
  <td><code>true</code></td>
  <td>
  In YARN cluster mode, controls whether the client waits to exit until the application completes.
  If set to <code>true</code>, the client process will stay alive reporting the application's status.
  Otherwise, the client process will exit after submission.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.yarn.am.nodeLabelExpression</code></td>
  <td>(none)</td>
  <td>
  A YARN node label expression that restricts the set of nodes AM will be scheduled on.
  Only versions of YARN greater than or equal to 2.6 support node label expressions, so when
  running against earlier versions, this property will be ignored.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.yarn.executor.nodeLabelExpression</code></td>
  <td>(none)</td>
  <td>
  A YARN node label expression that restricts the set of nodes executors will be scheduled on.
  Only versions of YARN greater than or equal to 2.6 support node label expressions, so when
  running against earlier versions, this property will be ignored.
  </td>
  <td>1.4.0</td>
</tr>
<tr>
  <td><code>spark.yarn.tags</code></td>
  <td>(none)</td>
  <td>
  Comma-separated list of strings to pass through as YARN application tags appearing
  in YARN ApplicationReports, which can be used for filtering when querying YARN apps.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.yarn.priority</code></td>
  <td>(none)</td>
  <td>
  Application priority for YARN to define pending applications ordering policy, those with higher
  integer value have a better opportunity to be activated. Currently, YARN only supports application
  priority when using FIFO ordering policy.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.config.gatewayPath</code></td>
  <td>(none)</td>
  <td>
  A path that is valid on the gateway host (the host where a Spark application is started) but may
  differ for paths for the same resource in other nodes in the cluster. Coupled with
  <code>spark.yarn.config.replacementPath</code>, this is used to support clusters with
  heterogeneous configurations, so that Spark can correctly launch remote processes.
  <p/>
  The replacement path normally will contain a reference to some environment variable exported by
  YARN (and, thus, visible to Spark containers).
  <p/>
  For example, if the gateway node has Hadoop libraries installed on <code>/disk1/hadoop</code>, and
  the location of the Hadoop install is exported by YARN as the  <code>HADOOP_HOME</code>
  environment variable, setting this value to <code>/disk1/hadoop</code> and the replacement path to
  <code>$HADOOP_HOME</code> will make sure that paths used to launch remote processes properly
  reference the local YARN configuration.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.yarn.config.replacementPath</code></td>
  <td>(none)</td>
  <td>
  See <code>spark.yarn.config.gatewayPath</code>.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.yarn.rolledLog.includePattern</code></td>
  <td>(none)</td>
  <td>
  Java Regex to filter the log files which match the defined include pattern
  and those log files will be aggregated in a rolling fashion.
  This will be used with YARN's rolling log aggregation, to enable this feature in YARN side
  <code>yarn.nodemanager.log-aggregation.roll-monitoring-interval-seconds</code> should be
  configured in yarn-site.xml. The Spark log4j appender needs be changed to use
  FileAppender or another appender that can handle the files being removed while it is running. Based
  on the file name configured in the log4j configuration (like spark.log), the user should set the
  regex (spark*) to include all the log files that need to be aggregated.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.rolledLog.excludePattern</code></td>
  <td>(none)</td>
  <td>
  Java Regex to filter the log files which match the defined exclude pattern
  and those log files will not be aggregated in a rolling fashion. If the log file
  name matches both the include and the exclude pattern, this file will be excluded eventually.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.executor.launch.excludeOnFailure.enabled</code></td>
  <td>false</td>
  <td>
  Flag to enable exclusion of nodes having YARN resource allocation problems.
  The error limit for excluding can be configured by
  <code>spark.excludeOnFailure.application.maxFailedExecutorsPerNode</code>.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.yarn.exclude.nodes</code></td>
  <td>(none)</td>
  <td>
  Comma-separated list of YARN node names which are excluded from resource allocation.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.metrics.namespace</code></td>
  <td>(none)</td>
  <td>
  The root namespace for AM metrics reporting. 
  If it is not set then the YARN application ID is used.
  </td>
  <td>2.4.0</td>
</tr>
</table>

#### Available patterns for SHS custom executor log URL

<table class="table">
    <tr><th>Pattern</th><th>Meaning</th></tr>
    <tr>
      <td>&#123;&#123;HTTP_SCHEME&#125;&#125;</td>
      <td><code>http://</code> or <code>https://</code> according to YARN HTTP policy. (Configured via <code>yarn.http.policy</code>)</td>
    </tr>
    <tr>
      <td>&#123;&#123;NM_HOST&#125;&#125;</td>
      <td>The "host" of node where container was run.</td>
    </tr>
    <tr>
      <td>&#123;&#123;NM_PORT&#125;&#125;</td>
      <td>The "port" of node manager where container was run.</td>
    </tr>
    <tr>
      <td>&#123;&#123;NM_HTTP_PORT&#125;&#125;</td>
      <td>The "port" of node manager's http server where container was run.</td>
    </tr>
    <tr>
      <td>&#123;&#123;NM_HTTP_ADDRESS&#125;&#125;</td>
      <td>Http URI of the node on which the container is allocated.</td>
    </tr>
    <tr>
      <td>&#123;&#123;CLUSTER_ID&#125;&#125;</td>
      <td>The cluster ID of Resource Manager. (Configured via <code>yarn.resourcemanager.cluster-id</code>)</td>
    </tr>
    <tr>
      <td>&#123;&#123;CONTAINER_ID&#125;&#125;</td>
      <td>The ID of container.</td>
    </tr>
    <tr>
      <td>&#123;&#123;USER&#125;&#125;</td>
      <td><code>SPARK_USER</code> on system environment.</td>
    </tr>
    <tr>
      <td>&#123;&#123;FILE_NAME&#125;&#125;</td>
      <td><code>stdout</code>, <code>stderr</code>.</td>
    </tr>
</table>

For example, suppose you would like to point log url link to Job History Server directly instead of let NodeManager http server redirects it, you can configure `spark.history.custom.executor.log.url` as below:

<code>&#123;&#123;HTTP_SCHEME&#125;&#125;&lt;JHS_HOST&gt;:&lt;JHS_PORT&gt;/jobhistory/logs/&#123;&#123;NM_HOST&#125;&#125;:&#123;&#123;NM_PORT&#125;&#125;/&#123;&#123;CONTAINER_ID&#125;&#125;/&#123;&#123;CONTAINER_ID&#125;&#125;/&#123;&#123;USER&#125;&#125;/&#123;&#123;FILE_NAME&#125;&#125;?start=-4096</code>

NOTE: you need to replace `<JHS_HOST>` and `<JHS_PORT>` with actual value.

# Resource Allocation and Configuration Overview

Please make sure to have read the Custom Resource Scheduling and Configuration Overview section on the [configuration page](configuration.html). This section only talks about the YARN specific aspects of resource scheduling.

YARN needs to be configured to support any resources the user wants to use with Spark. Resource scheduling on YARN was added in YARN 3.1.0. See the YARN documentation for more information on configuring resources and properly setting up isolation. Ideally the resources are setup isolated so that an executor can only see the resources it was allocated. If you do not have isolation enabled, the user is responsible for creating a discovery script that ensures the resource is not shared between executors.

YARN supports user defined resource types but has built in types for GPU (<code>yarn.io/gpu</code>) and FPGA (<code>yarn.io/fpga</code>). For that reason, if you are using either of those resources, Spark can translate your request for spark resources into YARN resources and you only have to specify the <code>spark.{driver/executor}.resource.</code> configs. Note, if you are using a custom resource type for GPUs or FPGAs with YARN you can change the Spark mapping using <code>spark.yarn.resourceGpuDeviceName</code> and <code>spark.yarn.resourceFpgaDeviceName</code>.
 If you are using a resource other then FPGA or GPU, the user is responsible for specifying the configs for both YARN (<code>spark.yarn.{driver/executor}.resource.</code>) and Spark (<code>spark.{driver/executor}.resource.</code>).

For example, the user wants to request 2 GPUs for each executor. The user can just specify <code>spark.executor.resource.gpu.amount=2</code> and Spark will handle requesting <code>yarn.io/gpu</code> resource type from YARN.

If the user has a user defined YARN resource, lets call it `acceleratorX` then the user must specify <code>spark.yarn.executor.resource.acceleratorX.amount=2</code> and <code>spark.executor.resource.acceleratorX.amount=2</code>.

YARN does not tell Spark the addresses of the resources allocated to each container. For that reason, the user must specify a discovery script that gets run by the executor on startup to discover what resources are available to that executor. You can find an example scripts in `examples/src/main/scripts/getGpusResources.sh`. The script must have execute permissions set and the user should setup permissions to not allow malicious users to modify it. The script should write to STDOUT a JSON string in the format of the ResourceInformation class. This has the resource name and an array of resource addresses available to just that executor.

# Stage Level Scheduling Overview

Stage level scheduling is supported on YARN when dynamic allocation is enabled. One thing to note that is YARN specific is that each ResourceProfile requires a different container priority on YARN. The mapping is simply the ResourceProfile id becomes the priority, on YARN lower numbers are higher priority. This means that profiles created earlier will have a higher priority in YARN. Normally this won't matter as Spark finishes one stage before starting another one, the only case this might have an affect is in a job server type scenario, so its something to keep in mind.
Note there is a difference in the way custom resources are handled between the base default profile and custom ResourceProfiles. To allow for the user to request YARN containers with extra resources without Spark scheduling on them, the user can specify resources via the <code>spark.yarn.executor.resource.</code> config. Those configs are only used in the base default profile though and do not get propagated into any other custom ResourceProfiles. This is because there would be no way to remove them if you wanted a stage to not have them. This results in your default profile getting custom resources defined in <code>spark.yarn.executor.resource.</code> plus spark defined resources of GPU or FPGA. Spark converts GPU and FPGA resources into the YARN built in types <code>yarn.io/gpu</code>) and <code>yarn.io/fpga</code>, but does not know the mapping of any other resources. Any other Spark custom resources are not propagated to YARN for the default profile. So if you want Spark to schedule based off a custom resource and have it requested from YARN, you must specify it in both YARN (<code>spark.yarn.{driver/executor}.resource.</code>) and Spark (<code>spark.{driver/executor}.resource.</code>) configs. Leave the Spark config off if you only want YARN containers with the extra resources but Spark not to schedule using them. Now for custom ResourceProfiles, it doesn't currently have a way to only specify YARN resources without Spark scheduling off of them. This means for custom ResourceProfiles we propagate all the resources defined in the ResourceProfile to YARN. We still convert GPU and FPGA to the YARN build in types as well. This requires that the name of any custom resources you specify match what they are defined as in YARN.

# Important notes

- Whether core requests are honored in scheduling decisions depends on which scheduler is in use and how it is configured.
- In `cluster` mode, the local directories used by the Spark executors and the Spark driver will be the local directories configured for YARN (Hadoop YARN config `yarn.nodemanager.local-dirs`). If the user specifies `spark.local.dir`, it will be ignored. In `client` mode, the Spark executors will use the local directories configured for YARN while the Spark driver will use those defined in `spark.local.dir`. This is because the Spark driver does not run on the YARN cluster in `client` mode, only the Spark executors do.
- The `--files` and `--archives` options support specifying file names with the # similar to Hadoop. For example, you can specify: `--files localtest.txt#appSees.txt` and this will upload the file you have locally named `localtest.txt` into HDFS but this will be linked to by the name `appSees.txt`, and your application should use the name as `appSees.txt` to reference it when running on YARN.
- The `--jars` option allows the `SparkContext.addJar` function to work if you are using it with local files and running in `cluster` mode. It does not need to be used if you are using it with HDFS, HTTP, HTTPS, or FTP files.

# Kerberos

Standard Kerberos support in Spark is covered in the [Security](security.html#kerberos) page.

In YARN mode, when accessing Hadoop file systems, aside from the default file system in the hadoop
configuration, Spark will also automatically obtain delegation tokens for the service hosting the
staging directory of the Spark application.

## YARN-specific Kerberos Configuration

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
<tr>
  <td><code>spark.kerberos.keytab</code></td>
  <td>(none)</td>
  <td>
  The full path to the file that contains the keytab for the principal specified above. This keytab
  will be copied to the node running the YARN Application Master via the YARN Distributed Cache, and
  will be used for renewing the login tickets and the delegation tokens periodically. Equivalent to
  the <code>--keytab</code> command line argument.

  <br /> (Works also with the "local" master.)
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.kerberos.principal</code></td>
  <td>(none)</td>
  <td>
  Principal to be used to login to KDC, while running on secure clusters. Equivalent to the
  <code>--principal</code> command line argument.

  <br /> (Works also with the "local" master.)
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td><code>spark.yarn.kerberos.relogin.period</code></td>
  <td>1m</td>
  <td>
  How often to check whether the kerberos TGT should be renewed. This should be set to a value
  that is shorter than the TGT renewal period (or the TGT lifetime if TGT renewal is not enabled).
  The default value should be enough for most deployments.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.yarn.kerberos.renewal.excludeHadoopFileSystems</code></td>
  <td>(none)</td>
  <td>
    A comma-separated list of Hadoop filesystems for whose hosts will be excluded from from delegation
    token renewal at resource scheduler. For example, <code>spark.yarn.kerberos.renewal.excludeHadoopFileSystems=hdfs://nn1.com:8032,
    hdfs://nn2.com:8032</code>. This is known to work under YARN for now, so YARN Resource Manager won't renew tokens for the application.
    Note that as resource scheduler does not renew token, so any application running longer than the original token expiration that tries
    to use that token will likely fail.
  </td>
  <td>3.2.0</td>
</tr>
</table>

## Troubleshooting Kerberos

Debugging Hadoop/Kerberos problems can be "difficult". One useful technique is to
enable extra logging of Kerberos operations in Hadoop by setting the `HADOOP_JAAS_DEBUG`
environment variable.

```bash
export HADOOP_JAAS_DEBUG=true
```

The JDK classes can be configured to enable extra logging of their Kerberos and
SPNEGO/REST authentication via the system properties `sun.security.krb5.debug`
and `sun.security.spnego.debug=true`

```
-Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true
```

All these options can be enabled in the Application Master:

```
spark.yarn.appMasterEnv.HADOOP_JAAS_DEBUG true
spark.yarn.am.extraJavaOptions -Dsun.security.krb5.debug=true -Dsun.security.spnego.debug=true
```

Finally, if the log level for `org.apache.spark.deploy.yarn.Client` is set to `DEBUG`, the log
will include a list of all tokens obtained, and their expiry details


# Configuring the External Shuffle Service

To start the Spark Shuffle Service on each `NodeManager` in your YARN cluster, follow these
instructions:

1. Build Spark with the [YARN profile](building-spark.html). Skip this step if you are using a
pre-packaged distribution.
1. Locate the `spark-<version>-yarn-shuffle.jar`. This should be under
`$SPARK_HOME/common/network-yarn/target/scala-<version>` if you are building Spark yourself, and under
`yarn` if you are using a distribution.
1. Add this jar to the classpath of all `NodeManager`s in your cluster.
1. In the `yarn-site.xml` on each node, add `spark_shuffle` to `yarn.nodemanager.aux-services`,
then set `yarn.nodemanager.aux-services.spark_shuffle.class` to
`org.apache.spark.network.yarn.YarnShuffleService`.
1. Increase `NodeManager's` heap size by setting `YARN_HEAPSIZE` (1000 by default) in `etc/hadoop/yarn-env.sh`
to avoid garbage collection issues during shuffle.
1. Restart all `NodeManager`s in your cluster.

The following extra configuration options are available when the shuffle service is running on YARN:

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.yarn.shuffle.stopOnFailure</code></td>
  <td><code>false</code></td>
  <td>
    Whether to stop the NodeManager when there's a failure in the Spark Shuffle Service's
    initialization. This prevents application failures caused by running containers on
    NodeManagers where the Spark Shuffle Service is not running.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.shuffle.service.metrics.namespace</code></td>
  <td><code>sparkShuffleService</code></td>
  <td>
    The namespace to use when emitting shuffle service metrics into Hadoop metrics2 system of the
    NodeManager.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.shuffle.service.logs.namespace</code></td>
  <td><code>(not set)</code></td>
  <td>
    A namespace which will be appended to the class name when forming the logger name to use for
    emitting logs from the YARN shuffle service, like
    <code>org.apache.spark.network.yarn.YarnShuffleService.logsNamespaceValue</code>. Since some logging frameworks
    may expect the logger name to look like a class name, it's generally recommended to provide a value which
    would be a valid Java package or class name and not include spaces.
  </td>
</tr>
</table>

Please note that the instructions above assume that the default shuffle service name,
`spark_shuffle`, has been used. It is possible to use any name here, but the values used in the
YARN NodeManager configurations must match the value of `spark.shuffle.service.name` in the
Spark application.

The shuffle service will, by default, take all of its configurations from the Hadoop Configuration
used by the NodeManager (e.g. `yarn-site.xml`). However, it is also possible to configure the
shuffle service independently using a file named `spark-shuffle-site.xml` which should be placed
onto the classpath of the shuffle service (which is, by default, shared with the classpath of the
NodeManager). The shuffle service will treat this as a standard Hadoop Configuration resource and
overlay it on top of the NodeManager's configuration.

# Launching your application with Apache Oozie

Apache Oozie can launch Spark applications as part of a workflow.
In a secure cluster, the launched application will need the relevant tokens to access the cluster's
services. If Spark is launched with a keytab, this is automatic.
However, if Spark is to be launched without a keytab, the responsibility for setting up security
must be handed over to Oozie.

The details of configuring Oozie for secure clusters and obtaining
credentials for a job can be found on the [Oozie web site](http://oozie.apache.org/)
in the "Authentication" section of the specific release's documentation.

For Spark applications, the Oozie workflow must be set up for Oozie to request all tokens which
the application needs, including:

- The YARN resource manager.
- The local Hadoop filesystem.
- Any remote Hadoop filesystems used as a source or destination of I/O.
- Hive —if used.
- HBase —if used.
- The YARN timeline server, if the application interacts with this.

To avoid Spark attempting —and then failing— to obtain Hive, HBase and remote HDFS tokens,
the Spark configuration must be set to disable token collection for the services.

The Spark configuration must include the lines:

```
spark.security.credentials.hive.enabled   false
spark.security.credentials.hbase.enabled  false
```

The configuration option `spark.kerberos.access.hadoopFileSystems` must be unset.

# Using the Spark History Server to replace the Spark Web UI

It is possible to use the Spark History Server application page as the tracking URL for running
applications when the application UI is disabled. This may be desirable on secure clusters, or to
reduce the memory usage of the Spark driver. To set up tracking through the Spark History Server,
do the following:

- On the application side, set <code>spark.yarn.historyServer.allowTracking=true</code> in Spark's
  configuration. This will tell Spark to use the history server's URL as the tracking URL if
  the application's UI is disabled.
- On the Spark History Server, add <code>org.apache.spark.deploy.yarn.YarnProxyRedirectFilter</code>
  to the list of filters in the <code>spark.ui.filters</code> configuration.

Be aware that the history server information may not be up-to-date with the application's state.

# Running multiple versions of the Spark Shuffle Service

Please note that this section only applies when running on YARN versions >= 2.9.0.

In some cases it may be desirable to run multiple instances of the Spark Shuffle Service which are
using different versions of Spark. This can be helpful, for example, when running a YARN cluster
with a mixed workload of applications running multiple Spark versions, since a given version of
the shuffle service is not always compatible with other versions of Spark. YARN versions since 2.9.0
support the ability to run shuffle services within an isolated classloader
(see [YARN-4577](https://issues.apache.org/jira/browse/YARN-4577)), meaning multiple Spark versions
can coexist within a single NodeManager. The
`yarn.nodemanager.aux-services.<service-name>.classpath` and, starting from YARN 2.10.2/3.1.1/3.2.0,
`yarn.nodemanager.aux-services.<service-name>.remote-classpath` options can be used to configure
this. Note that YARN 3.3.0/3.3.1 have an issue which requires setting
`yarn.nodemanager.aux-services.<service-name>.system-classes` as a workaround. See
[YARN-11053](https://issues.apache.org/jira/browse/YARN-11053) for details. In addition to setting
up separate classpaths, it's necessary to ensure the two versions advertise to different ports.
This can be achieved using the `spark-shuffle-site.xml` file described above. For example, you may
have configuration like:

```properties
  yarn.nodemanager.aux-services = spark_shuffle_x,spark_shuffle_y
  yarn.nodemanager.aux-services.spark_shuffle_x.classpath = /path/to/spark-x-path/fat.jar:/path/to/spark-x-config
  yarn.nodemanager.aux-services.spark_shuffle_y.classpath = /path/to/spark-y-path/fat.jar:/path/to/spark-y-config
```
Or
```properties
  yarn.nodemanager.aux-services = spark_shuffle_x,spark_shuffle_y
  yarn.nodemanager.aux-services.spark_shuffle_x.classpath = /path/to/spark-x-path/*:/path/to/spark-x-config
  yarn.nodemanager.aux-services.spark_shuffle_y.classpath = /path/to/spark-y-path/*:/path/to/spark-y-config
```

The two `spark-*-config` directories each contain one file, `spark-shuffle-site.xml`. These are XML
files in the [Hadoop Configuration format](https://hadoop.apache.org/docs/r3.2.2/api/org/apache/hadoop/conf/Configuration.html)
which each contain a few configurations to adjust the port number and metrics name prefix used:
```xml
<configuration>
  <property>
    <name>spark.shuffle.service.port</name>
    <value>7001</value>
  </property>
  <property>
    <name>spark.yarn.shuffle.service.metrics.namespace</name>
    <value>sparkShuffleServiceX</value>
  </property>
</configuration>
```
The values should both be different for the two different services.

Then, in the configuration of the Spark applications, one should be configured with:
```properties
  spark.shuffle.service.name = spark_shuffle_x
  spark.shuffle.service.port = 7001
```
and one should be configured with:
```properties
  spark.shuffle.service.name = spark_shuffle_y
  spark.shuffle.service.port = <other value>
```
