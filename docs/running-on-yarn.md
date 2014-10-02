---
layout: global
title: Running Spark on YARN
---

Support for running on [YARN (Hadoop
NextGen)](http://hadoop.apache.org/docs/r2.0.2-alpha/hadoop-yarn/hadoop-yarn-site/YARN.html)
was added to Spark in version 0.6.0, and improved in subsequent releases.

# Preparations

Running Spark-on-YARN requires a binary distribution of Spark which is built with YARN support.
Binary distributions can be downloaded from the Spark project website. 
To build Spark yourself, refer to [Building Spark](building-spark.html).

# Configuration

Most of the configs are the same for Spark on YARN as for other deployment modes. See the [configuration page](configuration.html) for more information on those.  These are configs that are specific to Spark on YARN.

#### Spark Properties

<table class="table">
<tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
<tr>
  <td><code>spark.yarn.applicationMaster.waitTries</code></td>
  <td>10</td>
  <td>
    Set the number of times the ApplicationMaster waits for the the Spark master and then also the number of tries it waits for the SparkContext to be initialized
  </td>
</tr>
<tr>
  <td><code>spark.yarn.submit.file.replication</code></td>
  <td>3</td>
  <td>
    HDFS replication level for the files uploaded into HDFS for the application. These include things like the Spark jar, the app jar, and any distributed cache files/archives.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.preserve.staging.files</code></td>
  <td>false</td>
  <td>
    Set to true to preserve the staged files (Spark jar, app jar, distributed cache files) at the end of the job rather then delete them.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.scheduler.heartbeat.interval-ms</code></td>
  <td>5000</td>
  <td>
    The interval in ms in which the Spark application master heartbeats into the YARN ResourceManager.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.max.executor.failures</code></td>
  <td>numExecutors * 2, with minimum of 3</td>
  <td>
    The maximum number of executor failures before failing the application.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.historyServer.address</code></td>
  <td>(none)</td>
  <td>
    The address of the Spark history server (i.e. host.com:18080). The address should not contain a scheme (http://). Defaults to not being set since the history server is an optional service. This address is given to the YARN ResourceManager when the Spark application finishes to link the application from the ResourceManager UI to the Spark history server UI.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.dist.archives</code></td>
  <td>(none)</td>
  <td>
    Comma separated list of archives to be extracted into the working directory of each executor.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.dist.files</code></td>
  <td>(none)</td>
  <td>
    Comma-separated list of files to be placed in the working directory of each executor.
  </td>
</tr>
<tr>
 <td><code>spark.yarn.executor.memoryOverhead</code></td>
  <td>executorMemory * 0.07, with minimum of 384 </td>
  <td>
    The amount of off heap memory (in megabytes) to be allocated per executor. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to grow with the executor size (typically 6-10%).
  </td>
</tr>
<tr>
  <td><code>spark.yarn.driver.memoryOverhead</code></td>
  <td>driverMemory * 0.07, with minimum of 384 </td>
  <td>
    The amount of off heap memory (in megabytes) to be allocated per driver. This is memory that accounts for things like VM overheads, interned strings, other native overheads, etc. This tends to grow with the container size (typically 6-10%).
  </td>
</tr>
<tr>
  <td><code>spark.yarn.jar</code></td>
  <td>(none)</td>
  <td>
    The location of the Spark jar file, in case overriding the default location is desired.
    By default, Spark on YARN will use a Spark jar installed locally, but the Spark jar can also be
    in a world-readable location on HDFS. This allows YARN to cache it on nodes so that it doesn't
    need to be distributed each time an application runs. To point to a jar on HDFS, for example,
    set this configuration to "hdfs:///some/path".
  </td>
</tr>
<tr>
  <td><code>spark.yarn.access.namenodes</code></td>
  <td>(none)</td>
  <td>
    A list of secure HDFS namenodes your Spark application is going to access. For 
    example, `spark.yarn.access.namenodes=hdfs://nn1.com:8032,hdfs://nn2.com:8032`. 
    The Spark application must have acess to the namenodes listed and Kerberos must 
    be properly configured to be able to access them (either in the same realm or in 
    a trusted realm). Spark acquires security tokens for each of the namenodes so that 
    the Spark application can access those remote HDFS clusters.
  </td>
</tr>
<tr>
  <td><code>spark.yarn.appMasterEnv.[EnvironmentVariableName]</code></td>
  <td>(none)</td>
  <td>
     Add the environment variable specified by <code>EnvironmentVariableName</code> to the 
     Application Master process launched on YARN. The user can specify multiple of 
     these and to set multiple environment variables. In yarn-cluster mode this controls 
     the environment of the SPARK driver and in yarn-client mode it only controls 
     the environment of the executor launcher. 
  </td>
</tr>
<tr>
  <td><code>spark.yarn.containerLauncherMaxThreads</code></td>
  <td>25</td>
  <td>
    The maximum number of threads to use in the application master for launching executor containers.
  </td>
</tr>
</table>

# Launching Spark on YARN

Ensure that `HADOOP_CONF_DIR` or `YARN_CONF_DIR` points to the directory which contains the (client side) configuration files for the Hadoop cluster.
These configs are used to write to the dfs and connect to the YARN ResourceManager.

There are two deploy modes that can be used to launch Spark applications on YARN. In yarn-cluster mode, the Spark driver runs inside an application master process which is managed by YARN on the cluster, and the client can go away after initiating the application. In yarn-client mode, the driver runs in the client process, and the application master is only used for requesting resources from YARN.

Unlike in Spark standalone and Mesos mode, in which the master's address is specified in the "master" parameter, in YARN mode the ResourceManager's address is picked up from the Hadoop configuration.  Thus, the master parameter is simply "yarn-client" or "yarn-cluster".

To launch a Spark application in yarn-cluster mode:

    ./bin/spark-submit --class path.to.your.Class --master yarn-cluster [options] <app jar> [app options]
    
For example:

    $ ./bin/spark-submit --class org.apache.spark.examples.SparkPi \
        --master yarn-cluster \
        --num-executors 3 \
        --driver-memory 4g \
        --executor-memory 2g \
        --executor-cores 1 \
        --queue thequeue \
        lib/spark-examples*.jar \
        10

The above starts a YARN client program which starts the default Application Master. Then SparkPi will be run as a child thread of Application Master. The client will periodically poll the Application Master for status updates and display them in the console. The client will exit once your application has finished running.  Refer to the "Viewing Logs" section below for how to see driver and executor logs.

To launch a Spark application in yarn-client mode, do the same, but replace "yarn-cluster" with "yarn-client".  To run spark-shell:

    $ ./bin/spark-shell --master yarn-client

## Adding Other JARs

In yarn-cluster mode, the driver runs on a different machine than the client, so `SparkContext.addJar` won't work out of the box with files that are local to the client. To make files on the client available to `SparkContext.addJar`, include them with the `--jars` option in the launch command. 

    $ ./bin/spark-submit --class my.main.Class \
        --master yarn-cluster \
        --jars my-other-jar.jar,my-other-other-jar.jar
        my-main-jar.jar
        app_arg1 app_arg2

# Debugging your Application

In YARN terminology, executors and application masters run inside "containers". YARN has two modes for handling container logs after an application has completed. If log aggregation is turned on (with the `yarn.log-aggregation-enable` config), container logs are copied to HDFS and deleted on the local machine. These logs can be viewed from anywhere on the cluster with the "yarn logs" command.

    yarn logs -applicationId <app ID>
    
will print out the contents of all log files from all containers from the given application.

When log aggregation isn't turned on, logs are retained locally on each machine under `YARN_APP_LOGS_DIR`, which is usually configured to `/tmp/logs` or `$HADOOP_HOME/logs/userlogs` depending on the Hadoop version and installation. Viewing logs for a container requires going to the host that contains them and looking in this directory.  Subdirectories organize log files by application ID and container ID.

To review per-container launch environment, increase `yarn.nodemanager.delete.debug-delay-sec` to a
large value (e.g. 36000), and then access the application cache through `yarn.nodemanager.local-dirs`
on the nodes on which containers are launched. This directory contains the launch script, JARs, and
all environment variables used for launching each container. This process is useful for debugging
classpath problems in particular. (Note that enabling this requires admin privileges on cluster
settings and a restart of all node managers. Thus, this is not applicable to hosted clusters).

To use a custom log4j configuration for the application master or executors, there are two options:

- upload a custom log4j.properties using spark-submit, by adding it to the "--files" list of files
  to be uploaded with the application.
- add "-Dlog4j.configuration=<location of configuration file>" to "spark.driver.extraJavaOptions"
  (for the driver) or "spark.executor.extraJavaOptions" (for executors). Note that if using a file,
  the "file:" protocol should be explicitly provided, and the file needs to exist locally on all
  the nodes.

Note that for the first option, both executors and the application master will share the same
log4j configuration, which may cause issues when they run on the same node (e.g. trying to write
to the same log file).

If you need a reference to the proper location to put log files in the YARN so that YARN can properly display and aggregate them, use "${spark.yarn.app.container.log.dir}" in your log4j.properties. For example, log4j.appender.file_appender.File=${spark.yarn.app.container.log.dir}/spark.log. For streaming application, configuring RollingFileAppender and setting file location to YARN's log directory will avoid disk overflow caused by large log file, and logs can be accessed using YARN's log utility.

# Important notes

- Before Hadoop 2.2, YARN does not support cores in container resource requests. Thus, when running against an earlier version, the numbers of cores given via command line arguments cannot be passed to YARN.  Whether core requests are honored in scheduling decisions depends on which scheduler is in use and how it is configured.
- The local directories used by Spark executors will be the local directories configured for YARN (Hadoop YARN config `yarn.nodemanager.local-dirs`). If the user specifies `spark.local.dir`, it will be ignored.
- The `--files` and `--archives` options support specifying file names with the # similar to Hadoop. For example you can specify: `--files localtest.txt#appSees.txt` and this will upload the file you have locally named localtest.txt into HDFS but this will be linked to by the name `appSees.txt`, and your application should use the name as `appSees.txt` to reference it when running on YARN.
- The `--jars` option allows the `SparkContext.addJar` function to work if you are using it with local files and running in `yarn-cluster` mode. It does not need to be used if you are using it with HDFS, HTTP, HTTPS, or FTP files.
