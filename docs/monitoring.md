---
layout: global
title: Monitoring and Instrumentation
description: Monitoring, metrics, and instrumentation guide for Spark SPARK_VERSION_SHORT
---

There are several ways to monitor Spark applications: web UIs, metrics, and external instrumentation.

# Web Interfaces

Every SparkContext launches a web UI, by default on port 4040, that
displays useful information about the application. This includes:

* A list of scheduler stages and tasks
* A summary of RDD sizes and memory usage
* Environmental information.
* Information about the running executors

You can access this interface by simply opening `http://<driver-node>:4040` in a web browser.
If multiple SparkContexts are running on the same host, they will bind to successive ports
beginning with 4040 (4041, 4042, etc).

Note that this information is only available for the duration of the application by default.
To view the web UI after the fact, set `spark.eventLog.enabled` to true before starting the
application. This configures Spark to log Spark events that encode the information displayed
in the UI to persisted storage.

## Viewing After the Fact

It is still possible to construct the UI of an application through Spark's history server, 
provided that the application's event logs exist.
You can start the history server by executing:

    ./sbin/start-history-server.sh

This creates a web interface at `http://<server-url>:18080` by default, listing incomplete
and completed applications and attempts.

When using the file-system provider class (see `spark.history.provider` below), the base logging
directory must be supplied in the `spark.history.fs.logDirectory` configuration option,
and should contain sub-directories that each represents an application's event logs.

The spark jobs themselves must be configured to log events, and to log them to the same shared,
writable directory. For example, if the server was configured with a log directory of
`hdfs://namenode/shared/spark-logs`, then the client-side options would be:

    spark.eventLog.enabled true
    spark.eventLog.dir hdfs://namenode/shared/spark-logs

The history server can be configured as follows:

### Environment Variables

<table class="table">
  <tr><th style="width:21%">Environment Variable</th><th>Meaning</th></tr>
  <tr>
    <td><code>SPARK_DAEMON_MEMORY</code></td>
    <td>Memory to allocate to the history server (default: 1g).</td>
  </tr>
  <tr>
    <td><code>SPARK_DAEMON_JAVA_OPTS</code></td>
    <td>JVM options for the history server (default: none).</td>
  </tr>
  <tr>
    <td><code>SPARK_PUBLIC_DNS</code></td>
    <td>
      The public address for the history server. If this is not set, links to application history
      may use the internal address of the server, resulting in broken links (default: none).
    </td>
  </tr>
  <tr>
    <td><code>SPARK_HISTORY_OPTS</code></td>
    <td>
      <code>spark.history.*</code> configuration options for the history server (default: none).
    </td>
  </tr>
</table>

### Spark configuration options

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td>spark.history.provider</td>
    <td><code>org.apache.spark.deploy.history.FsHistoryProvider</code></td>
    <td>Name of the class implementing the application history backend. Currently there is only
    one implementation, provided by Spark, which looks for application logs stored in the
    file system.</td>
  </tr>
  <tr>
    <td>spark.history.fs.logDirectory</td>
    <td>file:/tmp/spark-events</td>
    <td>
    For the filesystem history provider, the URL to the directory containing application event
    logs to load. This can be a local <code>file://</code> path,
    an HDFS path <code>hdfs://namenode/shared/spark-logs</code>
    or that of an alternative filesystem supported by the Hadoop APIs.
    </td>
  </tr>
  <tr>
    <td>spark.history.fs.update.interval</td>
    <td>10s</td>
    <td>
      The period at which the filesystem history provider checks for new or
      updated logs in the log directory. A shorter interval detects new applications faster,
      at the expense of more server load re-reading updated applications.
      As soon as an update has completed, listings of the completed and incomplete applications
      will reflect the changes.
    </td>
  </tr>
  <tr>
    <td>spark.history.retainedApplications</td>
    <td>50</td>
    <td>
      The number of applications to retain UI data for in the cache. If this cap is exceeded, then
      the oldest applications will be removed from the cache. If an application is not in the cache,
      it will have to be loaded from disk if its accessed from the UI.
    </td>
  </tr>
  <tr>
    <td>spark.history.ui.maxApplications</td>
    <td>Int.MaxValue</td>
    <td>
      The number of applications to display on the history summary page. Application UIs are still
      available by accessing their URLs directly even if they are not displayed on the history summary page.
    </td>
  </tr>
  <tr>
    <td>spark.history.ui.port</td>
    <td>18080</td>
    <td>
      The port to which the web interface of the history server binds.
    </td>
  </tr>
  <tr>
    <td>spark.history.kerberos.enabled</td>
    <td>false</td>
    <td>
      Indicates whether the history server should use kerberos to login. This is required
      if the history server is accessing HDFS files on a secure Hadoop cluster. If this is
      true, it uses the configs <code>spark.history.kerberos.principal</code> and
      <code>spark.history.kerberos.keytab</code>.
    </td>
  </tr>
  <tr>
    <td>spark.history.kerberos.principal</td>
    <td>(none)</td>
    <td>
      Kerberos principal name for the History Server.
    </td>
  </tr>
  <tr>
    <td>spark.history.kerberos.keytab</td>
    <td>(none)</td>
    <td>
      Location of the kerberos keytab file for the History Server.
    </td>
  </tr>
  <tr>
    <td>spark.history.ui.acls.enable</td>
    <td>false</td>
    <td>
      Specifies whether acls should be checked to authorize users viewing the applications.
      If enabled, access control checks are made regardless of what the individual application had
      set for <code>spark.ui.acls.enable</code> when the application was run. The application owner
      will always have authorization to view their own application and any users specified via
      <code>spark.ui.view.acls</code> and groups specified via <code>spark.ui.view.acls.groups</code>
      when the application was run will also have authorization to view that application.
      If disabled, no access control checks are made.
    </td>
  </tr>
  <tr>
    <td>spark.history.ui.admin.acls</td>
    <td>empty</td>
    <td>
      Comma separated list of users/administrators that have view access to all the Spark applications in
      history server. By default only the users permitted to view the application at run-time could
      access the related application history, with this, configured users/administrators could also
      have the permission to access it.
      Putting a "*" in the list means any user can have the privilege of admin.
    </td>
  </tr>
  <tr>
    <td>spark.history.ui.admin.acls.groups</td>
    <td>empty</td>
    <td>
      Comma separated list of groups that have view access to all the Spark applications in
      history server. By default only the groups permitted to view the application at run-time could
      access the related application history, with this, configured groups could also
      have the permission to access it.
      Putting a "*" in the list means any group can have the privilege of admin.
    </td>
  </tr>
  <tr>
    <td>spark.history.fs.cleaner.enabled</td>
    <td>false</td>
    <td>
      Specifies whether the History Server should periodically clean up event logs from storage.
    </td>
  </tr>
  <tr>
    <td>spark.history.fs.cleaner.interval</td>
    <td>1d</td>
    <td>
      How often the filesystem job history cleaner checks for files to delete.
      Files are only deleted if they are older than <code>spark.history.fs.cleaner.maxAge</code>
    </td>
  </tr>
  <tr>
    <td>spark.history.fs.cleaner.maxAge</td>
    <td>7d</td>
    <td>
      Job history files older than this will be deleted when the filesystem history cleaner runs.
    </td>
  </tr>
  <tr>
    <td>spark.history.fs.numReplayThreads</td>
    <td>25% of available cores</td>
    <td>
      Number of threads that will be used by history server to process event logs.
    </td>
  </tr>
</table>

Note that in all of these UIs, the tables are sortable by clicking their headers,
making it easy to identify slow tasks, data skew, etc.

Note

1. The history server displays both completed and incomplete Spark jobs. If an application makes
multiple attempts after failures, the failed attempts will be displayed, as well as any ongoing
incomplete attempt or the final successful attempt.

2. Incomplete applications are only updated intermittently. The time between updates is defined
by the interval between checks for changed files (`spark.history.fs.update.interval`).
On larger clusters the update interval may be set to large values.
The way to view a running application is actually to view its own web UI.

3. Applications which exited without registering themselves as completed will be listed
as incomplete —even though they are no longer running. This can happen if an application
crashes.

2. One way to signal the completion of a Spark job is to stop the Spark Context
explicitly (`sc.stop()`), or in Python using the `with SparkContext() as sc:` construct
to handle the Spark Context setup and tear down.


## REST API

In addition to viewing the metrics in the UI, they are also available as JSON.  This gives developers
an easy way to create new visualizations and monitoring tools for Spark.  The JSON is available for
both running applications, and in the history server.  The endpoints are mounted at `/api/v1`.  Eg.,
for the history server, they would typically be accessible at `http://<server-url>:18080/api/v1`, and
for a running application, at `http://localhost:4040/api/v1`.

In the API, an application is referenced by its application ID, `[app-id]`.
When running on YARN, each application may have multiple attempts, but there are attempt IDs
only for applications in cluster mode, not applications in client mode. Applications in YARN cluster mode
can be identified by their `[attempt-id]`. In the API listed below, when running in YARN cluster mode,
`[app-id]` will actually be `[base-app-id]/[attempt-id]`, where `[base-app-id]` is the YARN application ID.

<table class="table">
  <tr><th>Endpoint</th><th>Meaning</th></tr>
  <tr>
    <td><code>/applications</code></td>
    <td>A list of all applications.
    <br>
    <code>?status=[completed|running]</code> list only applications in the chosen state.
    <br>
    <code>?minDate=[date]</code> earliest start date/time to list.
    <br>
    <code>?maxDate=[date]</code> latest start date/time to list.
    <br>
    <code>?minEndDate=[date]</code> earliest end date/time to list.
    <br>
    <code>?maxEndDate=[date]</code> latest end date/time to list.
    <br>
    <code>?limit=[limit]</code> limits the number of applications listed.
    <br>Examples:
    <br><code>?minDate=2015-02-10</code>
    <br><code>?minDate=2015-02-03T16:42:40.000GMT</code>
    <br><code>?maxDate=2015-02-11T20:41:30.000GMT</code>
    <br><code>?minEndDate=2015-02-12</code>
    <br><code>?minEndDate=2015-02-12T09:15:10.000GMT</code>
    <br><code>?maxEndDate=2015-02-14T16:30:45.000GMT</code>
    <br><code>?limit=10</code></td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/jobs</code></td>
    <td>
      A list of all jobs for a given application.
      <br><code>?status=[running|succeeded|failed|unknown]</code> list only jobs in the specific state.
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/jobs/[job-id]</code></td>
    <td>Details for the given job.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages</code></td>
    <td>A list of all stages for a given application.</td>
    <br><code>?status=[active|complete|pending|failed]</code> list only stages in the state.
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]</code></td>
    <td>
      A list of all attempts for the given stage.
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]/[stage-attempt-id]</code></td>
    <td>Details for the given stage attempt.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]/[stage-attempt-id]/taskSummary</code></td>
    <td>
      Summary metrics of all tasks in the given stage attempt.
      <br><code>?quantiles</code> summarize the metrics with the given quantiles.
      <br>Example: <code>?quantiles=0.01,0.5,0.99</code>
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]/[stage-attempt-id]/taskList</code></td>
    <td>
       A list of all tasks for the given stage attempt.
      <br><code>?offset=[offset]&amp;length=[len]</code> list tasks in the given range.
      <br><code>?sortBy=[runtime|-runtime]</code> sort the tasks.
      <br>Example: <code>?offset=10&amp;length=50&amp;sortBy=runtime</code>
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/executors</code></td>
    <td>A list of all active executors for the given application.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/allexecutors</code></td>
    <td>A list of all(active and dead) executors for the given application.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/storage/rdd</code></td>
    <td>A list of stored RDDs for the given application.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/storage/rdd/[rdd-id]</code></td>
    <td>Details for the storage status of a given RDD.</td>
  </tr>
  <tr>
    <td><code>/applications/[base-app-id]/logs</code></td>
    <td>Download the event logs for all attempts of the given application as files within
    a zip file.
    </td>
  </tr>
  <tr>
    <td><code>/applications/[base-app-id]/[attempt-id]/logs</code></td>
    <td>Download the event logs for a specific application attempt as a zip file.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/streaming/statistics</code></td>
    <td>Statistics for the streaming context.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/streaming/receivers</code></td>
    <td>A list of all streaming receivers.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/streaming/receivers/[stream-id]</code></td>
    <td>Details of the given receiver.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/streaming/batches</code></td>
    <td>A list of all retained batches.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/streaming/batches/[batch-id]</code></td>
    <td>Details of the given batch.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/streaming/batches/[batch-id]/operations</code></td>
    <td>A list of all output operations of the given batch.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/streaming/batches/[batch-id]/operations/[outputOp-id]</code></td>
    <td>Details of the given operation and given batch.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/environment</code></td>
    <td>Environment details of the given application.</td>
  </tr>       
</table>

The number of jobs and stages which can retrieved is constrained by the same retention
mechanism of the standalone Spark UI; `"spark.ui.retainedJobs"` defines the threshold
value triggering garbage collection on jobs, and `spark.ui.retainedStages` that for stages.
Note that the garbage collection takes place on playback: it is possible to retrieve
more entries by increasing these values and restarting the history server.

### API Versioning Policy

These endpoints have been strongly versioned to make it easier to develop applications on top.
 In particular, Spark guarantees:

* Endpoints will never be removed from one version
* Individual fields will never be removed for any given endpoint
* New endpoints may be added
* New fields may be added to existing endpoints
* New versions of the api may be added in the future at a separate endpoint (eg., `api/v2`).  New versions are *not* required to be backwards compatible.
* Api versions may be dropped, but only after at least one minor release of co-existing with a new api version.

Note that even when examining the UI of a running applications, the `applications/[app-id]` portion is
still required, though there is only one application available.  Eg. to see the list of jobs for the
running app, you would go to `http://localhost:4040/api/v1/applications/[app-id]/jobs`.  This is to
keep the paths consistent in both modes.

# Metrics

Spark has a configurable metrics system based on the
[Dropwizard Metrics Library](http://metrics.dropwizard.io/).
This allows users to report Spark metrics to a variety of sinks including HTTP, JMX, and CSV
files. The metrics system is configured via a configuration file that Spark expects to be present
at `$SPARK_HOME/conf/metrics.properties`. A custom file location can be specified via the
`spark.metrics.conf` [configuration property](configuration.html#spark-properties).
By default, the root namespace used for driver or executor metrics is 
the value of `spark.app.id`. However, often times, users want to be able to track the metrics 
across apps for driver and executors, which is hard to do with application ID 
(i.e. `spark.app.id`) since it changes with every invocation of the app. For such use cases,
a custom namespace can be specified for metrics reporting using `spark.metrics.namespace`
configuration property. 
If, say, users wanted to set the metrics namespace to the name of the application, they
can set the `spark.metrics.namespace` property to a value like `${spark.app.name}`. This value is
then expanded appropriately by Spark and is used as the root namespace of the metrics system. 
Non driver and executor metrics are never prefixed with `spark.app.id`, nor does the 
`spark.metrics.namespace` property have any such affect on such metrics.

Spark's metrics are decoupled into different
_instances_ corresponding to Spark components. Within each instance, you can configure a
set of sinks to which metrics are reported. The following instances are currently supported:

* `master`: The Spark standalone master process.
* `applications`: A component within the master which reports on various applications.
* `worker`: A Spark standalone worker process.
* `executor`: A Spark executor.
* `driver`: The Spark driver process (the process in which your SparkContext is created).
* `shuffleService`: The Spark shuffle service.

Each instance can report to zero or more _sinks_. Sinks are contained in the
`org.apache.spark.metrics.sink` package:

* `ConsoleSink`: Logs metrics information to the console.
* `CSVSink`: Exports metrics data to CSV files at regular intervals.
* `JmxSink`: Registers metrics for viewing in a JMX console.
* `MetricsServlet`: Adds a servlet within the existing Spark UI to serve metrics data as JSON data.
* `GraphiteSink`: Sends metrics to a Graphite node.
* `Slf4jSink`: Sends metrics to slf4j as log entries.

Spark also supports a Ganglia sink which is not included in the default build due to
licensing restrictions:

* `GangliaSink`: Sends metrics to a Ganglia node or multicast group.

To install the `GangliaSink` you'll need to perform a custom build of Spark. _**Note that
by embedding this library you will include [LGPL](http://www.gnu.org/copyleft/lesser.html)-licensed
code in your Spark package**_. For sbt users, set the
`SPARK_GANGLIA_LGPL` environment variable before building. For Maven users, enable
the `-Pspark-ganglia-lgpl` profile. In addition to modifying the cluster's Spark build
user applications will need to link to the `spark-ganglia-lgpl` artifact.

The syntax of the metrics configuration file is defined in an example configuration file,
`$SPARK_HOME/conf/metrics.properties.template`.

# Advanced Instrumentation

Several external tools can be used to help profile the performance of Spark jobs:

* Cluster-wide monitoring tools, such as [Ganglia](http://ganglia.sourceforge.net/), can provide
insight into overall cluster utilization and resource bottlenecks. For instance, a Ganglia
dashboard can quickly reveal whether a particular workload is disk bound, network bound, or
CPU bound.
* OS profiling tools such as [dstat](http://dag.wieers.com/home-made/dstat/),
[iostat](http://linux.die.net/man/1/iostat), and [iotop](http://linux.die.net/man/1/iotop)
can provide fine-grained profiling on individual nodes.
* JVM utilities such as `jstack` for providing stack traces, `jmap` for creating heap-dumps,
`jstat` for reporting time-series statistics and `jconsole` for visually exploring various JVM
properties are useful for those comfortable with JVM internals.
