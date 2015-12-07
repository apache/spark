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

Spark's Standalone Mode cluster manager also has its own
[web UI](spark-standalone.html#monitoring-and-logging). If an application has logged events over
the course of its lifetime, then the Standalone master's web UI will automatically re-render the
application's UI after the application has finished.

If Spark is run on Mesos or YARN, it is still possible to reconstruct the UI of a finished
application through Spark's history server, provided that the application's event logs exist.
You can start the history server by executing:

    ./sbin/start-history-server.sh

When using the file-system provider class (see spark.history.provider below), the base logging
directory must be supplied in the <code>spark.history.fs.logDirectory</code> configuration option,
and should contain sub-directories that each represents an application's event logs. This creates a
web interface at `http://<server-url>:18080` by default. The history server can be configured as
follows:

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

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td>spark.history.provider</td>
    <td>org.apache.spark.deploy.history.FsHistoryProvider</td>
    <td>Name of the class implementing the application history backend. The default implementation,
    is the FsHistoryProvider, retrieves application logs stored in the file system.</td>
  </tr>
  <tr>
    <td>spark.history.fs.logDirectory</td>
    <td>file:/tmp/spark-events</td>
    <td>
     Directory that contains application event logs to be loaded by the history server
    </td>
  </tr>
  <tr>
    <td>spark.history.fs.update.interval</td>
    <td>10s</td>
    <td>
      The period at which information displayed by this history server is updated.
      Each update checks for any changes made to the event logs in persisted storage.
    </td>
  </tr>
  <tr>
    <td>spark.history.retainedApplications</td>
    <td>50</td>
    <td>
      The number of application UIs to retain. If this cap is exceeded, then the oldest
      applications will be removed.
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
      Indicates whether the history server should use kerberos to login. This is useful
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
      <code>spark.ui.view.acls</code> when the application was run will also have authorization
      to view that application. 
      If disabled, no access control checks are made. 
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
      How often the job history cleaner checks for files to delete.
      Files are only deleted if they are older than spark.history.fs.cleaner.maxAge.
    </td>
  </tr>
  <tr>
    <td>spark.history.fs.cleaner.maxAge</td>
    <td>7d</td>
    <td>
      Job history files older than this will be deleted when the history cleaner runs.
    </td>
  </tr>
</table>

Note that in all of these UIs, the tables are sortable by clicking their headers,
making it easy to identify slow tasks, data skew, etc.

Note that the history server only displays completed Spark jobs. One way to signal the completion of a Spark job is to stop the Spark Context explicitly (`sc.stop()`), or in Python using the `with SparkContext() as sc:` to handle the Spark Context setup and tear down, and still show the job history on the UI.

## REST API

In addition to viewing the metrics in the UI, they are also available as JSON.  This gives developers
an easy way to create new visualizations and monitoring tools for Spark.  The JSON is available for
both running applications, and in the history server.  The endpoints are mounted at `/api/v1`.  Eg.,
for the history server, they would typically be accessible at `http://<server-url>:18080/api/v1`, and
for a running application, at `http://localhost:4040/api/v1`.

<table class="table">
  <tr><th>Endpoint</th><th>Meaning</th></tr>
  <tr>
    <td><code>/applications</code></td>
    <td>A list of all applications</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/jobs</code></td>
    <td>A list of all jobs for a given application</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/jobs/[job-id]</code></td>
    <td>Details for the given job</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages</code></td>
    <td>A list of all stages for a given application</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]</code></td>
    <td>A list of all attempts for the given stage</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]/[stage-attempt-id]</code></td>
    <td>Details for the given stage attempt</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]/[stage-attempt-id]/taskSummary</code></td>
    <td>Summary metrics of all tasks in the given stage attempt</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]/[stage-attempt-id]/taskList</code></td>
    <td>A list of all tasks for the given stage attempt</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/executors</code></td>
    <td>A list of all executors for the given application</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/storage/rdd</code></td>
    <td>A list of stored RDDs for the given application</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/storage/rdd/[rdd-id]</code></td>
    <td>Details for the storage status of a given RDD</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/logs</code></td>
    <td>Download the event logs for all attempts of the given application as a zip file</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/[attempt-id]/logs</code></td>
    <td>Download the event logs for the specified attempt of the given application as a zip file</td>
  </tr>
</table>

When running on Yarn, each application has multiple attempts, so `[app-id]` is actually
`[app-id]/[attempt-id]` in all cases.

These endpoints have been strongly versioned to make it easier to develop applications on top.
 In particular, Spark guarantees:

* Endpoints will never be removed from one version
* Individual fields will never be removed for any given endpoint
* New endpoints may be added
* New fields may be added to existing endpoints
* New versions of the api may be added in the future at a separate endpoint (eg., `api/v2`).  New versions are *not* required to be backwards compatible.
* Api versions may be dropped, but only after at least one minor release of co-existing with a new api version

Note that even when examining the UI of a running applications, the `applications/[app-id]` portion is
still required, though there is only one application available.  Eg. to see the list of jobs for the
running app, you would go to `http://localhost:4040/api/v1/applications/[app-id]/jobs`.  This is to
keep the paths consistent in both modes.

## Hadoop YARN Timeline service history provider

As well as the Filesystem History Provider, Spark can integrate with the Hadoop YARN
"Application Timeline Service". This is a service which runs in a YARN cluster, recording
application- and YARN- published events to a database, retrieving them on request.

Spark integrates with the timeline service by
1. Publishing events to the timeline service as applications execute.
1. Listing application histories published to the timeline service.
1. Retrieving the details of specific application histories.

### Configuring the Timeline Service

For details on configuring and starting the timeline service, consult the Hadoop documentation.

From the perspective of Spark, the key requirements are
1. The YARN timeline service must be running.
1. Its URL is known, and configured in the `yarn-site.xml` configuration file.
1. The user has an Kerberos credentials required to interact with the service.

The timeline service URL must be declared in the property `yarn.timeline-service.webapp.address`,
or, if HTTPS is the protocol, `yarn.timeline-service.webapp.https.address`

The choice between HTTP and HTTPS is made on the value of `yarn.http.policy`, which can be one of
`http-only` (default), `https_only` or `http_and_https`; HTTP will be used unless the policy
is `https_only`.

Examples:

    <!-- Binding for HTTP endpoint -->
    <property>
      <name>yarn.timeline-service.webapp.address</name>
      <value>atshost.example.org:8188</value>
    </property>

    <property>
      <name>yarn.timeline-service.enabled</name>
      <value>true</value>
    </property>

The root web page of the timeline service can be verified with a web browser,
as an easy check that the service is live.

### Saving Application History to the YARN Timeline Service

To publish to the YARN Timeline Service, Spark applications executed in a YARN cluster
must be configured to instantiate the `YarnHistoryService`. This is done
by setting the spark configuration property `spark.yarn.services`
to `org.apache.spark.deploy.history.yarn.YarnHistoryService`

    spark.yarn.services org.apache.spark.deploy.history.yarn.YarnHistoryService

Notes

1. If the class-name is mis-spelled or cannot be instantiated, an error message will
be logged; the application will still run.
2. YARN history publishing can run alongside the filesystem history listener; both
histories can be viewed by an appropriately configured history service.
3. If the timeline service is disabled, that is `yarn.timeline-service.enabled` is not
`true`, then the history will not be published: the application will still run.
4. Similarly, in a cluster where the timeline service is disabled, the history server
will simply show an empty history, while warning that the history service is disabled.
5. In a secure cluster, the user must have the Kerberos credentials to interact
with the timeline server. Being logged in via `kinit` or a keytab should suffice.
6. If the application is killed it will be listed as incompleted. In an application
started as a `--master yarn-client` this happens if the client process is stopped
with a `kill -9` or process failure).
Similarly, an application started with `--master yarn-cluster` will remain incompleted
if killed without warning, if it fails, or it is killed via the `yarn kill` command.


Specific configuration options:

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.domain</code></td>
    <td></td>
    <td>
    If UI permissions are set through `spark.acls.enable` or `spark.ui.acls.enable` being true,
    the optional name of a predefined timeline domain to use . If unset,
    a value is created programmatically.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.post.retry.interval</code></td>
    <td>1s</td>
    <td>
    Interval in milliseconds between POST retries. Every
    failure adds another delay of this interval before the next retry
    attempt. That is, first 1s, then 2s, 3s, ...
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.batch.size</code></td>
    <td>3</td>
    <td>
    How many events to batch up before submitting them to the timeline service.
    This is a performance optimization.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.post.limit</code></td>
    <td>1000</td>
    <td>
    Limit on number of queued events to posts. When exceeded
    new events will be dropped. This is to place a limit on how much
    memory will be consumed if the timeline server goes offline.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.shutdown.waittime</code></td>
    <td>30s</td>
    <td>
    Maximum time in to wait for event posting to complete when the service stops.
    </td>
  </tr>
  <tr>
    <td><code>spark.hadoop.yarn.timeline.listen</code></td>
    <td>true</td>
    <td>
    This flag exists for testing: if `false` the history publishing
    service will not register for events with the spark context. As
    a result, lifecycle events will not be picked up.
    </td>
  </tr>
</table>


### Viewing Application Histories via the YARN Timeline Service

To retrieve and display history information in the YARN Timeline Service, the Spark history server must
be configured to query the timeline service for the lists of running and completed applications.

Note that the history server does not actually need to be deployed within the Hadoop cluster itself —it
simply needs access to the REST API offered by the timeline service.

To switch to the timeline history, set `spark.history.provider` to
`org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider`:

    spark.history.provider org.apache.spark.deploy.history.yarn.server.YarnHistoryProvider

The Timeline Server bindings in `yarn-site.xml` will also be needed, so that the YARN history
provider can retrieve events from the YARN Timeline Service.

The history provider retrieves data from the timeline service

1. On startup.
1. In the background at an interval set by the option
   `spark.history.yarn.backround.refresh.interval`.
1. When an HTTP request to the web UI or REST API is made and there has not been
any update to the history view in the interval defined by
`spark.history.yarn.manual.refresh.interval`. This triggers an asynchronous update,
so the results are not visible in the HTTP request which triggered the update.

The reason for this design is that in large YARN clusters, frequent polling of
the timeline server to probe for updated applications can place excessive load
on a shared resource.

Together options can offer different policies. Here are some examples, two
at the extremes and some more balanced

#### Background refresh only; updated every minute

      spark.history.yarn.backround.refresh.interval = 300s
      spark.history.yarn.manual.refresh.interval = 0s

The history is updated in the background, once a minute. The smaller
the refresh interval, the higher the load on the timeline server.

#### Manual refresh only; minimum interval one minute

      spark.history.yarn.backround.refresh.interval = 0s
      spark.history.yarn.manual.refresh.interval = 60s

There is no backgroud update; a manual page refresh will trigger an asynchronous refresh.

To get the most current history data, try refreshing the page more than once:
the first to trigger the fetch of the latest data; the second to view the updated history.

This configuration places no load on the YARN Timeline Service when there
is no user of the Spark History Service, at the cost of a slightly less
intuitive UI: because two refreshes are needed to get the updated information,
the state of the system will not be immediately obvious.


#### Manual and background refresh: responsive

      spark.history.yarn.backround.refresh.interval = 60s
      spark.history.yarn.manual.refresh.interval = 20s

Here the background refresh interval is 60s, but a page refresh will trigger
an update if the last refresh was more than 20 seconds ago.

#### Manual and background refresh: low-load

      spark.history.yarn.backround.refresh.interval = 300s
      spark.history.yarn.manual.refresh.interval = 60s

Here a background update takes place every five minutes; a refresh is
also triggered on a page refresh if there has not been one in the last minute.

What makes for the best configuration? It depends on cluster size. The smaller the cluster,
the smaller the background refresh interval can be -and the manual refresh interval then set to zero,
to disable that option entirely.

#### YARN History Provider Configuration Options:

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th></tr>
  <tr>
    <td><code>spark.history.yarn.window.limit</code></td>
    <td>24h</td>
    <td>
      The earliest time to look for events. The default value is 24 hours.
      This property limits the amount of data queried off the YARN timeline server;
      applications started before this window will not be checked to see if they have completed.
      Set this to 0 for no limits (and increased load on the timeline server).
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.backround.refresh.interval</code></td>
    <td>60s</td>
    <td>
      The interval between background refreshes of the history data.
      A value of 0s means "no background updates: manual refreshes only".
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.manual.refresh.interval</code></td>
    <td>30s</td>
    <td>
      Minimum interval between manual refreshes of the history data; refreshing the
      page before this limit will not trigger an update.
      A value of 0s means "page refreshes do not trigger updates of the application list"
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.event-fetch-limit</code></td>
    <td>1000</td>
    <td>
      Maximum number of application histories to fetch
      from the timeline server in a single GET request.
    </td>
  </tr>
  <tr>
    <td><code>spark.history.yarn.diagnostics</code></td>
    <td>false</td>
    <td>
      A flag to indicate whether low-level diagnostics information should be included in
      status pages. This is for debugging and diagnostics.
    </td>
  </tr>
  </tr>
    <tr>
    <td><code>spark.history.yarn.probe.running.applications</code></td>
    <td>true</td>
    <td>
      Should the history provider query the YARN Resource Manager to verify that
      incompleted applications are actually still running.
    </td>
  </tr>

</table>

# Metrics

Spark has a configurable metrics system based on the 
[Coda Hale Metrics Library](http://metrics.codahale.com/). 
This allows users to report Spark metrics to a variety of sinks including HTTP, JMX, and CSV 
files. The metrics system is configured via a configuration file that Spark expects to be present 
at `$SPARK_HOME/conf/metrics.properties`. A custom file location can be specified via the 
`spark.metrics.conf` [configuration property](configuration.html#spark-properties).
Spark's metrics are decoupled into different 
_instances_ corresponding to Spark components. Within each instance, you can configure a 
set of sinks to which metrics are reported. The following instances are currently supported:

* `master`: The Spark standalone master process.
* `applications`: A component within the master which reports on various applications.
* `worker`: A Spark standalone worker process.
* `executor`: A Spark executor.
* `driver`: The Spark driver process (the process in which your SparkContext is created).

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
