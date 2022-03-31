---
layout: global
title: Monitoring and Instrumentation
description: Monitoring, metrics, and instrumentation guide for Spark SPARK_VERSION_SHORT
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

There are several ways to monitor Spark applications: web UIs, metrics, and external instrumentation.

# Web Interfaces

Every SparkContext launches a [Web UI](web-ui.html), by default on port 4040, that
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
    <td><code>SPARK_DAEMON_CLASSPATH</code></td>
    <td>Classpath for the history server (default: none).</td>
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

### Applying compaction on rolling event log files

A long-running application (e.g. streaming) can bring a huge single event log file which may cost a lot to maintain and
also requires a bunch of resource to replay per each update in Spark History Server.

Enabling <code>spark.eventLog.rolling.enabled</code> and <code>spark.eventLog.rolling.maxFileSize</code> would
let you have rolling event log files instead of single huge event log file which may help some scenarios on its own,
but it still doesn't help you reducing the overall size of logs.

Spark History Server can apply compaction on the rolling event log files to reduce the overall size of
logs, via setting the configuration <code>spark.history.fs.eventLog.rolling.maxFilesToRetain</code> on the
Spark History Server.

Details will be described below, but please note in prior that compaction is LOSSY operation.
Compaction will discard some events which will be no longer seen on UI - you may want to check which events will be discarded
before enabling the option.

When the compaction happens, the History Server lists all the available event log files for the application, and considers
the event log files having less index than the file with smallest index which will be retained as target of compaction.
For example, if the application A has 5 event log files and <code>spark.history.fs.eventLog.rolling.maxFilesToRetain</code> is set to 2, then first 3 log files will be selected to be compacted.

Once it selects the target, it analyzes them to figure out which events can be excluded, and rewrites them
into one compact file with discarding events which are decided to exclude.

The compaction tries to exclude the events which point to the outdated data. As of now, below describes the candidates of events to be excluded:

* Events for the job which is finished, and related stage/tasks events
* Events for the executor which is terminated
* Events for the SQL execution which is finished, and related job/stage/tasks events

Once rewriting is done, original log files will be deleted, via best-effort manner. The History Server may not be able to delete
the original log files, but it will not affect the operation of the History Server.

Please note that Spark History Server may not compact the old event log files if figures out not a lot of space
would be reduced during compaction. For streaming query we normally expect compaction
will run as each micro-batch will trigger one or more jobs which will be finished shortly, but compaction won't run
in many cases for batch query.

Please also note that this is a new feature introduced in Spark 3.0, and may not be completely stable. Under some circumstances,
the compaction may exclude more events than you expect, leading some UI issues on History Server for the application.
Use it with caution.

### Spark History Server Configuration Options

Security options for the Spark History Server are covered more detail in the
[Security](security.html#web-ui) page.

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
  <tr>
    <td>spark.history.provider</td>
    <td><code>org.apache.spark.deploy.history.FsHistoryProvider</code></td>
    <td>Name of the class implementing the application history backend. Currently there is only
    one implementation, provided by Spark, which looks for application logs stored in the
    file system.</td>
    <td>1.1.0</td>
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
    <td>1.1.0</td>
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
    <td>1.4.0</td>
  </tr>
  <tr>
    <td>spark.history.retainedApplications</td>
    <td>50</td>
    <td>
      The number of applications to retain UI data for in the cache. If this cap is exceeded, then
      the oldest applications will be removed from the cache. If an application is not in the cache,
      it will have to be loaded from disk if it is accessed from the UI.
    </td>
    <td>1.0.0</td>
  </tr>
  <tr>
    <td>spark.history.ui.maxApplications</td>
    <td>Int.MaxValue</td>
    <td>
      The number of applications to display on the history summary page. Application UIs are still
      available by accessing their URLs directly even if they are not displayed on the history summary page.
    </td>
    <td>2.0.1</td>
  </tr>
  <tr>
    <td>spark.history.ui.port</td>
    <td>18080</td>
    <td>
      The port to which the web interface of the history server binds.
    </td>
    <td>1.0.0</td>
  </tr>
  <tr>
    <td>spark.history.kerberos.enabled</td>
    <td>false</td>
    <td>
      Indicates whether the history server should use kerberos to login. This is required
      if the history server is accessing HDFS files on a secure Hadoop cluster.
    </td>
    <td>1.0.1</td>
  </tr>
  <tr>
    <td>spark.history.kerberos.principal</td>
    <td>(none)</td>
    <td>
      When <code>spark.history.kerberos.enabled=true</code>, specifies kerberos principal name for the History Server.
    </td>
    <td>1.0.1</td>
  </tr>
  <tr>
    <td>spark.history.kerberos.keytab</td>
    <td>(none)</td>
    <td>
      When <code>spark.history.kerberos.enabled=true</code>, specifies location of the kerberos keytab file for the History Server.
    </td>
    <td>1.0.1</td>
  </tr>
  <tr>
    <td>spark.history.fs.cleaner.enabled</td>
    <td>false</td>
    <td>
      Specifies whether the History Server should periodically clean up event logs from storage.
    </td>
    <td>1.4.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.cleaner.interval</td>
    <td>1d</td>
    <td>
      When <code>spark.history.fs.cleaner.enabled=true</code>, specifies how often the filesystem job history cleaner checks for files to delete.
      Files are deleted if at least one of two conditions holds.
      First, they're deleted if they're older than <code>spark.history.fs.cleaner.maxAge</code>.
      They are also deleted if the number of files is more than
      <code>spark.history.fs.cleaner.maxNum</code>, Spark tries to clean up the completed attempts
      from the applications based on the order of their oldest attempt time.
    </td>
    <td>1.4.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.cleaner.maxAge</td>
    <td>7d</td>
    <td>
      When <code>spark.history.fs.cleaner.enabled=true</code>, job history files older than this will be deleted when the filesystem history cleaner runs.
    </td>
    <td>1.4.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.cleaner.maxNum</td>
    <td>Int.MaxValue</td>
    <td>
      When <code>spark.history.fs.cleaner.enabled=true</code>, specifies the maximum number of files in the event log directory.
      Spark tries to clean up the completed attempt logs to maintain the log directory under this limit.
      This should be smaller than the underlying file system limit like
      `dfs.namenode.fs-limits.max-directory-items` in HDFS.
    </td>
    <td>3.0.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.endEventReparseChunkSize</td>
    <td>1m</td>
    <td>
      How many bytes to parse at the end of log files looking for the end event. 
      This is used to speed up generation of application listings by skipping unnecessary
      parts of event log files. It can be disabled by setting this config to 0.
    </td>
    <td>2.4.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.inProgressOptimization.enabled</td>
    <td>true</td>
    <td>
      Enable optimized handling of in-progress logs. This option may leave finished
      applications that fail to rename their event logs listed as in-progress.
    </td>
    <td>2.4.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.driverlog.cleaner.enabled</td>
    <td><code>spark.history.fs.cleaner.enabled</code></td>
    <td>
      Specifies whether the History Server should periodically clean up driver logs from storage.
    </td>
    <td>3.0.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.driverlog.cleaner.interval</td>
    <td><code>spark.history.fs.cleaner.interval</code></td>
    <td>
      When <code>spark.history.fs.driverlog.cleaner.enabled=true</code>, specifies how often the filesystem driver log cleaner checks for files to delete.
      Files are only deleted if they are older than <code>spark.history.fs.driverlog.cleaner.maxAge</code>
    </td>
    <td>3.0.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.driverlog.cleaner.maxAge</td>
    <td><code>spark.history.fs.cleaner.maxAge</code></td>
    <td>
      When <code>spark.history.fs.driverlog.cleaner.enabled=true</code>, driver log files older than this will be deleted when the driver log cleaner runs.
    </td>
    <td>3.0.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.numReplayThreads</td>
    <td>25% of available cores</td>
    <td>
      Number of threads that will be used by history server to process event logs.
    </td>
    <td>2.0.0</td>
  </tr>
  <tr>
    <td>spark.history.store.maxDiskUsage</td>
    <td>10g</td>
    <td>
      Maximum disk usage for the local directory where the cache application history information
      are stored.
    </td>
    <td>2.3.0</td>
  </tr>
  <tr>
    <td>spark.history.store.path</td>
    <td>(none)</td>
    <td>
        Local directory where to cache application history data. If set, the history
        server will store application data on disk instead of keeping it in memory. The data
        written to disk will be re-used in the event of a history server restart.
    </td>
    <td>2.3.0</td>
  </tr>
  <tr>
    <td>spark.history.custom.executor.log.url</td>
    <td>(none)</td>
    <td>
        Specifies custom spark executor log URL for supporting external log service instead of using cluster
        managers' application log URLs in the history server. Spark will support some path variables via patterns
        which can vary on cluster manager. Please check the documentation for your cluster manager to
        see which patterns are supported, if any. This configuration has no effect on a live application, it only
        affects the history server.
        <p/>
        For now, only YARN mode supports this configuration
    </td>
    <td>3.0.0</td>
  </tr>
  <tr>
    <td>spark.history.custom.executor.log.url.applyIncompleteApplication</td>
    <td>true</td>
    <td>
        Specifies whether to apply custom spark executor log URL to incomplete applications as well.
        If executor logs for running applications should be provided as origin log URLs, set this to `false`.
        Please note that incomplete applications may include applications which didn't shutdown gracefully.
        Even this is set to `true`, this configuration has no effect on a live application, it only affects the history server.
    </td>
    <td>3.0.0</td>
  </tr>
  <tr>
    <td>spark.history.fs.eventLog.rolling.maxFilesToRetain</td>
    <td>Int.MaxValue</td>
    <td>
      The maximum number of event log files which will be retained as non-compacted. By default,
      all event log files will be retained. The lowest value is 1 for technical reason.<br/>
      Please read the section of "Applying compaction of old event log files" for more details.
    </td>
    <td>3.0.0</td>
  </tr>
  <tr>
    <td>spark.history.store.hybridStore.enabled</td>
    <td>false</td>
    <td>
      Whether to use HybridStore as the store when parsing event logs. HybridStore will first write data
      to an in-memory store and having a background thread that dumps data to a disk store after the writing
      to in-memory store is completed.
    </td>
    <td>3.1.0</td>
  </tr>
  <tr>
    <td>spark.history.store.hybridStore.maxMemoryUsage</td>
    <td>2g</td>
    <td>
      Maximum memory space that can be used to create HybridStore. The HybridStore co-uses the heap memory,
      so the heap memory should be increased through the memory option for SHS if the HybridStore is enabled.
    </td>
    <td>3.1.0</td>
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
On larger clusters, the update interval may be set to large values.
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
both running applications, and in the history server.  The endpoints are mounted at `/api/v1`.  For example,
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
    <td>
      A list of all stages for a given application.
        <br><code>?status=[active|complete|pending|failed]</code> list only stages in the given state.
        <br><code>?details=true</code> lists all stages with the task data.
        <br><code>?taskStatus=[RUNNING|SUCCESS|FAILED|KILLED|PENDING]</code> lists only those tasks with the specified task status. Query parameter taskStatus takes effect only when <code>details=true</code>. This also supports multiple <code>taskStatus</code> such as <code>?details=true&taskStatus=SUCCESS&taskStatus=FAILED</code> which will return all tasks matching any of specified task status.
        <br><code>?withSummaries=true</code> lists stages with task metrics distribution and executor metrics distribution.
        <br><code>?quantiles=0.0,0.25,0.5,0.75,1.0</code> summarize the metrics with the given quantiles. Query parameter quantiles takes effect only when <code>withSummaries=true</code>. Default value is <code>0.0,0.25,0.5,0.75,1.0</code>. 
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]</code></td>
    <td>
      A list of all attempts for the given stage.
        <br><code>?details=true</code> lists all attempts with the task data for the given stage.
        <br><code>?taskStatus=[RUNNING|SUCCESS|FAILED|KILLED|PENDING]</code> lists only those tasks with the specified task status. Query parameter taskStatus takes effect only when <code>details=true</code>. This also supports multiple <code>taskStatus</code> such as <code>?details=true&taskStatus=SUCCESS&taskStatus=FAILED</code> which will return all tasks matching any of specified task status.
        <br><code>?withSummaries=true</code> lists task metrics distribution and executor metrics distribution of each attempt.
        <br><code>?quantiles=0.0,0.25,0.5,0.75,1.0</code> summarize the metrics with the given quantiles. Query parameter quantiles takes effect only when <code>withSummaries=true</code>. Default value is <code>0.0,0.25,0.5,0.75,1.0</code>. 
      <br>Example:
        <br><code>?details=true</code>
        <br><code>?details=true&taskStatus=RUNNING</code>
        <br><code>?withSummaries=true</code>
        <br><code>?details=true&withSummaries=true&quantiles=0.01,0.5,0.99</code>
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/stages/[stage-id]/[stage-attempt-id]</code></td>
    <td>
      Details for the given stage attempt.
        <br><code>?details=true</code> lists all task data for the given stage attempt.
        <br><code>?taskStatus=[RUNNING|SUCCESS|FAILED|KILLED|PENDING]</code> lists only those tasks with the specified task status. Query parameter taskStatus takes effect only when <code>details=true</code>. This also supports multiple <code>taskStatus</code> such as <code>?details=true&taskStatus=SUCCESS&taskStatus=FAILED</code> which will return all tasks matching any of specified task status.
        <br><code>?withSummaries=true</code> lists task metrics distribution and executor metrics distribution for the given stage attempt.
        <br><code>?quantiles=0.0,0.25,0.5,0.75,1.0</code> summarize the metrics with the given quantiles. Query parameter quantiles takes effect only when <code>withSummaries=true</code>. Default value is <code>0.0,0.25,0.5,0.75,1.0</code>. 
      <br>Example:
        <br><code>?details=true</code>
        <br><code>?details=true&taskStatus=RUNNING</code>
        <br><code>?withSummaries=true</code>
        <br><code>?details=true&withSummaries=true&quantiles=0.01,0.5,0.99</code>
    </td>
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
      <br><code>?status=[running|success|killed|failed|unknown]</code> list only tasks in the state.
      <br>Example: <code>?offset=10&amp;length=50&amp;sortBy=runtime&amp;status=running</code>
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/executors</code></td>
    <td>A list of all active executors for the given application.</td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/executors/[executor-id]/threads</code></td>
    <td>
      Stack traces of all the threads running within the given active executor.
      Not available via the history server.
    </td>
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
    <td><code>/applications/[app-id]/sql</code></td>
    <td>A list of all queries for a given application.
    <br>
    <code>?details=[true (default) | false]</code> lists/hides details of Spark plan nodes.
    <br>
    <code>?planDescription=[true (default) | false]</code> enables/disables Physical <code>planDescription</code> on demand when Physical Plan size is high.
    <br>
    <code>?offset=[offset]&length=[len]</code> lists queries in the given range.
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/sql/[execution-id]</code></td>
    <td>Details for the given query.
    <br>
    <code>?details=[true (default) | false]</code> lists/hides metric details in addition to given query details.
    <br>
    <code>?planDescription=[true (default) | false]</code> enables/disables Physical <code>planDescription</code> on demand for the given query when Physical Plan size is high.
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/diagnostics/sql/[execution-id]</code></td>
    <td>Diagnostic for the given query. it includes:
    <br>
    1. plan change history of adaptive execution
    <br>
    2. physical plan description with unlimited fields
    </td>
  </tr>
  <tr>
    <td><code>/applications/[app-id]/environment</code></td>
    <td>Environment details of the given application.</td>
  </tr>
  <tr>
    <td><code>/version</code></td>
    <td>Get the current spark version.</td>
  </tr>
</table>

The number of jobs and stages which can be retrieved is constrained by the same retention
mechanism of the standalone Spark UI; `"spark.ui.retainedJobs"` defines the threshold
value triggering garbage collection on jobs, and `spark.ui.retainedStages` that for stages.
Note that the garbage collection takes place on playback: it is possible to retrieve
more entries by increasing these values and restarting the history server.

### Executor Task Metrics

The REST API exposes the values of the Task Metrics collected by Spark executors with the granularity
of task execution. The metrics can be used for performance troubleshooting and workload characterization.
A list of the available metrics, with a short description:

<table class="table">
  <tr><th>Spark Executor Task Metric name</th>
      <th>Short description</th>
  </tr>
  <tr>
    <td>executorRunTime</td>
    <td>Elapsed time the executor spent running this task. This includes time fetching shuffle data.
    The value is expressed in milliseconds.</td>
  </tr>
  <tr>
    <td>executorCpuTime</td>
    <td>CPU time the executor spent running this task. This includes time fetching shuffle data.
    The value is expressed in nanoseconds.</td>
  </tr>
  <tr>
    <td>executorDeserializeTime</td>
    <td>Elapsed time spent to deserialize this task. The value is expressed in milliseconds.</td>
  </tr>
  <tr>
    <td>executorDeserializeCpuTime</td>
    <td>CPU time taken on the executor to deserialize this task. The value is expressed
    in nanoseconds.</td>
  </tr>
  <tr>
    <td>resultSize</td>
    <td>The number of bytes this task transmitted back to the driver as the TaskResult.</td>
  </tr>
  <tr>
    <td>jvmGCTime</td>
    <td>Elapsed time the JVM spent in garbage collection while executing this task.
    The value is expressed in milliseconds.</td>
  </tr>
  <tr>
    <td>resultSerializationTime</td>
    <td>Elapsed time spent serializing the task result. The value is expressed in milliseconds.</td>
  </tr>
  <tr>
    <td>memoryBytesSpilled</td>
    <td>The number of in-memory bytes spilled by this task.</td>
  </tr>
  <tr>
    <td>diskBytesSpilled</td>
    <td>The number of on-disk bytes spilled by this task.</td>
  </tr>
  <tr>
    <td>peakExecutionMemory</td>
    <td>Peak memory used by internal data structures created during shuffles, aggregations and
        joins. The value of this accumulator should be approximately the sum of the peak sizes
        across all such data structures created in this task. For SQL jobs, this only tracks all
         unsafe operators and ExternalSort.</td>
  </tr>
  <tr>
    <td>inputMetrics.*</td>
    <td>Metrics related to reading data from <code>org.apache.spark.rdd.HadoopRDD</code>
    or from persisted data.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.bytesRead</td>
    <td>Total number of bytes read.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.recordsRead</td>
    <td>Total number of records read.</td>
  </tr>
  <tr>
    <td>outputMetrics.*</td>
    <td>Metrics related to writing data externally (e.g. to a distributed filesystem),
    defined only in tasks with output.</td>            
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.bytesWritten</td>
    <td>Total number of bytes written</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.recordsWritten</td>
    <td>Total number of records written</td>
  </tr>
  <tr>
    <td>shuffleReadMetrics.*</td>
    <td>Metrics related to shuffle read operations.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.recordsRead</td>
    <td>Number of records read in shuffle operations</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.remoteBlocksFetched</td>
    <td>Number of remote blocks fetched in shuffle operations</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.localBlocksFetched</td>
    <td>Number of local (as opposed to read from a remote executor) blocks fetched
    in shuffle operations</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.totalBlocksFetched</td>
    <td>Number of blocks fetched in shuffle operations (both local and remote)</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.remoteBytesRead</td>
    <td>Number of remote bytes read in shuffle operations</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.localBytesRead</td>
    <td>Number of bytes read in shuffle operations from local disk (as opposed to
    read from a remote executor)</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.totalBytesRead</td>
    <td>Number of bytes read in shuffle operations (both local and remote)</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.remoteBytesReadToDisk</td>
    <td>Number of remote bytes read to disk in shuffle operations.
    Large blocks are fetched to disk in shuffle read operations, as opposed to 
    being read into memory, which is the default behavior.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.fetchWaitTime</td>
    <td>Time the task spent waiting for remote shuffle blocks. 
        This only includes the time blocking on shuffle input data.
        For instance if block B is being fetched while the task is still not finished 
        processing block A, it is not considered to be blocking on block B.
        The value is expressed in milliseconds.</td>
  </tr>
  <tr>
    <td>shuffleWriteMetrics.*</td>
    <td>Metrics related to operations writing shuffle data.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.bytesWritten</td>
    <td>Number of bytes written in shuffle operations</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.recordsWritten</td>
    <td>Number of records written in shuffle operations</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.writeTime</td>
    <td>Time spent blocking on writes to disk or buffer cache. The value is expressed
     in nanoseconds.</td>
  </tr>
</table>

### Executor Metrics

Executor-level metrics are sent from each executor to the driver as part of the Heartbeat to describe the performance metrics of Executor itself like JVM heap memory, GC information.
Executor metric values and their measured memory peak values per executor are exposed via the REST API in JSON format and in Prometheus format.
The JSON end point is exposed at: `/applications/[app-id]/executors`, and the Prometheus endpoint at: `/metrics/executors/prometheus`.
The Prometheus endpoint is conditional to a configuration parameter: `spark.ui.prometheus.enabled=true` (the default is `false`).
In addition, aggregated per-stage peak values of the executor memory metrics are written to the event log if
`spark.eventLog.logStageExecutorMetrics` is true.  
Executor memory metrics are also exposed via the Spark metrics system based on the [Dropwizard metrics library](http://metrics.dropwizard.io/4.2.0).
A list of the available metrics, with a short description:

<table class="table">
  <tr><th>Executor Level Metric name</th>
      <th>Short description</th>
  </tr>
  <tr>
    <td>rddBlocks</td>
    <td>RDD blocks in the block manager of this executor.</td>
  </tr>
  <tr>
    <td>memoryUsed</td>
    <td>Storage memory used by this executor.</td>
  </tr>
  <tr>
    <td>diskUsed</td>
    <td>Disk space used for RDD storage by this executor.</td>
  </tr>
  <tr>
    <td>totalCores</td>
    <td>Number of cores available in this executor.</td>
  </tr>
  <tr>
    <td>maxTasks</td>
    <td>Maximum number of tasks that can run concurrently in this executor.</td>
  </tr>
  <tr>
    <td>activeTasks</td>
    <td>Number of tasks currently executing.</td>
  </tr>
  <tr>
    <td>failedTasks</td>
    <td>Number of tasks that have failed in this executor.</td>
  </tr>
  <tr>
    <td>completedTasks</td>
    <td>Number of tasks that have completed in this executor.</td>
  </tr>
  <tr>
    <td>totalTasks</td>
    <td>Total number of tasks (running, failed and completed) in this executor.</td>
  </tr>
  <tr>
    <td>totalDuration</td>
    <td>Elapsed time the JVM spent executing tasks in this executor.
    The value is expressed in milliseconds.</td>
  </tr>
  <tr>
    <td>totalGCTime</td>
    <td>Elapsed time the JVM spent in garbage collection summed in this executor.
    The value is expressed in milliseconds.</td>
  </tr>
  <tr>
    <td>totalInputBytes</td>
    <td>Total input bytes summed in this executor.</td>
  </tr>
  <tr>
    <td>totalShuffleRead</td>
    <td>Total shuffle read bytes summed in this executor.</td>
  </tr>
  <tr>
    <td>totalShuffleWrite</td>
    <td>Total shuffle write bytes summed in this executor.</td>
  </tr>
  <tr>
    <td>maxMemory</td>
    <td>Total amount of memory available for storage, in bytes.</td>
  </tr>
  <tr>
    <td>memoryMetrics.*</td>
    <td>Current value of memory metrics:</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.usedOnHeapStorageMemory</td>
    <td>Used on heap memory currently for storage, in bytes.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.usedOffHeapStorageMemory</td>
    <td>Used off heap memory currently for storage, in bytes.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.totalOnHeapStorageMemory</td>
    <td>Total available on heap memory for storage, in bytes. This amount can vary over time,  on the MemoryManager implementation.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.totalOffHeapStorageMemory</td>
    <td>Total available off heap memory for storage, in bytes. This amount can vary over time, depending on the MemoryManager implementation.</td>
  </tr>
  <tr>
    <td>peakMemoryMetrics.*</td>
    <td>Peak value of memory (and GC) metrics:</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.JVMHeapMemory</td>
    <td>Peak memory usage of the heap that is used for object allocation.
    The heap consists of one or more memory pools. The used and committed size of the returned memory usage is the sum of those values of all heap memory pools whereas the init and max size of the returned memory usage represents the setting of the heap memory which may not be the sum of those of all heap memory pools.
    The amount of used memory in the returned memory usage is the amount of memory occupied by both live objects and garbage objects that have not been collected, if any.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.JVMOffHeapMemory</td>
    <td>Peak memory usage of non-heap memory that is used by the Java virtual machine. The non-heap memory consists of one or more memory pools. The used and committed size of the returned memory usage is the sum of those values of all non-heap memory pools whereas the init and max size of the returned memory usage represents the setting of the non-heap memory which may not be the sum of those of all non-heap memory pools.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.OnHeapExecutionMemory</td>
    <td>Peak on heap execution memory in use, in bytes.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.OffHeapExecutionMemory</td>
    <td>Peak off heap execution memory in use, in bytes.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.OnHeapStorageMemory</td>
    <td>Peak on heap storage memory in use, in bytes.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.OffHeapStorageMemory</td>
    <td>Peak off heap storage memory in use, in bytes.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.OnHeapUnifiedMemory</td>
    <td>Peak on heap memory (execution and storage).</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.OffHeapUnifiedMemory</td>
    <td>Peak off heap memory (execution and storage).</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.DirectPoolMemory</td>
    <td>Peak memory that the JVM is using for direct buffer pool (<code>java.lang.management.BufferPoolMXBean</code>)</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.MappedPoolMemory</td>
    <td>Peak memory that the JVM is using for mapped buffer pool (<code>java.lang.management.BufferPoolMXBean</code>)</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.ProcessTreeJVMVMemory</td>
    <td>Virtual memory size in bytes. Enabled if spark.executor.processTreeMetrics.enabled is true.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.ProcessTreeJVMRSSMemory</td>
    <td>Resident Set Size: number of pages the process has
      in real memory.  This is just the pages which count
      toward text, data, or stack space.  This does not
      include pages which have not been demand-loaded in,
      or which are swapped out. Enabled if spark.executor.processTreeMetrics.enabled is true.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.ProcessTreePythonVMemory</td>
    <td>Virtual memory size for Python in bytes. Enabled if spark.executor.processTreeMetrics.enabled is true.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.ProcessTreePythonRSSMemory</td>
    <td>Resident Set Size for Python. Enabled if spark.executor.processTreeMetrics.enabled is true.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.ProcessTreeOtherVMemory</td>
    <td>Virtual memory size for other kind of process in bytes. Enabled if spark.executor.processTreeMetrics.enabled is true.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.ProcessTreeOtherRSSMemory</td>
    <td>Resident Set Size for other kind of process. Enabled if spark.executor.processTreeMetrics.enabled is true.</td>
  </tr>
    <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.MinorGCCount</td>
    <td>Total minor GC count. For example, the garbage collector is one of     Copy, PS Scavenge, ParNew, G1 Young Generation and so on.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.MinorGCTime</td>
    <td>Elapsed total minor GC time. 
    The value is expressed in milliseconds.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.MajorGCCount</td>
    <td>Total major GC count. For example, the garbage collector is one of     MarkSweepCompact, PS MarkSweep, ConcurrentMarkSweep, G1 Old Generation and so on.</td>
  </tr>
  <tr>
    <td>&nbsp;&nbsp;&nbsp;&nbsp;.MajorGCTime</td>
    <td>Elapsed total major GC time. 
    The value is expressed in milliseconds.</td>
  </tr>
</table>
The computation of RSS and Vmem are based on [proc(5)](http://man7.org/linux/man-pages/man5/proc.5.html)

### API Versioning Policy

These endpoints have been strongly versioned to make it easier to develop applications on top.
 In particular, Spark guarantees:

* Endpoints will never be removed from one version
* Individual fields will never be removed for any given endpoint
* New endpoints may be added
* New fields may be added to existing endpoints
* New versions of the api may be added in the future as a separate endpoint (e.g., `api/v2`).  New versions are *not* required to be backwards compatible.
* Api versions may be dropped, but only after at least one minor release of co-existing with a new api version.

Note that even when examining the UI of running applications, the `applications/[app-id]` portion is
still required, though there is only one application available.  E.g. to see the list of jobs for the
running app, you would go to `http://localhost:4040/api/v1/applications/[app-id]/jobs`.  This is to
keep the paths consistent in both modes.

# Metrics

Spark has a configurable metrics system based on the
[Dropwizard Metrics Library](http://metrics.dropwizard.io/4.2.0).
This allows users to report Spark metrics to a variety of sinks including HTTP, JMX, and CSV
files. The metrics are generated by sources embedded in the Spark code base. They
provide instrumentation for specific activities and Spark components.
The metrics system is configured via a configuration file that Spark expects to be present
at `$SPARK_HOME/conf/metrics.properties`. A custom file location can be specified via the
`spark.metrics.conf` [configuration property](configuration.html#spark-properties).
Instead of using the configuration file, a set of configuration parameters with prefix
`spark.metrics.conf.` can be used.
By default, the root namespace used for driver or executor metrics is 
the value of `spark.app.id`. However, often times, users want to be able to track the metrics 
across apps for driver and executors, which is hard to do with application ID 
(i.e. `spark.app.id`) since it changes with every invocation of the app. For such use cases,
a custom namespace can be specified for metrics reporting using `spark.metrics.namespace`
configuration property. 
If, say, users wanted to set the metrics namespace to the name of the application, they
can set the `spark.metrics.namespace` property to a value like `${spark.app.name}`. This value is
then expanded appropriately by Spark and is used as the root namespace of the metrics system. 
Non-driver and executor metrics are never prefixed with `spark.app.id`, nor does the
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
* `applicationMaster`: The Spark ApplicationMaster when running on YARN.
* `mesos_cluster`: The Spark cluster scheduler when running on Mesos.

Each instance can report to zero or more _sinks_. Sinks are contained in the
`org.apache.spark.metrics.sink` package:

* `ConsoleSink`: Logs metrics information to the console.
* `CSVSink`: Exports metrics data to CSV files at regular intervals.
* `JmxSink`: Registers metrics for viewing in a JMX console.
* `MetricsServlet`: Adds a servlet within the existing Spark UI to serve metrics data as JSON data.
* `PrometheusServlet`: (Experimental) Adds a servlet within the existing Spark UI to serve metrics data in Prometheus format.
* `GraphiteSink`: Sends metrics to a Graphite node.
* `Slf4jSink`: Sends metrics to slf4j as log entries.
* `StatsdSink`: Sends metrics to a StatsD node.

Spark also supports a Ganglia sink which is not included in the default build due to
licensing restrictions:

* `GangliaSink`: Sends metrics to a Ganglia node or multicast group.

To install the `GangliaSink` you'll need to perform a custom build of Spark. _**Note that
by embedding this library you will include [LGPL](http://www.gnu.org/copyleft/lesser.html)-licensed
code in your Spark package**_. For sbt users, set the
`SPARK_GANGLIA_LGPL` environment variable before building. For Maven users, enable
the `-Pspark-ganglia-lgpl` profile. In addition to modifying the cluster's Spark build
user applications will need to link to the `spark-ganglia-lgpl` artifact.

The syntax of the metrics configuration file and the parameters available for each sink are defined
in an example configuration file,
`$SPARK_HOME/conf/metrics.properties.template`.

When using Spark configuration parameters instead of the metrics configuration file, the relevant
parameter names are composed by the prefix `spark.metrics.conf.` followed by the configuration
details, i.e. the parameters take the following form:
`spark.metrics.conf.[instance|*].sink.[sink_name].[parameter_name]`.
This example shows a list of Spark configuration parameters for a Graphite sink:
```
"spark.metrics.conf.*.sink.graphite.class"="org.apache.spark.metrics.sink.GraphiteSink"
"spark.metrics.conf.*.sink.graphite.host"="graphiteEndPoint_hostName>"
"spark.metrics.conf.*.sink.graphite.port"=<graphite_listening_port>
"spark.metrics.conf.*.sink.graphite.period"=10
"spark.metrics.conf.*.sink.graphite.unit"=seconds
"spark.metrics.conf.*.sink.graphite.prefix"="optional_prefix"
"spark.metrics.conf.*.sink.graphite.regex"="optional_regex_to_send_matching_metrics"
```

Default values of the Spark metrics configuration are as follows:
```
"*.sink.servlet.class" = "org.apache.spark.metrics.sink.MetricsServlet"
"*.sink.servlet.path" = "/metrics/json"
"master.sink.servlet.path" = "/metrics/master/json"
"applications.sink.servlet.path" = "/metrics/applications/json"
```

Additional sources can be configured using the metrics configuration file or the configuration
parameter `spark.metrics.conf.[component_name].source.jvm.class=[source_name]`. At present the 
JVM source is the only available optional source. For example the following configuration parameter
activates the JVM source:
`"spark.metrics.conf.*.source.jvm.class"="org.apache.spark.metrics.source.JvmSource"`

## List of available metrics providers 

Metrics used by Spark are of multiple types: gauge, counter, histogram, meter and timer, 
see [Dropwizard library documentation for details](https://metrics.dropwizard.io/4.2.0/getting-started.html).
The following list of components and metrics reports the name and some details about the available metrics,
grouped per component instance and source namespace.
The most common time of metrics used in Spark instrumentation are gauges and counters. 
Counters can be recognized as they have the `.count` suffix. Timers, meters and histograms are annotated
in the list, the rest of the list elements are metrics of type gauge.
The large majority of metrics are active as soon as their parent component instance is configured,
some metrics require also to be enabled via an additional configuration parameter, the details are
reported in the list.

### Component instance = Driver
This is the component with the largest amount of instrumented metrics

- namespace=BlockManager
  - disk.diskSpaceUsed_MB
  - memory.maxMem_MB
  - memory.maxOffHeapMem_MB
  - memory.maxOnHeapMem_MB
  - memory.memUsed_MB
  - memory.offHeapMemUsed_MB
  - memory.onHeapMemUsed_MB
  - memory.remainingMem_MB
  - memory.remainingOffHeapMem_MB
  - memory.remainingOnHeapMem_MB

- namespace=HiveExternalCatalog
  - **note:** these metrics are conditional to a configuration parameter:
    `spark.metrics.staticSources.enabled` (default is true) 
  - fileCacheHits.count
  - filesDiscovered.count
  - hiveClientCalls.count
  - parallelListingJobCount.count
  - partitionsFetched.count

- namespace=CodeGenerator
  - **note:** these metrics are conditional to a configuration parameter:
    `spark.metrics.staticSources.enabled` (default is true) 
  - compilationTime (histogram)
  - generatedClassSize (histogram)
  - generatedMethodSize (histogram)
  - sourceCodeSize (histogram)

- namespace=DAGScheduler
  - job.activeJobs 
  - job.allJobs
  - messageProcessingTime (timer)
  - stage.failedStages
  - stage.runningStages
  - stage.waitingStages

- namespace=LiveListenerBus
  - listenerProcessingTime.org.apache.spark.HeartbeatReceiver (timer)
  - listenerProcessingTime.org.apache.spark.scheduler.EventLoggingListener (timer)
  - listenerProcessingTime.org.apache.spark.status.AppStatusListener (timer)
  - numEventsPosted.count
  - queue.appStatus.listenerProcessingTime (timer)
  - queue.appStatus.numDroppedEvents.count
  - queue.appStatus.size
  - queue.eventLog.listenerProcessingTime (timer)
  - queue.eventLog.numDroppedEvents.count
  - queue.eventLog.size
  - queue.executorManagement.listenerProcessingTime (timer)

- namespace=appStatus (all metrics of type=counter)
  - **note:** Introduced in Spark 3.0. Conditional to a configuration parameter:  
   `spark.metrics.appStatusSource.enabled` (default is false)
  - stages.failedStages.count
  - stages.skippedStages.count
  - stages.completedStages.count
  - tasks.blackListedExecutors.count // deprecated use excludedExecutors instead
  - tasks.excludedExecutors.count
  - tasks.completedTasks.count
  - tasks.failedTasks.count
  - tasks.killedTasks.count
  - tasks.skippedTasks.count
  - tasks.unblackListedExecutors.count // deprecated use unexcludedExecutors instead
  - tasks.unexcludedExecutors.count
  - jobs.succeededJobs
  - jobs.failedJobs
  - jobDuration
  
- namespace=AccumulatorSource  
  - **note:** User-configurable sources to attach accumulators to metric system
  - DoubleAccumulatorSource
  - LongAccumulatorSource

- namespace=spark.streaming
  - **note:** This applies to Spark Structured Streaming only. Conditional to a configuration
  parameter: `spark.sql.streaming.metricsEnabled=true` (default is false) 
  - eventTime-watermark
  - inputRate-total
  - latency
  - processingRate-total
  - states-rowsTotal
  - states-usedBytes

- namespace=JVMCPU
  - jvmCpuTime

- namespace=executor
  - **note:** These metrics are available in the driver in local mode only.
  - A full list of available metrics in this 
    namespace can be found in the corresponding entry for the Executor component instance.
    
- namespace=ExecutorMetrics
  - **note:** these metrics are conditional to a configuration parameter:
    `spark.metrics.executorMetricsSource.enabled` (default is true) 
  - This source contains memory-related metrics. A full list of available metrics in this 
    namespace can be found in the corresponding entry for the Executor component instance.

- namespace=ExecutorAllocationManager
  - **note:** these metrics are only emitted when using dynamic allocation. Conditional to a configuration
    parameter `spark.dynamicAllocation.enabled` (default is false)
  - executors.numberExecutorsToAdd  
  - executors.numberExecutorsPendingToRemove
  - executors.numberAllExecutors
  - executors.numberTargetExecutors
  - executors.numberMaxNeededExecutors
  - executors.numberExecutorsGracefullyDecommissioned.count
  - executors.numberExecutorsDecommissionUnfinished.count
  - executors.numberExecutorsExitedUnexpectedly.count
  - executors.numberExecutorsKilledByDriver.count

- namespace=plugin.\<Plugin Class Name>
  - Optional namespace(s). Metrics in this namespace are defined by user-supplied code, and
  configured using the Spark plugin API. See "Advanced Instrumentation" below for how to load
  custom plugins into Spark.

### Component instance = Executor
These metrics are exposed by Spark executors. 
 
- namespace=executor (metrics are of type counter or gauge)
  - **notes:**
    - `spark.executor.metrics.fileSystemSchemes` (default: `file,hdfs`) determines the exposed file system metrics.
  - bytesRead.count
  - bytesWritten.count
  - cpuTime.count
  - deserializeCpuTime.count
  - deserializeTime.count
  - diskBytesSpilled.count
  - filesystem.file.largeRead_ops
  - filesystem.file.read_bytes
  - filesystem.file.read_ops
  - filesystem.file.write_bytes
  - filesystem.file.write_ops
  - filesystem.hdfs.largeRead_ops
  - filesystem.hdfs.read_bytes
  - filesystem.hdfs.read_ops
  - filesystem.hdfs.write_bytes
  - filesystem.hdfs.write_ops
  - jvmGCTime.count
  - memoryBytesSpilled.count
  - recordsRead.count
  - recordsWritten.count
  - resultSerializationTime.count
  - resultSize.count
  - runTime.count
  - shuffleBytesWritten.count
  - shuffleFetchWaitTime.count
  - shuffleLocalBlocksFetched.count
  - shuffleLocalBytesRead.count
  - shuffleRecordsRead.count
  - shuffleRecordsWritten.count
  - shuffleRemoteBlocksFetched.count
  - shuffleRemoteBytesRead.count
  - shuffleRemoteBytesReadToDisk.count
  - shuffleTotalBytesRead.count
  - shuffleWriteTime.count
  - succeededTasks.count
  - threadpool.activeTasks
  - threadpool.completeTasks
  - threadpool.currentPool_size
  - threadpool.maxPool_size
  - threadpool.startedTasks

- namespace=ExecutorMetrics
  - **notes:** 
    - These metrics are conditional to a configuration parameter:
    `spark.metrics.executorMetricsSource.enabled` (default value is true) 
    - ExecutorMetrics are updated as part of heartbeat processes scheduled
   for the executors and for the driver at regular intervals: `spark.executor.heartbeatInterval` (default value is 10 seconds)
    - An optional faster polling mechanism is available for executor memory metrics, 
   it can be activated by setting a polling interval (in milliseconds) using the configuration parameter `spark.executor.metrics.pollingInterval`
  - JVMHeapMemory
  - JVMOffHeapMemory
  - OnHeapExecutionMemory
  - OnHeapStorageMemory
  - OnHeapUnifiedMemory
  - OffHeapExecutionMemory
  - OffHeapStorageMemory
  - OffHeapUnifiedMemory
  - DirectPoolMemory
  - MappedPoolMemory
  - MinorGCCount
  - MinorGCTime
  - MajorGCCount
  - MajorGCTime
  - "ProcessTree*" metric counters:
    - ProcessTreeJVMVMemory
    - ProcessTreeJVMRSSMemory
    - ProcessTreePythonVMemory
    - ProcessTreePythonRSSMemory
    - ProcessTreeOtherVMemory
    - ProcessTreeOtherRSSMemory
    - **note:** "ProcessTree*" metrics are collected only under certain conditions.
      The conditions are the logical AND of the following: `/proc` filesystem exists,
      `spark.executor.processTreeMetrics.enabled=true`.
      "ProcessTree*" metrics report 0 when those conditions are not met.

- namespace=JVMCPU
  - jvmCpuTime

- namespace=NettyBlockTransfer
  - shuffle-client.usedDirectMemory
  - shuffle-client.usedHeapMemory
  - shuffle-server.usedDirectMemory
  - shuffle-server.usedHeapMemory

- namespace=HiveExternalCatalog
  - **note:** these metrics are conditional to a configuration parameter:
    `spark.metrics.staticSources.enabled` (default is true) 
  - fileCacheHits.count
  - filesDiscovered.count
  - hiveClientCalls.count
  - parallelListingJobCount.count
  - partitionsFetched.count

- namespace=CodeGenerator
  - **note:** these metrics are conditional to a configuration parameter:
    `spark.metrics.staticSources.enabled` (default is true) 
  - compilationTime (histogram)
  - generatedClassSize (histogram)
  - generatedMethodSize (histogram)
  - sourceCodeSize (histogram)

- namespace=plugin.\<Plugin Class Name>
  - Optional namespace(s). Metrics in this namespace are defined by user-supplied code, and
  configured using the Spark plugin API. See "Advanced Instrumentation" below for how to load
  custom plugins into Spark.

### Source = JVM Source 
Notes: 
  - Activate this source by setting the relevant `metrics.properties` file entry or the 
  configuration parameter:`spark.metrics.conf.*.source.jvm.class=org.apache.spark.metrics.source.JvmSource`  
  - These metrics are conditional to a configuration parameter:
    `spark.metrics.staticSources.enabled` (default is true)
  - This source is available for driver and executor instances and is also available for other instances.  
  - This source provides information on JVM metrics using the 
  [Dropwizard/Codahale Metric Sets for JVM instrumentation](https://metrics.dropwizard.io/4.2.0/manual/jvm.html)
   and in particular the metric sets BufferPoolMetricSet, GarbageCollectorMetricSet and MemoryUsageGaugeSet. 

### Component instance = applicationMaster
Note: applies when running on YARN

- numContainersPendingAllocate
- numExecutorsFailed
- numExecutorsRunning
- numLocalityAwareTasks
- numReleasedContainers

### Component instance = mesos_cluster
Note: applies when running on mesos

- waitingDrivers
- launchedDrivers
- retryDrivers

### Component instance = master
Note: applies when running in Spark standalone as master

- workers
- aliveWorkers
- apps
- waitingApps

### Component instance = ApplicationSource
Note: applies when running in Spark standalone as master

- status
- runtime_ms
- cores

### Component instance = worker
Note: applies when running in Spark standalone as worker

- executors
- coresUsed
- memUsed_MB
- coresFree
- memFree_MB

### Component instance = shuffleService
Note: applies to the shuffle service

- blockTransferRate (meter) - rate of blocks being transferred
- blockTransferMessageRate (meter) - rate of block transfer messages,
  i.e. if batch fetches are enabled, this represents number of batches rather than number of blocks
- blockTransferRateBytes (meter)
- blockTransferAvgTime_1min (gauge - 1-minute moving average)
- numActiveConnections.count
- numRegisteredConnections.count
- numCaughtExceptions.count
- openBlockRequestLatencyMillis (histogram)
- registerExecutorRequestLatencyMillis (histogram)
- registeredExecutorsSize
- shuffle-server.usedDirectMemory
- shuffle-server.usedHeapMemory

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

Spark also provides a plugin API so that custom instrumentation code can be added to Spark
applications. There are two configuration keys available for loading plugins into Spark:

- <code>spark.plugins</code>
- <code>spark.plugins.defaultList</code>

Both take a comma-separated list of class names that implement the
<code>org.apache.spark.api.plugin.SparkPlugin</code> interface. The two names exist so that it's
possible for one list to be placed in the Spark default config file, allowing users to
easily add other plugins from the command line without overwriting the config file's list. Duplicate
plugins are ignored.
