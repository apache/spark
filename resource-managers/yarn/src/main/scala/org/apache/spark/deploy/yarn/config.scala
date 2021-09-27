/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.deploy.yarn

import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

package object config extends Logging {

  /* Common app configuration. */

  private[spark] val APPLICATION_TAGS = ConfigBuilder("spark.yarn.tags")
    .doc("Comma-separated list of strings to pass through as YARN application tags appearing " +
      "in YARN Application Reports, which can be used for filtering when querying YARN.")
    .version("1.5.0")
    .stringConf
    .toSequence
    .createOptional

  private[spark] val APPLICATION_PRIORITY = ConfigBuilder("spark.yarn.priority")
    .doc("Application priority for YARN to define pending applications ordering policy, those" +
      " with higher value have a better opportunity to be activated. Currently, YARN only" +
      " supports application priority when using FIFO ordering policy.")
    .version("3.0.0")
    .intConf
    .createOptional

  private[spark] val AM_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS =
    ConfigBuilder("spark.yarn.am.attemptFailuresValidityInterval")
      .doc("Interval after which AM failures will be considered independent and " +
        "not accumulate towards the attempt count.")
      .version("1.6.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val AM_CLIENT_MODE_EXIT_ON_ERROR =
    ConfigBuilder("spark.yarn.am.clientModeExitOnError")
      .doc("In yarn-client mode, when this is true, if driver got " +
        "application report with final status of KILLED or FAILED, " +
        "driver will stop corresponding SparkContext and exit program with code 1. " +
        "Note, if this is true and called from another application, it will terminate " +
        "the parent application as well.")
      .version("3.3.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS =
    ConfigBuilder("spark.yarn.executor.failuresValidityInterval")
      .doc("Interval after which Executor failures will be considered independent and not " +
        "accumulate towards the attempt count.")
      .version("2.0.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val MAX_APP_ATTEMPTS = ConfigBuilder("spark.yarn.maxAppAttempts")
    .doc("Maximum number of AM attempts before failing the app.")
    .version("1.3.0")
    .intConf
    .createOptional

  private[spark] val USER_CLASS_PATH_FIRST = ConfigBuilder("spark.yarn.user.classpath.first")
    .doc("Whether to place user jars in front of Spark's classpath.")
    .version("1.3.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val POPULATE_HADOOP_CLASSPATH = ConfigBuilder("spark.yarn.populateHadoopClasspath")
    .doc("Whether to populate Hadoop classpath from `yarn.application.classpath` and " +
      "`mapreduce.application.classpath` Note that if this is set to `false`, it requires " +
      "a `with-Hadoop` Spark distribution that bundles Hadoop runtime or user has to provide " +
      "a Hadoop installation separately. By default, for `with-hadoop` Spark distribution, " +
      "this is set to `false`; for `no-hadoop` distribution, this is set to `true`.")
    .version("2.4.6")
    .booleanConf
    .createWithDefault(isHadoopProvided())

  private[spark] val GATEWAY_ROOT_PATH = ConfigBuilder("spark.yarn.config.gatewayPath")
    .doc("Root of configuration paths that is present on gateway nodes, and will be replaced " +
      "with the corresponding path in cluster machines.")
    .version("1.5.0")
    .stringConf
    .createWithDefault(null)

  private[spark] val REPLACEMENT_ROOT_PATH = ConfigBuilder("spark.yarn.config.replacementPath")
    .doc(s"Path to use as a replacement for ${GATEWAY_ROOT_PATH.key} when launching processes " +
      "in the YARN cluster.")
    .version("1.5.0")
    .stringConf
    .createWithDefault(null)

  private[spark] val QUEUE_NAME = ConfigBuilder("spark.yarn.queue")
    .version("1.0.0")
    .stringConf
    .createWithDefault("default")

  private[spark] val HISTORY_SERVER_ADDRESS = ConfigBuilder("spark.yarn.historyServer.address")
    .version("1.0.0")
    .stringConf
    .createOptional

  private[spark] val ALLOW_HISTORY_SERVER_TRACKING_URL =
    ConfigBuilder("spark.yarn.historyServer.allowTracking")
      .doc("Allow using the History Server URL for the application as the tracking URL for the " +
        "application when the Web UI is not enabled.")
      .version("2.2.0")
      .booleanConf
      .createWithDefault(false)

  private[spark] val APPLICATION_TYPE = ConfigBuilder("spark.yarn.applicationType")
    .doc("Type of this application," +
      "it allows user to specify a more specific type for the application, such as SPARK," +
      "SPARK-SQL, SPARK-STREAMING, SPARK-MLLIB and SPARK-GRAPH")
    .version("3.1.0")
    .stringConf
    .createWithDefault("SPARK")

  /* File distribution. */

  private[spark] val SPARK_ARCHIVE = ConfigBuilder("spark.yarn.archive")
    .doc("Location of archive containing jars files with Spark classes.")
    .version("2.0.0")
    .stringConf
    .createOptional

  private[spark] val SPARK_JARS = ConfigBuilder("spark.yarn.jars")
    .doc("Location of jars containing Spark classes.")
    .version("2.0.0")
    .stringConf
    .toSequence
    .createOptional

  private[spark] val ARCHIVES_TO_DISTRIBUTE = ConfigBuilder("spark.yarn.dist.archives")
    .version("1.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val FILES_TO_DISTRIBUTE = ConfigBuilder("spark.yarn.dist.files")
    .version("1.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val JARS_TO_DISTRIBUTE = ConfigBuilder("spark.yarn.dist.jars")
    .version("2.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val PRESERVE_STAGING_FILES = ConfigBuilder("spark.yarn.preserve.staging.files")
    .doc("Whether to preserve temporary files created by the job in HDFS.")
    .version("1.1.0")
    .booleanConf
    .createWithDefault(false)

  private[spark] val STAGING_FILE_REPLICATION = ConfigBuilder("spark.yarn.submit.file.replication")
    .doc("Replication factor for files uploaded by Spark to HDFS.")
    .version("0.8.1")
    .intConf
    .createOptional

  /* Launcher configuration. */

  private[spark] val WAIT_FOR_APP_COMPLETION = ConfigBuilder("spark.yarn.submit.waitAppCompletion")
    .doc("In cluster mode, whether to wait for the application to finish before exiting the " +
      "launcher process.")
    .version("1.4.0")
    .booleanConf
    .createWithDefault(true)

  private[spark] val REPORT_INTERVAL = ConfigBuilder("spark.yarn.report.interval")
    .doc("Interval between reports of the current app status.")
    .version("0.9.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("1s")

  private[spark] val CLIENT_LAUNCH_MONITOR_INTERVAL =
    ConfigBuilder("spark.yarn.clientLaunchMonitorInterval")
      .doc("Interval between requests for status the client mode AM when starting the app.")
      .version("2.3.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("1s")

  private[spark] val CLIENT_INCLUDE_DRIVER_LOGS_LINK =
    ConfigBuilder("spark.yarn.includeDriverLogsLink")
      .doc("In cluster mode, whether the client application report includes links to the driver "
          + "container's logs. This requires polling the ResourceManager's REST API, so it "
          + "places some additional load on the RM.")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  /* Shared Client-mode AM / Driver configuration. */

  private[spark] val AM_MAX_WAIT_TIME = ConfigBuilder("spark.yarn.am.waitTime")
    .version("1.3.0")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("100s")

  private[spark] val YARN_METRICS_NAMESPACE = ConfigBuilder("spark.yarn.metrics.namespace")
    .doc("The root namespace for AM metrics reporting.")
    .version("2.4.0")
    .stringConf
    .createOptional

  private[spark] val AM_NODE_LABEL_EXPRESSION = ConfigBuilder("spark.yarn.am.nodeLabelExpression")
    .doc("Node label expression for the AM.")
    .version("1.6.0")
    .stringConf
    .createOptional

  private[spark] val CONTAINER_LAUNCH_MAX_THREADS =
    ConfigBuilder("spark.yarn.containerLauncherMaxThreads")
      .version("1.2.0")
      .intConf
      .createWithDefault(25)

  private[spark] val MAX_EXECUTOR_FAILURES = ConfigBuilder("spark.yarn.max.executor.failures")
    .version("1.0.0")
    .intConf
    .createOptional

  private[spark] val MAX_REPORTER_THREAD_FAILURES =
    ConfigBuilder("spark.yarn.scheduler.reporterThread.maxFailures")
      .version("1.2.0")
      .intConf
      .createWithDefault(5)

  private[spark] val RM_HEARTBEAT_INTERVAL =
    ConfigBuilder("spark.yarn.scheduler.heartbeat.interval-ms")
      .version("0.8.1")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")

  private[spark] val INITIAL_HEARTBEAT_INTERVAL =
    ConfigBuilder("spark.yarn.scheduler.initial-allocation.interval")
      .version("1.4.0")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("200ms")

  private[spark] val AM_FINAL_MSG_LIMIT = ConfigBuilder("spark.yarn.am.finalMessageLimit")
    .doc("The limit size of final diagnostic message for our ApplicationMaster to unregister from" +
      " the ResourceManager.")
    .version("2.4.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("1m")

  /* Client-mode AM configuration. */

  private[spark] val AM_CORES = ConfigBuilder("spark.yarn.am.cores")
    .version("1.3.0")
    .intConf
    .createWithDefault(1)

  private[spark] val AM_JAVA_OPTIONS = ConfigBuilder("spark.yarn.am.extraJavaOptions")
    .doc("Extra Java options for the client-mode AM.")
    .version("1.3.0")
    .stringConf
    .createOptional

  private[spark] val AM_LIBRARY_PATH = ConfigBuilder("spark.yarn.am.extraLibraryPath")
    .doc("Extra native library path for the client-mode AM.")
    .version("1.4.0")
    .stringConf
    .createOptional

  private[spark] val AM_MEMORY_OVERHEAD = ConfigBuilder("spark.yarn.am.memoryOverhead")
    .version("1.3.0")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  private[spark] val AM_MEMORY = ConfigBuilder("spark.yarn.am.memory")
    .version("1.3.0")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("512m")

  /* Driver configuration. */

  private[spark] val DRIVER_APP_UI_ADDRESS = ConfigBuilder("spark.driver.appUIAddress")
    .version("1.1.0")
    .stringConf
    .createOptional

  /* Executor configuration. */

  private[spark] val EXECUTOR_NODE_LABEL_EXPRESSION =
    ConfigBuilder("spark.yarn.executor.nodeLabelExpression")
      .doc("Node label expression for executors.")
      .version("1.4.0")
      .stringConf
      .createOptional

  /* Unmanaged AM configuration. */

  private[spark] val YARN_UNMANAGED_AM = ConfigBuilder("spark.yarn.unmanagedAM.enabled")
    .doc("In client mode, whether to launch the Application Master service as part of the client " +
      "using unmanaged am.")
    .version("3.0.0")
    .booleanConf
    .createWithDefault(false)

  /* Rolled log aggregation configuration. */

  private[spark] val ROLLED_LOG_INCLUDE_PATTERN =
    ConfigBuilder("spark.yarn.rolledLog.includePattern")
      .doc("Java Regex to filter the log files which match the defined include pattern and those " +
        "log files will be aggregated in a rolling fashion.")
      .version("2.0.0")
      .stringConf
      .createOptional

  private[spark] val ROLLED_LOG_EXCLUDE_PATTERN =
    ConfigBuilder("spark.yarn.rolledLog.excludePattern")
      .doc("Java Regex to filter the log files which match the defined exclude pattern and those " +
        "log files will not be aggregated in a rolling fashion.")
      .version("2.0.0")
      .stringConf
      .createOptional

  /* Private configs. */

  // Internal config to propagate the location of the user's jar to the driver/executors
  private[spark] val APP_JAR = ConfigBuilder("spark.yarn.user.jar")
    .internal()
    .version("1.1.0")
    .stringConf
    .createOptional

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  private[spark] val SECONDARY_JARS = ConfigBuilder("spark.yarn.secondary.jars")
    .internal()
    .version("0.9.2")
    .stringConf
    .toSequence
    .createOptional

  /* Configuration and cached file propagation. */

  private[spark] val CACHED_FILES = ConfigBuilder("spark.yarn.cache.filenames")
    .internal()
    .version("2.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_SIZES = ConfigBuilder("spark.yarn.cache.sizes")
    .internal()
    .version("2.0.0")
    .longConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_TIMESTAMPS = ConfigBuilder("spark.yarn.cache.timestamps")
    .internal()
    .version("2.0.0")
    .longConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_VISIBILITIES = ConfigBuilder("spark.yarn.cache.visibilities")
    .internal()
    .version("2.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  // Either "file" or "archive", for each file.
  private[spark] val CACHED_FILES_TYPES = ConfigBuilder("spark.yarn.cache.types")
    .internal()
    .version("2.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  // The location of the conf archive in HDFS.
  private[spark] val CACHED_CONF_ARCHIVE = ConfigBuilder("spark.yarn.cache.confArchive")
    .internal()
    .version("2.0.0")
    .stringConf
    .createOptional

  /* YARN allocator-level excludeOnFailure related config entries. */
  private[spark] val YARN_EXECUTOR_LAUNCH_EXCLUDE_ON_FAILURE_ENABLED =
    ConfigBuilder("spark.yarn.executor.launch.excludeOnFailure.enabled")
      .version("3.1.0")
      .withAlternative("spark.yarn.blacklist.executor.launch.blacklisting.enabled")
      .booleanConf
      .createWithDefault(false)

  /* Initially excluded YARN nodes. */
  private[spark] val YARN_EXCLUDE_NODES = ConfigBuilder("spark.yarn.exclude.nodes")
    .version("3.0.0")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[yarn] val YARN_EXECUTOR_RESOURCE_TYPES_PREFIX = "spark.yarn.executor.resource."
  private[yarn] val YARN_DRIVER_RESOURCE_TYPES_PREFIX = "spark.yarn.driver.resource."
  private[yarn] val YARN_AM_RESOURCE_TYPES_PREFIX = "spark.yarn.am.resource."

  def isHadoopProvided(): Boolean = IS_HADOOP_PROVIDED

  private lazy val IS_HADOOP_PROVIDED: Boolean = {
    val configPath = "org/apache/spark/deploy/yarn/config.properties"
    val propertyKey = "spark.yarn.isHadoopProvided"
    try {
      val prop = new Properties()
      prop.load(ClassLoader.getSystemClassLoader.getResourceAsStream(configPath))
      prop.getProperty(propertyKey).toBoolean
    } catch {
      case e: Exception =>
        log.warn(s"Can not load the default value of `$propertyKey` from " +
          s"`$configPath` with error, ${e.toString}. Using `false` as a default value.")
        false
    }
  }
}
