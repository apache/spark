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

import java.util.concurrent.TimeUnit

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit

package object config {

  /* Common app configuration. */

  private[spark] val APPLICATION_TAGS = ConfigBuilder("spark.yarn.tags")
    .doc("Comma-separated list of strings to pass through as YARN application tags appearing " +
      "in YARN Application Reports, which can be used for filtering when querying YARN.")
    .stringConf
    .toSequence
    .createOptional

  private[spark] val AM_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS =
    ConfigBuilder("spark.yarn.am.attemptFailuresValidityInterval")
      .doc("Interval after which AM failures will be considered independent and " +
        "not accumulate towards the attempt count.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val AM_PORT =
    ConfigBuilder("spark.yarn.am.port")
      .intConf
      .createWithDefault(0)

  private[spark] val EXECUTOR_ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS =
    ConfigBuilder("spark.yarn.executor.failuresValidityInterval")
      .doc("Interval after which Executor failures will be considered independent and not " +
        "accumulate towards the attempt count.")
      .timeConf(TimeUnit.MILLISECONDS)
      .createOptional

  private[spark] val MAX_APP_ATTEMPTS = ConfigBuilder("spark.yarn.maxAppAttempts")
    .doc("Maximum number of AM attempts before failing the app.")
    .intConf
    .createOptional

  private[spark] val USER_CLASS_PATH_FIRST = ConfigBuilder("spark.yarn.user.classpath.first")
    .doc("Whether to place user jars in front of Spark's classpath.")
    .booleanConf
    .createWithDefault(false)

  private[spark] val GATEWAY_ROOT_PATH = ConfigBuilder("spark.yarn.config.gatewayPath")
    .doc("Root of configuration paths that is present on gateway nodes, and will be replaced " +
      "with the corresponding path in cluster machines.")
    .stringConf
    .createWithDefault(null)

  private[spark] val REPLACEMENT_ROOT_PATH = ConfigBuilder("spark.yarn.config.replacementPath")
    .doc(s"Path to use as a replacement for ${GATEWAY_ROOT_PATH.key} when launching processes " +
      "in the YARN cluster.")
    .stringConf
    .createWithDefault(null)

  private[spark] val QUEUE_NAME = ConfigBuilder("spark.yarn.queue")
    .stringConf
    .createWithDefault("default")

  private[spark] val HISTORY_SERVER_ADDRESS = ConfigBuilder("spark.yarn.historyServer.address")
    .stringConf
    .createOptional

  private[spark] val ALLOW_HISTORY_SERVER_TRACKING_URL =
    ConfigBuilder("spark.yarn.historyServer.allowTracking")
      .doc("Allow using the History Server URL for the application as the tracking URL for the " +
        "application when the Web UI is not enabled.")
      .booleanConf
      .createWithDefault(false)

  /* File distribution. */

  private[spark] val SPARK_ARCHIVE = ConfigBuilder("spark.yarn.archive")
    .doc("Location of archive containing jars files with Spark classes.")
    .stringConf
    .createOptional

  private[spark] val SPARK_JARS = ConfigBuilder("spark.yarn.jars")
    .doc("Location of jars containing Spark classes.")
    .stringConf
    .toSequence
    .createOptional

  private[spark] val ARCHIVES_TO_DISTRIBUTE = ConfigBuilder("spark.yarn.dist.archives")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val FILES_TO_DISTRIBUTE = ConfigBuilder("spark.yarn.dist.files")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val JARS_TO_DISTRIBUTE = ConfigBuilder("spark.yarn.dist.jars")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val PRESERVE_STAGING_FILES = ConfigBuilder("spark.yarn.preserve.staging.files")
    .doc("Whether to preserve temporary files created by the job in HDFS.")
    .booleanConf
    .createWithDefault(false)

  private[spark] val STAGING_FILE_REPLICATION = ConfigBuilder("spark.yarn.submit.file.replication")
    .doc("Replication factor for files uploaded by Spark to HDFS.")
    .intConf
    .createOptional

  private[spark] val STAGING_DIR = ConfigBuilder("spark.yarn.stagingDir")
    .doc("Staging directory used while submitting applications.")
    .stringConf
    .createOptional

  /* Cluster-mode launcher configuration. */

  private[spark] val WAIT_FOR_APP_COMPLETION = ConfigBuilder("spark.yarn.submit.waitAppCompletion")
    .doc("In cluster mode, whether to wait for the application to finish before exiting the " +
      "launcher process.")
    .booleanConf
    .createWithDefault(true)

  private[spark] val REPORT_INTERVAL = ConfigBuilder("spark.yarn.report.interval")
    .doc("Interval between reports of the current app status in cluster mode.")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("1s")

  /* Shared Client-mode AM / Driver configuration. */

  private[spark] val AM_MAX_WAIT_TIME = ConfigBuilder("spark.yarn.am.waitTime")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("100s")

  private[spark] val AM_NODE_LABEL_EXPRESSION = ConfigBuilder("spark.yarn.am.nodeLabelExpression")
    .doc("Node label expression for the AM.")
    .stringConf
    .createOptional

  private[spark] val CONTAINER_LAUNCH_MAX_THREADS =
    ConfigBuilder("spark.yarn.containerLauncherMaxThreads")
      .intConf
      .createWithDefault(25)

  private[spark] val MAX_EXECUTOR_FAILURES = ConfigBuilder("spark.yarn.max.executor.failures")
    .intConf
    .createOptional

  private[spark] val MAX_REPORTER_THREAD_FAILURES =
    ConfigBuilder("spark.yarn.scheduler.reporterThread.maxFailures")
      .intConf
      .createWithDefault(5)

  private[spark] val RM_HEARTBEAT_INTERVAL =
    ConfigBuilder("spark.yarn.scheduler.heartbeat.interval-ms")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("3s")

  private[spark] val INITIAL_HEARTBEAT_INTERVAL =
    ConfigBuilder("spark.yarn.scheduler.initial-allocation.interval")
      .timeConf(TimeUnit.MILLISECONDS)
      .createWithDefaultString("200ms")

  private[spark] val SCHEDULER_SERVICES = ConfigBuilder("spark.yarn.services")
    .doc("A comma-separated list of class names of services to add to the scheduler.")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  /* Client-mode AM configuration. */

  private[spark] val AM_CORES = ConfigBuilder("spark.yarn.am.cores")
    .intConf
    .createWithDefault(1)

  private[spark] val AM_JAVA_OPTIONS = ConfigBuilder("spark.yarn.am.extraJavaOptions")
    .doc("Extra Java options for the client-mode AM.")
    .stringConf
    .createOptional

  private[spark] val AM_LIBRARY_PATH = ConfigBuilder("spark.yarn.am.extraLibraryPath")
    .doc("Extra native library path for the client-mode AM.")
    .stringConf
    .createOptional

  private[spark] val AM_MEMORY_OVERHEAD = ConfigBuilder("spark.yarn.am.memoryOverhead")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  private[spark] val AM_MEMORY = ConfigBuilder("spark.yarn.am.memory")
    .bytesConf(ByteUnit.MiB)
    .createWithDefaultString("512m")

  /* Driver configuration. */

  private[spark] val DRIVER_CORES = ConfigBuilder("spark.driver.cores")
    .intConf
    .createWithDefault(1)

  private[spark] val DRIVER_MEMORY_OVERHEAD = ConfigBuilder("spark.yarn.driver.memoryOverhead")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  /* Executor configuration. */

  private[spark] val EXECUTOR_CORES = ConfigBuilder("spark.executor.cores")
    .intConf
    .createWithDefault(1)

  private[spark] val EXECUTOR_MEMORY_OVERHEAD = ConfigBuilder("spark.yarn.executor.memoryOverhead")
    .bytesConf(ByteUnit.MiB)
    .createOptional

  private[spark] val EXECUTOR_NODE_LABEL_EXPRESSION =
    ConfigBuilder("spark.yarn.executor.nodeLabelExpression")
      .doc("Node label expression for executors.")
      .stringConf
      .createOptional

  /* Security configuration. */

  private[spark] val CREDENTIAL_FILE_MAX_COUNT =
    ConfigBuilder("spark.yarn.credentials.file.retention.count")
      .intConf
      .createWithDefault(5)

  private[spark] val CREDENTIALS_FILE_MAX_RETENTION =
    ConfigBuilder("spark.yarn.credentials.file.retention.days")
      .intConf
      .createWithDefault(5)

  private[spark] val NAMENODES_TO_ACCESS = ConfigBuilder("spark.yarn.access.namenodes")
    .doc("Extra NameNode URLs for which to request delegation tokens. The NameNode that hosts " +
      "fs.defaultFS does not need to be listed here.")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val FILESYSTEMS_TO_ACCESS = ConfigBuilder("spark.yarn.access.hadoopFileSystems")
    .doc("Extra Hadoop filesystem URLs for which to request delegation tokens. The filesystem " +
      "that hosts fs.defaultFS does not need to be listed here.")
    .fallbackConf(NAMENODES_TO_ACCESS)

  /* Rolled log aggregation configuration. */

  private[spark] val ROLLED_LOG_INCLUDE_PATTERN =
    ConfigBuilder("spark.yarn.rolledLog.includePattern")
      .doc("Java Regex to filter the log files which match the defined include pattern and those " +
        "log files will be aggregated in a rolling fashion.")
      .stringConf
      .createOptional

  private[spark] val ROLLED_LOG_EXCLUDE_PATTERN =
    ConfigBuilder("spark.yarn.rolledLog.excludePattern")
      .doc("Java Regex to filter the log files which match the defined exclude pattern and those " +
        "log files will not be aggregated in a rolling fashion.")
      .stringConf
      .createOptional

  /* Private configs. */

  private[spark] val CREDENTIALS_FILE_PATH = ConfigBuilder("spark.yarn.credentials.file")
    .internal()
    .stringConf
    .createWithDefault(null)

  // Internal config to propagate the location of the user's jar to the driver/executors
  private[spark] val APP_JAR = ConfigBuilder("spark.yarn.user.jar")
    .internal()
    .stringConf
    .createOptional

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  private[spark] val SECONDARY_JARS = ConfigBuilder("spark.yarn.secondary.jars")
    .internal()
    .stringConf
    .toSequence
    .createOptional

  /* Configuration and cached file propagation. */

  private[spark] val CACHED_FILES = ConfigBuilder("spark.yarn.cache.filenames")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_SIZES = ConfigBuilder("spark.yarn.cache.sizes")
    .internal()
    .longConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_TIMESTAMPS = ConfigBuilder("spark.yarn.cache.timestamps")
    .internal()
    .longConf
    .toSequence
    .createWithDefault(Nil)

  private[spark] val CACHED_FILES_VISIBILITIES = ConfigBuilder("spark.yarn.cache.visibilities")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  // Either "file" or "archive", for each file.
  private[spark] val CACHED_FILES_TYPES = ConfigBuilder("spark.yarn.cache.types")
    .internal()
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  // The location of the conf archive in HDFS.
  private[spark] val CACHED_CONF_ARCHIVE = ConfigBuilder("spark.yarn.cache.confArchive")
    .internal()
    .stringConf
    .createOptional

  private[spark] val CREDENTIALS_RENEWAL_TIME = ConfigBuilder("spark.yarn.credentials.renewalTime")
    .internal()
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefault(Long.MaxValue)

  private[spark] val CREDENTIALS_UPDATE_TIME = ConfigBuilder("spark.yarn.credentials.updateTime")
    .internal()
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefault(Long.MaxValue)

  // The list of cache-related config entries. This is used by Client and the AM to clean
  // up the environment so that these settings do not appear on the web UI.
  private[yarn] val CACHE_CONFIGS = Seq(
    CACHED_FILES,
    CACHED_FILES_SIZES,
    CACHED_FILES_TIMESTAMPS,
    CACHED_FILES_VISIBILITIES,
    CACHED_FILES_TYPES,
    CACHED_CONF_ARCHIVE)

}
