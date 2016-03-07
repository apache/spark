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
    .optional

  private[spark] val ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS =
    ConfigBuilder("spark.yarn.am.attemptFailuresValidityInterval")
      .doc("Interval after which AM failures will be considered independent and " +
        "not accumulate towards the attempt count.")
      .timeConf(TimeUnit.MILLISECONDS)
      .optional

  private[spark] val MAX_APP_ATTEMPTS = ConfigBuilder("spark.yarn.maxAppAttempts")
    .doc("Maximum number of AM attempts before failing the app.")
    .intConf
    .optional

  private[spark] val USER_CLASS_PATH_FIRST = ConfigBuilder("spark.yarn.user.classpath.first")
    .doc("Whether to place user jars in front of Spark's classpath.")
    .booleanConf
    .withDefault(false)

  private[spark] val GATEWAY_ROOT_PATH = ConfigBuilder("spark.yarn.config.gatewayPath")
    .doc("Root of configuration paths that is present on gateway nodes, and will be replaced " +
      "with the corresponding path in cluster machines.")
    .stringConf
    .withDefault(null)

  private[spark] val REPLACEMENT_ROOT_PATH = ConfigBuilder("spark.yarn.config.replacementPath")
    .doc(s"Path to use as a replacement for ${GATEWAY_ROOT_PATH.key} when launching processes " +
      "in the YARN cluster.")
    .stringConf
    .withDefault(null)

  private[spark] val QUEUE_NAME = ConfigBuilder("spark.yarn.queue")
    .stringConf
    .withDefault("default")

  private[spark] val HISTORY_SERVER_ADDRESS = ConfigBuilder("spark.yarn.historyServer.address")
    .stringConf
    .optional

  /* File distribution. */

  private[spark] val SPARK_JAR = ConfigBuilder("spark.yarn.jar")
    .doc("Location of the Spark jar to use.")
    .stringConf
    .optional

  private[spark] val ARCHIVES_TO_DISTRIBUTE = ConfigBuilder("spark.yarn.dist.archives")
    .stringConf
    .optional

  private[spark] val FILES_TO_DISTRIBUTE = ConfigBuilder("spark.yarn.dist.files")
    .stringConf
    .optional

  private[spark] val PRESERVE_STAGING_FILES = ConfigBuilder("spark.yarn.preserve.staging.files")
    .doc("Whether to preserve temporary files created by the job in HDFS.")
    .booleanConf
    .withDefault(false)

  private[spark] val STAGING_FILE_REPLICATION = ConfigBuilder("spark.yarn.submit.file.replication")
    .doc("Replication factor for files uploaded by Spark to HDFS.")
    .intConf
    .optional

  /* Cluster-mode launcher configuration. */

  private[spark] val WAIT_FOR_APP_COMPLETION = ConfigBuilder("spark.yarn.submit.waitAppCompletion")
    .doc("In cluster mode, whether to wait for the application to finishe before exiting the " +
      "launcher process.")
    .booleanConf
    .withDefault(true)

  private[spark] val REPORT_INTERVAL = ConfigBuilder("spark.yarn.report.interval")
    .doc("Interval between reports of the current app status in cluster mode.")
    .timeConf(TimeUnit.MILLISECONDS)
    .withDefaultString("1s")

  /* Shared Client-mode AM / Driver configuration. */

  private[spark] val AM_MAX_WAIT_TIME = ConfigBuilder("spark.yarn.am.waitTime")
    .timeConf(TimeUnit.MILLISECONDS)
    .withDefaultString("100s")

  private[spark] val AM_NODE_LABEL_EXPRESSION = ConfigBuilder("spark.yarn.am.nodeLabelExpression")
    .doc("Node label expression for the AM.")
    .stringConf
    .optional

  private[spark] val CONTAINER_LAUNCH_MAX_THREADS =
    ConfigBuilder("spark.yarn.containerLauncherMaxThreads")
      .intConf
      .withDefault(25)

  private[spark] val MAX_EXECUTOR_FAILURES = ConfigBuilder("spark.yarn.max.executor.failures")
    .intConf
    .optional

  private[spark] val MAX_REPORTER_THREAD_FAILURES =
    ConfigBuilder("spark.yarn.scheduler.reporterThread.maxFailures")
      .intConf
      .withDefault(5)

  private[spark] val RM_HEARTBEAT_INTERVAL =
    ConfigBuilder("spark.yarn.scheduler.heartbeat.interval-ms")
      .timeConf(TimeUnit.MILLISECONDS)
      .withDefaultString("3s")

  private[spark] val INITIAL_HEARTBEAT_INTERVAL =
    ConfigBuilder("spark.yarn.scheduler.initial-allocation.interval")
      .timeConf(TimeUnit.MILLISECONDS)
      .withDefaultString("200ms")

  private[spark] val SCHEDULER_SERVICES = ConfigBuilder("spark.yarn.services")
    .doc("A comma-separated list of class names of services to add to the scheduler.")
    .stringConf
    .toSequence
    .withDefault(Nil)

  /* Client-mode AM configuration. */

  private[spark] val AM_CORES = ConfigBuilder("spark.yarn.am.cores")
    .intConf
    .withDefault(1)

  private[spark] val AM_JAVA_OPTIONS = ConfigBuilder("spark.yarn.am.extraJavaOptions")
    .doc("Extra Java options for the client-mode AM.")
    .stringConf
    .optional

  private[spark] val AM_LIBRARY_PATH = ConfigBuilder("spark.yarn.am.extraLibraryPath")
    .doc("Extra native library path for the client-mode AM.")
    .stringConf
    .optional

  private[spark] val AM_MEMORY_OVERHEAD = ConfigBuilder("spark.yarn.am.memoryOverhead")
    .bytesConf(ByteUnit.MiB)
    .optional

  private[spark] val AM_MEMORY = ConfigBuilder("spark.yarn.am.memory")
    .bytesConf(ByteUnit.MiB)
    .withDefaultString("512m")

  /* Driver configuration. */

  private[spark] val DRIVER_CORES = ConfigBuilder("spark.driver.cores")
    .intConf
    .optional

  private[spark] val DRIVER_MEMORY_OVERHEAD = ConfigBuilder("spark.yarn.driver.memoryOverhead")
    .bytesConf(ByteUnit.MiB)
    .optional

  /* Executor configuration. */

  private[spark] val EXECUTOR_MEMORY_OVERHEAD = ConfigBuilder("spark.yarn.executor.memoryOverhead")
    .bytesConf(ByteUnit.MiB)
    .optional

  private[spark] val EXECUTOR_NODE_LABEL_EXPRESSION =
    ConfigBuilder("spark.yarn.executor.nodeLabelExpression")
      .doc("Node label expression for executors.")
      .stringConf
      .optional

  /* Security configuration. */

  private[spark] val CREDENTIAL_FILE_MAX_COUNT =
    ConfigBuilder("spark.yarn.credentials.file.retention.count")
      .intConf
      .withDefault(5)

  private[spark] val CREDENTIALS_FILE_MAX_RETENTION =
    ConfigBuilder("spark.yarn.credentials.file.retention.days")
      .intConf
      .withDefault(5)

  private[spark] val NAMENODES_TO_ACCESS = ConfigBuilder("spark.yarn.access.namenodes")
    .doc("Extra NameNode URLs for which to request delegation tokens. The NameNode that hosts " +
      "fs.defaultFS does not need to be listed here.")
    .stringConf
    .toSequence
    .withDefault(Nil)

  private[spark] val TOKEN_RENEWAL_INTERVAL = ConfigBuilder("spark.yarn.token.renewal.interval")
    .internal
    .timeConf(TimeUnit.MILLISECONDS)
    .optional

  /* Private configs. */

  private[spark] val CREDENTIALS_FILE_PATH = ConfigBuilder("spark.yarn.credentials.file")
    .internal
    .stringConf
    .withDefault(null)

  // Internal config to propagate the location of the user's jar to the driver/executors
  private[spark] val APP_JAR = ConfigBuilder("spark.yarn.user.jar")
    .internal
    .stringConf
    .optional

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  private[spark] val SECONDARY_JARS = ConfigBuilder("spark.yarn.secondary.jars")
    .internal
    .stringConf
    .toSequence
    .optional

}
