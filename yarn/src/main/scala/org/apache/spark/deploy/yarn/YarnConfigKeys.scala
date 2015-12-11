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

import org.apache.spark.config.ConfigEntry._
import org.apache.spark.network.util.ByteUnit

private[spark] object YarnConfigKeys {

  /* Common app configuration. */

  val MAX_APP_ATTEMPTS = intConf("spark.yarn.maxAppAttempts",
    doc = "Maximum number of AM attempts before failing the app.")
    .optional

  val ATTEMPT_FAILURE_VALIDITY_INTERVAL_MS =
    timeConf("spark.yarn.am.attemptFailuresValidityInterval",
      unit = TimeUnit.MILLISECONDS,
      doc = "Interval after which AM failures will be considered independent and not accumulate " +
        "towards the attempt count.")
      .optional

  val AM_NODE_LABEL_EXPRESSION = stringConf("spark.yarn.am.nodeLabelExpression",
    doc = "Node label expression for the AM.")
    .optional

  val SPARK_JAR = stringConf("spark.yarn.jar",
    doc = "Location of the Spark jar to use.")
    .optional

  val USER_CLASS_PATH_FIRST = booleanConf("spark.yarn.user.classpath.first",
    defaultValue = Some(false),
    doc = "Whether to place user jars in front of Spark's classpath.")

  val GATEWAY_ROOT_PATH = stringConf("spark.yarn.config.gatewayPath",
    defaultValue = Some(null),
    doc = "Root of configuration paths that is present on gateway nodes, and will be replaced " +
      "with the corresponding path in cluster machines.")

  val REPLACEMENT_ROOT_PATH = stringConf("spark.yarn.config.replacementPath",
    defaultValue = Some(null),
    doc = s"Path to use as a replacement for ${GATEWAY_ROOT_PATH.key} when launching processes " +
      "in the YARN cluster.")

  val APPLICATION_TAGS = stringSeqConf("spark.yarn.tags",
    doc = "Comma-separated list of strings to pass through as YARN application tags appearing " +
      "in YARN Application Reports, which can be used for filtering when querying YARN.")
    .optional

  val QUEUE_NAME = stringConf("spark.yarn.queue",
    defaultValue = Some("default"))

  val FILES_TO_DISTRIBUTE = stringConf("spark.yarn.dist.files").optional

  val ARCHIVES_TO_DISTRIBUTE = stringConf("spark.yarn.dist.archives").optional

  val MAX_EXECUTOR_FAILURES = intConf("spark.yarn.max.executor.failures").optional

  val RM_HEARTBEAT_INTERVAL = timeConf("spark.yarn.scheduler.heartbeat.interval-ms",
    unit = TimeUnit.MILLISECONDS,
    defaultValue = Some("3s"))

  val INITIAL_HEARTBEAT_INTERVAL = timeConf("spark.yarn.scheduler.initial-allocation.interval",
    unit = TimeUnit.MILLISECONDS,
    defaultValue = Some("200ms"))

  val HISTORY_SERVER_ADDRESS = stringConf("spark.yarn.historyServer.address").optional

  val MAX_REPORTER_THREAD_FAILURES = intConf("spark.yarn.scheduler.reporterThread.maxFailures",
    defaultValue = Some(5))

  val AM_MAX_WAIT_TIME = timeConf("spark.yarn.am.waitTime",
    unit = TimeUnit.MILLISECONDS,
    defaultValue = Some("100s"))

  val SCHEDULER_SERVICES = stringSeqConf("spark.yarn.services",
    defaultValue = Some(Nil),
    doc = "A comma-separated list of class names of services to add to the scheduler.")

  val CONTAINER_LAUNCH_MAX_THREADS = intConf("spark.yarn.containerLauncherMaxThreads",
    defaultValue = Some(25))

  /* Client configuration. */

  val WAIT_FOR_APP_COMPLETION = booleanConf("spark.yarn.submit.waitAppCompletion",
    defaultValue = Some(true),
    doc = "In cluster mode, whether to wait for the application to finishe before exiting the " +
      "launcher process.")

  val PRESERVE_STAGING_FILES = booleanConf("spark.yarn.preserve.staging.files",
    defaultValue = Some(false),
    doc = "Whether to preserve temporary files created by the job in HDFS.")

  val STAGING_FILE_REPLICATION = intConf("spark.yarn.submit.file.replication",
    doc = "Replication factor for files uploaded by Spark to HDFS.")
    .optional

  val REPORT_INTERVAL = timeConf("spark.yarn.report.interval",
    unit = TimeUnit.MILLISECONDS,
    defaultValue = Some("1s"),
    doc = "Interval between reports of the current app status in cluster mode.")

  /* Client-mode AM configuration. */

  val AM_JAVA_OPTIONS = stringConf("spark.yarn.am.extraJavaOptions",
    doc = "Extra Java options for the client-mode AM.")
    .optional

  val AM_LIBRARY_PATH = stringConf("spark.yarn.am.extraLibraryPath",
    doc = "Extra native library path for the client-mode AM.")
    .optional

  val AM_MEMORY_OVERHEAD = bytesConf("spark.yarn.am.memoryOverhead",
    unit = ByteUnit.MiB)
    .optional

  val AM_MEMORY = bytesConf("spark.yarn.am.memory",
    unit = ByteUnit.MiB,
    defaultValue = Some("512m"))

  val AM_CORES = intConf("spark.yarn.am.cores",
    defaultValue = Some(1))

  /* Driver configuration. */

  val DRIVER_MEMORY_OVERHEAD = bytesConf("spark.yarn.driver.memoryOverhead",
    unit = ByteUnit.MiB)
    .optional

  val EXECUTOR_MEMORY_OVERHEAD = bytesConf("spark.yarn.executor.memoryOverhead",
    unit = ByteUnit.MiB)
    .optional

  val DRIVER_CORES = intConf("spark.driver.cores")

  /* Executor configuration. */

  val EXECUTOR_NODE_LABEL_EXPRESSION = stringConf("spark.yarn.executor.nodeLabelExpression",
    doc = "Node label expression for executors.")
    .optional

  /* Security configuration. */

  val TOKEN_RENEWAL_INTERVAL = timeConf("spark.yarn.token.renewal.interval",
    unit = TimeUnit.MILLISECONDS,
    isPublic = false)

  val NAMENODES_TO_ACCESS = stringSeqConf("spark.yarn.access.namenodes",
    defaultValue = Some(Nil),
    doc = "Extra NameNode URLs for which to request delegation tokens. The NameNode that hosts " +
      "fs.defaultFS does not need to be listed here.")

  val CREDENTIALS_FILE_MAX_RETENTION = intConf("spark.yarn.credentials.file.retention.days",
    defaultValue = Some(5))

  val CREDENTIAL_FILE_MAX_COUNT = intConf("spark.yarn.credentials.file.retention.count",
    defaultValue = Some(5))

  /* Private configs. */

  val CREDENTIALS_FILE_PATH = stringConf("spark.yarn.credentials.file",
    isPublic = false)

  // Internal config to propagate the location of the user's jar to the driver/executors
  val APP_JAR = stringConf("spark.yarn.user.jar",
    isPublic = false)
    .optional

  // Internal config to propagate the locations of any extra jars to add to the classpath
  // of the executors
  val SECONDARY_JARS = stringSeqConf("spark.yarn.secondary.jars",
    isPublic = false)
    .optional

}
