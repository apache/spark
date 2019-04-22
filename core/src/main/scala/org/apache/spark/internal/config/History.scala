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

package org.apache.spark.internal.config

import java.util.concurrent.TimeUnit

import org.apache.spark.network.util.ByteUnit

private[spark] object History {

  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"

  val HISTORY_LOG_DIR = ConfigBuilder("spark.history.fs.logDirectory")
    .stringConf
    .createWithDefault(DEFAULT_LOG_DIR)

  val SAFEMODE_CHECK_INTERVAL_S = ConfigBuilder("spark.history.fs.safemodeCheck.interval")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("5s")

  val UPDATE_INTERVAL_S = ConfigBuilder("spark.history.fs.update.interval")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("10s")

  val CLEANER_ENABLED = ConfigBuilder("spark.history.fs.cleaner.enabled")
    .booleanConf
    .createWithDefault(false)

  val CLEANER_INTERVAL_S = ConfigBuilder("spark.history.fs.cleaner.interval")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("1d")

  val MAX_LOG_AGE_S = ConfigBuilder("spark.history.fs.cleaner.maxAge")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("7d")

  val LOCAL_STORE_DIR = ConfigBuilder("spark.history.store.path")
    .doc("Local directory where to cache application history information. By default this is " +
      "not set, meaning all history information will be kept in memory.")
    .stringConf
    .createOptional

  val MAX_LOCAL_DISK_USAGE = ConfigBuilder("spark.history.store.maxDiskUsage")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("10g")

  val HISTORY_SERVER_UI_PORT = ConfigBuilder("spark.history.ui.port")
    .doc("Web UI port to bind Spark History Server")
    .intConf
    .createWithDefault(18080)

  val FAST_IN_PROGRESS_PARSING =
    ConfigBuilder("spark.history.fs.inProgressOptimization.enabled")
      .doc("Enable optimized handling of in-progress logs. This option may leave finished " +
        "applications that fail to rename their event logs listed as in-progress.")
      .booleanConf
      .createWithDefault(true)

  val END_EVENT_REPARSE_CHUNK_SIZE =
    ConfigBuilder("spark.history.fs.endEventReparseChunkSize")
      .doc("How many bytes to parse at the end of log files looking for the end event. " +
        "This is used to speed up generation of application listings by skipping unnecessary " +
        "parts of event log files. It can be disabled by setting this config to 0.")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1m")

  val DRIVER_LOG_CLEANER_ENABLED = ConfigBuilder("spark.history.fs.driverlog.cleaner.enabled")
    .fallbackConf(CLEANER_ENABLED)

  val DRIVER_LOG_CLEANER_INTERVAL = ConfigBuilder("spark.history.fs.driverlog.cleaner.interval")
    .fallbackConf(CLEANER_INTERVAL_S)

  val MAX_DRIVER_LOG_AGE_S = ConfigBuilder("spark.history.fs.driverlog.cleaner.maxAge")
    .fallbackConf(MAX_LOG_AGE_S)

  val HISTORY_SERVER_UI_ACLS_ENABLE = ConfigBuilder("spark.history.ui.acls.enable")
    .booleanConf
    .createWithDefault(false)

  val HISTORY_SERVER_UI_ADMIN_ACLS = ConfigBuilder("spark.history.ui.admin.acls")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val HISTORY_SERVER_UI_ADMIN_ACLS_GROUPS = ConfigBuilder("spark.history.ui.admin.acls.groups")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val NUM_REPLAY_THREADS = ConfigBuilder("spark.history.fs.numReplayThreads")
    .intConf
    .createWithDefaultFunction(() => Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)

  val RETAINED_APPLICATIONS = ConfigBuilder("spark.history.retainedApplications")
    .intConf
    .createWithDefault(50)

  val PROVIDER = ConfigBuilder("spark.history.provider")
    .stringConf
    .createOptional

  val KERBEROS_ENABLED = ConfigBuilder("spark.history.kerberos.enabled")
    .booleanConf
    .createWithDefault(false)

  val KERBEROS_PRINCIPAL = ConfigBuilder("spark.history.kerberos.principal")
    .stringConf
    .createOptional

  val KERBEROS_KEYTAB = ConfigBuilder("spark.history.kerberos.keytab")
    .stringConf
    .createOptional

  val CUSTOM_EXECUTOR_LOG_URL = ConfigBuilder("spark.history.custom.executor.log.url")
    .doc("Specifies custom spark executor log url for supporting external log service instead of " +
      "using cluster managers' application log urls in the history server. Spark will support " +
      "some path variables via patterns which can vary on cluster manager. Please check the " +
      "documentation for your cluster manager to see which patterns are supported, if any. " +
      "This configuration has no effect on a live application, it only affects the history server.")
    .stringConf
    .createOptional

  val APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP =
    ConfigBuilder("spark.history.custom.executor.log.url.applyIncompleteApplication")
      .doc("Whether to apply custom executor log url, as specified by " +
        "`spark.history.custom.executor.log.url`, to incomplete application as well. " +
        "Even if this is true, this still only affects the behavior of the history server, " +
        "not running spark applications.")
      .booleanConf
      .createWithDefault(true)
}
