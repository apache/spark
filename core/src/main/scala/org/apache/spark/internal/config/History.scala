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

import java.util.Locale
import java.util.concurrent.TimeUnit

import org.apache.spark.network.util.ByteUnit

private[spark] object History {

  val DEFAULT_LOG_DIR = "file:/tmp/spark-events"

  val HISTORY_LOG_DIR = ConfigBuilder("spark.history.fs.logDirectory")
    .version("1.1.0")
    .doc("Directory where app logs are stored")
    .stringConf
    .createWithDefault(DEFAULT_LOG_DIR)

  val SAFEMODE_CHECK_INTERVAL_S = ConfigBuilder("spark.history.fs.safemodeCheck.interval")
    .version("1.6.0")
    .doc("Interval between HDFS safemode checks for the event log directory")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("5s")

  val UPDATE_INTERVAL_S = ConfigBuilder("spark.history.fs.update.interval")
    .version("1.4.0")
    .doc("How often(in seconds) to reload log data from storage")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("10s")

  val UPDATE_BATCHSIZE = ConfigBuilder("spark.history.fs.update.batchSize")
    .doc("Specifies the batch size for updating new eventlog files. " +
      "This controls each scan process to be completed within a reasonable time, and such " +
      "prevent the initial scan from running too long and blocking new eventlog files to " +
      "be scanned in time in large environments.")
    .version("3.4.0")
    .intConf
    .checkValue(v => v > 0, "The update batchSize should be a positive integer.")
    .createWithDefault(Int.MaxValue)

  val CLEANER_ENABLED = ConfigBuilder("spark.history.fs.cleaner.enabled")
    .version("1.4.0")
    .doc("Whether the History Server should periodically clean up event logs from storage")
    .booleanConf
    .createWithDefault(false)

  val CLEANER_INTERVAL_S = ConfigBuilder("spark.history.fs.cleaner.interval")
    .version("1.4.0")
    .doc("When spark.history.fs.cleaner.enabled=true, specifies how often the filesystem " +
      "job history cleaner checks for files to delete.")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("1d")

  val MAX_LOG_AGE_S = ConfigBuilder("spark.history.fs.cleaner.maxAge")
    .version("1.4.0")
    .doc("When spark.history.fs.cleaner.enabled=true, history files older than this will be " +
      "deleted when the filesystem history cleaner runs.")
    .timeConf(TimeUnit.SECONDS)
    .createWithDefaultString("7d")

  val MAX_LOG_NUM = ConfigBuilder("spark.history.fs.cleaner.maxNum")
    .doc("The maximum number of log files in the event log directory.")
    .version("3.0.0")
    .intConf
    .createWithDefault(Int.MaxValue)

  val LOCAL_STORE_DIR = ConfigBuilder("spark.history.store.path")
    .doc("Local directory where to cache application history information. By default this is " +
      "not set, meaning all history information will be kept in memory.")
    .version("2.3.0")
    .stringConf
    .createOptional

  object LocalStoreSerializer extends Enumeration {
    val JSON, PROTOBUF = Value
  }

  val LOCAL_STORE_SERIALIZER = ConfigBuilder("spark.history.store.serializer")
    .doc("Serializer for writing/reading in-memory UI objects to/from disk-based KV Store; " +
      "JSON or PROTOBUF. JSON serializer is the only choice before Spark 3.4.0, thus it is the " +
      "default value. PROTOBUF serializer is fast and compact, and it is the default " +
      "serializer for disk-based KV store of live UI.")
    .version("3.4.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(LocalStoreSerializer.values.map(_.toString))
    .createWithDefault(LocalStoreSerializer.JSON.toString)

  val MAX_LOCAL_DISK_USAGE = ConfigBuilder("spark.history.store.maxDiskUsage")
    .version("2.3.0")
    .doc("Maximum disk usage for the local directory where the cache application history " +
      "information are stored.")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("10g")

  val HISTORY_SERVER_UI_TITLE = ConfigBuilder("spark.history.ui.title")
    .version("4.0.0")
    .doc("Specifies the title of the History Server UI page.")
    .stringConf
    .createWithDefault("History Server")

  val HISTORY_SERVER_UI_PORT = ConfigBuilder("spark.history.ui.port")
    .doc("Web UI port to bind Spark History Server")
    .version("1.0.0")
    .intConf
    .createWithDefault(18080)

  val FAST_IN_PROGRESS_PARSING =
    ConfigBuilder("spark.history.fs.inProgressOptimization.enabled")
      .doc("Enable optimized handling of in-progress logs. This option may leave finished " +
        "applications that fail to rename their event logs listed as in-progress.")
      .version("2.4.0")
      .booleanConf
      .createWithDefault(true)

  val END_EVENT_REPARSE_CHUNK_SIZE =
    ConfigBuilder("spark.history.fs.endEventReparseChunkSize")
      .doc("How many bytes to parse at the end of log files looking for the end event. " +
        "This is used to speed up generation of application listings by skipping unnecessary " +
        "parts of event log files. It can be disabled by setting this config to 0.")
      .version("2.4.0")
      .bytesConf(ByteUnit.BYTE)
      .createWithDefaultString("1m")

  private[spark] val EVENT_LOG_ROLLING_MAX_FILES_TO_RETAIN =
    ConfigBuilder("spark.history.fs.eventLog.rolling.maxFilesToRetain")
      .doc("The maximum number of event log files which will be retained as non-compacted. " +
        "By default, all event log files will be retained. Please set the configuration " +
        s"and ${EVENT_LOG_ROLLING_MAX_FILE_SIZE.key} accordingly if you want to control " +
        "the overall size of event log files.")
      .version("3.0.0")
      .intConf
      .checkValue(_ > 0, "Max event log files to retain should be higher than 0.")
      .createWithDefault(Integer.MAX_VALUE)

  private[spark] val EVENT_LOG_COMPACTION_SCORE_THRESHOLD =
    ConfigBuilder("spark.history.fs.eventLog.rolling.compaction.score.threshold")
      .doc("The threshold score to determine whether it's good to do the compaction or not. " +
        "The compaction score is calculated in analyzing, and being compared to this value. " +
        "Compaction will proceed only when the score is higher than the threshold value.")
      .version("3.0.0")
      .internal()
      .doubleConf
      .createWithDefault(0.7d)

  val DRIVER_LOG_CLEANER_ENABLED = ConfigBuilder("spark.history.fs.driverlog.cleaner.enabled")
    .version("3.0.0")
    .doc("Specifies whether the History Server should periodically clean up driver logs from " +
      "storage.")
    .fallbackConf(CLEANER_ENABLED)

  val MAX_DRIVER_LOG_AGE_S = ConfigBuilder("spark.history.fs.driverlog.cleaner.maxAge")
    .version("3.0.0")
    .doc(s"When ${DRIVER_LOG_CLEANER_ENABLED.key}=true, driver log files older than this will be " +
      s"deleted when the driver log cleaner runs.")
    .fallbackConf(MAX_LOG_AGE_S)

  val DRIVER_LOG_CLEANER_INTERVAL = ConfigBuilder("spark.history.fs.driverlog.cleaner.interval")
    .version("3.0.0")
    .doc(s" When ${DRIVER_LOG_CLEANER_ENABLED.key}=true, specifies how often the filesystem " +
      s"driver log cleaner checks for files to delete. Files are only deleted if they are older " +
      s"than ${MAX_DRIVER_LOG_AGE_S.key}.")
    .fallbackConf(CLEANER_INTERVAL_S)

  val HISTORY_SERVER_UI_ACLS_ENABLE = ConfigBuilder("spark.history.ui.acls.enable")
    .version("1.0.1")
    .doc("Specifies whether ACLs should be checked to authorize users viewing the applications " +
      "in the history server. If enabled, access control checks are performed regardless of " +
      "what the individual applications had set for spark.ui.acls.enable. The application owner " +
      "will always have authorization to view their own application and any users specified via " +
      "spark.ui.view.acls and groups specified via spark.ui.view.acls.groups when the " +
      "application was run will also have authorization to view that application. If disabled, " +
      "no access control checks are made for any application UIs available through the history " +
      "server.")
    .booleanConf
    .createWithDefault(false)

  val HISTORY_SERVER_UI_ADMIN_ACLS = ConfigBuilder("spark.history.ui.admin.acls")
    .version("2.1.1")
    .doc("Comma separated list of users that have view access to all the Spark applications in " +
      "history server.")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val HISTORY_SERVER_UI_ADMIN_ACLS_GROUPS = ConfigBuilder("spark.history.ui.admin.acls.groups")
    .version("2.1.1")
    .doc("Comma separated list of groups that have view access to all the Spark applications " +
      "in history server.")
    .stringConf
    .toSequence
    .createWithDefault(Nil)

  val HISTORY_UI_MAX_APPS = ConfigBuilder("spark.history.ui.maxApplications")
    .version("2.0.1")
    .doc("The number of applications to display on the history summary page. Application UIs " +
      "are still available by accessing their URLs directly even if they are not displayed on " +
      "the history summary page.")
    .intConf
    .createWithDefault(Integer.MAX_VALUE)

  val NUM_REPLAY_THREADS = ConfigBuilder("spark.history.fs.numReplayThreads")
    .version("2.0.0")
    .doc("Number of threads that will be used by history server to process event logs.")
    .intConf
    .createWithDefaultFunction(() => Math.ceil(Runtime.getRuntime.availableProcessors() / 4f).toInt)

  val RETAINED_APPLICATIONS = ConfigBuilder("spark.history.retainedApplications")
    .version("1.0.0")
    .doc("The number of applications to retain UI data for in the cache. If this cap is " +
      "exceeded, then the oldest applications will be removed from the cache. If an application " +
      "is not in the cache, it will have to be loaded from disk if it is accessed from the UI.")
    .intConf
    .createWithDefault(50)

  val PROVIDER = ConfigBuilder("spark.history.provider")
    .version("1.1.0")
    .doc("Name of the class implementing the application history backend.")
    .stringConf
    .createWithDefault("org.apache.spark.deploy.history.FsHistoryProvider")

  val KERBEROS_ENABLED = ConfigBuilder("spark.history.kerberos.enabled")
    .version("1.0.1")
    .doc("Indicates whether the history server should use kerberos to login. This is required " +
      "if the history server is accessing HDFS files on a secure Hadoop cluster.")
    .booleanConf
    .createWithDefault(false)

  val KERBEROS_PRINCIPAL = ConfigBuilder("spark.history.kerberos.principal")
    .version("1.0.1")
    .doc(s"When ${KERBEROS_ENABLED.key}=true, specifies kerberos principal name for " +
      s" the History Server.")
    .stringConf
    .createOptional

  val KERBEROS_KEYTAB = ConfigBuilder("spark.history.kerberos.keytab")
    .version("1.0.1")
    .doc(s"When ${KERBEROS_ENABLED.key}=true, specifies location of the kerberos keytab file " +
      s"for the History Server.")
    .stringConf
    .createOptional

  val CUSTOM_EXECUTOR_LOG_URL = ConfigBuilder("spark.history.custom.executor.log.url")
    .doc("Specifies custom spark executor log url for supporting external log service instead of " +
      "using cluster managers' application log urls in the history server. Spark will support " +
      "some path variables via patterns which can vary on cluster manager. Please check the " +
      "documentation for your cluster manager to see which patterns are supported, if any. " +
      "This configuration has no effect on a live application, it only affects the history server.")
    .version("3.0.0")
    .stringConf
    .createOptional

  val APPLY_CUSTOM_EXECUTOR_LOG_URL_TO_INCOMPLETE_APP =
    ConfigBuilder("spark.history.custom.executor.log.url.applyIncompleteApplication")
      .doc("Whether to apply custom executor log url, as specified by " +
        s"${CUSTOM_EXECUTOR_LOG_URL.key}, to incomplete application as well. " +
        "Even if this is true, this still only affects the behavior of the history server, " +
        "not running spark applications.")
      .version("3.0.0")
      .booleanConf
      .createWithDefault(true)

  val HYBRID_STORE_ENABLED = ConfigBuilder("spark.history.store.hybridStore.enabled")
    .doc("Whether to use HybridStore as the store when parsing event logs. " +
      "HybridStore will first write data to an in-memory store and having a background thread " +
      "that dumps data to a disk store after the writing to in-memory store is completed.")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(false)

  val MAX_IN_MEMORY_STORE_USAGE = ConfigBuilder("spark.history.store.hybridStore.maxMemoryUsage")
    .doc("Maximum memory space that can be used to create HybridStore. The HybridStore co-uses " +
      "the heap memory, so the heap memory should be increased through the memory option for SHS " +
      "if the HybridStore is enabled.")
    .version("3.1.0")
    .bytesConf(ByteUnit.BYTE)
    .createWithDefaultString("2g")

  object HybridStoreDiskBackend extends Enumeration {
    val LEVELDB, ROCKSDB = Value
  }

  val HYBRID_STORE_DISK_BACKEND = ConfigBuilder("spark.history.store.hybridStore.diskBackend")
    .doc("Specifies a disk-based store used in hybrid store; ROCKSDB or LEVELDB (deprecated).")
    .version("3.3.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(HybridStoreDiskBackend.values.map(_.toString))
    .createWithDefault(HybridStoreDiskBackend.ROCKSDB.toString)
}
