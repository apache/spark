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

private[spark] object Worker {
  val SPARK_WORKER_PREFIX = "spark.worker"

  val SPARK_WORKER_RESOURCE_FILE =
    ConfigBuilder("spark.worker.resourcesFile")
    .internal()
    .doc("Path to a file containing the resources allocated to the worker. " +
      "The file should be formatted as a JSON array of ResourceAllocation objects. " +
      "Only used internally in standalone mode.")
    .version("3.0.0")
    .stringConf
    .createOptional

  val WORKER_TIMEOUT = ConfigBuilder("spark.worker.timeout")
    .version("0.6.2")
    .longConf
    .createWithDefault(60)

  val WORKER_DRIVER_TERMINATE_TIMEOUT = ConfigBuilder("spark.worker.driverTerminateTimeout")
    .version("2.1.2")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("10s")

  val WORKER_CLEANUP_ENABLED = ConfigBuilder("spark.worker.cleanup.enabled")
    .version("1.0.0")
    .booleanConf
    .createWithDefault(false)

  val WORKER_CLEANUP_INTERVAL = ConfigBuilder("spark.worker.cleanup.interval")
    .version("1.0.0")
    .longConf
    .createWithDefault(60 * 30)

  val APP_DATA_RETENTION = ConfigBuilder("spark.worker.cleanup.appDataTtl")
    .version("1.0.0")
    .longConf
    .createWithDefault(7 * 24 * 3600)

  val PREFER_CONFIGURED_MASTER_ADDRESS = ConfigBuilder("spark.worker.preferConfiguredMasterAddress")
    .version("2.2.1")
    .booleanConf
    .createWithDefault(false)

  val WORKER_UI_PORT = ConfigBuilder("spark.worker.ui.port")
    .version("1.1.0")
    .intConf
    .createOptional

  val WORKER_UI_RETAINED_EXECUTORS = ConfigBuilder("spark.worker.ui.retainedExecutors")
    .version("1.5.0")
    .intConf
    .createWithDefault(1000)

  val WORKER_UI_RETAINED_DRIVERS = ConfigBuilder("spark.worker.ui.retainedDrivers")
    .version("1.5.0")
    .intConf
    .createWithDefault(1000)

  val UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF =
    ConfigBuilder("spark.worker.ui.compressedLogFileLengthCacheSize")
      .version("2.0.2")
      .intConf
      .createWithDefault(100)
}
