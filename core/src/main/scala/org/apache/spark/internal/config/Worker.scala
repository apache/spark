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

  val WORKER_INITIAL_REGISTRATION_RETRIES = ConfigBuilder("spark.worker.initialRegistrationRetries")
    .version("4.0.0")
    .internal()
    .doc("The number of retries to reconnect in short intervals (between 5 and 15 seconds).")
    .intConf
    .checkValue(_ > 0, "The number of initial registration retries should be positive")
    .createWithDefault(6)

  val WORKER_MAX_REGISTRATION_RETRIES = ConfigBuilder("spark.worker.maxRegistrationRetries")
    .version("4.0.0")
    .internal()
    .doc("The max number of retries to reconnect. After spark.worker.initialRegistrationRetries " +
      "attempts, the interval is between 30 and 90 seconds.")
    .intConf
    .checkValue(_ > 0, "The max number of registration retries should be positive")
    .createWithDefault(16)

  val WORKER_DRIVER_TERMINATE_TIMEOUT = ConfigBuilder("spark.worker.driverTerminateTimeout")
    .version("2.1.2")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("10s")

  val WORKER_CLEANUP_ENABLED = ConfigBuilder("spark.worker.cleanup.enabled")
    .version("1.0.0")
    .booleanConf
    .createWithDefault(true)

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

  val WORKER_DECOMMISSION_SIGNAL =
    ConfigBuilder("spark.worker.decommission.signal")
      .doc("The signal that used to trigger the worker to start decommission.")
      .version("3.2.0")
      .stringConf
      .createWithDefaultString("PWR")

  val WORKER_ID_PATTERN = ConfigBuilder("spark.worker.idPattern")
    .internal()
    .doc("The pattern for worker ID generation based on Java `String.format` method. The " +
      "default value is `worker-%s-%s-%d` which represents the existing worker id string, e.g.," +
      " `worker-20231109183042-[fe80::1%lo0]-39729`. Please be careful to generate unique IDs")
    .version("4.0.0")
    .stringConf
    .checkValue(!_.format("20231109000000", "host", 0).exists(_.isWhitespace),
      "Whitespace is not allowed.")
    .createWithDefaultString("worker-%s-%s-%d")
}
