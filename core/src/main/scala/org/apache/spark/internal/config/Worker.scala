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
  val WORKER_TIMEOUT = ConfigBuilder("spark.worker.timeout")
    .longConf
    .createWithDefault(60)

  val WORKER_DRIVER_TERMINATE_TIMEOUT = ConfigBuilder("spark.worker.driverTerminateTimeout")
    .timeConf(TimeUnit.MILLISECONDS)
    .createWithDefaultString("10s")

  val WORKER_CLEANUP_ENABLED = ConfigBuilder("spark.worker.cleanup.enabled")
    .booleanConf
    .createWithDefault(false)

  val WORKER_CLEANUP_INTERVAL = ConfigBuilder("spark.worker.cleanup.interval")
    .longConf
    .createWithDefault(60 * 30)

  val APP_DATA_RETENTION = ConfigBuilder("spark.worker.cleanup.appDataTtl")
    .longConf
    .createWithDefault(7 * 24 * 3600)

  val PREFER_CONFIGURED_MASTER_ADDRESS = ConfigBuilder("spark.worker.preferConfiguredMasterAddress")
    .booleanConf
    .createWithDefault(false)

  val WORKER_UI_PORT = ConfigBuilder("spark.worker.ui.port")
    .intConf
    .createOptional

  val WORKER_UI_RETAINED_EXECUTORS = ConfigBuilder("spark.worker.ui.retainedExecutors")
    .intConf
    .createWithDefault(1000)

  val WORKER_UI_RETAINED_DRIVERS = ConfigBuilder("spark.worker.ui.retainedDrivers")
    .intConf
    .createWithDefault(1000)

  val UNCOMPRESSED_LOG_FILE_LENGTH_CACHE_SIZE_CONF =
    ConfigBuilder("spark.worker.ui.compressedLogFileLengthCacheSize")
    .intConf
    .createWithDefault(100)
}
