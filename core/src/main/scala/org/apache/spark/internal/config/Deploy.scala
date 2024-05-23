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

private[spark] object Deploy {
  val RECOVERY_MODE = ConfigBuilder("spark.deploy.recoveryMode")
    .version("0.8.1")
    .stringConf
    .createWithDefault("NONE")

  val RECOVERY_COMPRESSION_CODEC = ConfigBuilder("spark.deploy.recoveryCompressionCodec")
    .doc("A compression codec for persistence engines. none (default), lz4, lzf, snappy, and " +
      "zstd. Currently, only FILESYSTEM mode supports this configuration.")
    .version("4.0.0")
    .stringConf
    .createOptional

  val RECOVERY_MODE_FACTORY = ConfigBuilder("spark.deploy.recoveryMode.factory")
    .version("1.2.0")
    .stringConf
    .createWithDefault("")

  val RECOVERY_DIRECTORY = ConfigBuilder("spark.deploy.recoveryDirectory")
    .version("0.8.1")
    .stringConf
    .createWithDefault("")

  val RECOVERY_TIMEOUT = ConfigBuilder("spark.deploy.recoveryTimeout")
    .doc("Configures the timeout for recovery process. The default value is the same " +
      "with ${WORKER_TIMEOUT.key}.")
    .version("4.0.0")
    .timeConf(TimeUnit.SECONDS)
    .checkValue(_ > 0, "spark.deploy.recoveryTimeout must be positive.")
    .createOptional

  val ZOOKEEPER_URL = ConfigBuilder("spark.deploy.zookeeper.url")
    .doc(s"When `${RECOVERY_MODE.key}` is set to ZOOKEEPER, this " +
      "configuration is used to set the zookeeper URL to connect to.")
    .version("0.8.1")
    .stringConf
    .createOptional

  val ZOOKEEPER_DIRECTORY = ConfigBuilder("spark.deploy.zookeeper.dir")
    .version("0.8.1")
    .stringConf
    .createOptional

  val RETAINED_APPLICATIONS = ConfigBuilder("spark.deploy.retainedApplications")
    .version("0.8.0")
    .intConf
    .createWithDefault(200)

  val RETAINED_DRIVERS = ConfigBuilder("spark.deploy.retainedDrivers")
    .version("1.1.0")
    .intConf
    .createWithDefault(200)

  val REAPER_ITERATIONS = ConfigBuilder("spark.dead.worker.persistence")
    .version("0.8.0")
    .intConf
    .createWithDefault(15)

  val MAX_EXECUTOR_RETRIES = ConfigBuilder("spark.deploy.maxExecutorRetries")
    .version("1.6.3")
    .intConf
    .createWithDefault(10)

  val SPREAD_OUT_DRIVERS = ConfigBuilder("spark.deploy.spreadOutDrivers")
    .version("4.0.0")
    .booleanConf
    .createWithDefault(true)

  val SPREAD_OUT_APPS = ConfigBuilder("spark.deploy.spreadOutApps")
    .version("0.6.1")
    .withAlternative("spark.deploy.spreadOut")
    .booleanConf
    .createWithDefault(true)

  object WorkerSelectionPolicy extends Enumeration {
    val CORES_FREE_ASC, CORES_FREE_DESC, MEMORY_FREE_ASC, MEMORY_FREE_DESC, WORKER_ID = Value
  }

  val WORKER_SELECTION_POLICY = ConfigBuilder("spark.deploy.workerSelectionPolicy")
    .doc("A policy to assign executors on one of the assignable workers; " +
      "CORES_FREE_ASC to choose a worker with the least free cores, " +
      "CORES_FREE_DESC to choose a worker with the most free cores, " +
      "MEMORY_FREE_ASC to choose a worker with the least free memory, " +
      "MEMORY_FREE_DESC to choose a worker with the most free memory, " +
      "WORKER_ID to choose a worker with the smallest worker id. " +
      "CORES_FREE_DESC is the default behavior.")
    .version("4.0.0")
    .stringConf
    .transform(_.toUpperCase(Locale.ROOT))
    .checkValues(WorkerSelectionPolicy.values.map(_.toString))
    .createWithDefault(WorkerSelectionPolicy.CORES_FREE_DESC.toString)

  val DEFAULT_CORES = ConfigBuilder("spark.deploy.defaultCores")
    .version("0.9.0")
    .intConf
    .checkValue(_ > 0, "spark.deploy.defaultCores must be positive.")
    .createWithDefault(Int.MaxValue)

  val MAX_DRIVERS = ConfigBuilder("spark.deploy.maxDrivers")
    .doc("The maximum number of running drivers.")
    .version("4.0.0")
    .intConf
    .checkValue(_ > 0, "The maximum number of running drivers should be positive.")
    .createWithDefault(Int.MaxValue)

  val APP_NUMBER_MODULO = ConfigBuilder("spark.deploy.appNumberModulo")
    .doc("The modulo for app number. By default, the next of `app-yyyyMMddHHmmss-9999` is " +
      "`app-yyyyMMddHHmmss-10000`. If we have 10000 as modulo, it will be " +
      "`app-yyyyMMddHHmmss-0000`. In most cases, the prefix `app-yyyyMMddHHmmss` is increased " +
      "already during creating 10000 applications.")
    .version("4.0.0")
    .intConf
    .checkValue(_ >= 1000, "The modulo for app number should be greater than or equal to 1000.")
    .createOptional

  val DRIVER_ID_PATTERN = ConfigBuilder("spark.deploy.driverIdPattern")
    .doc("The pattern for driver ID generation based on Java `String.format` method. " +
      "The default value is `driver-%s-%04d` which represents the existing driver id string " +
      ", e.g., `driver-20231031224459-0019`. Please be careful to generate unique IDs")
    .version("4.0.0")
    .stringConf
    .checkValue(!_.format("20231101000000", 0).exists(_.isWhitespace), "Whitespace is not allowed.")
    .createWithDefault("driver-%s-%04d")

  val APP_ID_PATTERN = ConfigBuilder("spark.deploy.appIdPattern")
    .doc("The pattern for app ID generation based on Java `String.format` method.. " +
      "The default value is `app-%s-%04d` which represents the existing app id string, " +
      "e.g., `app-20231031224509-0008`. Plesae be careful to generate unique IDs.")
    .version("4.0.0")
    .stringConf
    .checkValue(!_.format("20231101000000", 0).exists(_.isWhitespace), "Whitespace is not allowed.")
    .createWithDefault("app-%s-%04d")
}
