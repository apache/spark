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

private[spark] object Tests {

  val TEST_USE_COMPRESSED_OOPS_KEY = "spark.test.useCompressedOops"

  val TEST_MEMORY = ConfigBuilder("spark.testing.memory")
    .version("1.6.0")
    .longConf
    .createWithDefault(Runtime.getRuntime.maxMemory)

  val TEST_DYNAMIC_ALLOCATION_SCHEDULE_ENABLED =
    ConfigBuilder("spark.testing.dynamicAllocation.schedule.enabled")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(true)

  val IS_TESTING = ConfigBuilder("spark.testing")
    .version("1.0.1")
    .booleanConf
    .createOptional

  val TEST_NO_STAGE_RETRY = ConfigBuilder("spark.test.noStageRetry")
    .version("1.2.0")
    .booleanConf
    .createWithDefault(false)

  val TEST_RESERVED_MEMORY = ConfigBuilder("spark.testing.reservedMemory")
    .version("1.6.0")
    .longConf
    .createOptional

  val TEST_N_HOSTS = ConfigBuilder("spark.testing.nHosts")
    .version("3.0.0")
    .intConf
    .createWithDefault(5)

  val TEST_N_EXECUTORS_HOST = ConfigBuilder("spark.testing.nExecutorsPerHost")
    .version("3.0.0")
    .intConf
    .createWithDefault(4)

  val TEST_N_CORES_EXECUTOR = ConfigBuilder("spark.testing.nCoresPerExecutor")
    .version("3.0.0")
    .intConf
    .createWithDefault(2)

  val RESOURCES_WARNING_TESTING = ConfigBuilder("spark.resources.warnings.testing")
    .version("3.1.0")
    .booleanConf
    .createWithDefault(false)

  val RESOURCE_PROFILE_MANAGER_TESTING =
    ConfigBuilder("spark.testing.resourceProfileManager")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

  // This configuration is used for unit tests to allow skipping the task cpus to cores validation
  // to allow emulating standalone mode behavior while running in local mode. Standalone mode
  // by default doesn't specify a number of executor cores, it just uses all the ones available
  // on the host.
  val SKIP_VALIDATE_CORES_TESTING =
    ConfigBuilder("spark.testing.skipValidateCores")
      .version("3.1.0")
      .booleanConf
      .createWithDefault(false)

}
