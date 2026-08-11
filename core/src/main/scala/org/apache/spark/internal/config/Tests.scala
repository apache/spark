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

  val TEST_USE_COMPACT_OBJECT_HEADERS_KEY = "spark.test.useCompactObjectHeaders"

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

  val INJECT_SHUFFLE_FETCH_FAILURES =
    ConfigBuilder("spark.testing.injectShuffleFetchFailures")
      .doc("Corrupt the registered MapStatus of the first successful attempt of partition 0 " +
        "of every shuffle map stage, to induce downstream FetchFailed and stage retry. The " +
        "timing of the corruption is governed by INJECT_SHUFFLE_FETCH_FAILURES_" +
        "DOWNSTREAM_DELAY and INJECT_SHUFFLE_FETCH_FAILURES_RESULT_STAGE_DELAY (deferred " +
        "until N consumer task successes). Testing only.")
      .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
      .booleanConf
      .createWithDefault(false)

  val INJECT_SHUFFLE_FETCH_FAILURES_DOWNSTREAM_DELAY =
    ConfigBuilder("spark.testing.injectShuffleFetchFailuresDownstreamDelay")
      .doc("Used with INJECT_SHUFFLE_FETCH_FAILURES. Defer the producer's partition-0 " +
        "corruption until N ShuffleMapStage consumer task successes have been observed. " +
        "Default 1; set to 0 to corrupt at registration. Testing only.")
      .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
      .intConf
      .checkValue(_ >= 0, "Downstream-success delay must be non-negative")
      .createWithDefault(1)

  val INJECT_SHUFFLE_FETCH_FAILURES_RESULT_STAGE_DELAY =
    ConfigBuilder("spark.testing.injectShuffleFetchFailuresResultStageDelay")
      .doc("Used with INJECT_SHUFFLE_FETCH_FAILURES. Counterpart to " +
        "INJECT_SHUFFLE_FETCH_FAILURES_DOWNSTREAM_DELAY for ResultStage consumers. With the " +
        "default 0, when a ResultStage is the consumer of a pending corruption it is corrupted " +
        "before the result tasks dispatch, so the result stage has no completed tasks when " +
        "INJECT_SHUFFLE_FORCE_CHECKSUM_MISMATCH_ON_RECOMPUTE fires (the rollback would " +
        "otherwise abort the result stage, since OSS Spark does not support rolling result " +
        "stages back). Set to N > 0 to defer until N result-stage tasks have succeeded - this " +
        "is the only way to actually exercise the result-stage abort path. Testing only.")
      .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
      .intConf
      .checkValue(_ >= 0, "Result-stage-success delay must be non-negative")
      .createWithDefault(0)

  val INJECT_SHUFFLE_FORCE_CHECKSUM_MISMATCH_ON_RECOMPUTE =
    ConfigBuilder("spark.testing.injectShuffleForceChecksumMismatchOnRecompute")
      .doc("Used with INJECT_SHUFFLE_FETCH_FAILURES. Flag the recompute as checksum " +
        "mismatched, forcing downstream `rollbackSucceedingStages`. Testing only.")
      .withBindingPolicy(ConfigBindingPolicy.NOT_APPLICABLE)
      .booleanConf
      .createWithDefault(false)

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

  val TEST_SKIP_ESS_REGISTER = ConfigBuilder("spark.testing.skipESSRegister")
    .version("4.0.0")
    .doc("None of Spark testing modes (local, local-cluster) enables shuffle service. So it is " +
      s"hard to test ${SHUFFLE_SERVICE_ENABLED.key} when you only want to test this flag but " +
      s"without the real server. This config provides a way to allow tests run with " +
      s"${SHUFFLE_SERVICE_ENABLED.key} enabled without registration failures.")
    .booleanConf
    .createWithDefault(false)

}
