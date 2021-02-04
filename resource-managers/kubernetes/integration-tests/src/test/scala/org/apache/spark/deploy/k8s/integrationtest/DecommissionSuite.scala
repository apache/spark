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
package org.apache.spark.deploy.k8s.integrationtest

import org.apache.spark.internal.config

private[spark] trait DecommissionSuite { k8sSuite: KubernetesSuite =>

  import DecommissionSuite._
  import KubernetesSuite.k8sTestTag

  test("Test basic decommissioning", k8sTestTag) {
    sparkAppConf
      .set(config.DECOMMISSION_ENABLED.key, "true")
      .set("spark.kubernetes.container.image", pyImage)
      .set(config.STORAGE_DECOMMISSION_ENABLED.key, "true")
      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key, "true")
      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key, "true")
      // Ensure we have somewhere to migrate our data too
      .set("spark.executor.instances", "3")
      // The default of 30 seconds is fine, but for testing we just want to get this done fast.
      .set("spark.storage.decommission.replicationReattemptInterval", "1")

    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_DECOMISSIONING,
      mainClass = "",
      expectedDriverLogOnCompletion = Seq(
        "Finished waiting, stopping Spark",
        "Decommission executors",
        "Final accumulator value is: 100"),
      appArgs = Array.empty[String],
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = None,
      executorPatience = None,
      decommissioningTest = true)
  }

  test("Test basic decommissioning with shuffle cleanup", k8sTestTag) {
    sparkAppConf
      .set(config.DECOMMISSION_ENABLED.key, "true")
      .set("spark.kubernetes.container.image", pyImage)
      .set(config.STORAGE_DECOMMISSION_ENABLED.key, "true")
      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key, "true")
      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key, "true")
      .set(config.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED.key, "true")
      .set(config.DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT.key, "400")
      // Ensure we have somewhere to migrate our data too
      .set("spark.executor.instances", "3")
      // The default of 30 seconds is fine, but for testing we just want to get this done fast.
      .set("spark.storage.decommission.replicationReattemptInterval", "1")

    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_DECOMISSIONING_CLEANUP,
      mainClass = "",
      expectedDriverLogOnCompletion = Seq(
        "Finished waiting, stopping Spark",
        "Decommission executors"),
      appArgs = Array.empty[String],
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = None,
      executorPatience = None,
      decommissioningTest = true)
  }

  test("Test decommissioning with dynamic allocation & shuffle cleanups", k8sTestTag) {
    sparkAppConf
      .set(config.DECOMMISSION_ENABLED.key, "true")
      .set("spark.kubernetes.container.image", pyImage)
      .set(config.STORAGE_DECOMMISSION_ENABLED.key, "true")
      .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key, "true")
      .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key, "true")
      .set(config.DYN_ALLOCATION_SHUFFLE_TRACKING_ENABLED.key, "true")
      .set(config.DYN_ALLOCATION_SHUFFLE_TRACKING_TIMEOUT.key, "30")
      .set(config.DYN_ALLOCATION_CACHED_EXECUTOR_IDLE_TIMEOUT.key, "30")
      .set(config.DYN_ALLOCATION_EXECUTOR_IDLE_TIMEOUT.key, "5")
      .set(config.DYN_ALLOCATION_MIN_EXECUTORS.key, "1")
      .set(config.DYN_ALLOCATION_INITIAL_EXECUTORS.key, "2")
      .set(config.DYN_ALLOCATION_ENABLED.key, "true")
      // The default of 30 seconds is fine, but for testing we just want to get this done fast.
      .set("spark.storage.decommission.replicationReattemptInterval", "1")

    var execLogs: String = ""

    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_SCALE,
      mainClass = "",
      expectedDriverLogOnCompletion = Seq(
        "Finished waiting, stopping Spark",
        "Decommission executors"),
      appArgs = Array.empty[String],
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = doBasicExecutorPyPodCheck,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = None,
      executorPatience = None,
      decommissioningTest = false)
  }
}

private[spark] object DecommissionSuite {
  val TEST_LOCAL_PYSPARK: String = "local:///opt/spark/tests/"
  val PYSPARK_DECOMISSIONING: String = TEST_LOCAL_PYSPARK + "decommissioning.py"
  val PYSPARK_DECOMISSIONING_CLEANUP: String = TEST_LOCAL_PYSPARK + "decommissioning_cleanup.py"
  val PYSPARK_SCALE: String = TEST_LOCAL_PYSPARK + "autoscale.py"
}
