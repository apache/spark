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

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import com.google.common.io.Files
import io.fabric8.kubernetes.api.model.Pod
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.matchers.should.Matchers._
import org.scalatest.time.{Minutes, Seconds, Span}

import org.apache.spark.deploy.k8s.Config.{KUBERNETES_EXECUTOR_DECOMMISSION_LABEL, KUBERNETES_EXECUTOR_DECOMMISSION_LABEL_VALUE}
import org.apache.spark.internal.config
import org.apache.spark.internal.config.PLUGINS

private[spark] trait DecommissionSuite { k8sSuite: KubernetesSuite =>

  import DecommissionSuite._
  import KubernetesSuite.k8sTestTag

  def runDecommissionTest(f: () => Unit): Unit = {
    val logConfFilePath = s"${sparkHomeDir.toFile}/conf/log4j2.properties"

    try {
      Files.write(
        """rootLogger.level = info
          |rootLogger.appenderRef.stdout.ref = console
          |appender.console.type = Console
          |appender.console.name = console
          |appender.console.target = SYSTEM_OUT
          |appender.console.layout.type = PatternLayout
          |appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n
          |
          |logger.spark.name = org.apache.spark
          |logger.spark.level = debug
      """.stripMargin,
        new File(logConfFilePath),
        StandardCharsets.UTF_8)

      f()
    } finally {
      new File(logConfFilePath).delete()
    }
  }

  test("Test basic decommissioning", k8sTestTag) {
    runDecommissionTest(() => {
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
        isJVM = false,
        pyFiles = None,
        executorPatience = None,
        decommissioningTest = true)
    })
  }

  test("Test basic decommissioning with shuffle cleanup", k8sTestTag) {
    runDecommissionTest(() => {
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
        isJVM = false,
        pyFiles = None,
        executorPatience = None,
        decommissioningTest = true)
    })
  }

  test("Test decommissioning with dynamic allocation & shuffle cleanups", k8sTestTag) {
    runDecommissionTest(() => {
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
        // The default of 30 seconds is fine, but for testing we just want to
        // give enough time to validate the labels are set.
        .set("spark.storage.decommission.replicationReattemptInterval", "75")
        // Configure labels for decommissioning pods.
        .set(KUBERNETES_EXECUTOR_DECOMMISSION_LABEL.key, "solong")
        .set(KUBERNETES_EXECUTOR_DECOMMISSION_LABEL_VALUE.key, "cruelworld")

      // This is called on all exec pods but we only care about exec 0 since it's the "first."
      // We only do this inside of this test since the other tests trigger k8s side deletes where we
      // do not apply labels.
      def checkFirstExecutorPodGetsLabeled(pod: Pod): Unit = {
        if (pod.getMetadata.getName.endsWith("-1")) {
          val client = kubernetesTestComponents.kubernetesClient
          // The label will be added eventually, but k8s objects don't refresh.
          Eventually.eventually(
            PatienceConfiguration.Timeout(Span(120, Seconds)),
            PatienceConfiguration.Interval(Span(1, Seconds))) {

            val currentPod = client.pods().withName(pod.getMetadata.getName).get
            val labels = currentPod.getMetadata.getLabels.asScala

            labels should not be (null)
            labels should (contain key ("solong") and contain value ("cruelworld"))
          }
        }
        doBasicExecutorPyPodCheck(pod)
      }

      runSparkApplicationAndVerifyCompletion(
        appResource = PYSPARK_SCALE,
        mainClass = "",
        expectedDriverLogOnCompletion = Seq(
          "Finished waiting, stopping Spark",
          "Decommission executors",
          "Remove reason statistics: (gracefully decommissioned: 1, decommision unfinished: 0, " +
            "driver killed: 0, unexpectedly exited: 0)."),
        appArgs = Array.empty[String],
        driverPodChecker = doBasicDriverPyPodCheck,
        executorPodChecker = checkFirstExecutorPodGetsLabeled,
        isJVM = false,
        pyFiles = None,
        executorPatience = Some(None, Some(DECOMMISSIONING_FINISHED_TIMEOUT)),
        decommissioningTest = false)
    })
  }

  test("Test decommissioning timeouts", k8sTestTag) {
    runDecommissionTest(() => {
      sparkAppConf
        .set(config.DECOMMISSION_ENABLED.key, "true")
        .set("spark.kubernetes.container.image", pyImage)
        .set(config.STORAGE_DECOMMISSION_ENABLED.key, "true")
        .set(config.STORAGE_DECOMMISSION_SHUFFLE_BLOCKS_ENABLED.key, "true")
        .set(config.STORAGE_DECOMMISSION_RDD_BLOCKS_ENABLED.key, "true")
        // Ensure we have somewhere to migrate our data too
        .set("spark.executor.instances", "3")
        // Set super high so the timeout is triggered
        .set("spark.storage.decommission.replicationReattemptInterval", "8640000")
        // Set super low so the timeout is triggered
        .set(config.EXECUTOR_DECOMMISSION_FORCE_KILL_TIMEOUT.key, "10")

      runSparkApplicationAndVerifyCompletion(
        appResource = PYSPARK_DECOMISSIONING,
        mainClass = "",
        expectedDriverLogOnCompletion = Seq(
          "Finished waiting, stopping Spark",
          "Decommission executors",
          "failed to decommission in 10, killing",
          "killed by driver."),
        appArgs = Array.empty[String],
        driverPodChecker = doBasicDriverPyPodCheck,
        executorPodChecker = doBasicExecutorPyPodCheck,
        isJVM = false,
        pyFiles = None,
        executorPatience = None,
        decommissioningTest = true)
    })
  }

  test("SPARK-37576: Rolling decommissioning", k8sTestTag) {
    runDecommissionTest(() => {
      sparkAppConf
        .set("spark.kubernetes.container.image", pyImage)
        .set(config.DECOMMISSION_ENABLED.key, "true")
        .set(PLUGINS.key, "org.apache.spark.scheduler.cluster.k8s.ExecutorRollPlugin")
        .set("spark.kubernetes.executor.rollInterval", "30s")
        .set("spark.kubernetes.executor.rollPolicy", "ID")

      runSparkApplicationAndVerifyCompletion(
        appResource = PythonTestsSuite.PYSPARK_PI,
        mainClass = "",
        expectedDriverLogOnCompletion = Seq(
          "Initialized driver component for plugin " +
            "org.apache.spark.scheduler.cluster.k8s.ExecutorRollPlugin",
          "Ask to decommission executor 1",
          "Removed 1 successfully in removeExecutor",
          "Going to request 1 executors",
          "Ask to decommission executor 2",
          "Removed 2 successfully in removeExecutor",
          "Going to request 1 executors"),
        appArgs = Array("10000"),
        driverPodChecker = doBasicDriverPyPodCheck,
        executorPodChecker = doBasicExecutorPyPodCheck,
        isJVM = false,
        pyFiles = None,
        executorPatience = None,
        decommissioningTest = true)
    })
  }
}

private[spark] object DecommissionSuite {
  val TEST_LOCAL_PYSPARK: String = "local:///opt/spark/tests/"
  val PYSPARK_DECOMISSIONING: String = TEST_LOCAL_PYSPARK + "decommissioning.py"
  val PYSPARK_DECOMISSIONING_CLEANUP: String = TEST_LOCAL_PYSPARK + "decommissioning_cleanup.py"
  val PYSPARK_SCALE: String = TEST_LOCAL_PYSPARK + "autoscale.py"
  val DECOMMISSIONING_FINISHED_TIMEOUT = PatienceConfiguration.Timeout(Span(4, Minutes))
}
