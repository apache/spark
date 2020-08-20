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

import io.fabric8.kubernetes.api.model.Pod
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}

import org.apache.spark.internal.config
import org.apache.spark.internal.config.Worker

private[spark] trait ExternalShuffleSidecarSuite { k8sSuite: KubernetesSuite =>

  import ExternalShuffleSidecarSuite._
  import KubernetesSuite._

  test("Test external shuffle service", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.pyspark.pythonVersion", "3")
      .set("spark.kubernetes.container.image", pyImage)
      .set(config.SHUFFLE_SERVICE_ENABLED.key, "true")
      // Ensure we have somewhere to migrate our data too
      .set("spark.executor.instances", "3")

    runSparkApplicationAndVerifyCompletion(
      appResource = PYSPARK_RESOURCE,
      mainClass = "",
      expectedLogOnCompletion = Seq(
        "Finished waiting, stopping Spark",
        "Final accumulator value is: 100",
      ),
      appArgs = Array.empty[String],
      driverPodChecker = doBasicDriverPyPodCheck,
      executorPodChecker = checkESSIsLaunchedAndUsed,
      appLocator = appLocator,
      isJVM = false,
      pyFiles = None,
      executorPatience = None,
      decommissioningTest = false)
  }

  def checkESSIsLaunchedAndUsed(executorPod: Pod): Unit = {
    doBasicExecutorPyPodCheck(executorPod)
    assert(executorPod.getSpec.getContainers.get(1).getName === "spark-kubernetes-shuffle")
    val expectedShuffleLogOnCompletion = Seq("MAGIC CHEESE")
    val expectedExecLogOnCompletion = Seq("MAGIC EXEC CHEESE")

    var shuffleLogText = ""
    var execLogText = ""
    Eventually.eventually(TIMEOUT, INTERVAL) {
      try {
        shuffleLogText = kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(executorPod.getMetadata.getName)
          .inContainer("spark-kubernetes-shuffle")
          .getLog
      } catch {
        case e: Exception =>
          logWarning("Not able to update log text", e)
          shuffleLogText = s"${shuffleLogText} no update ${e} for spark-kubernetes-shuffle\n"
      }
      try {
        execLogText = kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(executorPod.getMetadata.getName)
          .inContainer("spark-kubernetes-executor")
          .getLog
      } catch {
        case e: Exception =>
          logWarning("Not able to update log text", e)
          execLogText = s"${execLogText} no update ${e} for spark-kubernetes-executor\n"
      }
      expectedShuffleLogOnCompletion.foreach { e =>
        assert(shuffleLogText
          .contains(e),
          s"The application did not complete, did not find str ${e} in ${shuffleLogText}")
      }
    }
  }

}

private[spark] object ExternalShuffleSidecarSuite {
  val TEST_LOCAL_PYSPARK: String = "local:///opt/spark/tests/"
  val PYSPARK_RESOURCE: String = TEST_LOCAL_PYSPARK + "decommissioning.py"
}
