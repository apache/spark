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
import org.scalatest.concurrent.Eventually
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite._

trait EnvTestsSuite { k8sSuite: KubernetesSuite =>
  import EnvTestsSuite._

  private val expectedPathLine =
    s"""
       |PATH="/opt/spark/bin:$$PATH"
       |""".stripMargin.trim
  private val expectedLDLibPathLine =
    s"""
       |LD_LIBRARY_PATH="/opt/spark/lib:$$LD_LIBRARY_PATH"
       |""".stripMargin.trim

  test("SPARK-43505: Run SparkPi with extraLibraryPath and Path", k8sTestTag) {
    sparkAppConf
      .set("spark.executor.extraLibraryPath", "/opt/spark/lib")
      .set("spark.kubernetes.driverEnv.PATH", "/opt/spark/bin:$PATH")
      .set("spark.executorEnv.PATH", "/opt/spark/bin:$PATH")
    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkEnv(driverPod, true)
      },
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        checkEnv(executorPod, false)
      },
      appArgs = Array("1000") // give it enough time for all execs to be visible
    )
  }

  private def checkEnv(pod: Pod, isDriver: Boolean): Unit = {
    logDebug(s"Checking env for ${pod}")
    // Wait for the pod to become ready & have secrets provisioned
    implicit val podName: String = pod.getMetadata.getName
    implicit val components: KubernetesTestComponents = kubernetesTestComponents
    val env = Eventually.eventually(TIMEOUT, INTERVAL) {
      logDebug(s"Checking env of ${pod.getMetadata().getName()} with entrypoint")
      val env = Utils.executeCommand("/opt/entrypoint.sh", "env")
      assert(!env.isEmpty)
      env
    }
    env should include("PATH=/opt/spark/bin:")
    if (!isDriver) {
      // when on executor, extra lib path should also be udpated
      env should include("LD_LIBRARY_PATH=/opt/spark/lib:")
    }

    // Make sure our secret files are mounted correctly
    val files = Utils.executeCommand("ls", s"${PROFILE_MOUNT_PATH}")
    files should include(PROFILE_FILE)

    // check file content
    val profile = Utils.executeCommand("cat", s"$PROFILE_MOUNT_PATH/$PROFILE_FILE")
    profile should include(expectedPathLine)
    if (!isDriver) {
      profile should include(expectedLDLibPathLine)
    }
  }
}

private[spark] object EnvTestsSuite {
  val PROFILE_MOUNT_PATH = "/opt/spark/.profiles"
  val PROFILE_FILE = ".spark.profile"
}
