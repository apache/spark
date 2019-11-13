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

import io.fabric8.kubernetes.api.model.{Pod, Toleration, TolerationBuilder}
import org.scalatest.concurrent.Eventually
import org.scalatest.Tag

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite._

private[spark] trait TolerationsTestsSuite { k8sSuite: KubernetesSuite =>

  private def driverTolerations(): List[Toleration] = {
    List(
      new TolerationBuilder()
        .withNewKey("key0")
        .withNewOperator("Equal")
        .withNewEffect("NoSchedule")
        .withNewValue("value")
        .build(),
      new TolerationBuilder()
        .withNewKey("key1")
        .withNewOperator("Exists")
        .withNewEffect("NoExecute")
        .withTolerationSeconds(15)
        .build(),
      new TolerationBuilder()
        .withNewOperator("Exists")
        .build()
    )
  }

  private def executorTolerations(): List[Toleration] = {
    List(
      new TolerationBuilder()
        .withNewKey("key2")
        .withNewOperator("Equal")
        .withNewEffect("NoSchedule")
        .withNewValue("value")
        .build(),
      new TolerationBuilder()
        .withNewKey("key3")
        .withNewOperator("Exists")
        .withNewEffect("NoExecute")
        .withTolerationSeconds(15)
        .build(),
      new TolerationBuilder()
        .withNewKey("key4")
        .withNewOperator("Exists")
        .build()
    )
  }

  test("Run SparkPi with tolerations.", k8sTestTag) {
    sparkAppConf
      .set(s"spark.kubernetes.driver.tolerations.0.key", "key0")
      .set(s"spark.kubernetes.driver.tolerations.0.operator", "Equal")
      .set(s"spark.kubernetes.driver.tolerations.0.effect", "NoSchedule")
      .set(s"spark.kubernetes.driver.tolerations.0.value", "value")
      .set(s"spark.kubernetes.driver.tolerations.1.key", "key1")
      .set(s"spark.kubernetes.driver.tolerations.1.operator", "Exists")
      .set(s"spark.kubernetes.driver.tolerations.1.effect", "NoExecute")
      .set(s"spark.kubernetes.driver.tolerations.1.tolerationSeconds", "15")
      .set(s"spark.kubernetes.driver.tolerations.2.operator", "Exists")
      .set(s"spark.kubernetes.executor.tolerations.0.key", "key2")
      .set(s"spark.kubernetes.executor.tolerations.0.operator", "Equal")
      .set(s"spark.kubernetes.executor.tolerations.0.effect", "NoSchedule")
      .set(s"spark.kubernetes.executor.tolerations.0.value", "value")
      .set(s"spark.kubernetes.executor.tolerations.1.key", "key3")
      .set(s"spark.kubernetes.executor.tolerations.1.operator", "Exists")
      .set(s"spark.kubernetes.executor.tolerations.1.effect", "NoExecute")
      .set(s"spark.kubernetes.executor.tolerations.1.tolerationSeconds", "15")
      .set(s"spark.kubernetes.executor.tolerations.2.key", "key4")
      .set(s"spark.kubernetes.executor.tolerations.2.operator", "Exists")

    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkTolerations(driverPod, driverTolerations())
      },
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        checkTolerations(executorPod, executorTolerations())
      },
      appArgs = Array("1000") // give it enough time for all execs to be visible
    )
  }

  private def checkTolerations(pod: Pod, tolerations: List[Toleration]): Unit = {
    Eventually.eventually(TIMEOUT, INTERVAL) {
      tolerations.map { case (t) =>
        assert(kubernetesTestComponents.kubernetesClient
          .pods()
          .withName(pod.getMetadata().getName())
          .get()
          .getSpec()
          .getTolerations
          .contains(t), "Configured toleration wasn't found")
      }
    }
  }
}