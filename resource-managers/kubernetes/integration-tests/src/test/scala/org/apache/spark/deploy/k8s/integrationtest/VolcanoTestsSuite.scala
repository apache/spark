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
import io.fabric8.volcano.client.VolcanoClient

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.features.VolcanoFeatureStep
import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.k8sTestTag
import org.apache.spark.deploy.k8s.integrationtest.VolcanoSuite.volcanoTag

private[spark] trait VolcanoTestsSuite { k8sSuite: KubernetesSuite =>
  import VolcanoTestsSuite._

  protected def checkScheduler(pod: Pod): Unit = {
    assert(pod.getSpec.getSchedulerName === "volcano")
  }

  protected def checkAnnotaion(pod: Pod): Unit = {
    val appId = pod.getMetadata.getLabels.get("spark-app-selector")
    val annotations = pod.getMetadata.getAnnotations
    assert(annotations.get("scheduling.k8s.io/group-name") === s"$appId-podgroup")
  }

  protected def checkPodGroup(pod: Pod): Unit = {
    val appId = pod.getMetadata.getLabels.get("spark-app-selector")
    val podGroupName = s"$appId-podgroup"
    val volcanoClient = kubernetesTestComponents.kubernetesClient.adapt(classOf[VolcanoClient])
    val podGroup = volcanoClient.podGroups().withName(podGroupName).get()
    assert(podGroup.getMetadata.getOwnerReferences.get(0).getName === pod.getMetadata.getName)
  }

  test("Run SparkPi with volcano scheduler", k8sTestTag, volcanoTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.pod.featureSteps", VOLCANO_FEATURE_STEP)
      .set("spark.kubernetes.executor.pod.featureSteps", VOLCANO_FEATURE_STEP)
    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkScheduler(driverPod)
        checkAnnotaion(driverPod)
        checkPodGroup(driverPod)
      },
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        checkScheduler(executorPod)
        checkAnnotaion(executorPod)
      }
    )
  }
}

private[spark] object VolcanoTestsSuite extends SparkFunSuite {
  val VOLCANO_FEATURE_STEP = classOf[VolcanoFeatureStep].getName
}
