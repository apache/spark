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

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.features.YuniKornFeatureStep
import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.k8sTestTag
import org.apache.spark.deploy.k8s.integrationtest.YuniKornSuite.yunikornTag

private[spark] trait YuniKornTestsSuite { k8sSuite: KubernetesSuite =>
  import YuniKornTestsSuite._

  protected def checkScheduler(pod: Pod): Unit = {
    assert(pod.getSpec.getSchedulerName === "yunikorn")
  }

  protected def checkAnnotations(pod: Pod): Unit = {
    val appId = pod.getMetadata.getLabels.get("spark-app-selector")
    val annotations = pod.getMetadata.getAnnotations
    assert(annotations.get("yunikorn.apache.org/app-id") === appId)
  }

  test("Run SparkPi with yunikorn scheduler", k8sTestTag, yunikornTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.pod.featureSteps", YUNIKORN_FEATURE_STEP)
      .set("spark.kubernetes.executor.pod.featureSteps", YUNIKORN_FEATURE_STEP)
    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        doBasicDriverPodCheck(driverPod)
        checkScheduler(driverPod)
        checkAnnotations(driverPod)
      },
      executorPodChecker = (executorPod: Pod) => {
        doBasicExecutorPodCheck(executorPod)
        checkScheduler(executorPod)
        checkAnnotations(executorPod)
      }
    )
  }
}

private[spark] object YuniKornTestsSuite extends SparkFunSuite {
  val YUNIKORN_FEATURE_STEP = classOf[YuniKornFeatureStep].getName
}
