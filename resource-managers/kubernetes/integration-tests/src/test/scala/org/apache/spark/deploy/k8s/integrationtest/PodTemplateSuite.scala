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

import io.fabric8.kubernetes.api.model.Pod

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.{k8sTestTag, schedulingTestTag}

private[spark] trait PodTemplateSuite { k8sSuite: KubernetesSuite =>

  import PodTemplateSuite._

  test("Start pod creation from template", k8sTestTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.podTemplateFile", DRIVER_TEMPLATE_FILE.getAbsolutePath)
      .set("spark.kubernetes.executor.podTemplateFile", EXECUTOR_TEMPLATE_FILE.getAbsolutePath)
    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        assert(driverPod.getMetadata.getName === driverPodName)
        assert(driverPod.getSpec.getContainers.get(0).getImage === image)
        assert(driverPod.getSpec.getContainers.get(0).getName === "test-driver-container")
        assert(driverPod.getMetadata.getLabels.containsKey(LABEL_KEY))
        assert(driverPod.getMetadata.getLabels.get(LABEL_KEY) === "driver-template-label-value")
      },
      executorPodChecker = (executorPod: Pod) => {
        assert(executorPod.getSpec.getContainers.get(0).getImage === image)
        assert(executorPod.getSpec.getContainers.get(0).getName === "test-executor-container")
        assert(executorPod.getMetadata.getLabels.containsKey(LABEL_KEY))
        assert(executorPod.getMetadata.getLabels.get(LABEL_KEY) === "executor-template-label-value")
      }
    )
  }

  test("SPARK-38398: Schedule pod creation from template", k8sTestTag, schedulingTestTag) {
    sparkAppConf
      .set("spark.kubernetes.driver.podTemplateFile",
        DRIVER_SCHEDULE_TEMPLATE_FILE.getAbsolutePath)
      .set("spark.kubernetes.executor.podTemplateFile", EXECUTOR_TEMPLATE_FILE.getAbsolutePath)
    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        assert(driverPod.getMetadata.getName === driverPodName)
        assert(driverPod.getSpec.getContainers.get(0).getImage === image)
        assert(driverPod.getSpec.getContainers.get(0).getName === "test-driver-container")
        assert(driverPod.getMetadata.getLabels.containsKey(LABEL_KEY))
        assert(driverPod.getMetadata.getLabels.get(LABEL_KEY) === "driver-template-label-value")
        assert(driverPod.getSpec.getPriority() === 2000001000)
      },
      executorPodChecker = (executorPod: Pod) => {
        assert(executorPod.getSpec.getContainers.get(0).getImage === image)
        assert(executorPod.getSpec.getContainers.get(0).getName === "test-executor-container")
        assert(executorPod.getMetadata.getLabels.containsKey(LABEL_KEY))
        assert(executorPod.getMetadata.getLabels.get(LABEL_KEY) === "executor-template-label-value")
        assert(executorPod.getSpec.getPriority() === 0) // When there is no default, 0 is used.
      }
    )
  }
}

private[spark] object PodTemplateSuite {
  val LABEL_KEY = "template-label-key"
  val DRIVER_TEMPLATE_FILE = new File(getClass.getResource("/driver-template.yml").getFile)
  val DRIVER_SCHEDULE_TEMPLATE_FILE =
    new File(getClass.getResource("/driver-schedule-template.yml").getFile)
  val EXECUTOR_TEMPLATE_FILE = new File(getClass.getResource("/executor-template.yml").getFile)
}
