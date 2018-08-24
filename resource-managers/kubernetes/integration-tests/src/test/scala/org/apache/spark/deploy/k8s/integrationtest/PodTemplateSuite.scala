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

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}

private[spark] trait PodTemplateSuite { k8sSuite: KubernetesSuite =>

  import PodTemplateSuite._

  test("Start pod creation from template") {
    createPodTemplateFiles()
    sparkAppConf
      .set("spark.kubernetes.driver.podTemplateFile", DRIVER_TEMPLATE_FILE.getAbsolutePath)
      .set("spark.kubernetes.executor.podTemplateFile", EXECUTOR_TEMPLATE_FILE.getAbsolutePath)
      .set("spark.kubernetes.driver.containerName", DRIVER_CONTAINER_NAME)
      .set("spark.kubernetes.executor.containerName", EXECUTOR_LABEL_VALUE)
    runSparkPiAndVerifyCompletion(
      driverPodChecker = (driverPod: Pod) => {
        checkDriverPod(driverPod)
      },
      executorPodChecker = (executorPod: Pod) => {
        checkExecutorPod(executorPod)
      }
    )
  }

  private def checkDriverPod(pod: Pod): Unit = {
    assert(pod.getMetadata.getName === driverPodName)
    assert(pod.getSpec.getContainers.get(0).getImage === image)
    assert(pod.getSpec.getContainers.get(0).getName === DRIVER_CONTAINER_NAME)
    assert(pod.getMetadata.getLabels.containsKey(LABEL_KEY))
    assert(pod.getMetadata.getLabels.get(LABEL_KEY) === DRIVER_LABEL_VALUE)
  }

  private def checkExecutorPod(pod: Pod): Unit = {
    assert(pod.getMetadata.getName === "template-pod")
    assert(pod.getSpec.getContainers.get(0).getImage === image)
    assert(pod.getSpec.getContainers.get(0).getName === EXECUTOR_CONTAINER_NAME)
    assert(pod.getMetadata.getLabels.containsKey(LABEL_KEY))
    assert(pod.getMetadata.getLabels.get(LABEL_KEY) === EXECUTOR_LABEL_VALUE)
  }

  private def createPodTemplateFiles(): Unit = {
    val objectMapper = new ObjectMapper(new YAMLFactory())
    val driverTemplatePod = new PodBuilder()
      .withApiVersion("1")
      .withKind("Pod")
      .withNewMetadata()
      .addToLabels(LABEL_KEY, DRIVER_LABEL_VALUE)
      .endMetadata()
      .withNewSpec()
      .addNewContainer()
      .withName(DRIVER_CONTAINER_NAME)
      .withImage("will-be-overwritten")
      .endContainer()
      .endSpec()
      .build()

    val executorTemplatePod = new PodBuilder()
      .withApiVersion("1")
      .withKind("Pod")
      .withNewMetadata()
      .withName("template-pod")
      .addToLabels(LABEL_KEY, EXECUTOR_LABEL_VALUE)
      .endMetadata()
      .withNewSpec()
      .addNewContainer()
      .withName(EXECUTOR_CONTAINER_NAME)
      .withImage("will-be-overwritten")
      .endContainer()
      .endSpec()
      .build()

    objectMapper.writeValue(DRIVER_TEMPLATE_FILE, driverTemplatePod)
    objectMapper.writeValue(EXECUTOR_TEMPLATE_FILE, executorTemplatePod)
  }
}

private[spark] object PodTemplateSuite {
  val DRIVER_CONTAINER_NAME = "test-driver-container"
  val EXECUTOR_CONTAINER_NAME = "test-executor-container"
  val LABEL_KEY = "template-label-key"
  val DRIVER_LABEL_VALUE = "driver-template-label-value"
  val EXECUTOR_LABEL_VALUE = "executor-template-label-value"

  val DRIVER_TEMPLATE_FILE = Files.createTempFile("driver-pod-template", "yml").toFile
  DRIVER_TEMPLATE_FILE.deleteOnExit()

  val EXECUTOR_TEMPLATE_FILE = Files.createTempFile("executor-pod-template", "yml").toFile
  EXECUTOR_TEMPLATE_FILE.deleteOnExit()
}
