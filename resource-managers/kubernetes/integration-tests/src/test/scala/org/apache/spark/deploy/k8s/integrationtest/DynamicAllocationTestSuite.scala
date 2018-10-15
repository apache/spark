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

import java.util

import io.fabric8.kubernetes.api.model.{VolumeBuilder, VolumeMountBuilder}
import io.fabric8.kubernetes.api.model.extensions.DaemonSetBuilder
import scala.collection.JavaConverters._

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite.k8sTestTag

private[spark] trait DynamicAllocationTestSuite { k8sSuite: KubernetesSuite =>
  import DynamicAllocationTestSuite._

  private def startShuffleService(): Unit = {
    val volume = new VolumeBuilder()
      .withName(VOLUME_NAME)
      .withNewHostPath(EXECUTOR_LOCAL_DIR)
      .build()
    val volumeMount = new VolumeMountBuilder()
      .withName(VOLUME_NAME)
      .withMountPath(EXECUTOR_LOCAL_DIR)
      .build()
    val daemonSet = new DaemonSetBuilder()
      .withNewMetadata()
        .withName(SHUFFLE_SERVICE_NAME)
        .withLabels(SHUFFLE_SERVICE_LABEL)
        .endMetadata()
      .withNewSpec()
        .withNewTemplate()
          .withNewMetadata()
            .withLabels(SHUFFLE_SERVICE_LABEL)
            .endMetadata()
          .withNewSpec()
            .addNewContainer()
              .withName("k8s-shuffle-service")
              .withImage(image)
              .withArgs("shuffle-service")
              .withVolumeMounts(volumeMount)
              .endContainer()
            .withVolumes(volume)
            .endSpec()
          .endTemplate()
        .endSpec()
      .build()
    kubernetesTestComponents.kubernetesClient
      .extensions()
      .daemonSets()
      .inNamespace(kubernetesTestComponents.namespace)
      .create(daemonSet)
  }

  private def stopShuffleService(): Unit = {
    kubernetesTestComponents.kubernetesClient
      .extensions()
      .daemonSets()
      .inNamespace(kubernetesTestComponents.namespace)
      .withName(SHUFFLE_SERVICE_NAME)
      .delete()
  }

  test("Run in cluster mode with dynamic allocation.", k8sTestTag) {
    startShuffleService()
    try {
      runAndVerifyCompletion()
    } finally {
      stopShuffleService()
    }
  }

  private def runAndVerifyCompletion(): Unit = {
    sparkAppConf
      .set("spark.local.dir", EXECUTOR_LOCAL_DIR)
      .set("spark.kubernetes.shuffle.namespace", kubernetesTestComponents.namespace)
      .set("spark.dynamicAllocation.enabled", "true")
      .set("spark.shuffle.service.enabled", "true")
      .set("spark.dynamicAllocation.maxExecutors", "4")
      .set("spark.kubernetes.shuffle.labels", "app=k8s-shuffle-service")
    runSparkApplicationAndVerifyCompletion(
      containerLocalSparkDistroExamplesJar,
      SPARK_GROUPBY_MAIN_CLASS,
      Seq("RESULT: "),
      Array("10", "400000", "2"),
      doBasicDriverPodCheck,
      doBasicExecutorPodCheck,
      appLocator,
      isJVM = true
    )
  }
}

private[spark] object DynamicAllocationTestSuite {
  val SPARK_GROUPBY_MAIN_CLASS = "org.apache.spark.examples.SimpleSkewedGroupByTest"
  val SHUFFLE_SERVICE_NAME = "k8s-external-shuffle-service"
  val VOLUME_NAME = "shuffle-dir"
  val EXECUTOR_LOCAL_DIR = "/tmp/spark-local"
  val SHUFFLE_SERVICE_LABEL: util.Map[String, String] = Map("app" -> "k8s-shuffle-service").asJava
}
