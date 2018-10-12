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
package org.apache.spark.deploy.k8s.features

import io.fabric8.kubernetes.api.model.{EnvVarBuilder, VolumeBuilder, VolumeMountBuilder}
import org.mockito.Mockito
import org.scalatest._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, KubernetesRoleSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._

class LocalDirsFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {
  private val defaultLocalDir = "/var/data/default-local-dir"
  private var sparkConf: SparkConf = _
  private var kubernetesConf: KubernetesConf[_ <: KubernetesRoleSpecificConf] = _

  before {
    val realSparkConf = new SparkConf(false)
    sparkConf = Mockito.spy(realSparkConf)
    kubernetesConf = KubernetesConf(
      sparkConf,
      KubernetesDriverSpecificConf(
        None,
        "app-name",
        "main",
        Seq.empty),
      "resource",
      "app-id",
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      Seq.empty[String])
  }

  test("Resolve to default local dir if neither env nor configuration are set") {
    Mockito.doReturn(null).when(sparkConf).get("spark.local.dir")
    Mockito.doReturn(null).when(sparkConf).getenv("SPARK_LOCAL_DIRS")
    val stepUnderTest = new LocalDirsFeatureStep(kubernetesConf, defaultLocalDir)
    val configuredPod = stepUnderTest.configurePod(SparkPod.initialPod())
    assert(configuredPod.pod.getSpec.getVolumes.size === 1)
    assert(configuredPod.pod.getSpec.getVolumes.get(0) ===
      new VolumeBuilder()
        .withName(s"spark-local-dir-1")
        .withNewEmptyDir()
        .endEmptyDir()
        .build())
    assert(configuredPod.container.getVolumeMounts.size === 1)
    assert(configuredPod.container.getVolumeMounts.get(0) ===
      new VolumeMountBuilder()
        .withName(s"spark-local-dir-1")
        .withMountPath(defaultLocalDir)
        .build())
    assert(configuredPod.container.getEnv.size === 1)
    assert(configuredPod.container.getEnv.get(0) ===
      new EnvVarBuilder()
        .withName("SPARK_LOCAL_DIRS")
        .withValue(defaultLocalDir)
        .build())
  }

  test("Use configured local dirs split on comma if provided.") {
    Mockito.doReturn("/var/data/my-local-dir-1,/var/data/my-local-dir-2")
      .when(sparkConf).getenv("SPARK_LOCAL_DIRS")
    val stepUnderTest = new LocalDirsFeatureStep(kubernetesConf, defaultLocalDir)
    val configuredPod = stepUnderTest.configurePod(SparkPod.initialPod())
    assert(configuredPod.pod.getSpec.getVolumes.size === 2)
    assert(configuredPod.pod.getSpec.getVolumes.get(0) ===
      new VolumeBuilder()
        .withName(s"spark-local-dir-1")
        .withNewEmptyDir()
        .endEmptyDir()
        .build())
    assert(configuredPod.pod.getSpec.getVolumes.get(1) ===
      new VolumeBuilder()
        .withName(s"spark-local-dir-2")
        .withNewEmptyDir()
        .endEmptyDir()
        .build())
    assert(configuredPod.container.getVolumeMounts.size === 2)
    assert(configuredPod.container.getVolumeMounts.get(0) ===
      new VolumeMountBuilder()
        .withName(s"spark-local-dir-1")
        .withMountPath("/var/data/my-local-dir-1")
        .build())
    assert(configuredPod.container.getVolumeMounts.get(1) ===
      new VolumeMountBuilder()
        .withName(s"spark-local-dir-2")
        .withMountPath("/var/data/my-local-dir-2")
        .build())
    assert(configuredPod.container.getEnv.size === 1)
    assert(configuredPod.container.getEnv.get(0) ===
      new EnvVarBuilder()
        .withName("SPARK_LOCAL_DIRS")
        .withValue("/var/data/my-local-dir-1,/var/data/my-local-dir-2")
        .build())
  }

  test("Use tmpfs to back default local dir") {
    Mockito.doReturn(null).when(sparkConf).get("spark.local.dir")
    Mockito.doReturn(null).when(sparkConf).getenv("SPARK_LOCAL_DIRS")
    Mockito.doReturn(true).when(sparkConf).get(KUBERNETES_LOCAL_DIRS_TMPFS)
    val stepUnderTest = new LocalDirsFeatureStep(kubernetesConf, defaultLocalDir)
    val configuredPod = stepUnderTest.configurePod(SparkPod.initialPod())
    assert(configuredPod.pod.getSpec.getVolumes.size === 1)
    assert(configuredPod.pod.getSpec.getVolumes.get(0) ===
      new VolumeBuilder()
        .withName(s"spark-local-dir-1")
        .withNewEmptyDir()
          .withMedium("Memory")
        .endEmptyDir()
        .build())
    assert(configuredPod.container.getVolumeMounts.size === 1)
    assert(configuredPod.container.getVolumeMounts.get(0) ===
      new VolumeMountBuilder()
        .withName(s"spark-local-dir-1")
        .withMountPath(defaultLocalDir)
        .build())
    assert(configuredPod.container.getEnv.size === 1)
    assert(configuredPod.container.getEnv.get(0) ===
      new EnvVarBuilder()
        .withName("SPARK_LOCAL_DIRS")
        .withValue(defaultLocalDir)
        .build())
  }
}
