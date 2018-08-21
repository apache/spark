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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s._

class MountVolumesFeatureStepSuite extends SparkFunSuite {
  private val sparkConf = new SparkConf(false)
  private val emptyKubernetesConf = KubernetesConf(
    sparkConf = sparkConf,
    roleSpecificConf = KubernetesDriverSpecificConf(
      None,
      "app-name",
      "main",
      Seq.empty),
    appResourceNamePrefix = "resource",
    appId = "app-id",
    roleLabels = Map.empty,
    roleAnnotations = Map.empty,
    roleSecretNamesToMountPaths = Map.empty,
    roleSecretEnvNamesToKeyRefs = Map.empty,
    roleEnvs = Map.empty,
    roleVolumes = Nil,
    sparkFiles = Nil)

  test("Mounts hostPath volumes") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      false,
      KubernetesHostPathVolumeConf("/hostPath/tmp")
    )
    val kubernetesConf = emptyKubernetesConf.copy(roleVolumes = volumeConf :: Nil)
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getHostPath.getPath === "/hostPath/tmp")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === false)
  }

  test("Mounts pesistentVolumeClaims") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      true,
      KubernetesPVCVolumeConf("pvcClaim")
    )
    val kubernetesConf = emptyKubernetesConf.copy(roleVolumes = volumeConf :: Nil)
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val pvcClaim = configuredPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(pvcClaim.getClaimName === "pvcClaim")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === true)

  }

  test("Mounts emptyDir") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      false,
      KubernetesEmptyDirVolumeConf(Some("Memory"), Some("6G"))
    )
    val kubernetesConf = emptyKubernetesConf.copy(roleVolumes = volumeConf :: Nil)
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val emptyDir = configuredPod.pod.getSpec.getVolumes.get(0).getEmptyDir
    assert(emptyDir.getMedium === "Memory")
    assert(emptyDir.getSizeLimit.getAmount === "6G")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === false)
  }

  test("Mounts emptyDir with no options") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      false,
      KubernetesEmptyDirVolumeConf(None, None)
    )
    val kubernetesConf = emptyKubernetesConf.copy(roleVolumes = volumeConf :: Nil)
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val emptyDir = configuredPod.pod.getSpec.getVolumes.get(0).getEmptyDir
    assert(emptyDir.getMedium === "")
    assert(emptyDir.getSizeLimit.getAmount === null)
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === false)
  }

  test("Mounts multiple volumes") {
    val hpVolumeConf = KubernetesVolumeSpec(
      "hpVolume",
      "/tmp",
      false,
      KubernetesHostPathVolumeConf("/hostPath/tmp")
    )
    val pvcVolumeConf = KubernetesVolumeSpec(
      "checkpointVolume",
      "/checkpoints",
      true,
      KubernetesPVCVolumeConf("pvcClaim")
    )
    val volumesConf = hpVolumeConf :: pvcVolumeConf :: Nil
    val kubernetesConf = emptyKubernetesConf.copy(roleVolumes = volumesConf)
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 2)
    assert(configuredPod.container.getVolumeMounts.size() === 2)
  }
}
