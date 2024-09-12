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

import scala.jdk.CollectionConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s._

class MountVolumesFeatureStepSuite extends SparkFunSuite {
  test("Mounts hostPath volumes") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      false,
      KubernetesHostPathVolumeConf("/hostPath/tmp")
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getHostPath.getPath === "/hostPath/tmp")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === false)
  }

  test("Mounts persistentVolumeClaims") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      true,
      KubernetesPVCVolumeConf("pvcClaim")
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val pvcClaim = configuredPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(pvcClaim.getClaimName === "pvcClaim")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly)

  }

  test("SPARK-32713 Mounts parameterized persistentVolumeClaims in executors") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      true,
      KubernetesPVCVolumeConf("pvc-spark-SPARK_EXECUTOR_ID")
    )
    val driverConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val driverStep = new MountVolumesFeatureStep(driverConf)
    val driverPod = driverStep.configurePod(SparkPod.initialPod())

    assert(driverPod.pod.getSpec.getVolumes.size() === 1)
    val driverPVC = driverPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(driverPVC.getClaimName === "pvc-spark-SPARK_EXECUTOR_ID")

    val executorConf = KubernetesTestConf.createExecutorConf(volumes = Seq(volumeConf))
    val executorStep = new MountVolumesFeatureStep(executorConf)
    val executorPod = executorStep.configurePod(SparkPod.initialPod())

    assert(executorPod.pod.getSpec.getVolumes.size() === 1)
    val executorPVC = executorPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(executorPVC.getClaimName === s"pvc-spark-${KubernetesTestConf.EXECUTOR_ID}")
  }

  test("SPARK-32713 Mounts parameterized persistentVolumeClaims in executors with storage class") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      true,
      KubernetesPVCVolumeConf("pvc-spark-SPARK_EXECUTOR_ID", Some("fast"), Some("512M"))
    )
    val driverConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val driverStep = new MountVolumesFeatureStep(driverConf)
    val driverPod = driverStep.configurePod(SparkPod.initialPod())

    assert(driverPod.pod.getSpec.getVolumes.size() === 1)
    val driverPVC = driverPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(driverPVC.getClaimName === "pvc-spark-SPARK_EXECUTOR_ID")

    val executorConf = KubernetesTestConf.createExecutorConf(volumes = Seq(volumeConf))
    val executorStep = new MountVolumesFeatureStep(executorConf)
    val executorPod = executorStep.configurePod(SparkPod.initialPod())

    assert(executorPod.pod.getSpec.getVolumes.size() === 1)
    val executorPVC = executorPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(executorPVC.getClaimName === s"pvc-spark-${KubernetesTestConf.EXECUTOR_ID}")
  }

  test("Create and mounts persistentVolumeClaims in driver") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      true,
      KubernetesPVCVolumeConf(MountVolumesFeatureStep.PVC_ON_DEMAND)
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val pvcClaim = configuredPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(pvcClaim.getClaimName.endsWith("-driver-pvc-0"))
  }

  test("Create and mounts persistentVolumeClaims in driver with labels") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      true,
      KubernetesPVCVolumeConf(MountVolumesFeatureStep.PVC_ON_DEMAND,
        labels = Map("foo" -> "bar", "env" -> "test"),
        size = Some("1Mi"),
        storageClass = Some("gp"))
    )

    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())
    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val pvcClaim = configuredPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(pvcClaim.getClaimName.endsWith("-driver-pvc-0"))
  }

  test("Create and mounts persistentVolumeClaims in executors with labels") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      true,
      KubernetesPVCVolumeConf(MountVolumesFeatureStep.PVC_ON_DEMAND,
        labels = Map("foo1" -> "bar1", "env" -> "exec-test"),
        size = Some("1Mi"),
        storageClass = Some("gp"))
    )

    val executorConf = KubernetesTestConf.createExecutorConf(volumes = Seq(volumeConf))
    val executorStep = new MountVolumesFeatureStep(executorConf)
    val executorPod = executorStep.configurePod(SparkPod.initialPod())

    assert(executorPod.pod.getSpec.getVolumes.size() === 1)
    val executorPVC = executorPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(executorPVC.getClaimName.endsWith("-exec-1-pvc-0"))
  }

  test("Mounts multiple volumes to executor with labels") {
    val pvcVolumeConf1 = KubernetesVolumeSpec(
      "checkpointVolume1",
      "/checkpoints1",
      "",
      true,
      KubernetesPVCVolumeConf("pvcClaim1",
        labels = Map("foo1" -> "bar1", "env1" -> "exec-test-1"),
        size = Some("1Mi"),
        storageClass = Some("gp"))
    )
    val pvcVolumeConf2 = KubernetesVolumeSpec(
      "checkpointVolume2",
      "/checkpoints2",
      "",
      true,
      KubernetesPVCVolumeConf("pvcClaim2",
        labels = Map("foo2" -> "bar2", "env2" -> "exec-test-2"),
        size = Some("1Mi"),
        storageClass = Some("gp"))
    )

    val kubernetesConf = KubernetesTestConf.createExecutorConf(
      volumes = Seq(pvcVolumeConf1, pvcVolumeConf2))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 2)
    assert(configuredPod.container.getVolumeMounts.size() === 2)
  }

  test("Create and mount persistentVolumeClaims in executors") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      true,
      KubernetesPVCVolumeConf(MountVolumesFeatureStep.PVC_ON_DEMAND)
    )
    val executorConf = KubernetesTestConf.createExecutorConf(volumes = Seq(volumeConf))
    val executorStep = new MountVolumesFeatureStep(executorConf)
    val executorPod = executorStep.configurePod(SparkPod.initialPod())

    assert(executorPod.pod.getSpec.getVolumes.size() === 1)
    val executorPVC = executorPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(executorPVC.getClaimName.endsWith("-exec-1-pvc-0"))
  }

  test("Mounts emptyDir") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      false,
      KubernetesEmptyDirVolumeConf(Some("Memory"), Some("6G"))
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val emptyDir = configuredPod.pod.getSpec.getVolumes.get(0).getEmptyDir
    assert(emptyDir.getMedium === "Memory")
    assert(emptyDir.getSizeLimit.getAmount ===  "6")
    assert(emptyDir.getSizeLimit.getFormat === "G")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === false)
  }

  test("Mounts emptyDir with no options") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      false,
      KubernetesEmptyDirVolumeConf(None, None)
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val emptyDir = configuredPod.pod.getSpec.getVolumes.get(0).getEmptyDir
    assert(emptyDir.getMedium === "")
    assert(emptyDir.getSizeLimit === null)
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === false)
  }

  test("Mounts read/write nfs volumes") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      false,
      KubernetesNFSVolumeConf("/share/name", "nfs.example.com")
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getNfs.getPath === "/share/name")
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getNfs.getReadOnly === null)
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getNfs.getServer === "nfs.example.com")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === false)
  }

  test("Mounts read-only nfs volumes") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "",
      true,
      KubernetesNFSVolumeConf("/share/name", "nfs.example.com")
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getNfs.getPath === "/share/name")
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getNfs.getReadOnly === null)
    assert(configuredPod.pod.getSpec.getVolumes.get(0).getNfs.getServer === "nfs.example.com")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    assert(configuredPod.container.getVolumeMounts.get(0).getMountPath === "/tmp")
    assert(configuredPod.container.getVolumeMounts.get(0).getName === "testVolume")
    assert(configuredPod.container.getVolumeMounts.get(0).getReadOnly === true)
  }

  test("Mounts multiple volumes") {
    val hpVolumeConf = KubernetesVolumeSpec(
      "hpVolume",
      "/tmp",
      "",
      false,
      KubernetesHostPathVolumeConf("/hostPath/tmp")
    )
    val pvcVolumeConf = KubernetesVolumeSpec(
      "checkpointVolume",
      "/checkpoints",
      "",
      true,
      KubernetesPVCVolumeConf("pvcClaim")
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(
      volumes = Seq(hpVolumeConf, pvcVolumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 2)
    assert(configuredPod.container.getVolumeMounts.size() === 2)
  }

  test("mountPath should be unique") {
    val hpVolumeConf = KubernetesVolumeSpec(
      "hpVolume",
      "/data",
      "",
      false,
      KubernetesHostPathVolumeConf("/hostPath/tmp")
    )
    val pvcVolumeConf = KubernetesVolumeSpec(
      "checkpointVolume",
      "/data",
      "",
      true,
      KubernetesPVCVolumeConf("pvcClaim")
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(
      volumes = Seq(hpVolumeConf, pvcVolumeConf))

    val step = new MountVolumesFeatureStep(kubernetesConf)
    val m = intercept[IllegalArgumentException] {
      step.configurePod(SparkPod.initialPod())
    }.getMessage
    assert(m.contains("Found duplicated mountPath: '/data'"))
  }

  test("Mounts subpath on emptyDir") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "foo",
      false,
      KubernetesEmptyDirVolumeConf(None, None)
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val emptyDirMount = configuredPod.container.getVolumeMounts.get(0)
    assert(emptyDirMount.getMountPath === "/tmp")
    assert(emptyDirMount.getName === "testVolume")
    assert(emptyDirMount.getSubPath === "foo")
  }

  test("Mounts subpath on persistentVolumeClaims") {
    val volumeConf = KubernetesVolumeSpec(
      "testVolume",
      "/tmp",
      "bar",
      true,
      KubernetesPVCVolumeConf("pvcClaim")
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(volumeConf))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 1)
    val pvcClaim = configuredPod.pod.getSpec.getVolumes.get(0).getPersistentVolumeClaim
    assert(pvcClaim.getClaimName === "pvcClaim")
    assert(configuredPod.container.getVolumeMounts.size() === 1)
    val pvcMount = configuredPod.container.getVolumeMounts.get(0)
    assert(pvcMount.getMountPath === "/tmp")
    assert(pvcMount.getName === "testVolume")
    assert(pvcMount.getSubPath === "bar")
  }

  test("Mounts multiple subpaths") {
    val volumeConf = KubernetesEmptyDirVolumeConf(None, None)
    val emptyDirSpec = KubernetesVolumeSpec(
      "testEmptyDir",
      "/tmp/foo",
      "foo",
      true,
      KubernetesEmptyDirVolumeConf(None, None)
    )
    val pvcSpec = KubernetesVolumeSpec(
      "testPVC",
      "/tmp/bar",
      "bar",
      true,
      KubernetesEmptyDirVolumeConf(None, None)
    )
    val kubernetesConf = KubernetesTestConf.createDriverConf(volumes = Seq(emptyDirSpec, pvcSpec))
    val step = new MountVolumesFeatureStep(kubernetesConf)
    val configuredPod = step.configurePod(SparkPod.initialPod())

    assert(configuredPod.pod.getSpec.getVolumes.size() === 2)
    val mounts = configuredPod.container.getVolumeMounts.asScala.sortBy(_.getName())
    assert(mounts.size === 2)
    assert(mounts(0).getName === "testEmptyDir")
    assert(mounts(0).getMountPath === "/tmp/foo")
    assert(mounts(0).getSubPath === "foo")
    assert(mounts(1).getName === "testPVC")
    assert(mounts(1).getMountPath === "/tmp/bar")
    assert(mounts(1).getSubPath === "bar")
  }
}
