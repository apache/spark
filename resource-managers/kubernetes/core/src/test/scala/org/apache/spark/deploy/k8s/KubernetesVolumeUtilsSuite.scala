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
package org.apache.spark.deploy.k8s

import org.apache.spark.{SparkConf, SparkFunSuite}

class KubernetesVolumeUtilsSuite extends SparkFunSuite {
  test("Parses hostPath volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.hostPath.volumeName.mount.path", "/path")
    sparkConf.set("test.hostPath.volumeName.mount.readOnly", "true")
    sparkConf.set("test.hostPath.volumeName.options.path", "/hostPath")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesHostPathVolumeConf] ===
      KubernetesHostPathVolumeConf("/hostPath"))
  }

  test("Parses subPath correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.emptyDir.volumeName.mount.path", "/path")
    sparkConf.set("test.emptyDir.volumeName.mount.readOnly", "true")
    sparkConf.set("test.emptyDir.volumeName.mount.subPath", "subPath")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountSubPath === "subPath")
  }

  test("Parses persistentVolumeClaim volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.path", "/path")
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.readOnly", "true")
    sparkConf.set("test.persistentVolumeClaim.volumeName.options.claimName", "claimName")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesPVCVolumeConf] ===
      KubernetesPVCVolumeConf("claimName"))
  }

  test("Parses persistentVolumeClaim volumes correctly with labels") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.path", "/path")
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.readOnly", "true")
    sparkConf.set("test.persistentVolumeClaim.volumeName.options.claimName", "claimName")
    sparkConf.set("test.label.persistentVolumeClaim.volumeName.env", "test")
    sparkConf.set("test.label.persistentVolumeClaim.volumeName.foo", "bar")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesPVCVolumeConf] ===
      KubernetesPVCVolumeConf(claimName = "claimName",
        labels = Map("env" -> "test", "foo" -> "bar")))
  }

  test("Parses persistentVolumeClaim volumes & puts labels as empty Map if not provided") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.path", "/path")
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.readOnly", "true")
    sparkConf.set("test.persistentVolumeClaim.volumeName.options.claimName", "claimName")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesPVCVolumeConf] ===
      KubernetesPVCVolumeConf(claimName = "claimName", labels = Map()))
  }

  test("Parses emptyDir volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.emptyDir.volumeName.mount.path", "/path")
    sparkConf.set("test.emptyDir.volumeName.mount.readOnly", "true")
    sparkConf.set("test.emptyDir.volumeName.options.medium", "medium")
    sparkConf.set("test.emptyDir.volumeName.options.sizeLimit", "5G")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesEmptyDirVolumeConf] ===
      KubernetesEmptyDirVolumeConf(Some("medium"), Some("5G")))
  }

  test("Parses emptyDir volume options can be optional") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.emptyDir.volumeName.mount.path", "/path")
    sparkConf.set("test.emptyDir.volumeName.mount.readOnly", "true")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesEmptyDirVolumeConf] ===
      KubernetesEmptyDirVolumeConf(None, None))
  }

  test("Defaults optional readOnly to false") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.hostPath.volumeName.mount.path", "/path")
    sparkConf.set("test.hostPath.volumeName.options.path", "/hostPath")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.mountReadOnly === false)
  }

  test("Fails on missing mount key") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.emptyDir.volumeName.mnt.path", "/path")

    val e = intercept[NoSuchElementException] {
      KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.")
    }
    assert(e.getMessage.contains("emptyDir.volumeName.mount.path"))
  }

  test("Fails on missing option key") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.hostPath.volumeName.mount.path", "/path")
    sparkConf.set("test.hostPath.volumeName.mount.readOnly", "true")
    sparkConf.set("test.hostPath.volumeName.options.pth", "/hostPath")

    val e = intercept[NoSuchElementException] {
      KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.")
    }
    assert(e.getMessage.contains("hostPath.volumeName.options.path"))
  }

  test("SPARK-33063: Fails on missing option key in persistentVolumeClaim") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.path", "/path")
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.readOnly", "true")

    val e = intercept[NoSuchElementException] {
      KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.")
    }
    assert(e.getMessage.contains("persistentVolumeClaim.volumeName.options.claimName"))
  }

  test("Parses read-only nfs volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.nfs.volumeName.mount.path", "/path")
    sparkConf.set("test.nfs.volumeName.mount.readOnly", "true")
    sparkConf.set("test.nfs.volumeName.options.path", "/share")
    sparkConf.set("test.nfs.volumeName.options.server", "nfs.example.com")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly === true)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesNFSVolumeConf] ===
      KubernetesNFSVolumeConf("/share", "nfs.example.com"))
  }

  test("Parses read/write nfs volumes correctly") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.nfs.volumeName.mount.path", "/path")
    sparkConf.set("test.nfs.volumeName.mount.readOnly", "false")
    sparkConf.set("test.nfs.volumeName.options.path", "/share")
    sparkConf.set("test.nfs.volumeName.options.server", "nfs.example.com")

    val volumeSpec = KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.").head
    assert(volumeSpec.volumeName === "volumeName")
    assert(volumeSpec.mountPath === "/path")
    assert(volumeSpec.mountReadOnly === false)
    assert(volumeSpec.volumeConf.asInstanceOf[KubernetesNFSVolumeConf] ===
      KubernetesNFSVolumeConf("/share", "nfs.example.com"))
  }

  test("Fails on missing path option") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.nfs.volumeName.mount.path", "/path")
    sparkConf.set("test.nfs.volumeName.mount.readOnly", "true")
    sparkConf.set("test.nfs.volumeName.options.server", "nfs.example.com")

    val e = intercept[NoSuchElementException] {
      KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.")
    }
    assert(e.getMessage.contains("nfs.volumeName.options.path"))
  }

  test("Fails on missing server option") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.nfs.volumeName.mount.path", "/path")
    sparkConf.set("test.nfs.volumeName.mount.readOnly", "true")
    sparkConf.set("test.nfs.volumeName.options.path", "/share")

    val e = intercept[NoSuchElementException] {
      KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.")
    }
    assert(e.getMessage.contains("nfs.volumeName.options.server"))
  }

  test("SPARK-47003: Check emptyDir volume size") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.emptyDir.volumeName.mount.path", "/path")
    sparkConf.set("test.emptyDir.volumeName.mount.readOnly", "true")
    sparkConf.set("test.emptyDir.volumeName.options.medium", "medium")
    sparkConf.set("test.emptyDir.volumeName.options.sizeLimit", "5")

    val m = intercept[IllegalArgumentException] {
      KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.")
    }.getMessage
    assert(m.contains("smaller than 1KiB. Missing units?"))
  }

  test("SPARK-47003: Check persistentVolumeClaim volume size") {
    val sparkConf = new SparkConf(false)
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.path", "/path")
    sparkConf.set("test.persistentVolumeClaim.volumeName.mount.readOnly", "false")
    sparkConf.set("test.persistentVolumeClaim.volumeName.options.claimName", "claimName")
    sparkConf.set("test.persistentVolumeClaim.volumeName.options.sizeLimit", "1000")

    val m = intercept[IllegalArgumentException] {
      KubernetesVolumeUtils.parseVolumesWithPrefix(sparkConf, "test.")
    }.getMessage
    assert(m.contains("smaller than 1KiB. Missing units?"))
  }
}
