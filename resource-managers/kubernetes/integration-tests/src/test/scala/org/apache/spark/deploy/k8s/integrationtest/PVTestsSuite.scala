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

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.storage.StorageClassBuilder
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Milliseconds, Span}

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite._
import org.apache.spark.deploy.k8s.integrationtest.backend.minikube.Minikube
import org.apache.spark.util.{Utils => SparkUtils}

private[spark] trait PVTestsSuite { k8sSuite: KubernetesSuite =>
  import PVTestsSuite._
  var mountProcess: Option[Process] = None

  private def setupLocalStorage(): Unit = {
    // Mount our host path into the VM
    mountProcess = Some(Minikube.mount(HOST_PATH, VM_PATH))

    val scBuilder = new StorageClassBuilder()
      .withKind("StorageClass")
      .withApiVersion("storage.k8s.io/v1")
      .withNewMetadata()
        .withName(STORAGE_NAME)
      .endMetadata()
      .withProvisioner("kubernetes.io/no-provisioner")
      .withVolumeBindingMode("WaitForFirstConsumer")

    val pvBuilder = new PersistentVolumeBuilder()
      .withKind("PersistentVolume")
      .withApiVersion("v1")
      .withNewMetadata()
        .withName(PV_NAME)
      .endMetadata()
      // TODO(SPARK-29076): Validate with docker 4 desktop
      .withNewSpec()
        .withCapacity(Map("storage" -> new QuantityBuilder().withAmount("1Gi").build()).asJava)
        .withAccessModes("ReadWriteOnce")
        .withPersistentVolumeReclaimPolicy("Retain")
        .withStorageClassName(STORAGE_NAME)
        .withNewHostPath().withPath(VM_PATH).endHostPath()
      .endSpec()

    val pvcBuilder = new PersistentVolumeClaimBuilder()
      .withKind("PersistentVolumeClaim")
      .withApiVersion("v1")
      .withNewMetadata()
        .withName(PVC_NAME)
      .endMetadata()
      .withNewSpec()
        .withAccessModes("ReadWriteOnce")
        .withStorageClassName(STORAGE_NAME)
        .withResources(new ResourceRequirementsBuilder()
        .withRequests(Map("storage" -> new QuantityBuilder()
          .withAmount("1Gi").build()).asJava).build())
      .endSpec()

    kubernetesTestComponents
      .kubernetesClient
      .storage()
      .storageClasses()
      .create(scBuilder.build())

    kubernetesTestComponents
      .kubernetesClient
      .persistentVolumes()
      .create(pvBuilder.build())

    kubernetesTestComponents
      .kubernetesClient
      .persistentVolumeClaims()
      .create(pvcBuilder.build())
  }

  private def deleteLocalStorage(): Unit = {
    kubernetesTestComponents
      .kubernetesClient
      .persistentVolumeClaims()
      .withName(PVC_NAME)
      .delete()

    kubernetesTestComponents
      .kubernetesClient
      .persistentVolumes()
      .withName(PV_NAME)
      .delete()

    kubernetesTestComponents
      .kubernetesClient
      .storage()
      .storageClasses()
      .withName(STORAGE_NAME)
      .delete()

    mountProcess.foreach(_.destroy())

  }

  private def checkPVs(pod: Pod, file: String) = {
    implicit val podName: String = pod.getMetadata.getName
    implicit val components: KubernetesTestComponents = kubernetesTestComponents
    Eventually.eventually(TIMEOUT, INTERVAL) {
      Utils.executeCommand("ls", s"$CONTAINER_MOUNT_PATH/")
    }
    Eventually.eventually(TIMEOUT, INTERVAL) {
      try {
        val contents = Utils.executeCommand("cat", s"$CONTAINER_MOUNT_PATH/$file")
        contents.toString.trim should equal (FILE_CONTENTS.trim)
      } catch {
        case e: Exception =>
          logDebug(e.toString())
          throw e
      }
    }
    logDebug("PV ok!")
  }

  test("PVs with local storage", k8sTestTag, MinikubeTag) {
    sparkAppConf
      .set(s"spark.kubernetes.driver.volumes.persistentVolumeClaim.data.mount.path",
        CONTAINER_MOUNT_PATH)
      .set(s"spark.kubernetes.driver.volumes.persistentVolumeClaim.data.options.claimName",
        PVC_NAME)
      .set(s"spark.kubernetes.executor.volumes.persistentVolumeClaim.data.mount.path",
        CONTAINER_MOUNT_PATH)
      .set(s"spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.claimName",
        PVC_NAME)
    val file = Utils.createTempFile(FILE_CONTENTS, HOST_PATH)
    try {
      setupLocalStorage()
      runDFSReadWriteAndVerifyCompletion(
        FILE_CONTENTS.split(" ").length,
        driverPodChecker = (driverPod: Pod) => {
          doBasicDriverPodCheck(driverPod)
          checkPVs(driverPod, file)
        },
        executorPodChecker = (executorPod: Pod) => {
          doBasicExecutorPodCheck(executorPod)
          checkPVs(executorPod, file)
        },
        appArgs = Array(s"$CONTAINER_MOUNT_PATH/$file", s"$CONTAINER_MOUNT_PATH"),
        interval = Some(PV_TESTS_INTERVAL)
      )
    } finally {
      // make sure this always runs
      deleteLocalStorage()
    }
  }
}

private[spark] object PVTestsSuite {
  val STORAGE_NAME = "test-local-storage-2"
  val PV_NAME = "test-local-pv"
  val PVC_NAME = "test-local-pvc"
  val CONTAINER_MOUNT_PATH = "/opt/spark/pv-tests"
  // Directly mounting /tmp fails on OSX so make a sub-dir
  val host_temp_dir = {
    val dir = SparkUtils.createDirectory(System.getProperty("java.io.tmpdir"))
    dir.deleteOnExit()
    dir
  }
  val HOST_PATH = sys.env.getOrElse("PVC_TESTS_HOST_PATH", host_temp_dir.toString())
  val VM_PATH = sys.env.getOrElse("PVC_TESTS_VM_PATH", "/host-tmp")
  val FILE_CONTENTS = "test PVs"
  val PV_TESTS_INTERVAL = PatienceConfiguration.Interval(Span(10, Milliseconds))
}
