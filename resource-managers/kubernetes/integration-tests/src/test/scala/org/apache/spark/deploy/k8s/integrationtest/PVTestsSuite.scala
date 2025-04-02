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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.{Milliseconds, Span}

import org.apache.spark.deploy.k8s.integrationtest.KubernetesSuite._
import org.apache.spark.deploy.k8s.integrationtest.backend.minikube.MinikubeTestBackend

private[spark] trait PVTestsSuite { k8sSuite: KubernetesSuite =>
  import PVTestsSuite._

  private def setupLocalStorage(): Unit = {
    val storageClassName = if (testBackend == MinikubeTestBackend) "standard" else "hostpath"
    val hostname = if (testBackend == MinikubeTestBackend) "minikube" else "docker-desktop"
    val pvBuilder = new PersistentVolumeBuilder()
      .withKind("PersistentVolume")
      .withApiVersion("v1")
      .withNewMetadata()
        .withName("test-local-pv")
      .endMetadata()
      .withNewSpec()
        .withCapacity(Map("storage" -> new Quantity("1Gi")).asJava)
        .withAccessModes("ReadWriteOnce")
        .withPersistentVolumeReclaimPolicy("Retain")
        .withStorageClassName(storageClassName)
        .withLocal(new LocalVolumeSourceBuilder().withPath(VM_PATH).build())
          .withNewNodeAffinity()
            .withNewRequired()
              .withNodeSelectorTerms(new NodeSelectorTermBuilder()
                .withMatchExpressions(new NodeSelectorRequirementBuilder()
                  .withKey("kubernetes.io/hostname")
                  .withOperator("In")
                  .withValues(hostname)
                  .build()).build())
            .endRequired()
          .endNodeAffinity()
      .endSpec()

    val pvcBuilder = new PersistentVolumeClaimBuilder()
      .withKind("PersistentVolumeClaim")
      .withApiVersion("v1")
      .withNewMetadata()
        .withName(PVC_NAME)
      .endMetadata()
      .withNewSpec()
        .withAccessModes("ReadWriteOnce")
        .withStorageClassName(storageClassName)
        .withResources(new VolumeResourceRequirementsBuilder()
          .withRequests(Map("storage" -> new Quantity("1Gi")).asJava).build())
      .endSpec()

    kubernetesTestComponents
      .kubernetesClient
      .persistentVolumes()
      .create(pvBuilder.build())

    kubernetesTestComponents
      .kubernetesClient
      .persistentVolumeClaims()
      .inNamespace(kubernetesTestComponents.namespace)
      .create(pvcBuilder.build())
  }

  private def deleteLocalStorage(): Unit = {
    kubernetesTestComponents
      .kubernetesClient
      .persistentVolumeClaims()
      .inNamespace(kubernetesTestComponents.namespace)
      .withName(PVC_NAME)
      .delete()

    kubernetesTestComponents
      .kubernetesClient
      .persistentVolumes()
      .withName(PV_NAME)
      .delete()
  }

  private def checkPVs(pod: Pod, file: String) = {
    Eventually.eventually(TIMEOUT, INTERVAL) {
      implicit val podName: String = pod.getMetadata.getName
      implicit val components: KubernetesTestComponents = kubernetesTestComponents
      val contents = Utils.executeCommand("cat", s"$CONTAINER_MOUNT_PATH/$file")
      assert(contents.toString.trim.equals(FILE_CONTENTS))
    }
  }

  test("PVs with local hostpath storage on statefulsets", k8sTestTag, pvTestTag) {
    assume(this.getClass.getSimpleName == "KubernetesSuite")
    sparkAppConf
      .set(s"spark.kubernetes.driver.volumes.persistentVolumeClaim.data.mount.path",
        CONTAINER_MOUNT_PATH)
      .set(s"spark.kubernetes.driver.volumes.persistentVolumeClaim.data.options.claimName",
        PVC_NAME)
      .set(s"spark.kubernetes.executor.volumes.persistentVolumeClaim.data.mount.path",
        CONTAINER_MOUNT_PATH)
      .set(s"spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.claimName",
        PVC_NAME)
      .set("spark.kubernetes.allocation.pods.allocator", "statefulset")
    val file = Utils.createTempFile(FILE_CONTENTS, HOST_PATH)
    try {
      setupLocalStorage()
      runMiniReadWriteAndVerifyCompletion(
        FILE_CONTENTS.split(" ").length,
        driverPodChecker = (driverPod: Pod) => {
          doBasicDriverPodCheck(driverPod)
        },
        executorPodChecker = (executorPod: Pod) => {
          doBasicExecutorPodCheck(executorPod)
        },
        appArgs = Array(s"$CONTAINER_MOUNT_PATH/$file"),
        interval = Some(PV_TESTS_INTERVAL)
      )
    } finally {
      // make sure this always runs
      deleteLocalStorage()
    }
  }

  ignore("PVs with local hostpath and storageClass on statefulsets", k8sTestTag, pvTestTag) {
    assume(this.getClass.getSimpleName == "KubernetesSuite")
    sparkAppConf
      .set(s"spark.kubernetes.driver.volumes.persistentVolumeClaim.data.mount.path",
        CONTAINER_MOUNT_PATH)
      .set(s"spark.kubernetes.driver.volumes.persistentVolumeClaim.data.options.claimName",
        PVC_NAME)
      .set(s"spark.kubernetes.executor.volumes.persistentVolumeClaim.data.mount.path",
        CONTAINER_MOUNT_PATH)
      .set(s"spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.claimName",
        PVC_NAME + "OnDemand")
      .set(s"spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.storageClass",
        "standard")
      .set(s"spark.kubernetes.executor.volumes.persistentVolumeClaim.data.options.sizeLimit", "1G")
      .set("spark.kubernetes.allocation.pods.allocator", "statefulset")
    val file = Utils.createTempFile(FILE_CONTENTS, HOST_PATH)
    try {
      setupLocalStorage()
      runMiniReadWriteAndVerifyCompletion(
        FILE_CONTENTS.split(" ").length,
        driverPodChecker = (driverPod: Pod) => {
          doBasicDriverPodCheck(driverPod)
        },
        executorPodChecker = (executorPod: Pod) => {
          doBasicExecutorPodCheck(executorPod)
        },
        appArgs = Array(s"$CONTAINER_MOUNT_PATH/$file"),
        interval = Some(PV_TESTS_INTERVAL)
      )
    } finally {
      // make sure this always runs
      deleteLocalStorage()
    }
  }

  test("PVs with local storage", k8sTestTag, pvTestTag) {
    assume(this.getClass.getSimpleName == "KubernetesSuite")
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
  val PV_NAME = "test-local-pv"
  val PVC_NAME = "test-local-pvc"
  val CONTAINER_MOUNT_PATH = "/opt/spark/pv-tests"
  val HOST_PATH = sys.env.getOrElse("PVC_TESTS_HOST_PATH", "/tmp")
  val VM_PATH = sys.env.getOrElse("PVC_TESTS_VM_PATH", "/tmp")
  val FILE_CONTENTS = "test PVs"
  val PV_TESTS_INTERVAL = PatienceConfiguration.Interval(Span(10, Milliseconds))
}
