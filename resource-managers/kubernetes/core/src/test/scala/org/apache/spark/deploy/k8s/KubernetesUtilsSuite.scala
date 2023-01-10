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

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.{ContainerBuilder, EnvVarBuilder, EnvVarSourceBuilder, HasMetadata, PersistentVolumeClaim, PersistentVolumeClaimBuilder, PodBuilder}
import io.fabric8.kubernetes.api.model.apiextensions.v1.CustomResourceDefinitionBuilder
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.{BeforeAndAfter, PrivateMethodTester}

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.Fabric8Aliases.{PERSISTENT_VOLUME_CLAIMS, PVC_WITH_NAMESPACE, RESOURCE_LIST}

class KubernetesUtilsSuite extends SparkFunSuite with PrivateMethodTester with BeforeAndAfter {
  private val HOST = "test-host"
  private val NAMESPACE = "test-namespace"
  private val POD_UID = "pod-id"
  private val POD_API_VERSION = "v1"
  private val POD_KIND = "pod"
  private val POD = new PodBuilder()
    .withNewSpec()
    .withHostname(HOST)
    .withContainers(
      new ContainerBuilder().withName("first").build(),
      new ContainerBuilder().withName("second").build())
    .endSpec()
    .build()
  private val EXECUTOR_POD = new PodBuilder(POD)
    .withNewMetadata()
      .withName("executor")
      .withUid("executor-" + POD_UID)
    .endMetadata()
    .withApiVersion(POD_API_VERSION)
    .withKind(POD_KIND)
    .build()
  private val PVC = new PersistentVolumeClaimBuilder()
    .withNewMetadata()
      .withName("test-pvc")
      .endMetadata()
    .build()
  private val CRD = new CustomResourceDefinitionBuilder()
    .withNewMetadata()
      .withName("test-crd")
      .endMetadata()
    .build()
  private val PVC_WITH_OWNER_REFERENCES = new PersistentVolumeClaimBuilder(PVC)
    .editMetadata()
      .addNewOwnerReference()
        .withName("executor")
        .withApiVersion(POD_API_VERSION)
        .withUid("executor-" + POD_UID)
        .withKind(POD_KIND)
        .withController(true)
        .endOwnerReference()
      .endMetadata()
    .build()
  private val CRD_WITH_OWNER_REFERENCES = new CustomResourceDefinitionBuilder(CRD)
    .editMetadata()
      .addNewOwnerReference()
        .withName("executor")
        .withApiVersion(POD_API_VERSION)
        .withUid("executor-" + POD_UID)
        .withKind(POD_KIND)
        .withController(true)
        .endOwnerReference()
      .endMetadata()
    .build()

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var persistentVolumeClaims: PERSISTENT_VOLUME_CLAIMS = _

  @Mock
  private var pvcWithNamespace: PVC_WITH_NAMESPACE = _

  @Mock
  private var pvcResource: io.fabric8.kubernetes.client.dsl.Resource[PersistentVolumeClaim] = _

  @Mock
  private var resourceList: RESOURCE_LIST = _

  private var createdPvcArgumentCaptor: ArgumentCaptor[PersistentVolumeClaim] = _
  private var createdResourcesArgumentCaptor: ArgumentCaptor[HasMetadata] = _

  before {
    MockitoAnnotations.openMocks(this).close()
    when(kubernetesClient.persistentVolumeClaims()).thenReturn(persistentVolumeClaims)
    when(persistentVolumeClaims.inNamespace(any())).thenReturn(pvcWithNamespace)
    when(pvcWithNamespace.resource(any())).thenReturn(pvcResource)
    when(pvcWithNamespace.withName(any())).thenReturn(pvcResource)
    when(kubernetesClient.resourceList(any(classOf[HasMetadata]))).thenReturn(resourceList)
    createdPvcArgumentCaptor = ArgumentCaptor.forClass(classOf[PersistentVolumeClaim])
    createdResourcesArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
  }

  test("Selects the given container as spark container.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Some("second"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("first"))
    assert(sparkPod.container.getName == "second")
  }

  test("Selects the first container if no container name is given.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Option.empty)
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("second"))
    assert(sparkPod.container.getName == "first")
  }

  test("Falls back to the first container if given container name does not exist.") {
    val sparkPod = KubernetesUtils.selectSparkContainer(POD, Some("does-not-exist"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.pod.getSpec.getContainers.asScala.toList.map(_.getName) == List("second"))
    assert(sparkPod.container.getName == "first")
  }

  test("constructs spark pod correctly with pod template with no containers") {
    val noContainersPod = new PodBuilder(POD).editSpec().withContainers().endSpec().build()
    val sparkPod = KubernetesUtils.selectSparkContainer(noContainersPod, Some("does-not-exist"))
    assert(sparkPod.pod.getSpec.getHostname == HOST)
    assert(sparkPod.container.getName == null)
    val sparkPodWithNoContainerName =
      KubernetesUtils.selectSparkContainer(noContainersPod, Option.empty)
    assert(sparkPodWithNoContainerName.pod.getSpec.getHostname == HOST)
    assert(sparkPodWithNoContainerName.container.getName == null)
  }

  test("SPARK-38201: check uploadFileToHadoopCompatibleFS with different delSrc and overwrite") {
    withTempDir { srcDir =>
      withTempDir { destDir =>
        val upload = PrivateMethod[Unit](Symbol("uploadFileToHadoopCompatibleFS"))
        val fileName = "test.txt"
        val srcFile = new File(srcDir, fileName)
        val src = new Path(srcFile.getAbsolutePath)
        val dest = new Path(destDir.getAbsolutePath, fileName)
        val fs = src.getFileSystem(new Configuration())

        def checkUploadException(delSrc: Boolean, overwrite: Boolean): Unit = {
          val message = intercept[SparkException] {
            KubernetesUtils.invokePrivate(upload(src, dest, fs, delSrc, overwrite))
          }.getMessage
          assert(message.contains("Error uploading file"))
        }

        def appendFileAndUpload(content: String, delSrc: Boolean, overwrite: Boolean): Unit = {
          FileUtils.write(srcFile, content, StandardCharsets.UTF_8, true)
          KubernetesUtils.invokePrivate(upload(src, dest, fs, delSrc, overwrite))
        }

        // Write a new file, upload file with delSrc = false and overwrite = true.
        // Upload successful and record the `fileLength`.
        appendFileAndUpload("init-content", delSrc = false, overwrite = true)
        val firstLength = fs.getFileStatus(dest).getLen

        // Append the file, upload file with delSrc = false and overwrite = true.
        // Upload succeeded but `fileLength` changed.
        appendFileAndUpload("append-content", delSrc = false, overwrite = true)
        val secondLength = fs.getFileStatus(dest).getLen
        assert(firstLength < secondLength)

        // Upload file with delSrc = false and overwrite = false.
        // Upload failed because dest exists and not changed.
        checkUploadException(delSrc = false, overwrite = false)
        assert(fs.exists(dest))
        assert(fs.getFileStatus(dest).getLen == secondLength)

        // Append the file again, upload file delSrc = true and overwrite = true.
        // Upload succeeded, `fileLength` changed and src not exists.
        appendFileAndUpload("append-content", delSrc = true, overwrite = true)
        val thirdLength = fs.getFileStatus(dest).getLen
        assert(secondLength < thirdLength)
        assert(!fs.exists(src))

        // Rewrite a new file, upload file with delSrc = true and overwrite = false.
        // Upload failed because dest exists, src still exists.
        FileUtils.write(srcFile, "re-init-content", StandardCharsets.UTF_8, true)
        checkUploadException(delSrc = true, overwrite = false)
        assert(fs.exists(src))
      }
    }
  }

  test("SPARK-38582: verify that envVars is built with kv env as expected") {
    val input = for (i <- 9 to 1 by -1) yield (s"testEnvKey.$i", s"testEnvValue.$i")
    val expectedEnvVars = (input :+ ("testKeyWithEmptyValue" -> "")).map { case(k, v) =>
      new EnvVarBuilder()
        .withName(k)
        .withValue(v).build()
    }
    val outputEnvVars =
      KubernetesUtils.buildEnvVars(input ++
        Seq("testKeyWithNullValue" -> null, "testKeyWithEmptyValue" -> ""))
    assert(outputEnvVars.toSet == expectedEnvVars.toSet)
  }

  test("SPARK-38582: verify that envVars is built with field ref env as expected") {
    val input = for (i <- 9 to 1 by -1) yield (s"testEnvKey.$i", s"v$i", s"testEnvValue.$i")
    val expectedEnvVars = input.map { env =>
      new EnvVarBuilder()
        .withName(env._1)
        .withValueFrom(new EnvVarSourceBuilder()
          .withNewFieldRef(env._2, env._3)
          .build())
        .build()
    }
    val outputEnvVars =
      KubernetesUtils.buildEnvVarsWithFieldRef(
        input ++ Seq(
          ("testKey1", null, "testValue1"),
          ("testKey2", "v1", null),
          ("testKey3", null, null)))
    assert(outputEnvVars == expectedEnvVars)
  }

  test("SPARK-41781: verify pvc creation as expected") {
    KubernetesUtils.createPreResource(kubernetesClient, PVC, NAMESPACE)
    verify(pvcResource, times(1)).create()
    verify(resourceList, never()).createOrReplace()
  }

  test("SPARK-41781: verify resource creation as expected") {
    KubernetesUtils.createPreResource(kubernetesClient, CRD, NAMESPACE)
    verify(pvcResource, never()).create()
    verify(resourceList, times(1)).createOrReplace()
  }

  test("SPARK-41781: verify pvc creation exception as expected") {
    when(pvcResource.create()).thenThrow(new KubernetesClientException("PVC fails to create"))
    intercept[KubernetesClientException] {
      KubernetesUtils.createPreResource(kubernetesClient, PVC, NAMESPACE)
    }
    verify(resourceList, never()).createOrReplace()
  }

  test("SPARK-41781: verify resource creation exception as expected") {
    when(resourceList.createOrReplace())
      .thenThrow(new KubernetesClientException("Resource fails to create"))
    intercept[KubernetesClientException] {
      KubernetesUtils.createPreResource(kubernetesClient, CRD, NAMESPACE)
    }
    verify(pvcResource, never()).create()
  }

  test("SPARK-41781: verify ownerReference in PVC refreshed as expected") {
    when(pvcResource.get()).thenReturn(PVC)
    when(pvcResource.patch(createdPvcArgumentCaptor.capture())).thenReturn(PVC)

    KubernetesUtils.refreshOwnerReferenceInResource(
      kubernetesClient,
      PVC,
      NAMESPACE,
      EXECUTOR_POD)

    val pvcWithOwnerReference = createdPvcArgumentCaptor.getValue
    assert(pvcWithOwnerReference === PVC_WITH_OWNER_REFERENCES)
    verify(resourceList, never()).createOrReplace()
  }

  test("SPARK-41781: verify ownerReference in CRD refreshed as expected") {
    doReturn(resourceList)
      .when(kubernetesClient)
      .resourceList(createdResourcesArgumentCaptor.capture())
    KubernetesUtils.refreshOwnerReferenceInResource(
      kubernetesClient,
      CRD,
      NAMESPACE,
      EXECUTOR_POD)

    val crdWithOwnerReference = createdResourcesArgumentCaptor.getValue
    assert(crdWithOwnerReference === CRD_WITH_OWNER_REFERENCES)
    verify(pvcResource, never()).patch()
  }
}
