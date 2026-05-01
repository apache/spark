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
package org.apache.spark.scheduler.cluster.k8s

import java.util.Collections

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.Resource
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, times, verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._

class ExecutorPVCResizePluginSuite
    extends SparkFunSuite with BeforeAndAfter {

  private val namespace = "test-namespace"
  private val appId = "spark-test-app"

  private var kubernetesClient: KubernetesClient = _
  private var sparkContext: SparkContext = _
  private var schedulerBackend: KubernetesClusterSchedulerBackend = _
  private var podOperations: PODS = _
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _
  private var labeledPods: LABELED_PODS = _
  private var podList: PodList = _
  private var pvcOperations: PERSISTENT_VOLUME_CLAIMS = _
  private var pvcsWithNamespace: PVC_WITH_NAMESPACE = _

  before {
    kubernetesClient = mock(classOf[KubernetesClient])
    sparkContext = mock(classOf[SparkContext])
    schedulerBackend = mock(classOf[KubernetesClusterSchedulerBackend])
    podOperations = mock(classOf[PODS])
    podsWithNamespace = mock(classOf[PODS_WITH_NAMESPACE])
    labeledPods = mock(classOf[LABELED_PODS])
    podList = mock(classOf[PodList])
    pvcOperations = mock(classOf[PERSISTENT_VOLUME_CLAIMS])
    pvcsWithNamespace = mock(classOf[PVC_WITH_NAMESPACE])

    when(sparkContext.applicationId).thenReturn(appId)
    when(sparkContext.schedulerBackend).thenReturn(schedulerBackend)
    when(schedulerBackend.kubernetesClient).thenReturn(kubernetesClient)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace(namespace)).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withLabel(SPARK_APP_ID_LABEL, appId)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(labeledPods)
    when(labeledPods.list()).thenReturn(podList)
    when(kubernetesClient.persistentVolumeClaims()).thenReturn(pvcOperations)
    when(pvcOperations.inNamespace(namespace)).thenReturn(pvcsWithNamespace)
  }

  private def createPlugin(
      threshold: Double = 0.9,
      factor: Double = 0.1): ExecutorPVCResizeDriverPlugin = {
    val plugin = new ExecutorPVCResizeDriverPlugin()
    val cls = plugin.getClass
    setField(cls, plugin, "sparkContext", sparkContext)
    setField(cls, plugin, "namespace", namespace)
    setField(cls, plugin, "threshold", threshold)
    setField(cls, plugin, "factor", factor)
    plugin
  }

  private def setField(cls: Class[_], obj: Any, name: String, value: Any): Unit = {
    val f = cls.getDeclaredField(name)
    f.setAccessible(true)
    f.set(obj, value)
  }

  private def createPodWithPVC(
      executorId: Long,
      claimName: String,
      mountPath: String,
      containerName: String = DEFAULT_EXECUTOR_CONTAINER_NAME): Pod = {
    new PodBuilder()
      .withNewMetadata()
        .withName(s"spark-executor-$executorId")
        .addToLabels(SPARK_APP_ID_LABEL, appId)
        .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .addToLabels(SPARK_EXECUTOR_ID_LABEL, executorId.toString)
      .endMetadata()
      .withNewSpec()
        .addNewVolume()
          .withName("data")
          .withNewPersistentVolumeClaim()
            .withClaimName(claimName)
          .endPersistentVolumeClaim()
        .endVolume()
        .addNewContainer()
          .withName(containerName)
          .addNewVolumeMount()
            .withName("data")
            .withMountPath(mountPath)
          .endVolumeMount()
        .endContainer()
      .endSpec()
      .build()
  }

  private def createPVC(name: String, storageBytes: String): PersistentVolumeClaim = {
    new PersistentVolumeClaimBuilder()
      .withNewMetadata().withName(name).endMetadata()
      .withNewSpec()
        .withNewResources()
          .addToRequests("storage", new Quantity(storageBytes))
        .endResources()
      .endSpec()
      .build()
  }

  private def mockPvcResource(
      pvcName: String,
      storageBytes: String): Resource[PersistentVolumeClaim] = {
    val pvc = createPVC(pvcName, storageBytes)
    val resource = mock(classOf[Resource[PersistentVolumeClaim]])
    when(pvcsWithNamespace.withName(pvcName)).thenReturn(resource)
    when(resource.get()).thenReturn(pvc)
    resource
  }

  test("Empty pod list does not trigger any patch") {
    val plugin = createPlugin()
    when(podList.getItems).thenReturn(Collections.emptyList())
    plugin.receive(PVCDiskUsageReport("1", 0.1))

    plugin.checkAndResizePVCs()

    verify(pvcsWithNamespace, never()).withName(org.mockito.ArgumentMatchers.anyString())
  }

  test("Usage below threshold does not trigger patch") {
    val plugin = createPlugin()
    val pod = createPodWithPVC(1, "pvc-1", "/data")
    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    val resource = mockPvcResource("pvc-1", "1000000000") // 1GB
    plugin.receive(PVCDiskUsageReport("1", 0.5)) // 50%

    plugin.checkAndResizePVCs()

    verify(resource, never()).patch(any(), any(classOf[PersistentVolumeClaim]))
  }

  test("Usage above threshold triggers patch with grown size") {
    val plugin = createPlugin()
    val pod = createPodWithPVC(1, "pvc-1", "/data")
    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    val resource = mockPvcResource("pvc-1", "1000000000") // 1GB
    plugin.receive(PVCDiskUsageReport("1", 0.95)) // 95%

    plugin.checkAndResizePVCs()

    verify(resource, times(1)).patch(any(), any(classOf[PersistentVolumeClaim]))
  }

  test("Patch failure adds PVC to blacklist") {
    val plugin = createPlugin()
    val pod = createPodWithPVC(1, "pvc-1", "/data")
    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    val resource = mockPvcResource("pvc-1", "1000000000")
    when(resource.patch(any(), any(classOf[PersistentVolumeClaim])))
      .thenThrow(new RuntimeException("expansion not allowed"))
    plugin.receive(PVCDiskUsageReport("1", 0.95))

    plugin.checkAndResizePVCs()
    plugin.checkAndResizePVCs()

    // Only one patch attempt despite two check rounds.
    verify(resource, times(1)).patch(any(), any(classOf[PersistentVolumeClaim]))
  }

  test("Pod with no PVC volume triggers no patch") {
    val plugin = createPlugin()
    val pod = new PodBuilder()
      .withNewMetadata()
        .withName("spark-executor-1")
        .addToLabels(SPARK_APP_ID_LABEL, appId)
        .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .addToLabels(SPARK_EXECUTOR_ID_LABEL, "1")
      .endMetadata()
      .withNewSpec()
        .addNewContainer().withName(DEFAULT_EXECUTOR_CONTAINER_NAME).endContainer()
      .endSpec()
      .build()
    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    plugin.receive(PVCDiskUsageReport("1", 0.95))

    plugin.checkAndResizePVCs()

    verify(pvcsWithNamespace, never()).withName(org.mockito.ArgumentMatchers.anyString())
  }

  test("pvcsOf returns claim names mounted by the executor container") {
    val plugin = createPlugin()
    val pod = createPodWithPVC(7, "pvc-7", "/spark-local")
    assert(plugin.pvcsOf(pod) === Set("pvc-7"))
  }

  test("pvcsOf falls back to first container when default name absent") {
    val plugin = createPlugin()
    val pod = createPodWithPVC(1, "pvc-1", "/data", containerName = "custom")
    assert(plugin.pvcsOf(pod) === Set("pvc-1"))
  }

  test("receive ignores non-report messages") {
    val plugin = createPlugin()
    assert(plugin.receive("unrelated") == null)
    assert(plugin.receive(42) == null)
  }
}
