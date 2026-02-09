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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.metrics.v1beta1.{ContainerMetrics, PodMetrics}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{MetricAPIGroupDSL, PodMetricOperation}
import org.mockito.ArgumentMatchers.{any, anyString}
import org.mockito.Mockito.{mock, never, times, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._

class ExecutorResizePluginSuite
    extends SparkFunSuite with BeforeAndAfter with PrivateMethodTester {

  private val namespace = "test-namespace"
  private val appId = "spark-test-app"

  private var kubernetesClient: KubernetesClient = _
  private var sparkContext: SparkContext = _
  private var podOperations: PODS = _
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _
  private var labeledPods: LABELED_PODS = _
  private var podList: PodList = _
  private var topOperations: MetricAPIGroupDSL = _
  private var podMetricOperations: PodMetricOperation = _

  private val _checkAndIncreaseMemory =
    PrivateMethod[Unit](Symbol("checkAndIncreaseMemory"))

  before {
    kubernetesClient = mock(classOf[KubernetesClient])
    sparkContext = mock(classOf[SparkContext])
    podOperations = mock(classOf[PODS])
    podsWithNamespace = mock(classOf[PODS_WITH_NAMESPACE])
    labeledPods = mock(classOf[LABELED_PODS])
    podList = mock(classOf[PodList])
    topOperations = mock(classOf[MetricAPIGroupDSL])
    podMetricOperations = mock(classOf[PodMetricOperation])

    when(sparkContext.applicationId).thenReturn(appId)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace(namespace)).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withLabel(SPARK_APP_ID_LABEL, appId)).thenReturn(labeledPods)
    when(labeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(labeledPods)
    when(labeledPods.list()).thenReturn(podList)
    when(kubernetesClient.top()).thenReturn(topOperations)
    when(topOperations.pods()).thenReturn(podMetricOperations)
  }

  private def createPlugin(): ExecutorResizeDriverPlugin = {
    val plugin = new ExecutorResizeDriverPlugin()
    // Use reflection to set private fields
    val scField = plugin.getClass.getDeclaredField("sparkContext")
    scField.setAccessible(true)
    scField.set(plugin, sparkContext)

    val clientField = plugin.getClass.getDeclaredField("kubernetesClient")
    clientField.setAccessible(true)
    clientField.set(plugin, kubernetesClient)

    plugin
  }

  private def createPodWithMemoryLimit(
      executorId: Long,
      memoryLimit: String,
      containerName: String = DEFAULT_EXECUTOR_CONTAINER_NAME): Pod = {
    new PodBuilder()
      .withNewMetadata()
        .withName(s"spark-executor-$executorId")
        .addToLabels(SPARK_APP_ID_LABEL, appId)
        .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .addToLabels(SPARK_EXECUTOR_ID_LABEL, executorId.toString)
      .endMetadata()
      .withNewSpec()
        .addNewContainer()
          .withName(containerName)
          .withNewResources()
            .addToLimits("memory", new Quantity(memoryLimit))
          .endResources()
        .endContainer()
      .endSpec()
      .build()
  }

  private def createPodMetrics(
      podName: String,
      memoryUsage: String,
      containerName: String = DEFAULT_EXECUTOR_CONTAINER_NAME): PodMetrics = {
    val containerMetrics = new ContainerMetrics()
    containerMetrics.setName(containerName)
    containerMetrics.setUsage(Map("memory" -> new Quantity(memoryUsage)).asJava)

    val podMetrics = new PodMetrics()
    podMetrics.setContainers(Collections.singletonList(containerMetrics))
    podMetrics
  }

  test("Empty pod list should not trigger any action") {
    val plugin = createPlugin()
    when(podList.getItems).thenReturn(Collections.emptyList())

    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.9, 0.1))

    verify(podMetricOperations, never()).metrics(anyString(), anyString())
  }

  test("Pod without executor ID label should be skipped") {
    val plugin = createPlugin()
    val pod = new PodBuilder()
      .withNewMetadata()
        .withName("spark-executor-1")
        .addToLabels(SPARK_APP_ID_LABEL, appId)
        .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        // No SPARK_EXECUTOR_ID_LABEL
      .endMetadata()
      .build()

    when(podList.getItems).thenReturn(Collections.singletonList(pod))

    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.9, 0.1))

    verify(podMetricOperations, never()).metrics(anyString(), anyString())
  }

  test("Memory usage below threshold should not trigger resize") {
    val plugin = createPlugin()
    val pod = createPodWithMemoryLimit(1, "1000000000") // 1GB limit
    val metrics = createPodMetrics("spark-executor-1", "500000000") // 500MB usage (50%)

    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    when(podMetricOperations.metrics(namespace, "spark-executor-1")).thenReturn(metrics)

    val podResource = mock(classOf[SINGLE_POD])
    when(podsWithNamespace.withName("spark-executor-1")).thenReturn(podResource)

    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.9, 0.1))

    verify(podResource, never()).patch(any(), any(classOf[Pod]))
  }

  test("Memory usage above threshold should trigger resize") {
    val plugin = createPlugin()
    val pod = createPodWithMemoryLimit(1, "1000000000") // 1GB limit
    val metrics = createPodMetrics("spark-executor-1", "950000000") // 950MB usage (95%)

    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    when(podMetricOperations.metrics(namespace, "spark-executor-1")).thenReturn(metrics)

    val podResource = mock(classOf[SINGLE_POD])
    when(podsWithNamespace.withName("spark-executor-1")).thenReturn(podResource)
    when(podResource.subresource(anyString())).thenReturn(podResource)

    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.9, 0.1))

    verify(podResource, times(1)).patch(any(), any(classOf[Pod]))
  }

  test("Memory usage exactly at threshold should not trigger resize") {
    val plugin = createPlugin()
    val pod = createPodWithMemoryLimit(1, "1000000000") // 1GB limit
    val metrics = createPodMetrics("spark-executor-1", "900000000") // 900MB usage (90%)

    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    when(podMetricOperations.metrics(namespace, "spark-executor-1")).thenReturn(metrics)

    val podResource = mock(classOf[SINGLE_POD])
    when(podsWithNamespace.withName("spark-executor-1")).thenReturn(podResource)

    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.9, 0.1))

    verify(podResource, never()).patch(any(), any(classOf[Pod]))
  }

  test("Pod without memory limit should be skipped") {
    val plugin = createPlugin()
    val pod = new PodBuilder()
      .withNewMetadata()
        .withName("spark-executor-1")
        .addToLabels(SPARK_APP_ID_LABEL, appId)
        .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
        .addToLabels(SPARK_EXECUTOR_ID_LABEL, "1")
      .endMetadata()
      .withNewSpec()
        .addNewContainer()
          .withName(DEFAULT_EXECUTOR_CONTAINER_NAME)
          .withNewResources()
            // No memory limit
          .endResources()
        .endContainer()
      .endSpec()
      .build()

    val metrics = createPodMetrics("spark-executor-1", "500000000")

    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    when(podMetricOperations.metrics(namespace, "spark-executor-1")).thenReturn(metrics)

    val podResource = mock(classOf[SINGLE_POD])
    when(podsWithNamespace.withName("spark-executor-1")).thenReturn(podResource)

    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.9, 0.1))

    verify(podResource, never()).patch(any(), any(classOf[Pod]))
  }

  test("Multiple pods with mixed memory usage") {
    val plugin = createPlugin()
    val pod1 = createPodWithMemoryLimit(1, "1000000000") // 1GB limit
    val pod2 = createPodWithMemoryLimit(2, "1000000000") // 1GB limit

    val metrics1 = createPodMetrics("spark-executor-1", "500000000") // 50% - below threshold
    val metrics2 = createPodMetrics("spark-executor-2", "950000000") // 95% - above threshold

    when(podList.getItems).thenReturn(List(pod1, pod2).asJava)
    when(podMetricOperations.metrics(namespace, "spark-executor-1")).thenReturn(metrics1)
    when(podMetricOperations.metrics(namespace, "spark-executor-2")).thenReturn(metrics2)

    val podResource1 = mock(classOf[SINGLE_POD])
    val podResource2 = mock(classOf[SINGLE_POD])
    when(podsWithNamespace.withName("spark-executor-1")).thenReturn(podResource1)
    when(podsWithNamespace.withName("spark-executor-2")).thenReturn(podResource2)
    when(podResource2.subresource(anyString())).thenReturn(podResource2)

    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.9, 0.1))

    verify(podResource1, never()).patch(any(), any(classOf[Pod]))
    verify(podResource2, times(1)).patch(any(), any(classOf[Pod]))
  }

  test("Lower threshold should trigger resize more aggressively") {
    val plugin = createPlugin()
    val pod = createPodWithMemoryLimit(1, "1000000000") // 1GB limit
    val metrics = createPodMetrics("spark-executor-1", "600000000") // 60% usage

    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    when(podMetricOperations.metrics(namespace, "spark-executor-1")).thenReturn(metrics)

    val podResource = mock(classOf[SINGLE_POD])
    when(podsWithNamespace.withName("spark-executor-1")).thenReturn(podResource)
    when(podResource.subresource(anyString())).thenReturn(podResource)

    // Use 50% threshold - 60% usage should trigger resize
    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.5, 0.1))

    verify(podResource, times(1)).patch(any(), any(classOf[Pod]))
  }

  test("Fallback to first container when default container name not found") {
    val plugin = createPlugin()
    val customContainerName = "custom-executor"
    val pod = createPodWithMemoryLimit(1, "1000000000", customContainerName)
    val metrics = createPodMetrics("spark-executor-1", "950000000", customContainerName)

    when(podList.getItems).thenReturn(Collections.singletonList(pod))
    when(podMetricOperations.metrics(namespace, "spark-executor-1")).thenReturn(metrics)

    val podResource = mock(classOf[SINGLE_POD])
    when(podsWithNamespace.withName("spark-executor-1")).thenReturn(podResource)
    when(podResource.subresource(anyString())).thenReturn(podResource)

    plugin.invokePrivate(_checkAndIncreaseMemory(namespace, 0.9, 0.1))

    verify(podResource, times(1)).patch(any(), any(classOf[Pod]))
  }
}
