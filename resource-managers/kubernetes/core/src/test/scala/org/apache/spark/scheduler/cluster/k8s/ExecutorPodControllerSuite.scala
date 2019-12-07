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

import io.fabric8.kubernetes.api.model.{DoneablePod, Pod, PodBuilder, PodList}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch, Watcher}
import io.fabric8.kubernetes.client.dsl.{FilterWatchListDeletable, PodResource}
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_EXECUTOR_ID_LABEL, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.deploy.k8s.Fabric8Aliases.PODS
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils.TEST_SPARK_APP_ID

class ExecutorPodControllerSuite extends SparkFunSuite with BeforeAndAfter {

  private var executorPodController: ExecutorPodController = _

  private val sparkConf = new SparkConf(false)

  private val execExampleId = "exec-id"
  private val numPods = 5
  private val execPodList = (1 to numPods).map(_.toLong)
  private val execPodString = execPodList.map(_.toString)

  private def buildPod(execId: String ): Pod = {
    new PodBuilder()
      .withNewMetadata()
      .withName(execId)
      .endMetadata()
      .build()
  }

  private val execPod = buildPod(execExampleId)

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var execPodOperations: PodResource[Pod, DoneablePod] = _

  @Mock
  private var ePodOperations:
    FilterWatchListDeletable[Pod, PodList, java.lang.Boolean, Watch, Watcher[Pod]] = _

  @Mock
  private var fPodOperations:
    FilterWatchListDeletable[Pod, PodList, java.lang.Boolean, Watch, Watcher[Pod]] = _


  before {
    MockitoAnnotations.initMocks(this)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(execExampleId))
      .thenReturn(execPodOperations)
    when(podOperations
      .withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)).thenReturn(podOperations)
    when(podOperations
      .withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(ePodOperations)
    when(ePodOperations.delete()).thenReturn(true)
    when(ePodOperations
      .withLabelIn(SPARK_EXECUTOR_ID_LABEL, execPodString: _*)).thenReturn(fPodOperations)
    when(fPodOperations.delete()).thenReturn(true)
    executorPodController = new ExecutorPodControllerImpl(sparkConf)
    executorPodController.initialize(kubernetesClient, TEST_SPARK_APP_ID)
  }

  test("Adding a pod and watching counter go up correctly") {
    val numAllocated = 5
    for ( _ <- 0 until numAllocated) {
      executorPodController.addPod(execPod)
    }
    verify(podOperations, never()).create(execPod)
    assert(executorPodController.commitAndGetTotalAllocated() == numAllocated)
    verify(podOperations, times(numAllocated)).create(execPod)
    assert(executorPodController.commitAndGetTotalAllocated() == 0)
    executorPodController.addPod(execPod)
    assert(executorPodController.commitAndGetTotalAllocated() == 1)
  }

  test("Remove a single pod") {
    executorPodController.removePod(execPod)
    verify(podOperations).delete(execPod)
  }

  test("Remove a pod list") {
    executorPodController.removePods(execPodList)
    verify(fPodOperations, times(1)).delete()
  }

  test("Remove a pod and watching counter go up correctly") {
    execPodString.foreach{
      executorPodController.removePodById(_)
    }
    verify(fPodOperations, never()).delete()
    assert(executorPodController.commitAndGetTotalDeleted() == numPods)
    verify(fPodOperations, times(1)).delete()
  }

  test("Remove all pods") {
    executorPodController.removeAllPods()
    verify(ePodOperations).delete()
  }
}
