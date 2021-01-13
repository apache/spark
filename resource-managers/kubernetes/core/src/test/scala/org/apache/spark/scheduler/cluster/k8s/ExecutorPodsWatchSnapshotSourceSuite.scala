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

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.{KubernetesClient, Watch, Watcher}
import io.fabric8.kubernetes.client.Watcher.Action
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsWatchSnapshotSourceSuite extends SparkFunSuite with BeforeAndAfter {

  @Mock
  private val eventQueue: ExecutorPodsSnapshotsStore = null

  @Mock
  private val kubernetesClient: KubernetesClient = null

  @Mock
  private val podOperations: PODS = null

  @Mock
  private val appIdLabeledPods: LABELED_PODS = null

  @Mock
  private val executorRoleLabeledPods: LABELED_PODS = null

  @Mock
  private val watchConnection: Watch = null

  private var watch: ArgumentCaptor[Watcher[Pod]] = _

  private var watchSourceUnderTest: ExecutorPodsWatchSnapshotSource = _

  before {
    MockitoAnnotations.initMocks(this)
    watch = ArgumentCaptor.forClass(classOf[Watcher[Pod]])
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(appIdLabeledPods)
    when(appIdLabeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(executorRoleLabeledPods)
    when(executorRoleLabeledPods.watch(watch.capture())).thenReturn(watchConnection)
    watchSourceUnderTest = new ExecutorPodsWatchSnapshotSource(
      eventQueue, kubernetesClient)
    watchSourceUnderTest.start(TEST_SPARK_APP_ID)
  }

  test("Watch events should be pushed to the snapshots store as snapshot updates.") {
    val exec1 = runningExecutor(1)
    val exec2 = runningExecutor(2)
    watch.getValue.eventReceived(Action.ADDED, exec1)
    watch.getValue.eventReceived(Action.MODIFIED, exec2)
    verify(eventQueue).updatePod(exec1)
    verify(eventQueue).updatePod(exec2)
  }
}
