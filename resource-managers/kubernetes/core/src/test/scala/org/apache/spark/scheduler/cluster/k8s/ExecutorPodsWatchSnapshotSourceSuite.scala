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
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_ENABLE_API_WATCHER
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsWatchSnapshotSourceSuite extends SparkFunSuite with BeforeAndAfter {

  @Mock
  private var eventQueue: ExecutorPodsSnapshotsStore = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _

  @Mock
  private var appIdLabeledPods: LABELED_PODS = _

  @Mock
  private var executorRoleLabeledPods: LABELED_PODS = _

  @Mock
  private var watchConnection: Watch = _

  private var watch: ArgumentCaptor[Watcher[Pod]] = _

  private var watchSourceUnderTest: ExecutorPodsWatchSnapshotSource = _

  before {
    MockitoAnnotations.openMocks(this).close()
    watch = ArgumentCaptor.forClass(classOf[Watcher[Pod]])
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace("default")).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(appIdLabeledPods)
    when(appIdLabeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(executorRoleLabeledPods)
    when(executorRoleLabeledPods.watch(watch.capture())).thenReturn(watchConnection)
  }

  test("Watch events should be pushed to the snapshots store as snapshot updates.") {
    val conf = new SparkConf()
    watchSourceUnderTest = new ExecutorPodsWatchSnapshotSource(
      eventQueue, kubernetesClient, conf)
    watchSourceUnderTest.start(TEST_SPARK_APP_ID)
    val exec1 = runningExecutor(1)
    val exec2 = runningExecutor(2)
    watch.getValue.eventReceived(Action.ADDED, exec1)
    watch.getValue.eventReceived(Action.MODIFIED, exec2)
    verify(eventQueue).updatePod(exec1)
    verify(eventQueue).updatePod(exec2)
  }

  test("SPARK-36462: Verify if watchers are disabled we don't call pods() on the client") {
    val conf = new SparkConf()
    conf.set(KUBERNETES_EXECUTOR_ENABLE_API_WATCHER, false)
    watchSourceUnderTest = new ExecutorPodsWatchSnapshotSource(
      eventQueue, kubernetesClient, conf)
    watchSourceUnderTest.start(TEST_SPARK_APP_ID)
    verify(kubernetesClient, never()).pods()
  }
}
