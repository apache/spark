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

import io.fabric8.kubernetes.api.model.{Pod, PodBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.{ResourceEventHandler, SharedIndexInformer}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL
import org.apache.spark.deploy.k8s.Fabric8Aliases.{LABELED_PODS, PODS}
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils.{runningExecutor, TEST_SPARK_APP_ID}

class ExecutorPodsInformerSnapshotSourceSuite extends SparkFunSuite with BeforeAndAfterEach {

  private val sparkConf = new SparkConf()
  private val resyncInterval = sparkConf.get(KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL)

  private var snapshotSource: ExecutorPodsInformerSnapshotSource = _
  private var informerManager: InformerManager = _
  private var handlerCaptor: ArgumentCaptor[ResourceEventHandler[Pod]] = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var snapshotsStore: ExecutorPodsSnapshotsStore = _

  @Mock
  private var informer: SharedIndexInformer[Pod] = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var scopedPods: LABELED_PODS = _

  override def beforeEach(): Unit = {
    MockitoAnnotations.openMocks(this).close()
    handlerCaptor = ArgumentCaptor.forClass(classOf[ResourceEventHandler[Pod]])
    InformerTestUtils.stubInformerBuilder(
      kubernetesClient, podOperations, scopedPods, informer, TEST_SPARK_APP_ID, resyncInterval)

    informerManager = new InformerManager(kubernetesClient, sparkConf)
    snapshotSource = new ExecutorPodsInformerSnapshotSource(snapshotsStore, informerManager)
  }

  test("Informer should be started when snapshot source is started") {
    snapshotSource.start(TEST_SPARK_APP_ID)
    verify(informer, times(1)).start()
  }

  test("Informer should stop running when snapshot source is stopped") {
    snapshotSource.start(TEST_SPARK_APP_ID)
    snapshotSource.stop()
    verify(informer, times(1)).close()
  }

  test("start should throw when called twice") {
    snapshotSource.start(TEST_SPARK_APP_ID)
    val e = intercept[IllegalArgumentException] {
      snapshotSource.start(TEST_SPARK_APP_ID)
    }
    assert(e.getMessage.contains("Cannot start the informer source twice"))
  }

  test("Informer onAdd/onUpdate/onDelete should push updates to the snapshots store") {
    snapshotSource.start(TEST_SPARK_APP_ID)
    verify(informer).addEventHandler(handlerCaptor.capture())

    val exec1 = runningExecutor(1)
    val exec2 = runningExecutor(2)
    val exec2ResourceVersionChanged = withNewResourceVersion(exec2, "1")
    val exec3 = runningExecutor(3)

    val handler = handlerCaptor.getValue

    handler.onAdd(exec1)
    handler.onUpdate(exec2, exec2ResourceVersionChanged)
    handler.onDelete(exec3, false)

    verify(snapshotsStore).updatePod(exec1)
    verify(snapshotsStore).updatePod(exec2ResourceVersionChanged)
    verify(snapshotsStore).updatePod(exec3)
  }

  test("Informer onUpdate should skip when resourceVersion is unchanged (resync replay)") {
    snapshotSource.start(TEST_SPARK_APP_ID)
    verify(informer).addEventHandler(handlerCaptor.capture())
    val handler = handlerCaptor.getValue

    val exec = withNewResourceVersion(runningExecutor(1), "42")
    handler.onUpdate(exec, exec)

    verify(snapshotsStore, never()).updatePod(any(classOf[Pod]))
  }

  def withNewResourceVersion(pod: Pod, version: String): Pod = {
    new PodBuilder(pod)
      .editMetadata()
      .withResourceVersion(version)
      .endMetadata()
      .build()
  }
}
