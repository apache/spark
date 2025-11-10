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
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.deploy.k8s.Fabric8Aliases.{LABELED_PODS, PODS}
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils.{runningExecutor, TEST_SPARK_APP_ID}

class ExecutorPodsInformerSnapshotSourceSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with MockitoSugar {

  private var snapshotSource: ExecutorPodsInformerSnapshotSource = _
  private var informerManager: InformerManager = _

  private val sparkConf = new SparkConf()
  private val resyncInterval = sparkConf.get(KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL)
  private val handlerCaptor: ArgumentCaptor[ResourceEventHandler[Pod]] =
    ArgumentCaptor.forClass(classOf[ResourceEventHandler[Pod]])

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var snapshotsStore: ExecutorPodsSnapshotsStore = _

  @Mock
  private var informer: SharedIndexInformer[Pod] = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var appIdLabeledPods: LABELED_PODS = _

  @Mock
  private var executorRoleLabeledPods: LABELED_PODS = _

  override def beforeEach(): Unit = {
    MockitoAnnotations.initMocks(this)
    informerManager = spy(new InformerManager(kubernetesClient, TEST_SPARK_APP_ID, sparkConf))

    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID))
      .thenReturn(appIdLabeledPods)
    when(appIdLabeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(executorRoleLabeledPods)
    when(executorRoleLabeledPods.runnableInformer(resyncInterval)).thenReturn(informer)
    when(informer.isRunning).thenReturn(false)

    snapshotSource = new ExecutorPodsInformerSnapshotSource
    snapshotSource.init(sparkConf, kubernetesClient, snapshotsStore, informerManager)
  }

  test("Informer should be run when snapshot source is started") {
    snapshotSource.start(TEST_SPARK_APP_ID)
    verify(informer, times(1)).run()
  }

  test("Informer should stop running when snapshot source is stopped") {
    snapshotSource.start(TEST_SPARK_APP_ID)
    snapshotSource.stop()
    verify(informer, times(1)).close()
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

  def withNewResourceVersion(pod: Pod, version: String): Pod = {
    new PodBuilder(pod)
      .editMetadata()
      .withResourceVersion(version)
      .endMetadata()
      .build()
  }
}
