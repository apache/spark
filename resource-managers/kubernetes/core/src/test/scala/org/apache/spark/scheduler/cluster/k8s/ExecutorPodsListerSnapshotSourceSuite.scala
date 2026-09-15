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

import java.util.concurrent.TimeUnit

import io.fabric8.kubernetes.api.model.{Pod, PodListBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.SharedIndexInformer
import io.fabric8.kubernetes.client.informers.cache.Indexer
import org.jmock.lib.concurrent.DeterministicScheduler
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Fabric8Aliases.{LABELED_PODS, PODS}
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class ExecutorPodsListerSnapshotSourceSuite extends SparkFunSuite with BeforeAndAfterEach {

  private val testNamespace = "test-namespace"
  private val sparkConf = new SparkConf
  private val pollingInterval = sparkConf.get(KUBERNETES_EXECUTOR_LISTER_POLLING_INTERVAL)
  private val resyncInterval = sparkConf.get(KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL)
  private val pollingExecutor = new DeterministicScheduler

  private var informerManager: InformerManager = _
  private var snapshotSource: ExecutorPodsListerSnapshotSource = _

  @Mock
  private var informer: SharedIndexInformer[Pod] = _

  @Mock
  private var indexer: Indexer[Pod] = _

  @Mock
  private var snapshotsStore: ExecutorPodsSnapshotsStore = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var scopedPods: LABELED_PODS = _

  override def beforeEach(): Unit = {
    MockitoAnnotations.openMocks(this).close()
    InformerTestUtils.stubInformerBuilder(
      kubernetesClient, podOperations, scopedPods, informer, TEST_SPARK_APP_ID, resyncInterval)
    when(kubernetesClient.getNamespace).thenReturn(testNamespace)
    when(informer.getIndexer).thenReturn(indexer)

    informerManager = new InformerManager(kubernetesClient, sparkConf)
    snapshotSource = new ExecutorPodsListerSnapshotSource(
      sparkConf, kubernetesClient, snapshotsStore, informerManager, pollingExecutor)
  }

  test("Lister snapshot source pushes all current pods to snapshot store") {
    when(informer.hasSynced).thenReturn(true)
    val exec1 = runningExecutor(1)
    val exec2 = runningExecutor(2)
    val podList = new PodListBuilder().addToItems(exec1, exec2).build().getItems
    when(indexer.byIndex("namespace", testNamespace)).thenReturn(podList)
    snapshotSource.start(TEST_SPARK_APP_ID)
    pollingExecutor.tick(pollingInterval, TimeUnit.MILLISECONDS)
    verify(snapshotsStore).replaceSnapshot(Seq(exec1, exec2))
  }

  test("Poll skips replacement when informer has not synced yet") {
    when(informer.hasSynced).thenReturn(false)
    when(indexer.byIndex("namespace", testNamespace))
      .thenReturn(new PodListBuilder().build().getItems)
    snapshotSource.start(TEST_SPARK_APP_ID)
    pollingExecutor.tick(pollingInterval, TimeUnit.MILLISECONDS)
    verify(snapshotsStore, never()).replaceSnapshot(any())
  }

  test("Poll replaces snapshot with empty seq when informer is synced but no pods exist") {
    when(informer.hasSynced).thenReturn(true)
    when(indexer.byIndex("namespace", testNamespace))
      .thenReturn(new PodListBuilder().build().getItems)
    snapshotSource.start(TEST_SPARK_APP_ID)
    pollingExecutor.tick(pollingInterval, TimeUnit.MILLISECONDS)
    verify(snapshotsStore).replaceSnapshot(Seq.empty)
  }

  test("start should throw when called twice") {
    snapshotSource.start(TEST_SPARK_APP_ID)
    val e = intercept[IllegalArgumentException] {
      snapshotSource.start(TEST_SPARK_APP_ID)
    }
    assert(e.getMessage.contains("Cannot start lister polling more than once"))
  }
}
