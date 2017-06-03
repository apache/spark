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
package org.apache.spark.deploy.rest.kubernetes

import java.util.concurrent.{ScheduledExecutorService, TimeUnit}

import io.fabric8.kubernetes.api.model.{DoneableNamespace, DoneablePod, Namespace, NamespaceList, Pod, PodList, PodListBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch, Watcher}
import io.fabric8.kubernetes.client.dsl.{FilterWatchListDeletable, MixedOperation, NonNamespaceOperation, PodResource, Resource}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Matchers.{eq => mockitoEq}
import org.mockito.Mockito.{never, verify, when}
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._

import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Clock

private[spark] class StagedResourcesCleanerSuite extends SparkFunSuite with BeforeAndAfter {

  private type PODS = MixedOperation[Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]
  private type PODSWITHLABELS = FilterWatchListDeletable[
      Pod, PodList, java.lang.Boolean, Watch, Watcher[Pod]]
  private type PODSINNAMESPACE = NonNamespaceOperation[
      Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]
  private type NAMESPACES = NonNamespaceOperation[
      Namespace, NamespaceList, DoneableNamespace, Resource[Namespace, DoneableNamespace]]
  private type NAMESPACEWITHNAME = Resource[Namespace, DoneableNamespace]

  private val INITIAL_ACCESS_EXPIRATION_MS = 5000L
  private val CURRENT_TIME = 10000L
  private val RESOURCE_ID = "resource-id"
  private val POD_NAMESPACE = "namespace"
  private val POD_LABELS = Map("label1" -> "label1value", "label2" -> "label2value")
  private val RESOURCES_OWNER = StagedResourcesOwner(
    ownerNamespace = POD_NAMESPACE,
    ownerLabels = POD_LABELS,
    ownerType = StagedResourcesOwnerType.Pod)

  @Mock
  private var stagedResourcesStore: StagedResourcesStore = _
  @Mock
  private var kubernetesClient: KubernetesClient = _
  @Mock
  private var clock: Clock = _
  @Mock
  private var cleanerExecutorService: ScheduledExecutorService = _
  @Mock
  private var podOperations: PODS = _
  @Mock
  private var podsInNamespaceOperations: PODSINNAMESPACE = _
  @Mock
  private var podsWithLabelsOperations: PODSWITHLABELS = _
  @Mock
  private var namespaceOperations: NAMESPACES = _
  @Mock
  private var namedNamespaceOperations: NAMESPACEWITHNAME = _
  private var cleanerUnderTest: StagedResourcesCleaner = _

  before {
    MockitoAnnotations.initMocks(this)
    cleanerUnderTest = new StagedResourcesCleanerImpl(
        stagedResourcesStore,
        kubernetesClient,
        cleanerExecutorService,
        clock,
        INITIAL_ACCESS_EXPIRATION_MS)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabels(POD_LABELS.asJava)).thenReturn(podsWithLabelsOperations)
    when(kubernetesClient.namespaces()).thenReturn(namespaceOperations)
  }

  test("Clean the resource if it is never accessed for the expiration interval.") {
    val cleanupRunnable = startCleanupAndGetCleanupRunnable()
    cleanerUnderTest.registerResourceForCleaning(RESOURCE_ID, RESOURCES_OWNER)
    when(clock.getTimeMillis()).thenReturn(CURRENT_TIME + INITIAL_ACCESS_EXPIRATION_MS)
    cleanupRunnable.run()
    verify(stagedResourcesStore).removeResources(RESOURCE_ID)
    verify(kubernetesClient, never()).pods()
  }

  test("Don't clean the resource if it is accessed in the expiration interval" +
    " and there are owners available.") {
    val cleanupRunnable = startCleanupAndGetCleanupRunnable()
    cleanerUnderTest.registerResourceForCleaning(RESOURCE_ID, RESOURCES_OWNER)
    cleanerUnderTest.markResourceAsUsed(RESOURCE_ID)
    when(clock.getTimeMillis()).thenReturn(CURRENT_TIME + INITIAL_ACCESS_EXPIRATION_MS)
    when(namespaceOperations.withName(POD_NAMESPACE)).thenReturn(namedNamespaceOperations)
    when(namedNamespaceOperations.get()).thenReturn(new Namespace())
    when(podOperations.inNamespace(POD_NAMESPACE)).thenReturn(podsInNamespaceOperations)
    when(podsInNamespaceOperations.withLabels(POD_LABELS.asJava))
        .thenReturn(podsWithLabelsOperations)
    when(podsWithLabelsOperations.list()).thenReturn(
        new PodListBuilder().addNewItemLike(new Pod()).endItem().build())
    cleanupRunnable.run()
    verify(stagedResourcesStore, never()).removeResources(RESOURCE_ID)
  }

  test("Clean the resource if no owners are available.") {
    val cleanupRunnable = startCleanupAndGetCleanupRunnable()
    cleanerUnderTest.registerResourceForCleaning(RESOURCE_ID, RESOURCES_OWNER)
    cleanerUnderTest.markResourceAsUsed(RESOURCE_ID)
    when(clock.getTimeMillis()).thenReturn(CURRENT_TIME + INITIAL_ACCESS_EXPIRATION_MS)
    when(namespaceOperations.withName(POD_NAMESPACE)).thenReturn(namedNamespaceOperations)
    when(namedNamespaceOperations.get()).thenReturn(new Namespace())
    when(podOperations.inNamespace(POD_NAMESPACE)).thenReturn(podsInNamespaceOperations)
    when(podsInNamespaceOperations.withLabels(POD_LABELS.asJava))
      .thenReturn(podsWithLabelsOperations)
    when(podsWithLabelsOperations.list()).thenReturn(new PodListBuilder().build())
    cleanupRunnable.run()
    verify(stagedResourcesStore).removeResources(RESOURCE_ID)
  }

  test("Clean up the resource if the namespace does not exist.") {
    val cleanupRunnable = startCleanupAndGetCleanupRunnable()
    cleanerUnderTest.registerResourceForCleaning(RESOURCE_ID, RESOURCES_OWNER)
    cleanerUnderTest.markResourceAsUsed(RESOURCE_ID)
    when(clock.getTimeMillis()).thenReturn(CURRENT_TIME + INITIAL_ACCESS_EXPIRATION_MS)
    when(namespaceOperations.withName(POD_NAMESPACE)).thenReturn(namedNamespaceOperations)
    when(namedNamespaceOperations.get()).thenReturn(null)
    cleanupRunnable.run()
    verify(stagedResourcesStore).removeResources(RESOURCE_ID)
  }

  private def startCleanupAndGetCleanupRunnable(): Runnable = {
    val captor = ArgumentCaptor.forClass(classOf[Runnable])
    cleanerUnderTest.start()
    verify(cleanerExecutorService).scheduleAtFixedRate(
        captor.capture(),
        mockitoEq(30000L),
        mockitoEq(30000L),
        mockitoEq(TimeUnit.MILLISECONDS))
    captor.getValue
  }
}
