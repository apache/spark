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
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.informers.SharedIndexInformer
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito._
import org.mockito.Mockito.verify
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.deploy.k8s.Fabric8Aliases.{LABELED_PODS, PODS}

class InformerManagerSuite extends SparkFunSuite with BeforeAndAfter {

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var informer: SharedIndexInformer[Pod] = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var appIdLabeledPods: LABELED_PODS = _

  @Mock
  private var executorRoleLabeledPods: LABELED_PODS = _

  private var conf: SparkConf = _
  private val applicationId = "test-app-id"

  before {
    MockitoAnnotations.initMocks(this)
    conf = new SparkConf().set(KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL, 10000L)

    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, applicationId)).thenReturn(appIdLabeledPods)
    when(appIdLabeledPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE))
      .thenReturn(executorRoleLabeledPods)
    when(executorRoleLabeledPods.runnableInformer(10000L)).thenReturn(informer)
  }

  test("If informer is null, getInformer should initialize it") {
    val manager = new InformerManager(kubernetesClient, applicationId, conf)
    assert(manager.informer == null)
    assert(manager.getInformer() == informer)
  }

  test("startInformer should not call run if informer is already running") {
    when(informer.isRunning).thenReturn(true)
    val manager = new InformerManager(kubernetesClient, applicationId, conf)

    manager.initInformer()
    manager.getInformer()
    manager.startInformer()

    verify(informer, times(0)).run()
  }

  test("stopInformer should close the informer and null it out") {
    val manager = new InformerManager(kubernetesClient, applicationId, conf)

    manager.initInformer()
    manager.startInformer()
    manager.stopInformer()

    verify(informer).close()
    assert(manager.informer == null)
  }

  test("getInformer should not re-initialize the informer after it has been stopped") {
    val manager = new InformerManager(kubernetesClient, applicationId, conf)
    manager.initInformer()
    manager.startInformer()
    assert(manager.getInformer() != null)
    manager.stopInformer()

    assert(manager.getInformer() == null)
  }

  test("Calling startInformer after stopInformer should throw") {
    val manager = new InformerManager(kubernetesClient, applicationId, conf)
    manager.initInformer()
    manager.startInformer()
    manager.stopInformer()
    val e = intercept[IllegalStateException] {
      manager.initInformer()
      manager.startInformer()
    }
    assert(e.getMessage.contains("Cannot run informer after stopInformer() " +
      "has been called."))
  }
}
