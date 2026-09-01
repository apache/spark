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
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_EXECUTOR_INACTIVE_LABEL, SPARK_POD_EXECUTOR_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.deploy.k8s.Fabric8Aliases.{LABELED_PODS, PODS}

class InformerManagerSuite extends SparkFunSuite with BeforeAndAfter {

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var informer: SharedIndexInformer[Pod] = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var scopedPods: LABELED_PODS = _

  private var conf: SparkConf = _
  private val applicationId = "test-app-id"

  before {
    MockitoAnnotations.initMocks(this)
    conf = new SparkConf().set(KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL, 10000L)

    // The informer is scoped server-side to app-id + role=executor + non-inactive pods.
    // Chain all filter calls into the same mock so the final .runnableInformer(...) hits.
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withLabel(SPARK_APP_ID_LABEL, applicationId)).thenReturn(scopedPods)
    when(scopedPods.withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)).thenReturn(scopedPods)
    when(scopedPods.withoutLabel(SPARK_EXECUTOR_INACTIVE_LABEL, "true")).thenReturn(scopedPods)
    when(scopedPods.runnableInformer(10000L)).thenReturn(informer)
  }

  test("If informer is null, initInformer should initialize it") {
    val manager = new InformerManager(kubernetesClient, conf)
    assert(manager.informer == null)
    manager.initInformer(applicationId)
    assert(manager.getInformer() == informer)
  }

  test("initInformer should scope the informer server-side to executor, non-inactive pods") {
    val manager = new InformerManager(kubernetesClient, conf)
    manager.initInformer(applicationId)
    verify(podOperations).withLabel(SPARK_APP_ID_LABEL, applicationId)
    verify(scopedPods).withLabel(SPARK_ROLE_LABEL, SPARK_POD_EXECUTOR_ROLE)
    verify(scopedPods).withoutLabel(SPARK_EXECUTOR_INACTIVE_LABEL, "true")
    verify(scopedPods).runnableInformer(10000L)
  }

  test("startInformer should not call run if informer is already running") {
    when(informer.isRunning).thenReturn(true)
    val manager = new InformerManager(kubernetesClient, conf)

    manager.initInformer(applicationId)
    manager.getInformer()
    manager.startInformer()

    verify(informer, times(0)).run()
  }

  test("stopInformer should close the informer and null it out") {
    val manager = new InformerManager(kubernetesClient, conf)

    manager.initInformer(applicationId)
    manager.startInformer()
    manager.stopInformer()

    verify(informer).close()
    assert(manager.informer == null)
  }

  test("getInformer should throw if the informer has not been initialized") {
    val manager = new InformerManager(kubernetesClient, conf)
    manager.initInformer(applicationId)
    manager.startInformer()
    assert(manager.getInformer() != null)
    manager.stopInformer()
    val e = intercept[IllegalStateException] {
      manager.getInformer()
    }
    assert(e.getMessage.contains("Informer has not been initialized"))
  }

  test("Calling startInformer after stopInformer should throw") {
    val manager = new InformerManager(kubernetesClient, conf)
    manager.initInformer(applicationId)
    manager.startInformer()
    manager.stopInformer()
    val e = intercept[IllegalStateException] {
      manager.initInformer(applicationId)
      manager.startInformer()
    }
    assert(e.getMessage.contains("Cannot run informer after stopInformer() has been called."))
  }
}
