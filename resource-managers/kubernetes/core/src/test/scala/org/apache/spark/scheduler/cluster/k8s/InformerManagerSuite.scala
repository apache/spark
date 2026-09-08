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
import io.fabric8.kubernetes.client.informers.{ExceptionHandler, SharedIndexInformer}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito._
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

  private val resyncInterval = 10000L
  private var conf: SparkConf = _
  private val applicationId = "test-app-id"

  before {
    MockitoAnnotations.openMocks(this).close()
    conf = new SparkConf().set(KUBERNETES_EXECUTOR_INFORMER_RESYNC_INTERVAL, resyncInterval)
    InformerTestUtils.stubInformerBuilder(
      kubernetesClient, podOperations, scopedPods, informer, applicationId, resyncInterval)
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
    verify(scopedPods).runnableInformer(resyncInterval)
  }

  test("initInformer installs an exceptionHandler that forces retries") {
    val manager = new InformerManager(kubernetesClient, conf)
    manager.initInformer(applicationId)

    val handlerCaptor = ArgumentCaptor.forClass(classOf[ExceptionHandler])
    verify(informer).exceptionHandler(handlerCaptor.capture())
    val handler = handlerCaptor.getValue

    assert(handler.retryAfterException(true, new RuntimeException("transient apiserver 5xx")))
    assert(handler.retryAfterException(false, new RuntimeException("startup hiccup")))
    assert(handler.retryAfterException(true, new ClassCastException("truncated response")))
  }

  test("startInformer should call start() when informer is not running") {
    val manager = new InformerManager(kubernetesClient, conf)

    manager.initInformer(applicationId)
    manager.startInformer()

    verify(informer, times(1)).start()
  }

  test("startInformer should not call start() if informer is already running") {
    when(informer.isRunning).thenReturn(true)
    val manager = new InformerManager(kubernetesClient, conf)

    manager.initInformer(applicationId)
    manager.startInformer()

    verify(informer, times(0)).start()
  }

  test("stopInformer should close the informer and null it out") {
    val manager = new InformerManager(kubernetesClient, conf)

    manager.initInformer(applicationId)
    manager.startInformer()
    manager.stopInformer()

    verify(informer).close()
    assert(manager.informer == null)
  }

  test("getInformer should throw when never initialized") {
    val manager = new InformerManager(kubernetesClient, conf)
    val e = intercept[IllegalStateException] {
      manager.getInformer()
    }
    assert(e.getMessage == "Informer has not been initialized. Call initInformer() first.")
  }

  test("getInformer should throw after stopInformer") {
    val manager = new InformerManager(kubernetesClient, conf)
    manager.initInformer(applicationId)
    manager.startInformer()
    assert(manager.getInformer() != null)
    manager.stopInformer()
    val e = intercept[IllegalStateException] {
      manager.getInformer()
    }
    assert(e.getMessage == "Informer has not been initialized. Call initInformer() first.")
  }

  test("initInformer should throw after stopInformer to prevent silent revival") {
    val manager = new InformerManager(kubernetesClient, conf)
    manager.initInformer(applicationId)
    manager.startInformer()
    manager.stopInformer()
    val e = intercept[IllegalStateException] {
      manager.initInformer(applicationId)
    }
    assert(e.getMessage == "Cannot re-initialize informer after stopInformer() has been called.")
  }

  test("startInformer should throw when never initialized") {
    val manager = new InformerManager(kubernetesClient, conf)
    val e = intercept[IllegalStateException] {
      manager.startInformer()
    }
    assert(e.getMessage == "Informer has not been initialized. Call initInformer() first.")
  }

  test("startInformer should throw after stopInformer") {
    val manager = new InformerManager(kubernetesClient, conf)
    manager.initInformer(applicationId)
    manager.startInformer()
    manager.stopInformer()
    val e = intercept[IllegalStateException] {
      manager.startInformer()
    }
    assert(e.getMessage == "Informer has not been initialized. Call initInformer() first.")
  }
}
