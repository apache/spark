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
package org.apache.spark.deploy.k8s

import java.util.function.UnaryOperator

import io.fabric8.kubernetes.api.model.Pod
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import org.apache.hadoop.util.StringUtils
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterEach
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants.EXIT_EXCEPTION_ANNOTATION
import org.apache.spark.deploy.k8s.Fabric8Aliases.PODS

class SparkKubernetesDiagnosticsSetterSuite extends SparkFunSuite
  with MockitoSugar with BeforeAndAfterEach {

  @Mock
  private var client: KubernetesClient = _
  @Mock
  private var clientProvider: KubernetesClientProvider = _
  @Mock
  private var podOperations: PODS = _
  @Mock
  private var driverPodOperations: PodResource = _

  private val driverPodName: String = "driver-pod"
  private val k8sClusterManagerUrl: String = "k8s://dummy"
  private val namespace: String = "default"

  private var setter: SparkKubernetesDiagnosticsSetter = _

  override def beforeEach(): Unit = {
    MockitoAnnotations.openMocks(this)
    when(client.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace(namespace)).thenReturn(podOperations)
    when(podOperations.withName(driverPodName)).thenReturn(driverPodOperations)
    when(clientProvider.create(any(classOf[SparkConf]))).thenReturn(client)
    setter = new SparkKubernetesDiagnosticsSetter(clientProvider)
  }

  test("supports() should return true only for k8s:// URLs") {
    assert(setter.supports(k8sClusterManagerUrl))
    assert(!setter.supports("yarn"))
    assert(!setter.supports("spark://localhost"))
  }

  test("setDiagnostics should patch pod with diagnostics annotation") {
    val diagnostics = new Throwable("Fake diagnostics stack trace")
    val conf = new SparkConf()
      .set(KUBERNETES_DRIVER_MASTER_URL, k8sClusterManagerUrl)
      .set(KUBERNETES_NAMESPACE, namespace)
      .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)

    setter.setDiagnostics(diagnostics, conf)

    val captor: ArgumentCaptor[UnaryOperator[Pod]] =
      ArgumentCaptor.forClass(classOf[UnaryOperator[Pod]])
    verify(driverPodOperations).edit(captor.capture())

    val fn = captor.getValue
    val initialPod = SparkPod.initialPod().pod
    val editedPod = fn.apply(initialPod)

    assert(editedPod.getMetadata.getAnnotations.get(EXIT_EXCEPTION_ANNOTATION)
      == StringUtils.stringifyException(diagnostics))
  }
}
