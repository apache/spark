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
package org.apache.spark.deploy.k8s.submit

import java.io.PrintStream
import java.util.Arrays
import scala.jdk.CollectionConverters._
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, PropagationPolicyConfigurable}
import io.fabric8.kubernetes.client.dsl.{Deletable, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, PodResource}
import io.fabric8.kubernetes.client.okhttp.OkHttpClientImpl
import org.mockito.{ArgumentMatchers, Mock, MockitoAnnotations}
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.BeforeAndAfter
import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_SUBMIT_GRACE_PERIOD
import org.apache.spark.deploy.k8s.Constants.{SPARK_APP_ID_LABEL, SPARK_POD_DRIVER_ROLE, SPARK_ROLE_LABEL}
import org.apache.spark.deploy.k8s.Fabric8Aliases.{PODS, PODS_WITH_NAMESPACE}
import org.apache.spark.deploy.k8s.SparkKubernetesClientFactory
import org.apache.spark.deploy.k8s.SparkKubernetesClientFactory.ClientType
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils.TEST_SPARK_APP_ID

class K8sSubmitOpSuite extends SparkFunSuite with BeforeAndAfter {
  private val driverPodName1 = "driver1"
  private val driverPodName2 = "driver2"
  private val driverPod1 = buildDriverPod(driverPodName1, "1")
  private val driverPod2 = buildDriverPod(driverPodName2, "2")
  private val podList = List(driverPod1, driverPod2)
  private val namespace = "test"

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _

  @Mock
  private var driverPodOperations1: PodResource = _

  @Mock
  private var driverPodOperations2: PodResource = _

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var deletable: PropagationPolicyConfigurable[_ <: Deletable] = _

  @Mock
  private var deletableList
      : NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable[HasMetadata] = _

  @Mock
  private var err: PrintStream = _

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  before {
    MockitoAnnotations.openMocks(this).close()
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace(namespace)).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withName(driverPodName1)).thenReturn(driverPodOperations1)
    when(podsWithNamespace.withName(driverPodName2)).thenReturn(driverPodOperations2)
    when(driverPodOperations1.get).thenReturn(driverPod1)
    when(driverPodOperations1.delete()).thenReturn(Arrays.asList(new StatusDetails))
    when(driverPodOperations2.get).thenReturn(driverPod2)
    when(driverPodOperations2.delete()).thenReturn(Arrays.asList(new StatusDetails))
  }

  test("List app status") {
    implicit val kubeClient: KubernetesClient = kubernetesClient
    val listStatus = new ListStatus
    listStatus.printStream = err
    listStatus.executeOnPod(driverPodName1, Option(namespace), new SparkConf())
    // scalastyle:off
    verify(err).println(ArgumentMatchers.eq(getPodStatus(driverPodName1, "1")))
    // scalastyle:on
  }

  test("List status for multiple apps with glob") {
    implicit val kubeClient: KubernetesClient = kubernetesClient
    val listStatus = new ListStatus
    listStatus.printStream = err
    listStatus.executeOnGlob(podList, Option(namespace), new SparkConf())
    // scalastyle:off
    verify(err).println(ArgumentMatchers.eq(getPodStatus(driverPodName1, "1")))
    verify(err).println(ArgumentMatchers.eq(getPodStatus(driverPodName2, "2")))
    // scalastyle:on
  }

  test("Kill app") {
    implicit val kubeClient: KubernetesClient = kubernetesClient
    val killApp = new KillApplication
    killApp.executeOnPod(driverPodName1, Option(namespace), new SparkConf())
    verify(driverPodOperations1, times(1)).delete()
  }

  test("Kill app with gracePeriod") {
    implicit val kubeClient: KubernetesClient = kubernetesClient
    val killApp = new KillApplication
    val conf = new SparkConf().set(KUBERNETES_SUBMIT_GRACE_PERIOD, 1L)
    doReturn(deletable).when(driverPodOperations1).withGracePeriod(1L)
    killApp.executeOnPod(driverPodName1, Option(namespace), conf)
    verify(driverPodOperations1, times(1)).withGracePeriod(1L)
    verify(deletable, times(1)).delete()
  }

  test("Kill multiple apps with glob without gracePeriod") {
    implicit val kubeClient: KubernetesClient = kubernetesClient
    val killApp = new KillApplication
    killApp.printStream = err
    doReturn(deletableList).when(kubernetesClient).resourceList(podList.asJava)
    killApp.executeOnGlob(podList, Option(namespace), new SparkConf())
    verify(deletableList, times(1)).delete()
    // scalastyle:off
    verify(err).println(ArgumentMatchers.eq(s"Deleting driver pod: $driverPodName1."))
    verify(err).println(ArgumentMatchers.eq(s"Deleting driver pod: $driverPodName2."))
    // scalastyle:on
  }

  test("Init kubernetes client") {
    val kubeClient = SparkKubernetesClientFactory.createKubernetesClient(
      "master",
      None,
      "",
      ClientType.Driver,
      new SparkConf(false),
      None)
    assert(kubeClient.getHttpClient.isInstanceOf[OkHttpClientImpl])
  }

  private def buildDriverPod(podName: String, id: String): Pod = {
    new PodBuilder()
      .withNewMetadata()
      .withName(podName)
      .withNamespace(namespace)
      .addToLabels(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)
      .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
      .withUid(s"driver-pod-$id")
      .endMetadata()
      .withNewSpec()
      .withServiceAccountName(s"test$id")
      .withVolumes()
      .withNodeName(s"testNode$id")
      .endSpec()
      .withNewStatus()
      .withPhase("Running")
      .endStatus()
      .build()
  }

  private def getPodStatus(podName: String, id: String): String = {
    "Application status (driver): " +
      s"""|${"\n\t"} pod name: $podName
          |${"\t"} namespace: N/A
          |${"\t"} labels: spark-app-selector -> spark-app-id, spark-role -> driver
          |${"\t"} pod uid: driver-pod-$id
          |${"\t"} creation time: N/A
          |${"\t"} service account name: test$id
          |${"\t"} volumes: N/A
          |${"\t"} node name: testNode$id
          |${"\t"} start time: N/A
          |${"\t"} phase: Running
          |${"\t"} container status: N/A""".stripMargin
  }
}
