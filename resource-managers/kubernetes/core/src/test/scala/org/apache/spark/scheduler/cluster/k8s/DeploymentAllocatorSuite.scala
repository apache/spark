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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apps.Deployment
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.{AppsAPIGroupDSL, PodResource}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.{never, times, verify, when}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SecurityManager, SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesExecutorSpec}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.resource.{ResourceProfile, ResourceProfileBuilder}
import org.apache.spark.scheduler.cluster.k8s.ExecutorLifecycleTestUtils._

class DeploymentAllocatorSuite extends SparkFunSuite with BeforeAndAfter {

  private val driverPodName = "driver"

  private val driverPod = new PodBuilder()
    .withNewMetadata()
      .withName(driverPodName)
      .withUid("driver-pod-uid")
      .addToLabels(SPARK_APP_ID_LABEL, TEST_SPARK_APP_ID)
      .addToLabels(SPARK_ROLE_LABEL, SPARK_POD_DRIVER_ROLE)
      .endMetadata()
    .build()

  private val conf = new SparkConf()
    .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
    .set(KUBERNETES_ALLOCATION_PODS_ALLOCATOR, "deployment")

  private val secMgr = new SecurityManager(conf)
  private val defaultProfile = ResourceProfile.getOrCreateDefaultProfile(conf)

  private val schedulerBackendAppId = "testapp"

  @Mock private var kubernetesClient: KubernetesClient = _
  @Mock private var appsClient: AppsAPIGroupDSL = _
  @Mock private var deployments: DEPLOYMENTS = _
  @Mock private var deploymentsNamespaced: DEPLOYMENTS_NAMESPACED = _
  @Mock private var deploymentResource: DEPLOYMENT_RES = _
  @Mock private var pods: PODS = _
  @Mock private var podsNamespaced: PODS_WITH_NAMESPACE = _
  @Mock private var driverPodResource: PodResource = _
  @Mock private var executorPodResource: PodResource = _
  @Mock private var executorBuilder: KubernetesExecutorBuilder = _
  @Mock private var schedulerBackend: KubernetesClusterSchedulerBackend = _

  private var allocator: DeploymentPodsAllocator = _
  private var snapshotsStore: DeterministicExecutorPodsSnapshotsStore = _

  before {
    MockitoAnnotations.openMocks(this).close()
    when(kubernetesClient.apps()).thenReturn(appsClient)
    when(appsClient.deployments()).thenReturn(deployments)
    when(deployments.inNamespace("default")).thenReturn(deploymentsNamespaced)
    when(deploymentsNamespaced.resource(any(classOf[Deployment]))).thenReturn(deploymentResource)
    when(deploymentsNamespaced.withName(any[String]())).thenReturn(deploymentResource)

    when(kubernetesClient.pods()).thenReturn(pods)
    when(pods.inNamespace("default")).thenReturn(podsNamespaced)
    when(podsNamespaced.withName(driverPodName)).thenReturn(driverPodResource)
    when(podsNamespaced.resource(any(classOf[Pod]))).thenReturn(executorPodResource)
    when(driverPodResource.get).thenReturn(driverPod)
    when(driverPodResource.waitUntilReady(any(), any())).thenReturn(driverPod)
    when(executorBuilder.buildFromFeatures(
      any(classOf[KubernetesExecutorConf]),
      meq(secMgr),
      meq(kubernetesClient),
      any(classOf[ResourceProfile])))
      .thenAnswer { invocation =>
        val k8sConf = invocation.getArgument[KubernetesExecutorConf](0)
        KubernetesExecutorSpec(
          executorPodWithId(0, k8sConf.resourceProfileId),
          Seq.empty)
      }

    snapshotsStore = new DeterministicExecutorPodsSnapshotsStore
    allocator = new DeploymentPodsAllocator(
      conf,
      secMgr,
      executorBuilder,
      kubernetesClient,
      snapshotsStore,
      snapshotsStore.clock)

    when(schedulerBackend.getExecutorIds()).thenReturn(Seq.empty)
    allocator.start(TEST_SPARK_APP_ID, schedulerBackend)
  }

  after {
    ResourceProfile.clearDefaultProfile()
  }

  test("creates deployments per resource profile and seeds deletion cost annotation") {
    val rpBuilder = new ResourceProfileBuilder()
    val secondProfile = rpBuilder.build()

    allocator.setTotalExpectedExecutors(
      Map(defaultProfile -> 3, secondProfile -> 2))

    val captor = ArgumentCaptor.forClass(classOf[Deployment])
    verify(deploymentsNamespaced, times(2)).resource(captor.capture())
    verify(deploymentResource, times(2)).create()

    val createdDeployments = captor.getAllValues.asScala
    createdDeployments.foreach { deployment =>
      assert(deployment.getMetadata.getNamespace === "default")
      assert(deployment.getSpec.getTemplate.getMetadata
        .getAnnotations.get("controller.kubernetes.io/pod-deletion-cost") === "0")
      assert(deployment.getSpec.getTemplate.getSpec.getContainers.asScala.exists(
        _.getName == "spark-executor"))
      val selectorLabels = deployment.getSpec.getSelector.getMatchLabels.asScala
      assert(selectorLabels(SPARK_APP_ID_LABEL) === TEST_SPARK_APP_ID)
      assert(selectorLabels(SPARK_ROLE_LABEL) === SPARK_POD_EXECUTOR_ROLE)
    }
  }

  test("scales existing deployment when replicas change") {
    allocator.setTotalExpectedExecutors(Map(defaultProfile -> 5))
    verify(deploymentResource, times(1)).create()

    allocator.setTotalExpectedExecutors(Map(defaultProfile -> 7))
    verify(deploymentResource).scale(7)
  }

  test("throws when executor template contributes dynamic PVCs") {
    val pvc = persistentVolumeClaim("spark-pvc", "standard", "1Gi")
    when(executorBuilder.buildFromFeatures(
      any(classOf[KubernetesExecutorConf]),
      meq(secMgr),
      meq(kubernetesClient),
      any(classOf[ResourceProfile])))
      .thenReturn(KubernetesExecutorSpec(
        executorPodWithId(0),
        Seq(pvc)))

    val error = intercept[SparkException] {
      allocator.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    }
    assert(error.getMessage.contains("PersistentVolumeClaims are not supported"))
    verify(deploymentResource, never()).create()
  }

  test("throws when executor template includes static PVC references") {
    when(executorBuilder.buildFromFeatures(
      any(classOf[KubernetesExecutorConf]),
      meq(secMgr),
      meq(kubernetesClient),
      any(classOf[ResourceProfile])))
      .thenReturn(KubernetesExecutorSpec(
        executorPodWithIdAndVolume(0),
        Seq.empty))

    val error = intercept[SparkException] {
      allocator.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    }
    assert(error.getMessage.contains("PersistentVolumeClaims are not supported"))
    verify(deploymentResource, never()).create()
  }

  test("deletes deployments on stop") {
    allocator.setTotalExpectedExecutors(Map(defaultProfile -> 1))
    allocator.stop(schedulerBackendAppId)
    verify(deploymentResource).delete()
  }
}
