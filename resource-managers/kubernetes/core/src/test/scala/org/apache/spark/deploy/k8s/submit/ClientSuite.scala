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

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.mockito.MockitoSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.deploy.k8s._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._

class ClientSuite extends SparkFunSuite with BeforeAndAfter {

  private def doReturn(value: Any) = org.mockito.Mockito.doReturn(value, Seq.empty: _*)

  private val DRIVER_POD_UID = "pod-id"
  private val DRIVER_POD_API_VERSION = "v1"
  private val DRIVER_POD_KIND = "pod"
  private val KUBERNETES_RESOURCE_PREFIX = "resource-example"
  private val POD_NAME = "driver"
  private val CONTAINER_NAME = "container"
  private val RESOLVED_JAVA_OPTIONS = Map(
    "conf1key" -> "conf1value",
    "conf2key" -> "conf2value")
  private val BUILT_DRIVER_POD =
    new PodBuilder()
      .withNewMetadata()
        .withName(POD_NAME)
        .endMetadata()
      .withNewSpec()
        .withHostname("localhost")
        .endSpec()
      .build()
  private val BUILT_DRIVER_CONTAINER = new ContainerBuilder().withName(CONTAINER_NAME).build()
  private val ADDITIONAL_RESOURCES = Seq(
    new SecretBuilder().withNewMetadata().withName("secret").endMetadata().build())

  private val BUILT_KUBERNETES_SPEC = KubernetesDriverSpec(
    SparkPod(BUILT_DRIVER_POD, BUILT_DRIVER_CONTAINER),
    ADDITIONAL_RESOURCES,
    RESOLVED_JAVA_OPTIONS)

  private val FULL_EXPECTED_CONTAINER = new ContainerBuilder(BUILT_DRIVER_CONTAINER)
    .addNewEnv()
      .withName(ENV_SPARK_CONF_DIR)
      .withValue(SPARK_CONF_DIR_INTERNAL)
      .endEnv()
    .addNewVolumeMount()
      .withName(SPARK_CONF_VOLUME)
      .withMountPath(SPARK_CONF_DIR_INTERNAL)
      .endVolumeMount()
    .build()
  private val FULL_EXPECTED_POD = new PodBuilder(BUILT_DRIVER_POD)
    .editSpec()
      .addToContainers(FULL_EXPECTED_CONTAINER)
      .addNewVolume()
        .withName(SPARK_CONF_VOLUME)
        .withNewConfigMap().withName(s"$KUBERNETES_RESOURCE_PREFIX-driver-conf-map").endConfigMap()
        .endVolume()
      .endSpec()
    .build()

  private val POD_WITH_OWNER_REFERENCE = new PodBuilder(FULL_EXPECTED_POD)
    .editMetadata()
      .withUid(DRIVER_POD_UID)
      .endMetadata()
    .withApiVersion(DRIVER_POD_API_VERSION)
    .withKind(DRIVER_POD_KIND)
    .build()

  private val ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES = ADDITIONAL_RESOURCES.map { secret =>
    new SecretBuilder(secret)
      .editMetadata()
        .addNewOwnerReference()
          .withName(POD_NAME)
          .withApiVersion(DRIVER_POD_API_VERSION)
          .withKind(DRIVER_POD_KIND)
          .withController(true)
          .withUid(DRIVER_POD_UID)
          .endOwnerReference()
        .endMetadata()
      .build()
  }

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: PODS = _

  @Mock
  private var namedPods: PodResource[Pod, DoneablePod] = _

  @Mock
  private var loggingPodStatusWatcher: LoggingPodStatusWatcher = _

  @Mock
  private var driverBuilder: KubernetesDriverBuilder = _

  @Mock
  private var resourceList: RESOURCE_LIST = _

  private var kconf: KubernetesDriverConf = _
  private var createdPodArgumentCaptor: ArgumentCaptor[Pod] = _
  private var createdResourcesArgumentCaptor: ArgumentCaptor[HasMetadata] = _

  before {
    MockitoAnnotations.initMocks(this)
    kconf = KubernetesTestConf.createDriverConf(
      resourceNamePrefix = Some(KUBERNETES_RESOURCE_PREFIX))
    when(driverBuilder.buildFromFeatures(kconf, kubernetesClient)).thenReturn(BUILT_KUBERNETES_SPEC)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(POD_NAME)).thenReturn(namedPods)

    createdPodArgumentCaptor = ArgumentCaptor.forClass(classOf[Pod])
    createdResourcesArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    when(podOperations.create(FULL_EXPECTED_POD)).thenReturn(POD_WITH_OWNER_REFERENCE)
    when(namedPods.watch(loggingPodStatusWatcher)).thenReturn(mock[Watch])
    doReturn(resourceList)
      .when(kubernetesClient)
      .resourceList(createdResourcesArgumentCaptor.capture())
  }

  test("The client should configure the pod using the builder.") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      false,
      loggingPodStatusWatcher)
    submissionClient.run()
    verify(podOperations).create(FULL_EXPECTED_POD)
  }

  test("The client should create Kubernetes resources") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      false,
      loggingPodStatusWatcher)
    submissionClient.run()
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues
    assert(otherCreatedResources.size === 2)
    val secrets = otherCreatedResources.toArray.filter(_.isInstanceOf[Secret]).toSeq
    assert(secrets === ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES)
    val configMaps = otherCreatedResources.toArray
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap])
    assert(secrets.nonEmpty)
    assert(configMaps.nonEmpty)
    val configMap = configMaps.head
    assert(configMap.getMetadata.getName ===
      s"$KUBERNETES_RESOURCE_PREFIX-driver-conf-map")
    assert(configMap.getData.containsKey(SPARK_CONF_FILE_NAME))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf1key=conf1value"))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf2key=conf2value"))
  }

  test("Waiting for app completion should stall on the watcher") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      true,
      loggingPodStatusWatcher)
    submissionClient.run()
    verify(loggingPodStatusWatcher).awaitCompletion()
  }
}
