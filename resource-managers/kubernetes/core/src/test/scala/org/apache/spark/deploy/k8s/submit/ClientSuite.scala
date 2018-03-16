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

import scala.collection.JavaConverters._

import com.google.common.collect.Iterables
import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.dsl.{MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, PodResource}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito.{doReturn, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatest.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.steps.DriverConfigurationStep

class ClientSuite extends SparkFunSuite with BeforeAndAfter {

  private val DRIVER_POD_UID = "pod-id"
  private val DRIVER_POD_API_VERSION = "v1"
  private val DRIVER_POD_KIND = "pod"
  private val KUBERNETES_RESOURCE_PREFIX = "resource-example"

  private type ResourceList = NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable[
      HasMetadata, Boolean]
  private type Pods = MixedOperation[Pod, PodList, DoneablePod, PodResource[Pod, DoneablePod]]

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var podOperations: Pods = _

  @Mock
  private var namedPods: PodResource[Pod, DoneablePod] = _

  @Mock
  private var loggingPodStatusWatcher: LoggingPodStatusWatcher = _

  @Mock
  private var resourceList: ResourceList = _

  private val submissionSteps = Seq(FirstTestConfigurationStep, SecondTestConfigurationStep)
  private var createdPodArgumentCaptor: ArgumentCaptor[Pod] = _
  private var createdResourcesArgumentCaptor: ArgumentCaptor[HasMetadata] = _
  private var createdContainerArgumentCaptor: ArgumentCaptor[Container] = _

  before {
    MockitoAnnotations.initMocks(this)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(FirstTestConfigurationStep.podName)).thenReturn(namedPods)

    createdPodArgumentCaptor = ArgumentCaptor.forClass(classOf[Pod])
    createdResourcesArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    when(podOperations.create(createdPodArgumentCaptor.capture())).thenAnswer(new Answer[Pod] {
      override def answer(invocation: InvocationOnMock): Pod = {
        new PodBuilder(invocation.getArgumentAt(0, classOf[Pod]))
          .editMetadata()
            .withUid(DRIVER_POD_UID)
            .endMetadata()
          .withApiVersion(DRIVER_POD_API_VERSION)
          .withKind(DRIVER_POD_KIND)
          .build()
      }
    })
    when(podOperations.withName(FirstTestConfigurationStep.podName)).thenReturn(namedPods)
    when(namedPods.watch(loggingPodStatusWatcher)).thenReturn(mock[Watch])
    doReturn(resourceList)
      .when(kubernetesClient)
      .resourceList(createdResourcesArgumentCaptor.capture())
  }

  test("The client should configure the pod with the submission steps.") {
    val submissionClient = new Client(
      submissionSteps,
      new SparkConf(false),
      kubernetesClient,
      false,
      "spark",
      loggingPodStatusWatcher,
      KUBERNETES_RESOURCE_PREFIX)
    submissionClient.run()
    val createdPod = createdPodArgumentCaptor.getValue
    assert(createdPod.getMetadata.getName === FirstTestConfigurationStep.podName)
    assert(createdPod.getMetadata.getLabels.asScala ===
      Map(FirstTestConfigurationStep.labelKey -> FirstTestConfigurationStep.labelValue))
    assert(createdPod.getMetadata.getAnnotations.asScala ===
      Map(SecondTestConfigurationStep.annotationKey ->
        SecondTestConfigurationStep.annotationValue))
    assert(createdPod.getSpec.getContainers.size() === 1)
    assert(createdPod.getSpec.getContainers.get(0).getName ===
      SecondTestConfigurationStep.containerName)
  }

  test("The client should create Kubernetes resources") {
    val EXAMPLE_JAVA_OPTS = "-XX:+HeapDumpOnOutOfMemoryError -XX:+PrintGCDetails"
    val EXPECTED_JAVA_OPTS = "-XX\\:+HeapDumpOnOutOfMemoryError -XX\\:+PrintGCDetails"
    val submissionClient = new Client(
      submissionSteps,
      new SparkConf(false)
        .set(org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS, EXAMPLE_JAVA_OPTS),
      kubernetesClient,
      false,
      "spark",
      loggingPodStatusWatcher,
      KUBERNETES_RESOURCE_PREFIX)
    submissionClient.run()
    val createdPod = createdPodArgumentCaptor.getValue
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues
    assert(otherCreatedResources.size === 2)
    val secrets = otherCreatedResources.toArray
      .filter(_.isInstanceOf[Secret]).map(_.asInstanceOf[Secret])
    val configMaps = otherCreatedResources.toArray
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap])
    assert(secrets.nonEmpty)
    val secret = secrets.head
    assert(secret.getMetadata.getName === FirstTestConfigurationStep.secretName)
    assert(secret.getData.asScala ===
      Map(FirstTestConfigurationStep.secretKey -> FirstTestConfigurationStep.secretData))
    val ownerReference = Iterables.getOnlyElement(secret.getMetadata.getOwnerReferences)
    assert(ownerReference.getName === createdPod.getMetadata.getName)
    assert(ownerReference.getKind === DRIVER_POD_KIND)
    assert(ownerReference.getUid === DRIVER_POD_UID)
    assert(ownerReference.getApiVersion === DRIVER_POD_API_VERSION)
    assert(configMaps.nonEmpty)
    val configMap = configMaps.head
    assert(configMap.getMetadata.getName ===
      s"$KUBERNETES_RESOURCE_PREFIX-driver-conf-map")
    assert(configMap.getData.containsKey(SPARK_CONF_FILE_NAME))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains(EXPECTED_JAVA_OPTS))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains(
      "spark.custom-conf=custom-conf-value"))
    val driverContainer = Iterables.getOnlyElement(createdPod.getSpec.getContainers)
    assert(driverContainer.getName === SecondTestConfigurationStep.containerName)
    val driverEnv = driverContainer.getEnv.asScala.head
    assert(driverEnv.getName === ENV_SPARK_CONF_DIR)
    assert(driverEnv.getValue === SPARK_CONF_DIR_INTERNAL)
    val driverMount = driverContainer.getVolumeMounts.asScala.head
    assert(driverMount.getName === SPARK_CONF_VOLUME)
    assert(driverMount.getMountPath === SPARK_CONF_DIR_INTERNAL)
  }

  test("Waiting for app completion should stall on the watcher") {
    val submissionClient = new Client(
      submissionSteps,
      new SparkConf(false),
      kubernetesClient,
      true,
      "spark",
      loggingPodStatusWatcher,
      KUBERNETES_RESOURCE_PREFIX)
    submissionClient.run()
    verify(loggingPodStatusWatcher).awaitCompletion()
  }

}

private object FirstTestConfigurationStep extends DriverConfigurationStep {

  val podName = "test-pod"
  val secretName = "test-secret"
  val labelKey = "first-submit"
  val labelValue = "true"
  val secretKey = "secretKey"
  val secretData = "secretData"

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val modifiedPod = new PodBuilder(driverSpec.driverPod)
      .editMetadata()
      .withName(podName)
      .addToLabels(labelKey, labelValue)
      .endMetadata()
      .build()
    val additionalResource = new SecretBuilder()
      .withNewMetadata()
      .withName(secretName)
      .endMetadata()
      .addToData(secretKey, secretData)
      .build()
    driverSpec.copy(
      driverPod = modifiedPod,
      otherKubernetesResources = driverSpec.otherKubernetesResources ++ Seq(additionalResource))
  }
}

private object SecondTestConfigurationStep extends DriverConfigurationStep {
  val annotationKey = "second-submit"
  val annotationValue = "submitted"
  val sparkConfKey = "spark.custom-conf"
  val sparkConfValue = "custom-conf-value"
  val containerName = "driverContainer"
  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    val modifiedPod = new PodBuilder(driverSpec.driverPod)
      .editMetadata()
        .addToAnnotations(annotationKey, annotationValue)
        .endMetadata()
      .build()
    val resolvedSparkConf = driverSpec.driverSparkConf.clone().set(sparkConfKey, sparkConfValue)
    val modifiedContainer = new ContainerBuilder(driverSpec.driverContainer)
      .withName(containerName)
      .build()
    driverSpec.copy(
      driverPod = modifiedPod,
      driverSparkConf = resolvedSparkConf,
      driverContainer = modifiedContainer)
  }
}
