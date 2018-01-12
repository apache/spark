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

import com.google.common.collect.Iterables
import io.fabric8.kubernetes.api.model.{ContainerBuilder, DoneablePod, HasMetadata, Pod, PodBuilder, PodList, Secret, SecretBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.dsl.{MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, NamespaceVisitFromServerGetWatchDeleteRecreateWaitApplicable, PodResource, Resource}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito.{doReturn, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatest.mock.MockitoSugar._
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.submitsteps.{DriverConfigurationStep, KubernetesDriverSpec}

class ClientSuite extends SparkFunSuite with BeforeAndAfter {

  private val DRIVER_POD_UID = "pod-id"
  private val DRIVER_POD_API_VERSION = "v1"
  private val DRIVER_POD_KIND = "pod"

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
        loggingPodStatusWatcher)
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

  test("The client should create the secondary Kubernetes resources.") {
    val submissionClient = new Client(
        submissionSteps,
        new SparkConf(false),
        kubernetesClient,
        false,
        "spark",
        loggingPodStatusWatcher)
    submissionClient.run()
    val createdPod = createdPodArgumentCaptor.getValue
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues
    assert(otherCreatedResources.size === 1)
    val createdResource = Iterables.getOnlyElement(otherCreatedResources).asInstanceOf[Secret]
    assert(createdResource.getMetadata.getName === FirstTestConfigurationStep.secretName)
    assert(createdResource.getData.asScala ===
        Map(FirstTestConfigurationStep.secretKey -> FirstTestConfigurationStep.secretData))
    val ownerReference = Iterables.getOnlyElement(createdResource.getMetadata.getOwnerReferences)
    assert(ownerReference.getName === createdPod.getMetadata.getName)
    assert(ownerReference.getKind === DRIVER_POD_KIND)
    assert(ownerReference.getUid === DRIVER_POD_UID)
    assert(ownerReference.getApiVersion === DRIVER_POD_API_VERSION)
  }

  test("The client should attach the driver container with the appropriate JVM options.") {
    val sparkConf = new SparkConf(false)
        .set("spark.logConf", "true")
        .set(
          org.apache.spark.internal.config.DRIVER_JAVA_OPTIONS,
          "-XX:+HeapDumpOnOutOfMemoryError -XX:+PrintGCDetails")
    val submissionClient = new Client(
        submissionSteps,
        sparkConf,
        kubernetesClient,
        false,
        "spark",
        loggingPodStatusWatcher)
    submissionClient.run()
    val createdPod = createdPodArgumentCaptor.getValue
    val driverContainer = Iterables.getOnlyElement(createdPod.getSpec.getContainers)
    assert(driverContainer.getName === SecondTestConfigurationStep.containerName)
    val driverJvmOptsEnvs = driverContainer.getEnv.asScala.filter { env =>
      env.getName.startsWith(ENV_JAVA_OPT_PREFIX)
    }.sortBy(_.getName)
    assert(driverJvmOptsEnvs.size === 4)

    val expectedJvmOptsValues = Seq(
        "-Dspark.logConf=true",
        s"-D${SecondTestConfigurationStep.sparkConfKey}=" +
            s"${SecondTestConfigurationStep.sparkConfValue}",
        s"-XX:+HeapDumpOnOutOfMemoryError",
        s"-XX:+PrintGCDetails")
    driverJvmOptsEnvs.zip(expectedJvmOptsValues).zipWithIndex.foreach {
      case ((resolvedEnv, expectedJvmOpt), index) =>
        assert(resolvedEnv.getName === s"$ENV_JAVA_OPT_PREFIX$index")
        assert(resolvedEnv.getValue === expectedJvmOpt)
    }
  }

  test("Waiting for app completion should stall on the watcher") {
    val submissionClient = new Client(
      submissionSteps,
      new SparkConf(false),
      kubernetesClient,
      true,
      "spark",
      loggingPodStatusWatcher)
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
