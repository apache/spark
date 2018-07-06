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
import io.fabric8.kubernetes.client.dsl.{ExtensionsAPIGroupDSL, MixedOperation, NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable, ScalableResource}
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito.{doReturn, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpec, KubernetesDriverSpecificConf, SparkJob}
import org.apache.spark.deploy.k8s.Constants._

class ClientSuite extends SparkFunSuite with BeforeAndAfter {

  private val DRIVER_JOB_UID = "job-id"
  private val DRIVER_JOB_API_VERSION = "v1"
  private val DRIVER_JOB_KIND = "job"
  private val KUBERNETES_RESOURCE_PREFIX = "resource-example"
  private val JOB_NAME = "driver"
  private val CONTAINER_NAME = "container"
  private val APP_ID = "app-id"
  private val APP_NAME = "app"
  private val MAIN_CLASS = "main"
  private val APP_ARGS = Seq("arg1", "arg2")
  private val RESOLVED_JAVA_OPTIONS = Map(
    "conf1key" -> "conf1value",
    "conf2key" -> "conf2value")
  private val BUILT_DRIVER_JOB =
    new JobBuilder()
      .withNewMetadata()
      .withName(JOB_NAME)
      .endMetadata()
      .withNewSpec()
      .withNewTemplate()
      .withNewSpec()
      .withHostname("localhost")
      .endSpec()
      .endTemplate()
      .endSpec()
      .build()
  private val BUILT_DRIVER_CONTAINER = new ContainerBuilder().withName(CONTAINER_NAME).build()
  private val ADDITIONAL_RESOURCES = Seq(
    new SecretBuilder().withNewMetadata().withName("secret").endMetadata().build())

  private val BUILT_KUBERNETES_SPEC = KubernetesDriverSpec(
    SparkJob(BUILT_DRIVER_JOB, BUILT_DRIVER_CONTAINER),
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
  private val FULL_EXPECTED_JOB = new JobBuilder(BUILT_DRIVER_JOB)
    .editSpec()
      .editTemplate()
        .editSpec()
          .addToContainers(FULL_EXPECTED_CONTAINER)
          .addNewVolume()
            .withName(SPARK_CONF_VOLUME)
            .withNewConfigMap()
              .withName(s"$KUBERNETES_RESOURCE_PREFIX-driver-conf-map")
            .endConfigMap()
          .endVolume()
          .withRestartPolicy("OnFailure")
        .endSpec()
      .endTemplate()
    .endSpec()
    .build()

  // BackoffLimit needs to be set after creation
  FULL_EXPECTED_JOB.getSpec.setAdditionalProperty("backoffLimit", 6)

  private val JOB_WITH_OWNER_REFERENCE = new JobBuilder(FULL_EXPECTED_JOB)
    .editMetadata()
      .withUid(DRIVER_JOB_UID)
      .withName(JOB_NAME)
    .endMetadata()
    .withApiVersion(DRIVER_JOB_API_VERSION)
    .withKind(DRIVER_JOB_KIND)
    .build()

  private val ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES = ADDITIONAL_RESOURCES.map { secret =>
    new SecretBuilder(secret)
      .editMetadata()
        .addNewOwnerReference()
          .withName(JOB_NAME)
          .withApiVersion(DRIVER_JOB_API_VERSION)
          .withKind(DRIVER_JOB_KIND)
          .withController(true)
          .withUid(DRIVER_JOB_UID)
        .endOwnerReference()
      .endMetadata()
      .build()
  }

  private type ResourceList = NamespaceListVisitFromServerGetDeleteRecreateWaitApplicable[
    HasMetadata, Boolean]
  private type Jobs = MixedOperation[Job, JobList, DoneableJob, ScalableResource[Job, DoneableJob]]

  @Mock
  private var kubernetesClient: KubernetesClient = _

  @Mock
  private var extension: ExtensionsAPIGroupDSL = _

  @Mock
  private var jobOperations: Jobs = _

  @Mock
  private var namedJobs: ScalableResource[Job, DoneableJob] = _

  @Mock
  private var loggingJobStatusWatcher: LoggingJobStatusWatcher = _

  @Mock
  private var driverBuilder: KubernetesDriverBuilder = _

  @Mock
  private var resourceList: ResourceList = _

  @Mock
  private var jobMetadata: ObjectMeta = _

  private var kubernetesConf: KubernetesConf[KubernetesDriverSpecificConf] = _

  private var sparkConf: SparkConf = _
  private var createdJobArgumentCaptor: ArgumentCaptor[Job] = _
  private var createdResourcesArgumentCaptor: ArgumentCaptor[HasMetadata] = _

  before {
    MockitoAnnotations.initMocks(this)
    sparkConf = new SparkConf(false)
    kubernetesConf = KubernetesConf[KubernetesDriverSpecificConf](
      sparkConf,
      KubernetesDriverSpecificConf(None, MAIN_CLASS, APP_NAME, APP_ARGS),
      KUBERNETES_RESOURCE_PREFIX,
      APP_ID,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Map.empty,
      Nil,
      Seq.empty[String])
    when(driverBuilder.buildFromFeatures(kubernetesConf)).thenReturn(BUILT_KUBERNETES_SPEC)
    when(kubernetesClient.extensions()).thenReturn(extension)
    when(kubernetesClient.extensions().jobs()).thenReturn(jobOperations)
    when(jobOperations.withName(JOB_NAME)).thenReturn(namedJobs)

    createdJobArgumentCaptor = ArgumentCaptor.forClass(classOf[Job])
    createdResourcesArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    when(jobOperations.create(FULL_EXPECTED_JOB)).thenReturn(JOB_WITH_OWNER_REFERENCE)
    when(namedJobs.watch(loggingJobStatusWatcher)).thenReturn(mock[Watch])
    doReturn(resourceList)
      .when(kubernetesClient)
      .resourceList(createdResourcesArgumentCaptor.capture())
  }

  test("The client should configure the job using the builder.") {
    val submissionClient = new Client(
      driverBuilder,
      kubernetesConf,
      kubernetesClient,
      false,
      "spark",
      loggingJobStatusWatcher,
      KUBERNETES_RESOURCE_PREFIX)
    submissionClient.run()
    verify(jobOperations).create(FULL_EXPECTED_JOB)
  }

  test("The client should create Kubernetes resources") {
    val submissionClient = new Client(
      driverBuilder,
      kubernetesConf,
      kubernetesClient,
      false,
      "spark",
      loggingJobStatusWatcher,
      KUBERNETES_RESOURCE_PREFIX)
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
      driverBuilder,
      kubernetesConf,
      kubernetesClient,
      true,
      "spark",
      loggingJobStatusWatcher,
      KUBERNETES_RESOURCE_PREFIX)
    submissionClient.run()
    verify(loggingJobStatusWatcher).awaitCompletion()
  }
}
