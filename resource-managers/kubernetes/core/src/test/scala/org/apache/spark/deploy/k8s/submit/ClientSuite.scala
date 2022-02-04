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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apiextensions.v1.{CustomResourceDefinition, CustomResourceDefinitionBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{Config, _}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.util.Utils

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

  private val PRE_RESOURCES = Seq(
    new CustomResourceDefinitionBuilder().withNewMetadata().withName("preCRD").endMetadata().build()
  )
  private val BUILT_KUBERNETES_SPEC = KubernetesDriverSpec(
    SparkPod(BUILT_DRIVER_POD, BUILT_DRIVER_CONTAINER),
    Nil,
    ADDITIONAL_RESOURCES,
    RESOLVED_JAVA_OPTIONS)
  private val BUILT_KUBERNETES_SPEC_WITH_PRERES = KubernetesDriverSpec(
    SparkPod(BUILT_DRIVER_POD, BUILT_DRIVER_CONTAINER),
    PRE_RESOURCES,
    ADDITIONAL_RESOURCES,
    RESOLVED_JAVA_OPTIONS)

  private val FULL_EXPECTED_CONTAINER = new ContainerBuilder(BUILT_DRIVER_CONTAINER)
    .addNewEnv()
      .withName(ENV_SPARK_CONF_DIR)
      .withValue(SPARK_CONF_DIR_INTERNAL)
      .endEnv()
    .addNewVolumeMount()
      .withName(SPARK_CONF_VOLUME_DRIVER)
      .withMountPath(SPARK_CONF_DIR_INTERNAL)
      .endVolumeMount()
    .build()

  private val KEY_TO_PATH =
    new KeyToPath(SPARK_CONF_FILE_NAME, 420, SPARK_CONF_FILE_NAME)

  private def fullExpectedPod(keyToPaths: List[KeyToPath] = List(KEY_TO_PATH)) =
    new PodBuilder(BUILT_DRIVER_POD)
      .editSpec()
        .addToContainers(FULL_EXPECTED_CONTAINER)
        .addNewVolume()
          .withName(SPARK_CONF_VOLUME_DRIVER)
          .withNewConfigMap()
            .withItems(keyToPaths.asJava)
            .withName(KubernetesClientUtils.configMapNameDriver)
            .endConfigMap()
          .endVolume()
        .endSpec()
      .build()

  private def podWithOwnerReference(keyToPaths: List[KeyToPath] = List(KEY_TO_PATH)) =
    new PodBuilder(fullExpectedPod(keyToPaths))
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

  private val PRE_ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES = PRE_RESOURCES.map { crd =>
    new CustomResourceDefinitionBuilder(crd)
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
  private var namedPods: PodResource[Pod] = _

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
    MockitoAnnotations.openMocks(this).close()
    kconf = KubernetesTestConf.createDriverConf(
      resourceNamePrefix = Some(KUBERNETES_RESOURCE_PREFIX))
    when(driverBuilder.buildFromFeatures(kconf, kubernetesClient)).thenReturn(BUILT_KUBERNETES_SPEC)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.withName(POD_NAME)).thenReturn(namedPods)

    createdPodArgumentCaptor = ArgumentCaptor.forClass(classOf[Pod])
    createdResourcesArgumentCaptor = ArgumentCaptor.forClass(classOf[HasMetadata])
    when(podOperations.create(fullExpectedPod())).thenReturn(podWithOwnerReference())
    when(namedPods.watch(loggingPodStatusWatcher)).thenReturn(mock[Watch])
    when(loggingPodStatusWatcher.watchOrStop(kconf.namespace + ":" + POD_NAME)).thenReturn(true)
    doReturn(resourceList)
      .when(kubernetesClient)
      .resourceList(createdResourcesArgumentCaptor.capture())
  }

  test("The client should configure the pod using the builder.") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    verify(podOperations).create(fullExpectedPod())
  }

  test("The client should create Kubernetes resources") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
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
      KubernetesClientUtils.configMapNameDriver)
    assert(configMap.getImmutable())
    assert(configMap.getData.containsKey(SPARK_CONF_FILE_NAME))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf1key=conf1value"))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf2key=conf2value"))
  }

  test("SPARK-37331: The client should create Kubernetes resources with pre resources") {
    val sparkConf = new SparkConf(false)
      .set(Config.CONTAINER_IMAGE, "spark-executor:latest")
      .set(Config.KUBERNETES_DRIVER_POD_FEATURE_STEPS.key,
        "org.apache.spark.deploy.k8s.TestStepTwo," +
          "org.apache.spark.deploy.k8s.TestStep")
    val preResKconf: KubernetesDriverConf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      resourceNamePrefix = Some(KUBERNETES_RESOURCE_PREFIX)
    )

    when(driverBuilder.buildFromFeatures(preResKconf, kubernetesClient))
      .thenReturn(BUILT_KUBERNETES_SPEC_WITH_PRERES)
    val submissionClient = new Client(
      preResKconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues

    // 2 for pre-resource creation/update, 1 for resource creation, 1 for config map
    assert(otherCreatedResources.size === 4)
    val preRes = otherCreatedResources.toArray
      .filter(_.isInstanceOf[CustomResourceDefinition]).toSeq

    // Make sure pre-resource creation/owner reference as expected
    assert(preRes.size === 2)
    assert(preRes.last === PRE_ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES.head)

    // Make sure original resource and config map process are not affected
    val secrets = otherCreatedResources.toArray.filter(_.isInstanceOf[Secret]).toSeq
    assert(secrets === ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES)
    val configMaps = otherCreatedResources.toArray
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap])
    assert(secrets.nonEmpty)
    assert(configMaps.nonEmpty)
    val configMap = configMaps.head
    assert(configMap.getMetadata.getName ===
      KubernetesClientUtils.configMapNameDriver)
    assert(configMap.getImmutable())
    assert(configMap.getData.containsKey(SPARK_CONF_FILE_NAME))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf1key=conf1value"))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf2key=conf2value"))
  }

  test("All files from SPARK_CONF_DIR, " +
    "except templates, spark config, binary files and are within size limit, " +
    "should be populated to pod's configMap.") {
    def testSetup: (SparkConf, Seq[String]) = {
      val tempDir = Utils.createTempDir()
      val sparkConf = new SparkConf(loadDefaults = false)
        .setSparkHome(tempDir.getAbsolutePath)

      val tempConfDir = new File(s"${tempDir.getAbsolutePath}/conf")
      tempConfDir.mkdir()
      // File names - which should not get mounted on the resultant config map.
      val filteredConfFileNames =
        Set("spark-env.sh.template", "spark.properties", "spark-defaults.conf",
          "test.gz", "test2.jar", "non_utf8.txt")
      val confFileNames = (for (i <- 1 to 5) yield s"testConf.$i") ++
        List("spark-env.sh") ++ filteredConfFileNames

      val testConfFiles = (for (i <- confFileNames) yield {
        val file = new File(s"${tempConfDir.getAbsolutePath}/$i")
        if (i.startsWith("non_utf8")) { // filling some non-utf-8 binary
          Files.write(file.toPath, Array[Byte](0x00.toByte, 0xA1.toByte))
        } else {
          Files.write(file.toPath, "conf1key=conf1value".getBytes(StandardCharsets.UTF_8))
        }
        file.getName
      })
      assert(tempConfDir.listFiles().length == confFileNames.length)
      val expectedConfFiles: Seq[String] = testConfFiles.filterNot(filteredConfFileNames.contains)
      (sparkConf, expectedConfFiles)
    }

    val (sparkConf: SparkConf, expectedConfFiles: Seq[String]) = testSetup

    val expectedKeyToPaths = (expectedConfFiles.map(x => new KeyToPath(x, 420, x)).toList ++
      List(KEY_TO_PATH)).sortBy(x => x.getKey)

    when(podOperations.create(fullExpectedPod(expectedKeyToPaths)))
      .thenReturn(podWithOwnerReference(expectedKeyToPaths))

    kconf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf,
      resourceNamePrefix = Some(KUBERNETES_RESOURCE_PREFIX))

    assert(kconf.sparkConf.getOption("spark.home").isDefined)
    when(driverBuilder.buildFromFeatures(kconf, kubernetesClient)).thenReturn(BUILT_KUBERNETES_SPEC)

    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues

    val configMaps = otherCreatedResources.toArray
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap])
    assert(configMaps.nonEmpty)
    val configMapName = KubernetesClientUtils.configMapNameDriver
    val configMap: ConfigMap = configMaps.head
    assert(configMap.getMetadata.getName == configMapName)
    val configMapLoadedFiles = configMap.getData.keySet().asScala.toSet -
        Config.KUBERNETES_NAMESPACE.key
    assert(configMapLoadedFiles === expectedConfFiles.toSet ++ Set(SPARK_CONF_FILE_NAME))
    for (f <- configMapLoadedFiles) {
      assert(configMap.getData.get(f).contains("conf1key=conf1value"))
    }
  }

  test("Waiting for app completion should stall on the watcher") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    verify(loggingPodStatusWatcher).watchOrStop(kconf.namespace + ":driver")
  }
}
