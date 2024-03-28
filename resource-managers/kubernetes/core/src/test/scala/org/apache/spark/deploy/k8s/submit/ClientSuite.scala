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
import java.net.HttpURLConnection
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apiextensions.v1.{CustomResourceDefinition, CustomResourceDefinitionBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.server.mock.KubernetesServer
import org.mockito.{ArgumentCaptor, Mock, MockitoAnnotations}
import org.mockito.Mockito.{verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.deploy.k8s.{Config, _}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_DRIVER_POD_NAME
import org.apache.spark.deploy.k8s.Config.WAIT_FOR_APP_COMPLETION
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.deploy.k8s.features.DriverServiceFeatureStep
import org.apache.spark.deploy.k8s.submit.Client.submissionId
import org.apache.spark.internal.config.UI.UI_ENABLED
import org.apache.spark.launcher.{InProcessLauncher, SparkAppHandle, SparkLauncher}
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
      .withNewStatus()
        .withPhase("Pending")
        .endStatus()
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
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _

  @Mock
  private var namedPods: PodResource = _

  @Mock
  private var loggingPodStatusWatcher: LoggingPodStatusWatcher = _

  @Mock
  private var driverBuilder: KubernetesDriverBuilder = _

  @Mock
  private var resourceList: RESOURCE_LIST = _

  private val server = new KubernetesServer(false, false)

  private var kconf: KubernetesDriverConf = _
  private var createdPodArgumentCaptor: ArgumentCaptor[Pod] = _
  private var createdResourcesArgumentCaptor: ArgumentCaptor[Array[HasMetadata]] = _

  before {
    beforeAll()
    MockitoAnnotations.openMocks(this).close()
    kconf = KubernetesTestConf.createDriverConf(
      resourceNamePrefix = Some(KUBERNETES_RESOURCE_PREFIX))
    when(driverBuilder.buildFromFeatures(kconf, kubernetesClient)).thenReturn(BUILT_KUBERNETES_SPEC)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace(kconf.namespace)).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withName(POD_NAME)).thenReturn(namedPods)

    createdPodArgumentCaptor = ArgumentCaptor.forClass(classOf[Pod])
    createdResourcesArgumentCaptor = ArgumentCaptor.forClass(classOf[Array[HasMetadata]])
    when(podsWithNamespace.resource(fullExpectedPod())).thenReturn(namedPods)
    when(resourceList.forceConflicts()).thenReturn(resourceList)
    when(namedPods.serverSideApply()).thenReturn(podWithOwnerReference())
    when(namedPods.create()).thenReturn(podWithOwnerReference())
    when(namedPods.watch(loggingPodStatusWatcher)).thenReturn(mock[Watch])
    val sId = submissionId(kconf.namespace, POD_NAME)
    when(loggingPodStatusWatcher.watchOrStop(sId)).thenReturn(true)
    doReturn(resourceList)
      .when(kubernetesClient)
      .resourceList(createdResourcesArgumentCaptor.capture(): _*)
    server.before()
    server.expect().post()
      .withPath("/api/v1/namespaces/default/pods")
      .andReturn(HttpURLConnection.HTTP_OK, BUILT_DRIVER_POD)
      .always()
    server.expect().patch()
      .withPath(
        "/api/v1/namespaces/default/services/resource-example-driver-svc?" +
        "fieldManager=fabric8&force=true"
      )
      .andReturn(HttpURLConnection.HTTP_OK,
        new DriverServiceFeatureStep(kconf).getAdditionalKubernetesResources().head)
      .always()
    server.expect().patch()
      .withPath(
       s"/api/v1/namespaces/default/configmaps/${KubernetesClientUtils.configMapNameDriver}" +
        "?fieldManager=fabric8&force=true"
      )
      .andReturn(HttpURLConnection.HTTP_OK,
        new DriverServiceFeatureStep(kconf).getAdditionalKubernetesResources().head)
      .always()
    server.expect().get()
      .withPath(
       s"/api/v1/namespaces/default/pods/${POD_NAME}"
      )
      .andReturn(HttpURLConnection.HTTP_OK, BUILT_DRIVER_POD)
      .once()
    server.expect().withPath(
        // Hint: different version of kubernetes client may cause the
        //  url params in different order, which may cause the mock failed
        s"/api/v1/namespaces/default/pods?allowWatchBookmarks=true&" +
        s"fieldSelector=metadata.name%3D${POD_NAME}&watch=true"
      )
      .andUpgradeToWebSocket()
      .open(new WatchEvent(BUILT_DRIVER_POD, "ADDED"))
      .waitFor(1000L)
      .andEmit(new WatchEvent(
        new PodBuilder()
          .withNewMetadata()
            .withName(POD_NAME)
            .endMetadata()
          .withNewSpec()
            .withHostname("localhost")
            .endSpec()
          .withNewStatus()
            .withPhase("Running")
            .endStatus()
          .build()
        , "MODIFIED"))
      .waitFor(3000L)
      .andEmit(new WatchEvent(
        new PodBuilder()
          .withNewMetadata()
            .withName(POD_NAME)
            .endMetadata()
          .withNewSpec()
            .withHostname("localhost")
            .endSpec()
          .withNewStatus()
            .withPhase("Succeeded")
            .endStatus()
          .build()
        , "MODIFIED"))
      .done().always();
  }

  after {
    server.after()
    afterAll()
  }

  test("The client should configure the pod using the builder.") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    verify(podsWithNamespace).resource(fullExpectedPod())
    verify(namedPods).create()
  }

  test("The client should create Kubernetes resources") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues.asScala.flatten
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
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues.asScala.flatten

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

    when(podsWithNamespace.resource(fullExpectedPod(expectedKeyToPaths)))
      .thenReturn(namedPods)
    when(namedPods.forceConflicts()).thenReturn(namedPods)
    when(namedPods.serverSideApply()).thenReturn(podWithOwnerReference(expectedKeyToPaths))

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
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues.asScala.flatten

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
    verify(loggingPodStatusWatcher).watchOrStop(submissionId(kconf.namespace, POD_NAME))
  }

  test("SPARK-42813: Print application info when waitAppCompletion is false") {
    val appName = "SPARK-42813"
    val logAppender = new LogAppender
    withLogAppender(logAppender) {
      val sparkConf = new SparkConf(loadDefaults = false)
        .set("spark.app.name", appName)
        .set(WAIT_FOR_APP_COMPLETION, false)
      kconf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf,
        resourceNamePrefix = Some(KUBERNETES_RESOURCE_PREFIX))
      when(driverBuilder.buildFromFeatures(kconf, kubernetesClient))
        .thenReturn(BUILT_KUBERNETES_SPEC)
      val submissionClient = new Client(
        kconf,
        driverBuilder,
        kubernetesClient,
        loggingPodStatusWatcher)
      submissionClient.run()
    }
    val appId = KubernetesTestConf.APP_ID
    val sId = submissionId(kconf.namespace, POD_NAME)
    assert(logAppender.loggingEvents.map(_.getMessage.getFormattedMessage).contains(
      s"Deployed Spark application $appName with application ID $appId " +
      s"and submission ID $sId into Kubernetes"))
  }

  test("SPARK-36832: manage application using spark launcher handle") {
    val kubeServer = server.getKubernetesMockServer
    val handle = new InProcessLauncher()
      .setConf(UI_ENABLED.key, "false")
      .setConf(KUBERNETES_DRIVER_POD_NAME.key, POD_NAME)
      .setConf(Config.CONTAINER_IMAGE.key, "spark-executor:latest")
      .setConf(Config.KUBERNETES_DRIVER_POD_NAME_PREFIX.key, KUBERNETES_RESOURCE_PREFIX)
      .setMaster(s"k8s://http://${kubeServer.getHostName}:${kubeServer.getPort}")
      .setDeployMode("cluster")
      .setAppResource(SparkLauncher.NO_RESOURCE)
      .setMainClass(KubernetesLauncherTestApp.getClass.getName.stripSuffix("$"))
      .startApplication()

    try {
      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        assert(handle.getState == SparkAppHandle.State.SUBMITTED)
      }

      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        assert(handle.getState == SparkAppHandle.State.RUNNING)
      }

      assert(handle.getAppId != null)
      handle.stop()

      eventually(timeout(5.seconds), interval(100.milliseconds)) {
        assert(handle.getState == SparkAppHandle.State.KILLED)
      }
    } finally {
      handle.kill()
    }

    assert(server.getClient.pods().withName(POD_NAME).get() == null)
  }
}

private object KubernetesLauncherTestApp {

  def main(args: Array[String]): Unit = {
    // Do not stop the application; the test will stop it using the launcher lib. Just run a task
    // that will prevent the process from exiting.
    val sc = new SparkContext(new SparkConf())
    sc.parallelize(Seq(1)).foreach { i =>
      this.synchronized {
        wait()
      }
    }
  }

}
