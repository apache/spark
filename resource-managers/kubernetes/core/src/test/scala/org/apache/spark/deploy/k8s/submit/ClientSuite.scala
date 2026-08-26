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

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apiextensions.v1.{CustomResourceDefinition, CustomResourceDefinitionBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, Watch}
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mock, MockitoAnnotations}
import org.mockito.Mockito.{doThrow, never, verify, when}
import org.scalatest.BeforeAndAfter
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{Config, _}
import org.apache.spark.deploy.k8s.Config.WAIT_FOR_APP_COMPLETION
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.deploy.k8s.submit.Client.submissionId
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
  private var podsWithNamespace: PODS_WITH_NAMESPACE = _

  @Mock
  private var namedPods: PodResource = _

  @Mock
  private var loggingPodStatusWatcher: LoggingPodStatusWatcher = _

  @Mock
  private var driverBuilder: KubernetesDriverBuilder = _

  @Mock
  private var resourceList: RESOURCE_LIST = _

  private var kconf: KubernetesDriverConf = _
  private var createdPodArgumentCaptor: ArgumentCaptor[Pod] = _
  private var createdResourcesArgumentCaptor: ArgumentCaptor[Array[HasMetadata]] = _

  before {
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
    // SPARK-38079: the driver's own config map is now a pre-resource, so it is sent via
    // resourceList() twice (once before pod creation, once for the owner-reference
    // refresh) -- 2 for the config map, 1 for the (post-resource) secret.
    assert(otherCreatedResources.size === 3)
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

  test("SPARK-38079: driver's own config map is created before the driver pod, " +
      "to avoid a mount race") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    // The first resourceList(...) call is always the pre-resources application, which
    // happens before the driver pod is created. The driver's own config map must be
    // included there so that it exists in Kubernetes before the pod that mounts it.
    val firstResourceListCall = createdResourcesArgumentCaptor.getAllValues.get(0)
    val configMaps = firstResourceListCall.filter(_.isInstanceOf[ConfigMap])
    assert(configMaps.nonEmpty,
      "the driver's own config map must be sent as a pre-resource, before the driver pod " +
        "is created, to avoid a \"configmap ... not found\" mount race (SPARK-38079)")

    // Safety check: the pre-resource owner-reference refresh (the second resourceList()
    // call) must still set an owner reference on the config map, same as before this
    // change, so that it is still garbage-collected along with the driver pod.
    val secondResourceListCall = createdResourcesArgumentCaptor.getAllValues.get(1)
    val refreshedConfigMap = secondResourceListCall
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap]).head
    val ownerReferences = refreshedConfigMap.getMetadata.getOwnerReferences
    assert(ownerReferences.size() === 1)
    assert(ownerReferences.get(0).getName === POD_NAME)
    assert(ownerReferences.get(0).getUid === DRIVER_POD_UID)
  }

  // SPARK-38079: making the driver's own config map (and other credential-bearing resources,
  // e.g. Kerberos keytab/delegation-token secrets) a pre-resource above means they briefly
  // exist without an owner reference (see the comment on cleanupOrphanedPreResources in
  // KubernetesClientApplication.scala). run() registers a shutdown hook to best-effort clean
  // those up if this process is terminated abruptly in that window. Actually triggering a JVM
  // shutdown hook from a test is impractical, so these tests instead call the (package-private)
  // cleanup method directly, the same way the hook itself would.
  private def newRecoveryClientMocks(): (KubernetesClient, RESOURCE_LIST, PodResource) = {
    val recoveryClient = mock[KubernetesClient]
    val recoveryResourceList = mock[RESOURCE_LIST]
    val recoveryPodOperations = mock[PODS]
    val recoveryPodsWithNamespace = mock[PODS_WITH_NAMESPACE]
    val recoveryNamedPod = mock[PodResource]
    doReturn(recoveryResourceList).when(recoveryClient).resourceList(ArgumentMatchers.any[Array[HasMetadata]](): _*)
    when(recoveryClient.pods()).thenReturn(recoveryPodOperations)
    when(recoveryPodOperations.inNamespace(kconf.namespace)).thenReturn(recoveryPodsWithNamespace)
    when(recoveryPodsWithNamespace.withName(POD_NAME)).thenReturn(recoveryNamedPod)
    (recoveryClient, recoveryResourceList, recoveryNamedPod)
  }

  test("SPARK-38079: shutdown-hook cleanup deletes orphaned pre-resources and the driver pod " +
      "if this submission created it") {
    val (recoveryClient, recoveryResourceList, recoveryNamedPod) = newRecoveryClientMocks()
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher,
      recoveryClientFactoryOverride = Some(() => recoveryClient))

    submissionClient.cleanupOrphanedPreResources(
      PRE_RESOURCES, POD_NAME, preResourcesApplied = true, podCreatedByUs = true)

    verify(recoveryResourceList).delete()
    verify(recoveryNamedPod).delete()
    // The recovery client is built fresh for this cleanup and must not leak.
    verify(recoveryClient).close()
  }

  test("SPARK-38079: shutdown-hook cleanup does not delete the driver pod if this submission " +
      "never created it") {
    val (recoveryClient, recoveryResourceList, recoveryNamedPod) = newRecoveryClientMocks()
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher,
      recoveryClientFactoryOverride = Some(() => recoveryClient))

    // podCreatedByUs = false: e.g. the pod creation API call itself never succeeded (or a
    // differently-submitted application happens to be using the same pod name), so this
    // cleanup must not delete a pod it did not create.
    submissionClient.cleanupOrphanedPreResources(
      PRE_RESOURCES, POD_NAME, preResourcesApplied = true, podCreatedByUs = false)

    verify(recoveryResourceList).delete()
    verify(recoveryNamedPod, never()).delete()
  }

  test("SPARK-38079: shutdown-hook cleanup is a no-op if pre-resources were never applied") {
    var recoveryClientFactoryInvoked = false
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher,
      recoveryClientFactoryOverride = Some(() => {
        recoveryClientFactoryInvoked = true
        mock[KubernetesClient]
      }))

    // preResourcesApplied = false: the pre-resource serverSideApply() call itself never
    // succeeded, so there is nothing to have been left orphaned -- and in particular, the
    // existing catch block for that call (in run()) has already handled cleanup of whatever
    // partial state that failed call may have left behind.
    submissionClient.cleanupOrphanedPreResources(
      PRE_RESOURCES, POD_NAME, preResourcesApplied = false, podCreatedByUs = false)

    assert(!recoveryClientFactoryInvoked,
      "the recovery client must not be built at all when there is nothing to clean up")
  }

  test("SPARK-38079: shutdown-hook cleanup swallows exceptions from the recovery client " +
      "(best-effort only)") {
    val (recoveryClient, recoveryResourceList, recoveryNamedPod) = newRecoveryClientMocks()
    doThrow(new RuntimeException("simulated API server failure"))
      .when(recoveryResourceList).delete()
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher,
      recoveryClientFactoryOverride = Some(() => recoveryClient))

    // Must not throw: this runs on the JVM shutdown-hook thread, where an uncaught exception
    // would only be printed to stderr and otherwise has no one left to meaningfully handle it.
    submissionClient.cleanupOrphanedPreResources(
      PRE_RESOURCES, POD_NAME, preResourcesApplied = true, podCreatedByUs = true)

    // The pod delete is independent of the (failed) pre-resource delete above, and must still
    // be attempted.
    verify(recoveryNamedPod).delete()
  }

  // SPARK-38079: the tests above all call cleanupOrphanedPreResources directly, since actually
  // triggering a JVM shutdown hook from a test is impractical. That leaves the wiring in run()
  // itself -- does it register a hook before applying pre-resources, and remove that exact hook
  // once done with them -- uncovered by those tests alone. These two tests close that gap by
  // injecting a fake ShutdownHookOps instead.
  test("SPARK-38079: cleanup hook is registered before pre-resources are applied and " +
      "removed once run() completes successfully") {
    var registeredHook: Option[() => Unit] = None
    var removedRef: Option[Any] = None
    val fakeOps = ShutdownHookOps(
      addHook = { hook =>
        registeredHook = Some(hook)
        "fake-hook-ref"
      },
      removeHook = { ref =>
        removedRef = Some(ref)
        true
      })
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher,
      shutdownHookOpsOverride = Some(fakeOps))

    submissionClient.run()

    assert(registeredHook.isDefined,
      "a cleanup hook must be registered before pre-resources are applied")
    assert(removedRef.contains("fake-hook-ref"),
      "the exact same hook reference returned by addHook must be passed to removeHook")
  }

  test("SPARK-38079: cleanup hook is still removed if run() fails before completing") {
    var removedRef: Option[Any] = None
    val fakeOps = ShutdownHookOps(
      addHook = { _ => "fake-hook-ref" },
      removeHook = { ref =>
        removedRef = Some(ref)
        true
      })
    val podCreationFailure = new RuntimeException("simulated pod creation failure")
    doThrow(podCreationFailure).when(namedPods).create()
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher,
      shutdownHookOpsOverride = Some(fakeOps))

    val thrown = intercept[RuntimeException] {
      submissionClient.run()
    }

    assert(thrown eq podCreationFailure)
    // The cleanup hook must be removed via the `finally` in run() even on this failure path,
    // since the existing catch block (unchanged by SPARK-38079) already deletes the
    // pre-resources itself -- the hook has nothing left to do from this point on, exactly as
    // on the success path.
    assert(removedRef.contains("fake-hook-ref"),
      "the cleanup hook must be removed even when run() fails partway through")
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

    // 2 for pre-resource creation/update, 1 for (post-resource) secret creation, and
    // 2 for the driver's own config map (SPARK-38079: now also a pre-resource, so it is
    // sent twice -- once before pod creation, once for the owner-reference refresh)
    assert(otherCreatedResources.size === 5)
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
}
