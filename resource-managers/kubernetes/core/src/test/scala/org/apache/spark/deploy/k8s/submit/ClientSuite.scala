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
import io.fabric8.kubernetes.client.dsl.{NamespaceableResource, PodResource}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mock, MockitoAnnotations}
import org.mockito.Mockito.{doThrow, inOrder, never, verify, when}
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

  private def keyToPath(key: String): KeyToPath =
    new KeyToPathBuilder().withKey(key).withMode(420).withPath(key).build()

  private val KEY_TO_PATH = keyToPath(SPARK_CONF_FILE_NAME)

  // SPARK-38079: configMapNameDriver is a `def`, not a `val` (see KubernetesClientUtils --
  // this is called exactly once per real submission, into a local val run() reuses, so unlike
  // configMapNameExecutor it has no cross-call consistency requirement to preserve). So this
  // cannot call it again here to predict the name run() actually used -- each call returns a
  // different random one. Instead, this takes the config map's name as a parameter, letting
  // every caller build its "expected pod" around whatever name was actually captured from the
  // real call, rather than around a prediction that only `val`'s caching ever made valid.
  private def fullExpectedPod(
      configMapName: String,
      keyToPaths: List[KeyToPath] = List(KEY_TO_PATH)) =
    new PodBuilder(BUILT_DRIVER_POD)
      .editSpec()
        .addToContainers(FULL_EXPECTED_CONTAINER)
        .addNewVolume()
          .withName(SPARK_CONF_VOLUME_DRIVER)
          .withNewConfigMap()
            .withItems(keyToPaths.asJava)
            .withName(configMapName)
            .endConfigMap()
          .endVolume()
        .addNewSchedulingGate(PRE_RESOURCES_SCHEDULING_GATE)
        .endSpec()
      .build()

  private def podWithOwnerReference(
      configMapName: String,
      keyToPaths: List[KeyToPath] = List(KEY_TO_PATH)) =
    new PodBuilder(fullExpectedPod(configMapName, keyToPaths))
      .editMetadata()
        .withUid(DRIVER_POD_UID)
        .endMetadata()
      .withApiVersion(DRIVER_POD_API_VERSION)
      .withKind(DRIVER_POD_KIND)
      .build()

  // SPARK-38079: what the created driver pod (i.e. podWithOwnerReference()) looks like once
  // its pre-resources scheduling gate has been removed (see run()'s "Remove the pre-resources
  // scheduling gate" step) -- this is the pod run()'s edit() call is applied against, not the
  // pre-creation fullExpectedPod(), so this must be based on the former to actually match what
  // a correct edit() call produces.
  private def fullExpectedPodGateRemoved(
      configMapName: String,
      keyToPaths: List[KeyToPath] = List(KEY_TO_PATH)) =
    new PodBuilder(podWithOwnerReference(configMapName, keyToPaths))
      .editSpec()
        .removeMatchingFromSchedulingGates(_.getName == PRE_RESOURCES_SCHEDULING_GATE)
        .endSpec()
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

  @Mock
  private var namedResource: NamespaceableResource[HasMetadata] = _

  private var kconf: KubernetesDriverConf = _
  private var createdPodArgumentCaptor: ArgumentCaptor[Pod] = _
  private var createdResourcesArgumentCaptor: ArgumentCaptor[Array[HasMetadata]] = _
  // SPARK-38079: configMapNameDriver is a `def`, not a `val` (see KubernetesClientUtils), so
  // the gated pod run() actually builds for a given test carries a config map name chosen at
  // call time -- not one this suite could predict in advance by calling that `def` again
  // itself. Each test instead reads the actual gated pod run() built, captured here off the
  // podsWithNamespace.resource(...) call in the `before` block below, and builds its
  // "expected"/"created" pod variants (podWithOwnerReference(), fullExpectedPodGateRemoved(),
  // etc.) from that captured pod's own config map name instead.
  private var gatedPodArgumentCaptor: ArgumentCaptor[Pod] = _
  private def gatedPod: Pod = gatedPodArgumentCaptor.getValue
  private def gatedConfigMapName: String = gatedPod.getSpec.getVolumes.asScala
    .find(_.getName == SPARK_CONF_VOLUME_DRIVER).get.getConfigMap.getName

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
    gatedPodArgumentCaptor = ArgumentCaptor.forClass(classOf[Pod])
    when(podsWithNamespace.resource(gatedPodArgumentCaptor.capture())).thenReturn(namedPods)
    // SPARK-38079: the failure-cleanup catch blocks in run() delete the *created* driver pod
    // via kubernetesClient.pods().resource(createdDriverPod) -- note: not .inNamespace(...)
    // first, unlike the pre-creation .resource(...) call above -- so mock that chain too,
    // resolving to the same namedPods mock instead of null, for any created (owner-reference-
    // carrying) pod it might be called with.
    when(podOperations.resource(ArgumentMatchers.any[Pod]())).thenReturn(namedPods)
    when(resourceList.forceConflicts()).thenReturn(resourceList)
    // SPARK-38079: the pod is created still scheduling-gated (see run()); .create() returns
    // that gated pod, with its UID already assigned by the (simulated) API server. Mockito
    // resolves the gatedPodArgumentCaptor.capture() matcher above against whatever pod this
    // call is actually made with, so this answer sees the real captured pod too.
    when(namedPods.create()).thenAnswer(_ => podWithOwnerReference(gatedConfigMapName))
    // SPARK-38079: run() removes the gate via a single .edit(UnaryOperator[Pod]) call once
    // its pre-resources exist. Mockito can't match a lambda by its behavior, so this answers
    // by actually invoking whatever function run() passes in -- exactly like the real
    // fabric8 implementation does -- against the created (owner-reference-carrying) pod, so
    // the returned pod reflects whatever edit run() actually asked for.
    when(namedPods.edit(ArgumentMatchers.any[java.util.function.UnaryOperator[Pod]]()))
      .thenAnswer(invocation => {
        val editFn = invocation.getArgument[java.util.function.UnaryOperator[Pod]](0)
        editFn.apply(podWithOwnerReference(gatedConfigMapName))
      })
    when(namedPods.watch(loggingPodStatusWatcher)).thenReturn(mock[Watch])
    val sId = submissionId(kconf.namespace, POD_NAME)
    when(loggingPodStatusWatcher.watchOrStop(sId)).thenReturn(true)
    doReturn(resourceList)
      .when(kubernetesClient)
      .resourceList(createdResourcesArgumentCaptor.capture(): _*)
    // SPARK-38079: cleanup deletes each pre-resource individually (not as a single
    // resourceList(...).delete() call -- see deletePodAndPreResources, review comment
    // #4129910518), so any HasMetadata resolves to the same trackable mock here.
    when(kubernetesClient.resource(ArgumentMatchers.any[HasMetadata]())).thenReturn(namedResource)
  }

  test("The client should configure the pod using the builder.") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    // SPARK-38079: the pod is created still scheduling-gated (see run()), then that gate is
    // removed via a single edit() once its pre-resources exist. The config map name in
    // fullExpectedPod() is taken from the actual gated pod captured above (configMapNameDriver
    // is a `def`, so this suite cannot predict it by calling that `def` again itself).
    assert(gatedPod === fullExpectedPod(gatedConfigMapName))
    verify(namedPods).create()
    val editFnCaptor = ArgumentCaptor.forClass(classOf[java.util.function.UnaryOperator[Pod]])
    verify(namedPods).edit(editFnCaptor.capture())
    // Actually apply the captured function to a still-gated pod and check the result, rather
    // than only checking that some function was passed to edit() -- a no-op (or wrong-field)
    // edit function would satisfy the verify() above without ever removing the gate.
    assert(editFnCaptor.getValue.apply(podWithOwnerReference(gatedConfigMapName)) ===
      fullExpectedPodGateRemoved(gatedConfigMapName))
    // SPARK-38079 review (dongjoon-hyun, #4129910551): checking each call happened is not
    // enough to show the pod is created *before* its pre-resources, or that the gate is
    // removed only *after* that -- these are independent verify() calls, so e.g. swapping the
    // order of the create() and resourceList() calls inside run() would still satisfy them
    // all. Pin down the actual order with InOrder instead.
    val order = inOrder(namedPods, kubernetesClient)
    order.verify(namedPods).create()
    order.verify(kubernetesClient).resourceList(ArgumentMatchers.any[Array[HasMetadata]](): _*)
    order.verify(namedPods).edit(ArgumentMatchers.any[java.util.function.UnaryOperator[Pod]]())
  }

  test("The client should create Kubernetes resources") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues.asScala.flatten
    // SPARK-38079: the driver's own config map is a pre-resource, created in a single
    // resourceList() call (with its owner reference already set -- see run()) now that the
    // driver pod exists -- 1 for the config map, 1 for the (post-resource) secret.
    assert(otherCreatedResources.size === 2)
    val secrets = otherCreatedResources.toArray.filter(_.isInstanceOf[Secret]).toSeq
    assert(secrets === ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES)
    val configMaps = otherCreatedResources.toArray
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap])
    assert(secrets.nonEmpty)
    assert(configMaps.nonEmpty)
    val configMap = configMaps.head
    // SPARK-38079: configMapNameDriver is a `def` -- see its own comment -- so this cannot
    // compare against a freshly-generated name (that call would return a different one); it
    // can only check the name has the shape configMapNameDriver is documented to produce.
    assert(configMap.getMetadata.getName.matches("spark-drv-\\S+-conf-map"))
    assert(configMap.getImmutable())
    assert(configMap.getData.containsKey(SPARK_CONF_FILE_NAME))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf1key=conf1value"))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf2key=conf2value"))
  }

  test("SPARK-38079: driver pod is created (still scheduling-gated) before its own config " +
      "map, and the config map's single create call already carries the owner reference") {
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()
    // The pod itself must be created before any pre-resource resourceList() call, since a
    // pre-resource's owner reference (set below) needs the pod's UID.
    assert(gatedPod === fullExpectedPod(gatedConfigMapName))
    verify(namedPods).create()

    // The (single) resourceList(...) call creating the driver's own config map must already
    // carry the owner reference -- there is no separate, later "refresh" call, unlike before
    // this refactor. The pod stays scheduling-gated (so kubelet cannot attempt to mount it)
    // for as long as this has not yet happened, avoiding the "configmap ... not found" mount
    // race (SPARK-38079) without ever creating the config map ownerless.
    val resourceListCall = createdResourcesArgumentCaptor.getAllValues.get(0)
    val configMaps = resourceListCall
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap])
    assert(configMaps.nonEmpty,
      "the driver's own config map must be created as a pre-resource")
    val ownerReferences = configMaps.head.getMetadata.getOwnerReferences
    assert(ownerReferences.size() === 1)
    assert(ownerReferences.get(0).getName === POD_NAME)
    assert(ownerReferences.get(0).getUid === DRIVER_POD_UID)

    // The scheduling gate must be removed only after the config map has been created.
    verify(namedPods).edit(ArgumentMatchers.any[java.util.function.UnaryOperator[Pod]]())
  }

  // SPARK-38079: the three failure branches in run() between pod creation and gate removal --
  // covering (a) the pod creation call itself failing, (b) the pre-resource creation call
  // failing after the pod exists, and (c) the gate-removal call failing after pre-resources
  // exist -- each verified against real-cluster behavior (KubernetesClientException on
  // AlreadyExists/Invalid/NotFound) before being written as these mock-based tests.
  test("SPARK-38079: pod creation failure propagates without attempting any pre-resource " +
      "creation") {
    val podCreationFailure = new RuntimeException("simulated pod creation failure")
    doThrow(podCreationFailure).when(namedPods).create()
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)

    val thrown = intercept[RuntimeException] {
      submissionClient.run()
    }

    assert(thrown eq podCreationFailure)
    verify(kubernetesClient, never()).resourceList(ArgumentMatchers.any[Array[HasMetadata]](): _*)
  }

  test("SPARK-38079: pre-resource creation failure (after the pod exists) deletes both the " +
      "pod and the pre-resources, then propagates") {
    val preResourceFailure = new RuntimeException("simulated pre-resource creation failure")
    doThrow(preResourceFailure).when(resourceList).serverSideApply()
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)

    val thrown = intercept[RuntimeException] {
      submissionClient.run()
    }

    assert(thrown eq preResourceFailure)
    // Not just "delete() was called on some mock" -- pods().resource(...) must have been
    // called with the actual created (gated) pod, i.e. the same pod object this cleanup path
    // is documented to delete, and each pre-resource must be deleted individually (not as a
    // single resourceList(...).delete() call, which stops at the first failing item).
    verify(namedPods).create()
    verify(podOperations).resource(podWithOwnerReference(gatedConfigMapName))
    verify(namedPods).delete()
    verify(namedResource).delete()
    val preResourceCalls = createdResourcesArgumentCaptor.getAllValues.asScala
    assert(preResourceCalls.exists(_.exists(_.isInstanceOf[ConfigMap])),
      "the failed resourceList() call must have been for the pre-resources (the driver's " +
        "own config map among them), not some other resource set")
    // The gate is never removed on this failure path.
    verify(namedPods, never()).edit(ArgumentMatchers.any[java.util.function.UnaryOperator[Pod]]())
  }

  test("SPARK-38079: gate-removal failure (after pre-resources exist) deletes the pod and " +
      "the pre-resources, then propagates") {
    val gateRemovalFailure = new RuntimeException("simulated gate-removal failure")
    doThrow(gateRemovalFailure)
      .when(namedPods).edit(ArgumentMatchers.any[java.util.function.UnaryOperator[Pod]]())
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)

    val thrown = intercept[RuntimeException] {
      submissionClient.run()
    }

    assert(thrown eq gateRemovalFailure)
    verify(podOperations).resource(podWithOwnerReference(gatedConfigMapName))
    verify(namedPods).delete()
    verify(namedResource).delete()
    val preResourceCalls = createdResourcesArgumentCaptor.getAllValues.asScala
    assert(preResourceCalls.exists(_.exists(_.isInstanceOf[ConfigMap])),
      "the pre-resources deleted here must be the same ones created before the gate-removal " +
        "attempt, not some other resource set")
  }

  test("SPARK-38079: a failure deleting the pod during cleanup does not prevent the " +
      "pre-resources from also being deleted, and the original exception still propagates") {
    val gateRemovalFailure = new RuntimeException("simulated gate-removal failure")
    doThrow(gateRemovalFailure)
      .when(namedPods).edit(ArgumentMatchers.any[java.util.function.UnaryOperator[Pod]]())
    // The pod-delete step of cleanup itself fails (e.g. a permissions or network error hitting
    // the delete call) -- this must not mask gateRemovalFailure, nor skip deleting the
    // pre-resources afterwards.
    doThrow(new RuntimeException("simulated delete failure")).when(namedPods).delete()
    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)

    val thrown = intercept[RuntimeException] {
      submissionClient.run()
    }

    assert(thrown eq gateRemovalFailure,
      "the original failure that triggered cleanup must still be the one that propagates, " +
        "not a failure from within the best-effort cleanup itself")
    verify(namedResource).delete()
  }

  test("SPARK-38079: a pod template's spec.nodeName is translated to an equivalent " +
      "nodeSelector, since a pod cannot be created with both nodeName and (non-empty) " +
      "schedulingGates set") {
    val podWithPinnedNodeName = new PodBuilder(BUILT_DRIVER_POD)
      .editSpec()
        .withNodeName("some-specific-node")
        .endSpec()
      .build()
    val specWithPinnedNodeName = KubernetesDriverSpec(
      SparkPod(podWithPinnedNodeName, BUILT_DRIVER_CONTAINER),
      Nil,
      ADDITIONAL_RESOURCES,
      RESOLVED_JAVA_OPTIONS)
    when(driverBuilder.buildFromFeatures(kconf, kubernetesClient))
      .thenReturn(specWithPinnedNodeName)

    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()

    assert(gatedPod.getSpec.getNodeName === null,
      "a gated pod must never carry spec.nodeName -- the API server rejects creating one " +
        "with both nodeName and schedulingGates set")
    assert(gatedPod.getSpec.getNodeSelector.get("kubernetes.io/hostname") ===
      "some-specific-node",
      "the pinned node must still be honored, via an equivalent nodeSelector instead")
    assert(!gatedPod.getSpec.getSchedulingGates.isEmpty,
      "the gate itself must still be present -- this is not a general nodeName removal")
  }

  test("SPARK-38079: a pod template's spec.nodeName is discarded, not translated, if an " +
      "explicit spark.kubernetes.driver.node.selector.kubernetes.io/hostname is already set " +
      "-- that explicit configuration must win, not be silently overwritten") {
    val podWithPinnedNodeNameAndExplicitSelector = new PodBuilder(BUILT_DRIVER_POD)
      .editSpec()
        .withNodeName("template-pinned-node")
        .addToNodeSelector("kubernetes.io/hostname", "explicitly-configured-node")
        .endSpec()
      .build()
    val specWithBoth = KubernetesDriverSpec(
      SparkPod(podWithPinnedNodeNameAndExplicitSelector, BUILT_DRIVER_CONTAINER),
      Nil,
      ADDITIONAL_RESOURCES,
      RESOLVED_JAVA_OPTIONS)
    when(driverBuilder.buildFromFeatures(kconf, kubernetesClient)).thenReturn(specWithBoth)

    val submissionClient = new Client(
      kconf,
      driverBuilder,
      kubernetesClient,
      loggingPodStatusWatcher)
    submissionClient.run()

    assert(gatedPod.getSpec.getNodeName === null)
    assert(gatedPod.getSpec.getNodeSelector.get("kubernetes.io/hostname") ===
      "explicitly-configured-node",
      "the explicit spark.kubernetes.driver.node.selector.* value (applied earlier, by " +
        "BasicDriverFeatureStep, in builder.buildFromFeatures()) must not be overwritten by " +
        "the nodeName-derived fallback")
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

    // SPARK-38079: the pre-resource CRD and the driver's own config map are both created via
    // the single (owner-reference-carrying) pre-resource resourceList() call -- 1 for the
    // CRD, 1 for the config map -- plus 1 for the (post-resource) secret.
    assert(otherCreatedResources.size === 3)
    val preRes = otherCreatedResources.toArray
      .filter(_.isInstanceOf[CustomResourceDefinition]).toSeq

    // Make sure pre-resource creation/owner reference as expected
    assert(preRes.size === 1)
    assert(preRes.head === PRE_ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES.head)

    // Make sure original resource and config map process are not affected
    val secrets = otherCreatedResources.toArray.filter(_.isInstanceOf[Secret]).toSeq
    assert(secrets === ADDITIONAL_RESOURCES_WITH_OWNER_REFERENCES)
    val configMaps = otherCreatedResources.toArray
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap])
    assert(secrets.nonEmpty)
    assert(configMaps.nonEmpty)
    val configMap = configMaps.head
    // SPARK-38079: see the comment on the earlier identical assertion -- configMapNameDriver
    // is a `def`, so only the name's shape can be checked here, not an exact match.
    assert(configMap.getMetadata.getName.matches("spark-drv-\\S+-conf-map"))
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
    val configMap: ConfigMap = configMaps.head
    // SPARK-38079: see the comment on the earlier identical assertion -- configMapNameDriver
    // is a `def`, so only the name's shape can be checked here, not an exact match.
    assert(configMap.getMetadata.getName.matches("spark-drv-\\S+-conf-map"))
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
