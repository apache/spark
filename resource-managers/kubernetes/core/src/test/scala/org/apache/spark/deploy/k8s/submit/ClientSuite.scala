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

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model._
import io.fabric8.kubernetes.api.model.apiextensions.v1.{CustomResourceDefinition, CustomResourceDefinitionBuilder}
import io.fabric8.kubernetes.client.{KubernetesClient, KubernetesClientException, Watch}
import io.fabric8.kubernetes.client.dsl.PodResource
import io.fabric8.kubernetes.client.dsl.base.{PatchContext, PatchType}
import org.mockito.{ArgumentCaptor, ArgumentMatchers, Mock, MockitoAnnotations}
import org.mockito.Mockito.{doAnswer, never, verify, when}
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.scalatest.BeforeAndAfter
import org.scalatestplus.mockito.MockitoSugar._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{Config, _}
import org.apache.spark.deploy.k8s.Config.{USE_DRIVER_SCHEDULING_GATE, WAIT_FOR_APP_COMPLETION}
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.deploy.k8s.submit.Client.submissionId
import org.apache.spark.util.Utils

class ClientSuite extends SparkFunSuite with BeforeAndAfter {

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
  // run() adds owner references to these objects, so every use gets fresh ones.
  private def ADDITIONAL_RESOURCES = Seq(
    new SecretBuilder().withNewMetadata().withName("secret").endMetadata().build())

  private def PRE_RESOURCES = Seq(
    new CustomResourceDefinitionBuilder().withNewMetadata().withName("preCRD").endMetadata().build()
  )
  private def BUILT_KUBERNETES_SPEC = KubernetesDriverSpec(
    SparkPod(BUILT_DRIVER_POD, BUILT_DRIVER_CONTAINER),
    Nil,
    ADDITIONAL_RESOURCES,
    RESOLVED_JAVA_OPTIONS)
  private def BUILT_KUBERNETES_SPEC_WITH_PRERES = KubernetesDriverSpec(
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

  private val TEMPLATE_GATE = "example.com/keep-me"

  private def fullExpectedPod(
      keyToPaths: List[KeyToPath] = List(KEY_TO_PATH),
      gated: Boolean = true) = {
    val pod = new PodBuilder(BUILT_DRIVER_POD)
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
    if (gated) {
      new PodBuilder(pod).editSpec().addNewSchedulingGate(DRIVER_RESOURCES_SCHEDULING_GATE)
        .endSpec().build()
    } else {
      pod
    }
  }

  private def podWithOwnerReference() =
    new PodBuilder(fullExpectedPod())
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
  private var patchContextCaptor: ArgumentCaptor[PatchContext] = _
  private var patchBodyCaptor: ArgumentCaptor[String] = _

  // Every call run() makes against the mocks, recorded as it happens. A resource's owner
  // reference is read when the call is made, since run() keeps mutating the same objects.
  private val events = mutable.ArrayBuffer.empty[String]
  // The pod each "remove gate" and "delete pod" call went through.
  private val podTargets = mutable.ArrayBuffer.empty[(String, Pod)]
  private var lastPodResource: Pod = _
  private var resourceListFor: Seq[HasMetadata] => RESOURCE_LIST = _

  private def describe(resource: HasMetadata): String = {
    val owned = Option(resource.getMetadata.getOwnerReferences).exists(!_.isEmpty)
    s"${resource.getKind}/${resource.getMetadata.getName}${if (owned) "+owner" else ""}"
  }

  private def answer[T](body: => T): Answer[T] = new Answer[T] {
    override def answer(invocation: InvocationOnMock): T = body
  }

  private def driverConf(
      gate: Boolean = true,
      sparkConf: SparkConf = new SparkConf(false)): KubernetesDriverConf = {
    KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf.set(USE_DRIVER_SCHEDULING_GATE, gate),
      resourceNamePrefix = Some(KUBERNETES_RESOURCE_PREFIX))
  }

  private def useSpec(spec: KubernetesDriverSpec): Unit = {
    when(driverBuilder.buildFromFeatures(kconf, kubernetesClient)).thenReturn(spec)
  }

  private def specWithPod(pod: Pod): KubernetesDriverSpec =
    BUILT_KUBERNETES_SPEC.copy(pod = SparkPod(pod, BUILT_DRIVER_CONTAINER))

  private def withGates(pod: Pod, gates: String*): Pod = {
    val builder = new PodBuilder(pod).editOrNewSpec()
    gates.foreach(builder.addNewSchedulingGate(_))
    builder.endSpec().build()
  }

  private def gateNames(pod: Pod): Seq[String] =
    Option(pod.getSpec.getSchedulingGates).map(_.asScala.map(_.getName).toSeq).getOrElse(Nil)

  // A resource list whose apply fails for the resources that match.
  private def failingWhen(failure: Throwable)(matches: Seq[HasMetadata] => Boolean): Unit = {
    val failing = mock[RESOURCE_LIST]
    when(failing.forceConflicts()).thenReturn(failing)
    when(failing.serverSideApply()).thenThrow(failure)
    when(failing.delete()).thenAnswer(answer {
      events += "delete resources"
      java.util.Collections.emptyList[StatusDetails]()
    })
    resourceListFor = resources => if (matches(resources)) failing else resourceList
  }

  private def failPodCall(failure: Throwable): Unit = {
    doAnswer(answer {
      events += "create pod"
      throw failure
    }).when(namedPods).create()
  }

  private def failGatePatch(failure: Throwable): Unit = {
    doAnswer(answer {
      events += "remove gate"
      podTargets += "remove gate" -> lastPodResource
      throw failure
    }).when(namedPods).patch(ArgumentMatchers.any[PatchContext](), ArgumentMatchers.any[String]())
  }

  private def failPodDelete(failure: Throwable): Unit = {
    doAnswer(answer {
      events += "delete pod"
      podTargets += "delete pod" -> lastPodResource
      throw failure
    }).when(namedPods).delete()
  }

  private def mountedConfigMapName: String = createdPodArgumentCaptor.getValue.getSpec.getVolumes
    .asScala.find(_.getName == SPARK_CONF_VOLUME_DRIVER).get.getConfigMap.getName

  private def newClient(): Client = new Client(
    kconf,
    driverBuilder,
    kubernetesClient,
    loggingPodStatusWatcher)

  // The calls of a submission that is not gated, or whose gate is removed last.
  private def submissionEvents(
      preResource: Option[String] = None,
      gateRemoved: Boolean = true): Seq[String] = {
    val configMapName = KubernetesClientUtils.configMapNameDriver
    val applied = s"list(${preResource.getOrElse("")})"
    val owned = s"list(${preResource.map(_ + "+owner").getOrElse("")})"
    Seq(applied, "apply", "create pod", owned, "apply",
      s"list(Secret/secret+owner,ConfigMap/$configMapName+owner)", "apply") ++
      (if (gateRemoved) Seq("remove gate") else Nil)
  }

  before {
    MockitoAnnotations.openMocks(this).close()
    kconf = driverConf()
    useSpec(BUILT_KUBERNETES_SPEC)
    when(kubernetesClient.pods()).thenReturn(podOperations)
    when(podOperations.inNamespace(kconf.namespace)).thenReturn(podsWithNamespace)
    when(podsWithNamespace.withName(POD_NAME)).thenReturn(namedPods)

    events.clear()
    podTargets.clear()
    lastPodResource = null
    createdPodArgumentCaptor = ArgumentCaptor.forClass(classOf[Pod])
    createdResourcesArgumentCaptor = ArgumentCaptor.forClass(classOf[Array[HasMetadata]])
    patchContextCaptor = ArgumentCaptor.forClass(classOf[PatchContext])
    patchBodyCaptor = ArgumentCaptor.forClass(classOf[String])
    when(podsWithNamespace.resource(createdPodArgumentCaptor.capture())).thenReturn(namedPods)
    when(podOperations.resource(ArgumentMatchers.any[Pod]())).thenAnswer(
      new Answer[PodResource] {
        override def answer(invocation: InvocationOnMock): PodResource = {
          lastPodResource = invocation.getArgument[Pod](0)
          namedPods
        }
      })
    when(resourceList.forceConflicts()).thenReturn(resourceList)
    when(resourceList.serverSideApply()).thenAnswer(answer {
      events += "apply"
      java.util.Collections.emptyList[HasMetadata]()
    })
    when(resourceList.delete()).thenAnswer(answer {
      events += "delete resources"
      java.util.Collections.emptyList[StatusDetails]()
    })
    when(namedPods.serverSideApply()).thenReturn(podWithOwnerReference())
    when(namedPods.create()).thenAnswer(answer {
      events += "create pod"
      podWithOwnerReference()
    })
    when(namedPods.patch(patchContextCaptor.capture(), patchBodyCaptor.capture()))
      .thenAnswer(answer {
        events += "remove gate"
        podTargets += "remove gate" -> lastPodResource
        podWithOwnerReference()
      })
    when(namedPods.delete()).thenAnswer(answer {
      events += "delete pod"
      podTargets += "delete pod" -> lastPodResource
      java.util.Collections.emptyList[StatusDetails]()
    })
    when(namedPods.watch(loggingPodStatusWatcher)).thenReturn(mock[Watch])
    val sId = submissionId(kconf.namespace, POD_NAME)
    when(loggingPodStatusWatcher.watchOrStop(sId)).thenReturn(true)
    resourceListFor = _ => resourceList
    doAnswer(new Answer[RESOURCE_LIST] {
      override def answer(invocation: InvocationOnMock): RESOURCE_LIST = {
        val resources = invocation.getArguments.toSeq.map(_.asInstanceOf[HasMetadata])
        events += s"list(${resources.map(describe).mkString(",")})"
        resourceListFor(resources)
      }
    }).when(kubernetesClient).resourceList(createdResourcesArgumentCaptor.capture(): _*)
  }

  test("The client should configure the pod using the builder.") {
    newClient().run()
    assert(createdPodArgumentCaptor.getValue === fullExpectedPod())
    verify(namedPods).create()
  }

  test("The client should create Kubernetes resources") {
    newClient().run()
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
    assert(configMap.getMetadata.getName === mountedConfigMapName)
    assert(configMap.getImmutable())
    assert(configMap.getData.containsKey(SPARK_CONF_FILE_NAME))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf1key=conf1value"))
    assert(configMap.getData.get(SPARK_CONF_FILE_NAME).contains("conf2key=conf2value"))
  }

  test("SPARK-38079: the driver pod is created gated, and the gate is removed last") {
    newClient().run()

    assert(events.toSeq === submissionEvents())
    assert(gateNames(createdPodArgumentCaptor.getValue) === Seq(DRIVER_RESOURCES_SCHEDULING_GATE))
  }

  test("SPARK-38079: pre resources are applied before the gated pod and owned after it") {
    useSpec(BUILT_KUBERNETES_SPEC_WITH_PRERES)

    newClient().run()

    assert(events.toSeq ===
      submissionEvents(preResource = Some("CustomResourceDefinition/preCRD")))
  }

  test("SPARK-38079: the gate is removed by name from the pod that create() returned") {
    newClient().run()

    verify(namedPods).patch(
      ArgumentMatchers.any[PatchContext](), ArgumentMatchers.any[String]())
    assert(patchContextCaptor.getValue.getPatchType === PatchType.STRATEGIC_MERGE)
    assert(patchBodyCaptor.getValue ===
      """{"spec":{"schedulingGates":[{"$patch":"delete",""" +
        s""""name":"$DRIVER_RESOURCES_SCHEDULING_GATE"}]}}""")
    assert(podTargets.map(_._1).toSeq === Seq("remove gate"))
    assert(podTargets.map(_._2.getMetadata.getUid).toSet === Set(DRIVER_POD_UID))
  }

  test("SPARK-38079: gates already on the pod template are kept next to the Spark gate") {
    useSpec(specWithPod(withGates(BUILT_DRIVER_POD, TEMPLATE_GATE)))

    newClient().run()

    assert(gateNames(createdPodArgumentCaptor.getValue) ===
      Seq(TEMPLATE_GATE, DRIVER_RESOURCES_SCHEDULING_GATE))
    // The patch is a strategic merge patch that names only Spark's gate.
    assert(patchContextCaptor.getValue.getPatchType === PatchType.STRATEGIC_MERGE)
    assert(!patchBodyCaptor.getValue.contains(TEMPLATE_GATE))
  }

  test("SPARK-38079: a Spark gate already on the pod is not added a second time") {
    useSpec(specWithPod(
      withGates(BUILT_DRIVER_POD, TEMPLATE_GATE, DRIVER_RESOURCES_SCHEDULING_GATE)))

    newClient().run()

    assert(gateNames(createdPodArgumentCaptor.getValue) ===
      Seq(TEMPLATE_GATE, DRIVER_RESOURCES_SCHEDULING_GATE))
    assert(events.last === "remove gate")
  }

  test("SPARK-38079: the gate is off by default, so the previous flow needs no patch") {
    kconf = KubernetesTestConf.createDriverConf(
      resourceNamePrefix = Some(KUBERNETES_RESOURCE_PREFIX))
    assert(!kconf.get(USE_DRIVER_SCHEDULING_GATE))
    useSpec(BUILT_KUBERNETES_SPEC)

    newClient().run()

    assert(createdPodArgumentCaptor.getValue === fullExpectedPod(gated = false))
    assert(events.toSeq === submissionEvents(gateRemoved = false))
  }

  test("SPARK-38079: disabling the gate keeps the previous flow and the template's gates") {
    kconf = driverConf(gate = false)
    useSpec(specWithPod(withGates(BUILT_DRIVER_POD, TEMPLATE_GATE)))

    newClient().run()

    assert(createdPodArgumentCaptor.getValue ===
      withGates(fullExpectedPod(gated = false), TEMPLATE_GATE))
    assert(events.toSeq === submissionEvents(gateRemoved = false))
  }

  test("SPARK-38079: a pod pinned with spec.nodeName is neither gated nor patched") {
    val pinnedPod = new PodBuilder(BUILT_DRIVER_POD)
      .editSpec()
        .withNodeName("some-node")
        .endSpec()
      .build()
    useSpec(specWithPod(pinnedPod))

    newClient().run()

    val expected = new PodBuilder(fullExpectedPod(gated = false))
      .editSpec().withNodeName("some-node").endSpec().build()
    assert(createdPodArgumentCaptor.getValue === expected)
    assert(events.toSeq === submissionEvents(gateRemoved = false))
  }

  test("SPARK-38079: a pinned pod keeps the gates of its template") {
    val pinnedPod = withGates(new PodBuilder(BUILT_DRIVER_POD)
      .editSpec().withNodeName("some-node").endSpec().build(), TEMPLATE_GATE)
    useSpec(specWithPod(pinnedPod))

    newClient().run()

    assert(gateNames(createdPodArgumentCaptor.getValue) === Seq(TEMPLATE_GATE))
    assert(events.toSeq === submissionEvents(gateRemoved = false))
  }

  test("SPARK-38079: a failure applying the pre resources deletes them and creates no pod") {
    useSpec(BUILT_KUBERNETES_SPEC_WITH_PRERES)
    val failure = new RuntimeException("apply failed")
    failingWhen(failure)(_.exists(_.isInstanceOf[CustomResourceDefinition]))

    val thrown = intercept[RuntimeException](newClient().run())

    assert(thrown eq failure)
    assert(events.toSeq === Seq(
      "list(CustomResourceDefinition/preCRD)",
      "list(CustomResourceDefinition/preCRD)", "delete resources"))
    assert(podTargets.isEmpty)
  }

  test("SPARK-38079: a failure creating the pod deletes nothing but the pre resources") {
    val failure = new RuntimeException("create failed")
    failPodCall(failure)

    val thrown = intercept[RuntimeException](newClient().run())

    assert(thrown eq failure)
    assert(events.toSeq === Seq("list()", "apply", "create pod", "list()", "delete resources"))
    assert(podTargets.isEmpty)
  }

  test("SPARK-38079: a failure refreshing the pre resources' owners deletes the pod and them") {
    useSpec(BUILT_KUBERNETES_SPEC_WITH_PRERES)
    val failure = new RuntimeException("owner refresh failed")
    failingWhen(failure) { resources =>
      resources.exists(r => r.isInstanceOf[CustomResourceDefinition] &&
        r.getMetadata.getOwnerReferences != null && !r.getMetadata.getOwnerReferences.isEmpty)
    }

    val thrown = intercept[RuntimeException](newClient().run())

    assert(thrown eq failure)
    val owned = "list(CustomResourceDefinition/preCRD+owner)"
    assert(events.toSeq.drop(3) === Seq(owned, "delete pod", owned, "delete resources"))
    assert(!events.contains("remove gate"))
  }

  test("SPARK-38079: a failure applying the post resources deletes only the pod") {
    val failure = new RuntimeException("apply failed")
    failingWhen(failure)(_.exists(_.isInstanceOf[Secret]))

    val thrown = intercept[RuntimeException](newClient().run())

    assert(thrown eq failure)
    assert(events.last === "delete pod")
    assert(!events.contains("remove gate"))
    assert(!events.contains("delete resources"))
    // The delete goes through the pod returned by create().
    assert(podTargets.map(_._2.getMetadata.getUid).toSet === Set(DRIVER_POD_UID))
  }

  test("SPARK-38079: a failure deleting the pod does not hide the original failure") {
    val failure = new RuntimeException("apply failed")
    failingWhen(failure)(_.exists(_.isInstanceOf[Secret]))
    failPodDelete(new RuntimeException("delete failed"))

    val thrown = intercept[RuntimeException](newClient().run())

    assert(thrown eq failure)
    assert(events.last === "delete pod")
  }

  test("SPARK-38079: a failure removing the gate deletes the pod and propagates") {
    val failure = new RuntimeException("patch failed")
    failGatePatch(failure)

    val thrown = intercept[RuntimeException](newClient().run())

    assert(thrown eq failure)
    assert(events.takeRight(2) === Seq("remove gate", "delete pod"))
    assert(podTargets.map(_._1).toSeq === Seq("remove gate", "delete pod"))
    assert(podTargets.map(_._2.getMetadata.getUid).toSet === Set(DRIVER_POD_UID))
    verify(namedPods, never()).watch(loggingPodStatusWatcher)
  }

  test("SPARK-38079: a failure removing the gate is not hidden by a failure deleting the pod") {
    val failure = new KubernetesClientException("forbidden", 403, null)
    failGatePatch(failure)
    failPodDelete(new RuntimeException("delete failed"))

    val thrown = intercept[KubernetesClientException](newClient().run())

    assert(thrown eq failure)
    assert(events.takeRight(2) === Seq("remove gate", "delete pod"))
  }

  test("SPARK-37331: The client should create Kubernetes resources with pre resources") {
    val sparkConf = new SparkConf(false)
      .set(Config.CONTAINER_IMAGE, "spark-executor:latest")
      .set(Config.KUBERNETES_DRIVER_POD_FEATURE_STEPS.key,
        "org.apache.spark.deploy.k8s.TestStepTwo," +
          "org.apache.spark.deploy.k8s.TestStep")
    val preResKconf: KubernetesDriverConf = driverConf(sparkConf = sparkConf)

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
    assert(configMap.getMetadata.getName === mountedConfigMapName)
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

    val expectedKeyToPaths = (expectedConfFiles.map(keyToPath).toList ++
      List(KEY_TO_PATH)).sortBy(x => x.getKey)

    kconf = driverConf(sparkConf = sparkConf)

    assert(kconf.sparkConf.getOption("spark.home").isDefined)
    useSpec(BUILT_KUBERNETES_SPEC)

    newClient().run()
    assert(createdPodArgumentCaptor.getValue === fullExpectedPod(expectedKeyToPaths))
    val otherCreatedResources = createdResourcesArgumentCaptor.getAllValues.asScala.flatten

    val configMaps = otherCreatedResources.toArray
      .filter(_.isInstanceOf[ConfigMap]).map(_.asInstanceOf[ConfigMap])
    assert(configMaps.nonEmpty)
    val configMapName = KubernetesClientUtils.configMapNameDriver
    val configMap: ConfigMap = configMaps.head
    assert(configMap.getMetadata.getName == configMapName)
    assert(configMap.getMetadata.getName == mountedConfigMapName)
    val configMapLoadedFiles = configMap.getData.keySet().asScala.toSet -
        Config.KUBERNETES_NAMESPACE.key
    assert(configMapLoadedFiles === expectedConfFiles.toSet ++ Set(SPARK_CONF_FILE_NAME))
    for (f <- configMapLoadedFiles) {
      assert(configMap.getData.get(f).contains("conf1key=conf1value"))
    }
  }

  test("Waiting for app completion should stall on the watcher") {
    newClient().run()
    verify(loggingPodStatusWatcher).watchOrStop(submissionId(kconf.namespace, POD_NAME))
  }

  test("SPARK-42813: Print application info when waitAppCompletion is false") {
    val appName = "SPARK-42813"
    val logAppender = new LogAppender
    withLogAppender(logAppender) {
      val sparkConf = new SparkConf(loadDefaults = false)
        .set("spark.app.name", appName)
        .set(WAIT_FOR_APP_COMPLETION, false)
      kconf = driverConf(sparkConf = sparkConf)
      useSpec(BUILT_KUBERNETES_SPEC)
      newClient().run()
    }
    // Nothing waits for the driver here, so the gate must be gone before spark-submit returns.
    assert(events.last === "remove gate")
    val appId = KubernetesTestConf.APP_ID
    val sId = submissionId(kconf.namespace, POD_NAME)
    assert(logAppender.loggingEvents.map(_.getMessage.getFormattedMessage).contains(
      s"Deployed Spark application $appName with application ID $appId " +
      s"and submission ID $sId into Kubernetes"))
  }
}
