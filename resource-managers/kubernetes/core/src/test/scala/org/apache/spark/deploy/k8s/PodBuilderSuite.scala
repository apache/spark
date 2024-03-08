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
package org.apache.spark.deploy.k8s

import java.io.File

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{Config => _, _}
import io.fabric8.kubernetes.client.KubernetesClient
import io.fabric8.kubernetes.client.dsl.PodResource
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, never, verify, when}

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.Fabric8Aliases._
import org.apache.spark.deploy.k8s.features.{KubernetesDriverCustomFeatureConfigStep, KubernetesExecutorCustomFeatureConfigStep, KubernetesFeatureConfigStep}
import org.apache.spark.internal.config.ConfigEntry

abstract class PodBuilderSuite extends SparkFunSuite {
  val POD_ROLE: String
  val TEST_ANNOTATION_KEY: String
  val TEST_ANNOTATION_VALUE: String

  protected def templateFileConf: ConfigEntry[_]

  protected def roleSpecificSchedulerNameConf: ConfigEntry[_]

  protected def userFeatureStepsConf: ConfigEntry[_]

  protected def userFeatureStepWithExpectedAnnotation: (String, String)

  protected def wrongTypeFeatureStep: String

  protected def buildPod(sparkConf: SparkConf, client: KubernetesClient): SparkPod

  protected val baseConf = new SparkConf(false)
    .set(Config.CONTAINER_IMAGE, "spark-executor:latest")

  test("use empty initial pod if template is not specified") {
    val client = mock(classOf[KubernetesClient])
    buildPod(baseConf.clone(), client)
    verify(client, never()).pods()
  }

  test("SPARK-36059: set custom scheduler") {
    val client = mockKubernetesClient()
    val conf1 = baseConf.clone().set(templateFileConf.key, "template-file.yaml")
      .set(Config.KUBERNETES_SCHEDULER_NAME.key, "custom")
    val pod1 = buildPod(conf1, client)
    assert(pod1.pod.getSpec.getSchedulerName === "custom")

    val conf2 = baseConf.clone().set(templateFileConf.key, "template-file.yaml")
      .set(Config.KUBERNETES_SCHEDULER_NAME.key, "custom")
      .set(roleSpecificSchedulerNameConf.key, "rolescheduler")
    val pod2 = buildPod(conf2, client)
    assert(pod2.pod.getSpec.getSchedulerName === "rolescheduler")
  }

  test("load pod template if specified") {
    val client = mockKubernetesClient()
    val sparkConf = baseConf.clone().set(templateFileConf.key, "template-file.yaml")
    val pod = buildPod(sparkConf, client)
    verifyPod(pod)
  }

  test("configure a custom test step") {
    val client = mockKubernetesClient()
    val sparkConf = baseConf.clone()
      .set(userFeatureStepsConf.key,
        "org.apache.spark.deploy.k8s.TestStepTwo," +
        "org.apache.spark.deploy.k8s.TestStep")
      .set(templateFileConf.key, "template-file.yaml")
    val pod = buildPod(sparkConf, client)
    verifyPod(pod)
    assert(pod.container.getVolumeMounts.asScala.exists(_.getName == "so_long"))
    assert(pod.container.getVolumeMounts.asScala.exists(_.getName == "so_long_two"))
  }

  test("SPARK-37145: configure a custom test step with base config") {
    val client = mockKubernetesClient()
    val sparkConf = baseConf.clone()
      .set(userFeatureStepsConf.key,
          "org.apache.spark.deploy.k8s.TestStepWithConf")
      .set(templateFileConf.key, "template-file.yaml")
      .set("test-features-key", "test-features-value")
    val pod = buildPod(sparkConf, client)
    verifyPod(pod)
    val metadata = pod.pod.getMetadata
    assert(metadata.getAnnotations.containsKey("test-features-key"))
    assert(metadata.getAnnotations.get("test-features-key") === "test-features-value")
  }

  test("SPARK-37145: configure a custom test step with driver or executor config") {
    val client = mockKubernetesClient()
    val (featureSteps, annotation) = userFeatureStepWithExpectedAnnotation
    val sparkConf = baseConf.clone()
      .set(templateFileConf.key, "template-file.yaml")
      .set(userFeatureStepsConf.key, featureSteps)
      .set(TEST_ANNOTATION_KEY, annotation)
    val pod = buildPod(sparkConf, client)
    verifyPod(pod)
    val metadata = pod.pod.getMetadata
    assert(metadata.getAnnotations.containsKey(TEST_ANNOTATION_KEY))
    assert(metadata.getAnnotations.get(TEST_ANNOTATION_KEY) === annotation)
  }

  test("SPARK-37145: configure a custom test step with wrong type config") {
    val client = mockKubernetesClient()
    val sparkConf = baseConf.clone()
      .set(templateFileConf.key, "template-file.yaml")
      .set(userFeatureStepsConf.key, wrongTypeFeatureStep)
    val e = intercept[SparkException] {
      buildPod(sparkConf, client)
    }
    assert(e.getMessage.contains(s"please make sure your $POD_ROLE side feature steps"))
  }

  test("SPARK-37145: configure a custom test step with wrong name") {
    val client = mockKubernetesClient()
    val featureSteps = "unknow.class"
    val sparkConf = baseConf.clone()
      .set(templateFileConf.key, "template-file.yaml")
      .set(userFeatureStepsConf.key, featureSteps)
    val e = intercept[ClassNotFoundException] {
      buildPod(sparkConf, client)
    }
    assert(e.getMessage.contains("unknow.class"))
  }

  test("complain about misconfigured pod template") {
    val client = mockKubernetesClient(
      new PodBuilder()
        .withNewMetadata()
        .addToLabels("test-label-key", "test-label-value")
        .endMetadata()
        .build())
    val sparkConf = baseConf.clone().set(templateFileConf.key, "template-file.yaml")
    val exception = intercept[SparkException] {
      buildPod(sparkConf, client)
    }
    assert(exception.getMessage.contains("Could not load pod from template file."))
  }

  protected def mockKubernetesClient(pod: Pod = podWithSupportedFeatures()): KubernetesClient = {
    val kubernetesClient = mock(classOf[KubernetesClient])
    val pods = mock(classOf[PODS])
    val podResource = mock(classOf[PodResource])
    when(kubernetesClient.pods()).thenReturn(pods)
    when(pods.load(any(classOf[File]))).thenReturn(podResource)
    when(podResource.item()).thenReturn(pod)
    kubernetesClient
  }

  private def verifyPod(pod: SparkPod): Unit = {
    val metadata = pod.pod.getMetadata
    assert(metadata.getLabels.containsKey("test-label-key"))
    assert(metadata.getAnnotations.containsKey("test-annotation-key"))
    assert(metadata.getNamespace === "namespace")
    assert(metadata.getOwnerReferences.asScala.exists(_.getName == "owner-reference"))
    val spec = pod.pod.getSpec
    assert(!spec.getContainers.asScala.exists(_.getName == "executor-container"))
    assert(spec.getDnsPolicy === "dns-policy")
    assert(spec.getHostAliases.asScala.exists(_.getHostnames.asScala.exists(_ == "hostname")))
    assert(spec.getImagePullSecrets.asScala.exists(_.getName == "local-reference"))
    assert(spec.getInitContainers.asScala.exists(_.getName == "init-container"))
    assert(spec.getNodeName == "node-name")
    assert(spec.getNodeSelector.get("node-selector-key") === "node-selector-value")
    assert(spec.getSchedulerName === "scheduler")
    assert(spec.getSecurityContext.getRunAsUser === 1000L)
    assert(spec.getServiceAccount === "service-account")
    assert(spec.getSubdomain === "subdomain")
    assert(spec.getTolerations.asScala.exists(_.getKey == "toleration-key"))
    assert(spec.getVolumes.asScala.exists(_.getName == "test-volume"))
    val container = pod.container
    assert(container.getName === "executor-container")
    assert(container.getArgs.contains("arg"))
    assert(container.getCommand.equals(List("command").asJava))
    assert(container.getEnv.asScala.exists(_.getName == "env-key"))
    assert(container.getResources.getLimits.get("gpu") === new Quantity("1"))
    assert(container.getSecurityContext.getRunAsNonRoot)
    assert(container.getStdin)
    assert(container.getTerminationMessagePath === "termination-message-path")
    assert(container.getTerminationMessagePolicy === "termination-message-policy")
    assert(pod.container.getVolumeMounts.asScala.exists(_.getName == "test-volume"))
  }

  private def podWithSupportedFeatures(): Pod = {
    new PodBuilder()
      .withNewMetadata()
        .addToLabels("test-label-key", "test-label-value")
        .addToAnnotations("test-annotation-key", "test-annotation-value")
        .withNamespace("namespace")
        .addNewOwnerReference()
          .withController(true)
          .withName("owner-reference")
          .endOwnerReference()
        .endMetadata()
      .withNewSpec()
        .withDnsPolicy("dns-policy")
        .withHostAliases(new HostAliasBuilder().withHostnames("hostname").build())
        .withImagePullSecrets(
          new LocalObjectReferenceBuilder().withName("local-reference").build())
        .withInitContainers(new ContainerBuilder().withName("init-container").build())
        .withNodeName("node-name")
        .withNodeSelector(Map("node-selector-key" -> "node-selector-value").asJava)
        .withSchedulerName("scheduler")
        .withNewSecurityContext()
          .withRunAsUser(1000L)
          .endSecurityContext()
        .withServiceAccount("service-account")
        .withSubdomain("subdomain")
        .withTolerations(new TolerationBuilder()
          .withKey("toleration-key")
          .withOperator("Equal")
          .withEffect("NoSchedule")
          .build())
        .addNewVolume()
          .withNewHostPath()
          .withPath("/test")
          .endHostPath()
          .withName("test-volume")
          .endVolume()
        .addNewContainer()
          .withArgs("arg")
          .withCommand("command")
          .addNewEnv()
            .withName("env-key")
            .withValue("env-value")
            .endEnv()
          .withImagePullPolicy("Always")
          .withName("executor-container")
          .withNewResources()
            .withLimits(Map("gpu" -> new Quantity("1")).asJava)
            .endResources()
          .withNewSecurityContext()
            .withRunAsNonRoot(true)
            .endSecurityContext()
          .withStdin(true)
          .withTerminationMessagePath("termination-message-path")
          .withTerminationMessagePolicy("termination-message-policy")
          .addToVolumeMounts(
            new VolumeMountBuilder()
              .withName("test-volume")
              .withMountPath("/test")
              .build())
          .endContainer()
        .endSpec()
      .build()
  }

}

/**
 * A test user feature step.
 */
class TestStep extends KubernetesFeatureConfigStep {
  import io.fabric8.kubernetes.api.model._

  override def configurePod(pod: SparkPod): SparkPod = {
    val localDirVolumes = Seq(new VolumeBuilder().withName("so_long").build())
    val localDirVolumeMounts = Seq(
      new VolumeMountBuilder().withName("so_long")
        .withMountPath("and_thanks_for_all_the_fish")
        .build()
    )

    val podWithLocalDirVolumes = new PodBuilder(pod.pod)
      .editSpec()
        .addToVolumes(localDirVolumes: _*)
        .endSpec()
      .build()
    val containerWithLocalDirVolumeMounts = new ContainerBuilder(pod.container)
      .addNewEnv()
        .withName("CUSTOM_SPARK_LOCAL_DIRS")
        .withValue("fishyfishyfishy")
        .endEnv()
      .addToVolumeMounts(localDirVolumeMounts: _*)
      .build()
    SparkPod(podWithLocalDirVolumes, containerWithLocalDirVolumeMounts)
  }
}

/**
 * A test user feature step.
 */
class TestStepTwo extends KubernetesFeatureConfigStep {
  import io.fabric8.kubernetes.api.model._

  override def configurePod(pod: SparkPod): SparkPod = {
    val localDirVolumes = Seq(new VolumeBuilder().withName("so_long_two").build())
    val localDirVolumeMounts = Seq(
      new VolumeMountBuilder().withName("so_long_two")
        .withMountPath("and_thanks_for_all_the_fish_eh")
        .build()
    )

    val podWithLocalDirVolumes = new PodBuilder(pod.pod)
      .editSpec()
        .addToVolumes(localDirVolumes: _*)
        .endSpec()
      .build()
    val containerWithLocalDirVolumeMounts = new ContainerBuilder(pod.container)
      .addNewEnv()
        .withName("CUSTOM_SPARK_LOCAL_DIRS_TWO")
        .withValue("fishyfishyfishyTWO")
        .endEnv()
      .addToVolumeMounts(localDirVolumeMounts: _*)
      .build()
    SparkPod(podWithLocalDirVolumes, containerWithLocalDirVolumeMounts)
  }
}

/**
 * A test user feature step would be used in driver and executor.
 */
class TestStepWithConf extends KubernetesDriverCustomFeatureConfigStep
  with KubernetesExecutorCustomFeatureConfigStep {
  import io.fabric8.kubernetes.api.model._

  private var kubernetesConf: KubernetesConf = _

  override def init(conf: KubernetesDriverConf): Unit = {
    kubernetesConf = conf
  }

  override def init(conf: KubernetesExecutorConf): Unit = {
    kubernetesConf = conf
  }

  override def configurePod(pod: SparkPod): SparkPod = {
    val k8sPodBuilder = new PodBuilder(pod.pod)
      .editOrNewMetadata()
        .addToAnnotations("test-features-key", kubernetesConf.get("test-features-key"))
      .endMetadata()
    val k8sPod = k8sPodBuilder.build()
    SparkPod(k8sPod, pod.container)
  }
}
