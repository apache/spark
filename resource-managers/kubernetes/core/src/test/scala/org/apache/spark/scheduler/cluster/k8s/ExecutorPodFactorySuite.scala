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
package org.apache.spark.scheduler.cluster.k8s

<<<<<<< HEAD
import io.fabric8.kubernetes.api.model.{Pod, PodBuilder, VolumeBuilder, VolumeMountBuilder}
import io.fabric8.kubernetes.client.KubernetesClient
import org.mockito.{AdditionalAnswers, Mock, Mockito, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{PodWithDetachedInitContainer, SparkPodInitContainerBootstrap}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._
import org.apache.spark.deploy.k8s.submit.{MountSecretsBootstrapImpl, MountSmallFilesBootstrap, MountSmallFilesBootstrapImpl}

class ExecutorPodFactorySuite extends SparkFunSuite with BeforeAndAfter with BeforeAndAfterEach {
  private val driverPodName: String = "driver-pod"
  private val driverPodUid: String = "driver-uid"
  private val driverUrl: String = "driver-url"
=======
import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model._
import org.mockito.{AdditionalAnswers, MockitoAnnotations}
import org.mockito.Matchers.any
import org.mockito.Mockito._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{InitContainerBootstrap, MountSecretsBootstrap, PodWithDetachedInitContainer, SecretVolumeUtils}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._

class ExecutorPodFactorySuite extends SparkFunSuite with BeforeAndAfter with BeforeAndAfterEach {

  private val driverPodName: String = "driver-pod"
  private val driverPodUid: String = "driver-uid"
>>>>>>> master
  private val executorPrefix: String = "base"
  private val executorImage: String = "executor-image"
  private val driverPod = new PodBuilder()
    .withNewMetadata()
<<<<<<< HEAD
      .withName(driverPodName)
      .withUid(driverPodUid)
      .endMetadata()
    .withNewSpec()
      .withNodeName("some-node")
      .endSpec()
    .withNewStatus()
      .withHostIP("192.168.99.100")
      .endStatus()
    .build()
  private var baseConf: SparkConf = _

  @Mock
  private var nodeAffinityExecutorPodModifier: NodeAffinityExecutorPodModifier = _

  @Mock
  private var executorLocalDirVolumeProvider: ExecutorLocalDirVolumeProvider = _

=======
    .withName(driverPodName)
    .withUid(driverPodUid)
    .endMetadata()
    .withNewSpec()
    .withNodeName("some-node")
    .endSpec()
    .withNewStatus()
    .withHostIP("192.168.99.100")
    .endStatus()
    .build()
  private var baseConf: SparkConf = _

>>>>>>> master
  before {
    MockitoAnnotations.initMocks(this)
    baseConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, driverPodName)
      .set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, executorPrefix)
<<<<<<< HEAD
      .set(EXECUTOR_DOCKER_IMAGE, executorImage)
    when(nodeAffinityExecutorPodModifier.addNodeAffinityAnnotationIfUseful(
      any(classOf[Pod]),
      any(classOf[Map[String, Int]]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    when(executorLocalDirVolumeProvider.getExecutorLocalDirVolumesWithMounts).thenReturn(Seq.empty)
  }
  private var kubernetesClient: KubernetesClient = _

  test("basic executor pod has reasonable defaults") {
    val factory = new ExecutorPodFactoryImpl(
      baseConf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    // The executor pod name and default labels.
    assert(executor.getMetadata.getName === s"$executorPrefix-exec-1")
    assert(executor.getMetadata.getLabels.size() === 3)
=======
      .set(CONTAINER_IMAGE, executorImage)
  }

  test("basic executor pod has reasonable defaults") {
    val factory = new ExecutorPodFactory(baseConf, None, None, None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    // The executor pod name and default labels.
    assert(executor.getMetadata.getName === s"$executorPrefix-exec-1")
    assert(executor.getMetadata.getLabels.size() === 3)
    assert(executor.getMetadata.getLabels.get(SPARK_EXECUTOR_ID_LABEL) === "1")
>>>>>>> master

    // There is exactly 1 container with no volume mounts and default memory limits.
    // Default memory limit is 1024M + 384M (minimum overhead constant).
    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getImage === executorImage)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.isEmpty)
    assert(executor.getSpec.getContainers.get(0).getResources.getLimits.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getResources
      .getLimits.get("memory").getAmount === "1408Mi")

    // The pod has no node selector, volumes.
    assert(executor.getSpec.getNodeSelector.isEmpty)
    assert(executor.getSpec.getVolumes.isEmpty)

    checkEnv(executor, Map())
    checkOwnerReferences(executor, driverPodUid)
  }

  test("executor pod hostnames get truncated to 63 characters") {
    val conf = baseConf.clone()
    conf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX,
      "loremipsumdolorsitametvimatelitrefficiendisuscipianturvixlegeresple")

<<<<<<< HEAD
    val factory = new ExecutorPodFactoryImpl(
        conf,
        nodeAffinityExecutorPodModifier,
        None,
        None,
        None,
        None,
        executorLocalDirVolumeProvider)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getHostname.length === 63)
  }

  test("secrets get mounted") {
    val conf = baseConf.clone()

    val secretsBootstrap = new MountSecretsBootstrapImpl(Map("secret1" -> "/var/secret1"))
    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      Some(secretsBootstrap),
      None,
      None,
      None,
      executorLocalDirVolumeProvider)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

=======
    val factory = new ExecutorPodFactory(conf, None, None, None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    assert(executor.getSpec.getHostname.length === 63)
  }

  test("classpath and extra java options get translated into environment variables") {
    val conf = baseConf.clone()
    conf.set(org.apache.spark.internal.config.EXECUTOR_JAVA_OPTIONS, "foo=bar")
    conf.set(org.apache.spark.internal.config.EXECUTOR_CLASS_PATH, "bar=baz")

    val factory = new ExecutorPodFactory(conf, None, None, None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)]("qux" -> "quux"), driverPod, Map[String, Int]())

    checkEnv(executor,
      Map("SPARK_JAVA_OPT_0" -> "foo=bar",
        ENV_CLASSPATH -> "bar=baz",
        "qux" -> "quux"))
    checkOwnerReferences(executor, driverPodUid)
  }

  test("executor secrets get mounted") {
    val conf = baseConf.clone()

    val secretsBootstrap = new MountSecretsBootstrap(Map("secret1" -> "/var/secret1"))
    val factory = new ExecutorPodFactory(
      conf,
      Some(secretsBootstrap),
      None,
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

>>>>>>> master
    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0).getName
      === "secret1-volume")
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getMountPath === "/var/secret1")

    // check volume mounted.
    assert(executor.getSpec.getVolumes.size() === 1)
    assert(executor.getSpec.getVolumes.get(0).getSecret.getSecretName === "secret1")

    checkOwnerReferences(executor, driverPodUid)
  }

  test("init-container bootstrap step adds an init container") {
    val conf = baseConf.clone()
<<<<<<< HEAD
    val initContainerBootstrap = mock(classOf[SparkPodInitContainerBootstrap])
    when(initContainerBootstrap.bootstrapInitContainerAndVolumes(
      any(classOf[PodWithDetachedInitContainer]))).thenAnswer(AdditionalAnswers.returnsFirstArg())

    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      Some(initContainerBootstrap),
      None,
      executorLocalDirVolumeProvider)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getMetadata.getAnnotations.size() === 1)
    assert(executor.getMetadata.getAnnotations.containsKey(INIT_CONTAINER_ANNOTATION))
    checkOwnerReferences(executor, driverPodUid)
  }

  test("The local dir volume provider's returned volumes and volume mounts should be added.") {
    Mockito.reset(executorLocalDirVolumeProvider)
    val localDirVolume = new VolumeBuilder()
        .withName("local-dir")
        .withNewEmptyDir().endEmptyDir()
        .build()
    val localDirVolumeMount = new VolumeMountBuilder()
        .withName("local-dir")
        .withMountPath("/tmp")
        .build()
    when(executorLocalDirVolumeProvider.getExecutorLocalDirVolumesWithMounts)
        .thenReturn(Seq((localDirVolume, localDirVolumeMount)))
    val factory = new ExecutorPodFactoryImpl(
      baseConf,
      nodeAffinityExecutorPodModifier,
      None,
      None,
      None,
      None,
      executorLocalDirVolumeProvider)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())
    assert(executor.getSpec.getVolumes.size === 1)
    assert(executor.getSpec.getVolumes.contains(localDirVolume))
    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.contains(localDirVolumeMount))
  }

  test("Small-files add a secret & secret volume mount to the container") {
    val conf = baseConf.clone()

    val smallFiles = new MountSmallFilesBootstrapImpl("secret1", "/var/secret1")
    val factory = new ExecutorPodFactoryImpl(
      conf,
      nodeAffinityExecutorPodModifier,
      None,
      Some(smallFiles),
      None,
      None,
      executorLocalDirVolumeProvider)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getName === "submitted-files")
    assert(executor.getSpec.getContainers.get(0).getVolumeMounts.get(0)
      .getMountPath === "/var/secret1")

    assert(executor.getSpec.getVolumes.size() === 1)
    assert(executor.getSpec.getVolumes.get(0).getSecret.getSecretName === "secret1")

    checkOwnerReferences(executor, driverPodUid)
    checkEnv(executor, Map("SPARK_MOUNTED_FILES_FROM_SECRET_DIR" -> "/var/secret1"))
  }

  test("classpath and extra java options get translated into environment variables") {
    val conf = baseConf.clone()
    conf.set(org.apache.spark.internal.config.EXECUTOR_JAVA_OPTIONS, "foo=bar")
    conf.set(org.apache.spark.internal.config.EXECUTOR_CLASS_PATH, "bar=baz")

    val factory = new ExecutorPodFactoryImpl(
        conf,
        nodeAffinityExecutorPodModifier,
        None,
        None,
        None,
        None,
        executorLocalDirVolumeProvider)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)]("qux" -> "quux"), driverPod, Map[String, Int]())

    verify(nodeAffinityExecutorPodModifier, times(1))
      .addNodeAffinityAnnotationIfUseful(any(classOf[Pod]), any(classOf[Map[String, Int]]))

    checkEnv(executor,
      Map("SPARK_JAVA_OPT_0" -> "foo=bar",
          "SPARK_EXECUTOR_EXTRA_CLASSPATH" -> "bar=baz",
          "qux" -> "quux"))
    checkOwnerReferences(executor, driverPodUid)
=======
    val initContainerBootstrap = mock(classOf[InitContainerBootstrap])
    when(initContainerBootstrap.bootstrapInitContainer(
      any(classOf[PodWithDetachedInitContainer]))).thenAnswer(AdditionalAnswers.returnsFirstArg())

    val factory = new ExecutorPodFactory(
      conf,
      None,
      Some(initContainerBootstrap),
      None)
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    assert(executor.getSpec.getInitContainers.size() === 1)
    checkOwnerReferences(executor, driverPodUid)
  }

  test("init-container with secrets mount bootstrap") {
    val conf = baseConf.clone()
    val initContainerBootstrap = mock(classOf[InitContainerBootstrap])
    when(initContainerBootstrap.bootstrapInitContainer(
      any(classOf[PodWithDetachedInitContainer]))).thenAnswer(AdditionalAnswers.returnsFirstArg())
    val secretsBootstrap = new MountSecretsBootstrap(Map("secret1" -> "/var/secret1"))

    val factory = new ExecutorPodFactory(
      conf,
      Some(secretsBootstrap),
      Some(initContainerBootstrap),
      Some(secretsBootstrap))
    val executor = factory.createExecutorPod(
      "1", "dummy", "dummy", Seq[(String, String)](), driverPod, Map[String, Int]())

    assert(executor.getSpec.getVolumes.size() === 1)
    assert(SecretVolumeUtils.podHasVolume(executor, "secret1-volume"))
    assert(SecretVolumeUtils.containerHasVolume(
      executor.getSpec.getContainers.get(0), "secret1-volume", "/var/secret1"))
    assert(executor.getSpec.getInitContainers.size() === 1)
    assert(SecretVolumeUtils.containerHasVolume(
      executor.getSpec.getInitContainers.get(0), "secret1-volume", "/var/secret1"))

    checkOwnerReferences(executor, driverPodUid)
>>>>>>> master
  }

  // There is always exactly one controller reference, and it points to the driver pod.
  private def checkOwnerReferences(executor: Pod, driverPodUid: String): Unit = {
    assert(executor.getMetadata.getOwnerReferences.size() === 1)
    assert(executor.getMetadata.getOwnerReferences.get(0).getUid === driverPodUid)
    assert(executor.getMetadata.getOwnerReferences.get(0).getController === true)
  }

  // Check that the expected environment variables are present.
  private def checkEnv(executor: Pod, additionalEnvVars: Map[String, String]): Unit = {
    val defaultEnvs = Map(
      ENV_EXECUTOR_ID -> "1",
      ENV_DRIVER_URL -> "dummy",
      ENV_EXECUTOR_CORES -> "1",
      ENV_EXECUTOR_MEMORY -> "1g",
      ENV_APPLICATION_ID -> "dummy",
<<<<<<< HEAD
      ENV_MOUNTED_CLASSPATH -> "/var/spark-data/spark-jars/*",
      ENV_EXECUTOR_POD_IP -> null,
      ENV_EXECUTOR_PORT -> "10000") ++ additionalEnvVars

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getEnv().size() === defaultEnvs.size)
=======
      ENV_EXECUTOR_POD_IP -> null,
      ENV_MOUNTED_CLASSPATH -> "/var/spark-data/spark-jars/*") ++ additionalEnvVars

    assert(executor.getSpec.getContainers.size() === 1)
    assert(executor.getSpec.getContainers.get(0).getEnv.size() === defaultEnvs.size)
>>>>>>> master
    val mapEnvs = executor.getSpec.getContainers.get(0).getEnv.asScala.map {
      x => (x.getName, x.getValue)
    }.toMap
    assert(defaultEnvs === mapEnvs)
  }
}
