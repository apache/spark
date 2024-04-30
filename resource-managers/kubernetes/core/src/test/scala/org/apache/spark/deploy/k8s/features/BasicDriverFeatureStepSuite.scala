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
package org.apache.spark.deploy.k8s.features

import scala.jdk.CollectionConverters._

import io.fabric8.kubernetes.api.model.{ContainerPort, ContainerPortBuilder, LocalObjectReferenceBuilder, Quantity}
import org.apache.hadoop.fs.{LocalFileSystem, Path}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesDriverConf, KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.KubernetesFeaturesTestUtils.TestResourceInformation
import org.apache.spark.deploy.k8s.submit._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.resource.ResourceID
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.util.Utils

class BasicDriverFeatureStepSuite extends SparkFunSuite {

  private val CUSTOM_DRIVER_LABELS = Map(
    "labelkey" -> "labelvalue",
    "customAppIdLabelKey" -> "{{APP_ID}}")
  private val CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent"
  private val DRIVER_ANNOTATIONS = Map(
    "customAnnotation" -> "customAnnotationValue",
    "customAppIdAnnotation" -> "{{APP_ID}}")
  private val DRIVER_ENVS = Map(
    "customDriverEnv1" -> "customDriverEnv1Value",
    "customDriverEnv2" -> "customDriverEnv2Value")
  private val TEST_IMAGE_PULL_SECRETS = Seq("my-secret-1", "my-secret-2")
  private val TEST_IMAGE_PULL_SECRET_OBJECTS =
    TEST_IMAGE_PULL_SECRETS.map { secret =>
      new LocalObjectReferenceBuilder().withName(secret).build()
    }

  test("Check the pod respects all configurations from the user.") {
    val resourceID = new ResourceID(SPARK_DRIVER_PREFIX, GPU)
    val resources =
      Map(("nvidia.com/gpu" -> TestResourceInformation(resourceID, "2", "nvidia.com")))
    val sparkConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, "spark-driver-pod")
      .set(DRIVER_CORES, 2)
      .set(KUBERNETES_DRIVER_LIMIT_CORES, "4")
      .set(DRIVER_MEMORY.key, "256M")
      .set(DRIVER_MEMORY_OVERHEAD, 200L)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(IMAGE_PULL_SECRETS, TEST_IMAGE_PULL_SECRETS)
    resources.foreach { case (_, testRInfo) =>
      sparkConf.set(testRInfo.rId.amountConf, testRInfo.count)
      sparkConf.set(testRInfo.rId.vendorConf, testRInfo.vendor)
    }
    val kubernetesConf: KubernetesDriverConf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = CUSTOM_DRIVER_LABELS,
      environment = DRIVER_ENVS,
      annotations = DRIVER_ANNOTATIONS)

    val featureStep = new BasicDriverFeatureStep(kubernetesConf)
    val basePod = SparkPod.initialPod()
    val configuredPod = featureStep.configurePod(basePod)

    assert(configuredPod.container.getName === DEFAULT_DRIVER_CONTAINER_NAME)
    assert(configuredPod.container.getImage === "spark-driver:latest")
    assert(configuredPod.container.getImagePullPolicy === CONTAINER_IMAGE_PULL_POLICY)

    val expectedPortNames = Set(
      containerPort(DRIVER_PORT_NAME, DEFAULT_DRIVER_PORT),
      containerPort(BLOCK_MANAGER_PORT_NAME, DEFAULT_BLOCKMANAGER_PORT),
      containerPort(UI_PORT_NAME, UI_PORT.defaultValue.get)
    )
    val foundPortNames = configuredPod.container.getPorts.asScala.toSet
    assert(expectedPortNames === foundPortNames)

    val envs = configuredPod.container
      .getEnv
      .asScala
      .map { env => (env.getName, env.getValue) }
      .toMap
    DRIVER_ENVS.foreach { case (k, v) =>
      assert(envs(k) === v)
    }
    assert(envs(ENV_SPARK_USER) === Utils.getCurrentUserName())
    assert(envs(ENV_APPLICATION_ID) === kubernetesConf.appId)

    assert(configuredPod.pod.getSpec().getImagePullSecrets.asScala ===
      TEST_IMAGE_PULL_SECRET_OBJECTS)

    assert(configuredPod.container.getEnv.asScala.exists(envVar =>
      envVar.getName.equals(ENV_DRIVER_BIND_ADDRESS) &&
        envVar.getValueFrom.getFieldRef.getApiVersion.equals("v1") &&
        envVar.getValueFrom.getFieldRef.getFieldPath.equals("status.podIP")))

    val resourceRequirements = configuredPod.container.getResources
    val requests = resourceRequirements.getRequests.asScala
    assert(amountAndFormat(requests("cpu")) === "2")
    assert(amountAndFormat(requests("memory")) === "456Mi")
    val limits = resourceRequirements.getLimits.asScala
    assert(amountAndFormat(limits("memory")) === "456Mi")
    assert(amountAndFormat(limits("cpu")) === "4")
    resources.foreach { case (k8sName, testRInfo) =>
      assert(amountAndFormat(limits(k8sName)) === testRInfo.count)
    }

    val driverPodMetadata = configuredPod.pod.getMetadata
    assert(driverPodMetadata.getName === "spark-driver-pod")

    // Check custom and preset labels are as expected
    val labels = driverPodMetadata.getLabels
    CUSTOM_DRIVER_LABELS.foreach { case (k, v) =>
      assert(labels.get(k) === Utils.substituteAppNExecIds(v, KubernetesTestConf.APP_ID, ""))
    }
    assert(labels === kubernetesConf.labels.asJava)

    val annotations = driverPodMetadata.getAnnotations.asScala
    DRIVER_ANNOTATIONS.foreach { case (k, v) =>
      assert(annotations(k) === Utils.substituteAppNExecIds(v, KubernetesTestConf.APP_ID, ""))
    }
    assert(configuredPod.pod.getSpec.getRestartPolicy === "Never")
    val expectedSparkConf = Map(
      KUBERNETES_DRIVER_POD_NAME.key -> "spark-driver-pod",
      "spark.app.id" -> KubernetesTestConf.APP_ID,
      "spark.kubernetes.submitInDriver" -> "true",
      MEMORY_OVERHEAD_FACTOR.key -> MEMORY_OVERHEAD_FACTOR.defaultValue.get.toString)
    assert(featureStep.getAdditionalPodSystemProperties() === expectedSparkConf)
  }

  test("Check driver pod respects kubernetes driver request cores") {
    val sparkConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, "spark-driver-pod")
      .set(CONTAINER_IMAGE, "spark-driver:latest")

    val basePod = SparkPod.initialPod()
    // if spark.driver.cores is not set default is 1
    val requests1 = new BasicDriverFeatureStep(KubernetesTestConf.createDriverConf(sparkConf))
      .configurePod(basePod)
      .container.getResources
      .getRequests.asScala
    assert(amountAndFormat(requests1("cpu")) === "1")

    // if spark.driver.cores is set it should be used
    sparkConf.set(DRIVER_CORES, 10)
    val requests2 = new BasicDriverFeatureStep(KubernetesTestConf.createDriverConf(sparkConf))
      .configurePod(basePod)
      .container.getResources
      .getRequests.asScala
    assert(amountAndFormat(requests2("cpu")) === "10")

    // spark.kubernetes.driver.request.cores should be preferred over spark.driver.cores
    Seq("0.1", "100m").foreach { value =>
      sparkConf.set(KUBERNETES_DRIVER_REQUEST_CORES, value)
      val requests3 = new BasicDriverFeatureStep(KubernetesTestConf.createDriverConf(sparkConf))
        .configurePod(basePod)
        .container.getResources
        .getRequests.asScala
      assert(amountAndFormat(requests3("cpu")) === value)
    }
  }

  test("Check appropriate entrypoint rerouting for various bindings") {
    val javaSparkConf = new SparkConf()
      .set(DRIVER_MEMORY.key, "4g")
      .set(CONTAINER_IMAGE, "spark-driver:latest")
    val pythonSparkConf = new SparkConf()
      .set(DRIVER_MEMORY.key, "4g")
      .set(CONTAINER_IMAGE, "spark-driver-py:latest")
    val javaKubernetesConf = KubernetesTestConf.createDriverConf(sparkConf = javaSparkConf)
    val pythonKubernetesConf = KubernetesTestConf.createDriverConf(
      sparkConf = pythonSparkConf,
      mainAppResource = PythonMainAppResource(""))
    val javaFeatureStep = new BasicDriverFeatureStep(javaKubernetesConf)
    val pythonFeatureStep = new BasicDriverFeatureStep(pythonKubernetesConf)
    val basePod = SparkPod.initialPod()
    val configuredJavaPod = javaFeatureStep.configurePod(basePod)
    val configuredPythonPod = pythonFeatureStep.configurePod(basePod)
    assert(configuredJavaPod.container.getImage === "spark-driver:latest")
    assert(configuredPythonPod.container.getImage === "spark-driver-py:latest")
  }

  // Memory overhead tests. Tuples are:
  //   test name, main resource, overhead factor, expected factor
  val driverDefault = DRIVER_MEMORY_OVERHEAD_FACTOR.defaultValue.get
  val oldConfigDefault = MEMORY_OVERHEAD_FACTOR.defaultValue.get
  val nonJvm = NON_JVM_MEMORY_OVERHEAD_FACTOR
  Seq(
    ("java", JavaMainAppResource(None), None, driverDefault, oldConfigDefault),
    ("python default", PythonMainAppResource(null), None, nonJvm, nonJvm),
    ("python w/ override", PythonMainAppResource(null), Some(0.9d), 0.9d, nonJvm),
    ("r default", RMainAppResource(null), None, nonJvm, nonJvm)
  ).foreach { case (name, resource, factor, expectedFactor, expectedPropFactor) =>
    test(s"memory overhead factor new config: $name") {
      // Choose a driver memory where the default memory overhead is > MEMORY_OVERHEAD_MIN_MIB
      val driverMem =
        DRIVER_MIN_MEMORY_OVERHEAD.defaultValue.get /
          DRIVER_MEMORY_OVERHEAD_FACTOR.defaultValue.get * 2

      // main app resource, overhead factor
      val sparkConf = new SparkConf(false)
        .set(CONTAINER_IMAGE, "spark-driver:latest")
        .set(DRIVER_MEMORY.key, s"${driverMem.toInt}m")
      factor.foreach { value => sparkConf.set(DRIVER_MEMORY_OVERHEAD_FACTOR, value) }
      val conf = KubernetesTestConf.createDriverConf(
        sparkConf = sparkConf,
        mainAppResource = resource)
      val step = new BasicDriverFeatureStep(conf)
      val pod = step.configurePod(SparkPod.initialPod())
      val mem = amountAndFormat(pod.container.getResources.getRequests.get("memory"))
      val expected = (driverMem + driverMem * expectedFactor).toInt
      assert(mem === s"${expected}Mi")

      val systemProperties = step.getAdditionalPodSystemProperties()
      assert(systemProperties(MEMORY_OVERHEAD_FACTOR.key) === expectedPropFactor.toString)
    }
  }

  Seq(
    ("java", JavaMainAppResource(None), None, driverDefault),
    ("python default", PythonMainAppResource(null), None, nonJvm),
    ("python w/ override", PythonMainAppResource(null), Some(0.9d), 0.9d),
    ("r default", RMainAppResource(null), None, nonJvm)
  ).foreach { case (name, resource, factor, expectedFactor) =>
    test(s"memory overhead factor old config: $name") {
      // Choose a driver memory where the default memory overhead is > MEMORY_OVERHEAD_MIN_MIB
      val driverMem =
        DRIVER_MIN_MEMORY_OVERHEAD.defaultValue.get / MEMORY_OVERHEAD_FACTOR.defaultValue.get * 2

      // main app resource, overhead factor
      val sparkConf = new SparkConf(false)
        .set(CONTAINER_IMAGE, "spark-driver:latest")
        .set(DRIVER_MEMORY.key, s"${driverMem.toInt}m")
      factor.foreach { value => sparkConf.set(MEMORY_OVERHEAD_FACTOR, value) }
      val conf = KubernetesTestConf.createDriverConf(
        sparkConf = sparkConf,
        mainAppResource = resource)
      val step = new BasicDriverFeatureStep(conf)
      val pod = step.configurePod(SparkPod.initialPod())
      val mem = amountAndFormat(pod.container.getResources.getRequests.get("memory"))
      val expected = (driverMem + driverMem * expectedFactor).toInt
      assert(mem === s"${expected}Mi")

      val systemProperties = step.getAdditionalPodSystemProperties()
      assert(systemProperties(MEMORY_OVERHEAD_FACTOR.key) === expectedFactor.toString)
    }
  }

  test(s"SPARK-38194: memory overhead factor precendence") {
    // Choose a driver memory where the default memory overhead is > MEMORY_OVERHEAD_MIN_MIB
    val driverMem =
      DRIVER_MIN_MEMORY_OVERHEAD.defaultValue.get /
        DRIVER_MEMORY_OVERHEAD_FACTOR.defaultValue.get * 2

    // main app resource, overhead factor
    val sparkConf = new SparkConf(false)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(DRIVER_MEMORY.key, s"${driverMem.toInt}m")

    // New config should take precedence
    val expectedFactor = 0.2
    val oldFactor = 0.3
    sparkConf.set(DRIVER_MEMORY_OVERHEAD_FACTOR, expectedFactor)
    sparkConf.set(MEMORY_OVERHEAD_FACTOR, oldFactor)

    val conf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf)
    val step = new BasicDriverFeatureStep(conf)
    val pod = step.configurePod(SparkPod.initialPod())
    val mem = amountAndFormat(pod.container.getResources.getRequests.get("memory"))
    val expected = (driverMem + driverMem * expectedFactor).toInt
    assert(mem === s"${expected}Mi")

    // The old config should be passed as a system property for use by the executor
    val systemProperties = step.getAdditionalPodSystemProperties()
    assert(systemProperties(MEMORY_OVERHEAD_FACTOR.key) === oldFactor.toString)
  }

  test(s"SPARK-38194: old memory factor settings is applied if new one isn't given") {
    // Choose a driver memory where the default memory overhead is > MEMORY_OVERHEAD_MIN_MIB
    val driverMem =
      DRIVER_MIN_MEMORY_OVERHEAD.defaultValue.get /
        DRIVER_MEMORY_OVERHEAD_FACTOR.defaultValue.get * 2

    // main app resource, overhead factor
    val sparkConf = new SparkConf(false)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(DRIVER_MEMORY.key, s"${driverMem.toInt}m")

    // Old config still works if new config isn't given
    val expectedFactor = 0.3
    sparkConf.set(MEMORY_OVERHEAD_FACTOR, expectedFactor)

    val conf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf)
    val step = new BasicDriverFeatureStep(conf)
    val pod = step.configurePod(SparkPod.initialPod())
    val mem = amountAndFormat(pod.container.getResources.getRequests.get("memory"))
    val expected = (driverMem + driverMem * expectedFactor).toInt
    assert(mem === s"${expected}Mi")

    val systemProperties = step.getAdditionalPodSystemProperties()
    assert(systemProperties(MEMORY_OVERHEAD_FACTOR.key) === expectedFactor.toString)
  }

  test("SPARK-35493: make spark.blockManager.port be able to be fallen back to in driver pod") {
    val initPod = SparkPod.initialPod()
    val sparkConf = new SparkConf()
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(BLOCK_MANAGER_PORT, 1234)
    val driverConf1 = KubernetesTestConf.createDriverConf(sparkConf)
    val pod1 = new BasicDriverFeatureStep(driverConf1).configurePod(initPod)
    val portMap1 =
      pod1.container.getPorts.asScala.map { cp => (cp.getName -> cp.getContainerPort) }.toMap
    assert(portMap1(BLOCK_MANAGER_PORT_NAME) === 1234, s"fallback to $BLOCK_MANAGER_PORT.key")

    val driverConf2 =
      KubernetesTestConf.createDriverConf(sparkConf.set(DRIVER_BLOCK_MANAGER_PORT, 1235))
    val pod2 = new BasicDriverFeatureStep(driverConf2).configurePod(initPod)
    val portMap2 =
      pod2.container.getPorts.asScala.map { cp => (cp.getName -> cp.getContainerPort) }.toMap
    assert(portMap2(BLOCK_MANAGER_PORT_NAME) === 1235)
  }

  test("SPARK-36075: Check driver pod respects nodeSelector/driverNodeSelector") {
    val initPod = SparkPod.initialPod()
    val sparkConf = new SparkConf()
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(s"${KUBERNETES_NODE_SELECTOR_PREFIX}nodeLabelKey", "nodeLabelValue")
      .set(s"${KUBERNETES_DRIVER_NODE_SELECTOR_PREFIX}driverNodeLabelKey", "driverNodeLabelValue")
      .set(s"${KUBERNETES_EXECUTOR_NODE_SELECTOR_PREFIX}execNodeLabelKey", "execNodeLabelValue")
    val driverConf = KubernetesTestConf.createDriverConf(sparkConf)
    val driver = new BasicDriverFeatureStep(driverConf).configurePod(initPod)
    assert(driver.pod.getSpec.getNodeSelector.asScala === Map(
      "nodeLabelKey" -> "nodeLabelValue",
      "driverNodeLabelKey" -> "driverNodeLabelValue"
    ))
  }

  test("SPARK-40817: Check that remote JARs do not get discarded in spark.jars") {
    val FILE_UPLOAD_PATH = "s3a://some-bucket/upload-path"
    val REMOTE_JAR_URI = "s3a://some-bucket/my-application.jar"
    val LOCAL_JAR_URI = "/tmp/some-local-jar.jar"

    val sparkConf = new SparkConf()
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(JARS, Seq(REMOTE_JAR_URI, LOCAL_JAR_URI))
      .set(KUBERNETES_FILE_UPLOAD_PATH, FILE_UPLOAD_PATH)
      // Instead of using the real S3A Hadoop driver, use a fake local one
      .set("spark.hadoop.fs.s3a.impl", classOf[TestFileSystem].getCanonicalName)
      .set("spark.hadoop.fs.s3a.impl.disable.cache", "true")
    val kubernetesConf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val featureStep = new BasicDriverFeatureStep(kubernetesConf)

    val sparkJars = featureStep.getAdditionalPodSystemProperties()(JARS.key).split(",")

    // Both the remote and the local JAR should be there
    assert(sparkJars.size == 2)
    // The remote JAR path should have been left untouched
    assert(sparkJars.contains(REMOTE_JAR_URI))
    // The local JAR should have been uploaded to spark.kubernetes.file.upload.path
    assert(!sparkJars.contains(LOCAL_JAR_URI))
    assert(sparkJars.exists(path =>
      path.startsWith(FILE_UPLOAD_PATH) && path.endsWith("some-local-jar.jar")))
  }

  test("SPARK-47208: User can override the minimum memory overhead of the driver") {
    val sparkConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, "spark-driver-pod")
      .set(DRIVER_MEMORY.key, "256M")
      .set(DRIVER_MIN_MEMORY_OVERHEAD, 500L)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
    val kubernetesConf: KubernetesDriverConf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = CUSTOM_DRIVER_LABELS,
      environment = DRIVER_ENVS,
      annotations = DRIVER_ANNOTATIONS)

    val featureStep = new BasicDriverFeatureStep(kubernetesConf)
    val basePod = SparkPod.initialPod()
    val configuredPod = featureStep.configurePod(basePod)

    val resourceRequirements = configuredPod.container.getResources
    val requests = resourceRequirements.getRequests.asScala
    assert(amountAndFormat(requests("memory")) === "756Mi")
    val limits = resourceRequirements.getLimits.asScala
    assert(amountAndFormat(limits("memory")) === "756Mi")
  }

  test("SPARK-47208: Explicit overhead takes precedence over minimum overhead") {
    val sparkConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, "spark-driver-pod")
      .set(DRIVER_MEMORY.key, "256M")
      .set(DRIVER_MIN_MEMORY_OVERHEAD, 500L)
      .set(DRIVER_MEMORY_OVERHEAD, 200L)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
    val kubernetesConf: KubernetesDriverConf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = CUSTOM_DRIVER_LABELS,
      environment = DRIVER_ENVS,
      annotations = DRIVER_ANNOTATIONS)

    val featureStep = new BasicDriverFeatureStep(kubernetesConf)
    val basePod = SparkPod.initialPod()
    val configuredPod = featureStep.configurePod(basePod)

    val resourceRequirements = configuredPod.container.getResources
    val requests = resourceRequirements.getRequests.asScala
    assert(amountAndFormat(requests("memory")) === "456Mi")
    val limits = resourceRequirements.getLimits.asScala
    assert(amountAndFormat(limits("memory")) === "456Mi")
  }

  test("SPARK-47208: Overhead is maximum between factor of memory and min base overhead") {
    val sparkConf = new SparkConf()
      .set(KUBERNETES_DRIVER_POD_NAME, "spark-driver-pod")
      .set(DRIVER_MEMORY.key, "5000M")
      .set(DRIVER_MIN_MEMORY_OVERHEAD, 200L)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
    val kubernetesConf: KubernetesDriverConf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = CUSTOM_DRIVER_LABELS,
      environment = DRIVER_ENVS,
      annotations = DRIVER_ANNOTATIONS)

    val featureStep = new BasicDriverFeatureStep(kubernetesConf)
    val basePod = SparkPod.initialPod()
    val configuredPod = featureStep.configurePod(basePod)

    val resourceRequirements = configuredPod.container.getResources
    val requests = resourceRequirements.getRequests.asScala
    // mem = 5000 + max(overhead_factor[0.1] * 5000, 200)
    assert(amountAndFormat(requests("memory")) === "5500Mi")
    val limits = resourceRequirements.getLimits.asScala
    assert(amountAndFormat(limits("memory")) === "5500Mi")
  }


  def containerPort(name: String, portNumber: Int): ContainerPort =
    new ContainerPortBuilder()
      .withName(name)
      .withContainerPort(portNumber)
      .withProtocol("TCP")
      .build()

  private def amountAndFormat(quantity: Quantity): String = quantity.getAmount + quantity.getFormat
}

/**
 * No-op Hadoop FileSystem
 */
private class TestFileSystem extends LocalFileSystem {
  override def copyFromLocalFile(
    delSrc: Boolean,
    overwrite: Boolean,
    src: Path,
    dst: Path): Unit = {}

  override def mkdirs(path: Path): Boolean = true
}
