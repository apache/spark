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

import scala.collection.JavaConverters._

import com.google.common.net.InternetDomainName
import io.fabric8.kubernetes.api.model._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SecurityManager, SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesTestConf, SecretVolumeUtils, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.KubernetesFeaturesTestUtils.TestResourceInformation
import org.apache.spark.internal.config
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Python._
import org.apache.spark.resource._
import org.apache.spark.resource.ResourceUtils._
import org.apache.spark.resource.TestResourceIDs._
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

class BasicExecutorFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {

  private val DRIVER_HOSTNAME = "localhost"
  private val DRIVER_PORT = 7098
  private val DRIVER_ADDRESS = RpcEndpointAddress(
    DRIVER_HOSTNAME,
    DRIVER_PORT.toInt,
    CoarseGrainedSchedulerBackend.ENDPOINT_NAME)
  private val DRIVER_POD_NAME = "driver-pod"

  private val DRIVER_POD_UID = "driver-uid"
  private val RESOURCE_NAME_PREFIX = "base"
  private val EXECUTOR_IMAGE = "executor-image"
  private val CUSTOM_EXECUTOR_LABELS = Map("label1key" -> "label1value")
  private var defaultProfile: ResourceProfile = _
  private val TEST_IMAGE_PULL_SECRETS = Seq("my-1secret-1", "my-secret-2")
  private val TEST_IMAGE_PULL_SECRET_OBJECTS =
    TEST_IMAGE_PULL_SECRETS.map { secret =>
      new LocalObjectReferenceBuilder().withName(secret).build()
    }
  private val DRIVER_POD = new PodBuilder()
    .withNewMetadata()
      .withName(DRIVER_POD_NAME)
      .withUid(DRIVER_POD_UID)
      .endMetadata()
    .withNewSpec()
      .withNodeName("some-node")
      .endSpec()
    .withNewStatus()
      .withHostIP("192.168.99.100")
      .endStatus()
    .build()
  private var baseConf: SparkConf = _

  before {
    baseConf = new SparkConf(false)
      .set(KUBERNETES_DRIVER_POD_NAME, DRIVER_POD_NAME)
      .set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, RESOURCE_NAME_PREFIX)
      .set(CONTAINER_IMAGE, EXECUTOR_IMAGE)
      .set(KUBERNETES_DRIVER_SUBMIT_CHECK, true)
      .set(config.DRIVER_HOST_ADDRESS, DRIVER_HOSTNAME)
      .set(config.DRIVER_PORT, DRIVER_PORT)
      .set(IMAGE_PULL_SECRETS, TEST_IMAGE_PULL_SECRETS)
      .set("spark.kubernetes.resource.type", "java")
    initDefaultProfile(baseConf)
  }

  private def newExecutorConf(
      environment: Map[String, String] = Map.empty): KubernetesExecutorConf = {
    KubernetesTestConf.createExecutorConf(
      sparkConf = baseConf,
      driverPod = Some(DRIVER_POD),
      labels = CUSTOM_EXECUTOR_LABELS,
      environment = environment)
  }

  private def initDefaultProfile(baseConf: SparkConf): Unit = {
    ResourceProfile.clearDefaultProfile()
    defaultProfile = ResourceProfile.getOrCreateDefaultProfile(baseConf)
  }

  test("test spark resource missing vendor") {
    baseConf.set(EXECUTOR_GPU_ID.amountConf, "2")
    val error = intercept[SparkException] {
      initDefaultProfile(baseConf)
      val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
        defaultProfile)
      val executor = step.configurePod(SparkPod.initialPod())
    }.getMessage()
    assert(error.contains("Resource: gpu was requested, but vendor was not specified"))
  }

  test("test spark resource missing amount") {
    baseConf.set(EXECUTOR_GPU_ID.vendorConf, "nvidia.com")
    val error = intercept[SparkException] {
      initDefaultProfile(baseConf)
      val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)
      val executor = step.configurePod(SparkPod.initialPod())
    }.getMessage()
    assert(error.contains("You must specify an amount for gpu"))
  }

  test("basic executor pod with resources") {
    val fpgaResourceID = new ResourceID(SPARK_EXECUTOR_PREFIX, FPGA)
    val gpuExecutorResourceID = new ResourceID(SPARK_EXECUTOR_PREFIX, GPU)
    val gpuResources =
      Map(("nvidia.com/gpu" -> TestResourceInformation(gpuExecutorResourceID, "2", "nvidia.com")),
      ("foo.com/fpga" -> TestResourceInformation(fpgaResourceID, "1", "foo.com")))
    gpuResources.foreach { case (_, testRInfo) =>
      baseConf.set(testRInfo.rId.amountConf, testRInfo.count)
      baseConf.set(testRInfo.rId.vendorConf, testRInfo.vendor)
    }
    initDefaultProfile(baseConf)
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)
    val executor = step.configurePod(SparkPod.initialPod())

    assert(executor.container.getResources.getLimits.size() === 3)
    assert(amountAndFormat(executor.container.getResources
      .getLimits.get("memory")) === "1408Mi")
    gpuResources.foreach { case (k8sName, testRInfo) =>
      assert(amountAndFormat(
        executor.container.getResources.getLimits.get(k8sName)) === testRInfo.count)
    }
  }

  test("basic executor pod has reasonable defaults") {
    val conf = newExecutorConf()
    val step = new BasicExecutorFeatureStep(conf, new SecurityManager(baseConf),
      defaultProfile)
    val executor = step.configurePod(SparkPod.initialPod())

    // The executor pod name and default labels.
    assert(executor.pod.getMetadata.getName === s"$RESOURCE_NAME_PREFIX-exec-1")

    // Check custom and preset labels are as expected
    CUSTOM_EXECUTOR_LABELS.foreach { case (k, v) =>
      assert(executor.pod.getMetadata.getLabels.get(k) === v)
    }
    assert(executor.pod.getMetadata.getLabels === conf.labels.asJava)

    assert(executor.pod.getSpec.getImagePullSecrets.asScala === TEST_IMAGE_PULL_SECRET_OBJECTS)

    // There is exactly 1 container with 1 volume mount and default memory limits.
    // Default memory limit is 1024M + 384M (minimum overhead constant).
    assert(executor.container.getImage === EXECUTOR_IMAGE)
    assert(executor.container.getVolumeMounts.size() == 1)
    assert(executor.container.getResources.getLimits.size() === 1)
    assert(amountAndFormat(executor.container.getResources
      .getLimits.get("memory")) === "1408Mi")

    // The pod has no node selector, and 1 volume.
    assert(executor.pod.getSpec.getNodeSelector.isEmpty)
    assert(executor.pod.getSpec.getVolumes.size() == 1)

    checkEnv(executor, baseConf, Map())
    checkOwnerReferences(executor.pod, DRIVER_POD_UID)
  }

  def withPodNamePrefix(f: => Unit): Unit = {
    val namePrefixOld = baseConf.get(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)
    try {
      f
    } finally {
      namePrefixOld.foreach(baseConf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, _))
    }
  }

  test("executor pod hostnames get truncated to 63 characters") {
    withPodNamePrefix {
      val longPodNamePrefix = "loremipsumdolorsitametvimatelitrefficiendisuscipianturvixlegeresple"
      baseConf.remove(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)
      baseConf.set("spark.app.name", longPodNamePrefix)
      initDefaultProfile(baseConf)
      val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
        defaultProfile)
      assert(step.configurePod(SparkPod.initialPod()).pod.getSpec.getHostname.length ===
        KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH)
    }
  }

  test("SPARK-35460: invalid PodNamePrefixes") {
    withPodNamePrefix {
      Seq("_123", "spark_exec", "spark@", "a" * 238).foreach { invalid =>
        baseConf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, invalid)
        val e = intercept[IllegalArgumentException](newExecutorConf())
        assert(e.getMessage === s"'$invalid' in spark.kubernetes.executor.podNamePrefix is" +
          s" invalid. must conform https://kubernetes.io/docs/concepts/overview/" +
          "working-with-objects/names/#dns-subdomain-names and the value length <= 237")
      }
    }
  }

  test("hostname truncation generates valid host names") {
    withPodNamePrefix {
      val invalidPrefix = "abcdef-*_/[]{}+==.,;'\"-----------------------------------------------"
      baseConf.remove(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)
      baseConf.set("spark.app.name", invalidPrefix)
      initDefaultProfile(baseConf)
      val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
        defaultProfile)
      val hostname = step.configurePod(SparkPod.initialPod()).pod.getSpec().getHostname()
      assert(hostname.length <= KUBERNETES_DNS_LABEL_NAME_MAX_LENGTH)
      assert(InternetDomainName.isValid(hostname))
    }
  }

  test("classpath and extra java options get translated into environment variables") {
    baseConf.set(config.EXECUTOR_JAVA_OPTIONS, "foo=bar")
    baseConf.set(config.EXECUTOR_CLASS_PATH, "bar=baz")
    initDefaultProfile(baseConf)
    val kconf = newExecutorConf(environment = Map("qux" -> "quux"))
    val step = new BasicExecutorFeatureStep(kconf, new SecurityManager(baseConf),
      defaultProfile)
    val executor = step.configurePod(SparkPod.initialPod())

    checkEnv(executor, baseConf,
      Map("SPARK_JAVA_OPT_0" -> "foo=bar",
        ENV_CLASSPATH -> "bar=baz",
        "qux" -> "quux"))
    checkOwnerReferences(executor.pod, DRIVER_POD_UID)
  }

  test("SPARK-32655 Support appId/execId placeholder in SPARK_EXECUTOR_DIRS") {
    val kconf = newExecutorConf(environment = Map(ENV_EXECUTOR_DIRS ->
      "/p1/SPARK_APPLICATION_ID/SPARK_EXECUTOR_ID,/p2/SPARK_APPLICATION_ID/SPARK_EXECUTOR_ID"))
    val step = new BasicExecutorFeatureStep(kconf, new SecurityManager(baseConf),
      defaultProfile)
    val executor = step.configurePod(SparkPod.initialPod())

    checkEnv(executor, baseConf, Map(ENV_EXECUTOR_DIRS ->
      s"/p1/${KubernetesTestConf.APP_ID}/1,/p2/${KubernetesTestConf.APP_ID}/1"))
  }

  test("test executor pyspark memory") {
    baseConf.set("spark.kubernetes.resource.type", "python")
    baseConf.set(PYSPARK_EXECUTOR_MEMORY, 42L)
    initDefaultProfile(baseConf)
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)
    val executor = step.configurePod(SparkPod.initialPod())
    // This is checking that basic executor + executorMemory = 1408 + 42 = 1450
    assert(amountAndFormat(executor.container.getResources.getRequests.get("memory")) === "1450Mi")
  }

  test("auth secret propagation") {
    val conf = baseConf.clone()
      .set(config.NETWORK_AUTH_ENABLED, true)
      .set("spark.master", "k8s://127.0.0.1")

    val secMgr = new SecurityManager(conf)
    secMgr.initializeAuth()

    val step = new BasicExecutorFeatureStep(KubernetesTestConf.createExecutorConf(sparkConf = conf),
      secMgr, defaultProfile)

    val executor = step.configurePod(SparkPod.initialPod())
    checkEnv(executor, conf, Map(SecurityManager.ENV_AUTH_SECRET -> secMgr.getSecretKey()))
  }

  test("Auth secret shouldn't propagate if files are loaded.") {
    withSecretFile("some-secret") { secretFile =>
      val conf = baseConf.clone()
        .set(config.NETWORK_AUTH_ENABLED, true)
        .set(config.AUTH_SECRET_FILE, secretFile.getAbsolutePath)
        .set("spark.master", "k8s://127.0.0.1")
      val secMgr = new SecurityManager(conf)
      secMgr.initializeAuth()
      val step = new BasicExecutorFeatureStep(
        KubernetesTestConf.createExecutorConf(sparkConf = conf), secMgr, defaultProfile)

      val executor = step.configurePod(SparkPod.initialPod())
      assert(!KubernetesFeaturesTestUtils.containerHasEnvVar(
        executor.container, SecurityManager.ENV_AUTH_SECRET))
    }
  }

  test("SPARK-32661 test executor offheap memory") {
    baseConf.set(MEMORY_OFFHEAP_ENABLED, true)
    baseConf.set("spark.memory.offHeap.size", "42m")
    initDefaultProfile(baseConf)

    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)
    val executor = step.configurePod(SparkPod.initialPod())
    // This is checking that basic executor + executorMemory = 1408 + 42 = 1450
    assert(amountAndFormat(executor.container.getResources.getRequests.get("memory")) === "1450Mi")
  }

  test("basic resourceprofile") {
    baseConf.set("spark.kubernetes.resource.type", "python")
    initDefaultProfile(baseConf)
    val rpb = new ResourceProfileBuilder()
    val ereq = new ExecutorResourceRequests()
    val treq = new TaskResourceRequests()
    ereq.cores(4).memory("2g").memoryOverhead("1g").pysparkMemory("3g")
    treq.cpus(2)
    rpb.require(ereq).require(treq)
    val rp = rpb.build()
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf), rp)
    val executor = step.configurePod(SparkPod.initialPod())

    assert(amountAndFormat(executor.container.getResources
      .getRequests.get("cpu")) === "4")
    assert(amountAndFormat(executor.container.getResources
      .getLimits.get("memory")) === "6144Mi")
  }

  test("resourceprofile with gpus") {
    val rpb = new ResourceProfileBuilder()
    val ereq = new ExecutorResourceRequests()
    val treq = new TaskResourceRequests()
    ereq.cores(2).resource("gpu", 2, "/path/getGpusResources.sh", "nvidia.com")
    treq.cpus(1)
    rpb.require(ereq).require(treq)
    val rp = rpb.build()
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf), rp)
    val executor = step.configurePod(SparkPod.initialPod())

    assert(amountAndFormat(executor.container.getResources
      .getLimits.get("memory")) === "1408Mi")
    assert(amountAndFormat(executor.container.getResources
      .getRequests.get("cpu")) === "2")

    assert(executor.container.getResources.getLimits.size() === 2)
    assert(amountAndFormat(executor.container.getResources.getLimits.get("nvidia.com/gpu")) === "2")
  }

  test("Verify spark conf dir is mounted as configmap volume on executor pod's container.") {
    val baseDriverPod = SparkPod.initialPod()
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)
    val podConfigured = step.configurePod(baseDriverPod)
    assert(SecretVolumeUtils.containerHasVolume(podConfigured.container,
      SPARK_CONF_VOLUME_EXEC, SPARK_CONF_DIR_INTERNAL))
    assert(SecretVolumeUtils.podHasVolume(podConfigured.pod, SPARK_CONF_VOLUME_EXEC))
  }

  test("SPARK-34316 Disable configmap volume on executor pod's container") {
    baseConf.set(KUBERNETES_EXECUTOR_DISABLE_CONFIGMAP, true)
    val baseDriverPod = SparkPod.initialPod()
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)
    val podConfigured = step.configurePod(baseDriverPod)
    assert(!SecretVolumeUtils.containerHasVolume(podConfigured.container,
      SPARK_CONF_VOLUME_EXEC, SPARK_CONF_DIR_INTERNAL))
    assert(!SecretVolumeUtils.podHasVolume(podConfigured.pod, SPARK_CONF_VOLUME_EXEC))
  }

  test("SPARK-40065 Mount configmap on executors with non-default profile as well") {
    val baseDriverPod = SparkPod.initialPod()
    val rp = new ResourceProfileBuilder().build()
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf), rp)
    val podConfigured = step.configurePod(baseDriverPod)
    assert(SecretVolumeUtils.containerHasVolume(podConfigured.container,
      SPARK_CONF_VOLUME_EXEC, SPARK_CONF_DIR_INTERNAL))
    assert(SecretVolumeUtils.podHasVolume(podConfigured.pod, SPARK_CONF_VOLUME_EXEC))
  }

  test("SPARK-40065 Disable configmap volume on executor pod's container (non-default profile)") {
    baseConf.set(KUBERNETES_EXECUTOR_DISABLE_CONFIGMAP, true)
    val baseDriverPod = SparkPod.initialPod()
    val rp = new ResourceProfileBuilder().build()
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf), rp)
    val podConfigured = step.configurePod(baseDriverPod)
    assert(!SecretVolumeUtils.containerHasVolume(podConfigured.container,
      SPARK_CONF_VOLUME_EXEC, SPARK_CONF_DIR_INTERNAL))
    assert(!SecretVolumeUtils.podHasVolume(podConfigured.pod, SPARK_CONF_VOLUME_EXEC))
  }

  test("SPARK-35482: user correct block manager port for executor pods") {
    try {
      val initPod = SparkPod.initialPod()
      val sm = new SecurityManager(baseConf)
      val step1 =
        new BasicExecutorFeatureStep(newExecutorConf(), sm, defaultProfile)
      val containerPort1 = step1.configurePod(initPod).container.getPorts.get(0)
      assert(containerPort1.getContainerPort === DEFAULT_BLOCKMANAGER_PORT,
        s"should use port no. $DEFAULT_BLOCKMANAGER_PORT as default")

      baseConf.set(BLOCK_MANAGER_PORT, 12345)
      val step2 = new BasicExecutorFeatureStep(newExecutorConf(), sm, defaultProfile)
      val containerPort2 = step2.configurePod(initPod).container.getPorts.get(0)
      assert(containerPort2.getContainerPort === 12345)

      baseConf.set(BLOCK_MANAGER_PORT, 1000)
      val e = intercept[IllegalArgumentException] {
        new BasicExecutorFeatureStep(newExecutorConf(), sm, defaultProfile)
      }
      assert(e.getMessage.contains("port number must be 0 or in [1024, 65535]"))

      baseConf.set(BLOCK_MANAGER_PORT, 0)
      val step3 = new BasicExecutorFeatureStep(newExecutorConf(), sm, defaultProfile)
      assert(step3.configurePod(initPod).container.getPorts.isEmpty, "random port")
    } finally {
      baseConf.remove(BLOCK_MANAGER_PORT)
    }
  }

  test("SPARK-35969: Make the pod prefix more readable and tallied with K8S DNS Label Names") {
    baseConf.remove(KUBERNETES_EXECUTOR_POD_NAME_PREFIX)
    baseConf.set("spark.app.name", "xyz.abc _i_am_a_app_name_w/_some_abbrs")
    val baseDriverPod = SparkPod.initialPod()
    val step1 = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)
    val podConfigured1 = step1.configurePod(baseDriverPod)
    assert(podConfigured1.pod.getMetadata.getName
      .startsWith("xyz-abc-i-am-a-app-name-w-some-abbrs"))

    // scalastyle:off nonascii
    baseConf.set("spark.app.name", "time.is the#most￥valuable_—thing。it's&about?time.")
    // scalastyle:on

    val step2 = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)

    val podConfigured2 = step2.configurePod(baseDriverPod)
    assert(podConfigured2.pod.getMetadata.getName
      .startsWith("time-is-the-most-valuable-thing-it-s-about-time-"))

  }

  test("SPARK-36075: Check executor pod respects nodeSelector/executorNodeSelector") {
    val initPod = SparkPod.initialPod()
    val sparkConf = new SparkConf()
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(s"${KUBERNETES_NODE_SELECTOR_PREFIX}nodeLabelKey", "nodeLabelValue")
      .set(s"${KUBERNETES_EXECUTOR_NODE_SELECTOR_PREFIX}execNodeLabelKey", "execNodeLabelValue")
      .set(s"${KUBERNETES_DRIVER_NODE_SELECTOR_PREFIX}driverNodeLabelKey", "driverNodeLabelValue")

    val executorConf = KubernetesTestConf.createExecutorConf(sparkConf)
    val executor = new BasicExecutorFeatureStep(executorConf, new SecurityManager(baseConf),
      defaultProfile).configurePod(initPod)
    assert(executor.pod.getSpec.getNodeSelector.asScala === Map(
      "nodeLabelKey" -> "nodeLabelValue",
      "execNodeLabelKey" -> "execNodeLabelValue"
    ))
  }

  test(s"SPARK-38194: memory overhead factor precendence") {
    // Choose an executor memory where the default memory overhead is > MEMORY_OVERHEAD_MIN_MIB
    val defaultFactor = EXECUTOR_MEMORY_OVERHEAD_FACTOR.defaultValue.get
    val executorMem = ResourceProfile.MEMORY_OVERHEAD_MIN_MIB / defaultFactor * 2

    // main app resource, overhead factor
    val sparkConf = new SparkConf(false)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(EXECUTOR_MEMORY.key, s"${executorMem.toInt}m")

    // New config should take precedence
    val expectedFactor = 0.2
    sparkConf.set(EXECUTOR_MEMORY_OVERHEAD_FACTOR, expectedFactor)
    sparkConf.set(MEMORY_OVERHEAD_FACTOR, 0.3)

    val conf = KubernetesTestConf.createExecutorConf(
      sparkConf = sparkConf)
    ResourceProfile.clearDefaultProfile()
    val resourceProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    val step = new BasicExecutorFeatureStep(conf, new SecurityManager(baseConf),
      resourceProfile)
    val pod = step.configurePod(SparkPod.initialPod())
    val mem = amountAndFormat(pod.container.getResources.getRequests.get("memory"))
    val expected = (executorMem + executorMem * expectedFactor).toInt
    assert(mem === s"${expected}Mi")
  }

  test(s"SPARK-38194: old memory factor settings is applied if new one isn't given") {
    // Choose an executor memory where the default memory overhead is > MEMORY_OVERHEAD_MIN_MIB
    val defaultFactor = EXECUTOR_MEMORY_OVERHEAD_FACTOR.defaultValue.get
    val executorMem = ResourceProfile.MEMORY_OVERHEAD_MIN_MIB / defaultFactor * 2

    // main app resource, overhead factor
    val sparkConf = new SparkConf(false)
      .set(CONTAINER_IMAGE, "spark-driver:latest")
      .set(EXECUTOR_MEMORY.key, s"${executorMem.toInt}m")

    // New config should take precedence
    val expectedFactor = 0.3
    sparkConf.set(MEMORY_OVERHEAD_FACTOR, expectedFactor)

    val conf = KubernetesTestConf.createExecutorConf(
      sparkConf = sparkConf)
    ResourceProfile.clearDefaultProfile()
    val resourceProfile = ResourceProfile.getOrCreateDefaultProfile(sparkConf)
    val step = new BasicExecutorFeatureStep(conf, new SecurityManager(baseConf),
      resourceProfile)
    val pod = step.configurePod(SparkPod.initialPod())
    val mem = amountAndFormat(pod.container.getResources.getRequests.get("memory"))
    val expected = (executorMem + executorMem * expectedFactor).toInt
    assert(mem === s"${expected}Mi")
  }

  test("SPARK-39546: Support ports definition in executor pod template") {
    val baseDriverPod = SparkPod.initialPod()
    val ports = new ContainerPortBuilder()
      .withName("port-from-template")
      .withContainerPort(1000)
      .build()
    baseDriverPod.container.setPorts(Seq(ports).asJava)
    val step1 = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf),
      defaultProfile)
    val podConfigured1 = step1.configurePod(baseDriverPod)
    // port-from-template should exist after step1
    assert(podConfigured1.container.getPorts.contains(ports))
  }

  // There is always exactly one controller reference, and it points to the driver pod.
  private def checkOwnerReferences(executor: Pod, driverPodUid: String): Unit = {
    assert(executor.getMetadata.getOwnerReferences.size() === 1)
    assert(executor.getMetadata.getOwnerReferences.get(0).getUid === driverPodUid)
    assert(executor.getMetadata.getOwnerReferences.get(0).getController)
  }

  // Check that the expected environment variables are present.
  private def checkEnv(
      executorPod: SparkPod,
      conf: SparkConf,
      additionalEnvVars: Map[String, String]): Unit = {
    val defaultEnvs = Map(
      ENV_EXECUTOR_ID -> "1",
      ENV_DRIVER_URL -> DRIVER_ADDRESS.toString,
      ENV_EXECUTOR_CORES -> "1",
      ENV_EXECUTOR_MEMORY -> "1024m",
      ENV_APPLICATION_ID -> KubernetesTestConf.APP_ID,
      ENV_SPARK_CONF_DIR -> SPARK_CONF_DIR_INTERNAL,
      ENV_SPARK_USER -> Utils.getCurrentUserName(),
      ENV_RESOURCE_PROFILE_ID -> "0",
      // These are populated by K8s on scheduling
      ENV_EXECUTOR_POD_IP -> null,
      ENV_EXECUTOR_POD_NAME -> null)

    val extraJavaOptsStart = additionalEnvVars.keys.count(_.startsWith(ENV_JAVA_OPT_PREFIX))
    val extraJavaOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)
    val extraJavaOptsEnvs = extraJavaOpts.zipWithIndex.map { case (opt, ind) =>
      s"$ENV_JAVA_OPT_PREFIX${ind + extraJavaOptsStart}" -> opt
    }.toMap

    val containerEnvs = executorPod.container.getEnv.asScala.map {
      x => (x.getName, x.getValue)
    }.toMap

    val expectedEnvs = defaultEnvs ++ additionalEnvVars ++ extraJavaOptsEnvs
    assert(containerEnvs === expectedEnvs)
  }

  private def amountAndFormat(quantity: Quantity): String = quantity.getAmount + quantity.getFormat
}
