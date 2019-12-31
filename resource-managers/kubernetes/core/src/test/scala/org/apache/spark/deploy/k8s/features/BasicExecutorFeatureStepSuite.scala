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

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import scala.collection.JavaConverters._

import com.google.common.net.InternetDomainName
import io.fabric8.kubernetes.api.model._
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SecurityManager, SparkConf, SparkException, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.features.KubernetesFeaturesTestUtils.TestResourceInformation
import org.apache.spark.internal.config
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.Python._
import org.apache.spark.resource.ResourceID
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
  private val LABELS = Map("label1key" -> "label1value")
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
  }

  private def newExecutorConf(
      environment: Map[String, String] = Map.empty): KubernetesExecutorConf = {
    KubernetesTestConf.createExecutorConf(
      sparkConf = baseConf,
      driverPod = Some(DRIVER_POD),
      labels = LABELS,
      environment = environment)
  }

  test("test spark resource missing vendor") {
    baseConf.set(EXECUTOR_GPU_ID.amountConf, "2")
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf))
    val error = intercept[SparkException] {
      val executor = step.configurePod(SparkPod.initialPod())
    }.getMessage()
    assert(error.contains("Resource: gpu was requested, but vendor was not specified"))
  }

  test("test spark resource missing amount") {
    baseConf.set(EXECUTOR_GPU_ID.vendorConf, "nvidia.com")

    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf))
    val error = intercept[SparkException] {
      val executor = step.configurePod(SparkPod.initialPod())
    }.getMessage()
    assert(error.contains("You must specify an amount for gpu"))
  }

  test("basic executor pod with resources") {
    val fpgaResourceID = ResourceID(SPARK_EXECUTOR_PREFIX, FPGA)
    val gpuExecutorResourceID = ResourceID(SPARK_EXECUTOR_PREFIX, GPU)
    val gpuResources =
      Map(("nvidia.com/gpu" -> TestResourceInformation(gpuExecutorResourceID, "2", "nvidia.com")),
      ("foo.com/fpga" -> TestResourceInformation(fpgaResourceID, "1", "foo.com")))
    gpuResources.foreach { case (_, testRInfo) =>
      baseConf.set(testRInfo.rId.amountConf, testRInfo.count)
      baseConf.set(testRInfo.rId.vendorConf, testRInfo.vendor)
    }
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf))
    val executor = step.configurePod(SparkPod.initialPod())

    assert(executor.container.getResources.getLimits.size() === 3)
    assert(executor.container.getResources
      .getLimits.get("memory").getAmount === "1408Mi")
    gpuResources.foreach { case (k8sName, testRInfo) =>
      assert(executor.container.getResources.getLimits.get(k8sName).getAmount === testRInfo.count)
    }
  }

  test("basic executor pod has reasonable defaults") {
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf))
    val executor = step.configurePod(SparkPod.initialPod())

    // The executor pod name and default labels.
    assert(executor.pod.getMetadata.getName === s"$RESOURCE_NAME_PREFIX-exec-1")
    LABELS.foreach { case (k, v) =>
      assert(executor.pod.getMetadata.getLabels.get(k) === v)
    }
    assert(executor.pod.getSpec.getImagePullSecrets.asScala === TEST_IMAGE_PULL_SECRET_OBJECTS)

    // There is exactly 1 container with no volume mounts and default memory limits.
    // Default memory limit is 1024M + 384M (minimum overhead constant).
    assert(executor.container.getImage === EXECUTOR_IMAGE)
    assert(executor.container.getVolumeMounts.isEmpty)
    assert(executor.container.getResources.getLimits.size() === 1)
    assert(executor.container.getResources
      .getLimits.get("memory").getAmount === "1408Mi")

    // The pod has no node selector, volumes.
    assert(executor.pod.getSpec.getNodeSelector.isEmpty)
    assert(executor.pod.getSpec.getVolumes.isEmpty)

    checkEnv(executor, baseConf, Map())
    checkOwnerReferences(executor.pod, DRIVER_POD_UID)
  }

  test("executor pod hostnames get truncated to 63 characters") {
    val longPodNamePrefix = "loremipsumdolorsitametvimatelitrefficiendisuscipianturvixlegeresple"

    baseConf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, longPodNamePrefix)
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf))
    assert(step.configurePod(SparkPod.initialPod()).pod.getSpec.getHostname.length === 63)
  }

  test("hostname truncation generates valid host names") {
    val invalidPrefix = "abcdef-*_/[]{}+==.,;'\"-----------------------------------------------"

    baseConf.set(KUBERNETES_EXECUTOR_POD_NAME_PREFIX, invalidPrefix)
    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf))
    val hostname = step.configurePod(SparkPod.initialPod()).pod.getSpec().getHostname()
    assert(hostname.length <= 63)
    assert(InternetDomainName.isValid(hostname))
  }

  test("classpath and extra java options get translated into environment variables") {
    baseConf.set(config.EXECUTOR_JAVA_OPTIONS, "foo=bar")
    baseConf.set(config.EXECUTOR_CLASS_PATH, "bar=baz")
    val kconf = newExecutorConf(environment = Map("qux" -> "quux"))
    val step = new BasicExecutorFeatureStep(kconf, new SecurityManager(baseConf))
    val executor = step.configurePod(SparkPod.initialPod())

    checkEnv(executor, baseConf,
      Map("SPARK_JAVA_OPT_0" -> "foo=bar",
        ENV_CLASSPATH -> "bar=baz",
        "qux" -> "quux"))
    checkOwnerReferences(executor.pod, DRIVER_POD_UID)
  }

  test("test executor pyspark memory") {
    baseConf.set("spark.kubernetes.resource.type", "python")
    baseConf.set(PYSPARK_EXECUTOR_MEMORY, 42L)

    val step = new BasicExecutorFeatureStep(newExecutorConf(), new SecurityManager(baseConf))
    val executor = step.configurePod(SparkPod.initialPod())
    // This is checking that basic executor + executorMemory = 1408 + 42 = 1450
    assert(executor.container.getResources.getRequests.get("memory").getAmount === "1450Mi")
  }

  test("auth secret propagation") {
    val conf = baseConf.clone()
      .set(config.NETWORK_AUTH_ENABLED, true)
      .set("spark.master", "k8s://127.0.0.1")

    val secMgr = new SecurityManager(conf)
    secMgr.initializeAuth()

    val step = new BasicExecutorFeatureStep(KubernetesTestConf.createExecutorConf(sparkConf = conf),
      secMgr)

    val executor = step.configurePod(SparkPod.initialPod())
    checkEnv(executor, conf, Map(SecurityManager.ENV_AUTH_SECRET -> secMgr.getSecretKey()))
  }

  test("Auth secret shouldn't propagate if files are loaded.") {
    val secretDir = Utils.createTempDir("temp-secret")
    val secretFile = new File(secretDir, "secret-file.txt")
    Files.write(secretFile.toPath, "some-secret".getBytes(StandardCharsets.UTF_8))
    val conf = baseConf.clone()
      .set(config.NETWORK_AUTH_ENABLED, true)
      .set(config.AUTH_SECRET_FILE, secretFile.getAbsolutePath)
      .set("spark.master", "k8s://127.0.0.1")
    val secMgr = new SecurityManager(conf)
    secMgr.initializeAuth()

    val step = new BasicExecutorFeatureStep(KubernetesTestConf.createExecutorConf(sparkConf = conf),
      secMgr)

    val executor = step.configurePod(SparkPod.initialPod())
    assert(!KubernetesFeaturesTestUtils.containerHasEnvVar(
      executor.container, SecurityManager.ENV_AUTH_SECRET))
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
      ENV_EXECUTOR_MEMORY -> "1g",
      ENV_APPLICATION_ID -> KubernetesTestConf.APP_ID,
      ENV_SPARK_CONF_DIR -> SPARK_CONF_DIR_INTERNAL,
      ENV_EXECUTOR_POD_IP -> null,
      ENV_SPARK_USER -> Utils.getCurrentUserName())

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
}
