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

import io.fabric8.kubernetes.api.model.{ContainerPort, ContainerPortBuilder, LocalObjectReferenceBuilder}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
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

  private val DRIVER_LABELS = Map("labelkey" -> "labelvalue")
  private val CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent"
  private val DRIVER_ANNOTATIONS = Map("customAnnotation" -> "customAnnotationValue")
  private val DRIVER_ENVS = Map(
    "customDriverEnv1" -> "customDriverEnv2",
    "customDriverEnv2" -> "customDriverEnv2")
  private val TEST_IMAGE_PULL_SECRETS = Seq("my-secret-1", "my-secret-2")
  private val TEST_IMAGE_PULL_SECRET_OBJECTS =
    TEST_IMAGE_PULL_SECRETS.map { secret =>
      new LocalObjectReferenceBuilder().withName(secret).build()
    }

  test("Check the pod respects all configurations from the user.") {
    val resourceID = ResourceID(SPARK_DRIVER_PREFIX, GPU)
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
    val kubernetesConf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = DRIVER_LABELS,
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
      assert(envs(v) === v)
    }
    assert(envs(ENV_SPARK_USER) === Utils.getCurrentUserName())

    assert(configuredPod.pod.getSpec().getImagePullSecrets.asScala ===
      TEST_IMAGE_PULL_SECRET_OBJECTS)

    assert(configuredPod.container.getEnv.asScala.exists(envVar =>
      envVar.getName.equals(ENV_DRIVER_BIND_ADDRESS) &&
        envVar.getValueFrom.getFieldRef.getApiVersion.equals("v1") &&
        envVar.getValueFrom.getFieldRef.getFieldPath.equals("status.podIP")))

    val resourceRequirements = configuredPod.container.getResources
    val requests = resourceRequirements.getRequests.asScala
    assert(requests("cpu").getAmount === "2")
    assert(requests("memory").getAmount === "456Mi")
    val limits = resourceRequirements.getLimits.asScala
    assert(limits("memory").getAmount === "456Mi")
    assert(limits("cpu").getAmount === "4")
    resources.foreach { case (k8sName, testRInfo) =>
      assert(limits(k8sName).getAmount === testRInfo.count)
    }

    val driverPodMetadata = configuredPod.pod.getMetadata
    assert(driverPodMetadata.getName === "spark-driver-pod")
    DRIVER_LABELS.foreach { case (k, v) =>
      assert(driverPodMetadata.getLabels.get(k) === v)
    }
    assert(driverPodMetadata.getAnnotations.asScala === DRIVER_ANNOTATIONS)
    assert(configuredPod.pod.getSpec.getRestartPolicy === "Never")
    val expectedSparkConf = Map(
      KUBERNETES_DRIVER_POD_NAME.key -> "spark-driver-pod",
      "spark.app.id" -> KubernetesTestConf.APP_ID,
      KUBERNETES_EXECUTOR_POD_NAME_PREFIX.key -> kubernetesConf.resourceNamePrefix,
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
    assert(requests1("cpu").getAmount === "1")

    // if spark.driver.cores is set it should be used
    sparkConf.set(DRIVER_CORES, 10)
    val requests2 = new BasicDriverFeatureStep(KubernetesTestConf.createDriverConf(sparkConf))
      .configurePod(basePod)
      .container.getResources
      .getRequests.asScala
    assert(requests2("cpu").getAmount === "10")

    // spark.kubernetes.driver.request.cores should be preferred over spark.driver.cores
    Seq("0.1", "100m").foreach { value =>
      sparkConf.set(KUBERNETES_DRIVER_REQUEST_CORES, value)
      val requests3 = new BasicDriverFeatureStep(KubernetesTestConf.createDriverConf(sparkConf))
        .configurePod(basePod)
        .container.getResources
        .getRequests.asScala
      assert(requests3("cpu").getAmount === value)
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
  Seq(
    ("java", JavaMainAppResource(None), None, MEMORY_OVERHEAD_FACTOR.defaultValue.get),
    ("python default", PythonMainAppResource(null), None, NON_JVM_MEMORY_OVERHEAD_FACTOR),
    ("python w/ override", PythonMainAppResource(null), Some(0.9d), 0.9d),
    ("r default", RMainAppResource(null), None, NON_JVM_MEMORY_OVERHEAD_FACTOR)
  ).foreach { case (name, resource, factor, expectedFactor) =>
    test(s"memory overhead factor: $name") {
      // Choose a driver memory where the default memory overhead is > MEMORY_OVERHEAD_MIN_MIB
      val driverMem = MEMORY_OVERHEAD_MIN_MIB / MEMORY_OVERHEAD_FACTOR.defaultValue.get * 2

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
      val mem = pod.container.getResources.getRequests.get("memory").getAmount()
      val expected = (driverMem + driverMem * expectedFactor).toInt
      assert(mem === s"${expected}Mi")

      val systemProperties = step.getAdditionalPodSystemProperties()
      assert(systemProperties(MEMORY_OVERHEAD_FACTOR.key) === expectedFactor.toString)
    }
  }

  def containerPort(name: String, portNumber: Int): ContainerPort =
    new ContainerPortBuilder()
      .withName(name)
      .withContainerPort(portNumber)
      .withProtocol("TCP")
      .build()
}
