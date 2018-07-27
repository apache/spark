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

import io.fabric8.kubernetes.api.model.Service
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesConf, KubernetesDriverSpecificConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.util.Clock

class DriverServiceFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {

  private val SHORT_RESOURCE_NAME_PREFIX =
    "a" * (DriverServiceFeatureStep.MAX_SERVICE_NAME_LENGTH -
      DriverServiceFeatureStep.DRIVER_SVC_POSTFIX.length)

  private val LONG_RESOURCE_NAME_PREFIX =
    "a" * (DriverServiceFeatureStep.MAX_SERVICE_NAME_LENGTH -
      DriverServiceFeatureStep.DRIVER_SVC_POSTFIX.length + 1)
  private val DRIVER_LABELS = Map(
    "label1key" -> "label1value",
    "label2key" -> "label2value")

  @Mock
  private var clock: Clock = _

  private var sparkConf: SparkConf = _

  before {
    MockitoAnnotations.initMocks(this)
    sparkConf = new SparkConf(false)
  }

  test("Headless service has a port for the driver RPC and the block manager.") {
    sparkConf = sparkConf
      .set("spark.driver.port", "9000")
      .set(org.apache.spark.internal.config.DRIVER_BLOCK_MANAGER_PORT, 8080)
    val configurationStep = new DriverServiceFeatureStep(
      KubernetesConf(
        sparkConf,
        KubernetesDriverSpecificConf(
          None, "main", "app", Seq.empty),
        SHORT_RESOURCE_NAME_PREFIX,
        "app-id",
        DRIVER_LABELS,
        Map.empty,
        Map.empty,
        Map.empty,
        Map.empty,
        Nil,
        Seq.empty[String]))
    assert(configurationStep.configurePod(SparkPod.initialPod()) === SparkPod.initialPod())
    assert(configurationStep.getAdditionalKubernetesResources().size === 1)
    assert(configurationStep.getAdditionalKubernetesResources().head.isInstanceOf[Service])
    val driverService = configurationStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Service]
    verifyService(
      9000,
      8080,
      s"$SHORT_RESOURCE_NAME_PREFIX${DriverServiceFeatureStep.DRIVER_SVC_POSTFIX}",
      driverService)
  }

  test("Hostname and ports are set according to the service name.") {
    val configurationStep = new DriverServiceFeatureStep(
      KubernetesConf(
        sparkConf
          .set("spark.driver.port", "9000")
          .set(org.apache.spark.internal.config.DRIVER_BLOCK_MANAGER_PORT, 8080)
          .set(KUBERNETES_NAMESPACE, "my-namespace"),
        KubernetesDriverSpecificConf(
          None, "main", "app", Seq.empty),
        SHORT_RESOURCE_NAME_PREFIX,
        "app-id",
        DRIVER_LABELS,
        Map.empty,
        Map.empty,
        Map.empty,
        Map.empty,
        Nil,
        Seq.empty[String]))
    val expectedServiceName = SHORT_RESOURCE_NAME_PREFIX +
      DriverServiceFeatureStep.DRIVER_SVC_POSTFIX
    val expectedHostName = s"$expectedServiceName.my-namespace.svc"
    val additionalProps = configurationStep.getAdditionalPodSystemProperties()
    verifySparkConfHostNames(additionalProps, expectedHostName)
  }

  test("Ports should resolve to defaults in SparkConf and in the service.") {
    val configurationStep = new DriverServiceFeatureStep(
      KubernetesConf(
        sparkConf,
        KubernetesDriverSpecificConf(
          None, "main", "app", Seq.empty),
        SHORT_RESOURCE_NAME_PREFIX,
        "app-id",
        DRIVER_LABELS,
        Map.empty,
        Map.empty,
        Map.empty,
        Map.empty,
        Nil,
        Seq.empty[String]))
    val resolvedService = configurationStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Service]
    verifyService(
      DEFAULT_DRIVER_PORT,
      DEFAULT_BLOCKMANAGER_PORT,
      s"$SHORT_RESOURCE_NAME_PREFIX${DriverServiceFeatureStep.DRIVER_SVC_POSTFIX}",
      resolvedService)
    val additionalProps = configurationStep.getAdditionalPodSystemProperties()
    assert(additionalProps("spark.driver.port") === DEFAULT_DRIVER_PORT.toString)
    assert(additionalProps(org.apache.spark.internal.config.DRIVER_BLOCK_MANAGER_PORT.key)
      === DEFAULT_BLOCKMANAGER_PORT.toString)
  }

  test("Long prefixes should switch to using a generated name.") {
    when(clock.getTimeMillis()).thenReturn(10000)
    val configurationStep = new DriverServiceFeatureStep(
      KubernetesConf(
        sparkConf.set(KUBERNETES_NAMESPACE, "my-namespace"),
        KubernetesDriverSpecificConf(
          None, "main", "app", Seq.empty),
        LONG_RESOURCE_NAME_PREFIX,
        "app-id",
        DRIVER_LABELS,
        Map.empty,
        Map.empty,
        Map.empty,
        Map.empty,
        Nil,
        Seq.empty[String]),
      clock)
    val driverService = configurationStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Service]
    val expectedServiceName = s"spark-10000${DriverServiceFeatureStep.DRIVER_SVC_POSTFIX}"
    assert(driverService.getMetadata.getName === expectedServiceName)
    val expectedHostName = s"$expectedServiceName.my-namespace.svc"
    val additionalProps = configurationStep.getAdditionalPodSystemProperties()
    verifySparkConfHostNames(additionalProps, expectedHostName)
  }

  test("Disallow bind address and driver host to be set explicitly.") {
    try {
      new DriverServiceFeatureStep(
        KubernetesConf(
          sparkConf.set(org.apache.spark.internal.config.DRIVER_BIND_ADDRESS, "host"),
          KubernetesDriverSpecificConf(
            None, "main", "app", Seq.empty),
          LONG_RESOURCE_NAME_PREFIX,
          "app-id",
          DRIVER_LABELS,
          Map.empty,
          Map.empty,
          Map.empty,
          Map.empty,
          Nil,
          Seq.empty[String]),
        clock)
      fail("The driver bind address should not be allowed.")
    } catch {
      case e: Throwable =>
        assert(e.getMessage ===
          s"requirement failed: ${DriverServiceFeatureStep.DRIVER_BIND_ADDRESS_KEY} is" +
          " not supported in Kubernetes mode, as the driver's bind address is managed" +
          " and set to the driver pod's IP address.")
    }
    sparkConf.remove(org.apache.spark.internal.config.DRIVER_BIND_ADDRESS)
    sparkConf.set(org.apache.spark.internal.config.DRIVER_HOST_ADDRESS, "host")
    try {
      new DriverServiceFeatureStep(
        KubernetesConf(
          sparkConf,
          KubernetesDriverSpecificConf(
            None, "main", "app", Seq.empty),
          LONG_RESOURCE_NAME_PREFIX,
          "app-id",
          DRIVER_LABELS,
          Map.empty,
          Map.empty,
          Map.empty,
          Map.empty,
          Nil,
          Seq.empty[String]),
        clock)
      fail("The driver host address should not be allowed.")
    } catch {
      case e: Throwable =>
        assert(e.getMessage ===
          s"requirement failed: ${DriverServiceFeatureStep.DRIVER_HOST_KEY} is" +
          " not supported in Kubernetes mode, as the driver's hostname will be managed via" +
          " a Kubernetes service.")
    }
  }

  private def verifyService(
      driverPort: Int,
      blockManagerPort: Int,
      expectedServiceName: String,
      service: Service): Unit = {
    assert(service.getMetadata.getName === expectedServiceName)
    assert(service.getSpec.getClusterIP === "None")
    assert(service.getSpec.getSelector.asScala === DRIVER_LABELS)
    assert(service.getSpec.getPorts.size() === 2)
    val driverServicePorts = service.getSpec.getPorts.asScala
    assert(driverServicePorts.head.getName === DRIVER_PORT_NAME)
    assert(driverServicePorts.head.getPort.intValue() === driverPort)
    assert(driverServicePorts.head.getTargetPort.getIntVal === driverPort)
    assert(driverServicePorts(1).getName === BLOCK_MANAGER_PORT_NAME)
    assert(driverServicePorts(1).getPort.intValue() === blockManagerPort)
    assert(driverServicePorts(1).getTargetPort.getIntVal === blockManagerPort)
  }

  private def verifySparkConfHostNames(
      driverSparkConf: Map[String, String], expectedHostName: String): Unit = {
    assert(driverSparkConf(
      org.apache.spark.internal.config.DRIVER_HOST_ADDRESS.key) === expectedHostName)
  }
}
