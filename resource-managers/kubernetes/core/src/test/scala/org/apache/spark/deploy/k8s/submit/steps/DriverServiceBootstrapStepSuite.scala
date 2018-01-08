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
package org.apache.spark.deploy.k8s.submit.steps

import scala.collection.JavaConverters._

import io.fabric8.kubernetes.api.model.Service
import org.mockito.{Mock, MockitoAnnotations}
import org.mockito.Mockito.when
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec
import org.apache.spark.util.Clock

class DriverServiceBootstrapStepSuite extends SparkFunSuite with BeforeAndAfter {

  private val SHORT_RESOURCE_NAME_PREFIX =
    "a" * (DriverServiceBootstrapStep.MAX_SERVICE_NAME_LENGTH -
      DriverServiceBootstrapStep.DRIVER_SVC_POSTFIX.length)

  private val LONG_RESOURCE_NAME_PREFIX =
    "a" * (DriverServiceBootstrapStep.MAX_SERVICE_NAME_LENGTH -
      DriverServiceBootstrapStep.DRIVER_SVC_POSTFIX.length + 1)
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
    val configurationStep = new DriverServiceBootstrapStep(
      SHORT_RESOURCE_NAME_PREFIX,
      DRIVER_LABELS,
      sparkConf
        .set("spark.driver.port", "9000")
        .set(org.apache.spark.internal.config.DRIVER_BLOCK_MANAGER_PORT, 8080),
      clock)
    val baseDriverSpec = KubernetesDriverSpec.initialSpec(sparkConf.clone())
    val resolvedDriverSpec = configurationStep.configureDriver(baseDriverSpec)
    assert(resolvedDriverSpec.otherKubernetesResources.size === 1)
    assert(resolvedDriverSpec.otherKubernetesResources.head.isInstanceOf[Service])
    val driverService = resolvedDriverSpec.otherKubernetesResources.head.asInstanceOf[Service]
    verifyService(
      9000,
      8080,
      s"$SHORT_RESOURCE_NAME_PREFIX${DriverServiceBootstrapStep.DRIVER_SVC_POSTFIX}",
      driverService)
  }

  test("Hostname and ports are set according to the service name.") {
    val configurationStep = new DriverServiceBootstrapStep(
      SHORT_RESOURCE_NAME_PREFIX,
      DRIVER_LABELS,
      sparkConf
        .set("spark.driver.port", "9000")
        .set(org.apache.spark.internal.config.DRIVER_BLOCK_MANAGER_PORT, 8080)
        .set(KUBERNETES_NAMESPACE, "my-namespace"),
      clock)
    val baseDriverSpec = KubernetesDriverSpec.initialSpec(sparkConf.clone())
    val resolvedDriverSpec = configurationStep.configureDriver(baseDriverSpec)
    val expectedServiceName = SHORT_RESOURCE_NAME_PREFIX +
      DriverServiceBootstrapStep.DRIVER_SVC_POSTFIX
    val expectedHostName = s"$expectedServiceName.my-namespace.svc"
    verifySparkConfHostNames(resolvedDriverSpec.driverSparkConf, expectedHostName)
  }

  test("Ports should resolve to defaults in SparkConf and in the service.") {
    val configurationStep = new DriverServiceBootstrapStep(
      SHORT_RESOURCE_NAME_PREFIX,
      DRIVER_LABELS,
      sparkConf,
      clock)
    val baseDriverSpec = KubernetesDriverSpec.initialSpec(sparkConf.clone())
    val resolvedDriverSpec = configurationStep.configureDriver(baseDriverSpec)
    verifyService(
      DEFAULT_DRIVER_PORT,
      DEFAULT_BLOCKMANAGER_PORT,
      s"$SHORT_RESOURCE_NAME_PREFIX${DriverServiceBootstrapStep.DRIVER_SVC_POSTFIX}",
      resolvedDriverSpec.otherKubernetesResources.head.asInstanceOf[Service])
    assert(resolvedDriverSpec.driverSparkConf.get("spark.driver.port") ===
      DEFAULT_DRIVER_PORT.toString)
    assert(resolvedDriverSpec.driverSparkConf.get(
      org.apache.spark.internal.config.DRIVER_BLOCK_MANAGER_PORT) === DEFAULT_BLOCKMANAGER_PORT)
  }

  test("Long prefixes should switch to using a generated name.") {
    val configurationStep = new DriverServiceBootstrapStep(
      LONG_RESOURCE_NAME_PREFIX,
      DRIVER_LABELS,
      sparkConf.set(KUBERNETES_NAMESPACE, "my-namespace"),
      clock)
    when(clock.getTimeMillis()).thenReturn(10000)
    val baseDriverSpec = KubernetesDriverSpec.initialSpec(sparkConf.clone())
    val resolvedDriverSpec = configurationStep.configureDriver(baseDriverSpec)
    val driverService = resolvedDriverSpec.otherKubernetesResources.head.asInstanceOf[Service]
    val expectedServiceName = s"spark-10000${DriverServiceBootstrapStep.DRIVER_SVC_POSTFIX}"
    assert(driverService.getMetadata.getName === expectedServiceName)
    val expectedHostName = s"$expectedServiceName.my-namespace.svc"
    verifySparkConfHostNames(resolvedDriverSpec.driverSparkConf, expectedHostName)
  }

  test("Disallow bind address and driver host to be set explicitly.") {
    val configurationStep = new DriverServiceBootstrapStep(
      LONG_RESOURCE_NAME_PREFIX,
      DRIVER_LABELS,
      sparkConf.set(org.apache.spark.internal.config.DRIVER_BIND_ADDRESS, "host"),
      clock)
    try {
      configurationStep.configureDriver(KubernetesDriverSpec.initialSpec(sparkConf))
      fail("The driver bind address should not be allowed.")
    } catch {
      case e: Throwable =>
        assert(e.getMessage ===
          s"requirement failed: ${DriverServiceBootstrapStep.DRIVER_BIND_ADDRESS_KEY} is" +
          " not supported in Kubernetes mode, as the driver's bind address is managed" +
          " and set to the driver pod's IP address.")
    }
    sparkConf.remove(org.apache.spark.internal.config.DRIVER_BIND_ADDRESS)
    sparkConf.set(org.apache.spark.internal.config.DRIVER_HOST_ADDRESS, "host")
    try {
      configurationStep.configureDriver(KubernetesDriverSpec.initialSpec(sparkConf))
      fail("The driver host address should not be allowed.")
    } catch {
      case e: Throwable =>
        assert(e.getMessage ===
          s"requirement failed: ${DriverServiceBootstrapStep.DRIVER_HOST_KEY} is" +
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
      driverSparkConf: SparkConf, expectedHostName: String): Unit = {
    assert(driverSparkConf.get(
      org.apache.spark.internal.config.DRIVER_HOST_ADDRESS) === expectedHostName)
  }
}
