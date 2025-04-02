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

import com.google.common.net.InternetDomainName
import io.fabric8.kubernetes.api.model.Service

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.config._
import org.apache.spark.internal.config.UI._
import org.apache.spark.util.ManualClock

class DriverServiceFeatureStepSuite extends SparkFunSuite {

  private val LONG_RESOURCE_NAME_PREFIX =
    "a" * (DriverServiceFeatureStep.MAX_SERVICE_NAME_LENGTH -
      DriverServiceFeatureStep.DRIVER_SVC_POSTFIX.length + 1)
  private val DRIVER_LABELS = Map(
    "label1key" -> "label1value",
    "label2key" -> "label2value")
  private val DRIVER_SERVICE_ANNOTATIONS = Map(
    "annotation1key" -> "annotation1value",
    "annotation2key" -> "annotation2value")
  private val DRIVER_SERVICE_LABELS = Map(
    "svclabel1key" -> "svclabel1value",
    "svclabel2key" -> "svclabel2value")

  test("Headless service has a port for the driver RPC, the block manager and driver ui.") {
    val sparkConf = new SparkConf(false)
      .set(DRIVER_PORT, 9000)
      .set(DRIVER_BLOCK_MANAGER_PORT, 8080)
      .set(UI_PORT, 4080)
    val kconf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = DRIVER_LABELS,
      serviceLabels = DRIVER_SERVICE_LABELS,
      serviceAnnotations = DRIVER_SERVICE_ANNOTATIONS)
    val configurationStep = new DriverServiceFeatureStep(kconf)
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
      4080,
      s"${kconf.resourceNamePrefix}${DriverServiceFeatureStep.DRIVER_SVC_POSTFIX}",
      kconf.appId,
      driverService)
  }

  test("Hostname and ports are set according to the service name.") {
    val sparkConf = new SparkConf(false)
      .set(DRIVER_PORT, 9000)
      .set(DRIVER_BLOCK_MANAGER_PORT, 8080)
      .set(KUBERNETES_NAMESPACE, "my-namespace")
    val kconf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = DRIVER_LABELS)
    val configurationStep = new DriverServiceFeatureStep(kconf)
    val expectedServiceName = kconf.resourceNamePrefix + DriverServiceFeatureStep.DRIVER_SVC_POSTFIX
    val expectedHostName = s"$expectedServiceName.my-namespace.svc"
    val additionalProps = configurationStep.getAdditionalPodSystemProperties()
    assert(additionalProps(DRIVER_HOST_ADDRESS.key) === expectedHostName)
  }

  test("Ports should resolve to defaults in SparkConf and in the service.") {
    val kconf = KubernetesTestConf.createDriverConf(
      labels = DRIVER_LABELS,
      serviceLabels = DRIVER_SERVICE_LABELS,
      serviceAnnotations = DRIVER_SERVICE_ANNOTATIONS)
    val configurationStep = new DriverServiceFeatureStep(kconf)
    val resolvedService = configurationStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Service]
    verifyService(
      DEFAULT_DRIVER_PORT,
      DEFAULT_BLOCKMANAGER_PORT,
      UI_PORT.defaultValue.get,
      s"${kconf.resourceNamePrefix}${DriverServiceFeatureStep.DRIVER_SVC_POSTFIX}",
      kconf.appId,
      resolvedService)
    val additionalProps = configurationStep.getAdditionalPodSystemProperties()
    assert(additionalProps(DRIVER_PORT.key) === DEFAULT_DRIVER_PORT.toString)
    assert(additionalProps(DRIVER_BLOCK_MANAGER_PORT.key) === DEFAULT_BLOCKMANAGER_PORT.toString)
  }

  test("Long prefixes should switch to using a generated unique name.") {
    val clock = new ManualClock()
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_NAMESPACE, "my-namespace")

    // Ensure that multiple services created at the same time generate unique names.
    val services = (1 to 10).map { _ =>
      val kconf = KubernetesTestConf.createDriverConf(
        sparkConf = sparkConf,
        resourceNamePrefix = Some(LONG_RESOURCE_NAME_PREFIX),
        labels = DRIVER_LABELS,
        clock = clock)
      val configurationStep = new DriverServiceFeatureStep(kconf)
      val serviceName = configurationStep
        .getAdditionalKubernetesResources()
        .head
        .asInstanceOf[Service]
        .getMetadata
        .getName

      val hostAddress = configurationStep
        .getAdditionalPodSystemProperties()(DRIVER_HOST_ADDRESS.key)

      Tuple3(kconf, serviceName, hostAddress)
    }

    assert(services.size === 10)
    services.foreach { case (kconf, name, address) =>
      assert(!name.startsWith(kconf.resourceNamePrefix))
      assert(!address.startsWith(kconf.resourceNamePrefix))
      assert(InternetDomainName.isValid(address))
    }
  }

  test("Disallow bind address and driver host to be set explicitly.") {
    val sparkConf = new SparkConf(false)
      .set(DRIVER_BIND_ADDRESS, "host")
      .set("spark.app.name", LONG_RESOURCE_NAME_PREFIX)
    val e1 = intercept[IllegalArgumentException] {
      new DriverServiceFeatureStep(KubernetesTestConf.createDriverConf(sparkConf = sparkConf))
    }
    assert(e1.getMessage ===
      s"requirement failed: ${DriverServiceFeatureStep.DRIVER_BIND_ADDRESS_KEY} is" +
      " not supported in Kubernetes mode, as the driver's bind address is managed" +
      " and set to the driver pod's IP address.")

    sparkConf.remove(DRIVER_BIND_ADDRESS)
    sparkConf.set(DRIVER_HOST_ADDRESS, "host")

    val e2 = intercept[IllegalArgumentException] {
      new DriverServiceFeatureStep(KubernetesTestConf.createDriverConf(sparkConf = sparkConf))
    }
    assert(e2.getMessage ===
      s"requirement failed: ${DriverServiceFeatureStep.DRIVER_HOST_KEY} is" +
      " not supported in Kubernetes mode, as the driver's hostname will be managed via" +
      " a Kubernetes service.")
  }

  test("Support ipFamilies spec with default SingleStack and IPv4") {
    val sparkConf = new SparkConf(false)
    val kconf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = DRIVER_LABELS,
      serviceLabels = DRIVER_SERVICE_LABELS,
      serviceAnnotations = DRIVER_SERVICE_ANNOTATIONS)
    val configurationStep = new DriverServiceFeatureStep(kconf)
    assert(configurationStep.configurePod(SparkPod.initialPod()) === SparkPod.initialPod())
    val driverService = configurationStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Service]
    assert(driverService.getSpec.getIpFamilyPolicy() == "SingleStack")
    assert(driverService.getSpec.getIpFamilies.size() === 1)
    assert(driverService.getSpec.getIpFamilies.get(0) == "IPv4")
  }

  test("Support ipFamilies spec with SingleStack and IPv6") {
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_DRIVER_SERVICE_IP_FAMILIES, "IPv6")
    val kconf = KubernetesTestConf.createDriverConf(
      sparkConf = sparkConf,
      labels = DRIVER_LABELS,
      serviceLabels = DRIVER_SERVICE_LABELS,
      serviceAnnotations = DRIVER_SERVICE_ANNOTATIONS)
    val configurationStep = new DriverServiceFeatureStep(kconf)
    assert(configurationStep.configurePod(SparkPod.initialPod()) === SparkPod.initialPod())
    val driverService = configurationStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Service]
    assert(driverService.getSpec.getIpFamilyPolicy() == "SingleStack")
    assert(driverService.getSpec.getIpFamilies.size() === 1)
    assert(driverService.getSpec.getIpFamilies.get(0) == "IPv6")
  }

  test("Support DualStack") {
    Seq("PreferDualStack", "RequireDualStack").foreach { stack =>
      val configAndAnswers = Seq(
        ("IPv4,IPv6", Seq("IPv4", "IPv6")),
        ("IPv6,IPv4", Seq("IPv6", "IPv4")))
      configAndAnswers.foreach { case (config, answer) =>
        val sparkConf = new SparkConf(false)
          .set(KUBERNETES_DRIVER_SERVICE_IP_FAMILY_POLICY, stack)
          .set(KUBERNETES_DRIVER_SERVICE_IP_FAMILIES, config)
        val kconf = KubernetesTestConf.createDriverConf(
          sparkConf = sparkConf,
          labels = DRIVER_LABELS,
          serviceLabels = DRIVER_SERVICE_LABELS,
          serviceAnnotations = DRIVER_SERVICE_ANNOTATIONS)
        val configurationStep = new DriverServiceFeatureStep(kconf)
        assert(configurationStep.configurePod(SparkPod.initialPod()) === SparkPod.initialPod())
        val driverService = configurationStep
          .getAdditionalKubernetesResources()
          .head
          .asInstanceOf[Service]
        assert(driverService.getSpec.getIpFamilyPolicy() == stack)
        assert(driverService.getSpec.getIpFamilies === answer.asJava)
      }
    }
  }

  private def verifyService(
      driverPort: Int,
      blockManagerPort: Int,
      drierUIPort: Int,
      expectedServiceName: String,
      appId: String,
      service: Service): Unit = {
    assert(service.getMetadata.getName === expectedServiceName)
    assert(service.getMetadata.getLabels.containsKey(SPARK_APP_ID_LABEL) &&
      service.getMetadata.getLabels.get(SPARK_APP_ID_LABEL).equals(appId))
    assert(service.getSpec.getClusterIP === "None")
    DRIVER_LABELS.foreach { case (k, v) =>
      assert(service.getSpec.getSelector.get(k) === v)
    }
    DRIVER_SERVICE_LABELS.foreach { case (k, v) =>
      assert(service.getMetadata.getLabels.get(k) === v)
    }
    DRIVER_SERVICE_ANNOTATIONS.foreach { case (k, v) =>
      assert(service.getMetadata.getAnnotations.get(k) === v)
    }
    assert(service.getSpec.getPorts.size() === 3)
    val driverServicePorts = service.getSpec.getPorts.asScala
    assert(driverServicePorts.head.getName === DRIVER_PORT_NAME)
    assert(driverServicePorts.head.getPort.intValue() === driverPort)
    assert(driverServicePorts.head.getTargetPort.getIntVal === driverPort)
    assert(driverServicePorts(1).getName === BLOCK_MANAGER_PORT_NAME)
    assert(driverServicePorts(1).getPort.intValue() === blockManagerPort)
    assert(driverServicePorts(1).getTargetPort.getIntVal === blockManagerPort)
    assert(driverServicePorts(2).getName === UI_PORT_NAME)
    assert(driverServicePorts(2).getPort.intValue() === drierUIPort)
    assert(driverServicePorts(2).getTargetPort.getIntVal === drierUIPort)
  }
}
