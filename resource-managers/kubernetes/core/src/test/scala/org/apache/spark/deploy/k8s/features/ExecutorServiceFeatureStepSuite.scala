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

import io.fabric8.kubernetes.api.model.{EnvVarBuilder, Service}
import scala.collection.JavaConverters._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.{KubernetesExecutorConf, KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.internal.config._
import org.apache.spark.util.ManualClock


class ExecutorServiceFeatureStepSuite extends SparkFunSuite {

  private val LONG_RESOURCE_NAME_PREFIX =
    "a" * (ExecutorServiceFeatureStep.MAX_SERVICE_NAME_LENGTH -
      ExecutorServiceFeatureStep.EXECUTOR_SVC_POSTFIX.length + 1)
  private val EXECUTOR_LABELS = Map(
    "label1key" -> "label1value",
    "label2key" -> "label2value")
  private val EXECUTOR_SERVICE_ANNOTATIONS = Map(
    "annotation1key" -> "annotation1value",
    "annotation2key" -> "annotation2value")

  test("Headless service correct port for block manager") {
    val sparkConf = new SparkConf(false)
      .set(BLOCK_MANAGER_PORT, 9000)
    val kconf = KubernetesTestConf.createExecutorConf(
      sparkConf = sparkConf,
      labels = EXECUTOR_LABELS,
      serviceAnnotations = EXECUTOR_SERVICE_ANNOTATIONS)
    val configurationStep = new ExecutorServiceFeatureStep(kconf)
    assert(configurationStep.configurePod(SparkPod.initialPod()) === SparkPod.initialPod())
    assert(configurationStep.getAdditionalKubernetesResources().size === 1)
    assert(configurationStep.getAdditionalKubernetesResources().head.isInstanceOf[Service])
    val executorService = configurationStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Service]
    verifyService(
      9000,
      expectedServiceName(kconf),
      executorService)
  }

  test("Confirm pod IP is correctly post processed") {
    val sparkConf = new SparkConf(false)
      .set(BLOCK_MANAGER_PORT, 9000)
      .set(KUBERNETES_NAMESPACE, "my-namespace")
    val kconf = KubernetesTestConf.createExecutorConf(
      sparkConf = sparkConf,
      labels = EXECUTOR_LABELS)
    val configurationStep = new ExecutorServiceFeatureStep(kconf)
    val serviceName = expectedServiceName(kconf)
    val expectedHostName = s"$serviceName.my-namespace.svc"
    val emptyPod = SparkPod.initialPod()
    emptyPod.container.getEnv.add(new EnvVarBuilder().
      withName(ENV_EXECUTOR_POD_IP).withValue("dummy").build())
    val postConfiguredPod = configurationStep.configurePod(emptyPod)
    assert(postConfiguredPod.container.getEnv.asScala.filter
    (envVar => envVar.getName== ENV_EXECUTOR_POD_IP).head.getValue == expectedHostName)
  }

  private def expectedServiceName(kconf: KubernetesExecutorConf) = {
    s"${kconf.resourceNamePrefix}-${kconf.executorId}" +
      s"${ExecutorServiceFeatureStep.EXECUTOR_SVC_POSTFIX}"
  }

  test("Ports should resolve to defaults in SparkConf and in the service.") {
    val kconf = KubernetesTestConf.createExecutorConf(
      labels = EXECUTOR_LABELS,
      serviceAnnotations = EXECUTOR_SERVICE_ANNOTATIONS)
    val configurationStep = new ExecutorServiceFeatureStep(kconf)
    val resolvedService = configurationStep
      .getAdditionalKubernetesResources()
      .head
      .asInstanceOf[Service]
    verifyService(
      DEFAULT_BLOCKMANAGER_PORT,
      expectedServiceName(kconf),
      resolvedService)
  }

  test("Long prefixes should switch to using a generated unique name.") {
    val sparkConf = new SparkConf(false)
      .set(KUBERNETES_NAMESPACE, "my-namespace")
    val kconf = KubernetesTestConf.createExecutorConf(
      sparkConf = sparkConf,
      resourceNamePrefix = Some(LONG_RESOURCE_NAME_PREFIX),
      labels = EXECUTOR_LABELS)
    val clock = new ManualClock()

    // Ensure that multiple services created at the same time generate unique names.
    val services = (1 to 10).map { _ =>
      val configurationStep = new ExecutorServiceFeatureStep(kconf, clock = clock)
      val serviceName = configurationStep
        .getAdditionalKubernetesResources()
        .head
        .asInstanceOf[Service]
        .getMetadata
        .getName

      serviceName
    }.toSet

    assert(services.size === 10)
    services.foreach { case (name) =>
      assert(!name.startsWith(kconf.resourceNamePrefix))
    }
  }

  private def verifyService(
                             blockManagerPort: Int,
                             expectedServiceName: String,
                             service: Service): Unit = {
    assert(service.getMetadata.getName === expectedServiceName)
    assert(service.getSpec.getClusterIP === "None")
    EXECUTOR_LABELS.foreach { case (k, v) =>
      assert(service.getSpec.getSelector.get(k) === v)
    }
    EXECUTOR_SERVICE_ANNOTATIONS.foreach { case (k, v) =>
      assert(service.getMetadata.getAnnotations.get(k) === v)
    }
    assert(service.getSpec.getPorts.size() === 1)
    val driverServicePorts = service.getSpec.getPorts.asScala
    assert(driverServicePorts.head.getName === BLOCK_MANAGER_PORT_NAME)
    assert(driverServicePorts.head.getPort.intValue() === blockManagerPort)
    assert(driverServicePorts.head.getTargetPort.getIntVal === blockManagerPort)
  }
}
