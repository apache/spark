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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.Constants._
import org.apache.spark.deploy.k8s.KubernetesTestConf
import org.apache.spark.internal.config.UI._

class DriverUIServiceFeatureStepSuite extends SparkFunSuite {

  test("SPARK-58203: no additional resource when the feature is disabled") {
    val sparkConf = new SparkConf(false).set(UI_PORT, 4080)
    val kconf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new DriverUIServiceFeatureStep(kconf)

    assert(step.getAdditionalKubernetesResources().isEmpty)
    assert(!step.getAdditionalPodSystemProperties()
      .contains(DriverUIServiceFeatureStep.KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL))
  }

  test("SPARK-58203: dedicated UI service is created endpointless even for a fixed port") {
    val sparkConf = new SparkConf(false)
      .set(UI_PORT, 4080)
      .set(KUBERNETES_DRIVER_UI_SERVICE_ENABLED, true)
    val kconf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new DriverUIServiceFeatureStep(kconf)

    val resources = step.getAdditionalKubernetesResources()
    assert(resources.size === 1)

    val uiSvc = resources.head.asInstanceOf[Service]
    assert(uiSvc.getMetadata.getName ===
      s"${kconf.resourceNamePrefix}${DRIVER_UI_SVC_POSTFIX}")
    assert(uiSvc.getSpec.getType === "ClusterIP")
    assert(uiSvc.getSpec.getSelector === null || uiSvc.getSpec.getSelector.isEmpty)
    assert(uiSvc.getSpec.getPorts.size === 1)
    assert(uiSvc.getSpec.getPorts.get(0).getName === UI_PORT_NAME)
    assert(uiSvc.getSpec.getPorts.get(0).getPort.intValue() === 4080)
    assert(uiSvc.getSpec.getPorts.get(0).getTargetPort.getIntVal === 4080)

    val props = step.getAdditionalPodSystemProperties()
    assert(props.get(DriverUIServiceFeatureStep.KUBERNETES_DRIVER_UI_SERVICE_PORT_INTERNAL)
      === Some("4080"))
    val encoded = props.get(
      DriverUIServiceFeatureStep.KUBERNETES_DRIVER_UI_SERVICE_SELECTOR_INTERNAL)
    assert(encoded.isDefined)
    assert(DriverUIServiceFeatureStep.decodeSelector(encoded.get) === kconf.labels)
  }

  test("SPARK-58203: no UI service is created when the Spark UI is disabled") {
    val sparkConf = new SparkConf(false)
      .set(UI_ENABLED, false)
      .set(KUBERNETES_DRIVER_UI_SERVICE_ENABLED, true)
    val kconf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new DriverUIServiceFeatureStep(kconf)

    // The enable flag is ignored (not fatal) when the UI is off: no Service, no runtime metadata.
    assert(step.getAdditionalKubernetesResources().isEmpty)
    assert(step.getAdditionalPodSystemProperties().isEmpty)
  }

  test("SPARK-58203: spark.ui.port=0 defers the selector and honors name/type overrides") {
    val sparkConf = new SparkConf(false)
      .set(UI_PORT, 0)
      .set(KUBERNETES_DRIVER_UI_SERVICE_ENABLED, true)
      .set(KUBERNETES_DRIVER_UI_SERVICE_TYPE, "NodePort")
      .set(KUBERNETES_DRIVER_UI_SERVICE_NAME, "my-app-ui-svc")
    val kconf = KubernetesTestConf.createDriverConf(sparkConf = sparkConf)
    val step = new DriverUIServiceFeatureStep(kconf)

    val uiSvc = step.getAdditionalKubernetesResources().head.asInstanceOf[Service]
    // Explicit name and type overrides are honored.
    assert(uiSvc.getMetadata.getName === "my-app-ui-svc")
    assert(uiSvc.getSpec.getType === "NodePort")
    // The default UI port stands in as a placeholder until the real bound port is patched.
    val expectedPlaceholder = UI_PORT.defaultValue.get
    assert(uiSvc.getSpec.getPorts.size === 1)
    assert(uiSvc.getSpec.getPorts.get(0).getPort.intValue() === expectedPlaceholder)
    assert(uiSvc.getSpec.getPorts.get(0).getTargetPort.getIntVal === expectedPlaceholder)
    // The Service is created endpointless so it cannot route the placeholder port anywhere.
    assert(uiSvc.getSpec.getSelector === null || uiSvc.getSpec.getSelector.isEmpty)

    // The name, placeholder port, and withheld selector are carried to the driver runtime for
    // the patcher to resolve once the UI has bound.
    val props = step.getAdditionalPodSystemProperties()
    assert(props.get(DriverUIServiceFeatureStep.KUBERNETES_DRIVER_UI_SERVICE_NAME_INTERNAL)
      === Some("my-app-ui-svc"))
    assert(props.get(DriverUIServiceFeatureStep.KUBERNETES_DRIVER_UI_SERVICE_PORT_INTERNAL)
      === Some(expectedPlaceholder.toString))
    val encoded = props.get(
      DriverUIServiceFeatureStep.KUBERNETES_DRIVER_UI_SERVICE_SELECTOR_INTERNAL)
    assert(encoded.isDefined)
    assert(DriverUIServiceFeatureStep.decodeSelector(encoded.get) === kconf.labels)
  }

  test("SPARK-58203: selector encode/decode round-trips label maps") {
    val selector = Map(
      "spark-app-selector" -> "spark-abc123",
      "spark-role" -> "driver",
      "app.kubernetes.io/name" -> "my.app-1")
    val encoded = DriverUIServiceFeatureStep.encodeSelector(selector)
    assert(DriverUIServiceFeatureStep.decodeSelector(encoded) === selector)
    assert(DriverUIServiceFeatureStep.decodeSelector("") === Map.empty)
  }
}
