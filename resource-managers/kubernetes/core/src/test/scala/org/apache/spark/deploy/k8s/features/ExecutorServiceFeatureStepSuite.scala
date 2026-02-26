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

import scala.jdk.CollectionConverters.{CollectionHasAsScala, MapHasAsScala}

import io.fabric8.kubernetes.api.model.{HasMetadata, Service}
import org.scalatest.BeforeAndAfter

import org.apache.spark.{SparkConf, SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.deploy.k8s.{KubernetesTestConf, SparkPod}
import org.apache.spark.deploy.k8s.Config.KUBERNETES_EXECUTOR_SERVICE_COOL_DOWN_PERIOD
import org.apache.spark.internal.config.BLOCK_MANAGER_PORT


class ExecutorServiceFeatureStepSuite extends SparkFunSuite with BeforeAndAfter {

  private var baseConf: SparkConf = _

  before {
    baseConf = new SparkConf(false)
    baseConf.set(BLOCK_MANAGER_PORT, 1234)
  }

  test("no block manager port") {
    baseConf.remove(BLOCK_MANAGER_PORT)
    intercept[SparkIllegalArgumentException] {
      evaluateStep()
    }
  }

  test("default configuration") {
    val (sparkPod, resources) = evaluateStep()
    assertSparkPod(sparkPod)
    assertResources(resources, expectedOwner = "driver", expectedCooldownPeriod = 300)
  }

  test("custom configuration") {
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_COOL_DOWN_PERIOD, 123)
    val (sparkPod, resources) = evaluateStep()
    assertSparkPod(sparkPod)
    assertResources(resources, expectedOwner = "driver", expectedCooldownPeriod = 123)
  }

  test("no cooldown period") {
    baseConf.set(KUBERNETES_EXECUTOR_SERVICE_COOL_DOWN_PERIOD, 0)
    val (sparkPod, resources) = evaluateStep()
    assertSparkPod(sparkPod)
    assertResources(resources, expectedOwner = "executor", expectedCooldownPeriod = 0)
  }

  private def assertSparkPod(sparkPod: SparkPod): Unit = {
    assert(sparkPod.pod.getSpec.getEnableServiceLinks === false)
    val env = sparkPod.container.getEnv.asScala.map(v => v.getName -> v.getValue).toMap
    assert(env.get("EXECUTOR_SERVICE_NAME") === Some("svc-appId-exec-1"))
  }

  private def assertResources(
      resources: Seq[HasMetadata],
      expectedOwner: String,
      expectedCooldownPeriod: Int): Unit = {
    assert(resources.size === 1)
    assert(resources.head.getKind === "Service")
    assertService(resources.head.asInstanceOf[Service], expectedOwner, expectedCooldownPeriod)
  }

  private def assertService(
      service: Service,
      expectedOwner: String,
      expectedCooldownPeriod: Int): Unit = {
    assert(service.getKind === "Service")
    assert(service.getMetadata.getName === "svc-appId-exec-1")
    assert(service.getMetadata.getAnnotations.asScala === Map(
      "spark.owner-reference" -> expectedOwner,
      "spark.cooldown-period" -> expectedCooldownPeriod.toString
    ))
    assert(service.getSpec.getSelector.asScala ===
      Map("spark-exec-id" -> "1", "spark-app-selector" -> "appId"))
    assert(service.getSpec.getPorts.size() === 1)
    val port = service.getSpec.getPorts.get(0)
    assert(port.getName === "spark-block-manager")
    assert(port.getPort === 1234)
    assert(port.getTargetPort.getIntVal === 1234)
  }

  private def evaluateStep(): (SparkPod, Seq[HasMetadata]) = {
    val executorConf = KubernetesTestConf.createExecutorConf(
      sparkConf = baseConf)
    val step = new ExecutorServiceFeatureStep(executorConf)
    (step.configurePod(SparkPod.initialPod()), step.getAdditionalKubernetesResources())
  }
}
