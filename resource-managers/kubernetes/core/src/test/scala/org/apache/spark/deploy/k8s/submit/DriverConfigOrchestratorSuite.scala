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
package org.apache.spark.deploy.k8s.submit

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.submit.steps._

class DriverConfigOrchestratorSuite extends SparkFunSuite {

  private val DRIVER_IMAGE = "driver-image"
  private val IC_IMAGE = "init-container-image"
  private val APP_ID = "spark-app-id"
  private val LAUNCH_TIME = 975256L
  private val APP_NAME = "spark"
  private val MAIN_CLASS = "org.apache.spark.examples.SparkPi"
  private val APP_ARGS = Array("arg1", "arg2")
  private val SECRET_FOO = "foo"
  private val SECRET_BAR = "bar"
  private val SECRET_MOUNT_PATH = "/etc/secrets/driver"

  test("Base submission steps with a main app resource.") {
    val sparkConf = new SparkConf(false)
      .set(DRIVER_CONTAINER_IMAGE, DRIVER_IMAGE)
    val mainAppResource = JavaMainAppResource("local:///var/apps/jars/main.jar")
    val orchestrator = new DriverConfigOrchestrator(
      APP_ID,
      LAUNCH_TIME,
      Some(mainAppResource),
      APP_NAME,
      MAIN_CLASS,
      APP_ARGS,
      sparkConf)
    validateStepTypes(
      orchestrator,
      classOf[BaseDriverConfigurationStep],
      classOf[DriverServiceBootstrapStep],
      classOf[DriverKubernetesCredentialsStep],
      classOf[DependencyResolutionStep]
    )
  }

  test("Base submission steps without a main app resource.") {
    val sparkConf = new SparkConf(false)
      .set(DRIVER_CONTAINER_IMAGE, DRIVER_IMAGE)
    val orchestrator = new DriverConfigOrchestrator(
      APP_ID,
      LAUNCH_TIME,
      Option.empty,
      APP_NAME,
      MAIN_CLASS,
      APP_ARGS,
      sparkConf)
    validateStepTypes(
      orchestrator,
      classOf[BaseDriverConfigurationStep],
      classOf[DriverServiceBootstrapStep],
      classOf[DriverKubernetesCredentialsStep]
    )
  }

  test("Submission steps with an init-container.") {
    val sparkConf = new SparkConf(false)
      .set(DRIVER_CONTAINER_IMAGE, DRIVER_IMAGE)
      .set(INIT_CONTAINER_IMAGE, IC_IMAGE)
      .set("spark.jars", "hdfs://localhost:9000/var/apps/jars/jar1.jar")
    val mainAppResource = JavaMainAppResource("local:///var/apps/jars/main.jar")
    val orchestrator = new DriverConfigOrchestrator(
      APP_ID,
      LAUNCH_TIME,
      Some(mainAppResource),
      APP_NAME,
      MAIN_CLASS,
      APP_ARGS,
      sparkConf)
    validateStepTypes(
      orchestrator,
      classOf[BaseDriverConfigurationStep],
      classOf[DriverServiceBootstrapStep],
      classOf[DriverKubernetesCredentialsStep],
      classOf[DependencyResolutionStep],
      classOf[DriverInitContainerBootstrapStep])
  }

  test("Submission steps with driver secrets to mount") {
    val sparkConf = new SparkConf(false)
      .set(DRIVER_CONTAINER_IMAGE, DRIVER_IMAGE)
      .set(s"$KUBERNETES_DRIVER_SECRETS_PREFIX$SECRET_FOO", SECRET_MOUNT_PATH)
      .set(s"$KUBERNETES_DRIVER_SECRETS_PREFIX$SECRET_BAR", SECRET_MOUNT_PATH)
    val mainAppResource = JavaMainAppResource("local:///var/apps/jars/main.jar")
    val orchestrator = new DriverConfigOrchestrator(
      APP_ID,
      LAUNCH_TIME,
      Some(mainAppResource),
      APP_NAME,
      MAIN_CLASS,
      APP_ARGS,
      sparkConf)
    validateStepTypes(
      orchestrator,
      classOf[BaseDriverConfigurationStep],
      classOf[DriverServiceBootstrapStep],
      classOf[DriverKubernetesCredentialsStep],
      classOf[DependencyResolutionStep],
      classOf[DriverMountSecretsStep])
  }

  private def validateStepTypes(
      orchestrator: DriverConfigOrchestrator,
      types: Class[_ <: DriverConfigurationStep]*): Unit = {
    val steps = orchestrator.getAllConfigurationSteps
    assert(steps.size === types.size)
    assert(steps.map(_.getClass) === types)
  }
}
