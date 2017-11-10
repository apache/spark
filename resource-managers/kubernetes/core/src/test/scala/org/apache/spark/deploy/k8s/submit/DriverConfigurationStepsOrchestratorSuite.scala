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
import org.apache.spark.deploy.k8s.Config.DRIVER_DOCKER_IMAGE
import org.apache.spark.deploy.k8s.submit.steps.{BaseDriverConfigurationStep, DriverConfigurationStep, DriverKubernetesCredentialsStep, DriverServiceBootstrapStep}

class DriverConfigurationStepsOrchestratorSuite extends SparkFunSuite {

  private val NAMESPACE = "default"
  private val DRIVER_IMAGE = "driver-image"
  private val APP_ID = "spark-app-id"
  private val LAUNCH_TIME = 975256L
  private val APP_NAME = "spark"
  private val MAIN_CLASS = "org.apache.spark.examples.SparkPi"
  private val APP_ARGS = Array("arg1", "arg2")

  test("Base submission steps without an init-container or python files.") {
    val sparkConf = new SparkConf(false)
      .set(DRIVER_DOCKER_IMAGE, DRIVER_IMAGE)
    val mainAppResource = JavaMainAppResource("local:///var/apps/jars/main.jar")
    val orchestrator = new DriverConfigurationStepsOrchestrator(
      NAMESPACE,
      APP_ID,
      LAUNCH_TIME,
      mainAppResource,
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

  private def validateStepTypes(
      orchestrator: DriverConfigurationStepsOrchestrator,
      types: Class[_ <: DriverConfigurationStep]*): Unit = {
    val steps = orchestrator.getAllConfigurationSteps()
    assert(steps.size === types.size)
    assert(steps.map(_.getClass) === types)
  }
}
