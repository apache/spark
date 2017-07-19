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
package org.apache.spark.deploy.kubernetes.submit

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.kubernetes.submit.submitsteps.{BaseDriverConfigurationStep, DependencyResolutionStep, DriverKubernetesCredentialsStep, InitContainerBootstrapStep, PythonStep}

private[spark] class DriverConfigurationStepsOrchestratorSuite extends SparkFunSuite {

  private val NAMESPACE = "default"
  private val APP_ID = "spark-app-id"
  private val LAUNCH_TIME = 975256L
  private val APP_NAME = "spark"
  private val MAIN_CLASS = "org.apache.spark.examples.SparkPi"
  private val APP_ARGS = Array("arg1", "arg2")
  private val ADDITIONAL_PYTHON_FILES = Seq("local:///var/apps/python/py1.py")

  test("Base submission steps without an init-container or python files.") {
    val sparkConf = new SparkConf(false)
      .set("spark.jars", "local:///var/apps/jars/jar1.jar")
    val mainAppResource = JavaMainAppResource("local:///var/apps/jars/main.jar")
    val orchestrator = new DriverConfigurationStepsOrchestrator(
        NAMESPACE,
        APP_ID,
        LAUNCH_TIME,
        mainAppResource,
        APP_NAME,
        MAIN_CLASS,
        APP_ARGS,
        ADDITIONAL_PYTHON_FILES,
        sparkConf)
    val steps = orchestrator.getAllConfigurationSteps()
    assert(steps.size === 3)
    assert(steps(0).isInstanceOf[BaseDriverConfigurationStep])
    assert(steps(1).isInstanceOf[DriverKubernetesCredentialsStep])
    assert(steps(2).isInstanceOf[DependencyResolutionStep])
  }

  test("Submission steps with an init-container.") {
    val sparkConf = new SparkConf(false)
      .set("spark.jars", "hdfs://localhost:9000/var/apps/jars/jar1.jar")
    val mainAppResource = JavaMainAppResource("local:///var/apps/jars/main.jar")
    val orchestrator = new DriverConfigurationStepsOrchestrator(
        NAMESPACE,
        APP_ID,
        LAUNCH_TIME,
        mainAppResource,
        APP_NAME,
        MAIN_CLASS,
        APP_ARGS,
        ADDITIONAL_PYTHON_FILES,
        sparkConf)
    val steps = orchestrator.getAllConfigurationSteps()
    assert(steps.size === 4)
    assert(steps(0).isInstanceOf[BaseDriverConfigurationStep])
    assert(steps(1).isInstanceOf[DriverKubernetesCredentialsStep])
    assert(steps(2).isInstanceOf[DependencyResolutionStep])
    assert(steps(3).isInstanceOf[InitContainerBootstrapStep])
  }

  test("Submission steps with python files.") {
    val sparkConf = new SparkConf(false)
    val mainAppResource = PythonMainAppResource("local:///var/apps/python/main.py")
    val orchestrator = new DriverConfigurationStepsOrchestrator(
        NAMESPACE,
        APP_ID,
        LAUNCH_TIME,
        mainAppResource,
        APP_NAME,
        MAIN_CLASS,
        APP_ARGS,
        ADDITIONAL_PYTHON_FILES,
        sparkConf)
    val steps = orchestrator.getAllConfigurationSteps()
    assert(steps.size === 4)
    assert(steps(0).isInstanceOf[BaseDriverConfigurationStep])
    assert(steps(1).isInstanceOf[DriverKubernetesCredentialsStep])
    assert(steps(2).isInstanceOf[DependencyResolutionStep])
    assert(steps(3).isInstanceOf[PythonStep])
  }
}
