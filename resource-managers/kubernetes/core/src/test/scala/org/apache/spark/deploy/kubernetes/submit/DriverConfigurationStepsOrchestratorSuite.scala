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
import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.submit.submitsteps.{BaseDriverConfigurationStep, DependencyResolutionStep, DriverAddressConfigurationStep, DriverConfigurationStep, DriverKubernetesCredentialsStep, InitContainerBootstrapStep, MountSecretsStep, MountSmallLocalFilesStep, PythonStep}

private[spark] class DriverConfigurationStepsOrchestratorSuite extends SparkFunSuite {

  private val NAMESPACE = "default"
  private val APP_ID = "spark-app-id"
  private val LAUNCH_TIME = 975256L
  private val APP_NAME = "spark"
  private val MAIN_CLASS = "org.apache.spark.examples.SparkPi"
  private val APP_ARGS = Array("arg1", "arg2")
  private val ADDITIONAL_PYTHON_FILES = Seq("local:///var/apps/python/py1.py")
  private val SECRET_FOO = "foo"
  private val SECRET_BAR = "bar"
  private val SECRET_MOUNT_PATH = "/etc/secrets/driver"

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
    validateStepTypes(
        orchestrator,
        classOf[BaseDriverConfigurationStep],
        classOf[DriverAddressConfigurationStep],
        classOf[DriverKubernetesCredentialsStep],
        classOf[DependencyResolutionStep])
  }

  test("Submission steps with an init-container.") {
    val sparkConf = new SparkConf(false)
        .set("spark.jars", "hdfs://localhost:9000/var/apps/jars/jar1.jar")
        .set(RESOURCE_STAGING_SERVER_URI, "https://localhost:8080")
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
    validateStepTypes(
        orchestrator,
        classOf[BaseDriverConfigurationStep],
        classOf[DriverAddressConfigurationStep],
        classOf[DriverKubernetesCredentialsStep],
        classOf[DependencyResolutionStep],
        classOf[InitContainerBootstrapStep])
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
    validateStepTypes(
        orchestrator,
        classOf[BaseDriverConfigurationStep],
        classOf[DriverAddressConfigurationStep],
        classOf[DriverKubernetesCredentialsStep],
        classOf[DependencyResolutionStep],
        classOf[PythonStep])
  }

  test("Only local files without a resource staging server.") {
    val sparkConf = new SparkConf(false).set("spark.files", "/var/spark/file1.txt")
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
    validateStepTypes(
        orchestrator,
        classOf[BaseDriverConfigurationStep],
        classOf[DriverAddressConfigurationStep],
        classOf[DriverKubernetesCredentialsStep],
        classOf[DependencyResolutionStep],
        classOf[MountSmallLocalFilesStep])
  }

  test("Submission steps with driver secrets to mount") {
    val sparkConf = new SparkConf(false)
      .set(s"$KUBERNETES_DRIVER_SECRETS_PREFIX$SECRET_FOO", SECRET_MOUNT_PATH)
      .set(s"$KUBERNETES_DRIVER_SECRETS_PREFIX$SECRET_BAR", SECRET_MOUNT_PATH)
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
    validateStepTypes(
      orchestrator,
      classOf[BaseDriverConfigurationStep],
      classOf[DriverAddressConfigurationStep],
      classOf[DriverKubernetesCredentialsStep],
      classOf[DependencyResolutionStep],
      classOf[MountSecretsStep])
  }

  private def validateStepTypes(
      orchestrator: DriverConfigurationStepsOrchestrator,
      types: Class[_ <: DriverConfigurationStep]*): Unit = {
    val steps = orchestrator.getAllConfigurationSteps()
    assert(steps.size === types.size)
    assert(steps.map(_.getClass) === types)
  }
}
