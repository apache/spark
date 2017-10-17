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
package org.apache.spark.deploy.k8s.submit.submitsteps.initcontainer

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.config._
import org.apache.spark.deploy.k8s.constants._

class InitContainerConfigurationStepsOrchestratorSuite extends SparkFunSuite {
  private val NAMESPACE = "namespace"
  private val APP_RESOURCE_PREFIX = "spark-prefix"
  private val SPARK_JARS = Seq(
    "hdfs://localhost:9000/app/jars/jar1.jar", "file:///app/jars/jar2.jar")
  private val SPARK_FILES = Seq(
    "hdfs://localhost:9000/app/files/file1.txt", "file:///app/files/file2.txt")
  private val JARS_DOWNLOAD_PATH = "/var/data/jars"
  private val FILES_DOWNLOAD_PATH = "/var/data/files"
  private val DOCKER_IMAGE_PULL_POLICY: String = "IfNotPresent"
  private val APP_ID = "spark-id"
  private val CUSTOM_LABEL_KEY = "customLabel"
  private val CUSTOM_LABEL_VALUE = "customLabelValue"
  private val DEPRECATED_CUSTOM_LABEL_KEY = "deprecatedCustomLabel"
  private val DEPRECATED_CUSTOM_LABEL_VALUE = "deprecatedCustomLabelValue"
  private val DRIVER_LABELS = Map(
    CUSTOM_LABEL_KEY -> CUSTOM_LABEL_VALUE,
    DEPRECATED_CUSTOM_LABEL_KEY -> DEPRECATED_CUSTOM_LABEL_VALUE,
    SPARK_APP_ID_LABEL -> APP_ID,
    SPARK_ROLE_LABEL -> SPARK_POD_DRIVER_ROLE)
  private val INIT_CONTAINER_CONFIG_MAP_NAME = "spark-init-config-map"
  private val INIT_CONTAINER_CONFIG_MAP_KEY = "spark-init-config-map-key"
  private val STAGING_SERVER_URI = "http://localhost:8000"

  test ("error thrown if local jars provided without resource staging server") {
    val sparkConf = new SparkConf(true)
      .set(s"$KUBERNETES_DRIVER_LABEL_PREFIX$CUSTOM_LABEL_KEY", CUSTOM_LABEL_VALUE)

    assert(sparkConf.get(RESOURCE_STAGING_SERVER_URI).isEmpty)

    val thrown = intercept[IllegalArgumentException] {
      val orchestrator = new InitContainerConfigurationStepsOrchestrator(
        NAMESPACE,
        APP_RESOURCE_PREFIX,
        SPARK_JARS,
        SPARK_FILES,
        JARS_DOWNLOAD_PATH,
        FILES_DOWNLOAD_PATH,
        DOCKER_IMAGE_PULL_POLICY,
        DRIVER_LABELS,
        INIT_CONTAINER_CONFIG_MAP_NAME,
        INIT_CONTAINER_CONFIG_MAP_KEY,
        sparkConf)
    }

    assert(thrown.getMessage contains "Local JARs were provided, however no resource staging" +
      " server URI was found.")
  }

  test ("error not thrown with non-local jars and resource staging server provided") {
    val sparkConf = new SparkConf(true)
      .set(s"$KUBERNETES_DRIVER_LABEL_PREFIX$CUSTOM_LABEL_KEY", CUSTOM_LABEL_VALUE)
      .set(RESOURCE_STAGING_SERVER_URI, STAGING_SERVER_URI)

    val orchestrator = new InitContainerConfigurationStepsOrchestrator(
      NAMESPACE,
      APP_RESOURCE_PREFIX,
      SPARK_JARS.take(1),
      SPARK_FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      DOCKER_IMAGE_PULL_POLICY,
      DRIVER_LABELS,
      INIT_CONTAINER_CONFIG_MAP_NAME,
      INIT_CONTAINER_CONFIG_MAP_KEY,
      sparkConf)
    val initSteps : Seq[InitContainerConfigurationStep] =
        orchestrator.getAllConfigurationSteps()
    assert(initSteps.length == 2)
    assert(initSteps.head.isInstanceOf[BaseInitContainerConfigurationStep])
    assert(initSteps(1).isInstanceOf[SubmittedResourcesInitContainerConfigurationStep])
  }

  test ("error not thrown with non-local jars and no resource staging server provided") {
    val sparkConf = new SparkConf(true)
      .set(s"$KUBERNETES_DRIVER_LABEL_PREFIX$CUSTOM_LABEL_KEY", CUSTOM_LABEL_VALUE)

    val orchestrator = new InitContainerConfigurationStepsOrchestrator(
      NAMESPACE,
      APP_RESOURCE_PREFIX,
      SPARK_JARS.take(1),
      SPARK_FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      DOCKER_IMAGE_PULL_POLICY,
      DRIVER_LABELS,
      INIT_CONTAINER_CONFIG_MAP_NAME,
      INIT_CONTAINER_CONFIG_MAP_KEY,
      sparkConf)
    val initSteps : Seq[InitContainerConfigurationStep] =
        orchestrator.getAllConfigurationSteps()
    assert(initSteps.length == 1)
    assert(initSteps.head.isInstanceOf[BaseInitContainerConfigurationStep])
  }

  test ("including step to contact resource staging server") {
    val sparkConf = new SparkConf(true)
      .set(s"$KUBERNETES_DRIVER_LABEL_PREFIX$CUSTOM_LABEL_KEY", CUSTOM_LABEL_VALUE)
      .set(RESOURCE_STAGING_SERVER_URI, STAGING_SERVER_URI)

    val orchestrator = new InitContainerConfigurationStepsOrchestrator(
      NAMESPACE,
      APP_RESOURCE_PREFIX,
      SPARK_JARS,
      SPARK_FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      DOCKER_IMAGE_PULL_POLICY,
      DRIVER_LABELS,
      INIT_CONTAINER_CONFIG_MAP_NAME,
      INIT_CONTAINER_CONFIG_MAP_KEY,
      sparkConf)
    val initSteps : Seq[InitContainerConfigurationStep] =
        orchestrator.getAllConfigurationSteps()
    assert(initSteps.length == 2)
    assert(initSteps.head.isInstanceOf[BaseInitContainerConfigurationStep])
    assert(initSteps(1).isInstanceOf[SubmittedResourcesInitContainerConfigurationStep])
  }

  test ("not including steps because no contact to resource staging server") {
    val sparkConf = new SparkConf(true)
      .set(s"$KUBERNETES_DRIVER_LABEL_PREFIX$CUSTOM_LABEL_KEY", CUSTOM_LABEL_VALUE)

    val orchestrator = new InitContainerConfigurationStepsOrchestrator(
      NAMESPACE,
      APP_RESOURCE_PREFIX,
      SPARK_JARS.take(1),
      SPARK_FILES,
      JARS_DOWNLOAD_PATH,
      FILES_DOWNLOAD_PATH,
      DOCKER_IMAGE_PULL_POLICY,
      DRIVER_LABELS,
      INIT_CONTAINER_CONFIG_MAP_NAME,
      INIT_CONTAINER_CONFIG_MAP_KEY,
      sparkConf)
    val initSteps : Seq[InitContainerConfigurationStep] =
        orchestrator.getAllConfigurationSteps()
    assert(initSteps.length === 1)
    assert(initSteps.head.isInstanceOf[BaseInitContainerConfigurationStep])
  }
}
