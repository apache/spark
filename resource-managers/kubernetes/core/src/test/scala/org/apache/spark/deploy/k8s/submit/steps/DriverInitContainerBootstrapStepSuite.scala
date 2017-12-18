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

import java.io.StringReader
import java.util.Properties

import scala.collection.JavaConverters._

import com.google.common.collect.Maps
import io.fabric8.kubernetes.api.model.{ConfigMap, ContainerBuilder, HasMetadata, PodBuilder, SecretBuilder}

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.submit.KubernetesDriverSpec
import org.apache.spark.deploy.k8s.submit.steps.initcontainer.{InitContainerConfigurationStep, InitContainerSpec}
import org.apache.spark.util.Utils

class DriverInitContainerBootstrapStepSuite extends SparkFunSuite {

  private val CONFIG_MAP_NAME = "spark-init-config-map"
  private val CONFIG_MAP_KEY = "spark-init-config-map-key"

  test("The init container bootstrap step should use all of the init container steps") {
    val baseDriverSpec = KubernetesDriverSpec(
      driverPod = new PodBuilder().build(),
      driverContainer = new ContainerBuilder().build(),
      driverSparkConf = new SparkConf(false),
      otherKubernetesResources = Seq.empty[HasMetadata])
    val initContainerSteps = Seq(
      FirstTestInitContainerConfigurationStep,
      SecondTestInitContainerConfigurationStep)
    val bootstrapStep = new DriverInitContainerBootstrapStep(
      initContainerSteps,
      CONFIG_MAP_NAME,
      CONFIG_MAP_KEY)

    val preparedDriverSpec = bootstrapStep.configureDriver(baseDriverSpec)

    assert(preparedDriverSpec.driverPod.getMetadata.getLabels.asScala ===
      FirstTestInitContainerConfigurationStep.additionalLabels)
    val additionalDriverEnv = preparedDriverSpec.driverContainer.getEnv.asScala
    assert(additionalDriverEnv.size === 1)
    assert(additionalDriverEnv.head.getName ===
      FirstTestInitContainerConfigurationStep.additionalMainContainerEnvKey)
    assert(additionalDriverEnv.head.getValue ===
      FirstTestInitContainerConfigurationStep.additionalMainContainerEnvValue)

    assert(preparedDriverSpec.otherKubernetesResources.size === 2)
    assert(preparedDriverSpec.otherKubernetesResources.contains(
      FirstTestInitContainerConfigurationStep.additionalKubernetesResource))
    assert(preparedDriverSpec.otherKubernetesResources.exists {
      case configMap: ConfigMap =>
        val hasMatchingName = configMap.getMetadata.getName == CONFIG_MAP_NAME
        val configMapData = configMap.getData.asScala
        val hasCorrectNumberOfEntries = configMapData.size == 1
        val initContainerPropertiesRaw = configMapData(CONFIG_MAP_KEY)
        val initContainerProperties = new Properties()
        Utils.tryWithResource(new StringReader(initContainerPropertiesRaw)) {
          initContainerProperties.load(_)
        }
        val initContainerPropertiesMap = Maps.fromProperties(initContainerProperties).asScala
        val expectedInitContainerProperties = Map(
          SecondTestInitContainerConfigurationStep.additionalInitContainerPropertyKey ->
            SecondTestInitContainerConfigurationStep.additionalInitContainerPropertyValue)
        val hasMatchingProperties = initContainerPropertiesMap == expectedInitContainerProperties
        hasMatchingName && hasCorrectNumberOfEntries && hasMatchingProperties
      case _ => false
    })

    val initContainers = preparedDriverSpec.driverPod.getSpec.getInitContainers
    assert(initContainers.size() === 1)
    val initContainerEnv = initContainers.get(0).getEnv.asScala
    assert(initContainerEnv.size === 1)
    assert(initContainerEnv.head.getName ===
      SecondTestInitContainerConfigurationStep.additionalInitContainerEnvKey)
    assert(initContainerEnv.head.getValue ===
      SecondTestInitContainerConfigurationStep.additionalInitContainerEnvValue)

    val expectedSparkConf = Map(
      INIT_CONTAINER_CONFIG_MAP_NAME.key -> CONFIG_MAP_NAME,
      INIT_CONTAINER_CONFIG_MAP_KEY_CONF.key -> CONFIG_MAP_KEY,
      SecondTestInitContainerConfigurationStep.additionalDriverSparkConfKey ->
        SecondTestInitContainerConfigurationStep.additionalDriverSparkConfValue)
    assert(preparedDriverSpec.driverSparkConf.getAll.toMap === expectedSparkConf)
  }
}

private object FirstTestInitContainerConfigurationStep extends InitContainerConfigurationStep {

  val additionalLabels = Map("additionalLabelkey" -> "additionalLabelValue")
  val additionalMainContainerEnvKey = "TEST_ENV_MAIN_KEY"
  val additionalMainContainerEnvValue = "TEST_ENV_MAIN_VALUE"
  val additionalKubernetesResource = new SecretBuilder()
    .withNewMetadata()
    .withName("test-secret")
    .endMetadata()
    .addToData("secret-key", "secret-value")
    .build()

  override def configureInitContainer(initContainerSpec: InitContainerSpec): InitContainerSpec = {
    val driverPod = new PodBuilder(initContainerSpec.driverPod)
      .editOrNewMetadata()
      .addToLabels(additionalLabels.asJava)
      .endMetadata()
      .build()
    val mainContainer = new ContainerBuilder(initContainerSpec.driverContainer)
      .addNewEnv()
      .withName(additionalMainContainerEnvKey)
      .withValue(additionalMainContainerEnvValue)
      .endEnv()
      .build()
    initContainerSpec.copy(
      driverPod = driverPod,
      driverContainer = mainContainer,
      initContainerDependentResources = initContainerSpec.initContainerDependentResources ++
        Seq(additionalKubernetesResource))
  }
}

private object SecondTestInitContainerConfigurationStep extends InitContainerConfigurationStep {
  val additionalInitContainerEnvKey = "TEST_ENV_INIT_KEY"
  val additionalInitContainerEnvValue = "TEST_ENV_INIT_VALUE"
  val additionalInitContainerPropertyKey = "spark.initcontainer.testkey"
  val additionalInitContainerPropertyValue = "testvalue"
  val additionalDriverSparkConfKey = "spark.driver.testkey"
  val additionalDriverSparkConfValue = "spark.driver.testvalue"

  override def configureInitContainer(initContainerSpec: InitContainerSpec): InitContainerSpec = {
    val initContainer = new ContainerBuilder(initContainerSpec.initContainer)
      .addNewEnv()
      .withName(additionalInitContainerEnvKey)
      .withValue(additionalInitContainerEnvValue)
      .endEnv()
      .build()
    val initContainerProperties = initContainerSpec.initContainerProperties ++
      Map(additionalInitContainerPropertyKey -> additionalInitContainerPropertyValue)
    val driverSparkConf = initContainerSpec.driverSparkConf ++
      Map(additionalDriverSparkConfKey -> additionalDriverSparkConfValue)
    initContainerSpec.copy(
      initContainer = initContainer,
      initContainerProperties = initContainerProperties,
      driverSparkConf = driverSparkConf)
  }
}
