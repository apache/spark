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
package org.apache.spark.deploy.kubernetes.submit.submitsteps

import io.fabric8.kubernetes.api.model.{ContainerBuilder, HasMetadata}

import org.apache.spark.deploy.kubernetes.config._
import org.apache.spark.deploy.kubernetes.submit.{InitContainerUtil, PropertiesConfigMapFromScalaMapBuilder}
import org.apache.spark.deploy.kubernetes.submit.submitsteps.initcontainer.{InitContainerConfigurationStep, InitContainerSpec}

/**
 * Configures the init-container that bootstraps dependencies into the driver pod.
 */
private[spark] class InitContainerBootstrapStep(
    initContainerConfigurationSteps: Seq[InitContainerConfigurationStep],
    initContainerConfigMapName: String,
    initContainerConfigMapKey: String)
  extends DriverConfigurationStep {

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    var currentInitContainerSpec = InitContainerSpec(
      initContainerProperties = Map.empty[String, String],
      additionalDriverSparkConf = Map.empty[String, String],
      initContainer = new ContainerBuilder().build(),
      driverContainer = driverSpec.driverContainer,
      podToInitialize = driverSpec.driverPod,
      initContainerDependentResources = Seq.empty[HasMetadata])
    for (nextStep <- initContainerConfigurationSteps) {
      currentInitContainerSpec = nextStep.configureInitContainer(currentInitContainerSpec)
    }
    val configMap = PropertiesConfigMapFromScalaMapBuilder.buildConfigMap(
      initContainerConfigMapName,
      initContainerConfigMapKey,
      currentInitContainerSpec.initContainerProperties)
    val resolvedDriverSparkConf = driverSpec.driverSparkConf.clone()
      .set(EXECUTOR_INIT_CONTAINER_CONFIG_MAP, initContainerConfigMapName)
      .set(EXECUTOR_INIT_CONTAINER_CONFIG_MAP_KEY, initContainerConfigMapKey)
      .setAll(currentInitContainerSpec.additionalDriverSparkConf)
    val resolvedDriverPod = InitContainerUtil.appendInitContainer(
      currentInitContainerSpec.podToInitialize, currentInitContainerSpec.initContainer)
    driverSpec.copy(
      driverPod = resolvedDriverPod,
      driverContainer = currentInitContainerSpec.driverContainer,
      driverSparkConf = resolvedDriverSparkConf,
      otherKubernetesResources =
        driverSpec.otherKubernetesResources ++
          currentInitContainerSpec.initContainerDependentResources ++
          Seq(configMap))
  }
}
