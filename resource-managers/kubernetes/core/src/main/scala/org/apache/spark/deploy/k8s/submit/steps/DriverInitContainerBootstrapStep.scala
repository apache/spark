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

import java.io.StringWriter
import java.util.Properties

import io.fabric8.kubernetes.api.model.{ConfigMap, ConfigMapBuilder, ContainerBuilder, HasMetadata}

import org.apache.spark.deploy.k8s.Config._
import org.apache.spark.deploy.k8s.submit.{InitContainerUtil, KubernetesDriverSpec}
import org.apache.spark.deploy.k8s.submit.steps.initcontainer.{InitContainerConfigurationStep, InitContainerSpec}

/**
 * Configures the init-container that bootstraps dependencies into the driver pod, including
 * building a ConfigMap that will be mounted into the init-container. The ConfigMap carries
 * configuration properties for the init-container.
 */
private[spark] class DriverInitContainerBootstrapStep(
    steps: Seq[InitContainerConfigurationStep],
    configMapName: String,
    configMapKey: String)
  extends DriverConfigurationStep {

  override def configureDriver(driverSpec: KubernetesDriverSpec): KubernetesDriverSpec = {
    var initContainerSpec = InitContainerSpec(
      properties = Map.empty[String, String],
      driverSparkConf = Map.empty[String, String],
      initContainer = new ContainerBuilder().build(),
      driverContainer = driverSpec.driverContainer,
      driverPod = driverSpec.driverPod,
      dependentResources = Seq.empty[HasMetadata])
    for (nextStep <- steps) {
      initContainerSpec = nextStep.configureInitContainer(initContainerSpec)
    }

    val configMap = buildConfigMap(
      configMapName,
      configMapKey,
      initContainerSpec.properties)
    val resolvedDriverSparkConf = driverSpec.driverSparkConf
      .clone()
      .set(INIT_CONTAINER_CONFIG_MAP_NAME, configMapName)
      .set(INIT_CONTAINER_CONFIG_MAP_KEY_CONF, configMapKey)
      .setAll(initContainerSpec.driverSparkConf)
    val resolvedDriverPod = InitContainerUtil.appendInitContainer(
      initContainerSpec.driverPod, initContainerSpec.initContainer)

    driverSpec.copy(
      driverPod = resolvedDriverPod,
      driverContainer = initContainerSpec.driverContainer,
      driverSparkConf = resolvedDriverSparkConf,
      otherKubernetesResources =
        driverSpec.otherKubernetesResources ++
          initContainerSpec.dependentResources ++
          Seq(configMap))
  }

  private def buildConfigMap(
      configMapName: String,
      configMapKey: String,
      config: Map[String, String]): ConfigMap = {
    val properties = new Properties()
    config.foreach { entry =>
      properties.setProperty(entry._1, entry._2)
    }
    val propertiesWriter = new StringWriter()
    properties.store(propertiesWriter,
      s"Java properties built from Kubernetes config map with name: $configMapName " +
        s"and config map key: $configMapKey")
    new ConfigMapBuilder()
      .withNewMetadata()
        .withName(configMapName)
        .endMetadata()
      .addToData(configMapKey, propertiesWriter.toString)
      .build()
  }
}
